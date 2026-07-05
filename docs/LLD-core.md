# Tandem — `tandem-core` LLD

**Version:** 0.1 (Draft)  
**Module:** `tandem-core` · package `com.codingful.tandem.core`  
**Depends on:** nothing (**zero external runtime dependencies**, §1.3)  
**Resolves open questions:** Q1 (port signatures), Q2 (model), Q3 (payload), Q4 (metrics port),
Q5 (exceptions), Q24 (DISCARDED). See [open-questions-lld.md](open-questions-lld.md).

`tandem-core` is the functional core (HLD §1.2): it defines the **models**, the **ports**
(interfaces implemented by the adapters), the **exception** hierarchy, and the **pure logic**
(Lamport merge, status codes). It contains no I/O and no third-party dependencies.

---

## 1. Domain model

### 1.1 `AggregateId` (value object)

```java
public record AggregateId(String value) {
    public AggregateId {
        Objects.requireNonNull(value, "aggregateId");
        if (value.isBlank()) throw new IllegalArgumentException("aggregateId must not be blank");
        if (value.length() > 255) throw new IllegalArgumentException("aggregateId exceeds 255 chars");
    }
    public static AggregateId of(String value) { return new AggregateId(value); }
    @Override public String toString() { return value; }
}
```

A typed value object (Q2) — prevents mixing the several string fields (`aggregateId`,
`aggregateType`, `type`). Length bound mirrors `VARCHAR(255)`.

### 1.2 `OutboxStatus` (enum ↔ `SMALLINT`)

```java
public enum OutboxStatus {
    PENDING(0), IN_FLIGHT(1), DONE(2), FAILED(3), DISCARDED(4);   // Q24
    private final int code;
    OutboxStatus(int code) { this.code = code; }
    public int code() { return code; }
    public static OutboxStatus fromCode(int code) { /* switch; throws on unknown */ }
}
```

`DISCARDED` (4) is reachable only via the Admin API on a `FAILED` row; it is never polled and
does not block the aggregate (HLD §5.3).

### 1.3 `OutboxMessage` (write-side value)

What the client builds and inserts — immutable, with a builder.

```java
public final class OutboxMessage {
    private final AggregateId aggregateId;
    private final String aggregateType;
    private final String type;            // CloudEvents `type` (nullable; see Q20)
    private final long   seq;             // app-assigned, from the aggregate's version (HLD §4.2)
    private final byte[] payload;         // already serialized (Q3)
    private final String contentType;     // e.g. "application/json"; persisted into headers["content-type"] (LLD-jdbc §2)
    private final Map<String,String> headers;     // immutable copy; may be empty

    // builder(); accessors; equals/hashCode use Arrays.equals on payload
    public static Builder builder() { ... }
}
```

- **Payload is `byte[]`** (Q3): the core never serializes, so it forces no JSON library on the
  client (§1.3). Higher tiers may offer an `Object`-accepting overload backed by a
  `PayloadSerializer` (below); the plain tier passes bytes/`String`.
- `seq` is a `long`, app-assigned (HLD §4.2) — the core never generates it.
- **`contentType`** is the only typed convenience field that maps onto a header: the write-side
  serializes it into `headers["content-type"]` at insert (LLD-jdbc §2), the key the relay reads for
  the CloudEvents `datacontenttype` (LLD-kafka §3.2). No dedicated column — reuse `headers` (Pareto).
- **No `causationId` here.** Causation is part of the opt-in **causal-ordering** feature (the peer of
  `lamport`, off by default; HLD §9), so it is **not** carried on the basic-round write value. When
  causal ordering is enabled it is propagated like `lamport` (header/extension), not added to this
  basic model — keeping the basic round free of plumbing for a disabled feature.

### 1.4 `OutboxRecord` (stored row)

The persisted row with delivery state, returned by the relay-side store and the Admin API.

```java
public final class OutboxRecord {
    private final long id;
    private final OutboxMessage message;
    private final OutboxStatus status;
    private final int    attempts;
    private final String lockedBy;        // nullable
    private final Instant lockedUntil;    // nullable
    private final String lastError;       // nullable
    private final Instant nextAttemptAt;  // nullable
    private final Instant createdAt;
    private final Long   lamport;         // nullable; only when causal ordering enabled (§9)
    // accessors
}
```

---

## 2. Ports

Ports are interfaces **defined by the core** and implemented by adapters (HLD §1.2). The
"implemented in" column is informative — the signatures live here.

| Port | Implemented in | Purpose |
|---|---|---|
| `OutboxRepository` | `tandem-jdbc` (+ `InMemoryOutbox`) | Write-side insert |
| `OutboxStore` | `tandem-jdbc` | Relay-side persistence (poll/claim/update/cleanup) |
| `OutboxDispatcher` | `tandem-kafka` | Publish one record to Kafka |
| `PayloadSerializer` | client / `tandem-spring` (JSON) | Object → bytes |
| `TopicRouter` | `tandem-kafka` (default) | `aggregateType` → topic |
| `CausalContext` | `tandem-spring` (consumer side) | Inbound Lamport timestamp |
| `AttemptRecorder` | core (no-op) / `tandem-jdbc` | Attempt archive (§7.1) |
| `TracePropagator` | core (no-op) / `tandem-spring`, `tandem-tracing-otel` | Trace capture (§7.2) |
| `TandemMetrics` | core (no-op) / `tandem-micrometer` | Metrics (§7) |
| `ReplayService` | `tandem-jdbc` | Replay (§8) |
| `TandemAggregate` | client's aggregate | Expose pending messages (annotation tier) |

### 2.1 Write-side

```java
public interface OutboxRepository {
    void insert(OutboxMessage message);                 // within the caller's transaction
    void insertAll(Collection<OutboxMessage> messages); // same transaction
}
```

### 2.2 Relay-side persistence

```java
public interface OutboxStore {
    /** Poll the worker's owned virtual buckets for the **head of each aggregate's pending
        chain** (the earliest not-yet-DONE row, E2 — which also subsumes the poison gate) and
        mark the batch IN_FLIGHT for this worker (Q8: virtual-bucket sharding, §4.3/§6).
        Transaction boundaries: Q9 (LLD-jdbc). */
    List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId,
                                  Duration lease, int batchSize);
    void markDone(long id);
    void markForRetry(long id, String error, Instant nextAttemptAt);  // → PENDING
    void markFailed(long id, String error);                           // → FAILED
    int  reclaimExpiredLeases();                                       // → PENDING; returns count
    int  cleanup(Instant doneBefore, int batchSize);                  // housekeeping (Q12)
}
```

### 2.3 Publish

```java
public interface OutboxDispatcher {
    /** Publish the record **asynchronously**. The returned future completes when the broker
        acks (acks=all), or completes **exceptionally** with an {@link OutboxDispatchException}
        carrying the retriable/permanent verdict (Q17). The dispatcher never blocks on the ack,
        so the relay can keep many records of **distinct aggregates** in flight on a single
        producer — the per-shard concurrency window (LLD-jdbc §3.4). */
    CompletableFuture<Void> dispatch(OutboxRecord record);
}
```

`dispatch` is non-blocking by contract: per-aggregate ordering is enforced *upstream* (the relay
ever has only one row per aggregate — its head — in flight; LLD-jdbc §3.3/§3.4), not by blocking
here. `CompletableFuture` is `java.util.concurrent` (JDK), so the zero-dependency rule holds.

### 2.4 Serialization & routing

```java
public interface PayloadSerializer {
    byte[] serialize(Object payload);   // throws PayloadSerializationException
    String contentType();               // e.g. "application/json"
}

public interface TopicRouter {
    String topicFor(OutboxRecord record);   // the single routing method; default rule = Q18

    /** Default router: kebab-case(aggregateType) + suffix (LLD-kafka §5). */
    static TopicRouter kebabWithSuffix(String suffix) { /* … */ }
}
```

There is **no default `PayloadSerializer` in core** (a JSON one needs a JSON library — §1.3).
A Jackson-based default ships in `tandem-spring`; non-Spring users supply one or pass bytes.

### 2.5 Optional capability ports (no-op defaults in core)

```java
public interface TandemMetrics {                       // §7
    default boolean isEnabled() { return false; }
    void recordLag(long pending);
    void recordLagAgeSeconds(double age);
    void incrementPublished(long n);
    void recordFailed(long count);
    void incrementRetry();
    void incrementLeaseExpired(long n);
    void recordActiveWorkers(int n);
    void recordUncoveredBuckets(int n);   // buckets with PENDING rows but no live owner (§7)
    void recordConfigInvalid(String check); // startup config invariant violated (e.g. rowLease ≤ delivery.timeout.ms); LLD-jdbc §3.5
}

public interface AttemptRecorder {                     // §7.1
    default boolean isEnabled() { return false; }
    void record(AttemptOutcome outcome);
}

public interface TracePropagator {                     // §7.2
    default boolean isEnabled() { return false; }
    Map<String,String> capture();   // {} when disabled
}

public interface CausalContext {                       // §9
    OptionalLong inboundTimestamp();   // empty → mutation is a causal root
}
```

`AttemptOutcome` is a small record (outboxId, attemptNumber, status, started/finished, latency,
worker, topic/partition/offset, errorClass/message/detail, traceId, correlationId) — full shape
in HLD-attempt-archive. The **no-op defaults** make every capability zero-cost when off; callers
also guard on `isEnabled()` before building any payload (HLD §7.1/§7.2).

### 2.6 Replay & aggregate hook

```java
public interface ReplayService {                       // §8
    ReplayResult replay(ReplayCriteria criteria);      // honours dryRun
}
public record ReplayCriteria(AggregateId aggregateId, String aggregateType,
                             Long fromId, Long toId,
                             Set<OutboxStatus> statuses, boolean dryRun) { /* ≥1 selector required */ }
public record ReplayResult(long matched, long replayed, boolean dryRun) {}

public interface TandemAggregate {                     // annotation tier (HLD §3.1)
    Collection<OutboxMessage> pendingOutboxMessages();
}
```

---

## 3. Exception hierarchy (Q5)

All **unchecked** (`RuntimeException`) to keep the API clean — the insert already runs inside the
caller's `@Transactional`.

```java
public class TandemException extends RuntimeException { ... }          // base

public class OutboxInsertException     extends TandemException { ... }  // write-side INSERT failed
public class DuplicateSeqException     extends OutboxInsertException {} // UNIQUE(aggregate_id, seq)
public class PayloadSerializationException extends TandemException { ... }
public class OutboxDispatchException   extends TandemException {         // publish failed (Q17)
    private final boolean retriable;      // the dispatcher's verdict
    public boolean isRetriable() { return retriable; }
}
public class TandemConfigurationException  extends TandemException { ... }
```

`DuplicateSeqException` is the notable one: callers using optimistic locking catch it to detect a
`seq` conflict and retry, without parsing SQL state.

`OutboxDispatchException` **carries the retriable-vs-permanent verdict** (`isRetriable()`): the
`tandem-kafka` dispatcher classifies the Kafka error (LLD-kafka §4) and sets it, so the `tandem-jdbc`
relay routes the failure to `markForRetry` (retriable) or `markFailed` (permanent) without knowing
Kafka exception types — the seam that connects Q17 to Q9/Q10.

---

## 4. Pure logic (in core)

```java
public final class LamportClock {                      // §9.2
    /** new = max(local, inbound) + 1 */
    public static long merge(long local, long inbound) { return Math.max(local, inbound) + 1; }
}
```

```java
public final class BucketHash {                       // §4.3 HLD
    /** Virtual bucket for an aggregate id: a 64-bit FNV-1a hash over the UTF-8 bytes of
        {@code aggregateId}, reduced with {@code Math.floorMod} into {@code [0, bucketCount)}.
        Pure, dependency-free, and deterministic across DB engines and versions — so the
        stored bucket is stable for the life of the data (HLD §4.3). */
    public static int bucketFor(String aggregateId, int bucketCount) {
        long h = 0xcbf29ce484222325L;                 // FNV-1a 64-bit offset basis
        for (byte b : aggregateId.getBytes(StandardCharsets.UTF_8)) {
            h ^= (b & 0xff);
            h *= 0x100000001b3L;                       // FNV-1a 64-bit prime
        }
        return Math.floorMod(h, bucketCount);          // non-negative, overflow-free, any bucketCount
    }
}
```

`Math.floorMod` is used deliberately: it is always non-negative and **overflow-free even for
`Long.MIN_VALUE`** (unlike `abs(...) % n`, which overflows on the minimum value), and it works
for any `bucketCount`, not only powers of two.

Other pure helpers: `OutboxStatus.fromCode`, header-name constants (`ce_logicalclock`,
`traceparent`, `correlation-id`, `causation_id`), and the CloudEvents header keys. The
**bucket hash is computed in Java by the core** (above): `tandem-jdbc` calls it at insert and
`InMemoryOutbox` calls the *same* function, so in-memory tests and the real database agree on
the bucket. The core defines both the `(bucket, bucketCount)` contract **and** the hash itself —
nothing DB-side.

---

## 5. Still-open items touching this module

These are referenced by the signatures above but resolved in other LLDs:

- **Q18** — exact default `TopicRouter` rule (`tandem-kafka`).
- **Q9** — transaction boundaries of the poll→publish→DONE cycle (`tandem-jdbc`). *(Q8 resolved:
  **virtual-bucket sharding** — fixed `B`, workers own bucket subsets; `SINGLE` (in-process, all
  buckets) or `LEASE` (`tandem_bucket_lease`-table) coordination; structural exclusivity, §4.3.)*
- **Q20** — behaviour when `type` is null (`tandem-kafka`).
- **Q6** — consolidated `TandemProperties` reference (`tandem-spring`).
