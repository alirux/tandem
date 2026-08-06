# Tandem — Send Attempt Archive (Design Note)

**Version:** 1.1  
**Status:** Designed, **not implemented** — and deliberately absent from both the code and the
API contract.

A durable, append-only history of **every** publish attempt — temporal data, outcome,
error detail, attempt number, trace/correlation id, and the destination coordinates — for
forensic debugging and operations. The feature is **opt-in and off by default**; when off
it adds **no performance cost** and **no setup or configuration burden**.

**Nothing in Tandem implements any of this**, and nothing reserves surface for it: there is no
`AttemptRecorder` port, no `tandem_outbox_attempt` table, and no Admin API operation. Unused
API surface is worse than none — it appears in a consumer's IDE autocomplete and in a published
contract while doing nothing — so the design lives here alone, and everything below describes
what **would** be built. The definitions in §5.1 and §8 are exact, so a future implementation
starts from a specification rather than a sketch.

---

## 1. Motivation

The outbox row carries only the *latest* attempt state (`attempts`, `last_error`,
`next_attempt_at`). When an event misbehaves in production — flapping retries, a transient
broker error, a poison message — an operator cannot reconstruct *what happened on each
attempt*: when it ran, how long it took, which worker, which error, under which trace.

The attempt archive records one immutable row per attempt, so the full timeline of an
event's delivery is queryable after the fact.

---

## 2. What is recorded (per attempt)

| Field | Purpose |
|---|---|
| `outbox_id` | The event this attempt belongs to (join back to `outbox`) |
| `aggregate_id`, `aggregate_type` | Denormalized for ops queries without a join |
| `attempt_number` | Which attempt this was (mirrors `outbox.attempts` at the time) |
| `status` | Outcome: `SUCCESS` or `FAILED` |
| `started_at`, `finished_at` | When the attempt began and completed |
| `latency_ms` | Attempt duration (`finished_at − started_at`) |
| `worker_id` | The worker/`locked_by` that ran the attempt |
| `topic`, `partition`, `kafka_offset` | Destination coordinates (on success) |
| `error_class`, `error_message`, `error_detail` | Exception type, message, full detail/stacktrace (on failure) |
| `trace_id`, `correlation_id` | Distributed-tracing / correlation identifiers (from headers; see §7) |
| `created_at` | Row insertion time |

The intent is **everything useful to debug an event and help an operator** — extendable as
new diagnostic needs arise, without touching the hot outbox table.

---

## 3. Data model (created only when enabled)

```sql
-- Created ONLY when the attempt archive is enabled.
-- The base Tandem setup does NOT include this table.
CREATE TABLE tandem_outbox_attempt (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    outbox_id      BIGINT       NOT NULL,
    aggregate_id   VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    attempt_number INT          NOT NULL,
    status         SMALLINT     NOT NULL,        -- 1 = SUCCESS, 2 = FAILED
    started_at     TIMESTAMPTZ  NOT NULL,
    finished_at    TIMESTAMPTZ  NOT NULL,
    latency_ms     INT,
    worker_id      VARCHAR(64),
    topic          VARCHAR(255),                 -- on success
    partition      INT,                          -- on success
    kafka_offset   BIGINT,                       -- on success
    error_class    VARCHAR(255),                 -- on failure
    error_message  TEXT,                         -- on failure
    error_detail   TEXT,                         -- stacktrace / full detail, on failure
    trace_id       VARCHAR(128),
    correlation_id VARCHAR(128),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Full attempt timeline of one event
CREATE INDEX idx_tandem_outbox_attempt_outbox    ON tandem_outbox_attempt (outbox_id, attempt_number);
-- Per-aggregate forensic queries
CREATE INDEX idx_tandem_outbox_attempt_aggregate ON tandem_outbox_attempt (aggregate_id, created_at);
-- Failure investigation over time
CREATE INDEX idx_tandem_outbox_attempt_failed    ON tandem_outbox_attempt (created_at) WHERE status = 2;
```

The table is append-only; it is never updated, only inserted and purged (see §6).

---

## 4. Lifecycle — when an attempt is recorded

The relay invokes the recorder after each publish attempt completes, on **both** paths. Because the
async relay marks **success in batches across aggregates** but handles **failure per record** (HLD §6,
LLD-jdbc §3.4), the archive write follows the same shape — always inside the status-update transaction:

```
[KafkaRelay] publish attempts (async; batch_size records of distinct aggregates in flight)
   │
   ├── success (ack): the per-record ack data (topic/partition/offset, latency, trace…) is captured
   │        in the send's completion handler and buffered with the DONE id; at the markDoneBatch
   │        flush, the SAME transaction does
   │            UPDATE tandem_outbox SET status = DONE WHERE id = ANY(:ids)
   │            INSERT INTO tandem_outbox_attempt VALUES (…), (…), …   -- one row per acked record
   │
   └── failure: in the SAME per-record transaction that updates attempts/last_error/next_attempt_at
            (or marks FAILED),
            INSERT tandem_outbox_attempt(status=FAILED, error_class/message/detail, attempt#, trace…)
```

The archive insert **piggybacks on the status-update transaction the relay already performs** — no new
round-trip transaction (a **multi-row** insert on the batched success flush, a **single** insert on a
per-record failure). It is therefore as consistent as the status update itself: if that transaction
rolls back, so do its archive rows, and the subsequent retry records a fresh attempt (correct — a
re-attempt *is* a new attempt).

---

## 5. Opt-in, off by default, zero cost when off

This is the defining constraint. Realized via the Hexagonal pattern (HLD §1.2):

- **Port:** `AttemptRecorder` (in `tandem-core`).
- **Default adapter:** `NoOpAttemptRecorder` — does nothing. Wired by default.
- **Opt-in adapter:** `JdbcAttemptArchive` (in `tandem-jdbc`) — the table writer, wired
  only when the feature is enabled.

What "zero cost / zero setup when off" means concretely:

| Concern | When **off** (default) | When **on** |
|---|---|---|
| Schema | **No table.** Base setup is just the `outbox` table. | `tandem_outbox_attempt` table created (separate, optional DDL/migration) |
| Configuration | **Nothing to set.** | A single flag, e.g. `tandem.attempt-archive.enabled=true` |
| Write path | **No insert, no object built** (see guard below) | Archive rows inside the existing status-update tx — multi-row on the batched success flush, single on a per-record failure (§4) |
| Module | No extra module — the no-op lives in core | Uses the `tandem-jdbc` adapter |

**The guard (true zero-cost).** The relay checks the enabled flag *before* assembling the
attempt record, so when disabled there is **no timing capture, no error-detail extraction,
and no object allocation** — only a boolean check that the JIT renders negligible:

```
if (attemptRecorder.isEnabled()) {      // false by default → nothing below runs
    var record = buildAttemptRecord(...);   // timestamps, error detail, trace ids
    attemptRecorder.record(record);
}
```

A no-op recorder alone is not enough — the *construction* of the record must also be
skipped. The guard guarantees the off path adds nothing measurable.

### 5.1 Port and model

Both belong in `tandem-core`, which is dependency-free — so neither references anything beyond
the JDK and the core's own types.

```java
public interface AttemptRecorder {

    /** A no-op recorder — the default when the archive is disabled. */
    AttemptRecorder NOOP = new AttemptRecorder() {
    };

    /** {@code true} once a real adapter is wired; the relay only builds AttemptOutcome when enabled. */
    default boolean isEnabled() {
        return false;
    }

    default void record(AttemptOutcome outcome) {
    }
}

public record AttemptOutcome(long outboxId,
                             AggregateId aggregateId,
                             String aggregateType,
                             int attemptNumber,
                             AttemptStatus status,      // SUCCESS(1) | FAILED(2)
                             Instant startedAt,
                             Instant finishedAt,
                             Integer latencyMs,
                             String workerId,
                             String topic,              // success only
                             Integer partition,         // success only
                             Long kafkaOffset,          // success only
                             String errorClass,         // failure only
                             String errorMessage,       // failure only
                             String errorDetail,        // failure only
                             String traceId,
                             String correlationId) {
}
```

`AttemptOutcome` mirrors the `tandem_outbox_attempt` columns of §3 minus the two the database
owns (`id`, `created_at`). Note it is a **17-field record carrying `errorMessage` / `errorDetail`**:
building it would require the `toString()` redaction rule and its dedicated unit test that
AGENTS.md's logging conventions demand of every core type reachable from a log statement.

---

## 6. Retention

The archive grows **N× faster** than the outbox (one row per attempt, not per event), so
it needs its own retention policy — independent of the outbox cleanup:

- **Configurable retention window** (e.g. keep 7–30 days).
- **Recommended: time-partitioning** on `created_at` so old data is dropped by detaching a
  partition rather than row-by-row `DELETE`.
- Retention is **only relevant when the feature is on** — another reason it carries zero
  burden by default.

---

## 7. Relation to trace / correlation id propagation

`trace_id` and `correlation_id` are read from the event's headers/context at attempt time.
Fully populating them depends on those identifiers being propagated into the outbox
`headers` at write time — the **trace & correlation propagation** feature
([HLD-tracing.md](HLD-tracing.md), HLD §7.1). With propagation off, the archive simply captures
whatever identifiers the application already placed in the headers; with it on, capture is
automatic. The archive is the primary *consumer* of those ids, which is why the two
features are designed together: propagation puts the ids in `headers`, the archive reads them
out for forensics. **Each works without the other** — with propagation off the archive leaves
the two columns null, and propagation is fully useful on its own, since the standard
`traceparent` reaches consumers whether or not anything archives attempts. Tracing's
instrumented mode would additionally supply the `trace_id` of the real send instant rather than
the write's.

Also note the distinction that HLD §9 draws: `correlation_id` groups related work, whereas
`causation_id` links an effect to its one cause. The archive stores the former.

---

## 8. Admin API surface

The archive is a *read* surface for operators, exposed through the Admin API
([HLD-admin-api.md](HLD-admin-api.md), [admin-api.openapi.yaml](admin-api.openapi.yaml)) —
never a second control path. Two operations:

| Operation | Endpoint | Purpose |
|---|---|---|
| `getMessageAttempts` | `GET /outbox/messages/{id}/attempts` | Attempt timeline of one message, chronological — returns `AttemptRecord[]` |
| `searchAttempts` | `GET /outbox/attempts` | Search the archive by `aggregateId` / `aggregateType` / `status` / `traceId` / `correlationId` / `createdFrom` / `createdTo`, cursor-paginated (`limit` 1–500, default 50) — returns `AttemptPage` |

**`503 attempt-archive-disabled`** answers `searchAttempts` when the archive is off — a
canonical RFC 9457 problem (`https://tandem.codingful.com/problems/attempt-archive-disabled`,
title *The attempt archive is disabled*). `getMessageAttempts` returns an **empty list**
instead: a message with no recorded attempts is a legitimate reading, whereas a *search*
returning empty would falsely imply the archive was consulted and found nothing.

**The API model is its own type, not the core's** (a project invariant — the read model, the
write model, and the wire model evolve independently). `AttemptRecord` is what the contract
publishes; `AttemptOutcome` (§5.1) is what the relay hands the port. They differ deliberately:
the API adds the database-owned `id` and `createdAt`, and names the Kafka offset `offset`
rather than `kafkaOffset`.

```yaml
AttemptStatus:                    # shared by the search filter and AttemptRecord
  type: string
  enum: [SUCCESS, FAILED]

AttemptRecord:
  type: object
  required: [id, outboxId, aggregateId, aggregateType, attemptNumber, status,
             startedAt, finishedAt, createdAt]
  properties:
    id:             { type: integer, format: int64 }
    outboxId:       { type: integer, format: int64 }
    aggregateId:    { type: string }
    aggregateType:  { type: string }
    attemptNumber:  { type: integer }
    status:         { $ref: '#/components/schemas/AttemptStatus' }
    startedAt:      { type: string, format: date-time }
    finishedAt:     { type: string, format: date-time }
    latencyMs:      { type: integer, nullable: true }
    workerId:       { type: string,  nullable: true }
    topic:          { type: string,  nullable: true }
    partition:      { type: integer, nullable: true }
    offset:         { type: integer, format: int64, nullable: true }
    errorClass:     { type: string,  nullable: true }
    errorMessage:   { type: string,  nullable: true }
    errorDetail:    { type: string,  nullable: true }
    traceId:        { type: string,  nullable: true }
    correlationId:  { type: string,  nullable: true }
    createdAt:      { type: string, format: date-time }

AttemptPage:
  type: object
  required: [items]
  properties:
    items:      { type: array, items: { $ref: '#/components/schemas/AttemptRecord' } }
    nextCursor: { type: string, nullable: true }
```

Restoring these is an **additive** contract change (new endpoints, new schemas, a new problem
slug), so it lands within `/v1` rather than forcing a `/v2` — the same compatibility rule that
governs every Tandem contract.

---

## 9. Open decisions

| Area | Options |
|---|---|
| Write timing | **Synchronous in the status-update tx** (consistent, one extra INSERT — preferred) vs. asynchronous/batched writer (lower latency impact, weaker consistency, more complexity) |
| Retention default | Disabled / unbounded vs. a sane default window (e.g. 14 days) when the feature is enabled |
| Module placement | Adapter in `tandem-jdbc` (no new module — preferred, Pareto) vs. a dedicated `tandem-archive` module |
| Success rows | Record **every** attempt incl. the single successful one (full timeline) vs. only failures + final success (smaller archive) |
