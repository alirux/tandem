# Tandem — `tandem-kafka` LLD

**Version:** 0.1 (Draft)  
**Module:** `tandem-kafka` · package `com.codingful.tandem.kafka`  
**Depends on:** `tandem-core`, `org.apache.kafka:kafka-clients`, `io.cloudevents:cloudevents-kafka`
(+ `cloudevents-core`). **Relay-side only** — never on the client write-side (§3.2/§1.3).  
**Resolves:** Q17 (producer failure semantics), Q18 (TopicRouter default), Q19 (CloudEvents
binding), Q20 (null `type`). See [open-questions-lld.md](open-questions-lld.md).

`tandem-kafka` is the publish adapter: it implements `OutboxDispatcher` (build a CloudEvent from
an `OutboxRecord`, send to Kafka, complete a future on the ack) and the default `TopicRouter`. The
relay engine (`tandem-jdbc`) calls it asynchronously and overlaps `batch_size` records of distinct
aggregates in flight; per-aggregate order is structural (one head per aggregate, HLD §6, Q9/Q10).

---

## 1. Producer configuration (the mandated safe config — §4.4/§4.5)

Tandem sets these defaults and **fails fast** (`TandemConfigurationException`) if the user overrides
them to unsafe values — they protect the no-loss + ordering guarantees:

| Property | Value | Why |
|---|---|---|
| `enable.idempotence` | `true` | no duplicate/reordered batches from the producer's own retries (§4.4) |
| `acks` | `all` | the row is marked DONE only after a durable ack (§4.5); `0`/`1` risk loss → rejected |
| `max.in.flight.requests.per.connection` | ≤ 5 | required for idempotent ordering; >5 rejected |
| `retries` | high (default `Integer.MAX_VALUE`) | transient errors are retried by the producer, bounded by ↓ |
| `delivery.timeout.ms` | **30s** (default) | caps the producer's retry window before a send fails; **must stay below `rowLease`** so a row's lease cannot expire mid-send — relay fail-fasts if `rowLease ≤ delivery.timeout.ms` (LLD-jdbc §3.5) |

Everything else (bootstrap servers, security, compression, batching) is user-supplied and passed
through. Within an aggregate the relay keeps **only the head row in flight** and never sends the
next before its ack (§6/E6); **across the distinct aggregates of a claimed batch it overlaps sends
on the single async producer** — up to `batch_size` records in flight, the per-shard concurrency
window. That overlap (not one-at-a-time publishing) is where per-shard throughput comes from
(LLD-jdbc §3.4, HLD §10).

---

## 2. `OutboxDispatcher` implementation (`KafkaRelay`)

```java
CompletableFuture<Void> dispatch(OutboxRecord record) {
    ProducerRecord<…> pr = encode(record);          // §3: CloudEvent → ProducerRecord
    CompletableFuture<Void> ack = new CompletableFuture<>();
    producer.send(pr, (md, ex) -> {                 // async; callback on the producer I/O thread
        if (ex == null) ack.complete(null);         // broker ack (acks=all)
        else            ack.completeExceptionally(classify(ex));   // §4: retriable vs permanent
    });
    return ack;
}
```
- **Asynchronous send** (`send` + callback → future) — does not block a thread on the ack, so the
  relay overlaps `batch_size` records of distinct aggregates on one producer (the per-shard
  concurrency window, §1). The mark-DONE-after-ack flow (Q9) is preserved: the relay marks DONE in
  the future's completion handler.
- The future completes **exceptionally** with `OutboxDispatchException`, which **carries the
  retriable/permanent verdict** so `tandem-jdbc` routes it to `markForRetry` (backoff) or
  `markFailed` (§4) and stops that aggregate (Q10).
- **No in-aggregate reordering to guard here:** the relay only ever dispatches one row per aggregate
  (its head, LLD-jdbc §3.3), so the producer's per-partition ordering + idempotence are sufficient.

---

## 3. CloudEvents binding (Q19)

Built with the **CloudEvents Java SDK** (`io.cloudevents:cloudevents-kafka`). Per record:

```java
var b = CloudEventBuilder.v1()
    .withId(String.valueOf(record.id()))            // id source: outbox id (§6)
    .withSource(source(record))                     // single configured URI: tandem.kafka.source (§6)
    .withType(typeOf(record))                       // §3.3 — null fallback (Q20); version lives here (`.v{n}`)
    .withSubject(record.aggregateId().value())      // = aggregate_id
    .withTime(record.createdAt())
    .withDataContentType(contentType(record))       // §3.2
    .withDataSchema(dataSchema(record))             // §3.2 — optional, from stored header / config
    .withData(record.payload())                     // raw bytes
    .withExtension("seq", record.seq())             // always present
    .withExtension("partitionkey", record.aggregateId().value());  // always = key
// optional extensions — added ONLY when their feature is on (guard against null):
if (record.lamport() != null)                       // causal ordering enabled
    b.withExtension("logicalclock", record.lamport());  // → header ce_logicalclock
// trace extensions (traceparent/tracestate) copied from stored headers when present (§7.2)
CloudEvent ce = b.build();
ProducerRecord<…> pr = KafkaMessageFactory.createWriter(topic, key=aggregateId)
    .writeBinary(ce);                               // binary mode (default); writeStructured for structured
```

### 3.1 Content modes
- **Binary (default):** attributes → `ce_*` Kafka headers, `data` → body, `content-type` header =
  `datacontenttype`. Tandem extensions become `ce_seq` / `ce_logicalclock` / `ce_partitionkey`.
- **Structured (opt):** the whole CloudEvent (JSON) → body, `content-type:
  application/cloudevents+json`.
- **Raw (escape hatch):** no envelope — body = payload, key = `aggregate_id`, stored `headers` passed
  through as Kafka headers.

### 3.2 `datacontenttype` & `dataschema` sources
- **`datacontenttype`** = `headers["content-type"]` if the producing side stored one, else the
  configured default (`tandem.kafka.default-content-type`, default `application/json`).
- **`dataschema`** (optional) = `headers["dataschema"]` if present, else config; omitted when neither
  is set. For schema-registry / Avro-Protobuf users (HLD-cloudevents §7). Reuses `headers` (no new
  column — Pareto).

Event **versioning** lives in the `type` (`.v{n}` suffix), not in the topic — see HLD-cloudevents §7.

### 3.3 Header combination
The Kafka record headers in binary mode = the `ce_*` attribute headers + `content-type` + the stored
`headers` (e.g. `correlation-id`). Trace headers (`traceparent`/`tracestate`) from the stored
`headers` are mapped to the CloudEvents Distributed-Tracing extension. The causal-ordering value is
carried as the CloudEvents extension **`logicalclock`** → header **`ce_logicalclock`** in binary mode;
the consumer-side adapters (`tandem-kafka-streams` / `tandem-flink`) read `ce_logicalclock`. (The design
docs keep the technical term *lamport* for the concept; `logicalclock` is only the on-the-wire name.)

### 3.4 Null `type` fallback (Q20)
CloudEvents `type` is required but the `type` column is nullable. **`typeOf(record)` = `record.type()`
if present, else falls back to `aggregate_type`** (configurable fallback). Raw mode does not need a
`type`. This guarantees a valid required attribute without forcing the user to set `type`.

---

## 4. Error classification (Q17)

`dispatch` classifies the cause into the `OutboxDispatchException` so `tandem-jdbc` can act:

- **Transient → retriable** (`markForRetry` with backoff, → FAILED at max attempts): Kafka
  `RetriableException` subclasses surfacing after the producer's internal retries exhausted
  (timeouts, `NotEnoughReplicas`, leadership changes), and unknown errors by default.
- **Permanent → fail-fast** (`markFailed` immediately, no wasted retries): data/config errors that
  will never succeed — `RecordTooLargeException`, `SerializationException`, authorization/security
  exceptions, `InvalidTopicException`, `UnsupportedVersionException`.

The classifier is a pluggable `ErrorClassifier` SPI; the above is the default mapping.

---

## 5. `TopicRouter` default (Q18)

**Source field = `aggregate_type`** (not the CloudEvents `type`, which is the finer event type and
does not dictate the topic). Default rule: **`kebab-case(aggregate_type)` + suffix**, suffix
configurable (`tandem.kafka.topic-suffix`, default `-topic`), no pluralization:

| `aggregate_type` | topic |
|---|---|
| `Order` | `order-topic` |
| `OrderLine` | `order-line-topic` |

Override via a custom `TopicRouter` bean, or a static `aggregateType → topic` map in config.
(HLD §5.2/§12 examples corrected to `order-topic`; no pluralization.)

---

## 6. Sub-decisions — defaulted for the basic round

- **`id` source** → the **outbox `id`** (globally unique, supports consumer dedup). Settled.
- **`source` convention** → a **single configured URI** (`tandem.kafka.source`, e.g. `/tandem/orders`).
  Per-`aggregate_type` derivation can be added later without breaking consumers (additive).
- **Trace extension header naming** → **deferred**: it only applies when tracing is enabled (off in the
  basic round). Decide bare `traceparent` vs `ce_traceparent` when `tandem-tracing-otel` lands (§7.2).
