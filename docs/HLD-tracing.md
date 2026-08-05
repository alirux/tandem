# Tandem — Trace & Correlation Propagation (Design Note)

**Version:** 1.0  
**Status:** Draft  
**Companion to:** HLD §7.1 (Trace & Correlation Propagation)

Propagate distributed-tracing and correlation identifiers across the asynchronous outbox
boundary — domain transaction → outbox → relay → Kafka → consumer — so a consumed event
can be traced back to the business operation that produced it. **Opt-in, off by default**;
when off it adds no cost and nothing to the headers.

Assumes the vocabulary (span, trace, `traceparent`, span link) covered in
[tracing-concepts.md](tracing-concepts.md).

---

## 1. Why the outbox makes this non-trivial

With a synchronous publish, the active trace context is present at send time and standard
instrumentation propagates it. The outbox breaks that: the event is **produced** inside
the domain transaction (where the trace context is live) but **physically sent** later, by
the relay, on a different thread where the original context is gone.

So propagation must be split into two steps:

1. **Capture** the trace/correlation context *at produce time* and store it with the event.
2. **Restore/propagate** it *at publish time* onto the Kafka message, so the consumer
   continues the same trace.

---

## 2. Three identifiers — keep them distinct

| Identifier | What it is | Wire form |
|---|---|---|
| **Trace context** | W3C Trace Context — distributed trace + span id; the OpenTelemetry standard | `traceparent` (+ optional `tracestate`) header |
| **Correlation id** | App-level id grouping related operations (a request, a saga) | configurable header, default `correlation-id` |
| **Causation id** | Points at the *specific causing event* (used for dependency-parking, HLD §9) | `causation_id` header |

`correlation_id` ≠ `causation_id`: correlation *groups* related work; causation *links an
effect to its one cause*. Both may coexist in the headers; this note covers the first two.

---

## 3. Mechanism — capture, store, propagate

```
[Domain transaction]                         active trace context
   │  OutboxRepository.insert(...)
   ▼
TracePropagator.capture()  ──▶ { traceparent, tracestate, correlation-id }
   │                                   │ merged into
   ▼                                   ▼
[outbox row]  headers JSONB  ◀─────────┘        (durable, committed with the event)
   │
   ▼  relay already copies headers → Kafka headers (HLD §6)
[Kafka message]  headers: traceparent, tracestate, correlation-id
   │
   ▼  standard W3C header → any OTel-instrumented consumer continues the trace
[Consumer span]  linked back to the producing business operation
```

The decisive simplification: **the relay already publishes the outbox `headers` as Kafka
headers**. So once the trace context is captured into `headers` at produce time, downstream
propagation is *already done* — no relay change needed for basic propagation. And because
the stored value is the opaque W3C `traceparent` string, **`tandem-core` and the relay never
need a tracing library** to carry it; they just move bytes.

---

## 4. Storage — reuse the existing `headers` column

No schema change. The captured context is merged into the outbox `headers` JSONB under
standard keys:

| Key | Source |
|---|---|
| `traceparent` | W3C trace context (from the tracing adapter) |
| `tracestate` | Optional W3C vendor state |
| `correlation-id` | Configurable; from MDC key (default) or an explicit API |

Standard key names mean **automatic interop**: any OpenTelemetry-instrumented Kafka
consumer continues the trace with zero Tandem-specific knowledge.

---

## 5. Port & adapters (Hexagonal, §1.2)

- **Port:** `TracePropagator` (in `tandem-core`): `Map<String,String> capture()` returns the
  context headers; default no-op.
- **Default adapter:** `NoOpTracePropagator` — captures nothing. Wired by default.
- **Capture chokepoint:** `JdbcOutboxRepository.insert(...)` calls `capture()` and merges the
  result into `headers` — so **all four usage tiers** (plain, template, annotation, Spring
  events) get propagation transparently, with no per-tier code.
- **Opt-in adapters:**
  - **Spring / Micrometer Tracing** (in `tandem-spring-producer`): auto-wired when Spring Boot's
    tracing is on the classpath — covers the Spring majority with no extra module. Bridges
    to whatever backend the app uses (OTel or Brave).
  - **OpenTelemetry** (optional `tandem-tracing-otel` module): for non-Spring users; uses
    the OTel `TextMapPropagator` to capture `Context.current()`.
- **Correlation id** needs no tracing library: read from an MDC key (default) or set via an
  explicit Tandem API.

---

## 6. Basic vs rich mode

- **Basic (default when enabled).** Capture context into `headers`; the relay propagates it
  as-is; consumers continue the trace via the standard `traceparent`. Minimal, standard,
  dependency-free downstream.
- **Rich (optional).** The relay additionally emits a short **`tandem.relay.publish`** span
  linked to the captured context, timestamped at the actual send — so the trace shows the
  outbox dwell + relay latency + retries as a real span. Requires a tracing adapter on the
  relay side; off unless asked for.

Four rules constrain rich mode. Each one, broken, produces traces that *look* plausible and
are wrong.

### 6.1 One span per record — a batch span has no correct parent

`tandem.relay.publish` is emitted **per outbox record**, parented to the context captured on
*that* record. It is never one span per claimed batch.

A batch is a **fan-in**: `batchSize` rows claimed together routinely come from as many
unrelated business transactions, each with its own captured `traceparent`. A single span
covering the batch would have to pick one of them as its parent — and every such pick is wrong
for every other record in the batch, silently attributing the relay's work to whichever trace
happened to be first while the others show no publish at all. There is no "the" parent of a
batch.

Per-record spans are also the natural shape for this relay: the worker overlaps `batchSize`
sends of *distinct* aggregates on one async dispatcher, so there is no single batch-wide
operation to time (LLD-jdbc §3.4).

If a batch-level span is ever wanted for engine diagnostics (claim + flush timings), it is a
**root** span carrying one **span link** per record — links being the standard way to relate
one span to *many* causally-related traces — never a parent-child edge to one member's trace.

### 6.2 Span scope must not rely on a thread-local

The relay dispatches asynchronously and does not await per record (LLD-jdbc §3.4). A
scope-based idiom — open a thread-local span scope, run, close it — is therefore **invalid
here**: the scope would close when the dispatch call returns, not when the send completes, so
any span opened downstream would attach to whatever record was being dispatched at the time.

The captured context must be carried explicitly alongside the in-flight record, and the span
ended in the completion callback that already handles the ack/failure. The adapter contract
therefore must not assume ambient context.

### 6.3 A span is emitted only when work happened — never per poll

The relay polls continuously and, on an idle outbox, the overwhelming majority of cycles claim
nothing (dispatch-latency.md §1). A span per poll cycle, per empty claim, or per housekeeping
tick would emit tens of spans per second per worker that describe *the absence of work*,
burying the traces an operator is actually looking for and inflating export cost for nothing.

So: **no span for a poll that claimed nothing, no span for a reclaim/cleanup tick that changed
nothing.** A span exists only where a record was published, retried, or failed. For Tandem's own
instrumentation the guard is not creating the span, never filtering it afterwards. A *host
framework's* automatic instrumentation — scheduler or connection-pool spans around the relay's
own loop — is the application's to exclude, and the documentation must say so.

### 6.4 Span attributes carry identifiers, never payloads

Attributes are limited to the structural identifiers already deemed safe to log — `aggregate_id`,
`aggregate_type`, row id, bucket, topic/partition, attempt number, status. **Never** the payload,
header *values*, `last_error` bodies, or bound SQL parameters. The rule and its reasoning are the
same as for logging ([HLD-logging.md](HLD-logging.md)); a span exported to a tracing backend is at
least as widely readable as a log line, so it gets no exemption.

---

## 7. Off by default, zero cost when off (Pareto, §1.1)

| Concern | When **off** (default) | When **on** |
|---|---|---|
| Headers | Nothing added | `traceparent` / `tracestate` / `correlation-id` merged in |
| Configuration | Nothing to set | A flag, or auto-detected from a tracing adapter on the classpath |
| Capture cost | **None** — guarded: `capture()` is not called and no map is built | One context capture + a header merge per insert |
| Dependencies | None — `NoOpTracePropagator` in core | Tracing adapter only where enabled |

The insert path **guards** the capture call so that when disabled there is no context lookup
and no allocation — only a boolean check.

---

## 8. Consumer side

Emitting the standard `traceparent` header means **any** OpenTelemetry-instrumented Kafka
consumer continues the trace automatically — no Tandem dependency required. For Tandem's own
consumer-side helpers (the Spring tier, the `CausalContext` of §9, the future inbox
reorderer), the same header can be read back to restore the context and the `correlation-id`
into MDC, so application logs on the consumer carry the originating ids.

---

## 9. Open decisions

**Sampling is frozen at insert time, for the life of the row.** The sampling decision travels in
the `traceparent`'s trace-flags, so capturing the header also captures the verdict. A row written
while its trace was sampled out stays sampled out — when the relay publishes it seconds later,
when a retry publishes it an hour later, and when a replay publishes it next year. That inheritance
is what keeps a trace coherent end to end, and it has a consequence: **the outbox dwell and relay
behaviour are observable in traces only for the sampled fraction**, so tracing cannot be the primary
evidence for "is the relay keeping up" — that is the metrics port's job, which samples nothing
(HLD §7). Open: whether the relay may override the inherited decision for rows that failed or aged
past a threshold, so the interesting minority is always traced.

**Replay must not resurrect the original trace as a live parent.** A replayed row still carries
the `traceparent` of the business transaction that produced it, possibly long expired: making the
republish a child of it would append spans to a trace whose root has aged out of the backend,
producing an orphan fragment dated in the present under an identifier from the past — and would
mix a deliberate operator action into the trace of an unrelated user request. The likely answer is
that replay opens a **new** trace with a **span link** to the captured context, keeping the original
ids as attributes so the connection stays queryable. The choice also governs what the Admin API's
replay endpoint reports back.

| Area | Options |
|---|---|
| Enablement | Explicit flag (`tandem.tracing.enabled`) vs. auto-enable when a tracing adapter is detected on the classpath | 
| Correlation-id source | MDC key (default, e.g. `correlationId`) vs. explicit `TandemContext` API vs. both |
| Relay publish span | Off by default (basic mode) vs. on (rich mode) when a relay-side tracing adapter is present |
| OTel adapter module | Dedicated `tandem-tracing-otel` (preferred for non-Spring) vs. fold capture into existing modules |
| Sampling | Inherit the captured decision unconditionally vs. let the relay force-sample failed/aged rows |
| Replay semantics | New trace + span link to the original (preferred) vs. reuse the captured context as parent vs. no trace at all |
| Verification | Assert span structure in tests only vs. also a runnable demo through a real tracing backend, the way the metrics are demonstrated (LLD-benchmark.md §6.3) |
