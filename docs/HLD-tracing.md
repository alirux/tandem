# Tandem — Trace & Correlation Propagation (Design Note)

**Version:** 1.3  
**Status:** Implemented — propagation and instrumented mode ship for Spring applications
(`tandem-spring-*`) and for applications instrumenting themselves with the OpenTelemetry SDK
directly (`tandem-tracing-otel`). Only the Tempo + Grafana demo (§9's verification item) remains.  
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
propagation is *already done* — no relay change needed for propagation mode. And because
the stored value is the opaque W3C `traceparent` string, **`tandem-core` and the relay never
need a tracing library** to carry it; they just move bytes.

---

## 4. Storage — the `headers` column, plus one indexed column for the correlation id

The captured context is merged into the outbox `headers` JSONB under standard keys:

| Key | Source |
|---|---|
| `traceparent` | W3C trace context (from the tracing adapter) |
| `tracestate` | Optional W3C vendor state |
| `correlation-id` | Configurable; from MDC key (default) or an explicit API |

Standard key names mean **automatic interop**: any OpenTelemetry-instrumented Kafka
consumer continues the trace with zero Tandem-specific knowledge. **`headers` remains the
source of truth for what reaches Kafka** — the relay copies it verbatim (§3), so nothing
downstream depends on anything else.

### 4.1 Why the correlation id also gets its own column

`tandem_outbox.correlation_id` (`VARCHAR(255)`, nullable, indexed) is written at the same
chokepoint, copied from whatever ends up in `headers['correlation-id']` — so the two can
never disagree. It is **not** a second source of truth; it exists to make one specific
question answerable:

- **The correlation id is what an operator actually has during an incident.** It arrives
  from *outside* the application — an inbound request header, a consumed message — and is
  what appears in a log line, an alert, or a customer ticket. The aggregate id, which the
  existing filters are built around, is typically *not* known when an investigation starts.
- **The Admin API is the only sanctioned way to reach outbox state in production**
  (HLD-admin-api §1: production database access is normally forbidden, and replay/discard
  need authorization and an audit trail). So "search by correlation id" has to be an API
  capability, or it does not exist at all.
- **A capture nobody can query does not keep this note's promise.** §1 justifies the whole
  feature by the outbox breaking the causal link between write and publish; restoring the
  link but leaving it unsearchable during an incident restores it only in principle.

Why a column rather than an index over the JSONB:

- **Portability.** A B-tree on a real column ports to MySQL 8 unchanged. An expression index
  over `headers->>'correlation-id'` does not: MySQL has neither expression nor partial
  indexes and would need a generated-column workaround (LLD-jdbc §5) — i.e. the column,
  arrived at by a longer route and only on one engine.
- **The list view never reads `headers`.** `OutboxRowView` deliberately does not select the
  JSONB columns (HLD-admin-api §4); a searchable copy in its own column is what lets a search
  result *show* the correlation id without reading the payload/headers of every row.
- **Bounded, because the value is untrusted.** Coming from outside, it is truncated to the
  column width at insert rather than being allowed to fail the caller's business transaction
  or widen the index without limit. The header copy keeps the full value — it is not indexed.

The relationship is **one correlation id to many rows** (§2: correlation *groups* related
work), typically spanning several aggregates and possibly several services sharing the outbox.
So the search returns a page like any other, is normally combined with `status`, and its
results carry **no ordering guarantee relative to each other** — Tandem orders per aggregate,
and these rows generally belong to different ones.

Additive and compatible both ways (HLD §1.4): the column is nullable, an older writer simply
leaves it `NULL`, and an older reader never selects it.

---

## 5. Port & adapters (Hexagonal, §1.2)

- **Port:** `TracePropagator` (in `tandem-core`): `Map<String,String> capture()` returns the
  context headers; default no-op.
- **Default adapter:** `TracePropagator.NOOP` — captures nothing. Wired by default.
- **Capture chokepoint:** `JdbcOutboxRepository.insert(...)` calls `capture()` and merges the
  result into `headers` — so **all four usage tiers** (plain, template, annotation, Spring
  events) get propagation transparently, with no per-tier code. **Implemented.**
- **Opt-in capture adapters:**
  - **MDC correlation id** (`MdcCorrelationTracePropagator`, in `tandem-spring-producer`):
    reads a configured MDC key, falling back to the explicit `TandemContext` API — no
    tracing library needed. Gated by `tandem.tracing.enabled` (explicit only, §9).
    **Implemented.**
  - **Spring / Micrometer Tracing** (`MicrometerTracePropagator`, in `tandem-spring-producer`):
    the real distributed-trace-context bridge, covering the Spring majority with no extra module.
    Gated by `tandem.tracing.enabled`, and merged with the correlation-id adapter above via
    `TracePropagator.composite(...)` — the two identifiers come from independent sources (§2), so
    an application may have either without the other. **Implemented.**
    Capture delegates to the application's own `Propagator` bean rather than formatting a
    `traceparent` directly, so whatever propagation format the application configured is what lands
    in the row — W3C under Spring Boot's default, `b3` under a B3 configuration. Writing W3C
    unconditionally would produce a header the application's own consumers do not read. A
    B3-configured application therefore gets propagation but no relay publish span, which reads
    `traceparent` to find its parent.
  - **OpenTelemetry** (`OtelTracePropagator`, in the optional `tandem-tracing-otel` module):
    for applications that instrument themselves with the OpenTelemetry SDK directly rather than
    through Spring. Capture works exactly like the Micrometer adapter above — delegates to the
    application's own `OpenTelemetry` instance's configured `TextMapPropagator` rather than
    formatting a `traceparent` directly, so a B3- or baggage-configured application still gets
    the format its own consumers read, and returns no headers when no span is current. Only the
    OTel *API* is redistributed; the SDK stays the application's own dependency. **Implemented.**
  - **Correlation id without any tracing library** (`TracePropagator.fromTandemContext()`, in
    `tandem-core`): reads whatever the caller set through `TandemContext`, so an application
    outside Spring gets the incident-time identifier of §4.1 without adding a tracing library,
    a logging binding, or any dependency at all to its write side (§1.3). The Spring equivalent,
    `MdcCorrelationTracePropagator`, additionally reads a configured MDC key and falls back to
    `TandemContext` — that MDC integration needs SLF4J, so it stays in `tandem-spring-producer`
    rather than moving to core. **Implemented.**
- **Relay-side span-emission port:** `TandemSpanRecorder` (in `tandem-core`): `startPublishSpan(...)`
  returns a `Span` handle ended from the dispatch completion callback (§6.2), never via a
  thread-local scope. Default `TandemSpanRecorder.NOOP`. Deliberately explicit parameters
  (row id, aggregate type/id, attempts, topic, `traceparent`/`tracestate`, `correlation-id`), not a
  generic header map, so an adapter is structurally unable to attach a payload or an arbitrary
  header value to a span (§6.4). Wired into `KafkaRelay.dispatch(...)` (start after a successful
  encode, end in the producer's send callback).
  - **Spring / Micrometer Tracing** (`MicrometerTandemSpanRecorder`, in `tandem-spring-relay`):
    resolves the parent by handing the row's `traceparent`/`tracestate` to the application's own
    `Propagator`, tags the span with the identifiers of §6.4 under uniform `tandem.*` names, and
    ends it — success or failure — from the dispatch completion callback. Gated by its own explicit
    `tandem.tracing.publish-span` flag, separate from the write side's `tandem.tracing.enabled`
    because under the split topology the two run in different processes and the costs differ (a
    header on the row versus export volume in the backend). **Implemented.**
    **A row carrying no `traceparent` gets no span**, rather than a root span: the relay would
    otherwise become the root of a new trace holding one publish and nothing about the business
    operation behind it — the same orphan outcome §9 rejected when it ruled out force-sampling.
    Instrumented mode therefore builds on propagation mode.
  - **OpenTelemetry** (`OtelTandemSpanRecorder`, in the optional `tandem-tracing-otel` module):
    the same span, built through the application's own `Tracer` and parented via its own
    `TextMapPropagator`, for a relay running outside Spring. A row whose `traceparent` the
    configured propagator cannot extract — absent, or in a format that propagator does not
    read — gets no span, for the same orphan-trace reason. **Implemented.**
- **Search side:** the captured `correlation-id` is also stored in its own indexed column and
  exposed as an Admin API search filter — §4.1, the incident-time lookup.
- **Correlation id** needs no tracing library: read from an MDC key (default) or set via an
  explicit Tandem API.

---

## 6. Propagation vs instrumented mode

- **Propagation (default when enabled).** Capture context into `headers`; the relay propagates
  it as-is; consumers continue the trace via the standard `traceparent`. Minimal, standard,
  dependency-free downstream.
- **Instrumented (optional).** The relay additionally emits a short **`tandem.relay.publish`**
  span linked to the captured context, timestamped at the actual send — so the trace shows the
  outbox dwell + relay latency + retries as a real span. Requires a tracing adapter on the
  relay side; off unless asked for.

Four rules constrain instrumented mode. Each one, broken, produces traces that *look* plausible
and are wrong.

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
`aggregate_type`, row id, bucket, topic/partition, attempt number, status, and the
**`correlation-id`**. **Never** the payload, other header *values*, `last_error` bodies, or bound
SQL parameters. The rule and its reasoning are the same as for logging
([HLD-logging.md](HLD-logging.md)); a span exported to a tracing backend is at least as widely
readable as a log line, so it gets no exemption.

The `correlation-id` is on that list for the same reason logging already allows it — it is an
opaque identifier, not business data — and it earns its place by being the **join key between the
two tools**: an investigation that starts in the tracing backend (a slow or failed publish span)
crosses to the Admin API's search by that same id, and one that starts from a log line or a ticket
crosses the other way. Without it the two views can only be joined through the aggregate id, which
the incident may not yet know (§4.1).

Note the asymmetry this does *not* remove: a span reaches the tracing backend only in instrumented
mode and only for the **sampled** fraction (§9), whereas the `correlation_id` column is written for
**every** row whenever propagation is on. The tracing backend is therefore a complement to the
Admin API search, never a replacement for it.

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

## 9. Decisions

**Sampling is frozen at insert time, for the life of the row — no override.** The sampling
decision travels in the `traceparent`'s trace-flags, so capturing the header also captures the
verdict. A row written while its trace was sampled out stays sampled out — when the relay
publishes it seconds later, when a retry publishes it an hour later, and when a replay publishes
it next year. That inheritance is what keeps a trace coherent end to end, and it has a
consequence: **the outbox dwell and relay behaviour are observable in traces only for the
sampled fraction**, so tracing cannot be the primary evidence for "is the relay keeping up" —
that is the metrics port's job, which samples nothing (HLD §7). The relay never force-samples
failed or aged rows: if the domain-side span was never exported under a head-sampling decision,
flipping the trace-flag later does not resurrect it — it only produces an orphan relay span with
no root in the backend, the same failure mode the replay decision below rules out. Visibility
into failed/aged rows stays the metrics port's job.

**Replay opens a new trace with a span link to the original context — never a live parent.** A
replayed row still carries the `traceparent` of the business transaction that produced it,
possibly long expired: making the republish a child of it would append spans to a trace whose
root has aged out of the backend, producing an orphan fragment dated in the present under an
identifier from the past — and would mix a deliberate operator action into the trace of an
unrelated user request. Replay instead starts a **new** trace with a **span link** to the
captured context, keeping the original ids as attributes so the connection stays queryable. The
Admin API's replay endpoint reports the new trace id, with the original kept as an attribute.

| Area | Decision |
|---|---|
| Enablement | Explicit flag (`tandem.tracing.enabled`, default `false`) — never auto-enabled from a tracing adapter's mere presence on the classpath. A dependency pulled in transitively for an unrelated reason must not silently start capturing context and adding headers; opt-in means an explicit action. |
| Correlation-id source | Both: an MDC key (default, e.g. `correlationId`) for zero-code propagation where one is already populated, and an explicit `TandemContext` API for call sites with no active MDC (batch jobs, Kafka listeners). |
| Relay publish span | Off by default (propagation mode); instrumented mode requires its own explicit flag, for the same reason as enablement — the mere presence of a relay-side tracing adapter must not auto-enable span emission. |
| OTel adapter module | Dedicated `tandem-tracing-otel`, for applications instrumenting themselves with the OpenTelemetry SDK directly — same pattern as `tandem-micrometer`/`tandem-kafka`: an optional dependency stays isolated in its own module, never folded into one a client already depends on. **Implemented** (§5): `OtelTracePropagator` + `OtelTandemSpanRecorder`. |
| Sampling | Inherited unconditionally; no relay override (see above). |
| Replay semantics | New trace + span link to the original (see above). |
| Verification | Both: assert span structure in tests, and a runnable demo through a real tracing backend (Tempo + Grafana), mirroring `metricsDashboardDemo` (LLD-benchmark.md §6.3, backlog item 5). |
