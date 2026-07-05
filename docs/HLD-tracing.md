# Tandem — Trace & Correlation Propagation (Design Note)

**Version:** 1.0  
**Status:** Draft  
**Companion to:** HLD §7.2 (Trace & Correlation Propagation)

Propagate distributed-tracing and correlation identifiers across the asynchronous outbox
boundary — domain transaction → outbox → relay → Kafka → consumer — so a consumed event
can be traced back to the business operation that produced it. **Opt-in, off by default**;
when off it adds no cost and nothing to the headers.

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
  - **Spring / Micrometer Tracing** (in `tandem-spring`): auto-wired when Spring Boot's
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
  outbox dwell + relay latency + retries as a real span, and supplies the `trace_id` to the
  attempt archive (§7.1). Requires a tracing adapter on the relay side; off unless asked
  for.

---

## 7. Off by default, zero cost when off (Pareto, §1.1)

| Concern | When **off** (default) | When **on** |
|---|---|---|
| Headers | Nothing added | `traceparent` / `tracestate` / `correlation-id` merged in |
| Configuration | Nothing to set | A flag, or auto-detected from a tracing adapter on the classpath |
| Capture cost | **None** — guarded: `capture()` is not called and no map is built | One context capture + a header merge per insert |
| Dependencies | None — `NoOpTracePropagator` in core | Tracing adapter only where enabled |

As with the attempt archive, the insert path **guards** the capture call so that when
disabled there is no context lookup and no allocation — only a boolean check.

---

## 8. Consumer side

Emitting the standard `traceparent` header means **any** OpenTelemetry-instrumented Kafka
consumer continues the trace automatically — no Tandem dependency required. For Tandem's own
consumer-side helpers (the Spring tier, the `CausalContext` of §9, the future inbox
reorderer), the same header can be read back to restore the context and the `correlation-id`
into MDC, so application logs on the consumer carry the originating ids.

---

## 9. Relation to the attempt archive (§7.1)

The attempt archive is the primary in-library *consumer* of these ids: its `trace_id` and
`correlation_id` columns are populated from the captured headers. The two features are
designed together — propagation puts the ids in `headers`; the archive reads them out for
forensics. Each works without the other (the archive simply leaves the columns null if
propagation is off).

---

## 10. Open decisions

| Area | Options |
|---|---|
| Enablement | Explicit flag (`tandem.tracing.enabled`) vs. auto-enable when a tracing adapter is detected on the classpath | 
| Correlation-id source | MDC key (default, e.g. `correlationId`) vs. explicit `TandemContext` API vs. both |
| Relay publish span | Off by default (basic mode) vs. on (rich mode) when a relay-side tracing adapter is present |
| OTel adapter module | Dedicated `tandem-tracing-otel` (preferred for non-Spring) vs. fold capture into existing modules |
