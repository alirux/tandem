# Distributed Tracing — Concepts

**Version:** 1.0  
**Status:** Reference  
**Companion to:** [HLD-tracing.md](HLD-tracing.md)

A primer on the vocabulary distributed tracing uses — span, trace, the `traceparent` wire
format, and how `correlation-id`/`causation-id` relate to it. General background, not a
Tandem design decision; HLD-tracing.md assumes these terms and builds Tandem's specific
propagation design on top of them.

---

## 1. Span — the unit of work

A **span** represents one unit of work with a start and an end: an HTTP handler serving a
request, a SQL query running, a message being published. Concretely, a span carries:

- a **name** (`outbox.batch.publish`, `HTTP POST /orders`)
- a **start timestamp** and a **duration**
- a **status** (ok / error)
- **attributes** — key/value pairs (`aggregate_id=42`, `http.status_code=200`)
- optionally **events** (point-in-time log entries scoped to the span) and **links**
  (references to other spans outside the parent/child chain — §6)

## 2. Trace — a tree of spans

A **trace** is the set of spans describing one end-to-end operation, arranged as a **tree**:
every span except the root has a **parent**. An HTTP handler that queries a database and then
publishes to Kafka produces a root span (`HTTP POST /orders`) with two children (`SELECT`,
`kafka.send`); a consumer on the other end may open a further span as a child of `kafka.send`.

The whole trace shares one **`trace_id`** (a 128-bit id generated when the root span is
created). Each span has its own **`span_id`** (64 bits) and records its parent's `span_id`:

```
trace_id: 4bf92f3577b34da6a3ce929d0e0e4736     ← shared by every span in the trace
  span_id: 00f067aa0ba902b7   parent: (none)        "HTTP POST /orders"
    span_id: a1b2c3...        parent: 00f067aa...   "SELECT outbox batch"
    span_id: d4e5f6...        parent: 00f067aa...   "kafka.send"
      span_id: 9988aa...      parent: d4e5f6...     (in the consumer) "process order.placed"
```

A tracing backend (Tempo, Jaeger, Zipkin, …), given the `trace_id`, reconstructs the tree and
renders it as the familiar waterfall view.

## 3. `traceparent` — carrying a span across the wire

`trace_id` and `span_id` live in memory in the process that created them. Crossing a network
boundary — an HTTP call, a Kafka message — requires serializing them so the next process can
treat the sender's span as its own parent.

The W3C standardized this as the **`traceparent`** header:

```
00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
│  │                                │                │
│  trace_id (32 hex = 128 bit)      parent_span_id   trace-flags
│                                   (16 hex = 64 bit) (2 hex = 8 bit)
version (2 hex)
```

- **version** — currently always `00`.
- **trace_id** — the identifier shared by every span in the trace.
- **parent_span_id** — the `span_id` of whoever emitted this header; for the receiver, "my
  parent is this."
- **trace-flags** — an 8-bit mask; the low bit is `sampled` (`01` = this trace is/will be
  exported to the backend; `00` = not sampled).

On receipt, a service extracts `trace_id` and `parent_span_id`, generates its own `span_id`,
opens a span as a child of that parent, and — if it calls onward — injects a new `traceparent`
carrying the same `trace_id` with its **own** `span_id` as the new parent. It is a relay race:
each hop receives a baton, creates a child, passes a new baton.

A second, optional header, `tracestate`, carries vendor-specific state and does not affect the
propagation mechanism described here.

## 4. Why asynchronous boundaries break the relay

The relay race assumes the next hop happens **while the context is still live** — the calling
thread is still inside the span when it makes the next call, so there is something to read the
`traceparent` from. Any boundary that decouples *producing* work from *acting on it* breaks
that assumption: a message queue, a scheduled batch job, a retried background task, or a
transactional outbox all hand work to a different thread — possibly a different process,
possibly minutes or hours later — with no live span to inherit from.

```
[producing thread]  span "HTTP POST /orders" is open
                     ... work recorded for later processing ...
                     span ends, thread returns              ← the live context dies here

... later, a different thread, possibly a different process ...

[consuming thread]  no context is active to derive a traceparent from
```

If the later side opens a brand-new root span, the backend shows two disconnected trees for
what was really one operation. The standard remedy is to **capture** the `traceparent` string
at the point where the context is still live and **persist** it alongside the deferred work, so
whoever eventually acts on it can extract that same string and open a child span under it. The
string is inert data — no live object, no thread affinity — so this works across threads,
processes, and arbitrary elapsed time.

## 5. `correlation-id` and `causation-id` — a different layer

`trace_id`/`span_id` belong to the **observability** layer — a tracing backend understands
them natively. `correlation-id` and `causation-id` belong to the **application** layer: no
standard defines them, and the header name is a project convention, not a protocol.

**`correlation-id`** groups operations that belong to the same unit of business work: every
event produced while handling one request, or every step of one saga. It is typically constant
for the life of that request, even across multiple *technical* traces — an HTTP request is one
trace, and a retry three days later that reprocesses the same request is a different trace, but
both can carry the same `correlation-id`.

**`causation-id`** is narrower: it points at the *specific event that caused this one*. If
event A produces event B which produces event C, C's `causation_id` is B's id, not A's. It
supports reconstructing precise causal chains, one hop at a time.

| | Answers | Scope | Standardized? |
|---|---|---|---|
| `trace_id` / `span_id` | Which span tree, and where in it | One technical trace | Yes — W3C |
| `correlation-id` | Which operations belong to the same business unit of work | May span several traces over time | No — application convention |
| `causation-id` | Who specifically caused this | One causal hop (event → event) | No — application convention |

These can coexist without conflict on the same message: a `traceparent` (for the tracing
backend), a `correlation-id` (for grouping in logs and business queries), and a `causation-id`
(for the causal chain) are three independent headers answering three different questions.

## 6. Span links — for fan-in and fan-out

A parent/child edge is a 1-to-1, temporally-nested relationship: conceptually, the child exists
within the parent's lifetime. Some operations don't fit that shape — a single unit of work that
draws on **several** unrelated traces (fan-in: a batch built from many independent producers)
or produces several independent ones (fan-out). For these, tracing uses a **link**: a
lightweight reference to another `(trace_id, span_id)` pair without becoming its child. A link
records "these are causally related" without claiming "this is nested inside that."
