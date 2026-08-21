# Tandem — Comparison with Alternatives

**Version:** 1.1  
**Status:** Draft  
**Companion to:** HLD (High-Level Design)

> **Disclaimer:** This comparison was generated with an automated tool and has only had a
> light editorial pass. It should be treated as a starting point for a proper evaluation,
> not as a reliable reference. If you spot inaccuracies or know of tools that should be
> included, feedback is very welcome — feel free to open an issue.

This note compares Tandem with the established approaches to reliable event delivery
from a relational database to Apache Kafka. It expands the summary in HLD §11 with
per-alternative analysis, the trade-offs behind each row, and guidance on when *not* to
choose Tandem.

The contenders — mechanisms that get an event from a database transaction onto Kafka:

- **Debezium** — log-based Change Data Capture (CDC) via Kafka Connect.
- **Eventuate Tram** — a transactional-messaging library with its own CDC/polling relay.
- **Spring Modulith** — application-event publication with an event-publication registry.
- **Plain outbox (hand-rolled)** — a bespoke outbox table + polling loop.
- **Tandem** — this project.

And two **stream processors** that this note also names, in a different category — they
consume from Kafka rather than deliver to it, so they are compared separately in §6:

- **Kafka Streams** — an embedded stream-processing library.
- **Flink** — a distributed stream-processing cluster.

---

## 1. Summary matrix

| | Debezium | Eventuate Tram | Spring Modulith | Hand-rolled outbox | **Tandem** |
|---|---|---|---|---|---|
| Write-side ordering | App code | App code | App code | App code | App code **+ in-library contract** (`seq`, `UNIQUE`) |
| Relay ordering | Strong (CDC log) | Good | Weak | DIY | **Strong (hash routing)** |
| Monitoring | Connector metrics | Minimal | Spring Actuator | DIY | **Micrometer native** |
| Targeted replay (per aggregate) | No (offset rewind) | Manual | No | DIY | **Yes, first-class** |
| Operational admin API | Connect REST (connector-level) | No | No | DIY | **Opt-in REST (API-first)** |
| Trace / correlation propagation | Headers (CDC) | Partial | No | DIY | **Opt-in (W3C `traceparent`)** |
| Message envelope | CloudEvents (converter) | Custom | Spring event | DIY | **CloudEvents by default** |
| Extra infrastructure | Kafka Connect + CDC | CDC/polling service | None | None | **None** |
| DB coupling | Per-connector (WAL/binlog) | Supported DBs | JPA/relational | Yours | **PostgreSQL** |
| Spring required | No | Optional | **Yes** | No | Optional |
| Ordering granularity | Per-table/partition | Per-aggregate | Weak | DIY | **Per-aggregate** |
| Operational burden | High | Medium | Low | Medium | **Low** |
| License | Apache 2.0 | Apache 2.0 | Apache 2.0 | — | Apache 2.0 |

The rest of this note explains each column. Kafka Streams and Flink are deliberately
**not** columns here — this matrix scores database-to-Kafka delivery, which is not what a
stream processor does; §6 compares them on their own axes.

---

## 2. Debezium (log-based CDC)

**What it is.** Debezium tails the database transaction log (PostgreSQL WAL, MySQL
binlog) through Kafka Connect and emits a change event per row mutation. Combined with
the outbox pattern (the "Outbox Event Router" SMT), it can publish domain events rather
than raw row diffs.

**Strengths.**
- **Strongest relay ordering.** The transaction log is the database's own total order,
  so Debezium never has to reconstruct ordering — it reads it.
- **No polling.** Log tailing imposes negligible load on the database compared to a
  polling relay.
- **Mature, broad DB support.** Battle-tested across Postgres, MySQL, SQL Server, Oracle,
  MongoDB, and more.

**Costs.**
- **Heavy infrastructure.** Requires running and operating **Kafka Connect** plus the
  Debezium connector — a separate distributed system with its own scaling, failure, and
  upgrade story. This is the single biggest reason teams avoid it for a "just publish my
  events" need.
- **Log access privileges.** Needs replication-slot / binlog permissions, which many
  managed-database and security postures restrict.
- **Replay is offset-based, not aggregate-targeted.** Re-emitting events for a *single*
  aggregate means rewinding a connector offset and re-reading a window, then filtering
  downstream — there is no native "replay aggregate X" operation.
- **Per-aggregate ordering is not intrinsic.** The log gives a global order; mapping that
  onto per-aggregate Kafka partitions still requires the message key to be set correctly
  (write-side concern).

**Choose Debezium when:** you already operate Kafka Connect, want minimal DB load at high
volume, and need CDC for purposes beyond domain events (e.g. data replication, analytics
sink).

---

## 3. Eventuate Tram

**What it is.** A library by Chris Richardson providing transactional messaging and a
transactional outbox, with a relay (CDC or polling) that forwards messages to a broker.
The direct conceptual ancestor of Tandem.

**Strengths.**
- **Per-aggregate ordering** is a first-class concept.
- **Library-level integration** — no separate CDC product mandatory (it offers both
  polling and CDC relays).
- **Saga support** — Eventuate's broader ecosystem (Eventuate Tram Sagas) is a mature
  orchestration story Tandem does not attempt.

**Costs.**
- **Relay is a separate service** in the CDC configuration, reintroducing operational
  weight similar to Debezium.
- **Monitoring is modest** — limited out-of-the-box operational metrics; lag/age
  visibility must be assembled.
- **Replay must be built** — no first-class per-aggregate replay API.
- **Heavier dependency surface** and framework conventions than a minimal-dependency
  library.

**Choose Eventuate Tram when:** you want the broader saga/orchestration ecosystem, not
just outbox publishing, and are comfortable with its conventions.

**How Tandem differs.** The two are close peers — Eventuate is the acknowledged
inspiration — so the difference is one of **scope and philosophy: Tandem trades breadth for
depth + minimalism.** It keeps the per-aggregate ordering idea but:

- **Minimal footprint** — `tandem-core` has zero dependencies and the client imports almost
  nothing, without Eventuate's heavier dependency surface and framework conventions.
- **Operations as a first-class deliverable** — Micrometer-native monitoring, first-class
  per-aggregate replay, and an opt-in admin API, where Eventuate's monitoring is modest and
  replay is build-your-own.
- **No mandatory relay service** — in-process sharded polling by default, or in a separate
  process you assemble yourself; and a modern **CloudEvents** envelope by default.

**The trade-off, in Eventuate's favour:** Eventuate provides **sagas / orchestration**;
Tandem deliberately does **not**. If you need orchestration, choose Eventuate.

One honest clarification: like every option here, write-side *ordering* is ultimately
**app-established** — Tandem's added value is the in-library `seq` *contract* (the
`UNIQUE(aggregate_id, seq)` safety-net) and strong *relay* ordering, not generating the
order for you (see [HLD §4.2](HLD.md)). Tandem can assign the sequence *number* itself
([HLD-managed-seq.md](HLD-managed-seq.md) §4.1), which is a different thing: it removes the
need for a version field, not the need to serialize concurrent writers.

---

## 4. Spring Modulith

**What it is.** Spring Modulith's event-publication registry persists application events
in a registry table and republishes those not yet marked complete, giving at-least-once
delivery of Spring `ApplicationEvent`s — optionally externalized to a broker.

**Strengths.**
- **Zero extra infrastructure** — uses the application's own datasource.
- **Idiomatic Spring** — integrates naturally with `@TransactionalEventListener` and the
  Spring application-event model.
- **Low operational burden** for simple in-process module decoupling.

**Costs.**
- **Weak publication ordering.** The registry republishes incomplete events but does not
  provide strong per-aggregate ordering to the broker — events can be reordered on
  failure/retry. This is the critical weakness for an ordered-delivery requirement.
- **Spring-only.** Tightly coupled to the Spring programming model; not usable from plain
  Java or other frameworks.
- **No targeted replay** of a specific aggregate's history.
- Primarily designed for **intra-application module decoupling**, with broker
  externalization as a secondary capability.

**Choose Spring Modulith when:** you are decoupling modules *within* a Spring monolith and
ordering guarantees to an external broker are not a hard requirement.

**How Tandem differs.** Tandem's **Spring-events tier** (HLD §3.1) offers the same
`ApplicationEventPublisher` ergonomics — publish a Spring event, Tandem persists it — but
its listener writes to the outbox *in the same transaction* and the relay preserves strong
per-aggregate ordering. So you get the idiomatic-Spring feel of Modulith without its weak
publication ordering, and remain framework-agnostic underneath (the Spring tier is
optional sugar over a Spring-free core).

---

## 5. Hand-rolled outbox

**What it is.** A bespoke `outbox` table plus a custom polling loop that publishes and
marks rows done — the pattern most teams reach for first.

**Strengths.**
- **Full control** and **no dependencies**.
- **No extra infrastructure** beyond your DB and broker.

**Costs.**
- **Every hard part is yours to get right**: `SKIP LOCKED` polling under contention,
  per-aggregate ordering and sharding, lease-based failover, idempotent-producer
  configuration, exponential backoff, poison-message handling, monitoring, and replay.
- **The subtle correctness traps** (mark-DONE-only-after-ack, producer
  `max.in.flight`/idempotence reordering, poison-message-blocks-the-aggregate) are easy to
  miss and expensive to discover in production.
- **No shared, reviewed implementation** — each team re-derives and re-debugs the same
  edge cases.

**Choose hand-rolled when:** requirements are trivial (low volume, no strict ordering,
no replay) and a dependency is genuinely unwanted.

**How Tandem differs.** Tandem *is* the hand-rolled outbox, but with the correctness
traps already handled, monitoring and replay built in, and the design reviewed — at the
cost of a small, focused dependency.

---

## 6. Stream processors — Kafka Streams and Flink

**Why they are in this note.** Both are named in §8 as the answer to a requirement Tandem
sends elsewhere — heavy keyed-ordering fan-out, and any reordering or aggregation on the
consumer side. They come up in the same conversations as Tandem, so they deserve a
straight answer, but the answer is *different category*, not *better* or *worse*.

**What they are.**

- **Kafka Streams** is an embedded Java library: it runs inside your application's JVM,
  scales by starting more instances of that application as a consumer group, and keeps
  durable state in changelog topics on the Kafka you already operate. Its sources and
  sinks are **Kafka topics** — it cannot read a database table at all.
- **Flink** is a separate cluster (JobManager, TaskManagers, a state backend) with
  checkpointed state, event-time watermarks, and exactly-once sinks. Through its **CDC
  connectors** it *can* read a PostgreSQL WAL or MySQL binlog and write to Kafka, so
  unlike Kafka Streams it can technically stand in for a relay.

**Why neither is an outbox alternative.**

- Kafka Streams cannot solve the double write, because the problem is upstream of it:
  something must already have put the event in Kafka atomically with the database
  transaction. Using it *as* the delivery mechanism means running Connect/Debezium in
  front of it — you have not replaced the relay, you have added a stage after it.
- Flink can solve it, but its CDC connectors embed the Debezium engine, so you inherit
  every Debezium cost (§2: replication-slot/binlog privileges, offset-based rather than
  aggregate-targeted replay) **and** add a cluster to operate. If CDC is the answer for
  you, plain Debezium is the cheaper way to get it; Flink earns its weight when you also
  need its processing, not just its delivery.
- Neither offers what the matrix in §1 scores: an outbox contract in the write path, an
  aggregate-targeted replay API, or outbox-specific operational metrics and admin.

| | Kafka Streams | Flink | **Tandem** |
|---|---|---|---|
| Position in the pipeline | Consumer side | Consumer side (can also ingest via CDC) | Producer side |
| Reads from your database | No — Kafka topics only | Yes, via CDC connectors (embedded Debezium) | Yes — the outbox table |
| Solves the double write | No | Yes, with CDC's costs | **Yes** |
| Extra infrastructure | None (a library) | A cluster | **None** |
| Ordering it provides | Buffered merge across input partitions | Event-time buffering + watermarks | **Per-aggregate, at delivery** |
| Durable state | Changelog topics | Checkpoints / state backend | The outbox table itself |
| Outbox operations (replay, admin, metrics) | No | No | **Yes** |

**How Tandem relates to them.** Complement, not competitor, and the two sit on opposite
ends of the same pipeline: Tandem gets each aggregate's events onto the topic in order and
keyed by aggregate id, which is precisely the input a stream processor needs to keep
per-key order through its own partitioning. Tandem stops at the topic — it does no joins,
windowing, or state, and its stated rule is to **integrate with the specialised engine
rather than reimplement it** (HLD §1.1). Running both is the normal arrangement, not a
compromise.

**Choose a stream processor when:** the work is *downstream* of delivery — joins,
aggregations, windowed analytics, or elastically-scaled keyed reordering across many
partitions. That is orthogonal to Tandem, which still handles the producer side.

---

## 7. Where Tandem is positioned

Tandem targets the gap between **"hand-rolled outbox"** (correct but you build and
maintain everything) and **"Debezium/CDC"** (powerful but a separate distributed system
to operate):

- **No extra infrastructure** — just your relational database and Kafka, like a
  hand-rolled outbox, unlike Debezium/Connect.
- **Correctness built in** — the ordering, failover, idempotence, poison-message, and
  ack-before-DONE traps are handled, unlike a hand-rolled outbox.
- **First-class targeted replay** — per-aggregate replay as an API, which none of the
  alternatives offer natively.
- **Micrometer-native monitoring** — lag-age, per-shard lag, failure and retry metrics
  out of the box.
- **Operational suite, all opt-in** — an API-first REST admin API (HLD §7.2) and W3C
  trace/correlation propagation (§7.1). Each is off by default (Pareto), so they add nothing
  to the common case but are there when ops needs them — a depth none of the alternatives
  offer as first-class, outbox-specific tools.
- **Framework-agnostic** — `tandem-core` has zero runtime dependencies; Spring is
  optional, unlike Spring Modulith.

---

## 8. When *not* to choose Tandem

Honesty about the boundaries:

- **You already run Kafka Connect at scale** and want minimal DB load plus general CDC →
  **Debezium** is the better fit; Tandem's polling relay adds DB read load that log
  tailing avoids.
- **You need saga orchestration**, not just outbox publishing → **Eventuate Tram**'s
  ecosystem is purpose-built; Tandem deliberately omits sagas.
- **You only need to decouple modules inside a Spring monolith** with no strict ordering
  to an external broker → **Spring Modulith** is lighter and more idiomatic.
- **Your target is not PostgreSQL** (e.g. MySQL, Oracle, SQL Server, a non-relational
  store) → the shipped schema and claim SQL are PostgreSQL-only, so Tandem does not cover
  you; Debezium's connector breadth might.
- **You need heavy, elastically-scaled keyed-ordering fan-out on the consumer side** → a
  stream processor owns the reordering: **Kafka Streams** (an embedded library) or
  **Flink** (a cluster, for the extreme cases). Tandem delivers the events in order and
  keyed by aggregate, but does not replace the engine (§6). Note this is a *consumer-side*
  point — Tandem still handles the producer side regardless.
