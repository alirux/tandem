# Tandem — Comparison with Alternatives

**Version:** 1.0  
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

The contenders:

- **Debezium** — log-based Change Data Capture (CDC) via Kafka Connect.
- **Eventuate Tram** — a transactional-messaging library with its own CDC/polling relay.
- **Spring Modulith** — application-event publication with an event-publication registry.
- **Plain outbox (hand-rolled)** — a bespoke outbox table + polling loop.
- **Tandem** — this project.

---

## 1. Summary matrix

| | Debezium | Eventuate Tram | Spring Modulith | Hand-rolled outbox | **Tandem** |
|---|---|---|---|---|---|
| Write-side ordering | App code | App code | App code | App code | App code **+ in-library contract** (`seq`, `UNIQUE`) |
| Relay ordering | Strong (CDC log) | Good | Weak | DIY | **Strong (hash routing)** |
| Cross-aggregate causal order | No | No | No | No | **Opt-in (Lamport)** |
| Monitoring | Connector metrics | Minimal | Spring Actuator | DIY | **Micrometer native** |
| Targeted replay (per aggregate) | No (offset rewind) | Manual | No | DIY | **Yes, first-class** |
| Operational admin API | Connect REST (connector-level) | No | No | DIY | **Opt-in REST (API-first)** |
| Forensic attempt history | Logs / DLQ | No | No | DIY | **Opt-in archive** |
| Trace / correlation propagation | Headers (CDC) | Partial | No | DIY | **Opt-in (W3C `traceparent`)** |
| Message envelope | CloudEvents (converter) | Custom | Spring event | DIY | **CloudEvents by default** |
| Extra infrastructure | Kafka Connect + CDC | CDC/polling service | None | None | **None** |
| DB coupling | Per-connector (WAL/binlog) | Supported DBs | JPA/relational | Yours | **PostgreSQL / MySQL** |
| Spring required | No | Optional | **Yes** | No | Optional |
| Ordering granularity | Per-table/partition | Per-aggregate | Weak | DIY | **Per-aggregate** |
| Operational burden | High | Medium | Low | Medium | **Low** |
| License | Apache 2.0 | Apache 2.0 | Apache 2.0 | — | Apache 2.0 |

The rest of this note explains each column.

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
  per-aggregate replay, and opt-in admin API + forensic attempt archive, where Eventuate's
  monitoring is modest and replay is build-your-own.
- **No mandatory relay service** — in-process sharded polling by default, or standalone by
  choice; and a modern **CloudEvents** envelope by default.

**The trade-off, in Eventuate's favour:** Eventuate provides **sagas / orchestration**;
Tandem deliberately does **not**. If you need orchestration, choose Eventuate.

One honest clarification: like every option here, write-side ordering is ultimately
**app-assigned** — Tandem's added value is the in-library `seq` *contract* (the
`UNIQUE(aggregate_id, seq)` safety-net) and strong *relay* ordering, not generating the
order for you (see [HLD §4.2](HLD.md)).

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

## 6. Where Tandem is positioned

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
- **Optional cross-aggregate causal ordering** — the Lamport-clock capability (HLD §9)
  that none of the alternatives provide.
- **Operational & forensic suite, all opt-in** — an API-first REST admin API (HLD §7.3), a
  per-attempt forensic archive (§7.1), and W3C trace/correlation propagation (§7.2). Each is
  off by default (Pareto), so they add nothing to the common case but are there when ops
  needs them — a depth none of the alternatives offer as first-class, outbox-specific tools.
- **Framework-agnostic** — `tandem-core` has zero runtime dependencies; Spring is
  optional, unlike Spring Modulith.

---

## 7. When *not* to choose Tandem

Honesty about the boundaries:

- **You already run Kafka Connect at scale** and want minimal DB load plus general CDC →
  **Debezium** is the better fit; Tandem's polling relay adds DB read load that log
  tailing avoids.
- **You need saga orchestration**, not just outbox publishing → **Eventuate Tram**'s
  ecosystem is purpose-built; Tandem deliberately omits sagas.
- **You only need to decouple modules inside a Spring monolith** with no strict ordering
  to an external broker → **Spring Modulith** is lighter and more idiomatic.
- **Your target is not PostgreSQL or MySQL** (e.g. Oracle, SQL Server, a non-relational
  store) → Tandem's initial DB support does not cover you; Debezium's connector breadth
  might.
- **You need heavy, elastically-scaled keyed-ordering fan-out on the consumer side** → a
  stream processor owns the reordering: Kafka Streams (an embedded library, the default)
  or Flink (a cluster, for the extreme cases). Tandem supplies the ordering key and the
  adapters but does not replace the engine (HLD §9). Note this is a *consumer-side* point —
  Tandem still handles the producer side regardless.
