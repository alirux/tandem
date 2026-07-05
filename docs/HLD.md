# Tandem — High-Level Design

**Version:** 1.0  
**Status:** Draft  
**License:** Apache 2.0

---

## 1. Purpose & Scope

Tandem is a Java library that implements the **Transactional Outbox Pattern**, providing reliable, causally-ordered event delivery from a relational database to Apache Kafka — without external CDC infrastructure, Kafka Connect, or two-phase commit.

**In scope:**
- Atomic insertion of outbox messages within existing domain transactions
- Sharded, at-least-once relay from the outbox table to Kafka topics
- CloudEvents (CNCF) as the standard publication envelope (§4.8)
- Per-aggregate happens-before ordering
- Optional cross-aggregate causal ordering via per-aggregate Lamport clocks (opt-in; see §9)
- Idempotent replay for failed or historical messages
- Port-based operational monitoring (`TandemMetrics`; optional Micrometer adapter)
- Optional Spring Boot autoconfiguration with four usage tiers (plain, template, annotation, Spring application events)

**Out of scope:**
- Consumer-side deduplication (responsibility of consuming services)
- Schema registry / Avro / Protobuf serialization (pluggable, not built-in)
- Strict global total ordering across aggregates (only causal — happens-before — ordering is offered, opt-in)
- Vector-clock concurrency *detection* (Lamport clocks order causally but cannot detect concurrency)

### 1.1 Design philosophy — Pareto's Law

Tandem optimises for the **80%**: a service backed by PostgreSQL/MySQL and Kafka that needs reliable, ordered event delivery. For that majority, Tandem must be a near-drop-in — add the dependency, create the table, insert in your existing transaction. Everything the common case needs (per-aggregate ordering, failover, monitoring, replay) works out of the box with sensible defaults.

The hard **20%** — extreme scale, cross-aggregate global causal ordering, exotic datastores, massive analytical fan-out — is served **without taxing the 80%**:

- **Complexity is opt-in, never imposed.** Causal ordering is off by default; Spring is optional; the engine adapters and benchmark harness are separate modules. The simple path stays simple.
- **Sensible defaults over configuration.** The common case should need almost no tuning (`N = cores × 2`, 60s lease, exponential backoff, JSON payload).
- **Provide the key, don't reinvent the engine.** For the 20% that needs a stream processor or exotic ordering, Tandem integrates (Flink / Kafka Streams adapters, the Lamport key) rather than re-implementing those systems.
- **Scope discipline.** A feature that would complicate the 80% path to serve a fraction of the 20% is pushed out of scope or into an optional module — not folded into the core.

**Decision rule:** if a proposed feature makes the common case harder, slower, or more confusing in order to serve a minority case, it does not belong in the core path — make it opt-in, an adapter, or out of scope.

### 1.2 Architecture — Hexagonal (Ports & Adapters)

Where a component has a clear functional core with at least one port and one adapter, Tandem uses the **Hexagonal (Ports & Adapters)** architecture. The core holds the pure logic and *defines* the ports (interfaces); technology-specific modules are *adapters* that implement them. The invariant: **adapters depend on the core; the core never depends on an adapter.** This is already the shape of Tandem's modules:

| Hexagonal role | Tandem |
|---|---|
| Functional core | `tandem-core` — models, contracts, pure logic (Lamport merge, status state machine); zero external runtime deps |
| Ports (defined by the core) | `OutboxRepository` (persistence), `OutboxDispatcher` (publish), `PayloadSerializer`, `TopicRouter`, `CausalContext`, `AttemptRecorder` (default no-op), `TracePropagator` (default no-op), `TandemMetrics` (default no-op) |
| Driven (outbound) adapters | `tandem-jdbc` (JDBC persistence, `JdbcAttemptArchive`), `tandem-kafka` (Kafka publish), `tandem-test` `InMemoryOutbox` (in-memory persistence) |
| Driving (inbound) adapters | `tandem-spring-producer` usage tiers (template, annotation, Spring events); `tandem-admin` REST layer over `AdminService` |
| Observability / consumer-side adapters | `tandem-micrometer` (metrics), `tandem-tracing-otel` (trace capture), `tandem-kafka-streams`, `tandem-flink` |

Two consequences:

- **Testability** (a primary payoff): the core is exercised against the in-memory adapter (`InMemoryOutbox`) with no database, matching the no-mocks testing approach.
- **Pragmatic boundary** (Pareto, §1.1): apply hexagonal only where a real core/port/adapter split exists. Do not impose port-and-adapter ceremony on modules with no swappable boundary (e.g. the BOM, the benchmark harness) — that would be complexity for its own sake.

### 1.3 Minimal client footprint

The part of Tandem the **client application must import** — the **write-side** (the outbox INSERT; §3.2) — must carry the **minimum possible external dependencies, ideally none**.

- `tandem-core` already has **zero runtime dependencies** and stays that way.
- The write-side built on it (core + the JDBC insert in `tandem-jdbc`) must not drag in heavy transitive libraries: **no Kafka client, no CloudEvents SDK, no tracing library, no mandatory JSON binding, and no Spring unless the user opts into a Spring tier.** JDBC itself is the JDK's `java.sql` (not an external dependency); the client supplies the `DataSource`.
- Everything that *requires* an external dependency — the Kafka client, the CloudEvents SDK (§4.8), tracing adapters (§7.2), stream-processing adapters (§9) — lives on the **relay / optional side**, never on the client path.
- Where the write-side genuinely needs a library (e.g. JSON serialization of the payload), prefer the **client's already-present one** (a `provided`/optional dependency) or a **pluggable SPI with no forced default**, rather than bundling a heavy transitive tree.

**Why:** the client must not inherit Tandem's delivery-side dependencies, which would risk version conflicts with its own libraries (dependency hell) and bloat its artifact. This reinforces the split deployment topology (§3.2) and the zero-dependency-core rule, and is a hard constraint on every write-side LLD.

### 1.4 Backward & forward compatibility — every contract, not just the API

Tandem exposes several long-lived contracts: the **REST Admin API**, the **DB schema**
(`tandem_*` tables), and the **published Kafka messages** (the CloudEvents envelope + headers).
All must evolve with **both** backward and forward compatibility — and there is a *structural*
reason this is non-negotiable: the **split deployment topology (§3.2)** lets the client
write-side, the relay, and the Admin API run as **independently-deployed components at
different Tandem versions, sharing the same database and event stream**. A rolling upgrade
therefore *always* has mixed versions reading and writing the same contracts.

- **Backward compatibility (producers/writers evolve additively).** New optional columns,
  optional request/response fields, endpoints, or CloudEvents extension attributes — **never**
  a removal, rename, type change, newly-required field, narrowed range, or changed identifier
  (error `type` slug, event `type`). Breaking changes are a new major version (DB: a versioned
  migration with a transition window; REST: `/v2`).
- **Forward compatibility (readers are tolerant).** An older component reading a newer contract
  must not break on the unknown:
  - **SQL:** select **named columns, never `SELECT *`**; tolerate extra columns; new columns are
    nullable or defaulted.
  - **Kafka / CloudEvents:** ignore unknown headers and extension attributes; `type` /
    `partitionkey` semantics stay stable.
  - **REST:** tolerant readers (ignore unknown fields and enum values); schemas stay open.
- **Over-strict validation is the enemy of forward compatibility** — `SELECT *`,
  `additionalProperties: false`, closed enums that reject unknown values, or strict envelope
  parsing each break a reader the moment the contract grows. This directly shapes the schema
  migration strategy (Q7).

---

## 2. Problem Statement

### 2.1 The Double Write Problem

Writing a state change to a relational database *and* publishing an event to Kafka are two independent I/O operations with no shared transaction coordinator. The naive approach:

```
BEGIN TX
  UPDATE aggregate ...
COMMIT TX
producer.send(event)          ← crash here → event lost
```

Or reversed:

```
producer.send(event)          ← crash here → event sent but DB not updated
BEGIN TX
  UPDATE aggregate ...
COMMIT TX
```

Both orderings produce permanent divergence between database state and the event stream.

### 2.2 Why 2PC Is Not the Answer

Distributed two-phase commit (XA transactions) is theoretically correct but impractical:
- Kafka's producer does not support XA
- 2PC is a distributed availability risk: one slow participant blocks all
- Most JDBC connection pools and ORMs do not support XA in production configurations

### 2.3 The Outbox Solution

The outbox pattern eliminates the dual-write by making the event write part of the same local transaction as the domain mutation:

```
BEGIN TX
  UPDATE aggregate SET version = version + 1 WHERE id = ? FOR UPDATE
  INSERT INTO tandem_outbox (aggregate_id, seq, payload, ...) VALUES (...)
COMMIT TX                        ← both or neither, guaranteed by DB ACID
```

A separate **relay** process reads committed outbox rows and publishes them to Kafka. If the relay crashes after publishing but before marking the row `DONE`, it republishes on restart — a **duplicate**, not a divergence. Consumers handle duplicates (idempotent processing or deduplication on `(aggregate_id, seq)`).

### 2.4 Why the pattern is under-adopted (and where Tandem fits)

The outbox is essential for one specific class of service: those whose emitted events drive *other services' state* (sagas, inventory, payments — anywhere a lost or phantom event causes divergence). It is *not* needed for best-effort eventing (analytics, notifications, cache invalidation). Yet even where it is needed, adoption is low — for mostly avoidable reasons:

- **The risk is invisible and deferred.** The naive `save(); publish();` works ~99.9% of the time; divergence appears only on partial failure and is usually misdiagnosed as a fluke. The cost of skipping the outbox is probabilistic and late; the cost of building it is upfront and certain — so teams under-invest.
- **"Idempotent consumers" are mistaken for the guarantee.** Deduplication handles *duplicates*; it does nothing about *lost events / divergence*, which is the actual double-write failure. Teams feel covered by a mechanism for a different problem.
- **It looks trivial but is full of traps.** "Write a row and poll it" hides `SKIP LOCKED` contention, ordering, mark-DONE-only-after-ack, producer reordering, poison messages, and lease failover. Naive versions appear to work; the bugs surface rarely and are hard to attribute.
- **No lightweight, correct, standard library.** The space is fragmented into heavy CDC (Debezium), heavier opinionated frameworks (Eventuate Tram), weak-ordering Spring-only options (Spring Modulith), or DIY. No framework ships it on by default, so there is no paved road.
- **The "serious" answer is operationally heavy.** Robustness got associated with log-based CDC + Kafka Connect — a separate distributed system — so teams defer the heavy solution and, with it, the pattern entirely.
- **Defaults teach the anti-pattern.** Nearly every "Spring + Kafka" tutorial shows the double-write in the same method. Most teams learn the outbox only after being burned.

**Tandem's wedge:** the avoidable reasons above — no lightweight library, CDC too heavy, DIY traps, no paved road — are exactly what a small, correct, infrastructure-free library addresses. Tandem does not reinvent the pattern; it makes the right thing the easy thing for a team that already has a relational database and Kafka and wants neither a Connect cluster nor a pile of subtle bugs.

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Application Process                          │
│                                                                     │
│  ┌──────────────┐   same TX   ┌──────────────────────────────────┐  │
│  │  Domain Code │────────────▶│  outbox table (PostgreSQL)       │  │
│  │  (aggregate  │             │  id, aggregate_id, seq, payload, │  │
│  │   mutation)  │             │  status, locked_by, ...          │  │
│  └──────────────┘             └────────────────┬─────────────────┘  │
│                                                │                    │
│  ┌─────────────────────────────────────────────▼─────────────────┐  │
│  │               WorkerPool  (tandem-jdbc)                        │  │
│  │                                                                │  │
│  │  Worker A      Worker B      Worker C    ...                   │  │
│  │  (buckets 0..)  (buckets ..)  (buckets ..)                     │  │
│  │                                                                │  │
│  │  Aggregates → fixed virtual buckets (by hash);                 │  │
│  │  each bucket owned by one worker → its rows ORDER BY id        │  │
│  │                                                                │  │
│  └────────────────────────────┬───────────────────────────────────┘  │
│                               │                                     │
│  ┌────────────────────────────▼───────────────────────────────────┐  │
│  │               KafkaRelay  (tandem-kafka)                       │  │
│  │                                                                │  │
│  │  producer.send(topic, key=aggregate_id, value=payload)         │  │
│  │  await ack (acks=all, enable.idempotence=true)                 │  │
│  │  UPDATE tandem_outbox SET status=DONE                                 │  │
│  └────────────────────────────┬───────────────────────────────────┘  │
└───────────────────────────────┼─────────────────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │    Apache Kafka        │
                    │  (partitioned by       │
                    │   aggregate_id key)    │
                    └───────────────────────┘
```

> **This diagram shows the embedded topology** — the relay runs in the same process as the client. The relay (and the Admin API) can also run as **standalone processes**; only the write-side (the outbox INSERT) must run inside the client. See §3.2.

### 3.1 Module Dependency Graph

```
tandem-core             zero external runtime deps; defines models + ports
tandem-jdbc   ──▶ core   JDBC adapter: outbox INSERT + polling/lease/cleanup (NO Kafka)
tandem-kafka  ──▶ core   Kafka publish adapter (implements the OutboxDispatcher port)

# Spring autoconfig — write-side and relay split so the client can avoid Kafka (§3.2)
tandem-spring-producer  ──▶ tandem-jdbc                  write-side tiers (client; NO Kafka)
tandem-spring-relay     ──▶ tandem-jdbc + tandem-kafka   relay autoconfig
tandem-spring           ──▶ producer + relay             all-in-one (embedded default)
tandem-relay (runnable) ──▶ tandem-spring-relay          prebuilt standalone relay app

tandem-bom              version alignment only, no code
tandem-test   ──▶ core   (+ optional tandem-jdbc/kafka)

# Optional causal-ordering adapters (see §9)
tandem-kafka-streams ──▶ core   (Kafka Streams TimestampExtractor for the Lamport header)
tandem-flink         ──▶ core   (Flink TimestampAssigner/WatermarkStrategy for the Lamport header)
```

`tandem-core` is the only module with zero external runtime dependencies. All other modules depend on it.

`tandem-spring-producer` (the write-side, re-exported by the `tandem-spring` aggregator) provides four optional usage tiers, from lowest to highest abstraction:

| Tier | API | How it works |
|---|---|---|
| **Plain** | Inject `OutboxRepository`, call `.insert()` inside `@Transactional` | No Spring magic; full user control |
| **Template** | `TransactionalOutboxTemplate.execute(supplier)` | Wraps transaction + insert in one call; explicit and testable |
| **Annotation** | `@TransactionalOutbox(aggregateType = "Order")` on a `@Transactional` method | AOP intercept: extracts pending events from the return value after the method completes, then inserts them into the outbox within the same transaction |
| **Spring events** | `ApplicationEventPublisher.publishEvent(event)` inside `@Transactional` | Tandem's in-transaction listener maps each published event to an `OutboxMessage` and inserts it within the *same* transaction |

All four tiers are optional conveniences on top of the core library. Users choose the style that fits their codebase.

#### Annotation tier — constraints and conventions

`@TransactionalOutbox` is a **composed annotation**: it is itself meta-annotated with `@Transactional`, so a transaction is always guaranteed without requiring the user to add both annotations. It exposes all the same attributes as `@Transactional` (`propagation`, `isolation`, `timeout`, `readOnly`, `rollbackFor`, `noRollbackFor`), each aliased via `@AliasFor` to the corresponding `@Transactional` attribute. Users retain full transaction control within the annotation tier.

The annotation tier requires a domain-side convention for event extraction and `seq` assignment:

- **Event extraction:** the aggregate returned by the method must implement `TandemAggregate` (a `tandem-core` interface, analogous to Spring Data's `@DomainEvents`), exposing pending `OutboxMessage` instances. The AOP aspect collects them after the method returns, still within the open transaction.
- **`seq` source:** the `seq` per-aggregate sequence number must originate from the aggregate's `version` field. The annotation cannot invent it — the aggregate is responsible for assigning `seq` before returning.

#### Spring-events tier — constraints and conventions

The domain publishes ordinary Spring application events (`ApplicationEventPublisher.publishEvent(...)`) inside its own `@Transactional` method; Tandem listens and writes them to the outbox. This gives the Spring Modulith-style ergonomics **with** Tandem's strong per-aggregate ordering. The correctness rules:

- **The listener must run *inside* the transaction.** Tandem registers a **synchronous** `@EventListener` (which runs inline, in the publisher's thread and transaction) — **not** `@TransactionalEventListener(phase = AFTER_COMMIT)`, whose default phase runs *after* commit and would insert into the outbox in a separate transaction, breaking atomicity. This is the single most important constraint of this tier.
- **Fail-fast if no transaction.** A synchronous listener fires even with no active transaction, which would insert under autocommit and lose atomicity. The listener therefore asserts an active transaction (`TransactionSynchronizationManager.isActualTransactionActive()`) and throws if absent — never a silent insert, and never a silent drop.
- **Event → `OutboxMessage` mapping.** A published object that already is an `OutboxMessage` is inserted directly; otherwise Tandem looks up a registered `OutboxEventMapper<T>` (an SPI) to convert it. If neither applies, the listener fails fast with a clear error rather than dropping the event.
- **`seq` source:** unchanged — the event must carry the `seq` taken from the aggregate's `version`. Tandem never invents it.

### 3.2 Deployment topology & coordination mode — write-side in the client, relay embeddable or standalone, single or lease-coordinated

Because the database is Tandem's coordination point, only one thing *must* run inside the client application: the **write-side** (the outbox INSERT), because it participates in the domain transaction. Everything else — polling, publishing to Kafka, marking `DONE`, lease/failover, retry/backoff, poison handling, and table housekeeping/cleanup — is DB-coordinated and can run **embedded in the client or as a fully standalone process**.

| Component | Where it can run | Needs |
|---|---|---|
| Write-side (outbox INSERT) | **Client only** (in the domain transaction) | DB; core + JDBC; **no Kafka** |
| Relay (poll, publish, mark DONE, lease, retry, poison, cleanup) | Embedded in the client **or** standalone | DB + Kafka |
| Admin API (§7.3) | Embedded **or** standalone | DB (relay control mediated via DB) |

**Two orthogonal axes.** A relay deployment is described by *two independent* choices — **where** the relay process runs and **how** multiple relay instances coordinate bucket ownership. They compose freely; do not conflate them.

**Axis 1 — deployment location (where the relay runs):**

- **Embedded (default — Pareto 80%).** The relay runs in-process in the client app: one deployable, simplest setup. The client depends on Kafka because it hosts the relay — fine for the common case.
- **Split (opt-in — robustness / isolation).** The relay runs as its own deployable, pointed at the client's outbox DB and Kafka. Benefits:
  - **Dependency isolation** — the client depends only on the write-side (`tandem-spring-producer` → core + JDBC), with **no Kafka client** or its transitive tree, avoiding version conflicts with the app's own libraries.
  - **Physical separation / robustness** — relay and client have independent lifecycles, scaling, and failure domains: the client scales for request load, the relay for outbox throughput; a relay crash or GC pause does not touch request handling.
  - **No runtime coupling** — relay and client communicate only through the outbox DB. The sole shared contract is the DB *schema* (versioned), exactly as for the Admin API (§7.3).

**Axis 2 — coordination mode (how concurrent relay instances share buckets):**

- **`SINGLE` (default).** The relay instance owns **all** `B` buckets in-process; no coordination table. Correct **only when exactly one relay instance** runs against the outbox. Zero extra cost — the Pareto default.
- **`LEASE` (opt-in — multi-instance).** Bucket ownership is **partitioned across instances via the `tandem_bucket_lease` table** (§4.3): each bucket is owned by one instance under a renewable lease, a dead instance's leases expire and survivors reclaim them. Instances also **self-register their presence** (`tandem_relay_member`) so the fair-share split rebalances on a plain scale-up, not just on failover (§4.3, LLD-jdbc §3.2). Correct for **any number** of concurrent relay instances. Requires the lease + member tables and a unique `instanceId` per instance.

> **Why coordination is a *declared* static option, not auto-detected.** An instance cannot reliably discover "am I one of several?" without the very `tandem_bucket_lease` table that `LEASE` provides — the detection is circular. So the operator **declares** the mode (`tandem.relay.coordination`). `LEASE` with a single instance degrades gracefully (one owner claims all `B` buckets), so turning it on "to be safe" is cheap; but `SINGLE` stays the zero-cost default so the 80% pay nothing.

**Combinations (both axes compose):**

| Deployment × Coordination | When to use |
|---|---|
| **Embedded + `SINGLE`** | The Pareto default: one client deployable, a **single** instance hosting the relay. |
| **Embedded + `LEASE`** | The client app is scaled to **N replicas** with the relay co-located — each replica hosts a relay instance and they partition buckets safely. Without `LEASE`, N embedded relays would each own all buckets: correctness (ordering, no double-claim) still holds via the DB, but every instance re-scans every bucket — wasted DB load and amplified duplicate windows, no throughput gain. `LEASE` removes the overlap. |
| **Split + `LEASE`** | The standalone relay scaled out to multiple processes for outbox throughput. |
| **Split + `SINGLE`** | A single dedicated relay process (isolation without horizontal scale). |

> **Running more than one relay instance without `LEASE` is a misconfiguration, not a corruption.** Per-aggregate ordering and single-claim exclusivity are carried by `status = IN_FLIGHT` + `FOR UPDATE SKIP LOCKED` at the row (§4.3, §6), *not* by bucket ownership, so multiple `SINGLE` instances never reorder or double-claim a row — they merely all poll every bucket, multiplying DB work for no gain. `LEASE` is the correct answer whenever more than one relay instance runs.

The relay is **fully domain-agnostic**: it reads rows (`aggregate_id`, `aggregate_type`, serialized `payload`, `headers`) and ships them to Kafka (topic from `aggregate_type`, key = `aggregate_id`, value = payload bytes, headers copied). It never deserializes domain types, so it needs nothing from the client's code.

**Module support.** To let the client avoid the Kafka dependency, the Spring write-side autoconfig is separated from the relay autoconfig (see the module graph in §3.1 and LLD-base):

- `tandem-spring-producer` — write-side tiers + autoconfig (JDBC only, **no Kafka**) → used by the client in the split topology.
- `tandem-spring-relay` — relay autoconfig (JDBC + Kafka).
- `tandem-spring` — all-in-one aggregator (producer + relay) for the simple embedded default.
- `tandem-relay` — a prebuilt **standalone runnable** relay (Spring Boot app / container over `tandem-spring-relay`): point it at DB + Kafka and run.

Ordering and all delivery guarantees are **identical regardless of topology** — they are properties of the DB, the hash-sharding, and the Kafka config, not of where the relay process runs.

---

## 4. Key Architectural Decisions

### 4.1 Per-Aggregate Ordering, Not Global

**Decision:** Events are ordered within an `aggregate_id`, not across all aggregates.

**Rationale:** Global total ordering requires serializing all writes through a single sequence — this destroys parallel throughput and is unnecessary. Two events on `Order#42` must be ordered; events on `Order#42` and `Customer#7` have no causal relationship and can be processed concurrently. This maps directly to Kafka's partition-key model.

**Implication:** Cross-aggregate ordering is not provided by default. Consumers that need it can enable the optional Lamport-clock capability (see §9) or implement their own correlation logic.

### 4.2 Ordering Established at Write Time

**Decision:** The `seq` per-aggregate sequence number is assigned inside the domain transaction, not by the relay.

**Rationale:** The relay cannot reconstruct an order that was never imposed at the source. The only correct place to serialize per-aggregate writes is where the aggregate mutation happens.

**`seq` is a per-aggregate monotonic sequence number**, equivalent to an event-sourcing stream revision or a Kafka per-partition offset. It is monotonically increasing within a single aggregate and has no defined relationship across aggregates. The aggregate owns and advances it — Tandem reads and preserves it but never generates it. The `UNIQUE(aggregate_id, seq)` constraint is a safety net against bugs, not the source of ordering.

**Mechanism:**
```sql
-- Domain transaction
SELECT * FROM aggregates WHERE id = ? FOR UPDATE   -- pessimistic lock
-- or: optimistic lock with version field + retry on conflict
INSERT INTO tandem_outbox (aggregate_id, seq = version + 1, ...)
-- UNIQUE(aggregate_id, seq) constraint is the safety net
```

> **Hard precondition (not optional).** The per-aggregate write lock above must serialize writes so that, within an aggregate, **commit order = `seq` order = `id` order**. Without it, a lower-`seq` row can become *durable* after a higher-`seq` one (the relay polls committed rows, so it would publish them out of order — and no relay model can repair an ordering defect baked into the data). See [q8-worker-model-decision.md](q8-worker-model-decision.md) (E1).

### 4.3 Bucket-Sharded Relay Preserves Ordering

**Decision:** Aggregates are partitioned into a **fixed, large number of virtual buckets** (`B`, e.g. 256), computed **in Java** by `tandem-jdbc` at insert: `bucket = Math.floorMod(fnv1a64(aggregate_id), B)` — a 64-bit FNV-1a hash over the UTF-8 bytes of `aggregate_id`, reduced with `Math.floorMod` (always non-negative, overflow-free even for the minimum hash value, and works for any `B`). `B` is fixed once and **never changed**. Each bucket is owned by **exactly one worker at a time**; a worker owns a subset of buckets. All events of an aggregate fall in the same bucket → the same worker → published in `id` order. There is **no per-aggregate lock** — exclusivity is *structural* (by partitioning), so it cannot be lost to a non-fencing lock.

> **Why the hash is computed in Java, not in SQL.** The `bucket` is stored on the row at insert, and `B` is immutable, so an aggregate's events must *always* hash to the same bucket. A DB-side hash (`hashtextextended` / `CRC32`) ties that value to one engine's hash implementation: a **major Postgres upgrade** (no cross-version stability guarantee) or a **DB-engine migration** could change the hash for new rows and split an aggregate across buckets — the exact reorder hazard B5 guards against. A fixed, portable Java hash (FNV-1a) is identical across engines and versions, so the bucket is stable for the life of the data. It also lets `InMemoryOutbox` (the in-memory test adapter) compute the *same* bucket as the real database.

**Instance → bucket assignment (the coordination mode, §3.2 axis 2):**
- **`SINGLE` (single relay instance):** the instance owns **all** `B` buckets; its N worker threads split them in-process (`bucket % workerCount`). No coordination table, no lock; coverage is all-or-nothing (process liveness). Correct only when exactly one relay instance runs.
- **`LEASE` (multi-instance):** instances claim bucket ownership via a **`tandem_bucket_lease` table** — each bucket is a row owned under a renewable lease; a dead instance's lease expires and another instance claims the bucket, so **coverage self-heals**. Instances additionally **self-register presence** in `tandem_relay_member`, and the fair-share divisor counts live members rather than bucket owners — so a newly-scaled-up instance that starts with zero buckets is still visible to the incumbent, which releases its excess and the fleet rebalances (without this, an incumbent holding every bucket would never see the newcomer and starve it — LLD-jdbc §3.2). Within an instance, its owned buckets are still split across its worker threads (`bucket % workerCount`). Ownership is queryable (the Admin API reads it for relay status) and needs no per-worker dedicated connection. A brief membership-change window where two instances transiently own a bucket is rare and harmless (head-of-chain + `SKIP LOCKED` → at most a duplicate, never a reorder; §6). `LEASE` is available in **either** deployment location — co-located in a horizontally-scaled client (embedded) or across standalone relay processes (§3.2).

Because `B` is fixed, **changing the worker count never requires a schema migration** — only the bucket→worker assignment changes.

**Rationale:** Structural exclusivity removes the per-aggregate lock fragility (no new-insert race, no fencing concern) and makes the failure mode **loud**: an uncovered bucket simply stalls and `lag.age_seconds` climbs (alertable). For a library whose primary contract is ordering, *fewer silent ways to reorder* is the decisive property.

**Kafka partition alignment:** key = `aggregate_id` closes the full ordering chain: DB write lock → outbox `seq` → bucket → worker → Kafka partition.

**Prior art.** Virtual-bucket sharding is a long-established distributed-systems pattern, not a Tandem invention:
- **Consistent hashing with virtual nodes** — a fixed, large set of virtual partitions mapped onto physical nodes, so membership changes reassign buckets without reshuffling data. Canonically described in *Dynamo: Amazon's Highly Available Key-value Store* (DeCandia et al., SOSP 2007); used by Cassandra, Riak, and Hazelcast (which defaults to 271 partitions).
- **Single-owner partitions with rebalance** — exactly one owner per partition at a time, reassigned on membership change — as in **Apache Kafka consumer groups** (partition assignment) and **Akka/Pekko Cluster Sharding** (per-entity ordering via shard ownership).

Tandem applies the pattern to *outbox-table polling* (a bucket is an internal relay partition) and keeps the assignment **in the database** (a `tandem_bucket_lease` table) rather than in an external coordinator (§3.2).

> Per-aggregate dynamic claim (opaque UUID workers, advisory lock per aggregate) was analysed in [q8-worker-model-decision.md](q8-worker-model-decision.md) and **not chosen**: more elastic, but its ordering correctness is condition-dependent and lock-based, with *silent* failure modes. Single-leader sequential relay (Eventuate-style) was also rejected — it forgoes the parallelism that is Tandem's reason for existing.

### 4.4 Idempotent Kafka Producer Required

**Decision:** The Kafka producer must be configured with `enable.idempotence=true` and `acks=all`.

**Rationale:** With `max.in.flight.requests.per.connection > 1` (the default), retried batches can be reordered even from a single thread. Idempotent producer mode prevents this at the cost of a sequence number per partition.

**Alternative:** `max.in.flight.requests.per.connection=1` also prevents reordering but halves throughput on high-latency connections.

**Enforcement:** Tandem sets these as defaults and **fails fast** (`TandemConfigurationException` at startup) if the user overrides them to unsafe values (`acks=0/1`, idempotence off, `max.in.flight > 5`) — the no-loss and ordering guarantees depend on them (LLD-kafka §1).

### 4.5 Mark DONE Only After Kafka Ack

**Decision:** `status=DONE` is written to the database only after receiving a successful producer ack from Kafka (`acks=all`).

**Rationale:** The failure modes are asymmetric:
- Mark DONE before ack, then Kafka unavailable → **event permanently lost**
- Mark DONE after ack, relay crashes before marking → **duplicate on restart**, recoverable

Duplicates are a known, managed outcome. Data loss is not.

### 4.6 Poison Messages Block the Aggregate, Not the Relay

**Decision:** If an event fails after N retry attempts, its `status` is set to `FAILED`. The relay stops processing subsequent events for that `aggregate_id` until the failed message is resolved.

**Rationale:** Skipping a failed message and publishing the next would violate happens-before for that aggregate. The ordering invariant is the primary contract of the library; it must not be silently broken under error conditions.

**Operator path:** Failed messages are visible via metrics (`tandem.outbox.failed.count`) and can be replayed or manually resolved.

### 4.7 Immutable Rows, Deferred Cleanup

**Decision:** Outbox rows are never deleted by the relay. Successful rows are marked `DONE`; cleanup runs as a separate batch job or via table partitioning on `created_at`.

**Rationale:**
- Row-by-row `DELETE` under high write rates causes table bloat and index churn
- Immutable rows make replay trivial: reset `status=PENDING, attempts=0`
- Audit trail is preserved for the retention window (recommended: 7–30 days)

### 4.8 CloudEvents as the Publication Format

**Decision:** The relay publishes to Kafka using the **CloudEvents 1.0** (CNCF) envelope as the default standard, in **binary content mode** (attributes → Kafka `ce_*` headers, payload → message body). Structured mode is optional; a **raw passthrough** mode remains as an escape hatch.

**Rationale:** A standard envelope is interoperable with the CNCF ecosystem (Knative, brokers, tracing tooling) and exposes consistent metadata (`id`, `source`, `type`, `subject`, `time`) for routing/filtering without deserializing the payload — decoupling consumers from Tandem-internal shapes. CloudEvents' `partitionkey` extension and `subject` map cleanly onto `aggregate_id`, so per-aggregate ordering is unchanged (Kafka key stays `aggregate_id`).

**Where it lives:** CloudEvents formatting is a **relay-side** concern in `tandem-kafka` (via `io.cloudevents:cloudevents-kafka`). Consistent with the deployment topology (§3.2), the **client / write-side does not depend on CloudEvents** — it only captures the event `type`; the relay builds the CloudEvent from the stored row + configuration. Topic routing (`aggregate_type` → topic) and payload serialization (`PayloadSerializer` → `data`) are orthogonal and unchanged.

Full mapping, content modes, Tandem extensions (`seq` / `lamport` / `causationid`), **event versioning** (the version lives in the CloudEvents `type` as a `.v{n}` suffix, never in the topic; §1.4 compatibility applies), and schema impact: [HLD-cloudevents.md](HLD-cloudevents.md).

---

## 5. Data Model

### 5.1 Outbox Table (PostgreSQL)

```sql
CREATE TABLE tandem_outbox (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_id    VARCHAR(255) NOT NULL,
    aggregate_type  VARCHAR(255) NOT NULL,
    type            VARCHAR(255),          -- CloudEvents `type`, e.g. com.acme.order.placed (§4.8)
    bucket          SMALLINT     NOT NULL,  -- virtual bucket = Math.floorMod(fnv1a64(aggregate_id), B); computed in Java by tandem-jdbc; §4.3
    seq             BIGINT       NOT NULL,
    payload         JSONB        NOT NULL,   -- JSONB by default; BYTEA when a binary serializer is used (§5.2)
    headers         JSONB,
    status          SMALLINT     NOT NULL DEFAULT 0,
    -- 0 = PENDING, 1 = IN_FLIGHT, 2 = DONE, 3 = FAILED, 4 = DISCARDED
    locked_by       VARCHAR(64),
    locked_until    TIMESTAMPTZ,
    attempts        INT          NOT NULL DEFAULT 0,
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (aggregate_id, seq)
);

-- Partial index for bucket polling (only PENDING rows), by bucket then id
CREATE INDEX idx_tandem_outbox_dispatch
    ON tandem_outbox (bucket, id)
    WHERE status = 0;

-- Index for the head-of-chain / poison check per aggregate (§6, E2)
CREATE INDEX idx_tandem_outbox_aggregate
    ON tandem_outbox (aggregate_id, id)
    WHERE status IN (0, 1, 3);

-- Index for the periodic lease-reclaim (status = 1 AND locked_until < now()); partial on
-- IN_FLIGHT only, so it stays tiny and the ~5s reclaim scans expired leases, not the whole table
CREATE INDEX idx_tandem_outbox_inflight
    ON tandem_outbox (locked_until)
    WHERE status = 1;
```

### 5.2 Column Semantics

| Column | Purpose |
|---|---|
| `id` | Auto-increment surrogate; used as global tie-break for `ORDER BY` in relay |
| `aggregate_id` | Business identity of the aggregate; Kafka message key |
| `aggregate_type` | Routes to Kafka topic — `kebab-case(aggregate_type)` + suffix `-topic` by default (e.g. `Order` → `order-topic`); see LLD-kafka §5 |
| `type` | CloudEvents `type` — the event type (e.g. `com.acme.order.placed`); captured at produce time, mapped to the CloudEvent at publish (§4.8) |
| `bucket` | Virtual bucket `Math.floorMod(fnv1a64(aggregate_id), B)`, computed **in Java** by `tandem-jdbc` at insert (engine-independent; §4.3); the unit of worker ownership. All events of an aggregate share a bucket |
| `seq` | Per-aggregate causal sequence number; enforced by `UNIQUE(aggregate_id, seq)` |
| `payload` | Event data. The core treats it as `byte[]` produced by a pluggable `PayloadSerializer` (no JSON library forced on the client, §1.3). Stored as `JSONB` by default (readable/inspectable); `BYTEA` when a binary serializer (Avro/Protobuf) is used. Becomes CloudEvents `data`. |
| `headers` | Optional Kafka headers (e.g. `correlation-id`, `traceparent`); merged alongside the CloudEvents `ce_*` headers |
| `status` | State machine: `PENDING → IN_FLIGHT → DONE` / `FAILED`; `FAILED → DISCARDED` (admin only). See §5.3 |
| `locked_by` | Worker identity (e.g. `worker-0@hostname`); enables lease-based failover |
| `locked_until` | Lease expiry timestamp; if elapsed, another worker can reclaim the row |
| `attempts` | Total publish attempts; used for backoff and max-retry threshold |
| `last_error` | Error message from the last failed attempt; operational diagnostics |
| `next_attempt_at` | Earliest timestamp for the next retry (exponential backoff) |
| `created_at` | Insertion timestamp; used for lag age metrics and partition cleanup |

### 5.3 Status State Machine

```
         ┌─────────┐
         │ PENDING │  (status = 0)
         └────┬────┘
              │ worker acquires lease
              ▼
         ┌──────────┐
         │ IN_FLIGHT│  (status = 1)
         └────┬─────┘
              │                      │ attempts < max_attempts
     ack OK   │                      ▼
              │              ┌───────────────┐
              │  error ──────▶  backoff wait │
              │              └───────┬───────┘
              │                      │ next_attempt_at elapsed
              │                      ▼
              │              ┌─────────┐
              │ attempts=max ▶  FAILED │  (status = 3)
              │              └────┬────┘
              │                   │  ↑ blocks aggregate processing
              │                   │  admin discard (acknowledged ordering break)
              │                   ▼
              │              ┌───────────┐
              │              │ DISCARDED │  (status = 4)
              │              └───────────┘
              │                   ↑ not polled, does NOT block the aggregate
              ▼
         ┌──────┐
         │ DONE │  (status = 2)
         └──────┘
              │ cleanup job (batch delete or partition drop)
              ▼
           (gone)
```

`DISCARDED` (4) is reachable **only** via the Admin API discard operation on a `FAILED` row,
with an explicit ordering-break acknowledgement (HLD-admin-api). A discarded row is never
polled and is **not** counted by the head-of-chain check (§6, statuses 0/1/3), so it unblocks
the aggregate. It is otherwise immutable (subject to the same cleanup/retention as `DONE`).

**Replay edges (not drawn above):** `ReplayService` / the Admin API can reset a `DONE` (2) or
`FAILED` (3) row back to `PENDING` (0) — `DONE → PENDING` and `FAILED → PENDING` — clearing
`attempts` / `last_error` / `next_attempt_at` (§8). These are the only backward transitions;
`DISCARDED` is terminal and is never replayed.

### 5.4 MySQL Considerations

MySQL 8 supports `SKIP LOCKED`. The `bucket` is computed in Java (§4.3), so it is identical on MySQL with no DB-specific hash function. Other adjustments: `GENERATED ALWAYS AS IDENTITY` → `BIGINT AUTO_INCREMENT`, `JSONB` → `JSON`, `TIMESTAMPTZ` → `TIMESTAMP`/`DATETIME`, and partial indexes are unsupported (use a full index or a filtered equivalent) — full details in Q28 / LLD-jdbc.

---

## 6. End-to-End Flow

The flow below is shown as a single sequence for clarity. The boundary after `COMMIT TX` is the DB: the **write-side** (application thread) and the **relay** communicate only through committed outbox rows, so the relay portion runs either embedded in the client or as a standalone process (§3.2) with no change to the steps.

```
[Application Thread]
    │
    ├── BEGIN TX (existing domain transaction)
    ├── SELECT * FROM aggregates WHERE id=? FOR UPDATE    ← serialize writes per aggregate
    ├── UPDATE aggregates SET version = version+1, ...
    ├── INSERT INTO tandem_outbox
    │       (aggregate_id, aggregate_type, type, bucket, seq=version+1, payload, headers)
    │       -- bucket is computed by tandem-jdbc; the client never sees it
    └── COMMIT TX
              │
              │  (committed row is now visible to relay)
              ▼
[WorkerPool — each worker owns a subset of the fixed virtual buckets (§4.3)]
    │
    ├── Poll loop — claims back-to-back while work remains; sleeps pollInterval (e.g. 100ms)
    │   only when a claim is empty (idle backoff, NOT a per-batch throttle; §3.1 LLD-jdbc)
    │
    ├── Poll this worker's buckets for the HEAD of each aggregate's pending chain
    │   (E2 — never leapfrog a not-yet-DONE earlier row; this also subsumes the poison gate):
    │   SELECT id, aggregate_id, aggregate_type, type, seq, payload, headers
    │     FROM tandem_outbox o
    │    WHERE o.bucket IN (:my_buckets)
    │      AND o.status = 0
    │      AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= now())
    │      AND NOT EXISTS (                          -- only the earliest unfinished row per aggregate
    │          SELECT 1 FROM tandem_outbox e
    │           WHERE e.aggregate_id = o.aggregate_id
    │             AND e.id < o.id
    │             AND e.status IN (0, 1, 3) )        -- PENDING / IN_FLIGHT / FAILED
    │    ORDER BY o.id
    │    FOR UPDATE SKIP LOCKED                      -- guards the brief slot-handoff window
    │    LIMIT :batch_size
    │
    ├── UPDATE tandem_outbox SET status = 1 (IN_FLIGHT), locked_by = '<worker>',
    │       locked_until = now() + :lease_duration   WHERE id IN (...)
    │
    ▼
[KafkaRelay]
    ├── The claimed batch is ONE row per aggregate (heads) → independent rows.
    │   Dispatch them with OVERLAPPING in-flight sends on one ASYNC producer
    │   (up to batch_size in flight = the per-shard concurrency window, §10):
    │     -- wrap each row as a CloudEvent (binary mode) → ce_* headers + body (§4.8)
    │     future = producer.send(                       ← async; ack on producer I/O thread
    │       topic    = TopicRouter.topicFor(record),     ← default: kebab(aggregate_type)+suffix
    │       key      = aggregate_id,                    ← = CloudEvents partitionkey
    │       value    = payload,                         ← CloudEvents data (body)
    │       headers  = ce_id, ce_source, ce_type, ce_subject, ... + headers
    │     )                                             ← acks=all, enable.idempotence
    │
    ├── On each ack (success): collect id → batched UPDATE … SET status=2 (DONE)
    │
    ├── On each failure:
    │     attempts++
    │     If attempts < max_attempts: status=0 (PENDING), next_attempt_at = now()+backoff
    │     Else:                       status=3 (FAILED)   ← this aggregate is now blocked
    │
    └── Per-aggregate order is STRUCTURAL, not an inner loop: the head-of-chain claim keeps
        at most one row per aggregate in flight, and seq(N+1) only becomes a claimable head
        once seq(N) is DONE (E6). A failed/retrying row stays non-DONE, so the aggregate's
        later rows stay blocked next cycle (E2/Q10) — the aggregate stops with no explicit
        break, and the worker keeps its bucket ownership.
```

### 6.1 Lease Failover

If a worker crashes while holding `IN_FLIGHT` rows, the lease expiry (`locked_until`) allows recovery:

```sql
-- Lease reclaim job (runs periodically, e.g. every 5s)
UPDATE tandem_outbox
   SET status = 0, locked_by = NULL, locked_until = NULL
 WHERE status = 1
   AND locked_until < now();
```

This ensures at-least-once delivery even across worker crashes, at the cost of potential duplicates for the reclaimed batch.

---

## 7. Monitoring

Metrics are emitted through a **`TandemMetrics` port** (`tandem-core`, **no-op by default**) so
that core and `tandem-jdbc` never depend on a metrics library. The optional **`tandem-micrometer`**
adapter binds the port to a Micrometer `MeterRegistry`; it is wired only where the relay runs, so
the client write-side never inherits Micrometer (§1.3). The measurements below are the port's:

| Metric | Type | Description | Priority |
|---|---|---|---|
| `tandem.outbox.lag.count` | Gauge | Number of rows with `status=PENDING` | High |
| `tandem.outbox.lag.age_seconds` | Gauge | Age of the oldest PENDING row in seconds | **Critical** |
| `tandem.outbox.published.rate` | Counter | Rows transitioned to `status=DONE` per second | High |
| `tandem.outbox.failed.count` | Gauge | Rows with `status=FAILED` | High |
| `tandem.outbox.retry.count` | Counter | Cumulative retry attempts | Medium |
| `tandem.outbox.lease_expired.count` | Counter | Rows reclaimed from expired leases (proxy for worker crashes) | Medium |
| `tandem.outbox.workers.active` | Gauge | Number of active relay workers | Medium |
| `tandem.outbox.bucket.uncovered` | Gauge | Buckets with PENDING rows but no live owner (coverage stall); derived from `tandem_bucket_lease`, so reported under the **`LEASE`** coordination mode (§3.2) — standalone, or embedded-with-`LEASE`. Under `SINGLE` there is no lease table; supervised worker-thread restart (LLD-jdbc §3.1) keeps coverage and `lag.age_seconds` is the backstop | High |
| `tandem.relay.config.invalid` | Gauge | Set to 1 (tagged `check`) when a startup config invariant is violated — e.g. `rowLease ≤ delivery.timeout.ms`; the relay then fail-fasts (§12, LLD-jdbc §3.5) | High |

**Alerting guidance:**
- `lag.age_seconds` > threshold (e.g. 60s) → relay is stalled or under-provisioned
- `failed.count` > 0 → manual intervention required
- `lease_expired.count` growing rapidly → workers are crashing; investigate JVM health

### 7.1 Attempt Archive (optional, off by default)

Metrics answer *"is the system healthy?"*; they do not let an operator reconstruct *what
happened on each delivery attempt of one event*. For that, Tandem offers an optional,
append-only **attempt archive**: one immutable row per publish attempt capturing temporal
data, outcome, full error detail, attempt number, worker, destination coordinates, and
`trace_id` / `correlation_id`.

Design highlights (full design: [HLD-attempt-archive.md](HLD-attempt-archive.md)):

- **Off by default, zero cost when off.** No `tandem_outbox_attempt` table, no configuration, no
  module, and — via a guard that skips record construction entirely when disabled — no
  write-path overhead. The base setup remains just the `outbox` table.
- **Hexagonal (§1.2).** Port `AttemptRecorder` in `tandem-core`; default
  `NoOpAttemptRecorder`; opt-in `JdbcAttemptArchive` adapter in `tandem-jdbc`, enabled by a
  single flag (`tandem.attempt-archive.enabled`).
- **Consistent, cheap when on.** The archive `INSERT` rides the **same status-update transaction
  the relay already runs** — no new transaction. On **failure** that is the per-record retry/FAILED
  update (one archive row). On **success** the relay marks DONE in batches across aggregates (§6), so
  the archive rows for that batch are inserted **multi-row in the same `markDoneBatch` transaction**;
  each record's ack data (topic/partition/offset/latency) is captured in its send completion handler
  and flushed alongside the DONE ids.
- **Own retention.** The archive grows N× faster than the outbox, so it carries its own
  configurable retention / time-partitioning (relevant only when enabled).

### 7.2 Trace & Correlation Propagation (optional, off by default)

Tandem can propagate distributed-tracing and correlation identifiers across the
asynchronous outbox boundary, so a consumed event traces back to the business operation
that produced it. The outbox makes this non-trivial: the event is *produced* in the domain
transaction (trace context live) but *physically sent* later by the relay (context gone).
Propagation is therefore split into **capture at produce time** and **propagate at publish
time**.

Design highlights (full design: [HLD-tracing.md](HLD-tracing.md)):

- **Reuses the existing `headers` column** — the captured W3C `traceparent` / `tracestate`
  and a configurable `correlation-id` are merged into `headers`. Since the relay already
  publishes `headers` as Kafka headers (§6), basic propagation needs **no relay change and
  no tracing library in the core/relay** — they just carry the opaque standard strings.
- **Standard interop.** Emitting the W3C `traceparent` means any OpenTelemetry-instrumented
  consumer continues the trace automatically.
- **Hexagonal (§1.2).** Port `TracePropagator` in `tandem-core`, default
  `NoOpTracePropagator`; capture happens at the `JdbcOutboxRepository.insert` chokepoint so
  **all four usage tiers** get it transparently. Opt-in adapters: Micrometer Tracing (auto
  in `tandem-spring`) and OpenTelemetry (optional `tandem-tracing-otel`).
- **Off by default, zero cost when off.** Guarded capture — no context lookup, no
  allocation, nothing added to `headers` when disabled.
- **Feeds the attempt archive (§7.1)**, whose `trace_id` / `correlation_id` columns are
  populated from these headers. `correlation_id` is distinct from the `causation_id` of §9.

### 7.3 Admin API (optional, off by default)

An optional REST operations layer over the outbox (the *sends*) and the attempt archive
(the *trace of attempts*), plus relay control. Built **API-first**: the OpenAPI contract
([admin-api.openapi.yaml](admin-api.openapi.yaml)) is the source of truth, defined and
reviewed before implementation; full design in [HLD-admin-api.md](HLD-admin-api.md).

- **Operations.** Outbox: health summary, message search, single-message detail, single and
  bulk replay (with `dryRun`), discard (with explicit ordering-break acknowledgement).
  Attempts: per-message timeline and archive search (by aggregate / status / `traceId` /
  `correlationId` / time). Relay: status, pause/resume (whole or per shard).
- **Off by default + security is the host's job.** Not exposed unless `tandem.admin.enabled`
  is set; Tandem ships the endpoints, not the authentication — the OpenAPI declares the
  expected `bearerAuth` / `apiKeyAuth` schemes but wiring real auth is the application's
  responsibility.
- **Hexagonal (§1.2).** Operations are framework-agnostic use cases (`AdminService`),
  delegating to existing `OutboxRepository` / `ReplayService` / the attempt-archive query;
  REST is a driving adapter in the optional `tandem-admin` module (depends only on
  `tandem-core` + `tandem-jdbc`, never on the client's domain). The future Admin Web UI
  (out of scope) consumes this same contract.
- **Deployable embedded or fully standalone.** Because the DB is Tandem's coordination point,
  the Admin API needs only access to the client's outbox database — **no runtime dependency
  on the client service**. Reads, replay, and discard are pure DB operations (the relay reacts
  to DB state on its next poll); relay pause/resume/status is mediated through DB control /
  heartbeat tables so even that stays DB-only. The only coupling is a shared DB *schema*
  contract, not a runtime service dependency. Full detail in §4 of [HLD-admin-api.md](HLD-admin-api.md).

---

## 8. Replay

Replay resets DONE or FAILED rows back to PENDING, causing the relay to re-process and re-publish them. Safe because:
1. Rows are immutable (never deleted during normal operation)
2. The same worker handles the same aggregate, preserving order
3. Consumers must be idempotent on `(aggregate_id, seq)` — replay is an expected, documented scenario

```sql
-- Replay a specific aggregate, optional ID range
UPDATE tandem_outbox
   SET status = 0, attempts = 0, last_error = NULL, next_attempt_at = NULL
 WHERE aggregate_id = ?
   AND id BETWEEN :from_id AND :to_id
   AND status IN (2, 3);   -- DONE or FAILED

-- Replay all FAILED rows for an aggregate type
UPDATE tandem_outbox
   SET status = 0, attempts = 0, last_error = NULL, next_attempt_at = NULL
 WHERE aggregate_type = ?
   AND status = 3;
```

Tandem exposes a `ReplayService` API (in `tandem-core`) that wraps these queries with parameter validation and emits a `tandem.outbox.replay.count` metric.

---

## 9. Cross-Aggregate Causal Ordering (Optional)

> Full rationale, worked examples, and consumer-side reconstruction strategies are in the companion design note: [causal-ordering.md](causal-ordering.md).

By default Tandem orders events only within an aggregate (`seq`). Some consumers need to apply events from *different* aggregates in an order that never shows an effect before its cause — for example, a materialized view / projection that folds events from multiple aggregates into a single global timeline. For these, Tandem offers an **opt-in** per-aggregate **Lamport clock** that produces a total order *consistent with* cross-aggregate causality.

**What it guarantees:** if event A happened-before event B (across any aggregates), then `lamport(A) < lamport(B)`. The converse does **not** hold — Lamport values cannot *detect* concurrency, only impose a causally-consistent order. Strict global total ordering and vector-clock concurrency detection remain out of scope.

**Disabled by default:** the feature adds a column, a clock store, and a consumer-side context API. Users who need only per-aggregate ordering pay nothing.

### 9.1 `seq` vs `lamport`

| | `seq` | `lamport` |
|---|---|---|
| Scope | Single aggregate | Across all aggregates |
| Purpose | Order within one aggregate | Causally-consistent global order |
| Owner | The aggregate (its `version`) | Tandem (managed clock) |
| Presence | Always | Only when causal ordering is enabled |
| Comparable across aggregates? | No | Yes |

The two coexist: `seq` continues to enforce per-aggregate order and the `UNIQUE(aggregate_id, seq)` safety net; `lamport` is an additional, optional ordering key.

### 9.2 Mechanism

- **Per-aggregate clock.** Each aggregate carries a Lamport counter, advanced under the **same per-aggregate lock** that already serializes the aggregate's writes — so there is no new contention within an aggregate, and because no state is shared across aggregates, no global bottleneck.
- **Advance + merge**, inside the domain transaction:
  ```
  new_lamport = max(local_clock, inbound_ts /* 0 if none */) + 1
  ```
  A purely local mutation advances by 1; a mutation caused by consuming an event with timestamp `t` merges via `max(., t) + 1`. The merge is what makes the clock causal rather than a plain counter.
- **Inbound context.** Consuming code declares the causing event's timestamp via a `CausalContext` (`tandem-core`), auto-populated from the inbound Kafka header on the consumer side (`tandem-spring`). Without an inbound context, a mutation is treated as a causal root (local advance only).
- **Propagation.** The relay writes the value into a Kafka header (`ce_logicalclock`); downstream consumers read it back into their `CausalContext`.
- **Total order.** Consumers sort by `(lamport, aggregate_id)`; the tie-break makes the order deterministic across unrelated aggregates that share a Lamport value.

### 9.3 Schema additions (only when enabled)

```sql
-- Added to the outbox table only when causal ordering is enabled
ALTER TABLE tandem_outbox ADD COLUMN lamport BIGINT;   -- event's logical timestamp
```

The per-aggregate clock lives in a **Tandem-managed `tandem_aggregate_clock(aggregate_id PK, lamport BIGINT)` table** (created only when causal ordering is enabled):

```sql
CREATE TABLE tandem_aggregate_clock (
    aggregate_id VARCHAR(255) PRIMARY KEY,
    lamport      BIGINT NOT NULL
);
```

The clock is advanced at produce time with an atomic upsert whose row lock serializes the
per-aggregate advance (working with both pessimistic and optimistic client locking), and the
returned value is written to `outbox.lamport`:

```sql
INSERT INTO tandem_aggregate_clock (aggregate_id, lamport)
VALUES (:aggregate_id, GREATEST(0, :inbound) + 1)
ON CONFLICT (aggregate_id)
DO UPDATE SET lamport = GREATEST(tandem_aggregate_clock.lamport, :inbound) + 1
RETURNING lamport;
```

This keeps Tandem's boundary clean — it writes only its own tables (`outbox`, `tandem_aggregate_clock`,
`tandem_bucket_lease`), never the client's domain tables. The alternative (a `lamport_clock` column on
the client's aggregate row) was rejected: it would intrude on the client schema *and* force
Tandem to write domain tables.

### 9.4 Implemented scope — (a) clock + propagation, (b) engine adapters

**(a) Clock and propagation** spans the existing modules:

| Module | Responsibility |
|---|---|
| `tandem-core` | `CausalContext` abstraction + pure merge function + `ce_logicalclock` header constant |
| `tandem-jdbc` | clock store (column or `tandem_aggregate_clock` table) + transactional read-merge-write |
| `tandem-kafka` | write / read the `ce_logicalclock` header |
| `tandem-spring` | auto-populate `CausalContext` from inbound Kafka headers on the consumer side |

**(b) Engine adapters.** Tandem does not build a reordering engine; it ships thin adapters so existing stream processors order by `lamport`. The two differ in operational weight — Kafka Streams is an *embedded library* (no cluster; scales as a consumer group; state in changelog topics on the Kafka you already run), while Flink is a *cluster*. **Kafka Streams is the recommended default** since Kafka is already present:

| Module | Nature | Adapter |
|---|---|---|
| `tandem-kafka-streams` | Embedded library (default) | `TimestampExtractor` reading `lamport` from the header, enabling Kafka Streams' cross-partition timestamp-synchronized merge to order by it |
| `tandem-flink` | Cluster | `TimestampAssigner` / `WatermarkStrategy` reading `lamport`, enabling Flink's event-time buffering and late-data handling to order by it |

> **Adapter caveat:** these inject a *logical* counter where the engine expects an *event-time* timestamp. This is correct for **ordering**, but breaks time-based semantics (windows, grace periods, retention) — the adapters are for causal ordering, not windowed time analytics.

### 9.5 Consumer responsibilities (unchanged invariants)

- Delivery remains **at-least-once**: consumers must still deduplicate on `(aggregate_id, seq)`.
- The reordering engine (Kafka Streams by default, or Flink) owns the buffering, watermark, and late-data handling; Tandem only supplies the comparable key.
- A **dependency-parking** strategy (hold an effect until its cause is applied) is an alternative to timestamp reordering; `lamport` still identifies cause vs. effect.

### 9.6 Future development — `tandem-projection` (c)

> **Default vs. niche.** Since Kafka is already in the architecture, the **default** consumer-side reorderer is the `tandem-kafka-streams` adapter (b): Kafka Streams is an *embedded library* (not a cluster), it scales elastically as a consumer group, and it keeps durable state in changelog topics on the Kafka you already run. The inbox reorderer below is **not** a way to "avoid a cluster" — Kafka Streams has no cluster. Its narrower, genuine niche is the case where the **projection target is a relational read model in the same database**: there the inbox keeps the buffer, the apply, and the view in *one* transactional store, whereas Kafka Streams keeps reorder state in RocksDB/changelog and must then write *out* to the relational DB as a separate sink (a second durable store, plus a consume-side write not covered by Kafka Streams' Kafka-to-Kafka exactly-once).

For that relational-projection case, Tandem can provide an in-process reorderer backed by the consumer's own database — the mirror image of the producer-side outbox. **Not in the initial scope**, but specified here so the design is not reduced to a best-effort toy when built.

**The inbox table (durability without checkpoints).** The consuming service already has a relational database. Rather than buffer in memory (lost on crash), incoming events are staged in an `inbox` table — the symmetric counterpart of the outbox:

```
Kafka ──▶ [INSERT into tandem_inbox]  ──commit tx──▶ [commit Kafka offset]
                │  dedup on (aggregate_id, seq)
                ▼
        [drain in (lamport, aggregate_id) order]  ──▶ projection
                │  apply + mark-applied in the SAME tx
                ▼
            idempotent, crash-safe
```

Crash-safety is exactly the outbox story mirrored: insert into the inbox **first**, commit the Kafka offset **after**; a crash in between is recovered by re-reading and deduplicating on `(aggregate_id, seq)`. The drain applies and marks each event in one transaction, so re-application after a crash is idempotent. No RocksDB, no distributed checkpoints — just a table and a poller, reusing the same primitives the outbox relay already needs.

**Two ordering modes.** A durable, cheap buffer enables two distinct semantics rather than one generic "best-effort":

| Mode | Guarantee | Cost | Requires |
|---|---|---|---|
| **Window** | Best-effort: drain in `lamport` order after a bounded wait `D` | Latency `D`; a late cause past the window is a straggler | `lamport` |
| **Dependency** | Strict: apply an effect only once its cause is applied; otherwise park | May wait indefinitely → needs timeout / dead-letter | `causation_id` (see below) |

**`lamport` vs `causation_id`.** `lamport` gives a global *order* but not the *identity* of the cause — it says "cause < effect," not "this specific event is the cause." Precise dependency-parking therefore needs an explicit **causation reference** (`causation_id`) pointing at the causing event, propagated in the header alongside `lamport`. The two are complementary: `lamport` powers window mode, `causation_id` powers precise dependency mode. Both are propagations Tandem already performs.

**The scaling limit — and it is not unique to the inbox.** A *global* causal order across all aggregates needs a single serialization point in **any** engine. Kafka Streams reaches it only by funnelling all events through one key/task — a single writer, exactly like the inbox's single ordered drain. No engine escapes this for truly global ordering. The difference is in the **keyed** case (e.g. a per-customer timeline): there both can parallelize — Kafka Streams via `keyBy` across instances with automatic rebalancing, the inbox via one drain per shard (bounded by the shared DB). So Kafka Streams' real edge is *elastic* scaling of the keyed case and keeping state off the operational DB — not a magic escape from the global bottleneck.

| | `tandem-projection` (inbox) | Kafka Streams (adapter b) | Flink |
|---|---|---|---|
| Nature | Embedded (library) | Embedded (library) | Cluster |
| Infrastructure | Your DB only | Kafka (changelog topics) | Cluster + state backend |
| Reorder-state store | Inbox table (your DB) | RocksDB + changelog | Distributed checkpoints |
| Global ordering | Single drain (single-writer) | Single key/task (single-writer) | Single key/task (single-writer) |
| Keyed-ordering scale | Per-shard drains (DB-bound) | Elastic via `keyBy` | Elastic via `keyBy` |
| Relational projection in same DB | One transactional store | Sink write (second store + seam) | Sink write (second store + seam) |
| Sweet spot | Relational read model, one store, one mental model | Default reorderer; keyed scale; Kafka-to-Kafka | Massive analytical fan-out |

**Positioning:** the inbox is **not** the default — `tandem-kafka-streams` (b) is. The inbox earns its place specifically when the projection is a relational read model in the same DB and a single transactional store (matching the producer-side outbox model) is worth more than elastic scaling.

---

## 10. Non-Functional Requirements

> The throughput and latency targets are verified by the load-testing plan: [HLD-load-testing.md](HLD-load-testing.md). That plan also flags refinements these targets need to be testable (a hardware baseline, a reference payload size, latency percentiles beyond the median, and precise definitions of "normal load" and "sustained").

All throughput/latency targets are stated against the **reference baseline** (single host ≥ 8 cores / ≥ 32 GB / NVMe, PostgreSQL 16, single KRaft broker co-located, `N=8`, `batch_size=100`, **1 KB JSON payload**, `acks=all` + `enable.idempotence=true`) defined in [HLD-load-testing.md](HLD-load-testing.md) §5. The numbers are meaningless detached from that baseline.

| Requirement | Target |
|---|---|
| Throughput | Sustain ≥ 10k events/s **per shard** (≈80k/s aggregate at `N=8`) on the reference baseline, where *sustained* = rate held ≥ 10 min with `lag.age_seconds` flat (not growing) |
| Relay latency — median | `COMMIT` → Kafka ack **p50 < 200 ms** at *normal load* (≈50% of measured sustainable max) |
| Relay latency — tail | `COMMIT` → Kafka ack **p99 < 1 s** at normal load |
| Ordering guarantee | Strict per-aggregate, best-effort cross-aggregate; **zero per-aggregate ordering violations** under all load scenarios |
| Delivery semantics | At-least-once; consumers must be idempotent; **zero lost events** under all load scenarios |
| Java compatibility | Java 17+ (LTS) |
| DB compatibility | PostgreSQL 13+ (primary), MySQL 8.0+ (secondary) |
| Kafka compatibility | Kafka client 3.x |
| Spring Boot | **3.x and 4.x** autoconfiguration (see §10.1); usable without Spring |
| Dependency footprint | `tandem-core` zero runtime deps; the client-imported write-side carries minimal/ideally-zero external deps (§1.3); no mandatory Spring dependency |

### 10.1 Spring Boot 3.x / 4.x compatibility strategy

`tandem-spring` must support both Spring Boot generations. They share everything `tandem-spring` relies on: the autoconfiguration import mechanism (`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`), the Jakarta (`jakarta.*`) namespace, the `@AutoConfiguration` / `@ConditionalOn…` annotations, `@ConfigurationProperties` binding, and the core AOP / `@Transactional` model (including the composed-annotation + `@AliasFor` used by `@TransactionalOutbox`). They differ in the underlying Spring Framework (6.x for Boot 3, 7.x for Boot 4); both keep a **Java 17** baseline, matching Tandem.

**Strategy, in priority order:**

1. **Single artifact on the common API subset (preferred).** Compile `tandem-spring` against the minimal API common to Spring Framework 6.x and 7.x and rely on binary compatibility. The surface Tandem uses is stable across the two, so this is expected to work.
2. **Split modules (fallback).** If a real binary incompatibility surfaces, extract version-agnostic logic into `tandem-spring-core` and ship thin `tandem-spring-boot3` / `tandem-spring-boot4` modules over it. Adopt only if (1) proves infeasible — it doubles the published Spring surface.

**CI implication:** `tandem-spring` is tested against a **version matrix** — at least one Boot 3.x line and one Boot 4.x line — not a single pinned version. A green single-version build does not prove dual-generation compatibility.

---

## 11. Comparison with Alternatives

Tandem is positioned in the gap between a hand-rolled outbox (correct, but you build and maintain everything) and Debezium/CDC (powerful, but a separate distributed system to operate). The full per-alternative analysis — Debezium, Eventuate Tram, Spring Modulith, hand-rolled outbox — including trade-offs and a "when *not* to choose Tandem" section, is in the companion note: [comparison.md](comparison.md).

---

## 12. Open Decisions

| Area | Options | Notes |
|---|---|---|
| Virtual bucket count `B` | Fixed, large (default 256); **never changed after first deploy** | Unit of worker ownership; decouples worker count from the schema (§4.3) |
| Workers per instance | Configurable; default = `Runtime.availableProcessors() * 2` | Each owns `B / workers` of the instance's buckets; instance→bucket assignment follows the coordination mode — `SINGLE` (all buckets in-process) or `LEASE` (`tandem_bucket_lease` table), §3.2/§4.3 |
| Coordination mode | `SINGLE` (default) or `LEASE`; **statically declared**, not auto-detected | `SINGLE` = one relay instance owns all buckets (zero cost); `LEASE` = lease-partitioned ownership for **any** number of concurrent instances (embedded-multi-replica or standalone). §3.2 |
| Lease duration (`rowLease`) | Configurable; default = 60s | Hard invariant **`rowLease > delivery.timeout.ms`** (default = 2×); relay fail-fasts otherwise with a formula-bearing exception/log/metric (LLD-jdbc §3.5) |
| Backoff strategy | Exponential with jitter; max attempts configurable | Default: base 1s, multiplier 2, max 10 attempts |
| ~~Topic routing~~ | **Resolved:** `kebab-case(aggregate_type)` + suffix `-topic` (configurable, no pluralization); override via custom `TopicRouter` or a static map (LLD-kafka §5) | |
| Payload serialization | JSON default; Avro/Protobuf via pluggable `PayloadSerializer` | |
| `@TransactionalOutbox` event extraction | `TandemAggregate` interface (preferred) vs. `@DomainEvents`-style annotation vs. return-value serialization | `TandemAggregate` keeps domain model explicit; `@DomainEvents` reuses Spring Data convention if already adopted |
| Spring-events tier event mapping | Published object implements/extends `OutboxMessage` vs. registered `OutboxEventMapper<T>` SPI for arbitrary domain events | Direct is simplest; the mapper SPI decouples domain events from Tandem types |
| ~~Lamport clock store~~ | **Resolved:** Tandem-managed `tandem_aggregate_clock` table (clean boundary — Tandem never writes domain tables); atomic upsert serializes the per-aggregate advance (§9.3) | |
| Spring Boot dual-version packaging | Single artifact on the common 6.x/7.x API (preferred) vs. split `tandem-spring-boot3` / `-boot4` over `tandem-spring-core` | Decide by validating actual API compatibility; split only if a real incompatibility is found (§10.1) |
| Attempt archive write timing | Synchronous in the status-update tx (consistent; one extra INSERT — preferred) vs. async/batched writer | Only relevant when the attempt archive is enabled (§7.1) |
| Attempt archive — which attempts | Record every attempt incl. the successful one (full timeline) vs. failures + final success only (smaller) | Trade forensic completeness vs. archive size |
| Trace propagation enablement | Explicit flag (`tandem.tracing.enabled`) vs. auto-enable when a tracing adapter is on the classpath | Only relevant when trace/correlation propagation is used (§7.2) |
| Correlation-id source | MDC key (default) vs. explicit `TandemContext` API vs. both | |
| Admin API spec ↔ code binding | Generate server stubs from the OpenAPI at build time vs. hand-write + validate against the spec in CI | API-first either way (§7.3) |
| Admin API discard semantics | Hard skip (ordering break, acknowledged) vs. discard + tombstone/compensation | Only relevant when the Admin API is enabled |
| Producer/relay packaging (split topology) | Split modules (`tandem-spring-producer` / `tandem-spring-relay`) vs. single `tandem-spring` with Kafka as an optional dependency + conditional relay autoconfig | Both let the client avoid the Kafka dependency (§3.2); split is more explicit, optional-dep is fewer modules |
| ~~CloudEvents `id` / `source` / event versioning~~ | **Resolved:** `id` = outbox `id`; `source` = a single configured URI (`tandem.kafka.source`); event **version lives in the `type`** (`.v{n}`), topic stays version-agnostic; optional `dataschema` from header/config (HLD-cloudevents §7–§8, LLD-kafka §3/§6) | Content mode (binary), raw escape hatch, and `type` column also decided (§4.8) |
| CloudEvents trace header naming | Bare `traceparent` / `tracestate` vs. `ce_`-prefixed — **deferred** until tracing (`tandem-tracing-otel`) lands; tracing is off in the basic round | Only affects the (optional) trace extension (§7.2, HLD-cloudevents §8) |
| Write-side payload serializer dependency | Bundle a JSON lib vs. use the client's existing one (`provided`/optional) vs. a minimal built-in writer | Must honour minimal client footprint (§1.3) — the write-side must not force a heavy JSON dependency on the client |
