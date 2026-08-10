# Tandem — Cross-Aggregate Causal Ordering (Design Note)

**Version:** 1.2  
**Status:** Draft — **designed, not implemented** (see §0)  
**Companion to:** [HLD.md](HLD.md) §9 (Cross-Aggregate Causal Ordering)

**This document is the design.** [HLD §9](HLD.md) carries only what the rest of the HLD needs
inline — what the capability guarantees, `seq` vs `lamport`, and the decisions that bind other
sections — and defers everything else here. Nothing is stated in both places: if a detail is
missing from the HLD, it is because this is where it lives.

It explains *why* per-aggregate ordering is insufficient for some consumers, what a Lamport clock
adds, how it would be wired into Tandem, the burden it places on consumers, and which tools do
which part of the work.

---

## 0. Implementation status — read this first

**Nothing in this document is built.** Everything from §1 onward describes a design, in the
present tense, as designs are written. What *ships* is a small set of published-but-inert
declarations — types, constants and one field — kept so that building the feature later stays an
**additive** change (HLD §1.4) rather than a breaking one.

There is **no way to turn causal ordering on.** No flag, no property, no DDL. A javadoc saying
"only when causal ordering is enabled" would therefore be misleading, and the code says
"reserved" instead.

### 0.1 The reserved surface — everything that exists today

| # | Element | Module | State |
|---|---|---|---|
| 1 | `CausalContext` (port: `NONE` + `inboundTimestamp()`) | `tandem-core` | Zero references from any product code |
| 2 | `LamportClock.merge(local, inbound)` | `tandem-core` | Zero references from any product code; unit-tested |
| 3 | `OutboxRecord.lamport` (field + getter + builder setter) | `tandem-core` | Never set by product code; always `null` |
| 4 | `CloudEventsHeaders.EXT_LOGICAL_CLOCK` / `CE_LOGICAL_CLOCK` | `tandem-core` | Read only by #6 |
| 5 | `TandemHeaders.CAUSATION_ID` | `tandem-core` | Never written |
| 6 | `if (record.lamport() != null)` in `CloudEventEncoder` | `tandem-kafka` | **Unreachable branch** — see §0.3 |

Six elements, three of them ghosts with no caller at all. Any change to one of them should update
this table in the same commit.

### 0.2 What is missing — the whole mechanism

Everything that would make the six above do something:

- the `lamport` column on `tandem_outbox` and the `tandem_aggregate_clock` table (§3.1) —
  neither is in `schema/postgres/tandem-baseline.sql`
- the transactional advance-and-merge upsert on the write path (LLD-jdbc §2)
- the enablement switch, wherever it would live
- the consumer-side helper that populates `CausalContext` from the inbound header
- both engine adapters, `tandem-kafka-streams` and `tandem-flink` (§7b) — neither module exists

### 0.3 The one leak into a live path

Elements #1–#5 are inert declarations: nothing executes them. Element #6 is different — it is a
real branch on the relay's encode path, evaluated for every published record, that can never be
taken. It is kept deliberately (removing it would mean re-adding it verbatim the day the feature
lands, and a null check per encode costs nothing measurable), but it is the single place the
unbuilt feature touches running code, so it carries an explicit comment saying so.

### 0.4 Scope, if it is ever built

§7 requires shipping **(a) clock + propagation and (b) the engine adapters together**. This
is not a preference: (a) alone writes a number into a header that no consumer can act on, so it
delivers nothing on its own. Treat "implement causal ordering" as including two new modules.

Two properties of the design deserve to be re-read before committing to it, because both are easy
to discover too late:

- **It degrades silently.** The merge happens only if *every* consumer threads the
  `CausalContext`. One that does not silently reduces the clock to a per-aggregate counter — no
  error, no warning (§8).
- **It taxes the consumer's pipeline.** The engine adapters inject a logical counter where Kafka
  Streams and Flink expect an event-time timestamp, which is correct for ordering but breaks every
  time-based semantic of those engines — windows, grace periods, retention (§6.2, §7b).

---

## 1. Two different ordering problems

Tandem's default guarantee is **per-aggregate happens-before order**: two events are
ordered only if they share an `aggregate_id`. That is enough for the vast majority of
consumers and is what keeps throughput high (different aggregates are fully parallel).

A smaller set of consumers needs something stronger: an order that relates events
**across** aggregates so that an effect is never observed before its cause. The
canonical case is a **materialized view** — a CQRS read model / projection: a
denormalized table a consumer keeps up to date by folding events from *many*
aggregates into a single global timeline. There, the order in which events are applied
determines whether the view is correct.

These are two distinct problems:

| Problem | Scope | Provided by |
|---|---|---|
| Per-aggregate order | Events sharing an `aggregate_id` | `seq` (always on) |
| Cross-aggregate causal order | Events across different aggregates | `lamport` (opt-in) |

---

## 2. Why `seq` is not a Lamport clock

It is tempting to call `seq` a Lamport clock. It is not — and in a spec the label is
actively misleading.

A **Lamport clock**'s defining mechanism is the cross-process merge on message
receipt:

```
on local event:        L = L + 1
on send:               attach L to the message
on receive(msg, t):    L = max(L, t) + 1     ← THIS is the point of Lamport
```

That merge step is what propagates happens-before *between* processes. Strip it out and
what remains is just a per-process counter — a plain sequence number.

`seq` has no merge step. Each aggregate increments its own `version` in complete
isolation; nothing crosses aggregate boundaries. So `seq` is exactly a
**per-aggregate sequence number** — equivalent to an event-sourcing *stream revision*
or a Kafka *per-partition offset*: monotonic within one aggregate, with no defined
relationship across aggregates.

Calling it a Lamport clock would wrongly imply a cross-aggregate causal guarantee that
does not exist.

### 2.1 You cannot repurpose the columns you already have

- **Global `id` (`BIGINT IDENTITY`)** is assigned at INSERT time, but transactions
  commit in a *different* order than they grab IDs. TX1 can take `id=100`, stall, while
  TX2 takes `id=101` and commits first. So `id` does **not** respect happens-before
  across transactions — it is an arrival-ish tiebreak, not a causal clock.
- **`created_at` wall-clock** is subject to clock skew between nodes, which can place an
  effect *before* its cause.

A real, separate mechanism is required.

---

## 3. The mechanism

Model each **aggregate as a Lamport process**. The "messages" that carry causality
between processes are the events themselves, flowing aggregate → Kafka → consumer →
next aggregate.

**1. Per-aggregate clock.** Each aggregate carries a Lamport counter. It is advanced
under the **same per-aggregate lock** that already serializes that aggregate's writes.
Consequence: no new contention within an aggregate, and — because no state is shared
across aggregates — no global bottleneck (unlike a single shared counter, which would
serialize everything).

**2. Advance + merge**, inside the domain transaction:

```
new_lamport = max(local_clock, inbound_ts /* 0 if none */) + 1
```

- Purely local mutation → `local_clock + 1`
- Mutation *caused by* consuming an event carrying timestamp `t` → `max(local_clock, t) + 1`

The merge is the entire point: without it, this is just a per-aggregate counter again.

**3. Propagation.** The relay writes `lamport` into a Kafka header (`ce_logicalclock`)
alongside the payload. That header is how the timestamp reaches whatever consumes the
event downstream.

**4. Inbound capture — the boundary problem.** Tandem is producer-side; it does not own
your consumers. For the merge to work, consuming code must declare "this transaction is
caused by an inbound event with timestamp `t`." That needs a small API — a
`CausalContext` (ThreadLocal-backed, with explicit override) the consumer sets before
opening the domain transaction:

```
Order#42 ──emit──▶ outbox(lamport=5) ──▶ Kafka header ce_logicalclock=5
                                              │
                              consumer reads header, sets CausalContext(5)
                                              ▼
                    Customer#7 mutation in TX: max(local=2, inbound=5)+1 = 6
                                              ▼
                              outbox(lamport=6) ──▶ Kafka header ce_logicalclock=6
```

**5. Total order.** A total order comes from sorting by `(lamport, aggregate_id)` — the
tiebreak is needed because two unrelated aggregates can land on the same `lamport`
value.

### 3.1 Where the clock lives — schema and the advance query

Two schema additions, both created **only** when the feature is enabled (neither is in
`schema/postgres/tandem-baseline.sql` today — §0):

```sql
-- the event's logical timestamp, on the outbox row
ALTER TABLE tandem_outbox ADD COLUMN lamport BIGINT;

-- the per-aggregate clock itself
CREATE TABLE tandem_aggregate_clock (
    aggregate_id VARCHAR(255) PRIMARY KEY,
    lamport      BIGINT NOT NULL
);
```

The clock is advanced at produce time with an atomic upsert whose **row lock serializes the
per-aggregate advance** — which is what makes it work under pessimistic *and* optimistic client
locking alike. The returned value is written to `tandem_outbox.lamport` in the same transaction:

```sql
INSERT INTO tandem_aggregate_clock (aggregate_id, lamport)
VALUES (:aggregate_id, GREATEST(0, :inbound) + 1)
ON CONFLICT (aggregate_id)
DO UPDATE SET lamport = GREATEST(tandem_aggregate_clock.lamport, :inbound) + 1
RETURNING lamport;
```

`:inbound` is the `CausalContext` timestamp, or 0 when the mutation is a causal root. When the
feature is off the step is skipped entirely — no table, no upsert, no cost (HLD §1.3).

**Rejected alternative: a `lamport_clock` column on the client's own aggregate row.** It would
intrude on the client's schema *and* force Tandem to write domain tables. Keeping the clock in a
`tandem_*` table preserves the boundary the whole library is built on — Tandem writes only its own
tables (`tandem_outbox`, `tandem_aggregate_clock`, `tandem_bucket_lease`, `tandem_meta`), never
yours.

---

## 4. Worked example — a money transfer

Debiting Wallet#A **causes** crediting Wallet#B.

```
Wallet#A:  Debited(-50)    (seq 7 on A)
              │ causes
              ▼
Wallet#B:  Credited(+50)   (seq 3 on B)
```

### 4.1 Why per-aggregate order is not enough

`seq` values from different aggregates live on **independent number lines** — they are
not comparable:

```
Wallet#A timeline:   seq 1 ── seq 2 ── ... ── seq 7  (Debited -50)
Wallet#B timeline:   seq 1 ── seq 2 ── seq 3  (Credited +50)

Is A's seq=7 before or after B's seq=3?  ← undefined. Different counters.
```

A consumer building a **global ledger view** consumes both wallets. With only
per-aggregate ordering, nothing stops it applying `Credited` before `Debited`. The view
then shows the deposit appearing *before* the withdrawal that funded it. That is not
merely cosmetic:

- If the view enforces "a credit from a transfer must reference an existing debit,"
  applying `Credited` first means the referenced debit does not exist yet → error or
  corrupt intermediate state.
- A balance reconstruction that folds events in the wrong order transits through states
  that **never actually happened**.

### 4.2 What Lamport fixes

The transfer event carries the cause's Lamport timestamp in its header. The transaction
that produces `Credited` merges it:

```
Debited   → lamport 8        (header ce_logicalclock=8)
Credited  → max(local_B, 8)+1 = 9
```

Now `Credited(9) > Debited(8)` — **always**, regardless of which arrives first or what
the wall clocks say. The consumer sorts by `(lamport, aggregate_id)` and applies the
debit before the credit. The view never shows an effect before its cause.

The value proposition in one line: it turns *"these two events are on unrelated number
lines"* into *"the cause provably has a smaller number than the effect."*

---

## 5. Ordering ≠ delivery: the consumer's burden

A comparable timestamp answers only one question: *given two events I already hold,
which comes first?* It says nothing about *when* those events arrive. Two separate
problems:

- **Logical order** — solved by Lamport: `cause.lamport < effect.lamport`, always.
- **Delivery order** — *not* solved: events enter the consumer in whatever order the
  network, partitions, and scheduling deliver them, unrelated to Lamport order.

### 5.1 Why delivery is out of order

Tandem's throughput comes from partitioning: events for different aggregates land on
different Kafka partitions (key = `aggregate_id`), consumed in parallel. There is no
single totally-ordered channel — by design, because such a channel would serialize
everything.

```
Partition P0:  ...─ Debited(lamport 8) ───────────────▶ consumer
Partition P1:  ...──────── Credited(lamport 9) ─▶ consumer   ← arrives FIRST
```

`Credited` (effect, lamport 9) can reach the consumer before `Debited` (cause, lamport
8) simply because P1 is less loaded or consumed by a faster thread. Lamport tells you
8 < 9; it is up to the consumer not to apply the 9 until it is reasonably sure no 8 is
still coming.

### 5.2 Buffer + watermark

The consumer buffers and emits only when "safe." Safety is a **watermark** `W`: a
threshold guaranteeing no more events with `lamport ≤ W` will arrive. Buffered events
with `lamport ≤ W` can be applied in order; the rest wait.

```
buffer:  [Credited@9]   watermark W=7   → not yet (9 > 7)
arrives  Debited@8, watermark rises to W=9
buffer:  [Debited@8, Credited@9], W=9   → apply 8, then 9  ✓
```

### 5.3 The complication hash-sharding introduces

An honest caveat: **within a single Kafka partition, `lamport` values are NOT
monotonic.** A partition holds every aggregate that hashes to it, and their clocks are
independent:

```
Partition P0 (by offset):  Wallet#A@8 ─ Wallet#C@3 ─ Wallet#A@12 ─ Wallet#C@5
lamports seen:             8,           3,           12,            5   ← not increasing
```

So "the last lamport seen on this partition" is **not** a lower bound on that
partition's future lamports, and the clean per-partition-minimum watermark does not
apply directly. Achieving it would require partitioning by the lamport itself —
reintroducing the global serialization we set out to avoid.

### 5.4 What is done in practice: bounded-staleness window

The consumer trades the strict guarantee for a **bounded-staleness window**: buffer for
a maximum delay `D` (time- or count-based), sort by `lamport` within the window, then
apply. It is *best-effort*, not a formal proof, but correct in practice for two concrete
reasons:

1. **The cause precedes the effect in publication.** The effect's record is *created*
   only after the transaction consuming the cause has committed; so the cause's record
   exists on Kafka before the effect is even generated. The buffer absorbs only delivery
   *jitter*, not arbitrary disorder.
2. **Causal chains are short.** An effect rarely depends on a cause published many
   seconds earlier, so a small `D` (tens/hundreds of ms) covers the vast majority of
   cases.

The price is an explicit trade-off: **large `D` = more correct order but more latency;
small `D` = less latency but a risk of applying an effect before a late cause.**

### 5.5 Edge cases

- **Silent partition (head-of-line blocking).** A strict watermark stalls on a partition
  emitting nothing. An *idle timeout* / heartbeat must advance the watermark after a
  quiet period — at the cost of possibly ignoring a genuinely late event.
- **Stragglers.** An event arriving after its window has been applied must be handled
  explicitly: dropped, applied out of order with compensation, or treated as a
  correction.
- **Lamport ties.** Two unrelated events can share a lamport; the `(lamport,
  aggregate_id)` tiebreak gives a deterministic, reproducible total order across consumer
  replicas.

### 5.6 Alternative: dependency-parking, not timestamp reordering

Instead of reordering by clock, the consumer can make the **projection tolerant of
disorder**: if `Credited` arrives but the referenced debit has not been applied yet,
*park* it until the cause arrives, then apply it. Here Lamport is not the global sort
key, but it still **identifies which event is cause and which is effect** and surfaces
the dependency. This is often more robust than a watermark because it does not depend on
a time estimate.

---

## 6. Who does which part of the work

**Key fact:** no mainstream stream processor performs *causal* reordering from an
application clock out of the box. They all do *event-time* (timestamp) reordering. The
practical trick is to reuse their buffering machinery by feeding it the Lamport value
*in place of* a timestamp.

### 6.1 Kafka alone — no

Kafka guarantees only per-partition order. The broker has no notion of cross-partition
ordering, watermarks, or causality. In the `poll()` loop the consumer receives records
per-partition in offset order; any reordering is your code.

### 6.2 Kafka Streams — the recommended default

An **embedded library**, not a cluster: it runs inside the consumer's JVM, scales by
running more app instances as a consumer group, and stores durable state in changelog
topics on the Kafka you already operate. Because Kafka is already in the architecture,
this is the recommended default for consumer-side reordering.

Has a cross-partition buffered merge already: when a task has multiple input partitions,
it picks the record with the smallest timestamp among the buffer heads, and
`max.task.idle.ms` controls how long to wait for a slow partition (the buffer +
idle-timeout mechanism). A custom `TimestampExtractor` reading the `lamport` header makes
that merge order by Lamport.

- *Caveat:* Kafka Streams timestamps have "milliseconds" semantics — windows, grace
  periods, and retention assume real time. Injecting a logical counter works for the
  **merge order** but breaks every time-based semantic. Good for ordering, not for
  windowing.

### 6.3 Flink — the most capable substrate (but a cluster)

Unlike Kafka Streams, Flink is a separate cluster (JobManager, TaskManagers, a state
backend) — real infrastructure to operate. Reserve it for cases that justify the weight.


Two routes:

- Event-time + `WatermarkStrategy`: assign the Lamport value as the event timestamp;
  Flink buffers and emits in order with late-data handling (`allowedLateness`, side
  outputs). Same time-semantics caveat.
- Low-level `KeyedProcessFunction` with state + timers: implement the
  buffer/watermark/parking logic yourself, but with **durable state via checkpoints** —
  the buffer survives restarts without re-applying. The robust route.

### 6.4 Others

- **ksqlDB** = built on Kafka Streams → same properties and limits.
- **Materialize / RisingWave** (streaming SQL DBs) give consistent views, but assign
  their *own* internal logical timestamp (from source progress / offsets); they do not
  ingest *your* Lamport as the ordering authority.
- **Akka/Pekko, Esper/CEP** buffer and order, but always event-time, never causal
  natively.
- True *causal delivery* primitives exist in group-communication systems (causal
  broadcast), but those are a different world from the Kafka ecosystem and are not used
  for this.

**Conclusion:** you do not build the buffering engine — you reuse Flink's or Kafka
Streams'. What is missing from all of them, and what Tandem supplies, is the
**causal ordering key** (the Lamport value). They provide the engine; you provide the
key.

---

## 7. How Tandem implements it — three levels

**(a) The clock and its propagation — the base (always).**
Tandem owns the clock, not the projection engine: it stamps the `lamport`, writes it to
the Kafka header, and on the consumer side offers a `CausalContext` (populated from
inbound headers) so the merge `max(local, inbound)+1` happens on re-emit. Spans
`tandem-core` (abstraction + merge + header constant), `tandem-jdbc` (clock store +
transactional read-merge-write), `tandem-kafka` (header I/O), `tandem-spring-producer`
(auto-populate `CausalContext` from inbound headers on the consumer side).

**(b) Adapters to existing engines — the sweet spot, and the default.**
Small, high-value glue: a `TimestampExtractor` for Kafka Streams (`tandem-kafka-streams`)
and a `TimestampAssigner` / `WatermarkStrategy` for Flink (`tandem-flink`) that read the
`lamport` header. The user reuses the industrial reordering of those engines, ordered by
*Tandem's* clock. **Kafka Streams is the recommended default** — an embedded library that
needs no cluster and is already justified by Kafka's presence; Flink is reserved for the
heavy, cluster-justifying cases.

**(c) `tandem-projection` — future development, backed by an inbox table.**
First, the framing correction: this is **not** "the way to avoid a cluster." Since Kafka
is already present, the **default** consumer-side reorderer is the Kafka Streams adapter
(b) — Kafka Streams is an *embedded library*, not a cluster, scales as a consumer group,
and stores durable state in changelog topics on the Kafka you already run. Only **Flink**
is a true cluster.

The inbox reorderer has a narrower, genuine niche: when the **projection target is a
relational read model in the same database**. There, the inbox keeps the buffer, the
apply, and the view in *one* transactional store, whereas Kafka Streams keeps reorder
state in RocksDB/changelog and must then write *out* to the relational DB as a separate
sink — a second durable store, plus a consume-side write **not** covered by Kafka Streams'
Kafka-to-Kafka exactly-once (so it needs idempotent upserts on `(aggregate_id, seq)`
anyway). For a SQL-queryable materialized view, the inbox is the simpler, single-store
option — and it mirrors the producer-side outbox exactly.

The naive version (an in-memory buffer) is fragile — lost on crash, may re-apply. The
durable version uses an `inbox` table, the symmetric counterpart of the outbox:

```
Kafka ──▶ [INSERT into tandem_inbox]  ──commit tx──▶ [commit Kafka offset]
                │  dedup on (aggregate_id, seq)
                ▼
        [drain in (lamport, aggregate_id) order]  ──▶ projection
                │  apply + mark-applied in the SAME tx
                ▼
            idempotent, crash-safe
```

Crash-safety is the outbox story mirrored: insert into the inbox **first**, commit the
offset **after**; a crash in between is recovered by re-reading and deduplicating on
`(aggregate_id, seq)`. The drain applies and marks each event in one transaction, so
re-application is idempotent. No RocksDB, no distributed checkpoints — a table and a
poller, reusing the primitives the outbox relay already needs.

*Two ordering modes* become available with a durable buffer:

| Mode | Guarantee | Cost | Requires |
|---|---|---|---|
| **Window** | Best-effort: drain in `lamport` order after a bounded wait `D` | Latency `D`; a cause past the window is a straggler | `lamport` |
| **Dependency** | Strict: apply an effect only once its cause is applied; otherwise park | May wait indefinitely → needs timeout / dead-letter | `causation_id` |

*`lamport` vs `causation_id`.* `lamport` gives a global *order* but not the *identity*
of the cause. Precise dependency-parking needs an explicit causation reference
(`causation_id`) pointing at the causing event, propagated in the header alongside
`lamport`. They are complementary: `lamport` powers window mode, `causation_id` powers
precise dependency mode.

*The scaling limit — and it is not unique to the inbox.* A *global* causal order across
all aggregates needs a single serialization point in **any** engine. Kafka Streams
reaches it only by funnelling all events through one key/task — a single writer, exactly
like the inbox's single ordered drain. No engine escapes this for truly global ordering.
The difference shows only in the **keyed** case (e.g. a per-customer timeline): there both
can parallelize — Kafka Streams via `keyBy` across instances with automatic rebalancing,
the inbox via one drain per shard (bounded by the shared DB). Kafka Streams' real edge is
*elastic* scaling of the keyed case and keeping reorder state off the operational DB — not
a magic escape from the global bottleneck.

| | `tandem-projection` (inbox) | Kafka Streams (adapter b) | Flink |
|---|---|---|---|
| Nature | Embedded (library) | Embedded (library) | Cluster |
| Infrastructure | Your DB only | Kafka (changelog topics) | Cluster + state backend |
| Reorder-state store | Inbox table (your DB) | RocksDB + changelog | Distributed checkpoints |
| Global ordering | Single drain (single-writer) | Single key/task (single-writer) | Single key/task (single-writer) |
| Keyed-ordering scale | Per-shard drains (DB-bound) | Elastic via `keyBy` | Elastic via `keyBy` |
| Relational projection in same DB | One transactional store | Sink write (second store + seam) | Sink write (second store + seam) |
| Sweet spot | Relational read model, one store, one mental model | Default reorderer; keyed scale; Kafka-to-Kafka | Massive analytical fan-out |

The inbox is **not** the default — the Kafka Streams adapter (b) is. The inbox earns its
place specifically when the projection is a relational read model in the same DB and a
single transactional store (matching the producer-side outbox) is worth more than elastic
scaling.

**Scope decision (within the causal-ordering feature).** Causal ordering is **opt-in, off by
default, and not part of the basic round**; this phasing applies *when the feature is built*. At
that point ship **(a)** and **(b)** together — with the Kafka Streams adapter as the recommended
default consumer-side reorderer — and specify **(c)** as future work (above) for the narrower
relational-projection-in-same-DB case. This keeps Tandem faithful to its principle — *provide the
key, do not reinvent the engine*.

---

## 8. Limitations

- **One-directional guarantee.** Lamport gives `a → b ⟹ L(a) < L(b)`, but **not** the
  converse. You can build a causally-consistent total order, but you **cannot detect
  concurrency** from it. For conflict detection ("were these two events concurrent?") you
  need **vector clocks** — one entry per aggregate — which do not scale to an unbounded
  aggregate population, and are out of scope.
- **Requires consumer cooperation.** The merge only happens if downstream code threads
  the `CausalContext`. A consumer that ignores it silently degrades to per-aggregate
  counters. `tandem-spring-producer` makes this easy (auto-populate from inbound headers) but
  cannot enforce it.
- **At-least-once is unchanged.** Consumers must still deduplicate on `(aggregate_id,
  seq)`; causal ordering does not alter delivery semantics.
