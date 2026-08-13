# Tandem — `tandem-jdbc` LLD

**Version:** 0.1 (Draft)  
**Module:** `tandem-jdbc` · package `com.codingful.tandem.jdbc`  
**Depends on:** `tandem-core` + JDBC (`java.sql`, JDK). **No Kafka** (the relay publishes via the
`OutboxDispatcher` port, implemented by `tandem-kafka`). No metrics library (calls the
`TandemMetrics` port). Minimal client footprint (§1.3).  
**Resolves:** Q8 (bucket model — already decided), Q9 (transaction boundaries), Q10 (batch ordering
on failure), Q11 (head-of-chain / poison), Q12 (cleanup), Q13 (backoff). See [open-questions-lld.md](open-questions-lld.md).

`tandem-jdbc` is the JDBC persistence adapter: the **write-side insert** (used by the client) and
the **relay engine** (poll/publish-coordinate/mark, lease, bucket assignment, cleanup). It
implements `OutboxRepository`, `OutboxStore`, `ReplayService`.

---

## 1. Schema

The `outbox` table is defined in HLD §5.1 (note the `bucket SMALLINT NOT NULL` column). It also
carries a nullable **`correlation_id VARCHAR(255)`** column with a plain B-tree index
(`idx_tandem_outbox_correlation`), written at insert from `headers['correlation-id']` (§2) and
serving the Admin API's incident-time search (HLD-tracing §4.1). A real column rather than an
expression index over the `headers` JSONB, specifically so it ports to MySQL 8 unchanged (§5, where
neither expression nor partial indexes exist); the index is deliberately **not** partial for the same
reason, even though `WHERE correlation_id IS NOT NULL` would be smaller on PostgreSQL.

The relay adds two tables, used **only under the `LEASE` coordination mode** (§3.2) — i.e. whenever more than
one relay instance runs against the outbox, whether those instances are embedded in a horizontally-
scaled client or standalone processes. Under `SINGLE` (a single relay instance) there are no tables:
the instance owns all buckets in-process.

```sql
-- Bucket ownership under LEASE coordination (§3.2, §4.3). One row per virtual bucket.
CREATE TABLE tandem_bucket_lease (
    bucket       SMALLINT     PRIMARY KEY,   -- 0 .. B-1
    owner        VARCHAR(64),                -- worker id; NULL = free
    lease_until  TIMESTAMPTZ,                -- ownership expiry; renewed on heartbeat
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
-- Seeded with B rows (0 .. B-1) at setup.

-- Relay-instance presence, decoupled from bucket ownership (§3.2). One row per live instance, renewed
-- each heartbeat; self-registered at runtime (not seeded). Makes a zero-owned joiner visible to peers'
-- fair-share count, so an incumbent holding every bucket rebalances instead of starving the newcomer.
CREATE TABLE tandem_relay_member (
    owner        VARCHAR(64)  PRIMARY KEY,   -- matches tandem_bucket_lease.owner
    lease_until  TIMESTAMPTZ  NOT NULL,      -- presence expiry; renewed on heartbeat
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

`B` (virtual bucket count, default 256) is **immutable** after first deploy (B5): changing it
re-maps aggregates across buckets and would split an aggregate's events across workers. To change
`B`, drain all PENDING under the old `B` first, then switch.

Because `B` is split across two configured sides (the write-side `JdbcOutboxRepository` and the relay
`RelayConfig`) that are often separate processes, a divergent value would make the write-side insert
into buckets the relay never polls — silently stopping delivery. The **bucket-count guard** persists
the effective `B` in a small metadata table and fails fast on divergence, on both sides:

```sql
-- Cross-cutting metadata, keyed by name. Holds `bucket_count` (the effective B). NOT seeded here:
-- the guard seeds it on first startup with the operator's configured B, so a fresh DB with a
-- non-default B is correct without editing the DDL. A pre-guard database has the row seeded on first
-- startup under the new version (additive, backward/forward compatible — HLD §1.4).
CREATE TABLE tandem_meta (
    key         TEXT         PRIMARY KEY,
    value       TEXT         NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

Unlike the two `LEASE` tables, `tandem_meta` is part of the **core** schema (present in every
deployment, both coordination modes) since the write-side relies on it too. Its full design — the
pure `BucketCountReconciliation` strategy and `BucketCountStore` port in `tandem-core`, the
`JdbcBucketCountStore` adapter and `BucketCountGuard` orchestrator in `tandem-jdbc`, the atomic
seed-if-absent and the concurrency handling — is in [LLD-bucket-count-guard.md](LLD-bucket-count-guard.md).
Re-sharding (an intentional change of `B`) is a separate future feature, never something the guard
accepts.

**Bucket computation** (`tandem-jdbc`, at insert): computed **in Java** via the core
`BucketHash.bucketFor(aggregateId, B)` (64-bit FNV-1a + `Math.floorMod`; LLD-core §4) and bound as a
plain `SMALLINT` parameter in the INSERT. It is **engine-independent** (identical on PostgreSQL and
MySQL) and **stable across DB major-version upgrades** — no `hashtextextended`/`CRC32`, no `abs()`
overflow, and the in-memory test adapter computes the same value. The client's `OutboxMessage` never
carries `bucket`.

---

## 2. Write-side — `JdbcOutboxRepository` (implements `OutboxRepository`)

Runs inside the caller's transaction (the client's `@Transactional`); never opens its own.

```sql
INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq, payload, headers, correlation_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);
```
- `bucket` is computed in Java from `aggregate_id` (§1).
- `payload` is the `byte[]` from the `PayloadSerializer` (JSONB for JSON, BYTEA for binary; §5.2 HLD).
- **`contentType`, when set, is merged into `headers["content-type"]`** before the INSERT — the key the
  relay reads for the CloudEvents `datacontenttype` (LLD-kafka §3.2). The typed field is the **single
  source of truth**: if a `content-type` entry is also present in `headers`, the field overrides it. No
  dedicated column (Pareto). (Causation is not handled here — it belongs to the opt-in causal-ordering
  feature, LLD-core §1.3.)
- A `UNIQUE(aggregate_id, seq)` violation is translated to `DuplicateSeqException` (Q5).
- `insertAll` batches multiple messages via JDBC batch in the same transaction.

**Optional Lamport advance — RESERVED, not implemented** ([HLD-causal-ordering.md](HLD-causal-ordering.md) §0;
§3.1). No `tandem_aggregate_clock` table and no `lamport` column exist in the shipped DDL, and
`JdbcOutboxRepository` contains none of the code below. It is specified here so that building the
feature is an additive change. Were it built: before the outbox
INSERT, advance the per-aggregate clock with an atomic upsert and write the result to
`outbox.lamport` — all in the same domain transaction. The `tandem_aggregate_clock` row lock
serializes the advance regardless of the client's locking strategy:
```sql
INSERT INTO tandem_aggregate_clock (aggregate_id, lamport)
VALUES (?, GREATEST(0, ?) + 1)
ON CONFLICT (aggregate_id) DO UPDATE SET lamport = GREATEST(tandem_aggregate_clock.lamport, ?) + 1
RETURNING lamport;
```
`:inbound` is the `CausalContext` inbound timestamp (0 if none). When the feature is **off**,
this step is skipped entirely (no `tandem_aggregate_clock` table, no upsert) — zero cost (§1.3).

**Optional trace/correlation capture (only when tracing is enabled, HLD-tracing.md §5, §9).** After
the `contentType` merge, `insert`/`insertAll` call `TracePropagator.capture()` and merge the result
into `headers` for any key **not already present** — an explicit header the caller set (e.g. a
hand-set `correlation-id`) is never overwritten by captured context. Guarded by
`TracePropagator.isEnabled()`: when disabled (the default `TracePropagator.NOOP`, wired by the 2-arg
constructor), no context lookup happens and no map is built (§7 cost table). A manually-assembled
repository passes a real propagator to the 3-arg constructor; real adapters ship in
`tandem-spring-producer` / `tandem-tracing-otel`.

**The `correlation_id` column is filled from the headers just computed**, never from a separate
source — so the indexed copy and the header that reaches Kafka can never disagree (HLD-tracing §4.1).
The value is **truncated** to `MAX_CORRELATION_ID_LENGTH` (255, the column width) rather than
rejected: it normally originates outside the application (an inbound HTTP header, a consumed
message), so it is untrusted input, and an over-long value must not fail the caller's *business*
transaction — the insert shares it. The `headers` copy keeps the full value; it is not indexed. The
column stays `NULL` when no correlation id is present, which is every row when tracing is off.

---

## 3. Relay engine

### 3.1 Worker model & lifecycle

- A **WorkerPool** of `workersPerInstance` threads (default `cores × 2`). Each worker owns a subset
  of buckets and runs the poll loop. The loop **claims and dispatches back-to-back while work
  remains**, re-claiming as in-flight slots free (§3.4); `pollInterval` (default 100 ms) is the
  **idle backoff** applied *only* when a claim returns no rows — it is **not** a per-batch sleep. A
  fixed per-cycle sleep would cap a shard at `batch_size / pollInterval` (e.g. 1 000/s), an order of
  magnitude under the throughput target (HLD §10); the continuous claim-while-busy loop removes that
  ceiling.
- **The idle sleep carries ±20% jitter** (`PollBackoff`). The mean is exactly `pollInterval`, so
  discovery latency is what the operator configured (dispatch-latency.md §1); the jitter only stops
  workers that started in the same instant from polling in lockstep for the life of the relay,
  which would concentrate `instances × workersPerInstance` queries into periodic bursts. The narrow
  band is deliberate: an idle wait must never collapse towards zero and turn the loop into a spin.
- **A cycle that *throws* backs off exponentially instead**, from `pollInterval`, doubling, capped
  at `reclaimInterval` (5 s by default) and reset by the first cycle that completes. This is a
  per-worker counter held in the loop's own frame — never shared state. Under a database outage the
  fixed `pollInterval` retry meant every worker re-querying and logging a stack trace ten times a
  second, turning an outage into a log flood on top of itself; recovery stays bounded by
  `reclaimInterval`, the cadence the relay already uses for its own coordination work, so this
  introduces **no new configuration knob** (HLD §1.1). The cap is a hard ceiling — jitter is applied
  before the clamp, so a saturated worker waits within 20% *below* it, never past it.
- **Supervised threads (coverage must not be silently lost).** Each worker's poll loop runs inside a
  `try/catch` that never lets an uncaught exception kill the thread silently: a per-iteration error is
  logged and the loop continues (after a short idle backoff); a fatal/unexpected death **restarts the
  worker thread** so its buckets are not abandoned. This matters most under the **`SINGLE`** coordination
  mode (§3.2), which has no `tandem_bucket_lease` table to self-heal — a worker thread that died without
  restart would leave its buckets permanently uncovered, visible only as rising `lag.age_seconds`. (Under
  **`LEASE`** a dead *instance* still self-heals via lease expiry, §3.2; the `bucket.uncovered` metric is
  derived from `tandem_bucket_lease` and so reports coverage only under `LEASE` — under `SINGLE`,
  supervised restart is what keeps coverage, and lag age is the backstop signal. Note the two are
  independent recovery layers: within an instance, supervised restart covers a dead *thread*; across
  instances, `LEASE` covers a dead *instance*.) If a worker cannot be restarted, the pool escalates by
  failing the process rather than running with a coverage gap.
- **Graceful shutdown:** stop polling, let in-flight publishes finish (or let their row lease expire),
  release owned buckets (`LEASE` mode; no-op under `SINGLE`), then close. In-flight rows left
  `IN_FLIGHT` are recovered by the lease reclaim (§3.5) — no event is lost.
- **Connections:** plain pooled connections (HikariCP or the app's `DataSource`). **No dedicated/affined
  connection** is required (the bucket-lease design removed the advisory-lock connection-affinity of the
  rejected claim model).
- **Admin-API pause (HLD-admin-api §4.1) is a cached read, never a hot-path query.** A `RelayControlSource`
  refreshes the desired whole-relay/per-bucket pause state on the same maintenance cadence as
  `heartbeatTick`/`reclaimTick` (`reclaimInterval`, 5 s by default); `sliceFor()` consults the cached
  result on every claim, so pause support costs nothing beyond that one periodic query — the claim loop
  itself (§3.1, continuous while busy) never touches the database for it. `RelayControlSource.NOOP`
  keeps this a no-op where no `DataSource` is available (the basic-round convenience constructor).
- **The same tick also heartbeats, for `RelayStatus.state == DOWN` (HLD-admin-api §4.1).**
  `controlTick` calls `RelayControlSource.heartbeat()` right after `refresh()` — one more UPDATE of
  a single already-written row (`tandem_meta.coordination.updated_at`), same cadence, same
  connection cost class. Unlike `refresh()`'s pause state, this is unconditional on coordination
  mode: `SINGLE` heartbeats exactly like `LEASE` does, closing the gap where only `LEASE`'s
  `tandem_relay_member` gave an admin any liveness signal at all. `JdbcRelayControlSource` also
  publishes its own cadence once at `onStart()` (`relay_heartbeat_interval_seconds`), so the admin
  can compute a staleness threshold instead of guessing one.

### 3.2 Bucket assignment — the coordination mode

Bucket ownership is chosen by the **coordination mode** (`RelayConfig.coordination`, HLD §3.2 axis 2),
a **statically declared** option — `SINGLE` (default) or `LEASE`. It is orthogonal to *where* the relay
runs: `LEASE` is used both by a horizontally-scaled client with an embedded relay and by standalone
relay processes. A `BucketSource` abstracts the two so the `WorkerPool` is mode-agnostic:

```java
interface BucketSource {
    Set<Integer> ownedBuckets();          // this instance's currently-owned buckets
    default void heartbeat() {}            // renew/reconcile leases (no-op under SINGLE)
    default void release() {}              // release on shutdown (no-op under SINGLE)
    default OptionalInt uncoveredBuckets() { return OptionalInt.empty(); }  // bucket.uncovered (§4);
                                                                             // empty under SINGLE
}
```

- **`SINGLE` (single relay instance):** `BucketSource.embedded(B)` returns **all** `B` buckets; the
  `WorkerPool` splits them across its worker threads (`bucket % workerCount`). No table, no coordination,
  all-or-nothing coverage (process liveness). `heartbeat`/`release` are no-ops. **Correct only when
  exactly one relay instance runs against the outbox** — running several `SINGLE` instances does not
  corrupt data (ordering + single-claim are row-carried, §3.3) but makes every instance poll every
  bucket, so use `LEASE` instead whenever more than one instance runs.
- **`LEASE` (any number of instances):** `BucketLeaseManager` (a `BucketSource` backed by a
  `DataSource`) partitions the `B` buckets across live instances via the `tandem_bucket_lease` table.
  Each instance, on a heartbeat tick (`reclaimInterval`):
  1. **Register presence:** upsert its liveness into `tandem_relay_member`
     (`INSERT ... (owner, lease_until) VALUES (:me, now() + :lease) ON CONFLICT (owner) DO UPDATE SET lease_until = ...`),
     then **prune** expired members (`DELETE FROM tandem_relay_member WHERE lease_until < now() RETURNING owner`),
     logging the pruned owner id(s) at `WARNING` — the earliest point a survivor's heartbeat can tell a
     peer is gone, ahead of its buckets actually being reclaimed (step 4).
  2. **Renew** its owned buckets: `UPDATE tandem_bucket_lease SET lease_until = now() + :lease, updated_at = now() WHERE owner = :me;`
  3. **Compute fair share** `target = ceil(B / live_members)`, where `live_members` = `count(*)` of
     `tandem_relay_member` rows with `lease_until > now()` (includes self, just registered).
  4. **Reconcile:** if it owns more than `target`, **release** the excess
     (`UPDATE tandem_bucket_lease SET owner = NULL, lease_until = NULL WHERE bucket = ANY(:excess) AND owner = :me`);
     if fewer, **claim** free/expired buckets up to `target`:
     ```sql
     UPDATE tandem_bucket_lease
        SET owner = :me, lease_until = now() + :lease, updated_at = now()
      WHERE bucket IN ( SELECT bucket FROM tandem_bucket_lease
                         WHERE owner IS NULL OR lease_until < now()
                         ORDER BY bucket LIMIT :deficit
                         FOR UPDATE SKIP LOCKED );
     ```
  Within an instance, its owned buckets are still split across worker threads (`bucket % workerCount`).
  This decentralized greedy + lease converges to a fair, self-healing assignment with no central
  coordinator and no rebalance protocol. A dead instance's leases expire → its buckets are reclaimed;
  its presence row expires too and is pruned. On graceful shutdown an instance releases its buckets
  **and** deletes its presence row, so peers rebalance immediately rather than waiting for expiry.
  Ownership is queryable (the Admin API reads `tandem_bucket_lease` for relay status / `bucket.uncovered`).
  **Every actual ownership change logs at `INFO`** — a claim, an excess release, or a full release on
  shutdown — naming which buckets moved (`UPDATE ... RETURNING bucket`), not just a count; a single-instance
  startup claiming hundreds of buckets at once is logged as a count only (`buckets:` is added only up to
  20 entries) so one coordination event does not become one unreadable line. A heartbeat tick that changes
  nothing (`owned == target`) logs nothing — this is a low-frequency coordination-event log, not a
  per-cycle one (AGENTS.md logging §4). This is what lets an operator see, from the log alone, that a
  bucket the Admin API force-released (`RelayAdminService`, HLD-admin-api §3) was picked back up by a
  live instance on its next heartbeat. The `WARNING` presence-prune log (step 1) and the `INFO` claim log
  (step 4) together tell the causal story of a crash — peer died, buckets freed, buckets claimed — as two
  distinct, correlatable lines rather than a claim with no visible cause.

  > **Why presence is decoupled from ownership (the fair-share divisor counts `tandem_relay_member`,
  > not bucket owners).** If `live` were derived from bucket ownership, an instance that currently owns
  > **zero** buckets would have no `tandem_bucket_lease` row and be **invisible** to peers. An incumbent
  > holding all `B` buckets would then always compute `live = 1`, never release, while a newcomer's claim
  > finds every bucket under a valid, continuously-renewed lease and matches nothing — a **stable
  > scale-up starvation** (not a transient race; only the incumbent crashing/restarting breaks it). This
  > was surfaced by the S8 load test (LLD-benchmark §8.2). Counting **members** makes a zero-owned joiner
  > visible, so the incumbent sees `live = 2`, releases its excess, and the fleet rebalances to `B/2` each.
  > `SINGLE` is unaffected (no tables, no heartbeat). The transient during a rebalance — released-not-yet-
  > reclaimed buckets briefly free — is a short lag blip, never loss or reorder (row-carried, §3.3).

  **`LEASE` prerequisites (relay startup fail-fast, §3.5):**
  - The `tandem_bucket_lease` table exists and is **seeded with `B` rows**, and `tandem_relay_member`
    exists (baseline DDL, §1/§6). The relay verifies the lease-table row-count `= bucketCount` and probes
    the member table at startup, fail-fasting otherwise (a common misconfiguration is enabling `LEASE`
    against a DB where only `tandem_outbox` was applied).
  - A **unique `instanceId`** per instance (the lease `owner`, ≤ 64 chars). If unset it is derived as
    `host-pid-<rand>` (stable for the process lifetime); an accidental duplicate would let two instances
    share leases — still safe (row-carried exclusivity) but defeats partitioning, so uniqueness is a
    documented operator invariant.
  - **`bucketCount` identical across all instances** — it is baked into every row's `bucket` (B5) and
    into the lease table's seeded row set; a mismatch is an operator error.
  - **`bucketLease` duration** (the ownership lease, default 30s) is **independent** of `rowLease` (the
    per-row IN_FLIGHT lease, §3.5) — do not conflate them.

  > A brief membership-change window can leave two instances transiently believing they own a bucket.
  > This is **safe** — head-of-chain + `FOR UPDATE SKIP LOCKED` (§3.3) prevent concurrent processing of
  > the same aggregate; the worst case is a duplicate, absorbed by consumer dedup (q8-worker-model-decision §4.2).
  > The full reasoning — the slow/paused-then-woken relay ("slot-handoff split-brain") and why no bucket
  > **fencing token** is needed (exclusivity is *structural* by partitioning, not lock-carried) — is in
  > q8-worker-model-decision §4.2 (case B3) and E4.

  > **The DB is the only time authority — clock drift between relays does not matter.** Every lease
  > timestamp is both *stamped* and *compared* with the DB's `now()` — `lease_until = now() + :lease`
  > and `... < now()` above, `locked_until`/`next_attempt_at` likewise (§3.4/§3.5). A relay never
  > compares a lease against its own wall clock, so drift between relay hosts cannot make one believe it
  > still owns an expired bucket (or that a live peer's is free). **Invariant: never gate ownership or
  > expiry on the relay-local clock — all lease deadlines and comparisons must go through the DB `now()`.**
  > This holds for `next_attempt_at` too: the relay computes only the *relative* backoff and the store
  > anchors it (`markForRetry(id, error, retryDelay)` → `now() + retryDelay`), mirroring how `claimBatch`
  > passes `rowLease`. `RelayWorker` deliberately holds **no `Clock`**, so a locally-anchored deadline
  > cannot be reintroduced by accident.
  >
  > *Documented exception:* cleanup's retention cutoff (§3.7) **is** computed on the relay
  > (`clock.instant() - retention`, passed as an absolute `doneBefore`). It is deliberate and benign —
  > the window is days, so a drift of seconds only shifts when terminal rows are deleted, never what is
  > delivered. The tick cadence itself uses `scheduleWithFixedDelay` (a monotonic elapsed-time interval,
  > not wall clock), so drift does not affect it either.
  >
  > Assumes a single DB whose `now()` is coherent — a multi-primary / distributed clock would reopen this.

  > **Cleanup and lease-reclaim are NOT partitioned by this mechanism.** Both run globally on every
  > instance regardless of `coordination` mode — see §3.7's note on redundant-but-safe multi-instance
  > cleanup.

### 3.3 Poll & claim (`OutboxStore.claimBatch`) — Q9 tx1, Q11, E2

One transaction: select the **head of each aggregate's pending chain** in the worker's buckets and
mark it `IN_FLIGHT`. The head-of-chain predicate subsumes the poison gate and prevents backoff
leapfrog (E2).

```sql
-- tx1 (claim): runs in its own short transaction, then COMMIT
WITH claimed AS (
  SELECT o.id
    FROM tandem_outbox o
   WHERE o.bucket IN (:my_buckets)
     AND o.status = 0                                   -- PENDING
     AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= now())
     AND NOT EXISTS (                                   -- only the earliest unfinished row per aggregate
         SELECT 1 FROM tandem_outbox e
          WHERE e.aggregate_id = o.aggregate_id
            AND e.id < o.id
            AND e.status IN (0, 1, 3) )                 -- PENDING / IN_FLIGHT / FAILED
   ORDER BY o.id
   FOR UPDATE SKIP LOCKED
   LIMIT :batch_size
)
UPDATE tandem_outbox o
   SET status = 1, locked_by = :me, locked_until = now() + :row_lease
  FROM claimed c
 WHERE o.id = c.id
RETURNING o.*;                                          -- the claimed OutboxRecords
```
- `FOR UPDATE SKIP LOCKED` guards the brief membership-change window (§3.2).
- The `idx_tandem_outbox_dispatch (bucket, id) WHERE status=0` index drives the scan; the `NOT EXISTS` uses
  `idx_tandem_outbox_aggregate (aggregate_id, id) WHERE status IN (0,1,3)`.
- **Each claimed row is the head of a _distinct_ aggregate** — the `NOT EXISTS` excludes any aggregate
  that has an earlier unfinished row, so a batch is up to `batch_size` distinct aggregates. These are
  independent and are dispatched with **overlapping in-flight sends** in §3.4; `batch_size` is therefore
  the per-shard concurrency window, not just a fetch size.
- **Commit immediately** — the row lock is held only for this short tx; exclusivity during publish is
  carried by `status = IN_FLIGHT` + the `locked_until` lease, **not** an open transaction (Q9).

### 3.4 Publish & mark (Q9 tx2, Q10/E6) — driven by the relay loop

Because the claim (§3.3) returns the **head of each aggregate** — at most one row per aggregate — a
batch is a set of **independent rows of distinct aggregates**. The relay **dispatches the whole batch
with overlapping in-flight sends** on the single async producer (`dispatch` returns a future,
LLD-kafka §2), up to `batch_size` in flight. It does **not** publish one row at a time:

```
doneIds = concurrent collector
for row in batch:                              # distinct aggregates → independent
    dispatcher.dispatch(row)                   # async; future completes on ack (LLD-kafka §2)
        .whenComplete((ok, err) -> {
            if err == null: doneIds.add(row.id)               # success → DONE (batched, §3.4.1)
                            metrics.recordPublishLatency(now() - row.createdAt())   # §4, HLD §7
            else:           store.markForRetryOrFailed(row, err)   # §3.4.2; this aggregate stops
            freeInFlightSlot()                                 # lets the loop claim more (§3.1)
        })
# flush periodically (every N ids / few ms), not once per row:
store.markDoneBatch(drain(doneIds))            # tx2
```

The latency sample is taken **on the completion handler, at the ack instant** — `RelayWorker` carries
a `Clock` for exactly this (and nothing else: retry backoff still stays relative, anchored on the DB
clock, §3.2/§3.6, never on this one). `now()` is the *relay's* clock, `row.createdAt()` is the
*database's*, so the reading also carries a `created_at`-at-`INSERT`-not-`COMMIT` bias and a DB/relay
clock-skew offset — both documented as caveats on the port method, not hidden (§4).

Per-aggregate ordering and the poison gate hold **structurally**, not via an inner await-loop:

- **One row in flight per aggregate.** The head-of-chain claim already guarantees a batch holds at
  most one row per aggregate; the successor `seq(N+1)` only *becomes* a claimable head once `seq(N)`
  is `DONE`. So `seq(N+1)` is never even claimed — let alone sent — before `seq(N)`'s ack (E6). No
  per-record await is needed in the relay.
- **Stop on failure (E2/Q10).** A failed row goes to retry/`FAILED` (§3.4.2) and stays non-DONE, so
  the aggregate's later rows remain blocked by the head-of-chain predicate next cycle — the aggregate
  stops with no explicit `break`, and the worker keeps its bucket ownership throughout.

**3.4.1 `markDoneBatch` (Q9 tx2 — acked ids across aggregates):**
```sql
UPDATE tandem_outbox SET status = 2 WHERE id = ANY(:done_ids);   -- DONE
```
`:done_ids` accumulates **acked ids across different aggregates** (mark-DONE is order-independent), so
batching is safe and cuts DB round-trips at high rates. A crash before the flush re-publishes those
rows (duplicate, dedup'd) — never a reorder.

**3.4.2 `markForRetry` / `markFailed`:**
```sql
-- retry (attempts < max): back to PENDING with backoff
UPDATE tandem_outbox
   SET status = 0, attempts = attempts + 1, last_error = :err,
       next_attempt_at = now() + :backoff, locked_by = NULL, locked_until = NULL
 WHERE id = :id;
-- exhausted: FAILED (blocks the aggregate via head-of-chain until replay/discard)
UPDATE tandem_outbox
   SET status = 3, attempts = attempts + 1, last_error = :err, locked_by = NULL, locked_until = NULL
 WHERE id = :id;
```
The classification *retriable vs non-retriable* of the dispatch error is a `tandem-kafka` concern (Q17);
`tandem-jdbc` just records the outcome.

### 3.5 Lease reclaim (`reclaimExpiredLeases`) — failover

A periodic job (every ~5 s) resets `IN_FLIGHT` rows whose lease expired (worker crash) and
**counts the reclaim as an attempt** so a row that repeatedly kills its worker cannot loop forever:
```sql
UPDATE tandem_outbox
   SET attempts = attempts + 1,
       last_error = 'lease expired (worker crash or stall) before ack',
       status = CASE WHEN attempts + 1 >= :max_attempts THEN 3   -- FAILED (quarantine)
                     ELSE 0 END,                                  -- else back to PENDING
       locked_by = NULL, locked_until = NULL
 WHERE status = 1 AND locked_until < now();
```
This runs every `reclaimInterval` (~5s) on every instance, so it is backed by the partial index
`idx_tandem_outbox_inflight (locked_until) WHERE status = 1` — tiny (IN_FLIGHT rows are few and
transient) so the reclaim scans expired leases, not the whole table once `DONE` rows accumulate between
cleanup passes.

The reclaimed rows are re-polled (duplicate-safe). **Why increment `attempts`:** a dispatch failure
already bumps `attempts` (§3.4.2), but a worker that dies *before* the send completes (e.g. OOM on a
pathological row) leaves the row `IN_FLIGHT` without ever recording a failure. Counting each reclaim
as an attempt routes such a **crash-poison** row to `FAILED` at `maxAttempts` instead of looping
indefinitely. This is safe because the hard invariant `rowLease > delivery.timeout.ms` (below) means a
lease cannot expire while a send is merely *slow* — an expired lease genuinely indicates a dead/stalled
worker, not a healthy in-flight publish.

**Lease vs producer timeout — a hard invariant.** A row stays `IN_FLIGHT` for the whole publish, so its
lease must outlast the producer's own retry window; otherwise the reclaim resets a row whose send is
still in progress and another worker re-publishes it (duplicates). With the async model an aggregate has
exactly **one row in flight**, so its max publish time is the producer's `delivery.timeout.ms`. Hence:

```
rowLease > delivery.timeout.ms        (defaults 60 s > 30 s; recommended rowLease ≥ 2 × delivery.timeout.ms)
```

**Fail-fast at relay startup (enforced, not just documented).** Before the WorkerPool starts, the relay
reads the effective producer `delivery.timeout.ms` and validates the invariant. If
`rowLease ≤ delivery.timeout.ms` it **aborts startup**, and **all three diagnostics carry the formula and
the offending values** (consistent with the producer-config fail-fast, LLD-kafka §1). One canonical
message string is reused verbatim by the exception and the log:

> Unsafe relay config: rowLease (={rowLeaseMs} ms) must be > Kafka producer delivery.timeout.ms
> (={deliveryTimeoutMs} ms). When rowLease ≤ delivery.timeout.ms, a row's lease can expire while its
> publish is still in progress, so lease-reclaim resets it to PENDING and another worker re-publishes it
> → duplicate events. Required: **rowLease > delivery.timeout.ms** (recommended rowLease ≥ 2 ×
> delivery.timeout.ms = {recommendedMinRowLeaseMs} ms). Fix: raise `tandem.relay.row-lease` above
> delivery.timeout.ms, or lower the producer `delivery.timeout.ms` below rowLease.

- **Exception** — `TandemConfigurationException` with the canonical message above (placeholders filled).
- **Log** — one `ERROR` line with the same text, plus structured fields `rowLeaseMs`,
  `deliveryTimeoutMs`, `recommendedMinRowLeaseMs` (= `2 × deliveryTimeoutMs`) so it is greppable/alertable.
- **Metric** — `metrics.recordConfigInvalid("row_lease_not_above_delivery_timeout")` (→ gauge
  `tandem.relay.config.invalid{check="row_lease_not_above_delivery_timeout"} = 1`), emitted **before** the
  throw. It is best-effort because startup aborts (push registries capture it; a scrape registry may miss
  it) — the exception and log are authoritative; the metric's documented meaning is the same
  `rowLease > delivery.timeout.ms` rule (HLD §7).

### 3.6 Backoff (`BackoffStrategy`) — Q13

Default: **exponential with full jitter**, `delay = random(0, min(cap, base * 2^attempts))`, `base = 1 s`,
`cap` large (e.g. 5 min), `maxAttempts = 10`. Pluggable. Computed in Java; `next_attempt_at = now() +
delay` uses the **DB clock** consistently.

### 3.7 Cleanup (`cleanup`) — Q12

Default: periodic **batch `DELETE`** of terminal rows older than the retention window (default 14 days),
in chunks to avoid long locks:
```sql
DELETE FROM tandem_outbox
 WHERE id IN ( SELECT id FROM tandem_outbox
                WHERE status IN (2, 4)              -- DONE / DISCARDED
                  AND created_at < :doneBefore        -- relay-computed cutoff, see below
                ORDER BY id LIMIT :chunk );
```
Unlike every lease deadline (§3.2), the cutoff is the one timestamp computed on the **relay**
(`clock.instant() - retention`, hence the injectable `Clock` on `WorkerPool`) rather than with the DB's
`now()`. This is deliberate: it keeps the retention window testable with a controllable clock, and a
relay-to-DB clock offset only shifts *when* terminal rows are deleted — over a window of days, never
what gets delivered. Do not copy this pattern for anything that gates delivery.
**Time-partitioning** on `created_at` (drop old partitions) is an opt-in alternative for high volume
(instant drop, no bloat) at the cost of partition-management setup.

**Not bucket-scoped or coordinated across instances.** Unlike claim (§3.3, bucket-partitioned) or lease
renewal (§3.2, `LEASE`-partitioned), `cleanup` runs against the **whole** `tandem_outbox` table using the
same global predicate, and every `WorkerPool` instance schedules its own `cleanupTick` independently —
regardless of `coordination` mode (`SINGLE` or `LEASE`). With N instances, all N run the same `DELETE`
on the same candidate window every `cleanupInterval`. This is **safe**: the statement deletes by `id`, so
a second instance's `DELETE` on ids another instance already removed simply affects zero rows (no error,
no double-delete). It is **redundant** (N instances doing the same scan/delete instead of one), currently
accepted rather than fixed — same pattern as `reclaimExpiredLeases` (§3.5), which is also global and
per-instance. **Possible optimization (not implemented, left for later):** either (a) scope cleanup to
each instance's currently-owned buckets (`bucket_id` predicate via `BucketSource.ownedBuckets()`, cheap
under `LEASE` since ownership already partitions the fleet, no-op benefit under `SINGLE`), or (b) elect a
single cleanup runner — e.g. the `LEASE` member with the lowest `owner` id, or a dedicated
`tandem_relay_lock`-style advisory lock — so only one instance deletes per tick. (a) is simpler and reuses
existing partitioning; (b) fully eliminates the redundant scan but adds a new coordination primitive for a
job that is cheap and infrequent (15 min default) — likely not worth it unless the redundant `DELETE` scan
itself becomes measurably expensive at scale.

---

### 3.8 In-process status (`WorkerPool.status()`)

A read-only snapshot of the pool's own state — `RelayStatus`: `instanceId`, lifecycle `state`
(`STOPPED`/`RUNNING`/`STOPPING`), `coordination`, `workersConfigured`/`workersAlive`, and
`oldestWorkerCycle` (the least-recently-progressed live worker's last completed claim cycle). What an
embedding application's own readiness/liveness probe reads — Tandem draws no health verdict from it
(no threshold, no DOWN/UP), the same reasoning as shipping no logging configuration (AGENTS.md's
Logging section) or metrics export config: deciding what an acceptable worker deficit or cycle age is
stays the application's call.

**Never queries the database.** This is the contract, not an incidental property — it is what makes
`status()` safe to call at probe frequency (every few seconds, on every instance) at zero cost.
Backlog size/age, failed and blocked counts are deliberately absent for the same reason they are not
folded in here: they are already reachable through `OutboxStore.lag()`/`failedCount()`/`blockedCount()`,
where the caller controls query cadence and caching. Bucket ownership and coverage are absent because
under `LEASE` both are lease-table queries (`BucketLeaseManager.ownedBuckets()`/`uncoveredBuckets()`,
§3.2) — reading them at probe frequency would turn a liveness check into load on `tandem_bucket_lease`.

**`oldestWorkerCycle`, not just `workersAlive`.** A live `Thread` is not a working one — a worker
blocked inside a JDBC call that never returns stays `Thread.isAlive() == true` forever, which
`workersAlive` alone cannot distinguish from a healthy idle worker. Each worker stamps a per-index
`AtomicLongArray` entry with the current instant immediately after a claim-cycle **completes without
throwing** (`WorkerPool.runWorker`) — claiming zero rows still counts as progress (an idle relay is a
working relay); an iteration that throws before completing does not advance the stamp, so a worker
stuck failing every cycle shows an ageing timestamp instead of a fresh one. Starting a worker thread
stamps its entry immediately, before the thread runs its first cycle, so a worker that hangs on its
very first claim reports an ageing timestamp from start time rather than an absent one.
`workersConfigured` vs. `workersAlive` is the other half of the picture: the gap matters more than
either absolute number, since a died thread is restarted automatically (§3.1) and a transient deficit
during that restart is normal — a persistent one is a worker dying in a loop.

---

## 4. Metrics

Emitted through the `TandemMetrics` port (no Micrometer dependency here). Mapping to HLD §7:
`recordLag` / `recordLagAgeSeconds` (both from `OutboxStore.lag()` — a **single** query returning the
count of PENDING rows and the oldest `created_at`, so the two gauges always describe the same instant;
read every `metricsInterval`, default 10 s), `recordFailed` (from `OutboxStore.failedCount()`, the same
cadence — a **live** count of `FAILED` rows, not a tally of failure events: a row can leave `FAILED`
via the admin `DISCARDED` transition, and only a fresh read reflects that; an event-driven counter
would never go back down, permanently misreporting a resolved incident as still open),
`recordBlocked` (from `OutboxStore.blockedCount()`, same cadence — `PENDING` rows sitting behind a
`FAILED` row of their aggregate, which the head-of-chain rule (§3.3) makes permanently unclaimable
until an operator resolves the head. **Not subtracted from `lag()`**: they are undelivered events, and
a backlog gauge that hid them would read healthy while an aggregate is entirely stalled. Reported
separately because without it a single terminal failure makes `lag.age_seconds` climb for ever and
latches any alert built on it — see HLD §7. The query is driven from the `FAILED` rows, grouped by
aggregate, then range-scanned forward on `idx_tandem_outbox_aggregate`, so its cost tracks the number
of blocked rows rather than the size of the outbox; `idx_tandem_outbox_failed` was added for the
`status = 3` step it shares with `failedCount()`, which until then seq-scanned the whole table on every
tick),
`incrementPublished` (on `markDoneBatch`), `recordPublishLatency` (§3.4 — one sample per successfully
published row, computed on `RelayWorker`'s completion handler at the ack instant, never on the
`markDoneBatch` cadence; unlike every gauge above it needs no DB round trip of its own, since both ends
of the measurement — `row.createdAt()` and the ack — are already in hand), `incrementRetry`, `incrementLeaseExpired`
(reclaim count), `recordActiveWorkers` and `recordWorkerCycleAgeSeconds` (both from one `WorkerPool.status()`
call, §3.8 — an in-process, database-free reading, unlike every other metric in this list; the second
is what separates a worker merely alive from one making progress, since `Thread.isAlive()` cannot),
`recordUncoveredBuckets` (from `BucketSource.uncoveredBuckets()`,
same cadence as `lag()`/`failedCount()` — a bucket that is free/expired in `tandem_bucket_lease` **and**
has `PENDING` rows waiting; the `EXISTS` against `tandem_outbox` rides the existing
`idx_tandem_outbox_dispatch` partial index, so the query scans only `tandem_bucket_lease`'s `B` rows, no
new index needed. Empty under `SINGLE` — there is no lease table to stall; `BucketLeaseManager` is the
only implementation).

**No adapter = no cost.** With no `tandem-micrometer` adapter wired, the `TandemMetrics` port is
the no-op default (HLD §7), so each metric's **computation is guarded on `isEnabled()`** — the lag,
failed-count, blocked-count, and uncovered-bucket readings go further and are **never scheduled at
all**, so their queries never run. The `config.invalid` fail-fast metric
(§3.5) is the one exception — it is recorded once at startup regardless, before aborting.

---

## 5. PostgreSQL vs MySQL

The MySQL port is **not built** (Q28). This section is the specification it must follow. Everything
below was **verified by experiment against MySQL 8.4.11 and `postgres:16-alpine`**, not derived from
documentation — an earlier draft of this section claimed the claim strategy was "already portable",
and §5.2 is the measurement that disproved it.

### 5.1 What actually differs

| Concern | PostgreSQL | MySQL 8 |
|---|---|---|
| Bucket function | computed in Java (`BucketHash`, LLD-core §4) — engine-independent | same Java value (no DB hash function) |
| `SKIP LOCKED` | yes | yes (8.0+), but see §5.2 — the locking clause alone does not carry the semantics over |
| Transaction isolation | `READ COMMITTED` (engine default) | `REPEATABLE READ` (engine default) — **must be overridden**, §5.2 |
| `RETURNING` | yes (single CTE + `RETURNING`) | not supported → two-step claim, §5.3 |
| Array bind (`= ANY(?)`) | yes (`Connection.createArrayOf`) | not supported by Connector/J → generated `IN (?,?,…)` |
| `LIMIT` inside `IN (subquery)` | yes | `ERROR 1235` → wrap in a derived table |
| Subquery over the table being updated | yes | `ERROR 1093` → same derived-table wrapping |
| Locking-clause order | `… FOR UPDATE SKIP LOCKED LIMIT n` | **reversed**: `… LIMIT n FOR UPDATE SKIP LOCKED`; the PostgreSQL order is a syntax error |
| Unique-violation detection | `SQLSTATE 23505` | `SQLSTATE 23000` + **vendor errno 1062** (23000 alone is any integrity violation) |
| `key` as a column name | legal unquoted | **reserved word** → backticks, or the statement fails to parse |
| Types | `JSONB`, `TIMESTAMPTZ`, `BIGINT GENERATED … IDENTITY` | `JSON`, `DATETIME(3)`/`TIMESTAMP(3)`, `BIGINT AUTO_INCREMENT` |
| Sub-second clock | `now()` | `now()` is **second-precision** — every lease expression needs `now(3)` |
| Partial indexes | yes | not supported → full index with `status` as the leading column |
| JSON round-trip | `jsonb`, keys normalised | `JSON`, keys normalised — payload/headers port unchanged |

### 5.2 The claim: isolation level is load-bearing

**Tandem has always run at `READ COMMITTED`.** It never had to say so, because that is PostgreSQL's
default and the code never sets a level. MySQL defaults to `REPEATABLE READ`, and under that default
the relay **scales negatively**.

The head-of-chain claim (§3.3) makes the optimiser choose **`PRIMARY`** as the access path — not
`idx_tandem_outbox_dispatch` — in order to satisfy `ORDER BY id LIMIT n`. Under `REPEATABLE READ`
every row *examined* takes a next-key lock, whether or not it matches the `WHERE`. Measured with
one worker of four claiming a batch of 10 from buckets `{0,4}`, over 200 000 rows across 400
aggregates and B = 8:

| Isolation | Rows returned | Locks taken | Spread |
|---|---|---|---|
| `REPEATABLE READ` | 10 | **37** | **all 8 buckets** — 27 of them on rows owned by other workers |
| `READ COMMITTED` | 10 | **10** | buckets 0 and 4 only |

So under the default, bucket granularity — the property the entire worker/instance isolation rests
on — **disappears**. Throughput, same workload, 15 s per run, batch 10:

| Configuration | 1 worker | 4 workers | Scaling | Mean claim latency, 1 → 4 |
|---|---|---|---|---|
| MySQL, `REPEATABLE READ` | 345 rows/s | **216 rows/s** | **0.63× — negative** | 25.9 ms → 138.8 ms (peak 1.79 s) |
| MySQL, `READ COMMITTED` | 307 rows/s | **1 073 rows/s** | **3.49×** (87%) | 29.5 ms → 32.8 ms |
| PostgreSQL (the committed CTE claim) | 693 rows/s | **2 467 rows/s** | 3.56× (89%) | 13.8 ms → 15.2 ms |

Four workers under `REPEATABLE READ` are **37% slower than one**. `READ COMMITTED` restores
PostgreSQL-class scaling.

**The failure mode is silent.** No deadlock, no lock-wait timeout, and effectively no empty batches
(12 out of 353 cycles) — the loss is pure lock *waiting*. A functional integration suite passes green
while production throughput collapses, so **the MySQL test matrix must assert concurrent throughput**,
not just correctness; nothing else detects a regression of this shape.

`READ COMMITTED` is also the semantically correct level here, not merely the faster one: the
head-of-chain `NOT EXISTS` must see the latest committed state of the aggregate's chain, not a
transaction-start snapshot. Weakening the level costs nothing Tandem relies on — exclusivity during
publish is carried by `status = IN_FLIGHT` plus the `locked_until` lease, never by an open
transaction (Q9).

**Recommended mechanism (pending decision):** set the level on the relay's own claim transaction in
the adapter — `Connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED)`, restored in a
`finally`, since the `DataSource` belongs to the application and is normally pooled. Requiring the
operator to configure `transaction_isolation` server-side is the alternative, and is worse: a
deployment that misses it does not fail, it silently loses ~4× throughput (§1.1 — sensible defaults
over configuration). Either way the client write-side is untouched; only relay connections are
affected.

**The disjoint-bucket invariant is now load-bearing and must be stated as such.** Concurrent claims
never overlap: `WorkerPool.sliceFor` shards by `bucket % workerCount == index` within an instance,
and `LEASE` gives each instance exclusive bucket ownership across instances (§3.2). Under
`READ COMMITTED` that is what keeps workers off each other's rows. It was an implicit consequence of
the design before; on MySQL it is a precondition for throughput, and a change to worker sharding
that broke it would degrade silently.

### 5.3 Statement-level rewrites

The claim (§3.3) becomes two statements in **one explicit transaction** — a real change from the
PostgreSQL path, where every operation runs on autocommit (Q9): `SELECT … ORDER BY id LIMIT n FOR
UPDATE SKIP LOCKED`, collect the ids, then `UPDATE … WHERE id IN (:selected_ids)`. Do *not* re-`SELECT
… WHERE locked_by=:me AND status=1` instead: it also returns prior-cycle rows still `IN_FLIGHT`,
causing double dispatch. The locking read returns the row data *before* the update, so the adapter
synthesises the post-claim state (`status`, `locked_by`, `locked_until`) onto the returned
`OutboxRecord` rather than reading it back.

`BucketLeaseManager` (§3.2) is the other substantial rewrite: **four** of its statements depend on
`UPDATE … RETURNING bucket` (claim-deficit, release-excess, release-all, member prune), and two also
hit `ERROR 1235`/`ERROR 1093`. Each becomes a locking `SELECT` followed by an `UPDATE`/`DELETE` in the
same transaction.

Ten of the eleven SQL-carrying classes need dialect-specific statements — `JdbcDiscardService` is the
only one already portable. The `tandem_meta` upserts (`JdbcBucketCountStore`, `JdbcRelayControl`,
`JdbcRelayControlSource`) map `ON CONFLICT … DO UPDATE/DO NOTHING` to
`ON DUPLICATE KEY UPDATE` / `INSERT IGNORE`, and every reference to the `key` column needs backticks.

### 5.4 Schema mapping

The MySQL baseline (`schema/mysql/tandem-baseline.sql`, still to be written) keeps the same table and
column names — it is the same long-lived contract, evolving additively (§1.4). The four partial
indexes have no MySQL equivalent and become full indexes with **`status` as the leading column**
(`(status, bucket, id)`, `(aggregate_id, status, id)`, …). A generated-column workaround buys nothing:
MySQL indexes `NULL`s, so the "partial" index would not actually be sparser. The trade-off is real and
should be stated to operators: these indexes cover `DONE` rows too, so they grow with the whole table
between cleanup passes, unlike their PostgreSQL counterparts.

Timestamps need a decision at the schema level: `DATETIME(3)` with the session pinned to UTC, or
`TIMESTAMP(3)` with `connectionTimeZone=UTC`. `rs.getObject(col, OffsetDateTime.class)` behaves
differently across the two, and every lease is compared against the DB clock — this is the easiest
correctness trap in the port to miss.

Bucket seeding replaces `generate_series` with a recursive CTE (verified: seeds B = 256 rows).

### 5.5 Still open (Q28)

- **Adapter structure** — a `SqlDialect` seam inside `tandem-jdbc` with the engine detected from
  `DatabaseMetaData`, versus a separate module. Roughly 2 400 of the module's 3 400 lines
  (`WorkerPool`, `RelayWorker`, `BucketLeaseManager`, backoff, config) are engine-neutral
  orchestration that a second module would duplicate.
- **Test matrix** — which suites run against both engines, and where the concurrent-throughput
  assertion called for in §5.2 lives.
- **Gap locks at scale** — §5.2's measurements are single-node, B = 8, with a stored-procedure
  harness in which *neither* engine pays a JDBC round-trip. The real two-step claim pays one more
  than PostgreSQL's single CTE, so the measured MySQL/PostgreSQL throughput gap is a **lower bound**.
  Behaviour under `LEASE` with several instances is not yet measured.
- `mysql-connector-j` is GPLv2-with-FOSS-exception and must stay **test-only**, never a redistributed
  dependency (THIRD-PARTY-NOTICES stays unchanged).

---

## 6. Basic-round configuration defaults

The defaults the basic round needs (the full property reference is the `tandem.*` contract in the Spring modules, LLD-spring-config §2):

| Setting | Default | Notes |
|---|---|---|
| `bucketCount` (B) | 256 | immutable after first deploy (B5) |
| `coordination` | `SINGLE` | `SINGLE` (one relay instance owns all buckets, no table) or `LEASE` (lease-partitioned, any number of instances); statically declared (§3.2) |
| `instanceId` | derived `host-pid-<rand>` | `LEASE` only: unique lease owner (≤ 64 chars); operator may override for stability across restarts |
| `bucketLease` | 30 s | `LEASE` only: bucket-ownership lease, renewed each `reclaimInterval`; **independent** of `rowLease` (§3.2/§3.5) |
| `workersPerInstance` | `cores × 2` | per-process worker threads |
| `pollInterval` | 100 ms | **idle backoff** when a claim returns empty, ±20% jitter; not a per-batch sleep (§3.1). A cycle that *throws* backs off from here exponentially, capped at `reclaimInterval` |
| `batchSize` | 100 | claim batch = **per-shard in-flight concurrency window** (§3.4) |
| `rowLease` | 60 s | row IN_FLIGHT lease; **hard invariant `rowLease > delivery.timeout.ms`** (default = 2×); relay fail-fasts otherwise (§3.5) |
| backoff | base 1 s, ×2, cap ~5 min, max 10 attempts | full jitter (§3.6) |
| `retention` | 14 days | cleanup of DONE/DISCARDED (§3.7) |
| `metricsInterval` | 10 s | how often the lag gauges are read; the job is **only scheduled when a metrics adapter is wired** (§4) |
| `logEveryRows` | 10,000 | per-worker `INFO` progress log every N dispatch outcomes (ok + ko combined) — a row-count cadence, not a clock one, so an idle relay stays silent; unlike `metricsInterval`, always on, no adapter needed |
| `topicSuffix` | `-topic` | LLD-kafka §5 |
| `defaultContentType` | `application/json` | LLD-kafka §3.2 |

The **baseline DDL** to create is `tandem_outbox` + its indexes (HLD §5.1) plus, for the `LEASE`
coordination mode (§3.2), `tandem_bucket_lease` + `tandem_relay_member` (§1). Optional features add
their tables only when enabled.

**Delivery (decided):** Tandem ships the baseline as a **hand-written, versioned SQL script per DB**
that the operator applies; the library does not run migrations itself. The PostgreSQL baseline is
committed at [`schema/postgres/tandem-baseline.sql`](../schema/postgres/tandem-baseline.sql) (core
`tandem_outbox` + indexes, plus the `LEASE`-mode `tandem_bucket_lease` seeded for the default
`B=256`). The MySQL script (`schema/mysql/…`) is **pending Q28** — the schema mapping it must follow
(status-prefixed indexes, timestamp precision, bucket seeding) is specified in §5.4; the `bucket` is
computed in Java so there is no DB bucket function to port. Wrapping the
scripts in a migration tool (Liquibase/Flyway) is **deferred**.
The scripts must stay **additive** across versions (§1.4).

## 7. Manual wiring without Spring (basic round)

The basic round runs with **no Spring** — assemble the pieces directly:

```java
// write-side (call inside your own @Transactional):
OutboxRepository repo = new JdbcOutboxRepository(dataSource, bucketCount);
repo.insert(OutboxMessage.builder()...payload(bytes).build());   // payload is byte[] (you serialize)

// relay (embedded, or its own process):
OutboxStore       store      = new JdbcOutboxStore(dataSource, bucketCount);
TopicRouter       router     = TopicRouter.kebabWithSuffix("-topic");
OutboxDispatcher  dispatcher = new KafkaRelay(kafkaProps, router);          // tandem-kafka

// SINGLE (default — a single relay instance):
WorkerPool        relay      = new WorkerPool(store, dispatcher, cfg);      // §3.1, owns all buckets

// LEASE (multiple relay instances — embedded-multi-replica or standalone):
//   BucketSource.forCoordination picks embedded(B) for SINGLE, a BucketLeaseManager for LEASE.
BucketSource      buckets    = BucketSource.forCoordination(cfg, dataSource);   // §3.2
WorkerPool        relay      = new WorkerPool(store, dispatcher, cfg,
                                   TandemMetrics.NOOP, Clock.systemUTC(),
                                   BackoffStrategy.fullJitter(), buckets);       // full constructor

// With Admin-API pause/resume support (HLD-admin-api §4.1) — an eighth argument, RelayControlSource,
// publishes this instance's coordination mode to tandem_meta, caches the desired pause state, and
// heartbeats on the same cadence (for RelayStatus.state == DOWN) - reclaimInterval is also the
// heartbeat cadence, published once at startup so the admin can compute a staleness threshold:
RelayControlSource control    = new JdbcRelayControlSource(dataSource, cfg.coordination(), cfg.reclaimInterval());
WorkerPool        relay       = new WorkerPool(store, dispatcher, cfg,
                                   TandemMetrics.NOOP, Clock.systemUTC(),
                                   BackoffStrategy.fullJitter(), buckets, control);

relay.start();
// on shutdown: relay.stop();   // graceful — buckets released (LEASE), in-flight recovered by row lease
```

**Serialization in the basic round.** There is **no default `PayloadSerializer` without Spring** (a
JSON default ships in `tandem-spring-producer`; LLD-core §2.4). So in the basic round the client **serializes
the payload to `byte[]` itself** and passes the bytes (optionally setting `contentType`, persisted to
`headers["content-type"]`, §2). The end-to-end `TandemTestContainer` test does the same — it serializes
a sample payload to bytes and asserts the CloudEvent body on the topic (LLD-test §4).

`tandem-spring-producer` later automates exactly this wiring; nothing here requires it.

---

## 8. Open items touching this module (post basic round)

- **Q6** — full property reference (the `tandem.*` contract in `tandem-spring-producer` / `tandem-spring-relay`, LLD-spring-config §2); the basic-round defaults are in §6.
- **Q28** — the MySQL port. Specified in §5 (verified by experiment); what remains undecided is
  listed in §5.5. Note §5.2: the claim is **not** portable as-is — `READ COMMITTED` is a requirement,
  not a tuning option.
- ~~The `tandem_bucket_lease` table doubles as / aligns with the relay heartbeat-status the Admin API
  needs~~ — **done**: `JdbcRelayQuery`/`JdbcRelayControl` (`tandem-admin`) read/write the same
  `tandem_bucket_lease`/`tandem_relay_member`/`tandem_meta` tables the relay engine already
  maintains; no separate mechanism (HLD-admin-api §4.1).

*(Q17 — producer retriable/permanent classification — resolved in LLD-kafka §4; the verdict rides in
`OutboxDispatchException.isRetriable()`, LLD-core §3.)*
