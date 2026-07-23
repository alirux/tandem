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

The `outbox` table is defined in HLD §5.1 (note the `bucket SMALLINT NOT NULL` column). The relay
adds two tables, used **only under the `LEASE` coordination mode** (§3.2) — i.e. whenever more than
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
INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq, payload, headers)
VALUES (?, ?, ?, ?, ?, ?, ?);
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

**Optional Lamport advance (only when causal ordering is enabled, §9.3).** Before the outbox
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
     then **prune** expired members (`DELETE FROM tandem_relay_member WHERE lease_until < now()`).
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
  > (The relay's local clock is used only to *pace* the heartbeat/reclaim tick, a relative interval;
  > absolute drift does not affect it. Assumes a single DB whose `now()` is coherent — a multi-primary /
  > distributed clock would reopen this.)

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
            else:           store.markForRetryOrFailed(row, err)   # §3.4.2; this aggregate stops
            freeInFlightSlot()                                 # lets the loop claim more (§3.1)
        })
# flush periodically (every N ids / few ms), not once per row:
store.markDoneBatch(drain(doneIds))            # tx2
```

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
                  AND created_at < now() - :retention
                ORDER BY id LIMIT :chunk );
```
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

## 4. Metrics

Emitted through the `TandemMetrics` port (no Micrometer dependency here). Mapping to HLD §7:
`recordLag` / `recordLagAgeSeconds` (from a periodic count of PENDING rows and the oldest `created_at`),
`incrementPublished` (on `markDoneBatch`), `recordFailed`, `incrementRetry`, `incrementLeaseExpired`
(reclaim count), `recordActiveWorkers`, `recordUncoveredBuckets` (buckets in `tandem_bucket_lease` with an
expired/NULL owner but having PENDING rows).

**Basic round = no-op metrics.** With no `tandem-micrometer` adapter wired, the `TandemMetrics` port is
the no-op default (HLD §7), so each metric's **computation is guarded on `isEnabled()`** — like the
attempt-archive guard (HLD §7.1) — and does not run. In particular the slightly heavier
`recordUncoveredBuckets` aggregation (join `tandem_bucket_lease` × per-bucket PENDING lag) is a **stub in
the basic round**; its exact query is specified together with `tandem-micrometer`. The `config.invalid`
fail-fast metric (§3.5) is the one exception — it is recorded once at startup regardless, before aborting.

---

## 5. PostgreSQL vs MySQL

| Concern | PostgreSQL | MySQL 8 |
|---|---|---|
| Bucket function | computed in Java (`BucketHash`, LLD-core §4) — engine-independent | same Java value (no DB hash function) |
| `SKIP LOCKED` | yes | yes (8.0+) |
| `RETURNING` in the claim | yes (single CTE + `RETURNING`) | not supported → **`SELECT … FOR UPDATE SKIP LOCKED LIMIT n`** (returns the row data *and* locks the rows), collect the selected ids, then **`UPDATE … WHERE id IN (:selected_ids)`** in the **same tx**. (Do *not* re-`SELECT … WHERE locked_by=:me AND status=1`: it also returns prior-cycle rows still IN_FLIGHT, causing double dispatch.) |
| Types | `JSONB`, `TIMESTAMPTZ`, `BIGINT GENERATED … IDENTITY` | `JSON`, `TIMESTAMP`/`DATETIME`, `BIGINT AUTO_INCREMENT` |
| Partial indexes | yes | not supported → full index or generated-column workaround |

---

## 6. Basic-round configuration defaults

The defaults the basic round needs (the full `TandemProperties` reference stays Q6 / `tandem-spring`):

| Setting | Default | Notes |
|---|---|---|
| `bucketCount` (B) | 256 | immutable after first deploy (B5) |
| `coordination` | `SINGLE` | `SINGLE` (one relay instance owns all buckets, no table) or `LEASE` (lease-partitioned, any number of instances); statically declared (§3.2) |
| `instanceId` | derived `host-pid-<rand>` | `LEASE` only: unique lease owner (≤ 64 chars); operator may override for stability across restarts |
| `bucketLease` | 30 s | `LEASE` only: bucket-ownership lease, renewed each `reclaimInterval`; **independent** of `rowLease` (§3.2/§3.5) |
| `workersPerInstance` | `cores × 2` | per-process worker threads |
| `pollInterval` | 100 ms | **idle backoff** when a claim returns empty; not a per-batch sleep (§3.1) |
| `batchSize` | 100 | claim batch = **per-shard in-flight concurrency window** (§3.4) |
| `rowLease` | 60 s | row IN_FLIGHT lease; **hard invariant `rowLease > delivery.timeout.ms`** (default = 2×); relay fail-fasts otherwise (§3.5) |
| backoff | base 1 s, ×2, cap ~5 min, max 10 attempts | full jitter (§3.6) |
| `retention` | 14 days | cleanup of DONE/DISCARDED (§3.7) |
| `topicSuffix` | `-topic` | LLD-kafka §5 |
| `defaultContentType` | `application/json` | LLD-kafka §3.2 |

The **baseline DDL** to create is `tandem_outbox` + its indexes (HLD §5.1) plus, for the `LEASE`
coordination mode (§3.2), `tandem_bucket_lease` + `tandem_relay_member` (§1). Optional features add
their tables only when enabled.

**Delivery (decided):** Tandem ships the baseline as a **hand-written, versioned SQL script per DB**
that the operator applies; the library does not run migrations itself. The PostgreSQL baseline is
committed at [`schema/postgres/tandem-baseline.sql`](../schema/postgres/tandem-baseline.sql) (core
`tandem_outbox` + indexes, plus the `LEASE`-mode `tandem_bucket_lease` seeded for the default
`B=256`). The MySQL script (`schema/mysql/…`) is **pending Q28** (partial-index workaround, type
mappings; the `bucket` is computed in Java so there is no DB bucket function to port). Wrapping the
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

relay.start();
// on shutdown: relay.stop();   // graceful — buckets released (LEASE), in-flight recovered by row lease
```

**Serialization in the basic round.** There is **no default `PayloadSerializer` without Spring** (a
JSON default ships in `tandem-spring`; LLD-core §2.4). So in the basic round the client **serializes
the payload to `byte[]` itself** and passes the bytes (optionally setting `contentType`, persisted to
`headers["content-type"]`, §2). The end-to-end `TandemTestContainer` test does the same — it serializes
a sample payload to bytes and asserts the CloudEvent body on the topic (LLD-test §3).

`tandem-spring` later automates exactly this wiring; nothing here requires it.

---

## 8. Open items touching this module (post basic round)

- **Q6** — full `TandemProperties` reference (`tandem-spring`); the basic-round defaults are in §6.
- **Q28** — full MySQL DDL (partial-index workaround, partitioning).
- The `tandem_bucket_lease` table doubles as / aligns with the **relay heartbeat-status** the Admin API needs
  (HLD-admin-api §4.1) — reconcile the two into one mechanism when writing `tandem-admin`.

*(Q17 — producer retriable/permanent classification — resolved in LLD-kafka §4; the verdict rides in
`OutboxDispatchException.isRetriable()`, LLD-core §3.)*
