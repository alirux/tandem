# Tandem — Q8 Worker Model: Decision & Analysis

**Version:** 2.0  
**Status:** Decided  
**Companion to:** HLD §4.3, §6; open-questions-lld.md (Q8)

This is the decision record for the relay's worker/sharding model. It states the decision,
the options weighed, **verifies the chosen model (virtual-bucket sharding) against the
happens-before guarantee** with a devil's-advocate pass, and keeps the analysis of the
rejected per-aggregate-claim model as the **evidence** that drove the choice.

---

## 1. Decision

**Chosen: virtual-bucket sharding** (HLD §4.3). Aggregates map to a fixed, large number of
virtual buckets `B` (e.g. 256, never changed); each bucket is owned by exactly one worker at
a time; workers own bucket subsets (in-process for embedded, a `tandem_bucket_lease` table for
standalone). Per-aggregate exclusivity is **structural** (by partitioning), not lock-based.

**Why, in one line:** for a library whose primary contract is ordering, *structural
exclusivity with a loud failure mode* beats *lock-based exclusivity with silent reorder
modes*.

---

## 2. The guarantee

**Per-aggregate happens-before.** For two events A, B on the same aggregate with
`seq(A) < seq(B)`, a consumer must observe A before B (A at an earlier offset on the
aggregate's Kafka partition, key = `aggregate_id`). Different aggregates have no ordering
requirement.

A hard requirement from the project's origin: **high throughput via a parallel relay.** This
rules out the simplest correct model (a single sequential reader) — see §3.

---

## 3. Options weighed

| Model | Exclusivity | Parallel | Ordering-correctness | Failure mode | Verdict |
|---|---|---|---|---|---|
| **Single-leader sequential** (Eventuate-style: one ordered reader + leader election) | trivial (1 reader) | **no** | trivial | — | ✗ forgoes the parallelism that is Tandem's reason to exist |
| **Virtual-bucket sharding** | **structural** (hash → bucket → 1 worker) | yes | conditions E1/E2/E6 (model-independent) | **loud** (uncovered bucket → lag climbs) | ✔ **chosen** |
| **Per-aggregate dynamic claim** (UUID workers, advisory lock per aggregate) | lock (not a fence) | yes, elastic | conditions E1–E6 **+ lock fragility** | **silent** (reorder if a condition slips) | ✗ rejected (§6 evidence) |

The decisive axis: buckets make exclusivity a property of *partitioning* (cannot be lost to a
non-fencing lock) and make the worst case a **stall** (detectable: `lag.age_seconds`,
`bucket.uncovered`), whereas claim makes correctness depend on several conditions plus a
best-effort lock, with **silent reorder** if one slips.

---

## 4. Verification of the chosen model (buckets)

### 4.1 The happens-before chain (must all hold)

1. **Write-side serialization (E1).** Within an aggregate, commit order = `seq` order = `id`
   order — guaranteed by the per-aggregate write lock (HLD §4.2). *Precondition.*
2. **Head-of-chain processing (E2).** The poll selects only the **earliest unfinished row per
   aggregate** (`NOT EXISTS` an earlier row in status PENDING/IN_FLIGHT/FAILED) and only if
   eligible (`next_attempt_at` passed). A backing-off or FAILED earlier row blocks the rest —
   this also **subsumes the poison gate**. Never leapfrogs.
3. **Structural exclusivity.** Aggregate → fixed bucket → one owning worker. No lock to lose.
4. **Sequential publish within an aggregate (E6).** Await `seq(N)`'s ack before sending
   `seq(N+1)`, so a failure of `seq(N)` cannot leave `seq(N+1)` on the partition.
5. **Single-partition, single-producer ordering.** key = `aggregate_id` + idempotent producer
   (`enable.idempotence`, `acks=all`).
6. **Consumer dedup** on `(aggregate_id, seq)` keeps first occurrence — the safety net for the
   duplicates that at-least-once / reclaim / slot-handoff can produce.

Links 1, 2, 4 are preconditions/rules folded into the HLD; **link 3 is free** with buckets
(structural), which is the whole point.

### 4.2 Synchronization examples

**Normal — disjoint buckets, full parallelism:**
```
Order#42 → bucket 12 (Worker A)     Cust#7 → bucket 200 (Worker B)
A: poll bucket 12 → seq5,seq6 → pub(5)→done→pub(6)→done
B: poll bucket 200 → seq3 → pub(3)→done
```
Different aggregates live in different buckets owned by different workers → no contention at
all. Per-aggregate order preserved. ✔

**Contention for the same aggregate — impossible by construction.** `Order#42` is in exactly
one bucket, owned by one worker. There is no per-aggregate race to resolve (unlike claim).

**Slot-handoff split-brain (the bucket analog of claim's E4) — safe:**
```
Worker A owns bucket 12; A pauses → its bucket lease expires (not renewed)
Worker B claims bucket 12 (free lease) → B now owns it; A un-pauses (stale, still polls bucket 12)
Both poll bucket 12:
  - FOR UPDATE SKIP LOCKED → they never grab the SAME row
  - head-of-chain: if A holds seq5 IN_FLIGHT, seq6's NOT-EXISTS sees seq5 (status 1) → seq6
    is NOT a head → B cannot take seq6 while seq5 is unfinished
  → no concurrent processing of one aggregate → at most a DUPLICATE (lease reclaim), never reorder
```
Crucially, the guard here is **deterministic** (row `FOR UPDATE` + the head-of-chain
predicate + lease), not a best-effort lock — which is why buckets are *safer* than claim under
split-brain, not merely equal.

**Crash before `done` — duplicate, safe:** identical to the general case — IN_FLIGHT row's
lease expires, reclaimed, republished, dedup'd.

### 4.3 Devil's advocate — bucket-specific edge cases

| # | Case | Severity | Outcome |
|---|---|---|---|
| **B1** | **Coverage stall** — a bucket with PENDING rows but no live owner | medium, **loud** | Its aggregates stall; `lag.age_seconds` / `bucket.uncovered` climb → alertable. Liveness, not safety. Self-heals via `tandem_bucket_lease` reassignment (standalone) or process restart (embedded). |
| **B2** | **Hot bucket / skew** | low, throughput | `B` large (256) spreads aggregates; a single very-hot aggregate is inherently one-worker in *any* model (per-aggregate ordering). Not a correctness issue. |
| **B3** | **Slot-handoff split-brain** | low, safe | §4.2 — head-of-chain + `SKIP LOCKED` + lease → duplicates only, never reorder. |
| **B4** | **Workers > B** | low | Excess workers idle. `B=256` ≫ realistic worker counts. |
| **B5** | **Changing `B` after deploy** | **operational hazard** | If `B` changes, an aggregate's old/new events can land in different buckets → split across workers → reorder. `B` must be **immutable** (like a Kafka topic's partition count); changing it requires a controlled drain (process all PENDING under old `B`, then switch). |

Plus the **model-independent preconditions E1 / E2 / E6** (§4.1), already applied to HLD
§4.2/§6.

**Verdict:** the bucket model satisfies happens-before given E1/E2/E6 and an immutable `B`.
Its residual risks (B1–B5) are mostly **liveness/operational and loud** (B1) or
throughput-only (B2/B4) — not silent safety failures. This confirms the choice over claim.

---

## 5. Evidence — why per-aggregate claim was rejected

The claim model (UUID workers, advisory lock per aggregate) was analysed in depth; it is
*correct if* a longer list of conditions holds, several of which fail silently. Summary of the
findings (full reasoning below):

- **E3 — MySQL bare `SKIP LOCKED` is insufficient.** Row locks release at the IN_FLIGHT
  commit, so a *new* row of the same aggregate inserted mid-publish is claimable by another
  worker → reorder. Needs `GET_LOCK` (advisory) after all → both DBs end up on advisory locks.
- **E4 — the advisory lock is not a fencing token.** A connection drop auto-releases it while a
  paused-but-alive worker still believes it owns the aggregate → two processors. Ordering
  survives *only* because of seq-ordered emission + consumer dedup — so the lock is an
  optimization, not the guarantee.
- **E5 — connection affinity mandatory.** The lock is session-scoped; a naive connection pool
  silently breaks exclusivity.
- **E7 — lock leak on the exception path** stalls an aggregate until the connection recycles.
- **E8 — 64-bit key needed** to avoid hash-collision false-serialization.

Buckets eliminate E3/E4/E5/E7/E8 outright (no per-aggregate lock). The model-independent
findings **E1, E2, E6** survive and were folded into the HLD.

### 5.1 The claim happens-before chain (for the record)

Same six links as §4.1, except **link 3 was lock-based**: "while a worker holds the aggregate
advisory lock, no other worker processes it." That lock is exactly what E3/E4/E5/E7 attack —
it is best-effort, not structural. Worked failure timelines (split-brain producing duplicates
that dedup absorbs; the MySQL new-insert reorder; the backoff leapfrog) are what tipped the
decision toward making exclusivity structural instead.

> The original analysis carried full per-edge-case timelines (E1–E11, with synchronization and
> crash examples for the claim model). They are intentionally **condensed** here — the key
> findings are summarised above and in §4 — because the model they analyse was **not chosen**;
> keeping the verbatim timelines for a rejected design would be noise.

---

## 6. Consequences / follow-ups

- HLD §4.2 (E1 precondition), §4.3 (bucket model + prior art), §5.1 (`bucket` column + indexes),
  §6 (head-of-chain poll + structural per-aggregate order — one head in flight, overlapping sends
  across the batch's distinct aggregates — + stop-on-failure), §7 (`bucket.uncovered`) are
  updated to this decision.
- `LLD-jdbc` must specify: bucket assignment (in-process vs `tandem_bucket_lease` table), the exact
  head-of-chain SQL and its index usage (Q11), transaction boundaries (Q9), the immutable-`B`
  operational rule (B5), and the **Java-side** bucket computation (`BucketHash` = 64-bit FNV-1a +
  `Math.floorMod`, engine-independent and stable across DB upgrades; LLD-core §4).
