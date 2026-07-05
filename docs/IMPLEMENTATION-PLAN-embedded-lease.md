# Tandem — Implementation Plan: Multi-Instance Coordination (`LEASE` opt-in)

**Version:** 1.1
**Status:** Implemented (`tandem-jdbc`, 2026-07-02) — Spring property exposure still deferred to 2nd round
**Scope:** make the `LEASE` bucket-coordination mode a first-class, statically-declared option usable in
**any** deployment location — in particular a horizontally-scaled client with an **embedded** relay, not
only standalone relay processes.

This is the execution plan for the feature. The *design* is fixed in the HLD/LLDs (HLD §3.2 "two
orthogonal axes" + §4.3; LLD-jdbc §3.2/§6); this document only orders the work, fences the scope, and
defines done-ness. Read those sections first. Follow [AGENTS.md](../AGENTS.md) for every change.

---

## 1. Background & motivation

**The problem.** The embedded relay (the Pareto default) is built with `BucketSource.embedded(B)`, which
makes the instance own **all** `B` buckets. That is correct for a single process, but a client service is
routinely scaled to N replicas — and `tandem-spring` bundles the relay in-process by default. N embedded
replicas each own all buckets: correctness holds (ordering + single-claim are row-carried via
`status = IN_FLIGHT` + `FOR UPDATE SKIP LOCKED`, so no reorder and no double-claim), **but** every
instance polls every bucket, multiplying DB load for zero throughput gain and widening the reclaim-vs-late-
flush duplicate window (a peer instance's reclaim scheduler runs in a JVM that is *not* frozen during
another instance's GC pause). See the analysis captured in HLD §3.2.

**The fix.** Decouple the two axes the code currently conflates:
- **Deployment location** (embedded vs standalone) — already a real distinction.
- **Coordination mode** (`SINGLE` vs `LEASE`) — how concurrent instances share buckets.

`LEASE` (the `tandem_bucket_lease` mechanism that already exists for standalone) partitions buckets across
instances, removing the overlap. Expose it as a **statically-declared** config so an embedded,
multi-replica deployment can turn it on.

**Why static, not auto-detected.** An instance cannot discover "am I one of several?" without the very
lease table `LEASE` provides — the detection is circular. The operator declares it. `LEASE` with a single
instance degrades gracefully (one owner claims all `B` buckets), so enabling it "to be safe" is cheap;
`SINGLE` stays the zero-cost default (Pareto — the 80% pay nothing).

**Key leverage: most of the machinery already exists.**
- `BucketLeaseManager` already implements `BucketSource`.
- `WorkerPool`'s full constructor already accepts any `BucketSource`, and already drives
  `bucketSource.heartbeat()` (scheduled) and `bucketSource.release()` (on stop), regardless of mode.

So the feature is mostly **config surface + a selection factory + validation + docs + tests**, not new
engine logic.

---

## 2. Scope

### In scope (build now — `tandem-jdbc`)
- `RelayConfig`: new `coordination` (`SINGLE`|`LEASE`, default `SINGLE`), `bucketLease` (default 30s),
  `instanceId` (optional; derived if unset).
- `Coordination` enum (in `tandem-jdbc`, next to `RelayConfig`).
- `BucketSource.forCoordination(RelayConfig, DataSource)` factory — returns `embedded(B)` for `SINGLE`,
  a `BucketLeaseManager` for `LEASE`.
- Startup **fail-fast validation** for `LEASE` (lease table present + seeded to `bucketCount`).
- `instanceId` derivation helper (`host-pid-<rand>`, ≤ 64 chars).
- Unit + integration tests (two `WorkerPool`s sharing one DB under `LEASE`).

### Out of scope (do NOT build; stop and flag if a task seems to need it)
- **Spring autoconfig exposure** (`tandem.relay.coordination` property) — belongs to `tandem-spring`,
  which is **2nd round (Q6/Q21–Q23 open)**. This plan makes the mechanism ready; wiring it into
  autoconfig is a follow-up.
- **`tandem-relay` standalone runnable** — 2nd round.
- **Reclaim scoping / fencing hardening** (see §6) — a *separate*, optional hardening; not required for
  this feature and tracked independently.
- **MySQL DDL** for the lease table — Q28.
- Any change to the ordering/claim SQL (`SINGLE` and `LEASE` share the identical §3.3 claim path).

> **Hard rule:** if a task appears to require an out-of-scope decision (a Spring property binding, the
> standalone runnable, MySQL specifics), **stop and surface it** — do not invent design. The default
> (`SINGLE`) behaviour must be byte-for-byte unchanged.

---

## 3. Design details to pin

### 3.1 Config surface (`RelayConfig`)
| Field | Type | Default | Notes |
|---|---|---|---|
| `coordination` | `Coordination` | `SINGLE` | `SINGLE` \| `LEASE` |
| `instanceId` | `String` | `null` → derived | `LEASE` lease `owner`; ≤ 64 chars |
| `bucketLease` | `Duration` | 30 s | ownership lease; **independent** of `rowLease` |

- Builder methods mirror the existing style (`Objects.requireNonNull` / positive checks).
- `instanceId` derivation when unset: `"<hostname>-<pid>-<4hex>"`, truncated to 64 chars. Stable for the
  process lifetime. (Operators may override for stability across restarts so a bounced instance reclaims
  its own buckets before lease expiry.)
- **No interaction with the `rowLease > delivery.timeout.ms` invariant** — `bucketLease` is a different
  lease; the existing `checkRowLeaseSafe` is untouched.

### 3.2 Selection factory (`BucketSource.forCoordination`)
```java
static BucketSource forCoordination(RelayConfig cfg, DataSource dataSource) {
    return switch (cfg.coordination()) {
        case SINGLE -> embedded(cfg.bucketCount());
        case LEASE  -> new BucketLeaseManager(
                dataSource, cfg.resolvedInstanceId(), cfg.bucketCount(), cfg.bucketLease());
    };
}
```
- Keeps `WorkerPool` port-only: the factory (not the pool) is where the `DataSource` is introduced, and
  it lives in `tandem-jdbc` alongside `BucketLeaseManager` (already `DataSource`-coupled).
- The 3-arg `WorkerPool(store, dispatcher, cfg)` convenience constructor stays **`SINGLE`-only** (it has
  no `DataSource`); `LEASE` uses the full constructor with the factory-built source (LLD-jdbc §7).

### 3.3 `LEASE` startup validation (fail-fast)
A new `BucketLeaseManager` self-check, invoked once at relay start (mirrors `checkRowLeaseSafe`'s
fail-fast + metric + one-line log pattern):
- `SELECT count(*) FROM tandem_bucket_lease` must equal `bucketCount`. If the table is missing or the
  count differs, record `config.invalid` (new `check` tag, e.g. `bucket_lease_not_seeded`) and throw
  `TandemConfigurationException` with a formula-bearing message ("expected B rows, found N; apply the
  `LEASE` baseline DDL"). This catches the common "enabled `LEASE` but only applied `tandem_outbox`" error.

### 3.4 Behaviour invariants (must hold; assert in tests)
1. `SINGLE` path is unchanged — no lease table touched, `heartbeat`/`release` no-ops.
2. Under `LEASE`, no bucket is owned by two instances **at steady state** (transient membership window
   excepted, and safe per §3.2).
3. Ordering + at-least-once are **identical** across modes (claim SQL is shared).
4. `LEASE` with a single instance ⇒ that instance owns all `B` buckets (`target = ceil(B/1) = B`).

---

## 4. Work breakdown & order

```
1. RelayConfig (config fields + Coordination enum + instanceId derivation)
2. BucketSource.forCoordination factory
3. BucketLeaseManager startup validation (seeded-count fail-fast)
4. Unit tests (config defaults/validation; factory selection; derivation)
5. Integration test (two WorkerPools, one DB, LEASE — partition + correctness)
6. Docs already updated (HLD §3.2/§4.3/§12, LLD-jdbc §1/§3.1/§3.2/§6/§7, LLD-core, README, admin OpenAPI)
```

- Steps 1–3 are additive and independent of the engine loop; `SINGLE` callers are unaffected.
- Step 5 is the decisive test — see §5.

---

## 5. Testing (no-mocks rule; real Postgres via Testcontainers)

- **`RelayConfigTest`** (unit): `coordination` default = `SINGLE`; `bucketLease` default; `instanceId`
  derivation shape + 64-char cap; builder validation.
- **`BucketSourceTest`** (unit): `forCoordination(SINGLE)` → `embedded` (all `B`); `forCoordination(LEASE)`
  → `BucketLeaseManager` (via a `DataSource`; owned set behaviour covered in the IT).
- **`BucketLeaseManagerIT`** (existing, extend): add the seeded-count fail-fast case (drop/emptied table
  ⇒ `TandemConfigurationException`).
- **New `EmbeddedLeaseIT`** (integration): start **two** `WorkerPool`s with `coordination=LEASE` against
  one Postgres + one `tandem_outbox`, insert events across many aggregates, and assert:
  1. after heartbeats converge, the two instances' owned-bucket sets are **disjoint** and cover `[0,B)`;
  2. every event is published, per-aggregate order preserved (reuse the correlation-style assertions);
  3. killing one instance ⇒ survivor reclaims its buckets (coverage self-heals);
  4. duplicates, if any, are redeliveries only (never a reorder) — the `SINGLE`-vs-`LEASE` guarantee parity.
- Keep an `@Tag("integration")` smoke variant tiny for CI.

---

## 6. Companion hardening (separate, optional — do NOT bundle)

Independently tracked, not required for this feature:
- **Fence the status-update SQL.** `markDone`/`markForRetry`/`markFailed` currently update by `id` with no
  `locked_by`/`locked_until` guard, so a stale writer (a late flush after a peer reclaim) can stomp a row
  another instance now owns. Adding `AND locked_by = :me` makes the late write a safe no-op. The HLD
  already deems the transient double-ownership "harmless" (at most a duplicate, never a reorder), so this
  is a nice-to-have that blinds the last edge, not a blocker.
- **Scope the reclaim to owned buckets** (optional efficiency). `reclaimExpiredLeases` scans the whole
  table from every instance; under `LEASE` it could filter to owned buckets. Redundancy, not correctness.

These stay in the backlog; this plan ships the coordination opt-in without them.

---

## 7. Done-ness checklist
- [x] `SINGLE` default behaviour unchanged (existing tests green, no lease table required).
- [x] `LEASE` selectable via config (`RelayConfig.coordination`/`instanceId`/`bucketLease`); factory
  `BucketSource.forCoordination(cfg, dataSource)` wires `BucketLeaseManager`.
- [x] Startup fail-fast when the lease table is absent/unseeded under `LEASE`
  (`BucketLeaseManager.validateOnStart`, invoked from `WorkerPool.start`).
- [x] `EmbeddedLeaseIT` proves partitioned (disjoint) ownership + full coverage + zero-loss + per-aggregate
  ordering parity across two `LEASE` instances on one outbox. Lease self-heal already covered by
  `BucketLeaseManagerIT`.
- [x] Docs consistent (this plan + the HLD/LLD/README/OpenAPI edits).
- [x] `./gradlew check` passes (incl. the new IT).
- [ ] 2nd-round follow-up flagged: `tandem.relay.coordination` Spring property (Q6/Q21–Q23).

### As-built notes
- `WorkerPool` stays port-only: the `DataSource` enters via the `forCoordination` factory (called by the
  wiring layer), not the pool. The 3-arg convenience `WorkerPool(store, dispatcher, cfg)` remains
  `SINGLE`-only; `LEASE` uses the full constructor with the factory-built `BucketSource`.
- `instanceId` derivation = `tandem-<host>-<pid>-<rand>`, host capped, whole id capped to 64 (the
  `tandem_bucket_lease.owner` length). Resolved once at `RelayConfig` build time so it is stable per config.
- `EmbeddedLeaseIT` asserts an **empty intersection** of the two owned-bucket sets (not non-emptiness):
  a co-start race can transiently leave one instance owning all 256 buckets and the other 0 — still
  disjoint, still full coverage, still correct. It awaits the steady state before asserting.

### Scale-up rebalancing follow-up (membership presence) — done 2026-07-02
The S8 load test (LLD-benchmark §8.2) surfaced that `LEASE`'s original fair-share divisor
(`LIVE_OWNERS_SQL`, derived from bucket ownership) **starves a plain scale-up**: a zero-owned joiner is
invisible to an incumbent holding every bucket, a stable equilibrium broken only by the incumbent
crashing. Fixed by **decoupling presence from ownership**: a new `tandem_relay_member` table (baseline
DDL), each instance self-registers on every heartbeat + prunes expired members, and the divisor counts
live members. On graceful shutdown an instance deletes its presence row so peers rebalance without
waiting for expiry; `validateOnStart` also probes the member table. Verified by
`BucketLeaseManagerIT`'s sequential-join convergence test (128/128). Design: LLD-jdbc §3.2. This is a
`BucketLeaseManager` change, so it benefits standalone `LEASE` too, not just embedded.
