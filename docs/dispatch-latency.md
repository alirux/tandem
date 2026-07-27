# Tandem — Dispatch Latency & Post-Commit Wakeup (Design Note)

**Version:** 1.0  
**Status:** Draft — analysis only, no decision taken  
**Companion to:** HLD §6 (End-to-End Flow), LLD-jdbc §3.1 (poll loop)

This note analyses the latency between the commit of a business transaction and the
publication of its outbox rows to Kafka, and evaluates the options for shortening it —
in particular the family of designs generally called **post-commit wakeup**: signalling
the relay that work exists instead of waiting for it to discover the work by polling.

It exists because "the relay polls" is the most common objection raised against a
library-based outbox, and the objection deserves a quantified answer rather than a
reflexive one. The conclusion is deliberately conditional: for a large share of
deployments the current model is already the right one, and the mechanisms below earn
their complexity only under a specific load profile.

---

## 1. Where the latency actually is

The relay's dispatch latency decomposes into four terms:

```
commit → publish  =  T_discover  +  T_claim  +  T_encode  +  T_kafka
```

`T_claim`, `T_encode` and `T_kafka` are work that must happen regardless of how the
relay learns about the row. Only **`T_discover`** — the interval between the row
becoming visible and a worker looking for it — is what a wakeup mechanism addresses.

**The poll loop does not sleep between batches.** A worker claims back-to-back while
work remains, and sleeps `pollInterval` only when a claim returns empty (LLD-jdbc §3.1;
`WorkerPool.runWorker`). `pollInterval` is therefore an **idle backoff**, not a per-batch
delay, and `T_discover` splits into two regimes:

| Regime | `T_discover` | Notes |
|---|---|---|
| **Busy** — the worker's bucket already has pending rows | ≈ 0 | The worker is mid-cycle; the new row is picked up by the next claim of a loop that never slept. |
| **Idle** — the worker's bucket was drained and the worker is sleeping | uniform in `[0, pollInterval]`, mean `pollInterval / 2` | With the 100 ms default: mean ≈ 50 ms, worst case 100 ms. |

Two consequences follow, and both matter more than the raw number:

1. **Sustained throughput is unaffected by polling.** A wakeup mechanism cannot improve
   the busy regime, because there is nothing to improve — the loop is already spinning.
   Any claim that a wakeup "removes polling overhead from the hot path" is false here.
2. **The cost is paid exactly by low-rate, latency-sensitive traffic**: a bucket that
   receives one event every few seconds pays the full mean of 50 ms on every event,
   because it is always in the idle regime. This is the profile worth optimising, and
   it is the *opposite* of the profile most throughput benchmarks exercise.

A third, less visible cost is the **idle query load**, which scales as
`instances × workersPerInstance / pollInterval`. Each query is an index-only scan of
`idx_tandem_outbox_dispatch` (partial on `status = 0`, so it touches no heap on a drained
outbox), which is why a single relay at the default settings is unmeasurable. The number
stops being negligible at scale: 10 relay instances × 16 workers at a 20 ms interval is
8 000 queries/second to discover nothing.

**This tension is the whole point of the analysis.** Latency and idle load are traded
against each other by a single knob, and the only thing a wakeup mechanism genuinely buys
is the ability to **decouple them** — to have both a low `T_discover` and a long
`pollInterval`.

---

## 2. What any solution must preserve

These are non-negotiable and rule out most of the design space before it is explored.

**C1 — The signal carries no work.** A wakeup notification must mean *"bucket N may have
pending rows"*, never *"here is event 12345"* and never *"here is the payload"*. The
database stays the single source of truth for what is pending. A signal that carries work
becomes a delivery channel, and a delivery channel that can drop, reorder, or overflow
re-introduces exactly the failure modes the outbox exists to eliminate.

**C2 — Losing every signal must be harmless.** Polling remains active in all
configurations, so a lost, dropped, or never-delivered wakeup degrades latency and nothing
else. This is what makes the mechanism safe to add: it cannot be on the correctness path,
so it cannot have a correctness bug.

**C3 — No new infrastructure.** Tandem's positioning is "your database and Kafka, nothing
else" (HLD §11). A mechanism that requires a broker, a cache, or a coordination service
for signalling forfeits the project's main reason to exist.

**C4 — No cost on the client write-side.** The write-side must not acquire dependencies,
threads, or connections for the benefit of the relay (HLD §1.3). Anything that cannot be
expressed with the `Connection` the write-side already holds belongs on the relay side.

**C5 — Zero cost when unused, and a correct default without it.** Per Pareto (HLD §1.1),
the mechanism is opt-in, and the deployments that do not opt in — including every engine
where it is unavailable — must remain fully correct, differing only in latency.

**C6 — It must work across processes.** The relay may run in a separate process from the
writer, and in `LEASE` mode across several processes (HLD §3.2). A mechanism confined to
one JVM addresses only a subset of the supported topologies, and must be described as such
rather than as *the* solution.

---

## 3. Options

### 3.1 Tighter poll interval (the baseline, cost zero)

Setting `pollInterval` to 20 ms brings the mean idle `T_discover` to 10 ms with no new
mechanism, no new failure mode, on every supported engine and in every topology.

This is the honest baseline every other option must beat, and it is a real answer rather
than a placeholder: for a single relay with a handful of workers, the added query load is
noise. Its limit is the scaling term in §1 — it does not survive the combination of many
instances, many workers, and a mostly-idle outbox.

- **Pros:** available today; no code; universal; no new operational surface.
- **Cons:** trades idle DB load for latency linearly; cannot reach single-digit
  milliseconds without a load that becomes visible at scale.

### 3.2 In-process wakeup hint

When the relay is embedded in the same JVM as the writer, the write-side can nudge the
worker owning the row's bucket after the transaction commits — the bucket is already
computed in Java at insert time (HLD §4.3), so the signal is a bucket number and needs no
extra query. The worker parks on a bounded wait instead of a plain sleep, and the nudge
unparks it: `T_discover` collapses to sub-millisecond.

Two constraints shape it:

- **Attaching to "after commit" is the hard part.** Plain JDBC has no commit hook. In
  Spring this is natural (a transaction synchronisation registered by
  `tandem-spring-producer`, firing after commit); in plain Java it can only be an explicit
  call the user makes after committing, which must therefore be optional and inconsequential
  when omitted (C2, C5).
- **It is much weaker in `LEASE` mode than it appears.** With an embedded relay on `N`
  replicas, the local relay owns roughly `1/N` of the buckets, so roughly `(N-1)/N` of
  local writes land in a bucket owned by a *different* process. Those hints are useless,
  and the event waits for the remote instance's poll as before. The in-process hint is
  therefore effective in `SINGLE` mode and in co-located deployments, and largely
  ineffective in exactly the horizontally-scaled deployments where §1's idle-load argument
  bites hardest.

- **Pros:** no protocol, no connection, no engine dependency; trivially satisfies C1–C5.
- **Cons:** violates C6 (one JVM only); its benefit decays as `1/N` in `LEASE` mode;
  needs a commit hook the write-side cannot provide on its own outside Spring.

### 3.3 PostgreSQL `LISTEN` / `NOTIFY`

The only mechanism that crosses process boundaries without new infrastructure. Its
semantics line up with the outbox unusually well:

- **It is transactional.** A notification is delivered when its transaction commits and
  discarded when it rolls back. A signal can therefore never announce a row that does not
  exist — the pathology that makes out-of-transaction signalling unsound.
- **PostgreSQL coalesces duplicates.** Notifications with the same channel *and* the same
  payload within one transaction are delivered once. With the bucket as the payload, a
  transaction inserting fifty events for one aggregate produces one notification, with no
  coalescing code to write.
- **The payload is a bucket number**, satisfying C1 by construction — there is no room in
  it for work.

Three variants differ only in *who* emits the notification:

| Variant | Pros | Cons |
|---|---|---|
| **A second statement on the write-side connection** (`SELECT pg_notify('tandem_bucket', ?)` after the INSERT, same transaction) | No DDL; no commit hook needed — transactional semantics do the work; uses the `Connection` the write-side already holds (C4) | One extra round-trip per transaction (amortisable to one per distinct bucket per transaction) |
| **A CTE fused into the INSERT** (`WITH ins AS (INSERT … RETURNING bucket) SELECT pg_notify(…) FROM ins`) | No additional round-trip | Turns the insert into a result-set-producing statement, complicating JDBC batching and generated-key retrieval — a real cost on the hot write path |
| **An `AFTER INSERT` trigger** | The writer does nothing at all; works for *any* writer, including one on an older Tandem version or a direct SQL insert | Opinionated DDL the operator must apply; per-row trigger overhead; moves library behaviour into the schema, where it is invisible and versioned separately |

Operational caveats, all manageable but all mandatory to document:

- The relay must hold a **dedicated listening connection per instance**, outside the
  business pool. With pgjdbc this means a thread blocked in `getNotifications(timeout)` —
  a socket read rather than a query, but still a thread and a backend.
- **Connection poolers in transaction-pooling mode break `LISTEN`.** This is the most
  common way for the mechanism to silently stop working, and it must be stated wherever
  the feature is documented rather than discovered in production.
- The notification queue is cluster-wide and bounded (8 GB). A listener that stops draining
  can fill it and cause commits to fail — a new way to affect the database that does not
  exist today. Correct consumption avoids it; the failure mode still has to be acknowledged.
- **PostgreSQL only.** MySQL has no equivalent primitive (HLD §5.4), so the fallback there
  is §3.1, permanently.

### 3.4 Rejected

**An external signal bus** (a Redis channel, a dedicated Kafka topic). Violates C3, and —
more seriously — violates C1 and C2 the moment the signal is emitted outside the database
transaction: a signal sent before a commit that then rolls back, or a commit whose signal
is lost, reproduces the dual-write problem in miniature. On the write side it would also
breach C4.

**CDC / logical replication as the wakeup trigger.** Contradicts the project's premise: if
a CDC pipeline is available and operable, the outbox relay is largely redundant
(comparison.md §2).

**A dedicated signal table polled at a tight interval.** Relocates the polling to a smaller
table without eliminating it. Against a partial index on `status = 0` over a drained
outbox, the saving is close to zero — the current query is already the cheap one.

---

## 4. Summary

| | Idle `T_discover` | Cross-process | Engines | New failure surface | Effort |
|---|---|---|---|---|---|
| Current default (100 ms) | ~50 ms mean | n/a | all | none | none |
| §3.1 Tighter interval | ~10 ms mean at 20 ms | yes | all | none | none |
| §3.2 In-process hint | sub-ms, but only for locally-owned buckets | **no** | all | none | low–medium (commit hook) |
| §3.3 `LISTEN` / `NOTIFY` | sub-ms | yes | PostgreSQL only | listening connection; pooler incompatibility; notification queue | medium |

---

## 5. Proposed shape, if built

A port in `tandem-core` — a wakeup signal with a `signal(int bucket)` emission side and a
subscription side consumed by the relay — with a **no-op default** and three adapters
(HLD §1.2):

1. **None (default).** Current behaviour: polling with idle backoff. Zero cost, every
   engine, every topology.
2. **In-process**, applicable when the relay and the write-side share a JVM.
3. **`pg_notify`**, explicitly opt-in, for the split topology on PostgreSQL.

The relay's sleep becomes a bounded wait on the signal rather than a plain sleep, so the
poll interval remains the ceiling on discovery latency in every configuration. That single
property is what keeps the whole feature off the correctness path (C2): with no adapter
configured, the wait expires and the loop behaves exactly as it does today; with an adapter
configured but broken, misconfigured, or silently disabled by a connection pooler, the wait
expires and the loop behaves exactly as it does today.

Once a wakeup is present, `pollInterval` changes role — from a latency knob to a safety
net — and can be raised (500 ms–1 s), which is where the idle-load half of the §1 trade-off
is actually won.

---

## 6. When this is not worth building

Stated plainly, because the analysis does not support building it unconditionally:

- **If the outbox is rarely idle**, the gain is zero. The busy regime already has
  `T_discover ≈ 0`, and no mechanism improves on zero.
- **If the deployment is a single relay with a few workers**, §3.1 reaches ~10 ms for free,
  and 10 ms is below the noise floor of most of the systems this library serves.
- **If the target engine is MySQL**, only §3.1 and §3.2 are available at all.

The profile that justifies it is specific: **bursty traffic with long idle gaps**, or
**many instances × many workers**, plus a latency requirement in the single-digit
milliseconds. Absent that profile, the correct action is to tune `pollInterval` and
document the trade-off — which costs nothing and is available now.

---

## 7. Open questions

- **Q-A.** Should the signal be per-bucket or a single global "something changed"? Per-bucket
  wakes exactly one worker but couples the signal to the bucket count (immutable per HLD §4.3,
  so the coupling is stable); a global signal wakes every worker on every write, which does not
  scale with worker count.
- **Q-B.** In `LEASE` mode, is the `1/N` effectiveness of the in-process hint (§3.2) worth
  shipping at all, or should the in-process adapter be restricted to `SINGLE` mode so its
  behaviour is not misleading?
- **Q-C.** Which `pg_notify` emission variant (§3.3) — the extra round-trip is the safest for
  the write path, but the trigger is the only one that also covers writers outside Tandem's
  control.
- **Q-D.** Does the benchmark harness need an idle-path latency scenario before any of this is
  decided? The current targets are throughput-oriented (HLD §10, HLD-load-testing.md §5), so
  there is no measurement today that would show the improvement — or its absence.
