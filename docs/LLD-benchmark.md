# Tandem — `tandem-benchmark` LLD

**Version:** 0.2 (Implemented & smoke-verified 2026-07-02)  
**Module:** `tandem-benchmark` · package `com.codingful.tandem.benchmark`  
**Depends on:** `tandem-test` (transitively pulls `tandem-core`, `tandem-jdbc`, `tandem-kafka`,
`kafka-clients`, and the Testcontainers Postgres+Kafka runtime), HdrHistogram, HikariCP; JUnit 6 +
AssertJ (test scope, for the CI smoke run).  
**Toolchain:** **JDK 25** (per-module override; the rest of Tandem stays Java 17). Justified because
this module is **not published** — no consumer sees its bytecode — and the load driver benefits from
virtual threads (§4.2), which need Java 21+ and, for blocking JDBC, the Java 24+ no-pinning fix. It
depends on the Java 17 artifacts unchanged (a newer JVM runs older bytecode).  
**Companion to:** [HLD-load-testing.md](HLD-load-testing.md) — this LLD implements that plan.  
**Published:** No. Internal harness only (LLD-base §1 reserves `tandem-benchmark` as *not published*),
so its heavy dependencies (HdrHistogram, HikariCP, load-driver code) never leak into the released
artifacts.

This document specifies the runnable load/performance harness: how it drives Tandem through its
**real** APIs, how it measures throughput and COMMIT→ack latency, how each scenario is orchestrated,
and how it is executed and kept alive in CI. §3–§10 describe the **as-built** implementation,
including several things discovered only while implementing and verifying it against real Docker
containers (called out explicitly below rather than silently folded in) — see §12 for the full list.

---

## 1. Purpose & scope

Validate the two HLD §10 KPIs — **≥ 10k events/s per shard** throughput and **COMMIT→ack p50 < 200 ms
/ p99 < 1 s** latency — plus the correctness and resilience behaviours (ordering, zero-loss, hot-shard
isolation, failover, poison containment) under load. The system under test is **Tandem the library**,
not PostgreSQL or Kafka: the generator always inserts through the public write-side API, never a raw
SQL `INSERT` (HLD-load-testing.md §3, §3.1 records why external HTTP/SQL load tools were rejected).

Out of scope: the KPI *numbers* are only meaningful on the reference baseline (HLD-load-testing.md §5);
on a developer machine the harness runs for correctness and scenario behaviour only (§5.1 there).

---

## 2. Module layout & build

Gradle subproject `tandem-benchmark` (in `settings.gradle.kts`), JDK 25, **excluded from
publishing** exactly like `tandem-sample` (root `build.gradle.kts` collects both into one
`unpublishedModules` set, opted out of the shared java-library/publish convention block).

```
tandem-benchmark/
  build.gradle.kts                      // not published; application plugin for the loadTest entrypoint
  src/main/resources/bench-schema.sql   // the benchmark-owned bench_aggregate table (§4.1)
  src/main/java/com/codingful/tandem/benchmark/
    BenchmarkConfig.java                // §10 — the harness sizing knobs + toSmoke()/toDemo()
    AggregateSelector.java              // §4.2 — namespaced uniform/skewed aggregate-id generators
    BenchmarkHeaders.java               // the harness-owned Kafka header name (proxy latency, §5.1)
    CommitTimestamps.java               // §5.1 — the ACCURATE-mode in-process side-channel
    TransactionalUnitOfWork.java        // §4.1 — thread-bound-connection transaction join, no Spring
    LoadGenerator.java                  // §4   — the driver
    LatencySnapshot.java / LatencyRecorder.java  // §5.1 — HdrHistogram wrapper (p50/p95/p99/p99.9)
    CorrelationConsumer.java            // §5   — Kafka consumer: latency capture + correctness verifier
    BenchmarkMetrics.java               // §6   — in-process TandemMetrics adapter (counters only)
    LagProbe.java                       // §6.1 — direct-SQL lag/backlog observation
    FaultInjector.java / FaultInjectingDispatcher.java  // §8 — S6's poison-injection seam
    RampController.java                 // §7   — adaptive lag-feedback rate controller (S1)
    BenchmarkEnvironment.java           // §3   — containers + Hikari + relay wiring
    RelayInstance.java                  // §3   — one simulated relay instance (pool + BucketSource + producer), S8
    scenario/Scenario.java, ScenarioContext.java, ScenarioResult.java, ScenarioSupport.java
    scenario/S1SustainedThroughput.java … S6PoisonMessage.java, S8MultiInstanceLease.java
    LoadTestRunner.java                 // §9   — the `loadTest` entrypoint (selects & runs scenarios)
  src/test/java/…/SmokeLoadTest.java    // §9   — @Tag("integration") tiny-rate wiring check for CI
```

`build.gradle.kts` mirrors `tandem-sample` (plain `java` + `application`, not the published-module
convention block), with the toolchain set to Java 25 for this module only, and a hand-rolled
`integrationTest` task (the shared convention block's version isn't available to an opted-out module).
Dependency coordinates are hardcoded literals (`org.hdrhistogram:HdrHistogram:2.2.2`,
`com.zaxxer:HikariCP:5.1.0`, `org.junit:junit-bom:6.1.1`, `org.assertj:assertj-core:3.27.7`) rather
than the root version catalog, matching how `tandem-sample` declares its own dependencies. The one
deliberate exception in both modules is the SLF4J binding, taken from the catalog as
`libs.slf4j.simple` so it can never drift from `tandem-kafka`'s `slf4j-api` major version (an older
1.x binding is not loadable against a 2.x provider — see HLD-logging.md §10). CI needs a **JDK 25**; the existing
`foojay-resolver` convention (`settings.gradle.kts`) auto-provisions it if absent.

**`loadTest` is a dedicated `JavaExec` task**, not the `application` plugin's generic `run` — an
earlier draft of this document described `./gradlew :tandem-benchmark:loadTest` before that task
actually existed (it only had `run`, undiscoverable under that name); fixed once the gap was noticed
while trying to use it. Both `test` and `integrationTest` also carry an explicit
`testLogging { events("passed", "skipped", "failed") }`, so `PASS`/`FAIL` lines appear live in the
console during a run instead of only in the post-run XML/HTML report (Gradle's `Test` task type
prints nothing per-test by default).

---

## 3. `BenchmarkEnvironment` — containers + wiring

A dedicated environment — **not** `TandemTestContainer.newRelay`, whose `DataSource` opens a fresh,
unpooled connection per call and so cannot bound the driver's real concurrency (§4.2 needs a sized
connection pool). It composes a `TandemTestContainer` instance for container lifecycle + baseline-DDL
application, but layers its own **HikariCP-pooled** `DataSource` (sized to
`BenchmarkConfig.maxConnections`) on top, and assembles the relay directly rather than through the test
helper's convenience factories.

- Starts a real **PostgreSQL 16** and a real **Kafka** (KRaft) container (via the composed
  `TandemTestContainer`), applies the committed baseline DDL, then applies the benchmark's own
  `bench-schema.sql` (the `bench_aggregate` table, §4.1) over the Hikari pool.
- **Producer config: no explicit override needed for the mandated values.** `KafkaRelay` hardens
  *any* producer config to `acks=all` + `enable.idempotence=true` by default (LLD-kafka §1,
  `KafkaProducerConfig.harden`) — so simply not overriding them already gets the production config.
  `BenchmarkEnvironment` only sets `bootstrap.servers` and, when the config calls for a non-default
  `delivery.timeout.ms` (the smoke variant, §10), `delivery.timeout.ms` itself. **Discovered while
  implementing:** Kafka's producer validates `delivery.timeout.ms ≥ linger.ms + request.timeout.ms`
  at construction time (`request.timeout.ms` defaults to 30 s) — so shrinking `delivery.timeout.ms`
  below that floor also requires shrinking `request.timeout.ms` to match, or the producer refuses to
  construct. `BenchmarkEnvironment` sets `request.timeout.ms = min(30_000, deliveryTimeoutMs)`
  alongside it.
- Wraps the real `KafkaRelay` in a `FaultInjectingDispatcher` (§8) — always constructed, a pure
  pass-through until a scenario (S6) arms it via `FaultInjector`.
- Assembles the primary relay with the **full** `WorkerPool` constructor (not the 3-arg
  `SINGLE`-only convenience one), so a real `BenchmarkMetrics` and
  `Clock.systemUTC()`/`BackoffStrategy.fullJitter()` are wired in explicitly, and its `BucketSource` is
  selected via `BucketSource.forCoordination(relayCfg, dataSource)` (`tandem-jdbc`'s `Coordination`
  axis, §8) rather than a hardcoded `BucketSource.embedded(...)` — `relayConfigBuilder()` (below)
  defaults `coordination` to `RelayConfig`'s own default (`SINGLE`), so every scenario except S8 sees
  unchanged behaviour.
- **`relayConfigBuilder()`** returns a `RelayConfig.Builder` pre-populated from `BenchmarkConfig`
  (bucket count, workers, batch size, row lease, max attempts, the real negotiated delivery timeout) —
  the shared sizing every relay instance in the environment uses. S8 layers
  `.coordination(LEASE).instanceId(...).bucketLease(...)` on top of this to build its own additional
  instances without duplicating the shared config.
- **`newRelayInstance(RelayConfig)`** builds an *additional*, independent relay instance — its own
  Kafka producer (a separate `KafkaRelay`, as a real separate relay process would have) and its own
  `BucketSource` (per that config's `coordination`), sharing this environment's
  `DataSource`/`JdbcOutboxStore` (as real instances sharing one DB do). Returns a `RelayInstance(pool,
  bucketSource, producer)` record; the caller starts/stops the pool, and this environment closes the
  producer on `close()`. This is what lets S8 simulate more than one relay instance against one outbox.
- Exposes: the pooled `DataSource` (for `LoadGenerator`/`LagProbe`), the `JdbcOutboxStore` (S5 calls
  `reclaimExpiredLeases()` on it directly), the `WorkerPool` (caller `start()`/`stop()`s it per
  scenario), `BenchmarkMetrics`, `LagProbe`, `FaultInjector`, `relayConfigBuilder()`,
  `newRelayInstance(...)`, and `newConsumer(groupId)` for a `CorrelationConsumer`'s Kafka consumer.
- **Not implemented (deferred):** a Postgres session/instance-tuning hook (`shared_buffers`,
  `synchronous_commit`) for matching the §5 reference baseline. An early draft of this LLD described
  one; it was never built — the module only targets correctness/behaviour verification today, and the
  §5 baseline numbers are produced on the dedicated reference host, not through this harness's own
  tuning.

Co-location (DB + relay + broker + harness in one host/JVM) satisfies the single-clock requirement for
latency (HLD-load-testing.md §2.3) and is also what makes the ACCURATE latency path (§5.1) possible.

---

## 4. `LoadGenerator` — the driver

Reproduces the domain write path at a controlled offered rate.

### 4.1 Per-insert unit of work, and joining the transaction without Spring

Each generated event runs, in **one transaction**:

1. `SELECT … FOR UPDATE` on a synthetic aggregate row (`bench_aggregate(aggregate_id, version)`,
   `src/main/resources/bench-schema.sql`), then `version++` — mimicking the real domain contention the
   outbox pattern sits inside.
2. `repository.insert(OutboxMessage)` — the **real** `JdbcOutboxRepository`, joining the same
   transaction.
3. `COMMIT`.

`JdbcOutboxRepository`'s javadoc assumes a **transaction-aware `DataSource`** (LLD-jdbc §2) — one whose
repeated `getConnection()` calls, on the same thread, return the *same* physical connection already
bound to an open transaction, the way Spring's `TransactionAwareDataSourceProxy` does. Tandem itself
never ships such a thing (minimal-client-footprint), and the sample app's demo never actually exercises
atomicity for this reason. `LoadGenerator` needs the real behaviour, so it brings its own minimal
version: **`TransactionalUnitOfWork`**, a `ThreadLocal<Connection>`-bound helper whose
`runInTransaction(work)` borrows one connection, binds it, runs `work`, commits, then unbinds and
closes; its paired `transactionAware()` view is a `java.lang.reflect.Proxy`-based `DataSource` whose
`getConnection()` returns the bound connection wrapped in a `close()`-is-a-no-op `Connection` proxy (so
`JdbcOutboxRepository`'s own `try (Connection conn = …)` doesn't end the transaction early). This is the
one piece of "transaction glue" the harness had to invent that the design docs only gestured at.

The `OutboxMessage` carries `aggregateType = "BenchAggregate"`, `seq = version`, a **1 KB JSON reference
payload** (`BenchmarkConfig.payloadBytes`, default 1024, built once and reused), and the proxy latency
header (`BenchmarkHeaders.T0_NANOS`, §5.1).

### 4.2 Concurrency, rate, and aggregate distribution

- The unit of work runs on a **virtual thread** (`Executors.newVirtualThreadPerTaskExecutor()`), one per
  offered insert. The work is blocking (a JDBC transaction), so virtual threads fit: the driver can have
  many inserts *in flight or waiting for a connection* without a large platform-thread pool, and it
  never becomes the bottleneck while the DB is the limiter. This needs the Java 25 toolchain (§2) — on
  Java 17 there are no virtual threads, and even on 21–23 PgJDBC's `synchronized` blocks would *pin* the
  carrier on every JDBC call; the Java 24+ no-pinning fix (JEP 491) is what makes virtual-thread JDBC
  actually scale here.
- **Real concurrency is bounded by a `Semaphore` sized to `BenchmarkConfig.maxConnections`**, matching
  the DataSource pool — not by the virtual-thread count. A fixed-cadence pacer thread computes the next
  submit tick from the target rate, submits an insert task if a permit is free (skipping the tick
  otherwise — natural backpressure when the offered rate exceeds what the DB/broker can absorb), and
  resyncs its clock if it falls far behind rather than bursting to catch up.
- **Offered rate** is a `volatile` target the pacer reads every tick; `setRate(...)` lets a caller (the
  `RampController`, or a scenario directly) change it while the generator is running.
- **Aggregate distribution** is namespaced per scenario (`AggregateSelector.uniform`/`skewed`, both
  take a `String namespace` — see §4.3): `uniform` spreads load evenly over a configured cardinality
  (spreading across buckets); `skewed` sends a configurable `hotFraction` of traffic to a single
  aggregate (`universe().get(0)`) and the rest uniformly over the remainder (S3, and S6's poison
  target). "Skewed" here is a simple two-population hot/cold split, not a true rank-based Zipfian
  distribution — sufficient for S3's actual need (one dominant hot key) without a heavier generator.
- `LoadGenerator.insertedKeys()` exposes every successfully-committed `aggregateId#seq` — the set every
  scenario's zero-loss check reconciles against.

### 4.3 Aggregate-id namespacing — a correctness requirement, not just labelling

**Discovered during CI-smoke verification, not anticipated in the original design:** `SmokeLoadTest`
runs several scenarios against **one shared `BenchmarkEnvironment`** (one `tandem_outbox` table, one
Postgres). `AggregateSelector`'s id space is otherwise deterministic (`"bench-agg-0"`, `"bench-agg-1"`,
…), and S6 always poisons `universe().get(0)` — so an un-namespaced generator would let S6's poisoned
aggregate collide with another scenario's ordinary use of the *same* id. The poison gate is structural
and permanent (LLD-jdbc §3.4.2: a `FAILED` row blocks every later `seq` for that aggregate forever), so
that collision doesn't just corrupt one assertion — it makes every later scenario's drain-wait hang
indefinitely, because the shared `tandem_outbox` never fully drains again for the rest of the test run.

The fix: every `AggregateSelector` factory takes a `namespace` (each scenario passes its own `id()`,
e.g. `"S1"`, `"S6"`), producing ids like `"S1-bench-agg-0"` / `"S6-bench-agg-0"`. `LagProbe`'s
drain-related queries (§6.1) are scoped the same way, for the identical reason.

---

## 5. `CorrelationConsumer` — latency capture + correctness verifier

A single Kafka consumer per scenario, co-located with the harness, subscribed to the one benchmark
topic (`BenchmarkEnvironment.TOPIC`). It is the one component that observes the *output* of the
pipeline, and it does double duty (HLD-load-testing.md §2.2): the relay records nothing — measurement
needs **no product hook**.

For each received CloudEvent it reads the key (`aggregate_id`), the `ce_seq` header (the CloudEvents
`seq` extension in Kafka binary-mode encoding — read via `CloudEventsHeaders.CE_SEQ`, the same constant
`tandem-kafka` uses, parsed as a UTF-8 string), and the harness's own `bench-t0-nanos` header (passed
through verbatim by the relay's header-passthrough, since it's just an ordinary stored header — LLD-jdbc
§2).

- **Correctness (all scenarios).** Per `aggregate_id`, track the high-watermark `seq` seen and flag a
  violation only when a **strictly lower** `seq` arrives — **not** when the same `seq` repeats.
  **Discovered during CI-smoke verification:** an early version flagged `newSeq <= previousSeq` as a
  violation, which misclassified a legitimate at-least-once *duplicate* redelivery (expected under S5,
  and explicitly tolerated by design — HLD-load-testing.md §6) as an ordering violation. Duplicates are
  tracked separately (`duplicateCount`, via a `receivedKeys` set) and never fail a scenario on their own.
  Zero-loss is reconciled by each scenario after driving load: `generator.insertedKeys() \
  consumer.receivedKeys()` must be empty (`ScenarioSupport.verify`, §8).
- **Latency.** Compute `t1 − t0` and feed `LatencyRecorder`. `t1` is the consumer receive time; the
  delta includes the broker→consumer hop, so it **over-estimates** the COMMIT→ack KPI — accepted and
  conservative.

### 5.1 Two `t0` precisions

Both live in the same clock (the harness JVM), per HLD-load-testing.md §2.3:

- **Proxy (always on, every scenario).** `LoadGenerator` stamps
  `header[BenchmarkHeaders.T0_NANOS] = System.nanoTime()` at INSERT time (before `COMMIT`). Simple, no
  shared state; over-estimates by the small INSERT→COMMIT gap.
- **Accurate (opt-in, `BenchmarkConfig.LatencyMode.ACCURATE`).** Removing the INSERT→COMMIT skew needs
  a post-COMMIT timestamp, which cannot be written into an already-inserted row. Instead
  `LoadGenerator`, immediately after `COMMIT` returns, records `commitNanos` into `CommitTimestamps` — an
  in-process `ConcurrentHashMap<String, Long>` keyed by `aggregateId#seq`, shared between the generator
  (writer) and `CorrelationConsumer` (reader, on receive; entries are removed on read, bounding memory).
  Only **S2** wires this mode in; every other scenario passes `null` (proxy-only — they don't gate on
  latency accuracy).

Latencies go into an **HdrHistogram** (`LatencyRecorder`, backed by `org.HdrHistogram.Recorder` for
lock-free concurrent recording), lossless, reporting p50/p95/p99/p99.9 via `snapshot()` (which follows
`Recorder`'s snapshot-and-reset semantics).

---

## 6. `BenchmarkMetrics` — counters only; lag comes from `LagProbe`

**Correction vs. the original design:** the original plan for this section described
`TandemMetrics.recordLagAgeSeconds` as the signal `RampController` and a "steady-state detector"
consume. **While implementing this, it became clear no code in the product ever calls that method** —
`RelayWorker`/`WorkerPool` only ever call `incrementPublished`, `incrementRetry`, `recordFailed`,
`incrementLeaseExpired`, and `recordConfigInvalid` (grep-verified against `tandem-jdbc`). Lag/lag-age
was never something the relay self-reports; it is purely something an external observer (this harness,
or a future Admin API) has to compute from the table.

So the as-built split is:

- **`BenchmarkMetrics`** implements `TandemMetrics` and is registered with the relay purely as a
  **counter sink**: `publishedCount`, `failedCount`, `retryCount`, `leaseExpiredCount`,
  `configInvalidCount`, plus `publishedSinceLast()` (a sample-and-reset counter for a throughput
  window). It has no lag concept at all.
- **`LagProbe`** (§6.1) is the *only* source of lag/backlog signal, entirely via direct SQL over
  `tandem_outbox` — both the global signal `RampController` uses and the per-bucket signal S3 uses.

### 6.1 `LagProbe` — direct-SQL lag observation

The core port exposes no per-bucket lag metric today (product-side observability gap, tracked in the
project backlog), and as §6 established, it exposes no lag signal of any kind that the product itself
populates. `LagProbe` therefore queries `tandem_outbox` directly for everything:

| Method | Query shape | Used by |
|---|---|---|
| `overall()` | `count(*)`, oldest-age, `status IN (0,1,3)` (PENDING/IN_FLIGHT/FAILED — "not yet DONE") | `RampController`'s steady-state signal |
| `inProgressForNamespace(ns)` | as above but `status IN (0,1)` (excludes `FAILED`) `AND aggregate_id LIKE 'ns-%'` | `ScenarioSupport.waitForDrain` |
| `perBucket()` | `GROUP BY bucket`, `status IN (0,1,3)` | S3 (hot vs. cold bucket backlog) |
| `pendingExcludingAggregate(ns, excludedId)` | `status IN (0,1)`, namespace-scoped, `aggregate_id <> excludedId` | S6 (`waitForOthersToDrain`) |
| `hasFailedRow(aggregateId)` | `EXISTS(… status = 3)` | S6 (confirms the poison gate tripped) |

Two design points only became clear once real Docker runs exposed the failure modes:

- **`FAILED` must be excluded from any "did it drain" wait.** `FAILED` is terminal (LLD-jdbc §3.4.2) —
  it will never become `DONE` on its own. A wait that counts it as "still pending" hangs forever once a
  scenario has a genuinely, permanently failed row (S6, by design). `inProgressForNamespace` and
  `pendingExcludingAggregate` both exclude it; only `overall()`/`perBucket()` (informational/ramp
  signals, never a hard wait condition) still include it.
- **Every drain-related query must be namespace-scoped** (§4.3) for the same cross-scenario-collision
  reason: S6's permanently-stuck poisoned backlog must not count against *another* scenario's own
  drain-completeness check when they share one `tandem_outbox` table.

---

## 7. `RampController` — adaptive rate search (S1)

S1 finds the **highest sustainable** offered rate, which static injection profiles (the external-tool
model) cannot target — this is one reason those tools were rejected (HLD-load-testing.md §3.1). The
controller is a closed loop over `LagProbe.overall()` (§6).

**Two things that looked right on paper did not converge against a real relay** — both found only by
actually running the harness against Docker containers and getting a suspicious `0.0`/`1.0 events/s`
result back, not by inspection:

1. **Hold the rate fixed while verifying it, don't keep raising it.** The first version kept
   additively increasing the rate on every flat observation *while the sustain clock was already
   running* — which effectively tested whether the system could absorb a continuously **increasing**
   rate for the whole `sustainWindow`, a bar that is nearly impossible to clear (compounding a 10%
   step every 2s observation window over a 20s window is already a ~2.6× rate increase within the
   very window meant to *confirm* a fixed rate). Fixed by splitting into two explicit phases: once a
   candidate rate looks acceptable, **freeze it** and hold it fixed for the entire `sustainWindow`
   before either confirming it (then resuming the additive ramp *from* there) or backing off.
2. **Compare against a tolerance-banded baseline, not the immediately preceding sample with none.**
   Fixing (1) alone still produced `0.0`: the relay claims in batches (up to `batchSize` rows per poll
   cycle), so the pending count naturally saw-tooths by roughly that magnitude within a single poll
   cycle even at a genuinely sustainable steady state. A strict "no single sample may be higher than
   the immediately preceding one" check trips on that normal wobble almost every observation window,
   so the sustain gate still never completed. Fixed by anchoring each hold to a `holdBaselinePending`
   captured when the hold starts, and only treating growth **past `toleranceRows`** (both scenarios
   pass `BenchmarkConfig.batchSize()` — the claim-batch size is the natural unit of that wobble) as a
   real backlog-growth signal.

As-built algorithm (`findSustainableMax(generator, initialRate, budget)`):

- **Single continuous hold, re-anchored on every rate change.** Every time the rate changes (up on
  confirmation, down on backoff), a fresh hold starts immediately with `holdBaselinePending` = the
  current pending count. While `pending() <= holdBaselinePending + toleranceRows`, the hold continues
  and the rate is left untouched. If pending exceeds that band, **back off multiplicatively**
  (`rate *= 1 - backoffFraction`) and start a new hold at the lower rate. If the hold survives the
  whole `sustainWindow` within the band, **confirm** `bestSustained = rate`, then **step up
  additively** (`rate *= 1 + rampStepFraction`) and start a new hold at the higher rate.
- **Sustain gate.** The caller passes `BenchmarkConfig.duration()` as `sustainWindow`, so on the
  full-run default (10 min) it matches the HLD's "held ≥ 10 min" gate; on the smoke/demo configs
  (§10) it is proportionally short. A burst peak that cannot hold for the full window at a **fixed**
  rate is never reported as the max.
- `RampController` doesn't own the `LoadGenerator`'s lifecycle: it calls `generator.start(rate)`
  internally but leaves `generator.stop()` to the caller, so a scenario retains normal
  try-with-resources ownership of the generator it constructed.
- The reported result is the aggregate rate; each scenario divides by `BenchmarkConfig.workers()` for
  the per-shard number (HLD-load-testing.md §1.1).
- **S2's own quick-ramp needed the same search-budget/sustain-window split as S1** (`duration` vs.
  `duration × 2`, §8): it originally used one duration for both, which — once the hold-based algorithm
  needed the *entire* sustain window uninterrupted just to confirm a single candidate — left no time
  for even one backoff-and-retry cycle.

---

## 8. Scenarios

Each scenario implements the `Scenario` contract (`String id()`, `ScenarioResult run(ScenarioContext)`)
against a shared `BenchmarkEnvironment` + `BenchmarkConfig` (`ScenarioContext`). A scenario constructs
its own `LoadGenerator`/`CorrelationConsumer` (own Kafka consumer group, own `AggregateSelector`
namespaced to its own `id()`), starts/stops the `WorkerPool` around its run, and drives load directly
(`Thread.sleep` for fixed-duration phases, or `RampController` for adaptive ones).

**`ScenarioResult.passed` is correctness-only** (`ScenarioSupport.CorrectnessReport.passed()`: zero
ordering violations, zero missing keys) — never gated on throughput/latency numbers, which are purely
informational (`summary` string + `metrics` map). This matters because those numbers are only
KPI-meaningful on the §5 reference baseline (HLD-load-testing.md §5.1); a scenario that hits a low
throughput number on a laptop must still be able to *pass*.

`ScenarioSupport` (package-private) holds the logic every scenario shares: `verify(generator, consumer)`
(the correctness reconciliation above), `waitForDrain(lagProbe, namespace, timeout)` /
`waitForOthersToDrain(lagProbe, namespace, excludedId, timeout)` (§6.1's namespace-scoped, FAILED-excluding
polls), and small duration helpers (`observationWindowFor`, `maxDuration`, `minDuration`).

| ID | Focus | As-built orchestration |
|---|---|---|
| **S1** | Sustained max throughput | `RampController` over a uniform `AggregateSelector`; search budget = `duration × 2`; reports aggregate + per-worker rate |
| **S2** | Latency at normal load | A short internal ramp estimates a sustainable rate, then holds 50% of it for `duration`, discarding a `warmup` window; the only scenario using `ACCURATE` latency mode |
| **S3** | Hot partition / skew | `AggregateSelector.skewed` (80% hot fraction) at a fixed offered rate, driven for `min(duration, MAX_DRIVE=10s)` regardless of the configured `duration` (see below); reports hot-bucket pending vs. cold-buckets-with-backlog (`BucketHash.bucketFor` locates the hot bucket) — informational only, not gated |
| **S4** | Saturation / backpressure | Offers a deliberately enormous nominal rate (the in-flight semaphore + real DB/broker capacity self-limit actual throughput — no need to know S1's measured max first), then drops to a trickle and confirms full drain |
| **S5** | Worker failover | `env.relayPool().stop()` (no graceful drain — some in-flight dispatches may ack after the worker loop already exited and stopped flushing DONE), sleeps past `rowLease`, calls `store.reclaimExpiredLeases()` directly, restarts the pool, confirms drain and a bounded duplicate count |
| **S6** | Poison message | `FaultInjector.poisonAggregate(id)` before driving load; after the run, waits for every *other* aggregate to drain, confirms `hasFailedRow` on the poisoned one, and reconciles zero-loss **excluding** the poisoned aggregate's own (deliberately never-delivered) keys |
| **S7** | Causal-ordering overhead | **Deferred — 2nd round** (needs the causal-ordering feature); not implemented |
| **S8** | Multi-instance `LEASE` coordination + crash recovery | Runs **three** relay instances (`env.newRelayInstance`, each its own producer) under `Coordination.LEASE`; waits for a fair 3-way partition, **kills one** (`WorkerPool.kill()` — an abrupt crash, not `stop()`), and confirms the two survivors reclaim its share and delivery still completes correctly. See §8.2/§8.3 for what this scenario found and fixed along the way |

**S5's duplicate bound is wider than the HLD's ideal statement.** `WorkerPool` exposes no API to kill a
single worker thread among several — only whole-instance `stop()`/`start()`. S5 therefore simulates an
*instance* crash, not a *worker* crash, so the observed duplicate bound is conservatively
`batchSize × workers` (every worker's in-flight window) rather than `batchSize` for one worker
(HLD-load-testing.md §6 states the tighter per-worker bound as the target; this LLD's harness can only
verify the wider, whole-instance one without new `tandem-jdbc` surface).

**S3's drive phase is capped independent of `cfg.duration()`, and so is its drain timeout.** The hot
aggregate's backlog is structurally serialized (one in-flight dispatch at a time), so its drain time
scales with `offered rate × hot fraction × drive time`, not the milder scaling the other scenarios see.
`MAX_DRIVE = 10s` bounds how long S3 hammers the hot aggregate regardless of an overall run's
`duration` — and the drain-wait `timeout` (`DRAIN_TIMEOUT = 6 min`) had to be decoupled from
`cfg.duration()` **too**: an intermediate step fixed the drive cap but left the timeout tracking
`cfg.duration()`, so a multi-minute-`duration` run capped the drive at 10s (correctly bounding the
backlog) but then timed out waiting for that same, now-smaller backlog to drain within the *original*
(too-short-relative-to-10s) window. Found by actually running a `duration=150s` demo, not by inspection.

### 8.1 Observations from a `--demo --duration=150s` run (this Mac, 2026-07-02, all 6 scenarios, ~28 min)

Every scenario passed correctness (zero ordering violations, zero missing keys, all six). The
*non-gated* numbers are worth recording honestly rather than only as a PASS line, because they show the
scenarios' current parameters don't always exercise the behaviour they're meant to demonstrate as
sharply as intended:

- **S3 showed imperfect isolation** (`isolated=false`): 4 "cold" buckets carried some backlog too, not
  zero, alongside the hot bucket's 1493 pending rows. Most likely ordinary claim-batch interleaving
  noise rather than a real isolation failure (the scenario's `passed` never depended on this — it's
  informational), but it means S3's isolation claim is softer in practice than the summary table
  implies.
- **S4 barely overloaded anything**: offering a nominal 1,000,000 events/s for 150s only pushed the
  pending count from 0 to 72 before it drained. The `maxConnections` semaphore (16 in `toDemo()`)
  throttles real submission so aggressively that the relay comfortably kept up — a mild positive
  signal about graceful degradation, but it also means this scenario isn't demonstrating a dramatic
  backlog spike at these settings.
- **S5's interesting case (`reclaimed > 0`) has not fired in three separate runs.** `reclaimed=0` every
  time — no row happened to be genuinely `IN_FLIGHT` at the exact moment the pool was stopped, at
  `duration=20s`/`150s` and a 50/s offered rate. The assertion (`duplicates ≤ bound`) still passes
  trivially, but the scenario has yet to actually exercise the crash-recovery path it exists to
  demonstrate. Worth a smaller `duration`-to-`rowLease` ratio or a higher offered rate if this needs
  fixing later — not addressed in this round.

### 8.2 A significant `LEASE`-coordination finding from S8 (this Mac, 2026-07-02)

**Discovered by actually running S8 at load-test scale, not by inspection — this is a
`tandem-jdbc`-level finding (`BucketLeaseManager`), reported here because S8 is what surfaced it; no
`tandem-jdbc` code was changed to produce this LLD.** Across every run (`SmokeLoadTest` and
`--demo S8`, multiple repetitions), the two-instance split came back **`instance-1 owns 256 buckets,
instance-2 owns 0`** — never anything closer to even. Reading `BucketLeaseManager` end to end
(`tandem-jdbc`) confirms this is not sampling luck:

- "Live owners" (the divisor for each instance's fair-share target) is derived from
  `SELECT DISTINCT owner FROM tandem_bucket_lease WHERE owner IS NOT NULL AND lease_until > now()` —
  an instance that currently owns **zero** buckets has no row anywhere in the table, so it is
  structurally **invisible** to this query.
- Once one instance claims all `B` buckets and keeps renewing them, its own heartbeat always computes
  `live_owners = 1` (it never sees the newcomer), so it never releases anything. The newcomer computes
  `live_owners = 2` for *itself* (it always counts itself), targets `B/2`, and tries to claim — but
  every row is owned with a valid, continuously-renewed lease, so its claim query matches zero rows.
- **This is a stable equilibrium, not a transient race.** Elapsed time does not resolve it. Only the
  incumbent's lease *expiring* — a crash, a restart, a deliberate stop — breaks the deadlock and lets
  the newcomer claim the released buckets (this is exactly what `BucketLeaseManagerIT`'s existing
  self-heal test already covers, from a different angle: recovery *after* an owner disappears).

**Why this matters beyond S8 itself.** `LEASE` correctly solves two of the three things asked of it: no
double-processing when multiple instances run (row-carried exclusivity, always true regardless of
timing) and self-healing failover (a dead instance's leases expire and are reclaimed). The third —
redistribution on a plain **scale-up** (new instances alongside an already-stable incumbent, nothing
crashing) — was the very "N replicas" scenario `IMPLEMENTATION-PLAN-embedded-lease.md` was motivated by
(§1 there: "a client service is routinely scaled to N replicas"). S8's contribution was to show the
imbalance is not a brief startup race but the **steady state** for any late joiner, for as long as the
incumbent keeps running.

**RESOLVED (2026-07-02, `tandem-jdbc`).** The design decision this finding flagged was taken: presence is
now **decoupled from ownership**. Each instance self-registers in a new `tandem_relay_member` table on
every heartbeat, and `BucketLeaseManager`'s fair-share divisor counts **live members** instead of bucket
owners (the old `LIVE_OWNERS_SQL`). A zero-owned joiner is therefore visible, so the incumbent sees
`live = 2`, releases its excess, and the fleet rebalances to `B/2` each — verified by
`BucketLeaseManagerIT`'s new sequential-join convergence test and `EmbeddedLeaseIT`. Full design in
LLD-jdbc §3.2. S8's assertions are unchanged and still hold (disjoint ownership, full coverage,
correctness); with the fix a **fair** split is now the normal outcome rather than `256/0`, though S8
still asserts only what always holds, not a specific ratio.

**S8 does not paper over this.** It asserts exactly what always holds — disjoint ownership, full
coverage, correctness — never a fair split, so the lopsided outcome is a `PASS` with a self-explanatory
number (`instance-1 owns 256 buckets, instance-2 owns 0`), not a hidden or worked-around case.

### 8.3 S8 extended to three instances, with a real kill-and-recover step (2026-07-02)

S8 now runs **three** `LEASE` instances (not two) and, mid-run, **kills one** — `WorkerPool.kill()`, a
new `tandem-jdbc` method added specifically to make this test possible (below) — then confirms the two
survivors reclaim its share and delivery still completes with per-aggregate order intact and zero
loss. `EmbeddedLeaseIT` gained the equivalent test,
`GIVEN_three_lease_instances_WHEN_one_is_killed_mid_drain`, at unit-test scale.

**Why `pool.stop()` cannot simulate a crash under `LEASE`.** `WorkerPool.stop()` always calls
`bucketSource.release()` as its last step. For `SINGLE`, `release()` is a no-op, so `stop()` already
doubled as an adequate crash proxy for S5 (HLD-load-testing.md §4, LLD-benchmark §8). For `LEASE`,
`release()` now does real work (§8.2: releases buckets *and* deletes the `tandem_relay_member` row) —
calling it is a **graceful, immediate** departure, the opposite of what a crash test needs (staleness
discovered only once the lease *expires*). **Added `WorkerPool.kill()`**: halts the worker threads and
scheduler exactly like `stop()`, but deliberately skips `bucketSource.release()`. Under `SINGLE` it is
equivalent to `stop()`; under `LEASE` it is the only way to exercise the lease-expiry self-heal path
against a *live, running* instance rather than driving `BucketSource.heartbeat()` by hand (which is
what the lower-level `BucketLeaseManagerIT` self-heal test already did, and still does — `kill()` adds
the same coverage at the `WorkerPool`/real-relay level). `WorkerPoolTest` gained two unit tests proving
`stop()` calls `release()` exactly once and `kill()` never calls it, using a real recording
`BucketSource` test double (no mocks).

**A second, independently-discovered instance of the same class of bug.** The first S8 run against the
new 3-instance/kill flow reported `killed 1 (owned 0 buckets pre-kill)` — the "kill" hit an instance
that had not yet claimed anything, an almost-vacuous test of the reclaim path. The cause: S8's
"partition stabilized" wait condition checked only *disjoint + full coverage*, which is already true
the instant the very first instance to heartbeat claims all `B` buckets, **before** it has even learned
its peers exist (round 1 of the multi-round converge algorithm, §3.2) — the exact same weaker-than-
intended condition that produced the `256/0` finding in §8.2, now surfacing a second way. Fixed by
waiting for a genuinely fair split (every instance owning at least half of an even share) before
proceeding — `fairlyPartitioned(...)` in S8, mirrored as `hasAFairShare(...)` in `EmbeddedLeaseIT`
(which had the identical latent flakiness in its own await condition, confirmed by running it
repeatedly until it reproduced). With the fix, three consecutive `--demo S8` runs all reported the
killed instance owning **84 buckets** pre-kill (≈ `256/3`) and the two survivors converging to **128 +
128** post-kill, with zero ordering violations and zero missing events every time (occasional small
duplicate counts — 0 to 3 — from whatever the killed instance had genuinely in flight, exactly as
expected for a crash).

---

## 9. Execution & CI

- **Full runs:** `./gradlew :tandem-benchmark:loadTest` → `LoadTestRunner.main([--smoke|--demo]
  [--duration=<seconds>] [S1,S2,...])`, which builds a `BenchmarkEnvironment` from
  `BenchmarkConfig.defaults()` (or `.toSmoke()`/`.toDemo()`, optionally with `.withDuration(...)`
  layered on top), runs the selected scenarios (all six minus the deferred S7 by default) in sequence
  against it, and prints a PASS/FAIL line + summary per scenario. Kept **out of the normal
  `test`/`check` lifecycle** — slow, resource-hungry, not meant for shared CI runners. Pass
  `LoadTestRunner` args through Gradle with `--args`, e.g.
  `./gradlew :tandem-benchmark:loadTest --args="--demo S1,S2,S5,S6"`.
- **`--duration=<seconds>`** overrides whichever base config's `duration` (applied after
  `--smoke`/`--demo`) — for a run longer than `--demo`'s 20s but far short of the 10-minute full-run
  default. S3 is safe to include regardless: its own drive phase and drain timeout are both capped
  independent of `cfg.duration()` (§8), so it no longer needs excluding from a longer run the way an
  earlier draft of this document said.
- **Demo mode (`--demo`).** Real relay concurrency (`workers`/`batchSize`/`bucketCount` at their
  defaults) but a short `duration` instead of the 10-minute full-run default — for looking at the
  harness run against real Docker containers without an hours-long wait. Still not a KPI number on a
  developer machine (HLD-load-testing.md §5.1), just faster to look at.
  - **20s sample (`S1,S2,S5,S6`, ~2m46s wall-clock):** `S1` sustained 110 events/s aggregate (13.8/s
    per worker); `S2` normal-load rate 57.5/s with p50=54ms, p95=105ms, p99=127ms, p99.9=177ms; `S5`
    reclaimed=0 duplicates=0; `S6` poison aggregate confirmed blocked, all others flowed with zero
    violations.
  - **`--duration=150` sample, all six scenarios, ~28 min wall-clock:** same S1/S2 numbers (110
    events/s, 50/s normal-load, similar percentiles) — reproducible, not a fluke of the shorter run.
    S3/S4/S5's non-gated numbers are discussed in §8.1: all three passed correctness, but their current
    parameters don't always demonstrate their intended behaviour as sharply as the summary line
    suggests (S3's isolation is imperfect in practice, S4 barely built a backlog at these connection
    limits, S5's crash-recovery path hasn't actually fired across three runs).
- **CI smoke:** `SmokeLoadTest` (`@Tag("integration")`, one shared `BenchmarkEnvironment` per class via
  `@TestInstance(PER_CLASS)`) runs `BenchmarkConfig.defaults().toSmoke()` against **S1, S3, S5, S6,
  S8** — the scenarios that each exercise a structurally distinct code path (ramp, skew, failover,
  poison, multi-instance `LEASE` coordination); S2/S4 reuse S1's machinery and are exercised only in
  full runs. Runs in the existing `integrationTest` phase (Docker required), asserting **correctness
  only** — the reported throughput/latency numbers in a smoke or demo run are informational, never
  gated (§8). **Measured wall-clock (this Mac, 2026-07-02): ~113 s** with S8 added (container startup
  ≈ 18 s + S6 ≈ 4 s + S5 ≈ 14 s + S1 ≈ 7 s + S3 ≈ 55 s + S8 ≈ small, its `duration` and offered rate are
  tiny under `toSmoke()`). Not a hard CI budget, but useful context for anyone tuning it further. Both
  `test` and `integrationTest` print live `PASSED`/`FAILED` lines per test method in the console
  (`testLogging`, §2) — Gradle's `Test` task prints nothing per-test by default otherwise.
- **Official numbers** come from the reference host (§5 baseline), on a schedule or before a release;
  results are archived for regression tracking. Developer-machine runs are correctness/behaviour only
  (HLD-load-testing.md §5.1).

---

## 10. `BenchmarkConfig` — the knobs

Immutable, builder-based (consistent with `RelayConfig`/`OutboxMessage`). As-built fields (note: no
`scenarios`/`targetRatePerSec`/`producerAcks`-`idempotence` fields — scenario selection is a
`LoadTestRunner` CLI concern, per-scenario rates are scenario-internal constants or `RampController`
outputs, and the mandated producer safety values are `KafkaProducerConfig`'s hardened defaults, not a
knob to expose here):

| Knob | Default | Notes |
|---|---|---|
| `bucketCount` | 256 | must match the write-side + relay |
| `workers` | 8 | relay `workersPerInstance` |
| `batchSize` | 100 | per-shard in-flight window |
| `rowLease` | 60 s | relay row lease; must stay `> deliveryTimeoutMs` |
| `deliveryTimeoutMs` | 30000 | Kafka producer `delivery.timeout.ms`, actually wired into the producer config (§3) |
| `maxAttempts` | 10 | retriable failures before `FAILED` |
| `maxConnections` | 32 | Hikari pool size = the true in-flight-transaction limit (§4.2) |
| `payloadBytes` | 1024 | 1 KB JSON reference payload |
| `aggregateCardinality` | 1024 | size of the synthetic aggregate-id universe (§4.2) |
| `warmup` | 30 s | discarded before latency recording (S2) |
| `duration` | 10 min | steady-state window: S1's sustain gate, S2/S3/S6's drive time, S5's half-phases |
| `latencyMode` | `PROXY` | `PROXY` or `ACCURATE` (§5.1) |

**`toSmoke()`** derives the CI variant: `workers ≤ 2`, `batchSize ≤ 20`, `maxConnections ≤ 8`,
`aggregateCardinality ≤ 32`, `warmup = 1 s`, `duration = 3 s`, `deliveryTimeoutMs = 4000`,
`rowLease = 9 s` (> 2 × `deliveryTimeoutMs`, the `RelayConfig.checkRowLeaseSafe` recommended margin).
The `deliveryTimeoutMs`/`rowLease` pair must shrink **together** — shrinking `rowLease` alone fails the
relay-startup invariant fast (`WorkerPool.start()` → `TandemConfigurationException`); this was hit and
fixed once during smoke verification.

**`toDemo()`** derives the "show it running" variant (§9): keeps `workers`/`batchSize`/`bucketCount`
at their real defaults (unlike `toSmoke()`), caps `maxConnections ≤ 16` and `aggregateCardinality ≤
256`, `warmup = 3 s`, `duration = 20 s`, `deliveryTimeoutMs = 8000`, `rowLease = 20 s`.

---

## 11. Scope (this round)

**In:** the harness above — `BenchmarkEnvironment` (+ `RelayInstance`, `relayConfigBuilder()`,
`newRelayInstance(...)`), `LoadGenerator` (+ `TransactionalUnitOfWork`, `CommitTimestamps`,
`AggregateSelector`), `CorrelationConsumer` + `LatencyRecorder`, `BenchmarkMetrics` + `LagProbe`,
`FaultInjector`/`FaultInjectingDispatcher`, `RampController`, scenarios S1–S6 and S8, the `loadTest`
entrypoint, and the CI smoke test. PostgreSQL only.

**Out (later):** S7 causal-ordering overhead (needs the feature); MySQL repetition (needs the MySQL
DDL, open question Q28); distributed/multi-host runs (single-clock latency is out of scope,
HLD-load-testing.md §2.3); a `tandem-micrometer`-based telemetry path (stays the in-process
`BenchmarkMetrics` until that adapter exists); a Postgres session/instance-tuning hook in
`BenchmarkEnvironment` (§3); a real per-worker (rather than whole-instance) kill for S5; S8 with more
than two instances or a throughput-scaling comparison (currently correctness/partitioning only, §8.2);
any fix to the `LEASE` new-joiner-starvation finding (§8.2) — that is `tandem-jdbc` scope, not this
harness's.

---

## 12. Discoveries during implementation (delta from the original design)

Recorded here so the reasoning isn't re-derived later. All were found via real Docker-container runs
(`SmokeLoadTest`, and later `LoadTestRunner --demo`), not by inspection:

1. **`TandemMetrics.recordLagAgeSeconds` is never called by the product** (§6) — the original design
   assumed it was the ramp signal; it isn't populated by anything, so `LagProbe` (direct SQL) is the
   sole lag source instead.
2. **Aggregate-id collisions across scenarios sharing one environment cause a permanent hang** (§4.3) —
   fixed by namespacing every generated aggregate id and every drain-related `LagProbe` query per
   scenario.
3. **A `FAILED` row must never count as "still draining"** (§6.1) — it's terminal; a wait that includes
   it hangs forever once any row is genuinely, permanently failed.
4. **A duplicate redelivery of the same `seq` is not an ordering violation** (§5) — only a strictly
   *decreasing* `seq` is; the original check used `<=` and produced false-positive "violations" on
   every legitimate duplicate.
5. **Kafka's producer validates `delivery.timeout.ms ≥ linger.ms + request.timeout.ms` at construction**
   (§3) — shrinking `delivery.timeout.ms` for the smoke config required shrinking `request.timeout.ms`
   alongside it, or the producer fails to construct.
6. **No transaction-aware `DataSource` exists anywhere in Tandem** (§4.1) — `LoadGenerator` had to bring
   its own minimal one (`TransactionalUnitOfWork`) to actually join `repository.insert` to the same
   transaction as the domain `SELECT … FOR UPDATE`, since the design docs' "the insert joins the
   caller's transaction" assumed a Spring-provided proxy that doesn't exist in this harness.
7. **`./gradlew :tandem-benchmark:loadTest` didn't exist** — an earlier draft documented it before the
   task was actually added; the module only had the `application` plugin's generic `run`. Fixed by
   adding a dedicated `JavaExec` task named `loadTest` (§2, §9).
8. **`RampController`'s sustain gate never converged — reported `0.0`/`1.0 events/s` regardless of real
   throughput** (§7), for two independent reasons found only by actually running it: (a) it kept
   raising the rate *during* the sustain-verification window instead of holding it fixed, testing an
   almost-impossible "absorb a continuously increasing rate" bar; and (b) even after fixing that, its
   flatness check compared each sample only to the immediately preceding one with zero tolerance, and
   the relay's own batched claiming makes the pending count saw-tooth within every poll cycle even at
   a genuinely sustainable rate — so any single noisy sample reset the whole hold. Fixed by holding
   the rate fixed per candidate and comparing against a tolerance-banded baseline (`toleranceRows`,
   sized to `batchSize` — the natural unit of that wobble) instead of a zero-tolerance step comparison.
   S2's quick-ramp needed the same search-budget-vs-sustain-window split as S1 for the same reason.
9. **Capping S3's drive phase without capping its drain timeout to match still hangs it** (§8) — found
   while running a `--duration=150s` demo, all six scenarios. The `MAX_DRIVE=10s` cap correctly bounded
   how much backlog the hot aggregate builds, but the drain-wait `timeout` was left tracking
   `cfg.duration()` (now 150s) instead of being sized to what a 10s drive can actually produce — so the
   (now much smaller, but still multi-minute-to-drain) backlog outlasted a timeout that was
   accidentally still coupled to the overall run length rather than to the thing it was actually
   waiting on. Fixed with a fixed `DRAIN_TIMEOUT` constant, sized to `MAX_DRIVE`'s worst case with
   margin, fully decoupled from `cfg.duration()`.
10. **A `tandem-jdbc`-level finding S8 surfaced, since RESOLVED: `LEASE`'s bucket split used to starve a
    new instance under a plain scale-up** (§8.2) — S8 consistently observed `256/0` splits because
    `BucketLeaseManager`'s old "live owners" query only saw owners holding ≥ 1 bucket, so a zero-owned
    newcomer was invisible to an incumbent holding everything (a stable equilibrium broken only by the
    incumbent's crash/restart). **Fixed in `tandem-jdbc` (2026-07-02):** presence is now decoupled from
    ownership via `tandem_relay_member`, and the fair-share divisor counts live members, so the fleet
    rebalances to `B/2` on scale-up (LLD-jdbc §3.2; §8.2 above). S8's assertions are unchanged and still
    hold; a fair split is now the normal outcome.
11. **`WorkerPool.stop()` cannot simulate a crash under `LEASE`, so `kill()` was added** (§8.3) — `stop()`
    always calls `bucketSource.release()`, which for `LEASE` now does real, immediate cleanup (discovery
    #10), the opposite of what a crash test needs. `kill()` halts the threads/scheduler identically but
    skips `release()`, so ownership/presence go stale only via lease expiry, exercising the self-heal
    path against a live instance instead of driving `BucketSource.heartbeat()` by hand.
12. **"Disjoint + full coverage" is not a strong enough wait condition for "partition stabilized," and
    this class of bug appeared twice independently** (§8.3) — it is already true the instant the first
    instance to heartbeat claims everything, before any peer is even visible (round 1 of the multi-round
    converge algorithm). S8's first 3-instance run "killed" a victim owning zero buckets as a result; the
    *exact same* latent flakiness turned out to already be present in `EmbeddedLeaseIT`'s own await
    condition (found by re-running it until it reproduced, not by inspection). Fixed in both places by
    additionally requiring every instance to hold at least half of an even share before treating the
    partition as stable.
