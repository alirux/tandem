# Tandem — `tandem-benchmark` LLD

**Version:** 0.4 (Implemented & smoke-verified; gauge, metrics, and tracing demos added)  
**Module:** `tandem-benchmark` · package `com.codingful.tandem.benchmark`  
**Depends on:** `tandem-test` (transitively pulls `tandem-core`, `tandem-jdbc`, `tandem-kafka`,
`kafka-clients`, and the Testcontainers Postgres+Kafka runtime), HdrHistogram, HikariCP,
`tandem-micrometer` (§6.3), `tandem-tracing-otel` + the OpenTelemetry SDK/OTLP exporter (§6.4); JUnit 6
+ AssertJ (test scope, for the CI smoke run).  
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
  src/main/resources/grafana/tandem-dashboard.json  // §6.3 — the provisioned dashboard, ≤2 panels per row
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
    FaultInjector.java / FaultInjectingDispatcher.java  // §8 — the fault seam: permanent/retriable/stall
    FaultInjectingOutboxStore.java       // §6.3 — the claim-side fault axis, for workers.cycle_age_seconds
    MetricsExporter.java                // §6.3 — one relay instance's Prometheus /metrics endpoint
    ObservabilityStack.java             // §6.3 — Prometheus + Grafana containers, provisioned
    MetricsDashboardDemo.java           // §6.3 — the scripted run behind metricsDashboardDemo
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

## 6. `BenchmarkMetrics` — the relay's own readings; `LagProbe` — an independent one

Two sources of the same figures, kept deliberately separate so each can check the other:

- **`BenchmarkMetrics`** implements `TandemMetrics` and is registered with the relay, so it receives
  whatever the relay reports: the counters (`publishedCount`, `retryCount`, `leaseExpiredCount`,
  `configInvalidCount`, plus `publishedSinceLast()` — a sample-and-reset counter for a throughput
  window) and the gauges the relay reads periodically (backlog, backlog age, **failed-row count**,
  live workers; LLD-jdbc §4) — `failedCount` is a live read of `OutboxStore.failedCount()`, not a
  tally of failure events, so it can drop back to zero once a stuck row is resolved. A gauge reads
  `-1` until the relay has reported one, which is itself informative: a stopped relay reports nothing
  at all.
- **`LagProbe`** (§6.1) computes backlog signals from `tandem_outbox` directly, without going through
  the product at all. It stays for three reasons: it is the harness's **independent** check on what
  the relay reports (§6.2), it answers **per-bucket** questions the port still cannot (S3), and it
  works **while the relay is stopped**, which several scenarios need.

### 6.1 `LagProbe` — direct-SQL lag observation

The core port reports a global backlog reading but no **per-bucket** one (product-side observability
gap, tracked in the project backlog), and its readings only exist while the relay runs. `LagProbe`
therefore queries `tandem_outbox` directly for everything it needs:

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

### 6.2 `LagGaugeDemo` — showing what the gauges look like

`./gradlew :tandem-benchmark:lagGaugeDemo` prints the relay's lag gauges over a scripted run. It
**measures nothing and gates nothing**, and it is deliberately not a test: its purpose is to let the
shape of the signal be judged by looking at it, which a passing assertion cannot do. A test proves a
value matched an expectation; it says nothing about whether the metric is *useful* to someone
watching a dashboard — whether the scale, the units, the update frequency and the behaviour under
real load make sense. That judgement needs the actual output.

The run has three phases, sampled every 500ms. The reading interval is lowered from its 10s default
for the same reason the demo exists: at 10s the whole build-up and drain would fall between two
readings and the curve would collapse into a handful of points.

1. **relay down** — load runs with no relay at all. Only `LagProbe` sees anything.
2. **draining** — the relay starts and works off what piled up.
3. **steady load** — a rate the relay absorbs comfortably, then a settling window so the inserts still
   in flight land before the generator closes (closing interrupts them mid-transaction, which litters
   the report with driver stack traces).

Each line prints the relay's own last reported reading beside `LagProbe`'s SQL taken at that instant,
so the two independent implementations of the same two figures can be read against each other. The
program prints its own legend, reproduced here in full because the column names alone do not carry
their meaning:

```
What the columns mean
  time      seconds since the run started
  phase     what is being done to the relay at that moment
  waiting   events still to be published (rows in status PENDING)
  oldest    how long the oldest of those events has been waiting, in seconds
  workers   relay worker threads alive

  The left block is what the RELAY ITSELF last reported through TandemMetrics —
  what a dashboard would show, up to one reading interval old, and '-' until the
  relay has reported at all. The right block is the same two figures recomputed by
  the harness with its own SQL, at the instant the line is printed.

                      |   reported by the relay     |   measured now
   time  phase        |   waiting   oldest workers  |   waiting   oldest
------------------------------------------------------------------------------
   0.0s  relay down   |         -        -       -  |         0      0.0
   0.5s  relay down   |         -        -       -  |       202      0.5
   4.1s  relay down   |         -        -       -  |      1618      4.1
   8.2s  draining     |         -        -       -  |      3249      8.2
   8.7s  draining     |      2494      8.4       8  |      2666      8.7
   9.2s  draining     |      1223      7.4       8  |      1421      8.2
   9.7s  draining     |       287      6.8       8  |       270      6.8
  10.2s  draining     |        60      5.9       8  |        46      5.6
  10.8s  draining     |         5      4.2       8  |         4      4.2
  11.3s  draining     |         0      0.0       8  |         0      0.0
  27.0s  draining     |         0      0.0       8  |         0      0.0
  31.5s  steady load  |         4      0.1       8  |         4      0.1
  36.1s  steady load  |         3      0.1       8  |         4      0.1
  45.2s  settling     |         0      0.0       8  |         0      0.0
```

How to read it, and what the run establishes:

- **Until 8.2s the relay is down, so its half of the table is empty.** The backlog is real and
  growing — the probe watches it reach 3249 events aged 8.2s — but the relay reports *nothing*,
  because the reading is taken by the relay itself. This is the shape of the one failure these gauges
  cannot cover on their own, and the reason HLD §7's alerting guidance also requires alerting on a
  **stale or absent** reading, not only on a high one.
- **At 8.7s the relay starts and immediately reports** what it inherited: 2494 waiting, oldest 8.4s,
  8 workers alive. The first reading is taken at startup rather than one interval later precisely so
  that this moment is visible.
- **While draining, `oldest` falls** — 8.4 → 7.4 → 6.8 → 5.9 → 4.2 → 0 — because each pass removes the
  oldest rows, so the oldest *remaining* row is younger every time. A backlog age that drops is the
  system catching up; one that climbs is the alerting condition.
- **Steady state is a handful of rows at ~0.1s.** That is one poll cycle's breathing, not a problem —
  useful to know before setting an alert threshold, since a naive "waiting > 0" rule would fire
  constantly.
- **The two sources agree wherever the figure is stable** (287 vs 270, 5 vs 4, 0 vs 0, and throughout
  steady state) **and diverge only mid-drain** (2494 vs 2666), where the backlog halves between one
  sample and the next so the two readings land at genuinely different instants. That divergence is the
  reading interval making itself visible, not a computation error — and the agreement everywhere else
  is the strongest available check that the relay's query and the harness's independent SQL compute
  the same thing.

### 6.3 `MetricsDashboardDemo` — every meter, on a real dashboard

`./gradlew :tandem-benchmark:metricsDashboardDemo` runs the relay against a real Micrometer →
Prometheus → Grafana pipeline and holds it open (press Enter to stop, or pass
`--args="--hold=<seconds>"`). Same intent as §6.2 one level up: §6.2 prints two numbers to a terminal,
this shows all of them on the surface an operator actually watches. It **measures nothing and gates
nothing** — its purpose is that a signal's *usefulness* can only be judged by looking at it.

**Shape.** Two `MetricsExporter`s — a `PrometheusMeterRegistry` behind the JDK's own `HttpServer`, no
Spring or Actuator — publish `/metrics` for one relay instance each; `ObservabilityStack` starts
Prometheus (scraping both host ports through `host.testcontainers.internal`, 1s interval) and Grafana
with a provisioned datasource and the committed dashboard. **One exporter per instance, deliberately:**
`workers.active` is per-instance while `lag.count` is a global reading every instance reports, so a
single shared registry would silently collapse the first kind to whichever instance wrote last.

Nine scripted phases walk the relay through the states each meter exists to reveal: a backlog with no
relay behind it, a drain, steady load, an aggregate that fails, two unserialised writers to one
aggregate, a second instance joining, that instance's worker getting stuck without crashing, a genuine
crash with rows still in flight, recovery.
`tandem.relay.config.invalid` is the one meter not driven — it fires once and aborts the process by
design (LLD-micrometer §4), leaving nothing for a scraper to read.

Driving the failure paths needed `FaultInjector` to grow from one fault to three, plus a second,
independent axis. The original permanent-only fault could never move `retry.count`, and no fault at
all could put rows in the state lease reclaim needs to see:

| Fault | What it does | What it makes visible |
|---|---|---|
| `PERMANENT` | fails with `retriable = false` | `failed.count`, and the blocked chain behind it |
| `RETRIABLE` | fails with `retriable = true` | `retry.count`, then `failed.count` once attempts run out |
| `STALL` | returns a future that never completes | rows pinned `IN_FLIGHT` under a row lease, so `lease_expired.count` can be exercised at all |

`STALL` is what finally exercised the reclaim path: §8.1 records that S5's interesting case
(`reclaimed > 0`) had never fired across runs, because a dispatch that merely fails releases its row
long before any crash. A dispatch that hangs does not.

**`seq_regression.count` is the one phase that provokes nothing at all — it stages a real bug.**
`OutOfOrderWriter` writes two events for one aggregate from two concurrent transactions and commits the
later `seq` first, holding the earlier one open while the relay publishes. No fault injector is
involved and nothing is simulated: both events go through the real `JdbcOutboxRepository`, and the
inversion comes from where it comes from in production — `id` assigned at INSERT, visibility decided at
COMMIT. It is deliberately not a `Fault`, because it is not a relay failure to inject; it is a
*write-side* precondition violation (HLD §4.2), and the point of the phase is that the relay is the
only thing that can see it. It uses a dedicated aggregate id outside the generator's universe so it
owns its own `seq` numbering, and reuses `TransactionalUnitOfWork` (one instance per thread, each
binding its own connection) rather than duplicating the no-close connection proxy.

> **A real defect this phase caught on its first run, worth keeping.** The demo showed a flat zero:
> `FaultInjectingOutboxStore` forwards every `OutboxStore` method to the delegate *except* the newly
> added `replaysOf`, so it silently fell back to the port default — "unknown" — which the relay reads
> as "cannot rule out a replay" and therefore suppresses. Detection was off for everything running
> behind the decorator, with no error anywhere. Any delegating `OutboxStore` has the same trap, and
> nothing in the type system catches it, since the default makes the method optional to override. The
> fix is one forwarding method; the lesson is that this class of signal cannot be verified by unit
> tests alone, which is what the phase is for.

**A fourth fault, on a different axis entirely, was needed for `workers.cycle_age_seconds` (LLD-jdbc
§3.8) — and none of the three above could reach it.** Every `Fault` above acts on the *dispatch* path,
which `WorkerPool` calls asynchronously: `claimAndDispatch` hands off a `CompletableFuture` and returns
immediately, so even `STALL` leaves the worker's claim/flush cycle completing normally forever —
`workers.cycle_age_seconds` would read a flat zero no matter how long a dispatch hangs. The gauge exists
for a worker stuck *before* dispatch, synchronously, inside the claim call itself (the JDBC-call-that-
never-returns case) — which needed a fault on `OutboxStore.claimBatch`, not `OutboxDispatcher.dispatch`.
`FaultInjectingOutboxStore` wraps the real store and throws from `claimBatch` when `FaultInjector`
currently stalls the calling `workerId`; `runWorker`'s catch block logs it and does not stamp a progress
cycle, which is exactly what leaves the worker thread alive (`workers.active` unchanged) while its cycle
age climbs. Selected **per relay instance**, not per aggregate: a claim never sees a record, so the
existing aggregate-scoped rule has nothing to match against, and instance is the only axis that makes
sense for a claim-side fault. Has no dedicated `S`-numbered scenario — used only by this demo.

**The fault is per-aggregate, not per-instance, so `lease_expired.count` can climb more than once from
one `STALL` call.** `stallAggregate` stays armed until `Recovery` explicitly clears it (§3: `BenchmarkEnvironment` holds
one `FaultInjector`, shared by every dispatcher `newRelayInstance` builds). Whichever
instance next claims that aggregate's head-of-chain row — including the survivor, once it inherits the
dead instance's buckets — hangs on it too, and its own `rowLease` eventually expires and gets reclaimed
the same way. Watched directly on a run with a 40s crash-to-recovery window and `rowLease = 20s`:
reclaims landed at t+0s (the killed instance's own abandoned row), then again at +20s and +40s (the
survivor re-claiming, stalling, and losing its own lease) — three events, 20s apart, from a single
`stallAggregate` call, stopping only once `Recovery` calls `clear()`. Not a bug: it is what "the fault
follows the aggregate, not the process" actually looks like when the crash-to-clear window is a few
multiples of `rowLease` rather than a fraction of it.

**What the first real runs established** — the reason the demo exists, and none of it visible in a
passing assertion:

- **A single `FAILED` row breaks both lag gauges permanently.** One poisoned aggregate left a run at
  1 530 rows `DONE`, 1 `FAILED` and 73 `PENDING` — all 73 behind that one failure, since a `FAILED`
  head blocks its chain for good. `lag.count` never returns to zero and `lag.age_seconds` climbs at one
  second per second for ever, drawing a perfectly straight line on the dashboard, while the relay
  delivers every other aggregate normally. HLD §7's own alerting rule (`lag.age_seconds > 60s → relay
  stalled or under-provisioned`) would therefore latch on after the first poison message and never
  clear. This is what `blocked.count` was added for, and the dashboard's "waiting: claimable vs blocked"
  panel is where the distinction is read. (First observed at a ~10× larger offered rate, with the same
  shape and the same straight line — the effect is structural, not a function of volume.)
- **Backlog drain time is governed by depth-per-aggregate, not `batchSize`.** The claim already returns
  at most one row per aggregate (structural per-aggregate ordering), so a shallow backlog drains in a
  single claim cycle regardless of how small `batchSize` is — the original demo config (20 events/s for
  20s, depth ~25 across 16 aggregates) fell from ~400 to ~0 within a single 1s scrape sample, too fast to
  read on the panel. Cutting `batchSize` from 20 to 4 barely helped (402 → 14 within one second, still a
  snap): with `workers` held fixed, `batchSize` only changes how many claim/dispatch/mark round trips one
  depth level costs, and a local Postgres+Kafka round trip is a few milliseconds — cheap enough that even
  several extra round trips per depth level are still sub-second. What actually worked was making the
  backlog **deeper**: raising the burst rate (20 → 30 events/s) and stretching the no-relay window (20s →
  60s) to depth ~112 drained as a clean multi-point staircase — 1801 → 1493 → 1103 → 711 → 333 → 0 over
  ~6 seconds, queried directly against Prometheus, not eyeballed off the panel.
- **`bucket.uncovered` is a sustained-stall detector, not a failover detector.** At 1s resolution across
  a whole run it was non-zero exactly twice: 56 buckets for ~5s when the relay started onto an inherited
  backlog, and 16→27 for ~5s while a joining second instance rebalanced. It never moved during the
  crash. The gap between a dead owner's lease expiring and the survivor's next heartbeat claiming those
  buckets is bounded by `reclaimInterval` (5s default) — routinely shorter than one reading. The demo
  therefore leaves `reclaimInterval` at its default rather than shortening it the way S8 does: a
  sub-second value would close the gap faster than any reading could sample it, and the gauge would read
  a flat zero through a failover that genuinely happened.
- **`lag.count` must never be summed across instances.** Two relays reported 46 and 45 at the same
  moment — the same global backlog, read a beat apart. A panel using `sum()` would have claimed 91 rows
  waiting when there were 46. The dashboard uses `max()` for every global gauge and `sum()` only for the
  per-instance counters.
- **A crashed relay must stop being a scrape target, not report zeroes.** Killing only the worker pool
  left the exporter alive, so Prometheus kept scraping frozen gauges — `workers.active` still reading 8
  for an instance that was gone. The demo now closes the crashed instance's endpoint with it, which is
  what a dying process does, and makes "relay instances reporting" drop from 2 to 1.
- **Every meter's `# HELP` is empty**, because `MicrometerTandemMetrics` never calls
  `.description(...)`. Cosmetic, additive, not yet done.
- **`publish.latency` spikes to tens of seconds during the backlog-heavy phases, and that is correct,
  not a bug.** A run showed p99 climbing past 22s right after the initial no-relay backlog drained, and
  again past 29s after the crash/recovery phase — because a row published at that instant had been
  sitting `PENDING` since before any relay existed, or since before the survivor took over the crashed
  instance's buckets, and the metric is `created_at` (insert) to ack, not dispatch-call to ack. The
  panel's own description says as much (an `INSERT`→ack proxy, HLD §10), but seeing a healthy-looking
  relay post a 30s p99 is the kind of thing worth confirming against the backlog panel before assuming
  a regression: `histogram_quantile(0.99, sum(rate(tandem_outbox_publish_latency_seconds_bucket[30s]))
  by (le))`, queried directly against Prometheus during steady load with no backlog to drain, read a
  much more ordinary p50 ≈ 62ms / p95 ≈ 123ms / p99 ≈ 1.6s on this hardware.
- **The `config.invalid` panel was dropped, not left empty.** It fires once, immediately before the
  relay aborts (LLD-micrometer §4), so this demo — which never triggers that path — can only ever show
  it as "never seen." A panel that is permanently empty by construction teaches nothing by being
  looked at; the meter itself is unaffected and still documented in the meter-mapping table (§2 of
  LLD-micrometer).
- **Phase markers (a vertical line per phase) took two independent fixes, and the second was the
  Grafana version.** Annotations are posted to Grafana's HTTP API at each phase boundary. Two things
  had to be true before any line appeared, and each failed *silently* — the API returned 200, the
  frontend fetched the annotations, and nothing was drawn:
  1. **The annotation must carry `dashboardUID`.** A global annotation matched by a dashboard-level
     `type: "tags"` query is fetched but not painted. Every dashboard load also fires an *implicit*,
     always-on query scoped to `dashboardUID == <this dashboard>`; scoping the POST to it is what
     draws the line. That makes a custom tag query redundant (it would double-render the marker), so
     the dashboard JSON declares no `annotations.list` entry at all.
  2. **Grafana must be ≥ 11.6.0.** With (1) already correct, 11.3.0 still painted nothing;
     A/B-verified on this exact dashboard against the same Prometheus and the same annotation — lines
     on 11.6.0, none on 11.3.0. The container image is pinned accordingly.
- **A 2s refresh needs two settings, neither of them the dashboard's `refresh` field alone.** Grafana
  enforces a server-side floor (`min_refresh_interval`, default 5s) — set via
  `GF_DASHBOARDS_MIN_REFRESH_INTERVAL` — *and* its refresh picker offers a fixed list that starts at
  5s, so 2s must additionally be declared in the dashboard's `timepicker.refresh_intervals`. With only
  the `refresh` field set, the dashboard silently falls back and the interval is not even selectable.
- **The offered rate had to come *down* for the demo to read well, which is why §6.3's config is not
  `toDemo()`.** An early version built the opening backlog at 300 events/s, and the blocked chain then
  drew as a thin sliver against a ~6000-row burst peak on a shared y-axis: correct (verified by direct
  Prometheus queries — `lag_count == blocked_count`, `claimable == 0`) but only legible by zooming the
  panel. At 20 events/s the burst peaks near 400 and the blocked chain settles around 70, which reads
  on the same axis without any interaction. **`aggregateCardinality` has to fall with the rate**: the
  blocked chain grows at roughly `rate / cardinality`, so a small rate over a large aggregate universe
  leaves a row or two behind the poisoned aggregate — technically correct and visually nothing. This
  demo is sized for legibility; throughput belongs to `LoadTestRunner`.

### 6.4 `TracingDashboardDemo` — one real trace, write to consume

`./gradlew :tandem-benchmark:tracingDashboardDemo` runs one relay instance against a real OpenTelemetry
SDK → OTLP → Tempo pipeline, read on the same Grafana `MetricsDashboardDemo` uses (§6.3) over a second,
Tempo-backed datasource. Closes out HLD-tracing.md §9's verification item (slice 6) — the design was
fully specified once the Spring/Micrometer and OpenTelemetry adapters existed (item 5's slices 1–5,
`docs/HLD-tracing.md`), and this demo needed no further port work, only assembly. Same intent, same
non-goal as §6.2/§6.3: it **measures nothing and gates nothing** — a signal's usefulness is judged by
looking at it, and a trace is the one signal a metrics dashboard cannot show at all.

**Shape.** `ObservabilityStack` gained an optional third container, Tempo, and a second constructor
parameter (`withTracing`) — `false` reproduces the metrics-only stack byte-for-byte, so
`MetricsDashboardDemo` is untouched. A real `OpenTelemetrySdk` (W3C propagator, `BatchSpanProcessor`
over `OtlpHttpSpanExporter`, 1s schedule delay so a live demo doesn't wait out the 5s SDK default) feeds
both `OtelTracePropagator` (write side) and `OtelTandemSpanRecorder` (wired into this demo's one
`RelayInstance` via a new `BenchmarkEnvironment.newRelayInstance(RelayConfig, TandemMetrics,
TandemSpanRecorder)` overload — every other scenario keeps the free `TandemSpanRecorder.NOOP` default).
`LoadGenerator` gained two nullable collaborators, mirroring how `CommitTimestamps` was added earlier:
a `TracePropagator` (passed straight into `JdbcOutboxRepository`'s existing 3-arg constructor) and a
`WriteSpanScope` — a generic `(aggregateId, Runnable) -> void` seam, not an OpenTelemetry type, so the
harness's shared driver stays off the tracing SDK the way the product's own ports do. Every other call
site passes the two no-op defaults and is unaffected.

**Four span kinds stitch one trace**, and only two of them are product code:

| Span | Kind | Emitted by |
|---|---|---|
| `tandem.benchmark.write` | `INTERNAL` | This demo — opens it around each unit of work (`WriteSpanScope`), current on the thread that calls `repository.insert`, so `OtelTracePropagator.capture()` has something real to read. Not shipped: a real caller's own domain-transaction span plays this role, which is why HLD-tracing.md never specifies one — Tandem only ever captures whatever is already current. |
| *(the outbox dwell)* | — | Nobody. The gap between the write span ending and the publish span starting is deliberately unspannned — the gap itself, read on the waterfall, is what makes the async boundary HLD-tracing.md §1 describes visible. |
| `tandem.relay.publish` | `PRODUCER` | `OtelTandemSpanRecorder` — real product code, instrumented mode. |
| `tandem.benchmark.consume` | `CONSUMER` | This demo — extracts `traceparent`/`tracestate` back out of the Kafka headers the relay already copied the row's captured context into (HLD-tracing.md §3: no product change needed for propagation to reach here) and opens a child span. A real consumer instrumenting itself would do the same thing. |

**Correlation id, shown without standing up the Admin API.** Every unit of work carries
`correlation-id = "biz-" + aggregateId` — genuinely 1:n, since an aggregate's later events share it —
captured the dependency-free way via `TandemContext` and merged with the trace context through
`TracePropagator.composite(...)` (HLD-tracing.md §4.1, item 5's slice 7). The run prints the exact
`tandem outbox search --correlation-id <id>` an operator would run next. Running `tandem-admin` inside
this benchmark was considered and rejected: the relay here is assembled by hand without Spring (§3), and
standing up a Spring Boot admin app would be far more machinery than the join key needs to demonstrate.

**A real bug, found only by running it, not by inspection:** Tempo 2.7.2's OTLP/HTTP receiver defaults
to binding `localhost:4318` **inside the container** when the config leaves `endpoint` unset — confirmed
via `docker logs` (`endpoint=localhost:4318`). Every export from outside — including through Docker's
own published port mapping — failed with `Connection reset`, not a clean refusal, because the TCP
handshake completes against Docker's proxy before the loopback-only bind inside drops it; the JVM's
OTel SDK logged `SEVERE: Failed to export spans` on a tight retry loop and no trace ever reached Tempo,
even though the container's own `/ready` health check passed throughout. Fixed with an explicit
`endpoint: 0.0.0.0:4318` in the generated `tempo.yml`. **Verified past "it compiled":** queried Tempo's
own `/api/search` and `/api/traces/<id>` directly against a live run — real traces present, correct
span kinds, correct parent-child linkage (all four spans share one trace id), the outbox dwell gap
visible as real elapsed time between the write span ending and the publish span starting (~40ms on this
Mac), and `tandem.correlation_id` landing on every span as designed.

**Direction asymmetry, unlike §6.3's Prometheus scraping.** Prometheus reaches *into* the JVM
(`host.testcontainers.internal`) to scrape it; the JVM pushes spans *out* to Tempo's own mapped port
like any other OTLP client of a container — the two signals in this shared stack travel in opposite
directions, which is also why only the metrics side needed `Testcontainers.exposeHostPorts`.

**The traces dashboard panel** is a `type: "table"` panel running `queryType: "traceql"` with the query
string `{resource.service.name="tandem-benchmark"}` — a list of recent traces whose Trace ID column
carries the datasource's internal data link, so one click opens the full waterfall. Neither half of that
pairing is interchangeable with the plausible-looking alternative (discovery 14 below), and no
annotation mechanics (§6.3's hard-won `dashboardUID` lesson) apply here: a trace list has no timeline to
paint a vertical line on.

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

**No scenario measures idle-path dispatch latency, and S2 is not it.** S2 holds ~50% of a sustainable
rate for its whole window, so its buckets are continuously busy — and a busy worker never sleeps
`pollInterval` (it claims back-to-back while work remains, LLD-jdbc §3.1). S2 therefore measures the
`claim → encode → publish → consume` path with the discovery term already at ~0, which is the right
thing for a "latency at normal load" number but leaves the **other** regime unmeasured: a bucket that
was drained, whose worker is sleeping, pays a discovery delay uniform in `[0, pollInterval]` (mean ~50 ms
at the default) before its first row is even seen. That regime is what any post-commit wakeup mechanism
would improve, so **there is currently no measurement that would show such an improvement — or its
absence** (dispatch-latency.md, Q-D). A scenario for it would need a shape none of S1–S8 have: drive a
low, sparse rate (one event every few seconds per aggregate, well below any drain rate) and report the
distribution of `commit → first claim`, not just the end-to-end percentile. Deliberately not added in
this round — the mechanism it would evaluate is itself undecided, and an idle-path number is only
meaningful against the §5 reference baseline, not a laptop.

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
  **The mechanism now exists, though:** `FaultInjector.stallAggregate` (§6.3) pins rows `IN_FLIGHT`
  deterministically instead of hoping the timing lands, and reclaim was observed firing that way. S5
  could adopt it whenever these parameters are tuned — racing a real crash against real dispatch latency
  is what has never worked.

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

### 8.4 A consumer/drain race in `verify` (surfaced in CI, 2026-07-22)

`SmokeLoadTest.s5WorkerFailoverSmoke` failed once in GitHub-Actions CI with `missing=1` (one produced
key never seen by the consumer), while passing on every prior run and locally. Root cause was a latent
race in the shared `ScenarioSupport.verify`, not in the product and not in S5 specifically: a scenario
waits for the **outbox** to drain (`waitForDrain` → all rows published) and then immediately diffs
`generator.insertedKeys()` against `consumer.receivedKeys()`. But the `CorrelationConsumer` polls Kafka
on its own thread and can lag the outbox drain by a poll cycle, so a just-published, not-yet-received
event reads as loss. It bit S5 first only because that scenario's stop/restart timing makes the last
event land right at the drain boundary; all six verifying scenarios (S1–S5, S8) shared the exposure.
**Fixed centrally in `verify`:** before diffing, wait a bounded grace (15s) for the consumer to catch up
to what was published. This never hides real loss — a genuinely dropped event never arrives, the wait
times out, and `verify` still reports it; in the happy path the check passes on the first poll and adds
no measurable time. No product code involved.

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
- **Gauge demo:** `./gradlew :tandem-benchmark:lagGaugeDemo` → `LagGaugeDemo.main` (§6.2). Also out of
  `test`/`check`, and out of `loadTest`: it is a ~50s look at the shape of the lag gauges, not a
  measurement.
- **Dashboard demo:** `./gradlew :tandem-benchmark:metricsDashboardDemo` → `MetricsDashboardDemo.main`
  (§6.3). Same status — out of `test`/`check` and out of `loadTest`. ~3 minutes of scripted phases, then
  it holds the Grafana stack open until Enter; `--args="--hold=<seconds>"` for a non-interactive run.
  Needs Docker for two containers beyond the usual Postgres/Kafka pair.
- **Tracing demo:** `./gradlew :tandem-benchmark:tracingDashboardDemo` → `TracingDashboardDemo.main`
  (§6.4). Same status — out of `test`/`check` and out of `loadTest`. ~40s of live traffic, then holds
  the Grafana traces view open until Enter; `--args="--hold=<seconds>"` for a non-interactive run.
  Needs Docker for three containers beyond the usual Postgres/Kafka pair (Prometheus + Grafana + Tempo).
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
`AggregateSelector`, `WriteSpanScope`), `CorrelationConsumer` + `LatencyRecorder`, `BenchmarkMetrics` +
`LagProbe`, `FaultInjector`/`FaultInjectingDispatcher`, `RampController`, scenarios S1–S6 and S8, the
`loadTest` entrypoint, the CI smoke test, and the three observability demos (`LagGaugeDemo` §6.2,
`MetricsDashboardDemo` §6.3, `TracingDashboardDemo` §6.4). PostgreSQL only.

**Out (later):** S7 causal-ordering overhead (needs the feature); MySQL repetition (needs the MySQL
DDL, open question Q28); distributed/multi-host runs (single-clock latency is out of scope,
HLD-load-testing.md §2.3); a `tandem-micrometer`-based telemetry path (stays the in-process
`BenchmarkMetrics` until that adapter exists); a Postgres session/instance-tuning hook in
`BenchmarkEnvironment` (§3); a real per-worker (rather than whole-instance) kill for S5; S8 with more
than two instances or a throughput-scaling comparison (currently correctness/partitioning only, §8.2);
any fix to the `LEASE` new-joiner-starvation finding (§8.2) — that is `tandem-jdbc` scope, not this
harness's; an idle-path dispatch-latency scenario (§8, dispatch-latency.md Q-D — the mechanism it would
evaluate is undecided, and the number needs the reference baseline to mean anything).

---

## 12. Discoveries during implementation (delta from the original design)

Recorded here so the reasoning isn't re-derived later. All were found by actually running the harness
against real Docker containers (`SmokeLoadTest`, later `LoadTestRunner --demo`, and the §6.2–§6.4
demos) rather than from the design:

1. **The lag signal had no product-side source at all** (§6) — the original design assumed
   `TandemMetrics.recordLagAgeSeconds` was the ramp signal, but nothing in the relay called it, so
   `LagProbe`'s direct SQL became the harness's only lag source. The relay reports the backlog itself
   since 2026-07-27 (LLD-jdbc §4); `LagProbe` stays as the independent cross-check (§6.2), for the
   per-bucket signal the port still lacks, and because it works while the relay is stopped.
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
13. **Tempo 2.7.2's OTLP/HTTP receiver binds `localhost` by default, not `0.0.0.0`** (§6.4) — found only
    by running `TracingDashboardDemo` and seeing every span export fail with `Connection reset` even
    though the container's own `/ready` health check passed. `docker logs` showed
    `endpoint=localhost:4318`; the loopback-only bind rejects a connection arriving through Docker's own
    published port mapping, and the JVM-side symptom (a reset mid-handshake) gives no hint that the fix
    is server-side config, not a networking or Testcontainers issue. Fixed with an explicit
    `endpoint: 0.0.0.0:4318` in the generated config.
14. **Grafana's traces *panel* and the `traceqlSearch` *query type* are both the wrong half of the
    obvious-looking pair** (§6.4) — the traces dashboard rendered an empty panel while Tempo's own
    `/api/search` returned the traces fine, so the fault was entirely in the dashboard JSON, and it was
    two independent defects at once. First, `queryType: "traceqlSearch"` builds its query from the
    target's `filters` array and **silently ignores the `query` string**; with no `filters` the
    generated query is empty, so the authored `{resource.service.name="tandem-benchmark"}` never ran. A
    TraceQL string only takes effect under `queryType: "traceql"`. Second, `type: "traces"` renders a
    *single* trace's span hierarchy, not search results — handed the tabular frame a search returns, it
    prints "No data found in response". A list of recent traces is a `table` panel; the Trace ID column's
    internal data link is what opens the waterfall. Both failures look identical from the outside (an
    empty panel, no error), which is why the combination survived until someone opened the dashboard.
