# Tandem — Load & Performance Testing Plan

**Version:** 1.2  
**Status:** Implemented & smoke-verified (2026-07-02) — see [LLD-benchmark.md](LLD-benchmark.md)  
**Companion to:** HLD §10 (Non-Functional Requirements)

This note specifies how Tandem's throughput and latency KPIs (HLD §10) are measured and
verified. It defines the metrics precisely, the harness, the scenarios, the pass/fail
thresholds, and the measurement-integrity rules. The runnable harness (module
`tandem-benchmark`) is now built and passes its CI smoke suite against real Docker
containers; [LLD-benchmark.md](LLD-benchmark.md) is the as-built companion and carries a
handful of implementation-level corrections and discoveries (§12 there) this document
defers to.

---

## 1. KPIs under test

The two §10 numbers this plan validates:

| KPI (HLD §10) | Stated target |
|---|---|
| Throughput | Sustain **> 10k events/s per shard** with `batch_size=100`, `N=8` workers |
| Relay latency | **Median < 200ms** from `COMMIT` to Kafka ack under normal load |

### 1.1 How big is "10k/s per shard"?

The throughput target is deliberately **high** — it is a *capacity-demonstration* number,
not the load a typical adopter will generate. Read it in three registers:

- **Per shard vs aggregate.** 10k events/s per shard = one event every 100 µs; at the 1 KB
  reference payload that is ~10 MB/s on a single worker. Across `N=8` shards the aggregate
  target is **~80k events/s ≈ 80 MB/s**.
- **The bottleneck is the database, not Kafka.** A single broker handles far more than
  80k msg/s, so the broker is never the limit. The cost is on the write side: every event
  is a `SELECT … FOR UPDATE` + `INSERT` + `COMMIT`, and the plan mandates the real
  production producer config (`acks=all`, `enable.idempotence=true`, §3). Sustaining ~80k
  such transactions/s on the §5 reference host is a genuinely **high** load for a
  transactional-outbox pattern.
- **Relative to real systems.** For scale intuition:

  | Workload | Order of magnitude | Where Tandem sits |
  |---|---|---|
  | Typical CRUD / SaaS app | 10–500 events/s | vastly over-provisioned; one shard idles |
  | "Large" business system | 1k–5k events/s | a single shard suffices |
  | **Tandem target — per shard** | **10k/s** | needs the §5 baseline + tuning |
  | **Tandem target — aggregate (N=8)** | **~80k/s** | high-throughput territory |

  The great majority of outbox adopters never approach 10k/s **on a single shard**.

- **The shard is the unit of scale.** 10k/s is the throughput of *one* worker/bucket, not
  a system ceiling. Scaling is horizontal across buckets (Q8 virtual-bucket sharding):
  more shards ⇒ near-linear throughput until the shared DB saturates. Stating the KPI
  *per shard* expresses exactly this — the unit of parallelism holds the rate, and you
  multiply. This is also why scenario **S3 (hot partition)** matters: when one hot
  aggregate concentrates load on a single shard, that shard must hold its own 10k/s
  without leaning on the others.

### 1.2 KPI refinements (now incorporated into §10)

The original §10 targets were under-specified for a pass/fail gate. The following
refinements — surfaced by this plan — have been folded back into HLD §10:

- **Hardware baseline.** "10k events/s" is meaningless without a reference environment;
  §10 now states the targets against the baseline defined in §5 of this document.
- **Reference payload.** Throughput depends heavily on payload size; §10 now fixes a
  **1 KB JSON** reference payload.
- **Latency tail.** A median hides production-relevant tail behaviour; §10 now adds a
  **p99 < 1 s** bound alongside the **p50 < 200 ms** median. This plan additionally
  records p95 and p99.9 for diagnostics.
- **"Normal load" defined** as ≈50% of the measured sustainable maximum (scenario S2).
- **"Sustained" defined** as a rate held ≥ 10 min with **outbox lag age flat** (not
  growing) — so a burst rate is never mistaken for a sustainable one.

---

## 2. What is measured, and how

### 2.1 Throughput

**Definition:** events transitioned to `status=DONE` per second, in steady state.

- Measured **per shard** (the §10 unit) and reported **aggregate** (× N).
- Sampled from a **harness-local in-memory `TandemMetrics` implementation** (registered
  with the relay; `incrementPublished`) and cross-checked by counting `DONE` rows over a
  fixed window. The real Micrometer adapter (`tandem-micrometer`) is a future module; the
  benchmark does not depend on it.
- "Steady state" requires that the pending backlog is flat (not growing). **Correction
  (found during implementation, LLD-benchmark §6/§12):** no product code ever calls
  `TandemMetrics.recordLagAgeSeconds` — the relay does not self-report lag. The harness
  computes it itself with a direct SQL probe over `tandem_outbox` (`LagProbe`), the same
  mechanism S3's per-shard lag already used below.

### 2.2 Relay latency (COMMIT → Kafka ack)

**Definition:** wall-clock from the domain transaction's commit to the producer ack for
that event.

`t1` is captured by a **correlation consumer**: a Kafka consumer, co-located with the
harness, that receives every published event and records its receive time. The relay
itself records nothing — measurement needs no product hook. The delta therefore includes
the broker→consumer hop, so it slightly *over*-estimates the COMMIT→ack KPI — acceptable
and conservative. The same consumer doubles as the correctness verifier (§4):
per-aggregate `seq` monotonicity and zero-loss are asserted on the same stream.

Two choices for `t0`, in increasing accuracy:

- **Proxy (simple):** `t0 = created_at` (DB `now()` at INSERT). Caveat: `created_at` is
  set at *INSERT*, not *COMMIT*, so the delta additionally over-estimates by the
  INSERT→COMMIT gap.
- **Instrumented (accurate):** the producing harness records the commit timestamp itself
  and embeds it in an event header; the correlation consumer computes the delta on
  receive. Removes the INSERT→COMMIT skew and keeps both clock reads inside the harness
  process. Preferred for the official KPI run.

Latencies are recorded into an **HdrHistogram** to extract percentiles without
pre-aggregation loss.

### 2.3 Clock integrity

`t0` and `t1` must come from the **same clock** or synchronized clocks, or the latency
delta is noise. The instrumented path keeps both reads in the harness process (commit
timestamp and correlation-consumer receive time); the proxy path additionally involves the
DB clock (`created_at`). The reference run therefore co-locates DB, relay, broker, and
harness on **one host** (single monotonic clock). Distributed-clock runs are explicitly
out of scope for the latency KPI.

---

## 3. Harness & tooling

| Concern | Choice |
|---|---|
| Infrastructure | **Testcontainers** — real PostgreSQL + real Kafka (consistent with `tandem-test`/`TandemTestContainer`) |
| Load generator | Multi-threaded driver that mimics the domain path: `SELECT … FOR UPDATE` on a synthetic aggregate table, `version++`, then the outbox insert **through the real `tandem-jdbc` write-side API** (never a raw SQL `INSERT` — the library, not the database, is the system under test), `COMMIT` |
| Latency capture | Correlation consumer (§2.2) feeding an HdrHistogram (p50/p95/p99/p99.9), lossless |
| Throughput capture | Harness-local in-memory `TandemMetrics` implementation (§2.1) + direct `DONE` row counts |
| Producer config | `acks=all`, `enable.idempotence=true` (the mandated production config — load tests must run the real config, not a faster one) |
| Reference payload | 1 KB JSON event body |

**Warmup & steady state:** discard a warmup window (JIT compilation, connection-pool
ramp, page-cache warm) before recording; detect steady state by a flat lag age over a
sliding window.

### 3.1 Alternatives considered (and why a custom driver)

Standard load-testing tools were evaluated and rejected — recording the reasoning here so
it is not re-litigated later:

- **k6, Locust** — run outside the JVM (Go/Python) and cannot call the Java write-side
  API. Driving raw SQL would benchmark PostgreSQL, not Tandem.
- **JMeter** — same JDBC-sampler problem; calling Java through a JSR223 sampler is a
  custom harness inside an awkward container.
- **Gatling (Java DSL + custom Action)** — the closest fit: it can wrap the real insert
  call and provides injection profiles and percentile reports. Rejected because (a) its
  report measures caller-side insert duration, not the COMMIT→ack KPI, which is observed
  at the correlation consumer; (b) S1 needs an *adaptive* ramp driven by lag-age
  feedback, while Gatling injection profiles are declared up front; (c) S3–S7 are
  orchestration/correctness scenarios where it adds nothing. What remains (insert-rate
  generation) does not justify the framework.
- **OpenMessaging Benchmark** — the standard for produce→consume benchmarks, but it
  requires a custom driver plus a distributed-deployment model designed for broker
  benchmarks; cost out of proportion to the benefit (Pareto).
- **`kafka-producer-perf-test`** — measures the broker, not Tandem.
- **JMH** — the right tool for a *micro*-benchmark of the insert hot path, but not a
  sustained-load / end-to-end harness; it may complement this plan, not replace it.

---

## 4. Scenarios

| ID | Scenario | Validates | Method |
|---|---|---|---|
| **S1** | Sustained max throughput | Throughput KPI | Ramp offered insert rate until lag age stops being flat; report the highest rate with stable lag, per shard and aggregate |
| **S2** | Latency at normal load | Latency KPI | Hold offered rate at ≈50% of S1 max; record COMMIT→ack p50/p95/p99/p99.9 over ≥10 min |
| **S3** | Hot partition / skew | Per-shard lag isolation; ordering under skew | Skewed `aggregate_id` distribution so one shard is hot; confirm hot-shard lag is visible and other shards unaffected. Per-bucket lag is measured by the harness with direct SQL over pending rows — the product exposes no per-shard lag metric today (product-side gap tracked in the backlog, Admin API / observability) |
| **S4** | Saturation / backpressure | Graceful degradation; no loss | Drive beyond S1 max; confirm lag age rises predictably, no rows lost, no ordering violation, recovery when load drops |
| **S5** | Worker failover | Lease reclaim; at-least-once | Kill a worker mid-load; measure reclaim time (`lease_expired.count`), duplicate count, time to drain backlog |
| **S6** | Poison message | Per-aggregate blocking | Inject a permanently-failing event; confirm only its aggregate blocks (`failed.count`), other aggregates keep flowing, throughput impact bounded |
| **S7** | Causal-ordering overhead (opt-in) | Cost of the Lamport feature | **Deferred — 2nd round:** requires the causal-ordering feature (`tandem_aggregate_clock`) to be implemented. Run S1/S2 with the Lamport clock on vs off; report throughput/latency delta |

Each scenario asserts **zero ordering violations** per aggregate (consumer verifies
`seq` is strictly increasing per `aggregate_id`) and **zero lost events** (every committed
event eventually reaches `DONE` and the broker).

---

## 5. Reference environment (baseline)

The KPIs are only meaningful against a fixed baseline. The official run uses:

| Resource | Reference spec |
|---|---|
| Host | Single machine, ≥ 8 physical cores, ≥ 32 GB RAM, NVMe SSD |
| PostgreSQL | Version 16, default-tuned + `shared_buffers` sized to host, on local NVMe |
| Kafka | Single broker (KRaft), local |
| Co-location | DB + relay + broker on the same host (single clock; see §2.3) |
| Workers | `N=8`, `batch_size=100` |
| Payload | 1 KB JSON |
| JVM | Temurin 25 (current LTS), server JIT, fixed heap, G1 |

CI runners do **not** meet this baseline; official KPI numbers come from a dedicated
reference host, not from GitHub Actions (see §7).

**JVM note.** The harness and the system under test share one JVM (the driver, relay, and
correlation consumer run in-process — required for the single-clock latency capture, §2.3),
so the baseline JVM *is* the benchmark module's toolchain: **JDK 25** (LLD-benchmark §2,
chosen so the load driver can use virtual threads). Measuring on a current LTS is also more
representative of production, where adopters run Java 21/25 — while the Tandem libraries
themselves keep their Java 17 baseline for consumers.

### 5.1 Developer-machine / smoke runs (not a KPI gate)

Running the harness on a laptop is expected and useful — but **only for correctness and
scenario behaviour, never for KPI numbers.** Two effects put a developer machine one to
two orders of magnitude below the §5 baseline, well beyond the raw core/RAM gap:

- **Docker-on-macOS/Windows runs the containers in a VM.** Testcontainers starts
  PostgreSQL and Kafka in Docker, which on non-Linux hosts is virtualized. The outbox
  throughput is governed by durable-`COMMIT` (`fsync`) rate, and `fsync` latency through
  the VM I/O layer is far worse than on bare-metal NVMe. (Also check Docker Desktop's
  CPU/RAM allocation — its defaults are small and cap the containers artificially.)
- **The generator shares the host with the system under test.** The load driver and
  correlation consumer contend for the same cores as the relay (`N=8` workers), Kafka, and
  PostgreSQL, so the measured maximum is limited by the generator too, not just Tandem.

**Rough expectation on a typical developer machine** (e.g. a 6-core / 16 GB laptop with
NVMe, containers under Docker Desktop), with the mandated production producer config
(`acks=all`, `enable.idempotence=true`): aggregate throughput in the **low thousands of
events/s** — roughly **1–2k/s per shard, ~5–10× under target** — the durable-`fsync` rate
of the Dockerized PostgreSQL being the dominant limiter. COMMIT→ack median can stay under
200 ms at *low* offered rates, but the tail is spiky (VM I/O jitter, GC under contention).
These are order-of-magnitude estimates, not thresholds.

**What a developer machine *does* validate faithfully** (hardware-independent): zero
per-aggregate ordering violations and zero lost events, and the scenario behaviours of
S3–S6 (hot-shard isolation, predictable saturation/recovery, failover reclaim with
bounded duplicates, poison-message containment). This is the §7 *smoke* variant: assert
correctness, ignore the KPI numbers.

**Database scope:** the baseline — and this plan's first execution — targets PostgreSQL 16
only. The MySQL repetition is deferred until the MySQL DDL lands (2nd round, open
question Q28).

---

## 6. Pass/fail thresholds

| KPI | Pass condition (against §5 baseline) |
|---|---|
| Throughput (S1) | Sustained ≥ 10k events/s **per shard** with stable lag for ≥ 10 min |
| Latency median (S2) | COMMIT→ack **p50 < 200 ms** |
| Latency tail (S2) | COMMIT→ack **p99 < 1 s** (per §10) |
| Correctness (all) | Zero per-aggregate ordering violations; zero lost events |
| Failover (S5) | Backlog fully drained after reclaim; duplicates **≤ `batch_size` per killed worker** (the per-shard in-flight window: a crash after ack but before `markDone` can redeliver at most that window) and idempotently absorbable |

**Implementation note (S5).** `tandem-jdbc`'s `WorkerPool` exposes no API to kill a single
worker thread among several — only whole-instance `stop()`/`start()`. The harness's S5
therefore simulates an *instance* crash, not a single-*worker* crash, so it can only verify
the wider, conservative bound `batch_size × workers` rather than the tighter
per-worker `batch_size` above (LLD-benchmark §8). The per-worker bound remains the design
target; closing this gap would need new `tandem-jdbc` surface to kill one worker in
isolation.

A run fails the gate if any throughput/latency threshold is missed on the baseline, or if
*any* scenario records an ordering violation or a lost event (correctness is
non-negotiable regardless of performance).

---

## 7. Execution & CI

- Load tests live in the dedicated, **non-published** module **`tandem-benchmark`** —
  kept out of the normal `test`/`check` lifecycle.
- Triggered by an explicit task (`./gradlew :tandem-benchmark:loadTest`), **not** part of PR CI: they
  are slow, resource-hungry, and would be flaky on shared runners.
- Run on a schedule (nightly/weekly) on the reference host, or on demand before a release;
  results (throughput numbers, latency histograms) are archived for regression tracking.
- A *smoke* variant (tiny rate, short duration) **does** run — `SmokeLoadTest`
  (`@Tag("integration")`) covers S1, S3, S5, S6 against `BenchmarkConfig.toSmoke()`, purely
  to keep the harness compiling and wired; it asserts correctness, never KPI numbers.
  Measured wall-clock on a developer machine: **~106 s** (LLD-benchmark §9); most of that
  is deliberate idle time (S5's row-lease wait with the relay stopped, S3's drain-tail
  polling), not CPU load — consistent with §5.1's point that a developer-machine run is
  correctness-only, never a throughput demonstration.

---

## 8. Module placement

**Decision:** a dedicated **`tandem-benchmark`** module (internal, not published to Maven
Central — same pattern as `tandem-sample`). It depends on `tandem-jdbc`, `tandem-kafka`,
and `tandem-test` (for the Testcontainers harness). Keeping it separate ensures the heavy
benchmark dependencies (HdrHistogram, load-driver code) never leak into the published
artifacts.
