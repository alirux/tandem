<div align="center">

<img src="docs/tandem-logo-blackbg-shade.png" alt="Tandem logo" width="200" />

# Tandem

**Reliable, strictly-ordered event delivery from your database to Apache Kafka — no CDC, no Kafka Connect, no two-phase commit.**

[![CI](https://github.com/alirux/tandem/actions/workflows/ci.yml/badge.svg)](https://github.com/alirux/tandem/actions/workflows/ci.yml)
[![codecov](https://codecov.io/github/alirux/tandem/graph/badge.svg?token=YKA7T7YCFD)](https://codecov.io/github/alirux/tandem)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](#)
[![Maven Central](https://img.shields.io/maven-central/v/com.codingful/tandem-core.svg?label=maven)](https://central.sonatype.com/artifact/com.codingful/tandem-core)
[![Status](https://img.shields.io/badge/status-0.x%20—%20API%20evolving-yellow.svg)](https://github.com/alirux/tandem/releases)

**[tandem.codingful.com](https://tandem.codingful.com)**

</div>

## What is Tandem?

Tandem is a Java library that implements the **Transactional Outbox Pattern**. You insert an
event into an `outbox` table **inside the same transaction** that mutates your domain — so the
write is atomic by your database's ACID guarantees, with no dual-write and no distributed
transaction. A separate **relay** then polls the outbox and publishes to Kafka, at-least-once,
preserving per-aggregate ordering.

<img src="docs/tandem-architecture.png" alt="Tandem architecture: your application writes the domain change and the outbox row in one transaction to PostgreSQL; the Tandem relay polls tandem_outbox, publishes to Apache Kafka keyed by aggregate_id, and marks the row done — no CDC, no Kafka Connect, no extra infrastructure" width="100%" />

It targets the gap between a **hand-rolled outbox** (correct, but every subtle trap is yours to
get right) and **Debezium/CDC** (powerful, but a separate distributed system to operate):
no extra infrastructure — just your relational database and Kafka — with the correctness traps
already handled.

## Why Tandem?

The classic **double write** — write to the DB, then publish to Kafka as two non-atomic steps —
diverges permanently on partial failure. Tandem removes the dual-write:

```
BEGIN TX
  UPDATE aggregate SET version = version + 1 WHERE id = ? FOR UPDATE
  INSERT INTO tandem_outbox (aggregate_id, type, seq, payload, ...)
COMMIT TX                  ← both or neither, guaranteed by the DB
```

If the relay crashes after publishing but before marking the row done, it republishes — a
**duplicate** (manageable), never a **divergence**.

## Try it

`tandem-sample` is a self-contained tutorial you can run immediately — no Maven Central required.
It starts real PostgreSQL and Kafka containers via Testcontainers, inserts 5 outbox events for two
interleaved orders, and verifies that the relay delivers them in per-aggregate sequence order.

Two of the things you end up looking at — both reproduced by a command below, neither a mockup:

<p align="center"><img src="docs/tandem-cli-status-watch.png" alt="tandem-cli outbox summary --watch — a live terminal dashboard with color-coded bar charts for pending, in-flight, and failed message counts" width="700" /></p>

<p align="center"><em><code>tandem-cli outbox summary --watch</code> — the outbox, redrawing in place.</em></p>

<p align="center"><img src="docs/tandem-metrics-dashboard.png" alt="Tandem relay metrics — a live Grafana dashboard, showing the backlog and the blocked-vs-claimable split during a failing aggregate" width="800" /></p>

<p align="center"><em><code>metricsDashboardDemo</code> — the relay's own signals on a live Grafana, during a failing aggregate.</em></p>

**Prerequisites:** Java 17+, Docker (Docker Desktop or Colima).

```bash
# macOS / Linux
git clone https://github.com/alirux/tandem.git
cd tandem
./tandem-sample/run.sh
```

```cmd
:: Windows
git clone https://github.com/alirux/tandem.git
cd tandem
tandem-sample\run.cmd
```

The script prints JDBC and Kafka connection details so you can connect external clients while the
demo is running. Containers stay alive until you press ENTER.

For the **Spring Boot** write-side experience, run the Spring sample instead — it boots a Spring
application against a Testcontainers PostgreSQL, writes events through the `@TransactionalOutbox`,
Template and Spring-events tiers, and delivers them to Kafka in per-aggregate order:

```bash
# macOS / Linux
./tandem-sample-spring/run.sh
```

```cmd
:: Windows
tandem-sample-spring\run.cmd
```

The Spring sample also demonstrates the Admin API (`tandem.admin.enabled: true` in its
`application.yml`) against the same outbox it just wrote to — reads, and replay/discard on a row
the demo deliberately manufactures as `FAILED` for this purpose. Once the demo narration finishes,
the app keeps running as a web server (Ctrl+C to stop) and prints the exact commands to try,
including the real id of that row:

```bash
curl http://localhost:8080/tandem/admin/v1/outbox/summary
curl http://localhost:8080/tandem/admin/v1/outbox/messages
curl http://localhost:8080/tandem/admin/v1/outbox/messages/1

# Replace 1 with the id the demo printed
curl -X POST http://localhost:8080/tandem/admin/v1/outbox/messages/1/replay
curl -X POST http://localhost:8080/tandem/admin/v1/outbox/messages/1/discard \
     -H 'Content-Type: application/json' \
     -d '{"acknowledgeOrderingBreak": true, "reason": "demo"}'

# Relay control - works under this SINGLE coordination, the default:
curl http://localhost:8080/tandem/admin/v1/relay/status
curl -X POST http://localhost:8080/tandem/admin/v1/relay/pause
curl -X POST http://localhost:8080/tandem/admin/v1/relay/resume
```

`GET /relay/buckets`, `GET /relay/buckets/{bucket}`, `GET /relay/workers`, and
`POST /relay/buckets/{bucket}/release` need `LEASE` coordination — `SINGLE` refuses them (`409`)
rather than answer with misleading data. Run the sample under `LEASE` instead to try those for real,
against an actually-owned bucket:

```bash
./tandem-sample-spring/run-lease.sh
```

Prefer a CLI over hand-built `curl` calls? [`tandem-cli`](tandem-cli/) wraps the same Admin API
endpoints in discoverable verbs and typed flags. Build it from source and point it at the sample
(`--base-url` takes the same `.../tandem/admin/v1` prefix the `curl` commands above use):

```bash
cd tandem-cli && make build && cd ..
./tandem-cli/bin/tandem-cli --base-url http://localhost:8080/tandem/admin/v1 outbox summary
./tandem-cli/bin/tandem-cli --base-url http://localhost:8080/tandem/admin/v1 relay status
```

Add `--watch` to `outbox summary` for the live, redrawing-in-place dashboard shown at the top of
this section — bar charts for `PENDING`/`IN_FLIGHT`/`FAILED`, refreshed on an interval, colored so
a growing red `FAILED` bar catches the eye without reading the number.

See [tandem-cli/docs/cli](tandem-cli/docs/cli/tandem-cli.md) for the full command reference.

To see the relay's own metrics rather than take them on faith, `tandem-benchmark`'s
`metricsDashboardDemo` runs a real Micrometer → Prometheus → Grafana pipeline through eight
scripted phases — no relay running, a drain, steady load, a failing aggregate, a second instance
joining, that instance's worker getting stuck without crashing, a crash with rows in flight,
recovery — and holds the dashboard open so every signal `TandemMetrics` reports can be read on a
live graph instead of asserted in a test:

```bash
./gradlew :tandem-benchmark:metricsDashboardDemo
```

Needs Docker; the first run pulls the Prometheus and Grafana images. Press Enter to shut the stack
down, or pass `--args="--hold=<seconds>"` to close it automatically instead. See
[LLD-benchmark.md §6.3](docs/LLD-benchmark.md) for what each panel means, including the alerting
gap the first real runs found — the reason `blocked.count` exists.

The same benchmark's `tracingDashboardDemo` does the same for traces: a real OpenTelemetry SDK
exports through a real Tempo, read on the same Grafana over a second datasource, so one full trace
— write, the outbox dwell, `tandem.relay.publish`, and the consumer — can be opened as a waterfall
instead of taken on faith.

```bash
./gradlew :tandem-benchmark:tracingDashboardDemo
```

See [LLD-benchmark.md §6.4](docs/LLD-benchmark.md) for what stitches the trace together and which
spans are the shipped product versus the demo's own stand-ins for a caller's domain span and a
consumer.

## Key features

- **Per-aggregate happens-before ordering** — strict order within an `aggregate_id`, full
  parallelism across aggregates (the Kafka partition-key model, enforced end to end).
- **At-least-once relay** with sharded `SKIP LOCKED` polling, lease-based failover, exponential
  backoff, and poison-message isolation (a stuck event blocks only its aggregate).
- **CloudEvents by default** — messages are published using the CNCF CloudEvents envelope
  (binary mode), interoperable with the wider ecosystem.
- **First-class, per-aggregate replay** — re-publish a single aggregate's history through a
  programmatic Java API (`ReplayService`).
- **Pluggable metrics port** — `TandemMetrics` in `tandem-core` reports published and retried counts,
  a per-row publish-latency histogram (insert-to-ack, the runtime-checkable read on the p50/p99
  latency targets), config-validation failures, and the signals an operator actually alerts on: how
  many events are waiting, how long the oldest has been waiting, how many are permanently failed right
  now, how many later events are blocked behind one of those failures (queued but unclaimable until it
  is resolved — reported separately so it never gets confused with a relay that is merely falling
  behind), how many relay workers are alive, and — under `LEASE` coordination — how many buckets have
  work waiting but no live owner. All of it is read periodically and **only when an adapter is wired**,
  so a no-op default costs nothing. `tandem-micrometer` binds all of it to a real Micrometer
  `MeterRegistry`, autoconfigured by `tandem-spring-relay` the moment both are on the classpath.
- **Embedded or standalone, single or multi-instance** — the relay runs in your app or in a separate
  process you assemble yourself, and coordinates one or many concurrent instances via a declared
  mode: `SINGLE` (one instance owns all buckets, zero cost) or `LEASE` (lease-partitioned ownership
  for a horizontally-scaled client or multiple relay processes). Both modes are implemented and
  tested. Only the outbox INSERT must live in the client, which stays dependency-light.
- **An Admin API to see and act on a stuck outbox** — `tandem-admin`, an optional REST module
  (off by default) covering the outbox (health summary, search, message detail, replay, bulk
  replay, discard) and the relay (status, pause/resume — whole relay or one bucket under `LEASE`,
  per-bucket/per-worker observability, force-release a stalled bucket). API-first: the OpenAPI
  contract is the source of truth. Every write operation is audit-logged, including the caller's
  identity when the host application authenticates requests. Contract:
  [HLD-admin-api.md](docs/HLD-admin-api.md) · [admin-api.openapi.yaml](docs/admin-api.openapi.yaml).
  [`tandem-cli`](tandem-cli/) is a Go command-line frontend over the same contract — discoverable
  verbs and typed flags instead of hand-built `curl` calls, never a second control path. Separate
  Go module and release cadence from the library; command reference generated at
  [tandem-cli/docs/cli](tandem-cli/docs/cli/tandem-cli.md), design in [LLD-cli.md](docs/LLD-cli.md).
- **Framework-agnostic core** — works with plain Java and no container. Spring Boot autoconfiguration is
  implemented for both the **write side** (`tandem-spring-producer` — the four usage tiers over the outbox
  INSERT) and the **relay** (`tandem-spring-relay` — started and stopped with the application), so a Spring
  app needs no manual wiring (see the [Spring sample](#try-it)). One artifact per module serves **Boot 3.x
  and 4.x** alike — Spring is `compileOnly`, so your app's own version binds at runtime, and `./gradlew
  check` runs the autoconfiguration tests against both lines. Plain-Java wiring stays available (see
  [Usage](#usage)).
- **Trace and correlation propagation across the outbox boundary** — off by default
  (`tandem.tracing.enabled`), so a consumed event can be traced back to the domain transaction that
  produced it. Propagation mode carries the trace context and correlation id onto the row at insert;
  instrumented mode (`tandem.tracing.publish-span`) additionally emits a relay publish span at the
  real send instant. Ships for Spring applications (bridged to Micrometer Tracing) and, via the
  optional `tandem-tracing-otel` module, for any application instrumenting itself with the
  OpenTelemetry SDK directly. The correlation id alone needs no tracing library at all — read from an
  MDC key or the explicit `TandemContext` API — and is also searchable through the Admin API.
  Design: [HLD-tracing.md](docs/HLD-tracing.md).

## Architecture in detail

The four stages of the [diagram above](#what-is-tandem), and what each one buys you:

1. **The write.** Your domain change and the outbox row are inserted in the *same* transaction, so
   they commit together or not at all — no dual write, no distributed transaction.
2. **The store.** The outbox row lands in `tandem_outbox`. The database is the **only** coordination
   point: relay instances claim work, take leases and hand over there, and nowhere else.
3. **The relay.** Workers poll their own shard of buckets with `SKIP LOCKED`, publish, and mark the
   row done. A failure leaves the row for the next attempt rather than losing it.
4. **The publish.** Messages reach Kafka as CloudEvents, keyed by `aggregate_id`, so a single
   aggregate's events land on one partition in order while different aggregates run in parallel.

Only the **write-side** must run in the client; the relay and housekeeping are DB-coordinated and
can be deployed independently. See [HLD §3.2](docs/HLD.md).

## Add the dependency

Tandem is published to Maven Central under the `com.codingful` group. Import the
[BOM](CONTRIBUTING.md#project-layout) to keep module versions aligned, then declare only the
modules you need (no per-module version). Use the current version from
[Maven Central](https://central.sonatype.com/artifact/com.codingful/tandem-core) (also linked from
the badge above) or the [Releases](https://github.com/alirux/tandem/releases) page in place of
`x.y.z` below.

**Gradle (Kotlin DSL)**

```kotlin
dependencies {
    implementation(platform("com.codingful:tandem-bom:x.y.z"))
    implementation("com.codingful:tandem-jdbc")     // write-side + relay engine (PostgreSQL)
    implementation("com.codingful:tandem-kafka")    // Kafka publish + CloudEvents binding
    testImplementation("com.codingful:tandem-test") // in-memory doubles + Testcontainers helper
}
```

**Maven**

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.codingful</groupId>
      <artifactId>tandem-bom</artifactId>
      <version>x.y.z</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>com.codingful</groupId>
    <artifactId>tandem-jdbc</artifactId>
  </dependency>
  <dependency>
    <groupId>com.codingful</groupId>
    <artifactId>tandem-kafka</artifactId>
  </dependency>
</dependencies>
```

The write-side alone (`tandem-jdbc`) pulls no Kafka dependency; add `tandem-kafka` only where the
relay runs. On Spring Boot, take `tandem-spring-producer` where you write and `tandem-spring-relay`
where the relay runs — each brings its own tier of the stack and leaves Spring itself to your
application's versions. See [CONTRIBUTING.md](CONTRIBUTING.md#project-layout) for the full module
list, and [API reference](#api-reference) for each module's javadoc. What changed between versions,
breaking changes included, is on the [Releases](https://github.com/alirux/tandem/releases) page.

### Spring Boot compatibility

`tandem-spring-producer` and `tandem-spring-relay` ship **one artifact for both Spring Boot
generations** — Spring is `compileOnly`, so your application's own Boot BOM controls the runtime
version, and Tandem never appears in your dependency tree.

| | Spring Boot | Spring Framework |
|---|---|---|
| Compiled against (baseline) | 3.3.x | 6.1.x |
| Verified via `bootLatestThreeTest` | 3.5.x | 6.2.x |
| Verified via `bootFourTest` | 4.1.x | 7.0.x |

Any Boot 3.x ≥ the baseline or Boot 4.x ≥ the verified 4.x line is expected to work; CI pins and tests
exactly these three versions (see [gradle/libs.versions.toml](gradle/libs.versions.toml) for the exact
pins), not every intermediate release.

## Usage

**Write-side** — insert the event inside your own transaction (the relay never runs here):

```java
@Transactional
public Order placeOrder(Order order) {
    orderRepository.save(order);
    outboxRepository.insert(OutboxMessage.builder()
        .aggregateId(order.id())
        .aggregateType("Order")
        .type("com.acme.order.placed")
        .seq(order.version())          // your aggregate owns the sequence number
        .payload(serialize(order))     // plain write-side takes bytes; the Spring producer tiers accept an object
        .contentType("application/json")
        .build());
    return order;
}
```

**Relay** — wire it directly (no Spring required); it polls the outbox and publishes to Kafka,
preserving per-aggregate order:

```java
OutboxRepository repo = new JdbcOutboxRepository(dataSource, /* bucketCount */ 256);

// Startup guard: fail fast if the write-side and the relay disagree on bucketCount. A mismatch
// would route rows into buckets no worker polls — delivery stops with no error — so the first
// side to start records the value and every later start validates against it. Run it once, on a
// plain DataSource. (The write-side and relay usually run in separate processes; call it in each.)
BucketCountGuard.check(dataSource, /* bucketCount */ 256);   // must match the repository above

OutboxStore      store      = new JdbcOutboxStore(dataSource, /* maxAttempts */ 10);
TopicRouter      router     = TopicRouter.kebabWithSuffix("-topic");
OutboxDispatcher dispatcher = new KafkaRelay(kafkaProducerConfig, router, KafkaRelayConfig.of("/tandem/orders"));
WorkerPool       relay      = new WorkerPool(store, dispatcher, RelayConfig.defaults());
relay.start();   // on shutdown: relay.stop();  (in-flight rows recovered by lease)
```

Spring users write none of the above. `tandem-spring-producer` autoconfigures the write side (running
the bucket-count guard for you) and adds the `TransactionalOutboxTemplate`, the `@TransactionalOutbox`
annotation, and the Spring application-events tier; `tandem-spring-relay` autoconfigures the relay and
starts it with the application. Both wire from `tandem.*` properties, with IDE completion and hover help
from the metadata each module ships; each also ships a commented reference configuration
(`tandem-producer-reference.yml`, `tandem-relay-reference.yml`) listing every key it binds with its
default. See the [Spring sample](#try-it), [LLD-spring-producer.md](docs/LLD-spring-producer.md) and
[LLD-spring-config.md](docs/LLD-spring-config.md).

## Logging

Tandem ships **no logging configuration** — routing and formatting are the consuming application's
job, not the library's:

| Module | Logs via | To see its logs |
|---|---|---|
| `tandem-jdbc` (relay lifecycle, claim/reclaim cycles) | `java.lang.System.Logger` (JDK built-in, zero dependencies) | Needs a bridge — see below |
| `tandem-kafka` (publish/encode/send failures) | SLF4J | Nothing to do: picked up by the same SLF4J binding your Kafka client already uses |
| `tandem-core`, `tandem-test` | Nothing — no I/O, errors surface as exceptions | — |

Bridge `System.Logger` to your backend with one dependency — no code, it self-registers via
`ServiceLoader`:

```kotlin
runtimeOnly("org.slf4j:slf4j-jdk-platform-logging:2.0.16")
```

`INFO` covers relay lifecycle; `DEBUG` covers per-cycle detail (claims, reclaims) for
troubleshooting a stalled relay — set on the `com.codingful.tandem.jdbc` and
`com.codingful.tandem.kafka` logger names. Full policy, including a bridge-free alternative and
what Tandem never logs: [HLD-logging.md](docs/HLD-logging.md).

## Documentation

### API reference

Javadoc for every published module, served from the artifacts on Maven Central. `latest` follows
the newest release; replace it with a version (`.../tandem-core/0.6.0/index.html`) to read the API
of the version you actually depend on.

| Module | Contents |
|---|---|
| [tandem-core](https://javadoc.io/doc/com.codingful/tandem-core/latest/index.html) | Models, ports, exceptions and pure logic (zero runtime dependencies) |
| [tandem-jdbc](https://javadoc.io/doc/com.codingful/tandem-jdbc/latest/index.html) | Write-side insert and the relay engine (PostgreSQL baseline) |
| [tandem-kafka](https://javadoc.io/doc/com.codingful/tandem-kafka/latest/index.html) | `OutboxDispatcher` over the Kafka producer (CloudEvents binary binding) |
| [tandem-test](https://javadoc.io/doc/com.codingful/tandem-test/latest/index.html) | In-memory collaborators and the Testcontainers helper |
| [tandem-spring-producer](https://javadoc.io/doc/com.codingful/tandem-spring-producer/latest/index.html) | Spring Boot autoconfiguration — write-side (outbox INSERT + the convenience tiers) |
| [tandem-spring-relay](https://javadoc.io/doc/com.codingful/tandem-spring-relay/latest/index.html) | Spring Boot autoconfiguration — relay engine + CloudEvents publishing |
| [tandem-micrometer](https://javadoc.io/doc/com.codingful/tandem-micrometer/latest/index.html) | `TandemMetrics` backed by a Micrometer `MeterRegistry` |
| [tandem-tracing-otel](https://javadoc.io/doc/com.codingful/tandem-tracing-otel/latest/index.html) | Trace capture and relay publish spans without Spring |
| [tandem-admin](https://javadoc.io/doc/com.codingful/tandem-admin/latest/index.html) | Optional REST operations layer over the outbox and the relay |

`tandem-bom` is a version platform and carries no javadoc; `tandem-cli` is a Go module with its own
[command reference](tandem-cli/docs/cli/tandem-cli.md).

### Design documents

| Document | Contents |
|---|---|
| [HLD.md](docs/HLD.md) | High-Level Design — architecture, decisions, data model, flow |
| [LLD-base.md](docs/LLD-base.md) | Shared build/package conventions |
| [HLD-cloudevents.md](docs/HLD-cloudevents.md) | CloudEvents publication format |
| [HLD-tracing.md](docs/HLD-tracing.md) | Trace & correlation propagation |
| [HLD-attempt-archive.md](docs/HLD-attempt-archive.md) | Forensic per-attempt archive — designed, not implemented |
| [tracing-concepts.md](docs/tracing-concepts.md) | Distributed tracing vocabulary — span, trace, `traceparent`, span link (reference, not Tandem-specific) |
| [LLD-micrometer.md](docs/LLD-micrometer.md) | Micrometer metrics adapter — meter mapping, gauge registration mechanics, Spring autoconfiguration |
| [HLD-logging.md](docs/HLD-logging.md) | Logging posture — per-module logging API, level policy, what is never logged |
| [LLD-spring-config.md](docs/LLD-spring-config.md) | Spring modules & configuration contract — module split, property contract, autoconfiguration (not the write-side ergonomics) |
| [LLD-spring-producer.md](docs/LLD-spring-producer.md) | Spring write-side ergonomics — the Template, `@TransactionalOutbox`, and Spring-events tiers, plus optional payload serialization |
| [LLD-bucket-count-guard.md](docs/LLD-bucket-count-guard.md) | Guard against a divergent bucket count between write-side and relay (core strategy + port, JDBC adapter) |
| [HLD-admin-api.md](docs/HLD-admin-api.md) · [admin-api.openapi.yaml](docs/admin-api.openapi.yaml) | Admin API design + OpenAPI contract. Every error `type` it returns resolves to a [problem-type page](https://tandem.codingful.com/problems/). |
| [LLD-cli.md](docs/LLD-cli.md) | `tandem-cli` — the Go command-line frontend over the Admin API |
| [LLD-relay.md](docs/LLD-relay.md) | `tandem-relay` — the prebuilt standalone relay deployable (image + jar); designed, not implemented |
| [HLD-load-testing.md](docs/HLD-load-testing.md) · [LLD-benchmark.md](docs/LLD-benchmark.md) | Throughput/latency verification plan + the `tandem-benchmark` harness that implements it |
| [HLD-causal-ordering.md](docs/HLD-causal-ordering.md) | Cross-aggregate causal ordering (deep-dive) |
| [HLD-managed-seq.md](docs/HLD-managed-seq.md) | Managed `seq` — a Tandem-assigned sequence number whose counter also carries the per-aggregate write lock; designed, not implemented. Also records what the app-assigned `seq` contract costs today (measured) |
| [dispatch-latency.md](docs/dispatch-latency.md) | Commit-to-publish latency: where it comes from, and the post-commit wakeup options (analysis) |
| [comparison.md](docs/comparison.md) | Comparison with Debezium, Eventuate Tram, Spring Modulith, a hand-rolled outbox, and the stream processors (Kafka Streams, Flink) |
| [open-questions-lld.md](docs/open-questions-lld.md) | Tracked gaps to resolve before the LLDs |
| [IMPLEMENTATION-PLAN-basic-round.md](docs/IMPLEMENTATION-PLAN-basic-round.md) | Execution plan, scope fence, and per-module done-ness for the first milestone |
| [IMPLEMENTATION-PLAN-embedded-lease.md](docs/IMPLEMENTATION-PLAN-embedded-lease.md) | Plan for the `LEASE` multi-instance coordination opt-in (embedded-multi-replica or standalone) |

## Design principles

- **Pareto's Law** — simple for ≥ 80% of use cases; minority-case complexity is opt-in or out of scope.
- **Hexagonal (Ports & Adapters)** — a pure core defines ports; technology modules are adapters.
- **Minimal client footprint** — the part you import has minimal, ideally zero, external dependencies.
- **API-first** — external APIs are defined contract-first (OpenAPI) before implementation.

## Building & testing

Gradle (Kotlin DSL), Java 17 toolchain (auto-provisioned). Use the wrapper:

```bash
./gradlew test     # unit tests only — no Docker required
./gradlew check    # full verification, incl. @Tag("integration") Testcontainers tests (need Docker)
./gradlew build    # compile + unit tests + assemble
```

Integration tests spin up real PostgreSQL and Kafka via Testcontainers, so they need a running
Docker daemon (Docker Desktop or Colima); without one, run `./gradlew check -x integrationTest`.
Per-module coverage is written to each module's `build/reports/jacoco/test/jacocoTestReport.xml`.
For a single project-wide report that also credits cross-module coverage (e.g. a `tandem-jdbc`
integration test exercising a `tandem-core` class) to the class that owns it, run:

```bash
./gradlew :tandem-coverage:aggregatedCoverageReport   # unit + integration + e2e, all modules
```

It lands in `tandem-coverage/build/reports/jacoco/aggregated/` (HTML + XML) and is the report CI
uploads to Codecov.

## Build & license

- **Build:** Gradle · **Java:** 17+ · **Published to:** Maven Central (`com.codingful`)
- **License:** Apache 2.0

Tandem publishes standard, non-shaded JARs — third-party libraries are not bundled and
are resolved separately from Maven Central under their own licenses. The runtime footprint
is listed in [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).

Contributor conventions are in [AGENTS.md](AGENTS.md).

## Known issues & limitations

Behaviours of what **is** shipped that can surprise you in production. Each one is a deliberate
trade-off or a tracked gap — none is a bug report. (For what is *not yet* shipped, see
[Future work](#future-work) below.)

- **PostgreSQL only today.** The shipped schema, the claim/lease SQL, and every integration test
  target PostgreSQL — there is no MySQL baseline DDL and no MySQL engine variant in any released
  version, so running Tandem against MySQL is not possible today.

- **A permanently failed event stops its aggregate.** The claim query only takes a row when no
  earlier row of the same `aggregate_id` is still PENDING, IN_FLIGHT or FAILED — that is what
  preserves per-aggregate order, and it means a row that exhausts `maxAttempts` (default 10, roughly
  7 minutes with the jittered backoff ladder) leaves every later event of that aggregate
  undelivered. Other aggregates are unaffected: the blast radius is one aggregate, by design. The
  `TandemMetrics` `blocked.count` gauge reports how many later events are queued behind such a
  failure, so the blast radius is observable without querying the table directly — but it stays
  above zero, and a naive alert on backlog age alone will misread this as the relay stalling, until
  the failure is resolved.
  **Resolution:** the Admin API's replay/discard endpoints (`tandem-admin`) inspect `last_error` and
  either retry the row or move it to `status = 4` (DISCARDED) to unblock the chain — see
  [Try it](#try-it).

- **Ordering within an aggregate is only as good as your write-side.** Tandem relays rows in `id`
  order and `UNIQUE (aggregate_id, seq)` rejects a duplicate `seq`, but neither *creates* order: if
  two transactions insert for the same aggregate concurrently and the one with the lower `id` commits
  second, the relay will already have published the later event. The contract is that writers to one
  aggregate are serialized — a `SELECT … FOR UPDATE` on the aggregate row, or an optimistic version
  check — and that `seq` is that aggregate's version. See [HLD §4.2](docs/HLD.md).

- **Duplicates are expected, reordering is not.** At-least-once means a crash between the Kafka ack
  and the mark-DONE republishes the event. Consumers must be idempotent; this is the price the outbox
  pattern pays to never diverge.

- **A reclaimed row has a brief double-ownership window.** `markDone`/`markForRetry`/`markFailed`
  update by `id` without an `AND locked_by = :me` fence, so after a lease expiry and reclaim a late
  write from the previous owner can still land on a row another instance now owns. The effect is
  bounded to at most a duplicate publish — never a reorder — which is why the fence is tracked as
  hardening rather than a fix ([IMPLEMENTATION-PLAN-embedded-lease.md](docs/IMPLEMENTATION-PLAN-embedded-lease.md) §6).

- **Idle latency is bounded by `pollInterval`, not by the commit.** There is no post-commit wakeup
  yet: a bucket that was drained waits on average `pollInterval / 2` (≈ 50 ms at the 100 ms default,
  100 ms worst case) before the new row is discovered. Under sustained load the cost is ≈ 0 — the
  worker loop only sleeps when a claim comes back empty. Lowering `pollInterval` trades this against
  idle query load across all instances and workers; the full analysis, and the wakeup options, are in
  [dispatch-latency.md](docs/dispatch-latency.md).

- **`bucketCount` is immutable after the first deploy.** Changing it re-maps aggregates onto
  different buckets and would split one aggregate's events across workers, so the startup guard
  refuses a mismatch rather than accepting it — and re-sharding an existing outbox is not supported.
  Pick `B` once (default 256, comfortable to well past the parallelism most deployments need).

- **Cleanup and lease reclaim are not bucket-scoped.** Every relay instance scans the whole
  `tandem_outbox` for expired leases (every 5 s) and for terminal rows past the retention window
  (every 15 min, default retention 14 days). It is safe — the work is idempotent and keyed by
  `id`/`status` — just redundant under `LEASE` with N instances ([LLD-jdbc §3.2/§3.7](docs/LLD-jdbc.md)).
  Terminal rows also stay in the table for the whole retention window, which is what keeps the table
  large enough to be worth an index-only dispatch scan.

- **Configuration is still read once, at startup.** The Admin API can pause/resume the whole relay or
  a single bucket under `LEASE` at runtime (see [Try it](#try-it)) — so taking a misbehaving relay, or
  just one bucket, out of the picture no longer means stopping the process — but tunables like
  `pollInterval`, `batchSize` or `metricsInterval` still can't be changed without a restart.

- **Blocking JDBC only.** The relay is a thread-per-worker pool over a `DataSource`; R2DBC and
  reactive pipelines are not supported.

## Future work

Not yet shipped, in no particular order:

- **`tandem-relay`** — a prebuilt, standalone relay deployable. Fully designed
  ([LLD-relay.md](docs/LLD-relay.md)) but **not built**: today you assemble the relay process
  yourself (plain Java or Spring); see [Usage](#usage).
- **Cross-aggregate causal ordering** via Lamport clocks — fully designed
  ([HLD-causal-ordering.md](docs/HLD-causal-ordering.md)) but **not built**, and there is no way to switch it
  on: no flag, no `lamport` column, no clock table, no consumer-side adapter. What ships is a small
  **reserved surface** visible in IDE autocomplete and doing nothing — the `CausalContext` port,
  `LamportClock`, a nullable `OutboxRecord.lamport`, and the `logicalclock`/`causation_id` header
  names — published so that building the feature stays an additive change. The exact inventory of
  what exists versus what is missing is [HLD-causal-ordering.md §0](docs/HLD-causal-ordering.md).
- **MySQL support.** Fully specified and verified against MySQL 8.4
  ([LLD-jdbc §5](docs/LLD-jdbc.md)), but **not built** — PostgreSQL remains the only supported
  database. It is more than a dialect swap: MySQL has no `UPDATE ... RETURNING`, so the claim becomes
  a two-step transaction, and the relay has to run at `READ COMMITTED` — under MySQL's
  `REPEATABLE READ` default, four relay workers are measurably *slower* than one, with nothing in the
  logs to say why.
- **Attempt-level forensic history** — a timeline of every delivery attempt per message
  (when it ran, how long it took, which worker, which error), for forensic debugging. Fully
  designed in [HLD-attempt-archive.md](docs/HLD-attempt-archive.md) but **not built**: no port,
  no table, and no Admin API endpoints ship today. It would be opt-in and off by default like
  the capabilities above, and adding it back to the API contract stays an additive change.

The full per-module status is in [CONTRIBUTING.md](CONTRIBUTING.md#project-layout).
