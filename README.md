<div align="center">

<img src="docs/tandem-logo-blackbg-shade.png" alt="Tandem logo" width="200" />

# Tandem

**Reliable, causally-ordered event delivery from your database to Apache Kafka — no CDC, no Kafka Connect, no two-phase commit.**

[![CI](https://github.com/alirux/tandem/actions/workflows/ci.yml/badge.svg)](https://github.com/alirux/tandem/actions/workflows/ci.yml)
[![codecov](https://codecov.io/github/alirux/tandem/graph/badge.svg?token=YKA7T7YCFD)](https://codecov.io/github/alirux/tandem)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](#)
[![Status](https://img.shields.io/badge/status-basic%20round%20implemented-brightgreen.svg)](docs/IMPLEMENTATION-PLAN-basic-round.md)

</div>

---

> **Status: basic round implemented.** The first milestone — `tandem-core`, `tandem-jdbc`,
> `tandem-kafka`, and the `tandem-test` helpers — is built and tested end-to-end on PostgreSQL +
> Kafka (see the [implementation plan](docs/IMPLEMENTATION-PLAN-basic-round.md)). The Spring tiers,
> the standalone relay, the Admin API, MySQL support, and the optional adapters are **not yet
> implemented**. The basic-round modules are **available on Maven Central** under `com.codingful`
> (see [Add the dependency](#add-the-dependency)). Start with the [HLD](docs/HLD.md) for the full
> design.

## What is Tandem?

Tandem is a Java library that implements the **Transactional Outbox Pattern**. You insert an
event into an `outbox` table **inside the same transaction** that mutates your domain — so the
write is atomic by your database's ACID guarantees, with no dual-write and no distributed
transaction. A separate **relay** then polls the outbox and publishes to Kafka, at-least-once,
preserving per-aggregate ordering.

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

## Key features

- **Per-aggregate happens-before ordering** — strict order within an `aggregate_id`, full
  parallelism across aggregates (the Kafka partition-key model, enforced end to end).
- **At-least-once relay** with sharded `SKIP LOCKED` polling, lease-based failover, exponential
  backoff, and poison-message isolation (a stuck event blocks only its aggregate).
- **CloudEvents by default** — messages are published using the CNCF CloudEvents envelope
  (binary mode), interoperable with the wider ecosystem.
- **First-class, per-aggregate replay** — re-publish a single aggregate's history through a
  programmatic Java API (`ReplayService`); a REST equivalent arrives with the Admin API.
- **Pluggable metrics port** — `TandemMetrics` in `tandem-core` reports published, failed and retried
  counts, plus config-validation failures, with a no-op default. The Micrometer adapter is 🔜 planned,
  and the lag, active-worker and uncovered-bucket signals are declared on the port but **not yet
  emitted** by the relay.
- **Optional, opt-in capabilities, designed but 🔜 not yet implemented** — cross-aggregate causal
  ordering via Lamport clocks, a forensic per-attempt archive, W3C trace/correlation propagation and
  an API-first REST Admin API each have a design document and, where relevant, a port in
  `tandem-core`; all resolve to no-op defaults today. They are off by default *by design*, so
  adopting them will stay a per-capability opt-in with zero cost when unused.
- **Embedded or standalone, single or multi-instance** — the relay runs in your app or in a separate
  process you assemble yourself (a prebuilt `tandem-relay` deployable is 🔜 planned), and coordinates
  one or many concurrent instances via a declared mode: `SINGLE` (one instance owns all buckets, zero
  cost) or `LEASE` (lease-partitioned ownership for a horizontally-scaled client or multiple relay
  processes). Both modes are implemented and tested. Only the outbox INSERT must live in the client,
  which stays dependency-light.
- **Framework-agnostic core** — works with plain Java and no container; Spring Boot (3.x and 4.x)
  autoconfiguration is 🔜 planned, so wiring is manual for now (see [Usage](#usage)).

## Architecture at a glance

```
┌───────────────────── Client application ──────────────────────┐
│  Domain TX --same TX--> INSERT outbox row (PostgreSQL)        │
└───────────────────────────────┬───────────────────────────────┘
                                │  the DB is the only coordination point
                                ▼
           ┌──── Relay (embedded or standalone) ────┐
           │  sharded poll -> publish -> mark DONE  │
           └────────────────────────────────────────┘
                                │
                                ▼
              Apache Kafka (keyed by aggregate_id)
```

Only the **write-side** must run in the client; the relay, housekeeping, and Admin API are
DB-coordinated and can be deployed independently. See [HLD §3.2](docs/HLD.md).

## Add the dependency

Tandem is published to Maven Central under the `com.codingful` group. Import the
[BOM](#modules) to keep module versions aligned, then declare only the modules you need
(no per-module version).

**Gradle (Kotlin DSL)**

```kotlin
dependencies {
    implementation(platform("com.codingful:tandem-bom:0.1.1"))
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
      <version>0.1.1</version>
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
relay runs. See [Modules](#modules) for the full list.

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
        .payload(serialize(order))     // you provide the bytes (Spring tiers will add an object overload)
        .contentType("application/json")
        .build());
    return order;
}
```

**Relay** — wire it directly (the basic round runs with no Spring); it polls the outbox and publishes
to Kafka, preserving per-aggregate order:

```java
OutboxRepository repo = new JdbcOutboxRepository(dataSource, /* bucketCount */ 256);

OutboxStore      store      = new JdbcOutboxStore(dataSource, /* maxAttempts */ 10);
TopicRouter      router     = TopicRouter.kebabWithSuffix("-topic");
OutboxDispatcher dispatcher = new KafkaRelay(kafkaProducerConfig, router, KafkaRelayConfig.of("/tandem/orders"));
WorkerPool       relay      = new WorkerPool(store, dispatcher, RelayConfig.defaults());
relay.start();   // on shutdown: relay.stop();  (in-flight rows recovered by lease)
```

Spring users will later have higher-level options: autoconfiguration, a `TransactionalOutboxTemplate`,
a `@TransactionalOutbox` annotation, and a Spring application-events tier.

## Logging

Tandem ships **no logging configuration** — routing and formatting are the consuming application's
job, not the library's. What each module logs through is chosen so the client write-side stays
dependency-free:

| Module | Logs via | To see its logs |
|---|---|---|
| `tandem-jdbc` (relay lifecycle, claim/reclaim cycles) | `java.lang.System.Logger` (JDK built-in, zero dependencies) | Needs a bridge — see below |
| `tandem-kafka` (publish/encode/send failures) | SLF4J | Nothing to do: picked up by the same SLF4J binding your Kafka client already uses |
| `tandem-core`, `tandem-test` | Nothing — no I/O, errors surface as exceptions | — |

**Bridging `System.Logger` to your backend.** Add this runtime dependency and every `tandem-jdbc`
log line goes straight to SLF4J and whatever backend is bound behind it — no code, no properties
file, it self-registers as a `System.LoggerFinder` via `ServiceLoader`:

```kotlin
runtimeOnly("org.slf4j:slf4j-jdk-platform-logging:2.0.16")
```

Without a bridge, `System.Logger` falls back to the JDK's `java.util.logging`. You can redirect
from there instead with `org.slf4j:jul-to-slf4j` plus `handlers=org.slf4j.bridge.SLF4JBridgeHandler`
in `logging.properties` — config-only, but one extra hop.

> **Caveat:** `System.LoggerFinder` is a JVM-wide singleton resolved via `ServiceLoader`. If two
> bridges end up on the classpath (say SLF4J's and Log4j2's `log4j-jpl`), which one wins is
> undefined — keep exactly one.

**Levels.** `INFO` covers relay lifecycle (start/stop, worker count, coordination mode); `DEBUG`
covers per-cycle detail (rows claimed, leases reclaimed) and is what you want when troubleshooting
a stalled relay. Set them on the `com.codingful.tandem.jdbc` and `com.codingful.tandem.kafka`
logger names. Full policy, including what Tandem will never log: [HLD-logging.md](docs/HLD-logging.md).

## Try it

`tandem-sample` is a self-contained tutorial you can run immediately — no Maven Central required.
It starts real PostgreSQL and Kafka containers via Testcontainers, inserts 5 outbox events for two
interleaved orders, and verifies that the relay delivers them in per-aggregate sequence order.

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

## Modules

| Module | Role | Status |
|---|---|---|
| `tandem-bom` | Bill of Materials — aligned module versions (no code) | ✅ basic round |
| `tandem-core` | Models, ports, pure logic — **zero runtime dependencies** | ✅ basic round |
| `tandem-jdbc` | JDBC adapter: outbox insert + relay engine (poll/lease/cleanup), PostgreSQL (no Kafka) | ✅ basic round |
| `tandem-kafka` | Kafka publish adapter + CloudEvents binary binding | ✅ basic round |
| `tandem-test` | In-memory outbox/dispatcher + Testcontainers helper | ✅ basic round |
| `tandem-sample` | Runnable end-to-end tutorial — self-contained, not published to Maven Central | ✅ basic round |
| `tandem-benchmark` | Internal load/performance harness — not published (see [HLD-load-testing.md](docs/HLD-load-testing.md)) | ✅ implemented |
| `tandem-coverage` | Build-only — aggregates every module's coverage into one report (no code, not published) | ✅ implemented |
| `tandem-spring-producer` / `tandem-spring-relay` / `tandem-spring` | Spring Boot autoconfig (write-side / relay / all-in-one) | 🔜 planned |
| `tandem-relay` | Prebuilt standalone runnable relay | 🔜 planned |
| `tandem-admin` | Optional API-first REST admin API | 🔜 planned |
| `tandem-micrometer` | Optional relay-side Micrometer adapter for the metrics port | 🔜 planned |
| `tandem-kafka-streams` / `tandem-flink` / `tandem-tracing-otel` | Optional adapters | 🔜 planned |

> **Database support: PostgreSQL only today.** The shipped schema, the claim/lease SQL, and every
> integration test target PostgreSQL — there is no MySQL baseline DDL and no MySQL engine variant in
> any released version. MySQL is a **planned future addition**, not a supported option you can
> configure: the design keeps the claim strategy portable (`SELECT ... FOR UPDATE SKIP LOCKED`,
> supported by MySQL 8.0+), so it is a deliberate roadmap item rather than an architectural
> obstacle — but until it lands, running Tandem on MySQL is not possible. Optional
> capabilities (causal ordering, attempt archive, tracing, Micrometer) have their ports in
> `tandem-core` but resolve to no-op defaults until their adapters land.

## Documentation

| Document | Contents |
|---|---|
| [HLD.md](docs/HLD.md) | High-Level Design — architecture, decisions, data model, flow |
| [LLD-base.md](docs/LLD-base.md) | Shared build/package conventions |
| [HLD-cloudevents.md](docs/HLD-cloudevents.md) | CloudEvents publication format |
| [HLD-attempt-archive.md](docs/HLD-attempt-archive.md) | Forensic per-attempt archive |
| [HLD-tracing.md](docs/HLD-tracing.md) | Trace & correlation propagation |
| [HLD-logging.md](docs/HLD-logging.md) | Logging posture — per-module logging API, level policy, what is never logged |
| [HLD-admin-api.md](docs/HLD-admin-api.md) · [admin-api.openapi.yaml](docs/admin-api.openapi.yaml) | Admin API design + OpenAPI contract |
| [HLD-load-testing.md](docs/HLD-load-testing.md) · [LLD-benchmark.md](docs/LLD-benchmark.md) | Throughput/latency verification plan + the `tandem-benchmark` harness that implements it |
| [causal-ordering.md](docs/causal-ordering.md) | Cross-aggregate causal ordering (deep-dive) |
| [comparison.md](docs/comparison.md) | Comparison with Debezium, Eventuate Tram, Spring Modulith |
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
