# Tandem — Base LLD (Shared Foundations)

**Version:** 1.0  
**Status:** Draft  
**Applies to:** all Tandem modules

This document captures technical conventions shared across all module-level LLDs.
Each module LLD references this document rather than repeating these details.

---

## 1. Build & Publication Coordinates

**Build system:** Gradle (multi-project build), **Kotlin DSL** (`*.gradle.kts`, `settings.gradle.kts`) —
type-safe, best IDE support, and consistent with the `build.gradle.kts` assumed by the release workflow.  
**Publication target:** Maven Central

| Property | Value |
|---|---|
| `group` | `com.codingful` |
| Root project name | `tandem` |
| Root Java package | `com.codingful.tandem` |
| License | Apache 2.0 |
| Java version | 17+ |
| Gradle DSL | Kotlin (`*.gradle.kts`) |

### Subproject names and published artifact IDs

| Subproject | Published `artifactId` | Notes |
|---|---|---|
| `tandem-bom` | `tandem-bom` | |
| `tandem-core` | `tandem-core` | |
| `tandem-jdbc` | `tandem-jdbc` | |
| `tandem-kafka` | `tandem-kafka` | |
| `tandem-spring-producer` | `tandem-spring-producer` | Write-side Spring autoconfig (JDBC, **no Kafka**) — used by the client (§3.2 HLD) |
| `tandem-spring-relay` | `tandem-spring-relay` | Relay Spring autoconfig (JDBC + Kafka) |
| `tandem-spring` | `tandem-spring` | All-in-one aggregator (producer + relay) — embedded default |
| `tandem-relay` | `tandem-relay` | Prebuilt **standalone runnable** relay (Spring Boot app over `tandem-spring-relay`) — split topology (§3.2 HLD) |
| `tandem-test` | `tandem-test` | |
| `tandem-kafka-streams` | `tandem-kafka-streams` | Optional — causal-ordering adapter (§9 HLD) |
| `tandem-flink` | `tandem-flink` | Optional — causal-ordering adapter (§9 HLD) |
| `tandem-micrometer` | `tandem-micrometer` | Optional — relay-side Micrometer adapter for the `TandemMetrics` port (§7 HLD) |
| `tandem-tracing-otel` | `tandem-tracing-otel` | Optional — OpenTelemetry trace-capture adapter (§7.2 HLD) |
| `tandem-admin` | `tandem-admin` | Optional — REST admin API, API-first (§7.3 HLD, admin-api.openapi.yaml) |
| `tandem-benchmark` | *(not published)* | Internal — load/performance harness (see HLD-load-testing.md) |

---

## 2. Key Gradle Plugins

| Plugin | ID | Purpose |
|---|---|---|
| Maven Central publishing | `com.vanniktech.maven.publish` | Publishes all modules to Maven Central; provides `publishToMavenCentral` task and handles GPG signing |

---

## 3. Database Object Naming

All Tandem-managed database objects are **prefixed `tandem_`** to keep them clearly separate
from the client's own tables:

| Object | Name |
|---|---|
| Outbox table | `tandem_outbox` |
| Attempt archive (opt-in, §7.1) | `tandem_outbox_attempt` |
| Lamport clock store (opt-in, §9.3) | `tandem_aggregate_clock` |
| Bucket ownership (standalone, §4.3) | `tandem_bucket_lease` |
| Relay control flag (§4.1 admin) | `tandem_relay_control` |
| Inbox reorderer (future, §9.6) | `tandem_inbox` |
| Indexes | `idx_tandem_…` |

Column names are **not** prefixed (they are already scoped by their table). The prefix is
fixed for now; a configurable prefix/schema could be added later if multi-tenancy in one DB
is required.

---

## 4. Package Naming Convention

```
com.codingful.tandem.<module>[.<sub-package>]
```

| Module | Root package |
|---|---|
| tandem-core | `com.codingful.tandem.core` |
| tandem-jdbc | `com.codingful.tandem.jdbc` |
| tandem-kafka | `com.codingful.tandem.kafka` |
| tandem-spring-producer | `com.codingful.tandem.spring.producer` |
| tandem-spring-relay | `com.codingful.tandem.spring.relay` |
| tandem-spring | `com.codingful.tandem.spring` |
| tandem-relay | `com.codingful.tandem.relay` |
| tandem-test | `com.codingful.tandem.test` |
| tandem-kafka-streams | `com.codingful.tandem.kafkastreams` |
| tandem-flink | `com.codingful.tandem.flink` |
| tandem-micrometer | `com.codingful.tandem.micrometer` |
| tandem-tracing-otel | `com.codingful.tandem.tracing.otel` |
| tandem-admin | `com.codingful.tandem.admin` |
| tandem-benchmark | `com.codingful.tandem.benchmark` |
