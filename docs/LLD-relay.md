# Tandem — `tandem-relay` LLD

**Version:** 1.0 (Designed, not implemented)
**Module:** `tandem-relay` · Gradle subproject, package `com.codingful.tandem.relay`
**Depends on:** [`tandem-spring-relay`](LLD-spring-config.md) (the relay autoconfiguration it runs),
[`tandem-admin`](HLD-admin-api.md) (optional second role, §4)
**Companion to:** [HLD.md](HLD.md) §3.2 (deployment topology — the split relay this module ships),
[HLD-admin-api.md](HLD-admin-api.md) §4.1 (the standalone Admin API deployment, delivered by the same image)
**Resolves:** Q23 in [open-questions-lld.md](open-questions-lld.md) §D
**Published:** **Not** to Maven Central. Distributed as an OCI container image and an executable
jar attached to the GitHub Release (§7).
**Versioned with the library**, tagged `v<semver>` — not on an independent scheme (§7.3).

This document specifies `tandem-relay`, a prebuilt runnable application that hosts the Tandem
relay — and, optionally, the Admin API — as its own deployable. It is the **split topology**
of [HLD.md](HLD.md) §3.2 delivered as an artifact instead of as instructions.

---

## 1. Purpose & scope

The split topology is a documented, supported deployment: the relay runs as its own process
pointed at the client's outbox database and Kafka, so the client application depends only on the
write-side and never on the Kafka client. HLD §3.2 promises it, the README lists it, and
`tandem-spring-relay` implements every moving part of it — but an adopter who wants it today must
still create a Spring Boot project, add the dependency, write a main class, and build their own
image. This module removes that step.

**The gap is small and it is not in the engine.** `TandemRelayAutoConfiguration` already
contributes the topic router, the Kafka dispatcher, the outbox store, the bucket source, the
relay control source and the `WorkerPool`; `RelayLifecycle` already starts it after the context is
built and drains it before teardown; `tandem-relay-reference.yml` already documents every bound
key. What is missing is an application around them, and a way to ship it.

**In scope:**

- a Spring Boot application whose entire job is to host those beans (§3);
- a health contribution over the relay's in-process state, which the library deliberately does not
  ship (§5);
- an environment-variable path to the Kafka producer settings, which relaxed binding does not
  provide (§6.2);
- packaging as a container image and an executable jar, and their release path (§7);
- an integration test that boots the real application against a real database and broker (§8).

**Out of scope:**

- **Any new relay capability.** This module contributes no engine behaviour, no new port, and no
  new `tandem.*` key bound by the library. Every relay tunable stays exactly where it is, in
  `tandem-spring-relay`. If this module ever needs a knob the library does not have, that is a
  signal to add it to the library, not here.
- **Applying the schema.** The image does not create, migrate or verify the `tandem_*` tables
  (§6.4).
- **Authentication for the Admin API.** Tandem ships endpoints, not an auth policy
  (HLD-admin-api §3); the image inherits that position and defends it with defaults (§4.2).
- **A native image / GraalVM build.** A relay process starts once and runs for weeks; startup
  time and RSS are not what an outbox deployment is limited by, so the second toolchain buys
  nothing (Pareto, AGENTS.md).

### 1.1 Q23's third question is already answered

Q23 asks for "main class, config binding, packaging (JAR/Docker), how it receives N / shard
assignment (ties to Q8)". The first three are this document. **The fourth no longer exists:**
under `LEASE` there is no `N` to configure and no assignment to receive. An instance registers
itself in `tandem_relay_member`, the fair-share divisor counts live members, and buckets are
claimed and rebalanced dynamically (HLD §3.2, LLD-jdbc §3.2). Scaling the deployment up or down
needs no configuration change on any instance. That part of Q23 is recorded as resolved, not
designed.

---

## 2. Prerequisite — `tandem-admin` on Spring Boot 4

**The Admin API role of this image (§4) is blocked until the `tandem-admin` Boot 4 defect is
fixed.** It is tracked as its own backlog item, not specified here, but it gates this module and
must land first.

Spring Boot 4 defaults to Jackson 3 (`tools.jackson`). `tandem-admin` is typed on Jackson 2
(`com.fasterxml.jackson.databind.ObjectMapper`) in bean signatures and embeds a Jackson 2
`JsonNode` in `OutboxEntryResponse`. Verified by booting a real Boot 4.1.0 application against the
built module: without Jackson 2 on the classpath the context fails outright
(`NoClassDefFoundError` while Boot introspects `OutboxAdminConfiguration`), and with Boot's
`spring-boot-jackson2` module added it starts but renders the `payload` field through Jackson 3's
converter as bean introspection output (`{"array":false,"bigDecimal":false,…}`) instead of the
stored payload — silently violating the contract's `oneOf: [object, string]`.

The module's `bootFourTest` cannot catch this: it puts Jackson 2 on its own runtime classpath and
its MockMvc tests hand-wire a Jackson 2 converter, so its Boot 4 line never represents a stock
Boot 4 application. The relay role of the image is unaffected — `tandem-spring-relay` touches no
Jackson type.

---

## 3. Module layout

`tandem-relay/` is a Gradle subproject like any other, but it is an **application, not a library**,
which changes three things: it is listed in the root build's `unpublishedModules` (so it opts out
of the shared java-library/publishing convention and configures its own toolchain and tasks), it
carries **concrete** Spring dependencies rather than `compileOnly` ones, and it pins a Java and a
Spring Boot version of its own.

```
tandem-relay/
├── build.gradle.kts
├── Dockerfile
├── docker-compose.example.yml
└── src/
    ├── main/java/com/codingful/tandem/relay/
    │   ├── TandemRelayApplication.java              @SpringBootApplication, nothing else
    │   ├── RelayHealthIndicator.java                §5
    │   ├── RelayHealthProperties.java               §5.2
    │   ├── KafkaProducerEnvironmentPostProcessor.java   §6.2
    │   └── package-info.java
    ├── main/resources/
    │   ├── application.yml                          §6.1 — the image's defaults
    │   └── META-INF/spring.factories                 registers the EnvironmentPostProcessor
    └── test/java/com/codingful/tandem/relay/
        └── TandemRelayApplicationIT.java            §8
```

Four classes, one of which is empty. That ratio is the point: anything that grows here beyond
wiring and packaging belongs in `tandem-spring-relay` instead.

### 3.1 Runtime baseline — Java 21, Spring Boot 4.1

The library modules compile against the Boot 3.x baseline with Spring `compileOnly`, so that one
artifact serves both generations (LLD-spring-config §1.1). **An application cannot do that** — it
must resolve a concrete Spring Boot version and run on it.

**Spring Boot 4.1, Java 21.** Nothing about this choice reaches an adopter: the image is a
deployable, so its Boot version and its JVM are implementation details that constrain no
consumer's classpath and appear in no published POM. That freedom is what makes the newer line the
better pick — the longest support runway, and, as a side effect, the project's **first real
end-to-end validation of the single-artifact bet**, which until now rested entirely on
`bootFourTest`'s synthetic classpath. §2 exists because that synthetic classpath had already let a
genuine Boot 4 defect through.

Java 21 over 17 for the same reason (an application, not a library): generational ZGC, better
container ergonomics, and no consumer to constrain. The library modules stay on 17.

### 3.2 Dependencies

| Dependency | Why |
|---|---|
| `platform(libs.spring.boot.dependencies.v4)` | Version alignment for everything below |
| `spring-boot-starter-web` | The web container the Admin API and Actuator need |
| `spring-boot-starter-jdbc` | `DataSourceAutoConfiguration` + HikariCP |
| `spring-boot-starter-actuator` | Health endpoint and probe groups (§5) |
| `project(":tandem-spring-relay")` | The relay itself |
| `project(":tandem-admin")` | The optional second role (§4) |
| `project(":tandem-micrometer")` + a registry | The `TandemMetrics` adapter; Prometheus registry (§4.3) |
| `libs.postgresql` | The JDBC driver — the application must supply it, `tandem-jdbc` never does (§9) |
| `logback-classic` (via the starter) | A concrete backend: this is a leaf app, not a library (AGENTS.md §Logging) |

`tandem-tracing-otel` is deliberately **absent**. Instrumented-mode span emission on the relay side
comes from `tandem-spring-relay`'s own Micrometer Tracing adapter when the application runs
Micrometer Tracing; adding the OTel module too would put two span recorders in one image for a
feature that is off by default. An adopter who wants OTel wiring instead can extend the image.

---

## 4. Roles — one image, chosen by property

The image is a single artifact that can run as a relay, as an Admin API, or as both. The gates
already exist in the library and neither is invented here:

| Role | `tandem.relay.enabled` | `tandem.admin.enabled` | Notes |
|---|---|---|---|
| **Relay** (default) | `true` (default) | `false` (default) | The Pareto case: the split relay of HLD §3.2 |
| **Admin API** | `false` | `true` | The standalone deployment HLD-admin-api §4.1 promises and never shipped |
| **Both** | `true` | `true` | One process, one DB connection pool; convenient for small deployments |

The two roles genuinely compose: the Admin API acts on the outbox through the database, not
through the relay process, and relay control (pause/resume) is DB-mediated through `tandem_meta`
and `tandem_bucket_lease` (HLD-admin-api §4.1). Nothing changes if they run in the same JVM or in
two.

A `tandem-admin` deployable also unblocks `tandem-cli`'s integration test against a live server,
deferred in LLD-cli.md §11.2 for exactly this reason.

### 4.1 Ports

The Admin API and the management endpoints listen on **separate ports**, so they can be exposed
and firewalled independently:

| Port | Default | Serves |
|---|---|---|
| `server.port` | `8080` | The Admin API (`/tandem/admin/v1/**`), only when enabled |
| `management.server.port` | `8081` | Actuator: health, probes, Prometheus scrape |

In the default relay-only role nothing is served on `8080`; the process still starts a web
container because `management.server.port` needs one. That is the cost of making probes and the
metrics scrape available at all, and it is the reason `spring-boot-starter-web` is unconditional
in §3.2.

### 4.2 Security posture

`tandem.admin.enabled` defaults to **false** and the image does not change that. When an operator
turns it on they get an unauthenticated management surface, exactly as documented in
HLD-admin-api §3 — Tandem ships endpoints, not an auth policy. The image's obligations are to
default it off, to keep it on its own port so it can be bound to an internal network, and to say
so unmissably in the image documentation and in a startup `WARN` emitted when the Admin API is
enabled. It does not ship a default credential, since a default credential is worse than none.

### 4.3 Metrics

`tandem-micrometer` plus a Prometheus registry are in the image because a standalone relay with no
metrics endpoint is not operable, and the adapter is inert without a `MeterRegistry`
(LLD-micrometer §5). The scrape endpoint is exposed on the management port and, unlike the Admin
API, is **on by default** — it is a read-only surface that exposes counts and timings, never
payloads (AGENTS.md §Logging rule 5 applies to metric tags too).

---

## 5. Health — where Tandem is finally allowed to have an opinion

The library deliberately ships **no** health verdict: `WorkerPool.status()` returns a
database-free `RelayStatus` reading and `RelayStatus`'s own javadoc states that deciding what an
acceptable worker deficit or cycle age is belongs to the embedding application, exactly like
routing logs or exporting meters.

**In this module Tandem *is* the embedding application.** That is not a reversal of the earlier
decision, it is the half of it that had no home before: the threshold has to be written somewhere,
and a prebuilt image with no readiness signal is not deployable on any orchestrator. The library
keeps shipping no verdict; the deployable ships one.

### 5.1 The verdict

`RelayHealthIndicator` reads `WorkerPool.status()` — **never the database**, which is what makes it
safe at probe frequency on every instance forever — and is `@ConditionalOnBean(WorkerPool.class)`,
so it disappears in the Admin-API-only role rather than reporting on a relay that is not running.

| Condition | Verdict |
|---|---|
| `state != RUNNING` | `DOWN` |
| `workersAlive == 0` | `DOWN` |
| `oldestWorkerCycle` older than the stall threshold | `DOWN` |
| `workersAlive < workersConfigured` | `UP` with the deficit in the details |
| otherwise | `UP` |

A **worker deficit is not DOWN**, and that is deliberate: a died worker is restarted automatically
(LLD-jdbc §3.1), so a transient gap is normal operation. A *persistent* gap is what matters, and
the signal for it is the deficit visible in the details and the existing
`tandem.outbox.workers.active` gauge — not a probe that flaps.

The **stall threshold is the load-bearing condition**, because it is the only one that separates a
relay that is running from one that is merely started: a worker blocked in a database call that
never returns stays alive forever, and `oldestWorkerCycle` is precisely the reading that exposes
it.

**Pause is `UP`.** An operator who paused the relay through the Admin API intended it; reporting
`DOWN` would make an orchestrator act on a deliberate state. The paused flag goes in the details.

Details always carry `instanceId`, `state`, `coordination`, `workersConfigured`, `workersAlive`
and `cycleAgeSeconds` — the identifiers needed to find the affected instance without re-running
anything, the same rule AGENTS.md applies to `ERROR`/`WARN` logs.

### 5.2 Liveness vs readiness

The indicator is contributed to the **readiness** group, not liveness. A stalled or unstarted
relay should be taken out of service; it should **not** be killed and restarted, because the
overwhelmingly likely cause is the database or the broker being unreachable, and restarting the
process fixes neither while losing the diagnostic state. Liveness stays Spring's default
"the process responds".

One property, `management.health.tandem-relay.stalled-after`, default **60s**. It sits under
Actuator's own `management.health.<name>` convention rather than under `tandem.*`, because it
configures a health contributor, not the relay — and because `tandem.relay.*` is bound by the
library's `TandemRelayProperties` and must not acquire keys the library does not know about.

One knob, not four: thresholds for each condition would be configuration surface nobody tunes.
The default is justified rather than magic — 60s is well above the default `poll-interval` of
100ms and above any plausible claim-cycle duration, so it fires on a genuine stall and not on a
slow cycle.

---

## 6. Configuration

### 6.1 What the image sets, and what it refuses to set

The application binds **no `tandem.*` key of its own**. Every relay tunable comes from
`tandem-spring-relay`'s existing property classes, documented in `tandem-relay-reference.yml`,
which stays the single reference. The image's `application.yml` only sets deployment defaults that
have no meaning inside a library:

```yaml
server:
  port: 8080
  shutdown: graceful
management:
  server.port: 8081
  endpoints.web.exposure.include: health,info,prometheus
  endpoint.health.probes.enabled: true
spring:
  lifecycle.timeout-per-shutdown-phase: 60s
  application.name: tandem-relay
```

**`timeout-per-shutdown-phase` must exceed the producer's `delivery.timeout.ms`**, so that
`WorkerPool.stop()` can drain in-flight sends instead of being cut off mid-flight. 60s matches the
default `tandem.relay.row-lease`, which is already constrained to exceed the delivery timeout
(checked at startup by the library). An operator who raises either must raise this too — stated in
the image documentation, since no code can check a value that lives in the container runtime.

**Two required values with no default**, both failing fast and by name: `spring.datasource.url`
(plus credentials) and `tandem.kafka.source`, the CloudEvents source URI. The latter already fails
with a message naming the property (`TandemRelayAutoConfiguration.tandemOutboxDispatcher`); the
former is Boot's own error.

**`tandem.relay.coordination` keeps the library default, `SINGLE`.** A container image invites
scaling, and scaling a `SINGLE` deployment to two replicas is a misconfiguration — not a
corruption (ordering and single-claim exclusivity are carried at the row by `IN_FLIGHT` +
`SKIP LOCKED`, HLD §3.2), but every instance then re-scans every bucket for no gain. The
temptation is to default the image to `LEASE`. **Rejected:** the same configuration would then
mean different things embedded and standalone, which is a worse trap than the one it avoids. The
`docker-compose` example ships with `LEASE` set and two replicas, and the image documentation
states the rule where an operator meets it.

### 6.2 Kafka producer properties from the environment — a real gap

`tandem.kafka.producer` is a `Map<String, String>` passed through to the Kafka producer, so its
keys are Kafka's own dotted names (`bootstrap.servers`, `security.protocol`, `sasl.jaas.config`).
**Spring's relaxed binding cannot express a dot inside a map key from an environment variable:**
`TANDEM_KAFKA_PRODUCER_BOOTSTRAP_SERVERS` binds to the key `bootstrap-servers`, which the Kafka
producer discards as unknown. The operator's symptom is `No resolvable bootstrap urls`, several
frames from the cause. For a container-first deployable — where environment variables are the
primary configuration channel — that is not a documentation problem, it is a defect.

`KafkaProducerEnvironmentPostProcessor` closes it: before binding, every `TANDEM_KAFKA_PRODUCER_*`
environment variable is mapped to `tandem.kafka.producer[<name>]` with the remainder lowercased
and underscores turned into dots. `TANDEM_KAFKA_PRODUCER_BOOTSTRAP_SERVERS` →
`tandem.kafka.producer[bootstrap.servers]`.

**The mapping is total for Kafka and knowingly not general.** Every Kafka client configuration key
is dot-separated lowercase words — none contains a dash or an underscore — so no key is
unreachable and none is ambiguous. If Kafka ever introduces one that is, it stays reachable
through a mounted YAML file or `SPRING_APPLICATION_JSON`; the post-processor never overwrites a
value the operator set explicitly through either.

This lives in the application, not the library. `tandem-spring-relay` is consumed by applications
that already have their own configuration conventions, and an `EnvironmentPostProcessor` that
rewrites their environment would be an unpleasant surprise. The image is where the environment
*is* the interface.

### 6.3 Instance identity

`tandem.relay.instance-id` defaults to a derived `tandem-<host>-<pid>-<rand>`. In a container the
hostname is the pod name, which is stable for a StatefulSet and ephemeral for a Deployment; either
is a correct lease owner id, since a vanished owner's lease simply expires and is reclaimed. The
image sets nothing and documents that `TANDEM_RELAY_INSTANCE_ID` is worth setting for stable
correlation between logs, the lease table and Admin API output.

### 6.4 The schema is not the image's job

The image **does not create, migrate or verify** the `tandem_*` tables. No code in the shipped
product touches DDL today — the baseline SQL is applied by the test harness and, in production, by
whoever owns the database. Two reasons not to change that here: applying DDL requires privileges a
relay should not hold, and the relay is not the only writer to that schema (the client's write-side
is), so a relay that migrates on startup would be one participant unilaterally changing a contract
the other depends on.

The startup guards that do exist stay: `BucketCountGuard` fails startup on a divergent bucket
count, and `BucketLeaseManager` fails with a message naming the missing lease tables. Those detect;
they never create.

---

## 7. Packaging & distribution

### 7.1 Executable jar

The Spring Boot Gradle plugin is applied to this module only, at the version already in the
catalog (`spring-boot-v4`), producing a layered executable jar via `bootJar`. It is the first use
of that plugin in the project, which is why it is scoped to the one module that is an application.

### 7.2 Container image

A hand-written multi-stage `Dockerfile` over `eclipse-temurin:21-jre`, extracting Boot's layered
jar with `layertools` so dependency layers cache independently of the application layer.

**Chosen over `bootBuildImage`/buildpacks**, which would produce an image with no Dockerfile at
all. Buildpacks hide the base image behind a builder image, which makes "which base am I running,
and has it been patched" an indirect question; they need a Docker daemon and a large builder pull
at build time; and they are a second, unfamiliar mechanism in a project that has no other
container tooling. A fifteen-line Dockerfile is legible, patchable and explicit — the same
preference that made this project hand-write its Admin API server rather than generate it.

Image properties: runs as a **non-root** user; no `HEALTHCHECK` instruction (orchestrators use the
probes of §5.2, and the compose example wires one explicitly); OCI `org.opencontainers.image.*`
labels for source, version, licence; `-XX:MaxRAMPercentage=75` so the heap follows the container
limit; **multi-architecture** `linux/amd64` + `linux/arm64` via buildx, since arm64 is now both a
common development machine and a common production instance type.

### 7.3 Release path and versioning

The image and the jar are built and published by the existing `release.yml` on a `v*` tag, after
the Maven Central staging step: image to **GHCR** (`ghcr.io/alirux/tandem-relay`) tagged with the
release version and `latest`, jar attached to the GitHub Release. The workflow gains
`packages: write`. `ci.yml` builds the image on every run without pushing, so a broken Dockerfile
fails at PR time rather than at release time.

**Versioned with the library, on the same `v<semver>` tag** — explicitly *not* the independent
scheme `tandem-cli` uses. The reasoning that separated the CLI does not transfer: the CLI's
compatibility contract is the Admin API's major version, so a library release that leaves the
OpenAPI untouched cannot affect it. This module is the opposite — it *is* the library, packaged.
Every library change is in it by construction, so an independent version would assert an
independence that does not exist. The image's own Spring Boot and JDK versions carry no semver
meaning; they are implementation details of a deployable.

---

## 8. Testing

One integration test, `TandemRelayApplicationIT`, tagged `integration` and wired into `check` like
every other Docker-bound test in the project. It boots the **real application** — not a sliced
context — against `TandemTestContainer`'s real PostgreSQL and real Kafka, and asserts:

1. a row inserted into the outbox is published to Kafka by the relay, end to end;
2. `/actuator/health` reports the relay `UP` on the management port, with the expected details;
3. with `tandem.admin.enabled=true`, `GET /tandem/admin/v1/outbox/summary` returns 200, and
   `GET /outbox/messages/{id}` renders `payload` as **real JSON** — the assertion that would have
   caught §2's defect, and the project's first coverage of `tandem-admin` on a genuine Boot 4
   classpath;
4. `TANDEM_KAFKA_PRODUCER_BOOTSTRAP_SERVERS` alone is enough to reach the broker (§6.2), exercised
   as a real environment variable rather than a property.

Assertion 3 is the reason this test is worth its runtime. Everything else here is wiring that
would fail loudly; that one is the class of failure this module exists to stop shipping silently.

The module is unpublished, so — like `tandem-sample-spring` — it declares its own JUnit/AssertJ
dependencies and its own `integrationTest` task rather than inheriting the shared convention, and
it is **not** added to `tandem-coverage`'s aggregation (published, tested modules only).

---

## 9. Database engines — PostgreSQL now, a second engine without restructuring

`tandem-jdbc` declares no JDBC driver at all (only a test-scoped one), by design: the driver is
the application's to choose. In this module Tandem is the application, so the image must carry
one.

**The image bundles one driver per supported engine, and lets the URL select it.** Spring Boot's
`DataSourceAutoConfiguration` already derives the driver class from `spring.datasource.url`, so
nothing needs to be configured and no dialect key is introduced here. Today that means PostgreSQL
alone — which already covers every Postgres-wire-compatible managed service, since those need no
driver of their own. When a second engine lands, adding it is one dependency line and one line of
documentation, with no restructuring and no second image. This design is deliberately indifferent
to *which* engine that turns out to be.

**Per-engine images were considered and rejected**: they double the build matrix, the release
artifacts and the documentation to save an amount of image size that does not matter, and they
force an operator to pick an image variant for a fact the connection URL already states.

Two other things stay untouched when a second engine arrives. **The image still applies no DDL**
(§6.4) — which is also what keeps another engine from doubling anything here. And **this module
still binds no `tandem.*` key of its own** (§6.1): if the JDBC layer ends up needing an explicit
engine or dialect property rather than detecting it, that key is the library's, appears in
`tandem-relay-reference.yml`, and flows through this image with no code change.

---

## 10. Module registration checklist

Per AGENTS.md, each omission below fails silently:

| List | Action |
|---|---|
| `settings.gradle.kts` | Add `tandem-relay` |
| `unpublishedModules` (root `build.gradle.kts`) | Add — **not** published to Central, and it needs its own Java 21 toolchain and its own test tasks |
| `tandem-bom` | **No** — not a Maven artifact |
| `tandem-coverage`'s `coveredProjects` | **No** — published, tested modules only |
| README module table · CONTRIBUTING project layout · LLD-base.md | Add; **and correct LLD-base.md**, which currently lists `tandem-relay` with a published `artifactId` |
| README "Future work" | Remove the `tandem-relay` bullet — it ships |
| THIRD-PARTY-NOTICES.md | **Yes**, despite not being on Central: the image and the jar redistribute the whole Spring Boot runtime, so the licence footprint is real. Follow the `tandem-cli` precedent and derive it from the **actual jar contents**, not from the dependency graph |
| open-questions-lld.md | Mark Q23 resolved, including §1.1's already-answered fourth part |

---

## 11. Decisions

### 11.1 Resolved

| Decision | Rationale |
|---|---|
| One image, roles by property | The gates already exist and default correctly; delivers the standalone Admin API of HLD-admin-api §4.1 at no extra cost (§4) |
| Spring Boot 4.1 + Java 21 | An application constrains no consumer; longest runway, and real validation of the dual-generation bet (§3.1) |
| Health indicator here, not in the library | The library ships no verdict because the embedding app owns the threshold — here Tandem is that app (§5) |
| Readiness, not liveness | A stalled relay should be taken out of service, not restarted: the likely cause is DB/broker, which a restart does not fix (§5.2) |
| Dockerfile over buildpacks | Explicit, patchable base image; no second toolchain (§7.2) |
| Versioned `v<semver>` with the library | The module *is* the library, packaged — unlike the CLI, whose contract is the API's major version (§7.3) |
| One driver per supported engine, one image | The URL already selects the engine; per-engine images cost more than they save (§9) |
| No DDL application | Requires privileges a relay should not hold, and the relay is not the schema's only writer (§6.4) |
| No `tandem.*` key of its own | A knob this module needs is a knob the library is missing (§6.1) |
| `EnvironmentPostProcessor` for producer keys | Relaxed binding cannot express a dotted map key from an env var; the mapping is total for Kafka's key namespace (§6.2) |
| `coordination` stays `SINGLE` | Divergent defaults between embedded and standalone would be a worse trap than the one it avoids (§6.1) |

### 11.2 Open

- **Kubernetes manifests / a Helm chart.** The compose example covers "try it"; a chart is a
  distribution surface with its own release cadence and its own compatibility promises. Deferred
  until there is demand — the image plus the documented probes is what a chart would wrap.
- **Exposing the Admin API and the relay from separate images.** Only worth revisiting if the
  combined image's dependency surface becomes an obstacle in practice.
- **`tandem-tracing-otel` as an opt-in variant image**, for adopters wanting OTel span emission
  without Micrometer Tracing (§3.2).
