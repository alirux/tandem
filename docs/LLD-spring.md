# Tandem — LLD: Spring Boot integration (`tandem-spring-producer`, `tandem-spring-relay`)

**Version:** 0.1
**Status:** Work in progress
**Companion to:** [HLD.md](HLD.md) §3.1, §3.2, §10.1; [LLD-jdbc.md](LLD-jdbc.md); [LLD-kafka.md](LLD-kafka.md)

Defines the Spring Boot integration: which modules exist, the configuration property contract they
bind, and how the beans are assembled. This document covers the **configuration contract only**; the
write-side ergonomics (`TransactionalOutboxTemplate`, `@TransactionalOutbox`, `OutboxEventMapper<T>`)
are a separate, later increment and are listed as open in §7.

---

## 1. Module topology (resolves Q21)

Two published modules, split by **role**, not by Spring Boot generation:

| Module | Depends on | Purpose |
|---|---|---|
| `tandem-spring-producer` | `tandem-jdbc` | Write-side only: the outbox INSERT. **Never** pulls Kafka. |
| `tandem-spring-relay` | `tandem-jdbc`, `tandem-kafka` | Relay engine + CloudEvents publishing. |

**Why split by role.** The minimal-client-footprint invariant (HLD §1.3) says the client-imported
write-side must not force Kafka onto the application classpath. A single module with an optional
Kafka dependency and `@ConditionalOnClass` relay autoconfiguration would also achieve this, but only
as long as every conditional stays correct: the invariant would rest on convention. The split makes
it structural — a `tandem-spring-producer` user *cannot* acquire Kafka transitively, because the
module does not depend on it. This mirrors the reasoning already applied to the relay worker model:
structural exclusivity with a loud failure mode beats convention-based exclusivity with a silent one.

**Why not split by Spring Boot generation.** Boot 3.x and 4.x share everything Tandem relies on —
the autoconfiguration import mechanism, the `jakarta.*` namespace, `@AutoConfiguration` /
`@ConditionalOn…`, `@ConfigurationProperties` binding, and the AOP / `@Transactional` model — and both
keep a Java 17 baseline. Each module is therefore **one artifact** compiled against the API subset
common to Spring Framework 6.x and 7.x. A version split (`-core` / `-boot3` / `-boot4`) doubles the
published surface and protects no invariant; adopt it only if a real binary incompatibility appears.

**CI obligation.** Because a single artifact claims dual-generation support, it must be *tested*
against a version matrix — at least one Boot 3.x line and one Boot 4.x line. A green single-version
build does not prove dual-generation compatibility, so the matrix is part of this design, not an
optional extra.

**No aggregator module.** An all-in-one `tandem-spring` depending on both was considered and
rejected: an application that both writes and runs an embedded relay simply declares both modules,
which costs it one dependency line and keeps the published surface at two artifacts.

---

## 2. Property contract

Three namespaces, aligned to the module split. Every key is kebab-case, matching the names already
promised in shipped user-facing text — the relay's row-lease invariant error names
`tandem.relay.row-lease`, and `KafkaRelayConfig` documents `tandem.kafka.source` and
`tandem.kafka.default-content-type`. Those names are **already public** and are honoured here rather
than renamed.

| Prefix | Bound by | Covers |
|---|---|---|
| `tandem.outbox.*` | producer **and** relay | Values both sides must agree on |
| `tandem.relay.*` | relay only | The relay engine (`RelayConfig`) |
| `tandem.kafka.*` | relay only | CloudEvents binding + Kafka producer |

### 2.1 `tandem.outbox.*` — shared

| Property | Type | Default | Maps to |
|---|---|---|---|
| `tandem.outbox.bucket-count` | int | `256` | `JdbcOutboxRepository(dataSource, bucketCount)` and `RelayConfig.bucketCount` |

Bound by **both** modules. It is the only value the two sides must agree on, and it must never change
after first deployment — it is baked into every stored row's `bucket`. §3 specifies the guard that
makes a mismatch impossible to miss.

### 2.2 `tandem.relay.*` — relay engine

One key per `RelayConfig` setting, 1:1 in name and default, so the property contract and the
programmatic builder never drift:

| Property | Type | Default |
|---|---|---|
| `tandem.relay.enabled` | boolean | `true` |
| `tandem.relay.coordination` | `SINGLE` \| `LEASE` | `SINGLE` |
| `tandem.relay.instance-id` | String (≤ 64 chars) | derived `tandem-<host>-<pid>-<rand>` |
| `tandem.relay.bucket-lease` | Duration | `30s` |
| `tandem.relay.workers-per-instance` | int | `availableProcessors() * 2` |
| `tandem.relay.poll-interval` | Duration | `100ms` |
| `tandem.relay.batch-size` | int | `100` |
| `tandem.relay.row-lease` | Duration | `60s` |
| `tandem.relay.max-attempts` | int | `10` |
| `tandem.relay.retention` | Duration | `14d` |
| `tandem.relay.cleanup-batch-size` | int | `1000` |
| `tandem.relay.reclaim-interval` | Duration | `5s` |
| `tandem.relay.cleanup-interval` | Duration | `15m` |

`tandem.relay.enabled=false` is the supported way to deploy the relay module without running a relay
— for example when the same application image is deployed both as a write-side service and as a
dedicated relay process, selected by configuration.

### 2.3 `tandem.kafka.*` — publishing

| Property | Type | Default |
|---|---|---|
| `tandem.kafka.source` | URI | **required** — no default; startup fails if absent |
| `tandem.kafka.default-content-type` | String | `application/json` |
| `tandem.kafka.default-data-schema` | URI | unset (omitted from the envelope) |
| `tandem.kafka.topic-suffix` | String | `-topic` |
| `tandem.kafka.producer.*` | Map<String,String> | empty |

`tandem.kafka.producer.*` is a **raw passthrough** to the Kafka producer, handed to the existing
hardening step unchanged: it validates the unsafe-override invariants (`enable.idempotence` must not
be false, `acks` must be `all`/`-1`, `max.in.flight.requests.per.connection` ≤ 5), fills the safe
defaults, and forces the serializers the CloudEvents binary binding requires.

Spring Boot's own `spring.kafka.producer.*` is deliberately **not** reused. Tandem mandates producer
settings and fails fast on unsafe overrides, so consuming Spring's defaults would mean reconciling
two sources of truth for values that are not negotiable; it would also drag `spring-kafka` onto the
classpath of a module that only needs `kafka-clients`. An application that already configures its own
Kafka producer keeps it — Tandem's producer is separate by design.

---

## 3. The cross-module `bucket-count` guard

`bucket-count` is the one value the two modules must agree on, and under the split topology they are
usually two separate processes configured separately. A mismatch (write-side inserts into buckets the
relay never polls) stops delivery **silently**, so the value is persisted in the database and
validated at startup by both sides, with a loud fail-fast on divergence.

This is a core/adapter and schema concern, not a Spring one — a pure reconciliation strategy and a
storage port in `tandem-core`, a JDBC adapter in `tandem-jdbc`. It protects the manual assembly path
too, and the Spring modules inherit it purely by wiring the same components. It is therefore specified
separately in **[LLD-bucket-count-guard.md](LLD-bucket-count-guard.md)**. The Spring layer adds
nothing to it beyond binding `tandem.outbox.bucket-count` (§2.1) into the components that carry it.

---

## 4. Autoconfiguration

Each module ships one `@AutoConfiguration` class, registered through
`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.

**`tandem-spring-producer`** — requires a `DataSource`; contributes an `OutboxRepository` bean.

**`tandem-spring-relay`** — requires a `DataSource`, is conditional on `tandem.relay.enabled`
(default true), and contributes the `OutboxStore`, `TopicRouter`, `OutboxDispatcher` and the
`WorkerPool`, started and stopped with the application lifecycle.

Every contributed bean is `@ConditionalOnMissingBean`, so an application can replace any single piece
— most usefully a custom `TopicRouter` — without abandoning the autoconfiguration.

**Startup ordering matters for the relay.** The relay validates the row-lease invariant against the
delivery timeout the *dispatcher* reports, so the dispatcher must be constructed before the pool
starts. Wiring it as a constructor dependency of the pool is what guarantees this; nothing here may
reorder those two steps.

---

## 5. Deliberately not exposed

`RelayConfig.deliveryTimeoutMs` gets **no property**. It exists as a fallback for validating the
row-lease invariant when the wired dispatcher cannot report its own effective delivery timeout — and
in `tandem-spring-relay` the dispatcher is always the Kafka one, which *does* report it. A property
there would therefore never take effect while appearing to configure a safety check. That is exactly
the footgun removed before publication, when the invariant was validated against a hand-copied
configuration value instead of the producer's real one; re-introducing it as a property would undo
that fix. Applications that need a different delivery timeout set
`tandem.kafka.producer.delivery.timeout.ms`, which the relay then reads as the authoritative value.

---

## 6. Compatibility rules for this contract

Property names are a public contract and evolve under the project's compatibility rule: additive
changes only within a major version. A key may gain a default or be deprecated (bound and honoured,
with a warning) but must not be renamed or removed in place. Unknown `tandem.*` keys must not fail
binding, so a configuration file shared between two versions stays usable by both.

---

## 7. Open points

- **Q22 — write-side ergonomics.** `TransactionalOutboxTemplate` API, the `@TransactionalOutbox`
  aspect, `OutboxEventMapper<T>` signature, and the Spring application-events tier. Deliberately
  excluded from this increment: the configuration contract is what everything else binds to, so it
  is specified and reviewed first.
- **Micrometer.** A `TandemMetrics` implementation backed by Micrometer belongs to
  `tandem-micrometer`, not here. Note that several port methods have no caller in the relay today,
  so wiring an adapter alone would not produce lag telemetry.
- **`tandem.relay.coordination=LEASE` in a Spring context.** Behaviourally identical to manual
  assembly, but the derived `instance-id` deserves a review against typical container deployments,
  where hostnames may be recycled.
