# Tandem — LLD: Spring modules & configuration contract (`tandem-spring-producer`, `tandem-spring-relay`)

**Version:** 1.3
**Status:** Implemented and released — both modules built and tested against Boot 3.3.5 **and** 4.1.0
**Companion to:** [HLD.md](HLD.md) §3.1, §3.2, §10.1; [LLD-jdbc.md](LLD-jdbc.md); [LLD-kafka.md](LLD-kafka.md); [LLD-bucket-count-guard.md](LLD-bucket-count-guard.md)

Defines the **foundation** of the Spring Boot integration: which modules exist, the configuration
property contract they bind, and how the beans are assembled. This is deliberately **not** the whole
Spring integration — the write-side ergonomics (`TransactionalOutboxTemplate`, `@TransactionalOutbox`,
`OutboxEventMapper<T>`, the Spring application-events tier) and the optional adapters (Micrometer,
tracing) are separate, later increments, each with its own LLD, and are listed as open in §7. The
configuration contract comes first because everything else binds onto it.

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
optional extra. §1.2 specifies how the matrix is realised.

**No aggregator module.** An all-in-one `tandem-spring` depending on both was considered and
rejected: an application that both writes and runs an embedded relay simply declares both modules,
which costs it one dependency line and keeps the published surface at two artifacts.

### 1.1 How one artifact supports two Spring generations

A single jar cannot be *compiled* against two versions at once, so the mechanism is not "compile
against both" — it is "compile against one, force neither, resolve at runtime". Three parts:

1. **Compile against a single baseline.** The module compiles against one concrete version — the
   **lowest supported**, Spring Boot 3.x / Framework 6.x. The resulting bytecode holds only *symbolic
   references* to Spring (fully-qualified class names, method signatures, annotation types), not the
   Spring classes themselves.

2. **Declare Spring `compileOnly` (Gradle) / `provided` (Maven), never `implementation`/`api`.** This
   is the crux. A `compileOnly` dependency is visible to the compiler but is **omitted from the
   published POM**, so the baseline version is *not* propagated transitively to the consumer. Tandem's
   jar drags no Spring version onto the application classpath — it declares a dependency on
   `tandem-jdbc`/`tandem-kafka` (real, `api` scope) and on Spring only `compileOnly`.

3. **The consumer supplies the version; the JVM binds at runtime.** The consumer is a Spring Boot
   application — it brings its own Boot 3.x **or** 4.x via its starter/BOM. At class-load time the JVM
   resolves Tandem's symbolic references against whatever Spring is actually on the classpath. One jar,
   two runtimes.

**The condition that makes it sound.** Every Spring symbol Tandem references must exist with an
identical binary signature in **both** Framework 6.x and 7.x — the "common API subset". Tandem
restricts itself to API that is expected stable across the two (the `AutoConfiguration.imports`
mechanism, `@AutoConfiguration`, `@ConditionalOn…`, `@ConfigurationProperties` binding, the AOP model).
Where a referenced *member* is renamed or moved, the runtime usually fails loudly with
`NoSuchMethodError` / `NoClassDefFoundError`.

**Not every mismatch is loud, so two rules constrain how the modules are written.** A class name that only
appears as annotation *metadata* is read via ASM and never loaded, so naming a type absent on the other
generation degrades **silently** rather than throwing:

1. **Reference cross-generation autoconfigurations by name, never by class literal.** Boot 4 relocates the
   autoconfigurations Tandem orders itself against (`DataSourceAutoConfiguration`,
   `TransactionAutoConfiguration`) into their own modules under new packages, so a class literal in
   `@AutoConfiguration(after = …)` names a type that does not exist there and the ordering is lost with no
   error. Both modules use **`afterName`, listing both generations' coordinates**; unmatched names are
   ignored, so one jar satisfies both lines.
2. **Put class conditions on `@Bean` methods, not on a nested `@Configuration`.** A nested member
   configuration inside an `@AutoConfiguration` is **not processed** under Boot 4, so grouping optional
   beans behind a nested `@ConditionalOnClass` contributes nothing there — silently. Spring evaluates a
   method's conditions from ASM metadata before resolving its signature, so an optional type in the
   signature stays safe when absent.

**Verdict.** Boot 3 → 4 is a *major* framework bump (6 → 7), which is *permitted* to break binary
compatibility, so the single-artifact strategy is only as good as the matrix that checks it (§1.2). Under
the two rules above **one jar does satisfy both lines**, with the residual gaps §1.2 records. If an
incompatibility ever cannot be absorbed this way, the fallback is the version split (`-core` / `-boot3` /
`-boot4`, §1), adopted only then.

### 1.2 Verifying dual-generation compatibility

The dual-generation claim is only as good as the test that checks it. Realised as follows:

**The matrix lives in the build, not only in CI.** Each Spring module adds a `bootFourTest` task next to
`test` and wires it into `check`, so a single `./gradlew check` runs the autoconfiguration tests against
**both** generations, locally and in CI alike (consistent with the project convention that `check` is the
single source of truth). Versions are pinned in the version catalog: baseline **Boot 3.3.5** (Framework
6.1.x) and the latest **Boot 4.x** (4.1.0 at the time of writing, Framework 7.0.x) — the baseline stays
the *lowest* supported 3.x line while the 4.x line tracks the newest patch, so the matrix pits the oldest
jar Tandem compiles against against the newest runtime it claims to support. Both lines run on the same
**Java 17** toolchain, which is not a coincidence to be rechecked by experiment: Boot 4.x officially
requires "at least Java 17" (and supports up to 26), so Tandem's Java 17 baseline stays valid for both
generations. Boot 4.1.0 also requires Framework 7.0.8 or above, which is what the 4.x classpath resolves.

`bootFourTest` reuses the **already-compiled** test and main classes and only swaps Spring for the 4.x line
on its runtime classpath (a dedicated `bootFourTestRuntimeClasspath` configuration pinning the 4.x BOM). So
both runs exercise the *same baseline-compiled bytecode* — which is precisely the binary compatibility
under test — and neither recompiles the module's main jar. Running the in-memory tests twice adds seconds,
not a doubled build.

**The dual-run tests are lightweight `ApplicationContextRunner` tests** — no full context, no
container. Per module they assert:

- the autoconfiguration applies and its beans exist — `tandem-spring-producer`: an `OutboxRepository`;
  `tandem-spring-relay`: the `OutboxStore`, `TopicRouter`, `OutboxDispatcher`, `WorkerPool`;
- the `tandem.*` properties bind (e.g. `tandem.outbox.bucket-count`, `tandem.relay.batch-size` →
  `RelayConfig`);
- the conditionals behave — `tandem.relay.enabled=false` contributes no relay; a user `@Bean` replaces
  the autoconfigured one (`@ConditionalOnMissingBean`, §4);
- the bucket-count guard runs at startup.

**Wiring alone is a weak claim, so the matrix also runs behaviour.** Beans existing does not prove the
machinery works, and a major version is most likely to break the machinery. `tandem-spring-producer`
therefore has Docker-free **runtime** tests in a live context — a `@TransactionalOutbox` method is really
called and the template really executes, against an `InMemoryOutbox` and a resource-less transaction
manager. They pin the two mechanisms most exposed to a Spring major: **AspectJ proxying** (if the advice did
not intercept, nothing would reach the outbox) and the **composed `@Transactional`** of the annotation (if
Spring did not see it through `@AliasFor`, the aspect's active-transaction guard would throw).

**The matrix's signal is uneven, and that matters.** `tandem-spring-producer` carries the real signal: its
context-runner tests assert the autoconfiguration *applies* and each tier's beans exist, so a moved or
re-signed Spring symbol fails loudly. `tandem-spring-relay`'s no-Docker tests are mostly *negative* (the
relay backs off with no `DataSource`, or when disabled) and would pass even if the autoconfiguration
never loaded — so a green relay run alone proves little. Read the two together, and prefer adding
positive assertions to the relay's suite over trusting its colour.

**A green matrix is necessary, not sufficient.** It exercises what the tests *do*, so it cannot see a
mismatch that only degrades declared metadata — the relocated-autoconfiguration case of §1.1, rule 1, is
invisible to it and is caught only by checking what the annotations *name* against the other generation's
jars. Review that whenever an `@AutoConfiguration` ordering or a `@ConditionalOnClass` target is added.

**The heavier end-to-end smoke runs once, on the baseline only.** A `@SpringBootTest` over real
Postgres/Kafka via Testcontainers proves the wired producer + relay actually deliver under Spring, but it is
container-dominated, so it runs a **single** time against the baseline. Duplicating it across generations
would double its container startup for little added signal: the binary-compat question it re-answers is
already covered, far more cheaply, by the context-runner matrix above.

It lives in **`tandem-sample-spring`** — the only module that depends on both Spring modules, so neither
published module needs a test dependency on the other, and no module exists solely to host one test. It
boots the *documented sample application itself*, which makes CI the thing that keeps the published tutorial
from rotting. Because that module is unpublished it sits outside the shared build convention, so it declares
its own test dependencies and `integrationTest` phase; the coverage its run produces is therefore not in the
aggregated report, which is acceptable — the same production paths are covered by each module's own
integration tests. The sample's `CommandLineRunner` is excluded under the `test` profile: its demo writes and
console narration would be an uncontrolled precondition for an assertion.

**Known residual gaps on the 4.x line** (accepted, recorded so nobody mistakes green for complete):
end-to-end delivery and the *atomicity* of the tiers — a rollback discarding outbox rows — need a real
transactional resource and a broker, so they are exercised on the baseline only. `RelayLifecycle` actually
starting the pool is likewise baseline-only, and the `afterName` ordering of §1.1 is *declared* for 4.x
with nothing asserting it takes effect there. Closing these means running the container-backed tests on
both lines, which the trade-off above deliberately declines.

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

### 2.4 Deliverables of the contract: IDE metadata + a reference file

The property contract is not finished when the `@ConfigurationProperties` types compile — it must also
be **discoverable in the editor** and **documented as a copy-pasteable reference**. Two artifacts,
one source of truth.

**Single source of truth: the `@ConfigurationProperties` Javadoc.** Every property's meaning, default,
and unit lives as Javadoc on the corresponding field/record component. The tables in §2.1–§2.3 mirror
it; they must not drift from it.

**IDE tooltips + auto-completion — `spring-configuration-metadata.json`.** Each Spring module depends
on `spring-boot-configuration-processor` (annotation processor, `annotationProcessor` scope — compile
only, not shipped as a runtime dependency). At compile time it reads the properties-type Javadoc and
emits `META-INF/spring-configuration-metadata.json`, which Spring Boot IDEs (IntelliJ, VS Code Spring
Tools) use for property-name completion, type checking, default display, and the **hover tooltip** that
shows each property's description. The obligation this places on the code: **every property carries a
non-empty Javadoc description** — an undocumented property yields an empty tooltip.

**What the processor cannot infer — `additional-spring-configuration-metadata.json`.** A hand-written
`META-INF/additional-spring-configuration-metadata.json` (merged into the generated file at build)
covers what the processor misses:
- the raw map key `tandem.kafka.producer.*` (dynamic keys the processor cannot enumerate) — documented
  with a description and, ideally, `providers` hints pointing at the Kafka `ProducerConfig` keys;
- any default that is computed rather than a literal (e.g. `workers-per-instance` = `availableProcessors() * 2`);
- **deprecations** — when a property is renamed under the compatibility rule (§6), its old name stays
  bound with a `deprecation` entry (level + replacement), so the IDE flags it and points at the new name.

**Reference file — a documented `application.yml`.** The contract ships a reference configuration that
lists every `tandem.*` property with its default and a one-line explanation, as a starting point a user
copies and trims. It is generated from / kept in sync with the same property tables (§2.1–§2.3) — it is
a rendering of the contract, not a second definition. Committed under the module (e.g.
`src/main/resources/tandem-reference.yml`) and/or reproduced as an appendix here; whichever, it has no
independent authority over the Javadoc.

---

## 3. The cross-module `bucket-count` guard

`bucket-count` is the one value the two modules must agree on, and under the split topology they are
usually two separate processes configured separately. A mismatch (write-side inserts into buckets the
relay never polls) stops delivery **silently**, so the value is persisted in the database and
validated at startup by both sides, with a loud fail-fast on divergence.

This is a core/adapter and schema concern, not a Spring one — a pure reconciliation strategy and a
storage port in `tandem-core`, a JDBC adapter in `tandem-jdbc`. It is specified separately in
**[LLD-bucket-count-guard.md](LLD-bucket-count-guard.md)**. The guard is an explicit startup check
(`BucketCountGuard.check`) run against a plain `DataSource`, not something an adapter constructor does
— so each autoconfiguration runs it against the raw `DataSource` bean at startup: `tandem-spring-producer`
before exposing the write-side repository, `tandem-spring-relay` before starting the relay. Beyond that
call, the Spring layer only binds `tandem.outbox.bucket-count` (§2.1) into the components.

---

## 4. Autoconfiguration

Each module ships one `@AutoConfiguration` class, registered through
`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`. Both declare
`@AutoConfiguration(after = DataSourceAutoConfiguration.class)` so the application's `DataSource` is
already defined when Tandem's beans are created. Every contributed bean is `@ConditionalOnMissingBean`,
so an application can replace any single piece — most usefully a custom `TopicRouter` — without
abandoning the autoconfiguration.

### 4.1 The `DataSource` both modules bind to

Both modules consume the application's own `DataSource`; neither creates one. Each is
`@ConditionalOnSingleCandidate(DataSource.class)` — the same guard Spring Boot's own
`JdbcTemplateAutoConfiguration` uses: it resolves the `@Primary` one when several exist and **backs
off** (contributes nothing) when the choice is ambiguous, rather than guessing. An application with
multiple unqualified `DataSource`s therefore wires Tandem explicitly (its own `@Bean`s, which the
`@ConditionalOnMissingBean` guards then leave in place). Tandem never opens its own connection pool.

### 4.2 The `@ConfigurationProperties` types

The property contract (§2) binds through three `@ConfigurationProperties` types — the single source
of truth for names, defaults and Javadoc (§2.4):

| Type | Prefix | Module(s) |
|---|---|---|
| `TandemOutboxProperties` | `tandem.outbox` | producer **and** relay |
| `TandemRelayProperties` | `tandem.relay` | relay |
| `TandemKafkaProperties` | `tandem.kafka` | relay |

Their only job is to be **mapped onto the core configuration objects**, which stay the source of truth
for behaviour: `TandemRelayProperties` (+ `tandem.outbox.bucket-count`) is copied field-for-field onto
`RelayConfig.builder()` — the 1:1 naming of §2.2 is what keeps that mapping mechanical and driftless —
and `TandemKafkaProperties` becomes a `KafkaRelayConfig(source, defaultContentType, defaultDataSchema)`
plus the raw `producer` map. The properties types carry no logic beyond binding; all validation stays
where it already lives (`RelayConfig`'s row-lease invariant, the Kafka producer hardening).

### 4.3 `tandem-spring-producer` beans

The write-side module contributes exactly the plain tier:

1. runs `BucketCountGuard.check(dataSource, bucketCount)` as an explicit startup step **before** the
   repository is exposed (§3) — a mismatch fails context refresh, loudly;
2. contributes `OutboxRepository` = `new JdbcOutboxRepository(new TransactionAwareDataSourceProxy(dataSource), bucketCount)`.

The `TransactionAwareDataSourceProxy` is load-bearing, not decoration: `JdbcOutboxRepository` inserts on
whatever connection its `DataSource` hands it (its constructor does no I/O, LLD-jdbc), and the proxy hands
it the connection **bound to the application's active Spring transaction**. Without it the insert would
run on a separate, autocommitted connection and lose atomicity with the business state change — so it
is what makes all four write-side tiers transactional. It is `spring-jdbc`'s proxy (`compileOnly`;
present in any Spring app with a `DataSource`), wrapping the raw bean, so Tandem still opens no pool of
its own. The autoconfiguration is ordered `after` `TransactionAutoConfiguration` as well, so the
transaction manager the Template tier needs exists before its `@ConditionalOnBean` is evaluated.

The higher tiers (`TransactionalOutboxTemplate`, the `@TransactionalOutbox` aspect, the Spring-events
listener, the Jackson `PayloadSerializer`) are **Q22** and are not part of this increment (§7); a
`tandem-spring-producer` built to this LLD gives the *Plain* tier (HLD §3.1) and the guard, no more.

### 4.4 `tandem-spring-relay` beans

Conditional on `tandem.relay.enabled` (`@ConditionalOnProperty`, matchIfMissing = true, default true),
the relay module contributes the engine, each bean `@ConditionalOnMissingBean`:

1. `TopicRouter` = `TopicRouter.kebabWithSuffix(tandem.kafka.topic-suffix)`;
2. `OutboxDispatcher` = `new KafkaRelay(producerMap, topicRouter, kafkaRelayConfig)` — the constructor
   is where the producer hardening runs and the effective `delivery.timeout.ms` is fixed;
3. `OutboxStore` = `new JdbcOutboxStore(dataSource, tandem.relay.max-attempts)`;
4. `TandemMetrics` = `TandemMetrics.NOOP` (a real Micrometer bean overrides it once `tandem-micrometer`
   exists — hence `@ConditionalOnMissingBean`);
5. `BucketSource` = `BucketSource.forCoordination(relayConfig, dataSource)` — returns the in-process
   owner under `SINGLE`, the lease/member-backed one under `LEASE`, per `tandem.relay.coordination`;
6. `WorkerPool` = the full-topology constructor
   `new WorkerPool(outboxStore, outboxDispatcher, relayConfig, tandemMetrics, Clock.systemUTC(), BackoffStrategy.fullJitter(), bucketSource)`.

`tandem.relay.enabled=false` contributes none of these — the supported way to load the relay module
without running a relay (§2.2).

The `RelayConfig` is built from `tandem.outbox.bucket-count` plus `tandem.relay.*`, where every relay
property is **nullable and only overrides `RelayConfig`'s own default when set** — so the config object
stays the single source of truth for defaults and the two cannot drift (§2.2). `OutboxStore` and the
`WorkerPool` then take `maxAttempts`/the config from that one `RelayConfig` bean, not from the raw
properties, so the store and the pool can never disagree.

**Why the dispatcher is a constructor dependency of the pool.** The pool validates the row-lease
invariant against the delivery timeout the *dispatcher reports* (`OutboxDispatcher.deliveryTimeoutMillis()`),
not against a configured value — the footgun removed before publication (§5). Wiring the dispatcher as
a constructor argument of the `WorkerPool` bean is what forces it to exist first; the bean graph, not a
comment, enforces the order.

### 4.5 Relay lifecycle — a `SmartLifecycle`, not bean init

The `WorkerPool` must **start after** the context is fully built (its `DataSource` and Kafka producer
live) and **stop before** the context tears those down, draining in-flight sends gracefully. A `@Bean`
`initMethod`/`destroyMethod` is the wrong tool: init runs mid-refresh, too early and with no ordering
guarantee against the infrastructure beans. So the module contributes a thin **`SmartLifecycle`** bean
(the `WorkerPool` itself stays a plain `tandem-core`/JDBC type, unaware of Spring) whose:

- `start()` calls `workerPool.start()` — which itself performs the fail-fast checks (the `rowLease >
  delivery.timeout.ms` invariant against the dispatcher's reported timeout, and the `LEASE` lease-table
  precondition via `BucketSource.validateOnStart`) before spawning any worker thread, so a
  misconfiguration fails startup rather than surfacing at runtime;
- `stop()` calls `workerPool.stop()` — the graceful drain (finish in-flight, release `LEASE` ownership),
  distinct from `kill()`, which the autoconfiguration never calls;
- `isRunning()` mirrors the pool.

`autoStartup` is true; the default phase is used (the relay depends only on ordinary singleton beans,
which are constructed before any `SmartLifecycle` starts and destroyed after all have stopped, so no
custom phase is needed). Container images that deploy the same jar as a pure write-side simply set
`tandem.relay.enabled=false`, and no lifecycle bean is contributed at all.

**The lifecycle bean is declared with its concrete type, not `SmartLifecycle`.** `@ConditionalOnMissingBean`
on the `SmartLifecycle` *interface* would back off whenever the application has any other lifecycle bean —
which every real Boot app does — silently leaving the relay wired but never started. Declaring the bean as
the concrete `RelayLifecycle` scopes the condition to Tandem's own type. (This is a case the minimal
`ApplicationContextRunner` wiring tests cannot catch — they have no other lifecycle beans — so the
end-to-end integration test runs with an unrelated `SmartLifecycle` present to guard it.)

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
with a warning) but must not be renamed or removed in place. A rename is expressed as a `deprecation`
entry in `additional-spring-configuration-metadata.json` (§2.4) — the old key stays bound and the IDE
points at the replacement. Unknown `tandem.*` keys must not fail binding, so a configuration file
shared between two versions stays usable by both.

---

## 7. Open points

- **Q22 — write-side ergonomics.** ✅ Now specified separately in
  [LLD-spring-producer.md](LLD-spring-producer.md): the `TransactionalOutboxTemplate`, the
  `@TransactionalOutbox` aspect, the Spring application-events tier, `OutboxEventMapper<T>`, and the
  optional object-payload serialization. It was excluded from *this* increment on purpose — the
  configuration contract is what everything else binds to, so it was specified and reviewed first.
- **Micrometer.** A `TandemMetrics` implementation backed by Micrometer belongs to
  `tandem-micrometer`, not here. Note that several port methods have no caller in the relay today,
  so wiring an adapter alone would not produce lag telemetry.
- **`tandem.relay.coordination=LEASE` in a Spring context.** Behaviourally identical to manual
  assembly, but the derived `instance-id` deserves a review against typical container deployments,
  where hostnames may be recycled.
