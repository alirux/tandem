# Tandem — LLD: Spring modules & configuration contract (`tandem-spring-producer`, `tandem-spring-relay`)

**Version:** 1.6
**Status:** Implemented and released — both modules built and tested against Boot 3.3.13, **3.5.16**, **and** 4.1.0
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

### 1.2 Verifying compatibility across the three lines

The multi-generation claim is only as good as the test that checks it. Realised as follows:

**The matrix lives in the build, not only in CI.** Each Spring module adds a `bootLatestThreeTest` task
and a `bootFourTest` task next to `test`, and wires both into `check` (`tandem-admin` adds two more for
the JSON-binding axis of §1.3), so a single `./gradlew check` runs
the autoconfiguration tests against **all three lines**, locally and in CI alike (consistent with the
project convention that `check` is the single source of truth). Versions are pinned in the version
catalog: baseline **Boot 3.3.13** (Framework 6.1.x, `test`), the latest **Boot 3.x patch** (`3.5.16` at
the time of writing, Framework 6.2.x, `bootLatestThreeTest`), and the latest **Boot 4.x**
(`4.1.0` at the time of writing, Framework 7.0.x, `bootFourTest`). The baseline stays the *lowest*
supported 3.x line, deliberately never auto-bumped (§1.2.1); the other two each track their own line's
newest release. This closes a gap the two-line matrix left open: Framework 6.2.x sits *between* the two
lines that were actually tested, and is not merely an untested midpoint — some upstream Spring Framework
CVE fixes ship starting at 6.2.x and are never backported to 6.1.x, so 6.1.x-only testing cannot prove
compatibility with the line most consumers who track CVEs will actually run in production. All three
lines run on the same **Java 17** toolchain, which is not a coincidence to be rechecked by experiment:
Boot 4.x officially requires "at least Java 17" (and supports up to 26), so Tandem's Java 17 baseline
stays valid across all three. Boot 4.1.0 also requires Framework 7.0.8 or above, which is what the 4.x
classpath resolves.

`bootLatestThreeTest` and `bootFourTest` each reuse the **already-compiled** test and main classes and only
swap Spring on their own runtime classpath (dedicated `bootLatestThreeTestRuntimeClasspath` /
`bootFourTestRuntimeClasspath` configurations, each pinning its own BOM). So all three runs exercise the
*same baseline-compiled bytecode* — which is precisely the binary compatibility under test — and none
recompiles the module's main jar. Running the in-memory tests three times adds seconds, not a doubled
build.

**The multi-run tests are lightweight `ApplicationContextRunner` tests** — no full context, no
container. What they actually assert, per module:

- `tandem-spring-producer`: the autoconfiguration applies and each tier's bean exists (the Jackson
  `PayloadSerializer`, the Template, the `@TransactionalOutbox` aspect, the events listener + registry);
  `tandem.outbox.bucket-count` binds, default included; the conditionals behave (no `DataSource`, no
  transaction manager, a user `@Bean` replacing the autoconfigured one).
- `tandem-spring-relay`: the whole engine is wired from a `DataSource` plus the Kafka settings — router,
  dispatcher (a **real** Kafka producer, constructed), store, bucket source, pool, `RelayConfig`,
  `TandemMetrics.NOOP`; `tandem.outbox.*`/`tandem.relay.*` bind onto `RelayConfig` through Spring, with
  unset keys keeping `RelayConfig`'s own defaults; a user `TopicRouter` replaces the autoconfigured one;
  and it backs off with no `DataSource`, when disabled, or fails naming a missing `tandem.kafka.source`.
  The relay's tests replace exactly one bean, `RelayLifecycle`, with a non-starting one — starting the
  pool opens the database, which is the integration test's job.

One thing the matrix deliberately does **not** cover, so nobody reads more into a green run: the
**bucket-count guard never executes** in it (the producer's positive cases supply their own
`OutboxRepository`, so the JDBC bean that runs the guard backs off; the relay's guard runs from the
`RelayLifecycle` those tests neutralise). It is covered by `BucketCountGuardIT` and by each module's
integration test.

#### 1.2.1 Why Dependabot does not touch any of the three pins

All three catalog entries (`spring-boot`, `spring-boot-v3-latest`, `spring-boot-v4`) resolve to the same
Maven coordinate, `org.springframework.boot:spring-boot-dependencies` — only the pinned version differs.
Dependabot's `ignore` matches by coordinate, not by version-catalog key, so there is no way to tell it
"leave the frozen baseline alone but keep bumping the two trackers": a rule scoped to that coordinate
silences all three identically. [.github/dependabot.yml](../.github/dependabot.yml) therefore ignores the
coordinate outright, and all three numbers are moved by hand, in the same deliberate way as any other
change to this file: `spring-boot` only moves when the project is intentionally raising its declared
minimum supported version (a compatibility-contract change, not a routine bump); `spring-boot-v3-latest`
and `spring-boot-v4` move whenever a newer patch exists on their line, checked periodically rather than
on every upstream release. Whichever one changes, the accompanying doc references in this section, in
[LLD-micrometer.md](LLD-micrometer.md), and in [README.md](../README.md) move with it in the same commit.

**The matrix earns its keep, concretely.** Constructing the real Kafka producer under the 4.x line is
what caught Kafka 4's change of the default `linger.ms`, which had made Tandem's fixed
`delivery.timeout.ms` unable to build a producer at all on `kafka-clients` 4.x (LLD-kafka §1) — a
break no baseline-only run could see, because the Boot 4 BOM is what pulls the newer client in.

**Wiring alone is a weak claim, so the matrix also runs behaviour.** Beans existing does not prove the
machinery works, and a major version is most likely to break the machinery. `tandem-spring-producer`
therefore has Docker-free **runtime** tests in a live context — a `@TransactionalOutbox` method is really
called and the template really executes, against an `InMemoryOutbox` and a resource-less transaction
manager. They pin the two mechanisms most exposed to a Spring major: **AspectJ proxying** (if the advice did
not intercept, nothing would reach the outbox) and the **composed `@Transactional`** of the annotation (if
Spring did not see it through `@AliasFor`, the aspect's active-transaction guard would throw).

**Both modules now carry real signal.** Each one's context-runner tests assert the autoconfiguration
*applies* and that its beans exist, so a moved or re-signed Spring symbol fails loudly on either side.
That was not always true — the relay's suite was once purely negative (backs off with no `DataSource`,
or when disabled), which would have passed even if the autoconfiguration never loaded. Keep it that way:
a conditional-only test proves the absence of a relay, never the presence of one.

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

### 1.3 The JSON binding is a second axis — Jackson 2 vs Jackson 3

The three-line matrix above varies **Spring**. A module that renders JSON has a second thing varying
underneath it, and it does not move with the Boot line the way one would assume.

**Measured, not assumed** (resolved from Maven Central; the same check is worth repeating before
trusting any statement here). `spring-boot-starter-web` → `spring-boot-starter-json` →
`spring-boot-jackson` → **`tools.jackson.core:jackson-databind`** on *every* 4.x line:

| Boot line | JSON binding it brings | Jackson 2 present? |
|---|---|---|
| 3.3.13 / 3.5.16 | `com.fasterxml.jackson.core:jackson-databind` 2.x | yes, it *is* the binding |
| 4.0.0 / 4.0.2 | `tools.jackson.core:jackson-databind` **3.0.x** | **no** — only `jackson-annotations` 2.20 |
| 4.1.0 | `tools.jackson.core:jackson-databind` **3.1.x** | **no** — only `jackson-annotations` 2.21 |

Two consequences that are easy to get wrong:

1. **The switch happened at 4.0.0, not 4.1.** Both 4.x BOMs still *manage* a `jackson-2-bom` version
   and both publish the opt-in `spring-boot-jackson2` module, which makes it look like 4.0.x still
   carries Jackson 2. It does not, transitively.
2. **The axis is the host application's classpath, not the Boot line.** A Boot 4 application may add
   `spring-boot-jackson2` and run Jackson 2 deliberately. So the real matrix has four cells, and a
   per-line artifact (`-boot3` / `-boot4`) would still be wrong for one of them:

|  | Jackson 2 | Jackson 3 |
|---|---|---|
| **Boot 3.x** | the normal case | does not arise |
| **Boot 4.x** | `spring-boot-jackson2`, opt-in | the normal case |

**The rule this produces:** a module rendering JSON may name Jackson's **annotations**
(`com.fasterxml.jackson.annotation.*`, the one artifact both generations share) and **neither
generation's databind**. `tandem-admin` therefore renders its payload as a JSON *text fragment*
through `@JsonRawValue` rather than as a parsed tree, and pins its timestamps with
`@JsonFormat(shape = STRING)` instead of relying on a mapper it configures. Both annotations were
verified to behave identically on Jackson 2.17.3 and Jackson 3.1.4, producing byte-identical output.

**Two gates, because neither covers the other:**

- **The footprint gate** (`JacksonFootprintTest`, runs with every `test`) reads the module's compiled
  classes and fails on any reference to `com.fasterxml.jackson.databind` or `tools.jackson`. It reads
  bytecode rather than sources because the reference that broke Boot 4 lived in a `@Bean` method's
  *signature* — invisible to a passing read, and resolved by Spring while introspecting the whole
  configuration class, which is why one such reference took down every bean in that file.
- **The stock-Boot-4 gate** (`jacksonThreeTest`, a minimal source set of its own) compiles and runs
  against the latest 4.x line with Jackson 2's databind excluded from its classpath — and asserts that
  exclusion, so "this is a stock Boot 4 classpath" is verified rather than claimed. It covers what the
  footprint gate cannot: that the rendering is actually right, through Spring's own Jackson 3
  converter. **Its own classpath, not a swap of `bootFourTest`'s or the shared test source set's**,
  because those carry `openapi-request-validator-core` — a library needed for other tests, not this
  gate, whose own Jackson-2-internal transitives (`jackson-datatype-jsr310`, `jackson-dataformat-yaml`)
  survive excluding `jackson-databind` and end up *orphaned*: their bytecode still references databind
  types that are now absent. Spring's standalone `MockMvc` scans for Jackson modules and trips over
  those broken jars with the same `NoClassDefFoundError` shape as the original defect, for a reason
  that has nothing to do with this module's own code — reusing the heavier, already-tangled classpath
  surfaced more complexity than a fresh, minimal one avoids. One 4.x line, not two: the defect and its
  fix are sensitive to which Jackson *generation* is present, not which Jackson 3 *minor* — 4.0.0 and
  4.0.2 were checked by hand instead (backlog item 23), the same "track the newest of a line" trade-off
  `bootLatestThreeTest` already makes for Boot 3.x above.

**`bootFourTest` keeps Jackson 2 on its classpath on purpose.** It represents the opt-in cell of the
table above, which is a real deployment — not a stock one. Reading it as "Boot 4 is covered" is
exactly the mistake that let a defect ship: `tandem-admin` 0.6.0 could not start on *any* 4.x line,
and every task on the build was green.

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
| `tandem.relay.metrics-interval` | Duration | `10s` |
| `tandem.relay.log-every-rows` | long | `10000` |
| `tandem.relay.seq-regression-detection` | boolean | `true` |

`seq-regression-detection` is the only opt-*out* in the table: on by default because a violated
write-side ordering precondition is otherwise completely silent (LLD-jdbc §3.9), and switchable off
where writers to one aggregate are serialised by construction — the detection is bounded and therefore
partial, and costs one tracked entry per recently-published aggregate per worker.

Two of these carry a second role the key name does not reveal, both from the relay loop's sleep
timing (LLD-jdbc §3.1): `poll-interval` is also the *first* wait after a worker cycle throws, and
`reclaim-interval` is the **cap** that failure backoff grows to. Lowering `reclaim-interval` to
recover faster from expired leases therefore also shortens the longest a failing worker will wait
before retrying a database that is down — the same direction, but worth knowing before tuning it.

`tandem.relay.enabled=false` is the supported way to deploy the relay module without running a relay
— for example when the same application image is deployed both as a write-side service and as a
dedicated relay process, selected by configuration.

### 2.3 `tandem.kafka.*` — publishing

| Property | Type | Default |
|---|---|---|
| `tandem.kafka.source` | URI | **required** — no default; startup fails, naming the key, if absent |
| `tandem.kafka.default-content-type` | String | `application/json` |
| `tandem.kafka.default-data-schema` | URI | unset (omitted from the envelope) |
| `tandem.kafka.topic-suffix` | String | `-topic` |
| `tandem.kafka.producer.*` | Map<String,String> | empty |

`tandem.kafka.producer.*` is a **raw passthrough** to the Kafka producer, handed to the existing
hardening step unchanged: it validates the unsafe-override invariants (`enable.idempotence` must not
be false, `acks` must be `all`/`-1`, `max.in.flight.requests.per.connection` ≤ 5), fills the safe
defaults, and forces the serializers the CloudEvents binary binding requires.

`tandem.kafka.source` is the only key here with no default. Its absence is checked by the
autoconfiguration itself and fails context refresh with a message naming `tandem.kafka.source` —
not with the `NullPointerException` the CloudEvents config would otherwise raise several frames away
from the configuration that caused it.

**Tuning the producer stays safe by construction, including the timeout interaction.** The filled-in
`delivery.timeout.ms` grows to `max(30s, linger.ms + request.timeout.ms)`, so neither setting
`linger.ms` through this map nor running a newer `kafka-clients` (whose default `linger.ms` is no
longer 0) collides with Kafka's own `delivery.timeout.ms ≥ linger.ms + request.timeout.ms` rule
(LLD-kafka §1). This matters most in a Spring Boot 4 application, which resolves `kafka-clients` 4.x
through its own BOM. Because the relay validates `rowLease` against the *effective* timeout, a
producer that outgrows `tandem.relay.row-lease` fails loudly at startup instead of quietly shrinking
the safety margin.

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
and unit lives as Javadoc on the corresponding field/record component. The tables in §2.1–§2.3, §2.5
mirror it; they must not drift from it.

**IDE tooltips + auto-completion — `spring-configuration-metadata.json`.** Each Spring module depends
on `spring-boot-configuration-processor` (annotation processor, `annotationProcessor` scope — compile
only, not shipped as a runtime dependency). At compile time it reads the properties-type Javadoc and
emits `META-INF/spring-configuration-metadata.json`, which Spring Boot IDEs (IntelliJ, VS Code Spring
Tools) use for property-name completion, type checking, default display, and the **hover tooltip** that
shows each property's description. The obligation this places on the code: **every property carries a
non-empty Javadoc description** — an undocumented property yields an empty tooltip.

**What the processor cannot infer — `META-INF/additional-spring-configuration-metadata.json`.** A
hand-written file, merged into the generated one at build time, covers the processor's blind spots. Only
`tandem-spring-relay` needs one today (the producer's single key is fully inferred); it declares:
- `tandem.relay.enabled` — it gates the whole autoconfiguration via `@ConditionalOnProperty` and binds
  to no properties type, so the processor cannot see it at all. Without this entry the IDE would neither
  complete nor document the supported way to deploy the relay module without a relay;
- the raw map key `tandem.kafka.producer.*` (dynamic keys the processor cannot enumerate) — a description
  plus **value hints** (`tandem.kafka.producer.keys`) naming the producer settings a user actually
  reaches for, each with the Tandem-specific caveat where there is one;
- **deprecations** — when a property is renamed under the compatibility rule (§6), its old name stays
  bound with a `deprecation` entry (level + replacement), so the IDE flags it and points at the new name.

A computed default (`workers-per-instance` = `availableProcessors() * 2`) has no metadata expression:
JSON holds literals only, so the Javadoc description carries it in prose and the reference file below
repeats it.

**Build trap, load-bearing:** under Gradle the processor reads that file from the *processed* resources,
so `compileJava` must take `processResources` as an input. Without it the file is **silently ignored** —
the build stays green and the keys it documents simply vanish from the metadata.

**Reference file — a documented `application.yml`.** Each module ships one, listing every `tandem.*`
key it binds with its default and a one-line explanation, as a starting point a user copies and trims:
`tandem-producer-reference.yml` and `tandem-relay-reference.yml`, at the root of each module's
resources (distinct names, because both land on the classpath of an application that runs a relay).
Every key is shown commented out at its default, so a deployment that sets none of them behaves exactly
as the file describes; only the required `tandem.kafka.source` is active.

The file is a **rendering** of the contract, not a second definition — it has no authority over the
Javadoc. What keeps it from drifting is not discipline but a test: each module asserts, in both
directions, that the reference names exactly the keys the **generated metadata** declares. The
parsing/comparison logic itself is written once — `ConfigurationMetadataReference`, a `tandem-test`
**test fixture** (`java-test-fixtures`, not `tandem-test`'s published main sources), since the two
modules' own tests are its only callers and neither may depend on the other (the write side must
never pull `tandem-kafka`). Kept out of the published jar entirely — the build explicitly skips the
`testFixtures*` component variants, since `java-test-fixtures` publishes them by default otherwise.

Adding, renaming or removing a property without touching the reference fails the build.

### 2.5 `tandem.metrics.*` — the tandem-micrometer adapter

Numbered after §2.4 rather than inline with §2.2/§2.3, deliberately: renumbering would invalidate the
existing `LLD-spring-config §2.1`/`§2.2`/`§2.3`/`§2.4` citations already in code Javadoc across three
modules — additive-only numbering, same reasoning §6 applies to the property contract itself.

| Property | Type | Default |
|---|---|---|
| `tandem.metrics.max-publish-latency` | Duration | `5m` (`MicrometerTandemMetrics.DEFAULT_MAX_EXPECTED_PUBLISH_LATENCY`) |

Bound by `TandemMetricsProperties`, registered on `TandemMicrometerAutoConfiguration` — not on
`TandemRelayAutoConfiguration`/`TandemRelayProperties` — since it configures the metrics *adapter*
(`tandem-micrometer`, an optional dependency of this module, §5) rather than the relay engine. Only
takes effect when that adapter is actually wired (a `MeterRegistry` bean present); otherwise the
no-op `TandemMetrics` default ignores it. Nullable, same pattern as `TandemRelayProperties`: unset
leaves `MicrometerTandemMetrics`'s own default in force (LLD-micrometer §2).

### 2.6 `tandem.tracing.*` — trace capture and span emission

| Property | Module | Type | Default |
|---|---|---|---|
| `tandem.tracing.enabled` | `tandem-spring-producer` | boolean | `false` |
| `tandem.tracing.correlation-id-mdc-key` | `tandem-spring-producer` | String | `correlationId` |
| `tandem.tracing.publish-span` | `tandem-spring-relay` | boolean | `false` |

One namespace, **two independently bound records** — `TandemTracingProperties` in each module, each
registered on its own autoconfiguration. They are deliberately not merged: under the split topology
the write side and the relay are usually separate processes configured separately, and the two
decisions differ in cost (a header on the row versus export volume in the tracing backend).

Every key here is **explicit only**: none is ever auto-enabled because a tracing library happens to be
on the classpath (HLD-tracing.md §9).

`enabled` gates the write side's `TracePropagator` bean; absent, the repository falls back to
`TracePropagator.NOOP`. Which propagator is contributed depends on what the application runs:

- **Micrometer Tracing present** (a `Tracer` and a `Propagator` bean) → `TracePropagator.composite` of
  `MicrometerTracePropagator` (the distributed trace context, in the application's own propagation
  format) and `MdcCorrelationTracePropagator`.
- **Otherwise** → `MdcCorrelationTracePropagator` alone: the correlation id read from
  `correlation-id-mdc-key` in SLF4J's MDC, falling back to the explicit `TandemContext` API when that
  key is unset. No distributed trace context, since carrying one needs a tracing library.

`publish-span` gates the relay's `TandemSpanRecorder` bean (`MicrometerTandemSpanRecorder`), which
needs a `Propagator` bean; absent either, `KafkaRelay` falls back to `TandemSpanRecorder.NOOP` and only
propagation mode is in force. It builds on the write side's capture — a row carrying no trace context
gets no span (HLD-tracing.md §5).

⚠️ An optional type must never appear in a `@Bean` method's **erased signature**. The conditions are
read from ASM, so naming `Tracer`/`Propagator` in `@ConditionalOnClass`/`@ConditionalOnBean` is safe —
but Spring reflects over every method of the configuration class to build its bean definitions, and a
bare `Tracer` parameter then throws `NoClassDefFoundError` in any application without the library,
before a condition can back the bean off. Both modules therefore take `ObjectProvider<Tracer>` /
`ObjectProvider<Propagator>`, whose type argument erasure removes. The same reasoning is why the
optional-Jackson bean takes `ObjectProvider<ObjectMapper>`.

**No unit test in either module catches this**, and it is worth knowing why: `FilteredClassLoader`
only makes the classpath *checks* fail while the configuration class stays loaded by the parent
loader, so `@ConditionalOnClass` backs the bean off and the introspection never happens (verified by
reintroducing the bug — the filtered-classloader tests stayed green). What catches it is a classpath
genuinely without the library, i.e. `tandem-sample-spring`'s smoke integration test — one more reason
that test earns its keep beyond covering producer and relay together (§1.2).

⚠️ Both modules order their autoconfiguration **after the tracing autoconfigurations by name, listing
both generations** — Boot 4 moved every one of them out of `spring-boot-actuator-autoconfigure` into
`spring-boot-micrometer-tracing{,-brave,-opentelemetry}`. This is §1.1's rule, and the failure mode is
the same silent one: evaluated too early, the `@ConditionalOnBean` sees no tracer and the feature
quietly does nothing.

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
`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`. Both order
themselves after Spring Boot's `DataSourceAutoConfiguration` — the producer after
`TransactionAutoConfiguration` too (§4.3) — so the application's `DataSource` and transaction manager
are already defined when Tandem's beans are created. The ordering is declared with **`afterName`,
listing both generations' coordinates**, never a class literal (§1.1, rule 1).

Every contributed bean is `@ConditionalOnMissingBean`, so an application can replace any single piece —
most usefully a custom `TopicRouter` — without abandoning the autoconfiguration.

### 4.1 The `DataSource` both modules bind to

Both modules consume the application's own `DataSource`; neither creates one. Each is
`@ConditionalOnSingleCandidate(DataSource.class)` — the same guard Spring Boot's own
`JdbcTemplateAutoConfiguration` uses: it resolves the `@Primary` one when several exist and **backs
off** (contributes nothing) when the choice is ambiguous, rather than guessing. An application with
multiple unqualified `DataSource`s therefore wires Tandem explicitly (its own `@Bean`s, which the
`@ConditionalOnMissingBean` guards then leave in place). Tandem never opens its own connection pool.

### 4.2 The `@ConfigurationProperties` types

The property contract (§2) binds through four `@ConfigurationProperties` types — the single source
of truth for names, defaults and Javadoc (§2.4):

| Type | Prefix | Module(s) |
|---|---|---|
| `TandemOutboxProperties` | `tandem.outbox` | producer **and** relay |
| `TandemRelayProperties` | `tandem.relay` | relay |
| `TandemKafkaProperties` | `tandem.kafka` | relay |
| `TandemMetricsProperties` | `tandem.metrics` | relay (§2.5), registered on `TandemMicrometerAutoConfiguration`, not this section's `TandemRelayAutoConfiguration` |

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
2. contributes `OutboxRepository` = `new JdbcOutboxRepository(new TransactionAwareDataSourceProxy(dataSource), bucketCount, tracePropagator)`,
   where `tracePropagator` is whatever `TracePropagator` bean exists, or `TracePropagator.NOOP` when
   none does;
3. contributes `TracePropagator`, **only** when `tandem.tracing.enabled=true` (§2.6) — a
   `@ConditionalOnProperty`, not a classpath check, so an unrelated dependency landing SLF4J or a
   tracing library on the classpath cannot turn this on by itself. It is
   `TracePropagator.composite(MicrometerTracePropagator, MdcCorrelationTracePropagator)` when the
   application runs Micrometer Tracing, and `MdcCorrelationTracePropagator` alone otherwise.

The `TransactionAwareDataSourceProxy` is load-bearing, not decoration: `JdbcOutboxRepository` inserts on
whatever connection its `DataSource` hands it (its constructor does no I/O, LLD-jdbc), and the proxy hands
it the connection **bound to the application's active Spring transaction**. Without it the insert would
run on a separate, autocommitted connection and lose atomicity with the business state change — so it
is what makes all four write-side tiers transactional. It is `spring-jdbc`'s proxy (`compileOnly`;
present in any Spring app with a `DataSource`), wrapping the raw bean, so Tandem still opens no pool of
its own. The autoconfiguration is ordered `after` `TransactionAutoConfiguration` as well, so the
transaction manager the Template tier needs exists before its `@ConditionalOnBean` is evaluated.

The higher tiers (`TransactionalOutboxTemplate`, the `@TransactionalOutbox` aspect, the Spring-events
listener, the Jackson `PayloadSerializer`) were **Q22**, excluded from *this* increment because the
configuration contract is what they bind onto. They now ship in the same autoconfiguration class,
specified in [LLD-spring-producer.md](LLD-spring-producer.md) §6 — read the two sections together for
the module's full bean list.

None of these beans validate or generate `seq` — it arrives already set on the `OutboxMessage`/
`TandemAggregate` the caller built, from whatever the domain used as its source (commonly a JPA
`@Version`). That source has a flush-timing hazard this autoconfiguration cannot see or guard against
at wiring time — and the same flush timing decides whether the domain's own write lock serializes
concurrent writers at all: [LLD-spring-producer.md](LLD-spring-producer.md) §7.

### 4.4 `tandem-spring-relay` beans

Conditional on `tandem.relay.enabled` (`@ConditionalOnProperty`, matchIfMissing = true, default true),
the relay module contributes the engine, each bean `@ConditionalOnMissingBean`:

1. `TopicRouter` = `TopicRouter.kebabWithSuffix(tandem.kafka.topic-suffix)`;
2. `OutboxDispatcher` = `new KafkaRelay(producerMap, topicRouter, kafkaRelayConfig, spanRecorder)` —
   the constructor is where the producer hardening runs and the effective `delivery.timeout.ms` is
   fixed; `spanRecorder` is whatever `TandemSpanRecorder` bean exists, or `TandemSpanRecorder.NOOP`;
3. `OutboxStore` = `new JdbcOutboxStore(dataSource, tandem.relay.max-attempts)`;
4. `TandemMetrics` = `TandemMetrics.NOOP` (a real Micrometer bean overrides it once `tandem-micrometer`
   exists — hence `@ConditionalOnMissingBean`);
5. `BucketSource` = `BucketSource.forCoordination(relayConfig, dataSource)` — returns the in-process
   owner under `SINGLE`, the lease/member-backed one under `LEASE`, per `tandem.relay.coordination`;
6. `TandemSpanRecorder` = `MicrometerTandemSpanRecorder`, **only** when
   `tandem.tracing.publish-span=true` and the application has a `Propagator` bean (§2.6); absent, the
   relay emits no span and instrumented mode stays off;
7. `WorkerPool` = the full-topology constructor
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
  `tandem-micrometer`, not here. The relay bean is contributed with `@ConditionalOnMissingBean`
  (§4.4), so such an adapter replaces `TandemMetrics.NOOP` with no change to this module. Wiring one
  is now enough to get telemetry: the relay reports the backlog, its age and the live worker count on
  `tandem.relay.metrics-interval` (LLD-jdbc §4). The one signal still without a caller is
  `recordUncoveredBuckets`, whose query is specified together with that module.
- **`tandem.relay.coordination=LEASE` in a Spring context.** Behaviourally identical to manual
  assembly, but the derived `instance-id` deserves a review against typical container deployments,
  where hostnames may be recycled.
