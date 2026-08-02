# Tandem — `tandem-micrometer` LLD

**Version:** 0.1 (Draft)
**Module:** `tandem-micrometer` · package `com.codingful.tandem.micrometer`
**Depends on:** `tandem-core`, `io.micrometer:micrometer-core`. **Relay-side only** — never on the
client write-side (HLD §1.3: "wired only where the relay runs").
**Resolves:** Q31. See [open-questions-lld.md](open-questions-lld.md) §G.
**Companion to:** [HLD.md](HLD.md) §7 (the metric contract this binds), [LLD-jdbc.md](LLD-jdbc.md) §4
(what each metric measures and how often it is read), [LLD-spring-config.md](LLD-spring-config.md)
§1.1/§4.4 (the autoconfiguration rules this module's Spring wiring must follow).

`tandem-micrometer` is the adapter that makes the relay's metrics reachable: a `TandemMetrics`
implementation backed by a Micrometer `MeterRegistry`. Framework-agnostic — usable by a manually
assembled relay exactly as by a Spring one — with the Spring *wiring* (autoconfiguring the bean)
living in `tandem-spring-relay`, per Q31.

---

## 1. Module dependency shape

Structurally identical to `tandem-kafka`: a single-purpose adapter over one real third-party
library, published, `api`-scoped (its types appear in the module's own public constructor
signature, so a consumer needs them on their compile classpath too — the same reasoning
`tandem-kafka` uses for `kafka-clients`).

```kotlin
dependencies {
    api(project(":tandem-core"))
    api(libs.micrometer.core)
}
```

**Version:** pin to the project's own Spring Boot baseline generation — Micrometer **1.13.6**
(what Boot 3.3.5 manages; verified against the real `spring-boot-dependencies` POM, not assumed).
Micrometer has stayed on the same major (`1.x`) since, including through Boot's own 3→4 jump (Boot
4.1.0 manages **1.17.0**), so the binary-compatibility risk this baseline carries is structurally
lower than the Spring Framework 6→7 jump — but that comparison is not a substitute for a real test
(§6): this project's own 2026-07-27 lesson is that a green single-version run does not prove
compatibility with the other.

**Not `compileOnly`.** Unlike Spring in the two Spring modules, Micrometer is not optional *within*
this module — it is the entire reason `tandem-micrometer` exists. `compileOnly` belongs one layer up,
in `tandem-spring-relay`'s optional dependency *on this module* (§5).

---

## 2. Meter mapping

One row per `TandemMetrics` method. Every name below is `tandem.outbox.*` unless noted; dots are
Micrometer's own naming convention (HLD §7), translated by each registry implementation (e.g.
Prometheus renders `.` as `_` and appends its own unit/type suffixes — which is exactly why `published`
carries no `.rate` suffix of its own, corrected in HLD §7 on the same day this LLD was written: a
throughput rate is a query the TSDB derives, never something Tandem computes).

| Port method | Meter name | Type | Tags |
|---|---|---|---|
| `recordLag(long)` | `lag.count` | Gauge | — |
| `recordLagAgeSeconds(double)` | `lag.age_seconds` | Gauge | — |
| `incrementPublished(long)` | `published` | Counter | — |
| `recordFailed(long)` | `failed.count` | Gauge | — |
| `recordBlocked(long)` | `blocked.count` | Gauge | — |
| `incrementRetry()` | `retry.count` | Counter | — |
| `incrementLeaseExpired(long)` | `lease_expired.count` | Counter | — |
| `recordActiveWorkers(int)` | `workers.active` | Gauge | — |
| `recordWorkerCycleAgeSeconds(double)` | `workers.cycle_age_seconds` | Gauge | — |
| `recordUncoveredBuckets(int)` | `bucket.uncovered` | Gauge | — |
| `recordConfigInvalid(String)` | `tandem.relay.config.invalid` | Gauge | `check=<name>` |

---

## 3. Gauge registration mechanics

**Micrometer gauges are sampled, not set** (verified against Micrometer's own reference docs, not
assumed): a `Gauge` holds only a *weak* reference to a state object plus a value function, and reads
that function at scrape time. There is no `gauge.set(value)` — the port's `record*(value)` calls
instead mutate a state holder the `Gauge` was pointed at once, at construction time.

```java
public final class MicrometerTandemMetrics implements TandemMetrics {
    private final AtomicLong lag = new AtomicLong();
    private final AtomicLong lagAgeMillis = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong blocked = new AtomicLong();
    private final AtomicInteger activeWorkers = new AtomicInteger();
    private final AtomicLong workerCycleAgeMillis = new AtomicLong();
    private final AtomicInteger uncoveredBuckets = new AtomicInteger();
    private final Counter published;
    private final Counter retries;
    private final Counter leaseExpired;
    private final Map<String, AtomicInteger> configInvalidByCheck = new ConcurrentHashMap<>();
    private final MeterRegistry registry;

    public MicrometerTandemMetrics(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
        Gauge.builder("tandem.outbox.lag.count", lag, AtomicLong::get).register(registry);
        Gauge.builder("tandem.outbox.lag.age_seconds", lagAgeMillis, v -> v.get() / 1000d).register(registry);
        Gauge.builder("tandem.outbox.failed.count", failed, AtomicLong::get).register(registry);
        Gauge.builder("tandem.outbox.blocked.count", blocked, AtomicLong::get).register(registry);
        Gauge.builder("tandem.outbox.workers.active", activeWorkers, AtomicInteger::get).register(registry);
        Gauge.builder("tandem.outbox.workers.cycle_age_seconds", workerCycleAgeMillis, v -> v.get() / 1000d)
                .register(registry);
        Gauge.builder("tandem.outbox.bucket.uncovered", uncoveredBuckets, AtomicInteger::get).register(registry);
        this.published = Counter.builder("tandem.outbox.published").register(registry);
        this.retries = Counter.builder("tandem.outbox.retry.count").register(registry);
        this.leaseExpired = Counter.builder("tandem.outbox.lease_expired.count").register(registry);
    }

    @Override public boolean isEnabled() { return true; }
    @Override public void recordLag(long pending) { lag.set(pending); }
    @Override public void recordLagAgeSeconds(double age) { lagAgeMillis.set(Math.round(age * 1000)); }
    @Override public void recordFailed(long count) { failed.set(count); }
    @Override public void recordBlocked(long count) { blocked.set(count); }
    @Override public void recordActiveWorkers(int n) { activeWorkers.set(n); }
    @Override public void recordWorkerCycleAgeSeconds(double age) { workerCycleAgeMillis.set(Math.round(age * 1000)); }
    @Override public void recordUncoveredBuckets(int n) { uncoveredBuckets.set(n); }
    @Override public void incrementPublished(long n) { published.increment(n); }
    @Override public void incrementRetry() { retries.increment(); }
    @Override public void incrementLeaseExpired(long n) { leaseExpired.increment(n); }

    @Override
    public void recordConfigInvalid(String check) {
        configInvalidByCheck.computeIfAbsent(check, name -> {
            AtomicInteger holder = new AtomicInteger();
            Gauge.builder("tandem.relay.config.invalid", holder, AtomicInteger::get)
                    .tag("check", name).register(registry);
            return holder;
        }).set(1);
    }
}
```

**Why `AtomicInteger` and not the literal `1` for `config.invalid`.** Micrometer's own guidance is
explicit: constructing a gauge over a primitive/immutable `Number` is "always incorrect," since a
`Gauge` is meant to track a *mutable* value it re-reads on each scrape. `computeIfAbsent` also makes
registration idempotent per distinct `check` name — the same check reported twice does not attempt a
duplicate registration.

**The `lifetime` of the adapter instance is the strong reference Micrometer needs.** A `Gauge` only
weakly references its state object, so *something* must keep `lag`/`failed`/etc. alive for the
process's life — here, that is simply the fields of `MicrometerTandemMetrics` itself, which the
Spring wiring below registers as a singleton bean living exactly as long as the relay does.

---

## 4. `config.invalid`'s scrape-timing gap (accepted, not solved)

`recordConfigInvalid` fires exactly once, immediately before the relay aborts the process (LLD-jdbc
§3.5). Under a pull-based scraper (Prometheus) the process can exit before the next scrape ever reads
it — the same absence-vs-presence problem HLD §7 documents for a dead relay, sharper here because the
process itself is what is ending. **Decision: register it as an ordinary gauge anyway, accept the
gap, document it** (now also in HLD §7's table) — the relay's own `ERROR` log line at the same call
site is the durable channel for this specific case; the metric is a bonus for a continuous-poll
backend or a sidecar reading the registry directly, not the primary signal for this one failure mode.

---

## 5. Spring wiring (in `tandem-spring-relay`, not here)

**Decided (Q31): autoconfigured, not left to manual `@Bean` wiring** — matching every other capability
in this Spring integration (`JacksonPayloadSerializer` in `tandem-spring-producer` is the closest
precedent: an optional-library-backed port implementation gated by `@ConditionalOnClass`, contributed
by the module itself).

**The one structural difference from Jackson.** `JacksonPayloadSerializer` lives *inside* the module
that conditions on it, so one `@ConditionalOnClass` suffices. `MicrometerTandemMetrics` lives in a
*separate* module, so `tandem-spring-relay` needs a new **optional** dependency:

```kotlin
// tandem-spring-relay/build.gradle.kts
dependencies {
    compileOnly(project(":tandem-micrometer"))
    compileOnly(libs.micrometer.core)   // for MeterRegistry in the @ConditionalOnClass/@Bean signature
}
```

This is the first *optional* dependency between two Tandem modules — every existing sibling
dependency (`tandem-jdbc`, `tandem-kafka` in `tandem-spring-relay`) is mandatory (`api`).

**Not a second `@Bean` method inside `TandemRelayAutoConfiguration` — a separate, explicitly ordered
`@AutoConfiguration` class.** The first draft of this LLD put the Micrometer bean alongside the
existing `TandemMetrics tandemMetrics()` NOOP bean in one class, both `@ConditionalOnMissingBean`,
relying on "the more specific method is declared first" to make the Micrometer one win. That reliance
turned out to rest on an **unverified assumption**: Spring's own reference docs state plainly that
`@ConditionalOnMissingBean` ordering is only *guaranteed* across explicitly ordered auto-configuration
*classes* (`@AutoConfigureBefore`/`@AutoConfigureOrder`), never promised for the order of multiple
`@Bean` methods inside one class. Given a documented, official mechanism exists for exactly this case,
using it costs nothing and removes the guesswork:

```java
// New file: TandemMicrometerAutoConfiguration.java
@AutoConfiguration(before = TandemRelayAutoConfiguration.class)
@ConditionalOnClass({MeterRegistry.class, MicrometerTandemMetrics.class})
public class TandemMicrometerAutoConfiguration {

    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean(TandemMetrics.class)
    TandemMetrics tandemMicrometerMetrics(MeterRegistry registry) {
        return new MicrometerTandemMetrics(registry);
    }
}
```

Registered in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
alongside `TandemRelayAutoConfiguration`. `TandemRelayAutoConfiguration`'s own
`TandemMetrics tandemMetrics()` NOOP bean is **unchanged** — Boot's guaranteed cross-class ordering
means it runs *after* this class, sees the Micrometer bean already registered when the conditions
above were met, and its own `@ConditionalOnMissingBean` backs off exactly as it already does for a
user-supplied `TandemMetrics` bean. No change needed to that class at all.

`before = TandemRelayAutoConfiguration.class` is a **direct class literal**, deliberately — unlike the
`afterName` string-based ordering `TandemRelayAutoConfiguration` itself uses against Spring's own
`DataSourceAutoConfiguration`/`TransactionAutoConfiguration` (LLD-spring-config §1.1). That rule exists
because *Spring's* classes relocate between Boot generations; `TandemRelayAutoConfiguration` is
Tandem's own class, always in the same package, on both generations — a literal reference to it is
exactly as safe as the class-level `@ConditionalOnClass` above, and simpler to read.

**Two class-level conditions, deliberately both on `MeterRegistry` and the adapter class itself:**
`@ConditionalOnClass(MeterRegistry.class)` alone would let the whole class load with Micrometer
present but `tandem-micrometer` absent from the same application — an unlikely but real combination
(an app already using Micrometer for something else, without ever adding `tandem-micrometer`) — in
which the `@Bean` method's body would reference a genuinely missing `MicrometerTandemMetrics` class.
Both together is the same defensive shape `TandemProducerAutoConfiguration` already uses for Jackson.

**Verified, not just designed — 2026-07-30.** `TandemMicrometerAutoConfigurationTest` proves the
cross-class ordering empirically: with a `MeterRegistry` bean present the Micrometer bean wins over
the NOOP default, without one it falls back cleanly, and an application's own `TandemMetrics` bean
wins over both — real `ApplicationContextRunner` assertions, not an assumption about framework
internals. A separate test reads the `AutoConfiguration.imports` resource directly and asserts both
classes are listed; nothing else in the suite would have caught that specific line being forgotten,
since `AutoConfigurations.of(...)` in the wiring tests bypasses that file entirely.

---

## 6. Testing

**No mocks — `SimpleMeterRegistry`.** Micrometer ships its own dependency-free, in-memory
`MeterRegistry` implementation for exactly this purpose: a real collaborator, not a test double
Tandem has to write. Unit tests construct `new MicrometerTandemMetrics(new SimpleMeterRegistry())`,
call the port methods, and assert on `registry.get(name).gauge().value()` /
`.counter().count()` — pinning both the meter names in §2 and the mechanics in §3 (in particular,
that a *second* `recordLag` call moves the *same* gauge rather than registering a duplicate — the
state-holder pattern's whole point).

**Dual-generation check for the wiring, not for this module.** `tandem-micrometer` itself has no
Spring dependency, so it needs no `bootFourTest`. `TandemMicrometerAutoConfiguration` does, and must be
added to `tandem-spring-relay`'s existing dual-generation classpath (`bootFourTestRuntimeClasspath`)
once built — this is the "real dual-generation test" §1 says the version-number argument alone
doesn't substitute for.

---

## 7. Registration checklist (new module)

Per AGENTS.md's module checklist — every item below, in the same change that adds the module:

- `settings.gradle.kts`
- `tandem-bom/build.gradle.kts` (published)
- `tandem-coverage`'s `coveredProjects` (published + tested)
- README.md module table (🔜 → ✅) + "Key features" (the metrics bullet already describes the
  signals; only the "Micrometer adapter 🔜 planned" clause needs flipping)
- `docs/LLD-base.md` — already has the artifactId/package row (§ nothing to add, only to confirm)
- `CONTRIBUTING.md` project layout table
- `THIRD-PARTY-NOTICES.md` — new row: `tandem-micrometer` → `micrometer-core` (Apache-2.0)

---

## 8. Scope

**In:** everything above — the adapter class, its Spring autoconfiguration, the meter mapping.

**Out (unchanged from HLD/LLD-jdbc):** per-bucket dimensions on any gauge (`lag`, `failed`, etc. stay
global-only — a separate, larger design question the backlog already tracks independently); a
`tandem-micrometer-otel` bridge or any OpenTelemetry exporter — Micrometer's own OTLP registry
implementation already covers that path without Tandem writing anything.
