# AGENTS.md

Conventions for anyone (human or AI agent) working in this repository. These are
project-wide and committed on purpose — follow them for every change.

Tandem is a Java library implementing the Transactional Outbox Pattern: reliable,
causally-ordered event delivery from PostgreSQL/MySQL to Apache Kafka without CDC
infrastructure. See [docs/HLD.md](docs/HLD.md) for the architecture and design
decisions.

## Design philosophy — Pareto's Law

Tandem must be **useful and simple for at least 80% of use cases** — a service on
PostgreSQL/MySQL + Kafka needing reliable, ordered event delivery. Apply this decision
rule to every change: **if a feature makes the common case harder, slower, or more
confusing in order to serve a minority case, it does not belong in the core path** — make
it opt-in, a separate adapter/optional module, or out of scope. Prefer sensible defaults
over configuration, keep `tandem-core` dependency-free and the common API minimal, and
integrate with specialised tools (Flink, Kafka Streams) for the hard 20% rather than
reinventing them. See [docs/HLD.md](docs/HLD.md) §1.1.

## Architecture — Hexagonal (Ports & Adapters)

When a component has a clear functional core with at least one port and one adapter,
structure it as **Ports & Adapters**: the core holds pure logic and *defines* the ports
(interfaces); technology-specific code is an *adapter* implementing a port. Invariant:
**adapters depend on the core; the core never depends on an adapter** — `tandem-core`
stays dependency-free. The in-memory adapter (`InMemoryOutbox`) is what lets the core be
tested without a database. Do not impose this ceremony where there is no swappable
boundary (Pareto, §1.1). See [docs/HLD.md](docs/HLD.md) §1.2.

## API-first

Any external API Tandem exposes (starting with the Admin API) is built **contract-first**:
the OpenAPI document is the source of truth, authored and reviewed **before** the
implementation. The implementation must conform to the committed contract — generate stubs
from it or validate against it in CI; never back-derive the spec from the code. **Every API
change starts in the spec**: edit and review the OpenAPI, then implement; a breaking
contract change is a breaking library change (semver). The Admin API contract lives at
[docs/admin-api.openapi.yaml](docs/admin-api.openapi.yaml). See [docs/HLD-admin-api.md](docs/HLD-admin-api.md).

**Validate every API contract in CI** so spec problems are caught at build time,
independent of any editor. Specmatic (below) consolidates this — it fails on a malformed
contract as part of its conformance/backward-compat checks, so a standalone validate step is
largely redundant; `redocly lint` stays optional for style/governance rules only.

**Design for backward *and* forward compatibility — for *every* contract, not just the REST
API.** This covers the **DB schema** (`tandem_*` tables) and the **published Kafka messages**
(CloudEvents envelope + headers) as much as the API, because the split topology runs
client/relay/admin at possibly-different versions on the same DB and event stream (HLD §1.4).
Evolve **additively only** (new optional columns/fields/endpoints/extensions; never remove,
rename, retype, make-required, narrow ranges, or change an identifier like an error `type`
slug or an event `type`); breaking changes go to a new major version (`/v2`; DB: versioned
migration). And keep readers **forward-compatible / tolerant**: in SQL select **named columns,
never `SELECT *`** and tolerate extra columns; for Kafka ignore unknown headers/extension
attributes; for REST ignore unknown fields and enum values and keep schemas **open** (no
`additionalProperties: false`). **Over-strict validation breaks readers the moment the
contract grows.**

**Error responses follow RFC 9457** (Problem Details, `application/problem+json`) for any
external API Tandem exposes — `type` / `title` / `status` / `detail` / `instance`, extensions
allowed. Not a bespoke error shape. The `type` is always a canonical
`https://tandem.codingful.com/problems/{slug}` URL (never `about:blank`); the kebab-case
`{slug}` is the stable identifier and must never change once published (dereferenceable docs
are optional — adding them later is not a contract change).

**Contract testing is provider-side spec conformance, never consumer-driven.** Tandem
publishes its API contract (OpenAPI) to unknown/external consumers, so the provider stays
authoritative — verify the implementation against the spec (swagger-request-validator in the
integration tests; Specmatic for generative conformance + spec backward-compatibility
checks). Do **not** introduce Pact / Spring Cloud Contract or any consumer-driven contract
tooling. See [docs/HLD-admin-api.md](docs/HLD-admin-api.md) §5.

## Minimal client footprint

The part of Tandem the **client app imports** — the write-side (the outbox INSERT) — must
carry the **minimum external dependencies, ideally none**. `tandem-core` stays
zero-dependency; the write-side must not pull in the Kafka client, the CloudEvents SDK,
tracing libraries, a mandatory JSON binding, or Spring (unless the user opts into a Spring
tier). Anything needing an external dependency belongs on the relay/optional side, never on
the client path. Where the write-side needs a library, prefer the client's existing one
(`provided`/optional) or a pluggable SPI with no forced default. See [docs/HLD.md](docs/HLD.md) §1.3.

When you change a **redistributed compile/runtime dependency** — add or remove an `api`/
`implementation` dependency, or bump the version of one, in any published module (`tandem-core`,
`tandem-jdbc`, `tandem-kafka`) — update [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md) in the same
change: keep its per-module table, dependency list (name, version, license), and license-text
sections in sync. Test-only and benchmark-only dependencies are excluded and need no update.

## Logging

**Logging API is per-module, tied to the minimal-client-footprint boundary (§1.3), not a
blanket "use SLF4J" rule.** `tandem-core` and `tandem-jdbc` carry the client write-side in the
same jar as the relay engine, so both must log via the JDK's dependency-free
**`java.lang.System.Logger`** — never add SLF4J/Log4j2 there, it would land on the client's
classpath. `tandem-kafka` is relay-only and already pulls `slf4j-api` transitively via
`kafka-clients`, so it logs via **SLF4J** at no extra footprint cost. `tandem-sample` and
`tandem-benchmark` are leaf apps, not libraries — they may take a concrete logging backend, and
both use `slf4j-simple` (zero-config; their real output is `System.out` report text, so a
configurable backend would be weight for nothing). The library **ships no logging configuration** (no `logback.xml`/`log4j2.xml`) —
routing logs is the consuming application's job, same reasoning as every other unopinionated
default (§1.1). See [docs/HLD-logging.md](docs/HLD-logging.md).

1. **Always pass the `Throwable` to the logger**, never just `exception.getMessage()`
   concatenated into the string — that silently discards the stack trace and is the single
   most common way to make an `ERROR` log undebuggable.
2. **Every `ERROR`/`WARN` includes the identifiers needed to find the affected record**
   without re-running the failing operation: bucket id, worker index, aggregate id, Kafka
   topic/partition, outbox row id. A bare message with no id is close to useless once more
   than one relay instance or worker is running.
3. **Fixed message text, then a flat `name:value` tail — never interleave data mid-sentence.**
   `"Relay worker died; restarting workerIndex:3"`, not `"Relay worker 3 died; restarting"`.
   The message stays a constant string (greppable, aggregatable across occurrences); every
   variable goes after it as `name:value` pairs separated by `, `, always in the same order.
   Applies equally to SLF4J `{}` placeholders and `System.Logger` string concatenation — only
   *how* the value is inserted differs, never *where*. No brackets, no `=`, just `name:value`.
4. **Level by frequency, not by importance**: once per relay lifecycle or coordination event →
   `INFO`; once per poll/claim/dispatch cycle → `DEBUG`; once per row → `TRACE` (never `DEBUG`
   — a busy outbox would flood it). `FATAL` is not used — an unrecoverable failure is an
   `ERROR` plus the exception that stops the process.
5. **Never log payload/event bodies, credentials, tokens, JDBC URLs with embedded passwords,
   or bound SQL parameter values.** Structural identifiers, counts, timings, and the opaque
   `correlation-id`/`trace_id` values are safe to log; the business data they point to is not.
   **This applies to `toString()` too** — every `tandem-core` type reachable from a log
   statement (`OutboxRecord`, `OutboxMessage`, …) must keep the same rule: never print
   `payload`/`headers` values (print `payloadBytes=<length>`/`headerNames=<keySet()>` instead),
   only the structural identifiers. **Every such `toString()` gets a unit test** that builds
   the type with a fake-sensitive payload/header/error value and asserts the rendered string
   `doesNotContain(...)` it — a plain "does it compile" check is not enough, the whole point is
   to catch a future field addition that accidentally starts printing something it shouldn't
   (see `OutboxMessageTest`/`OutboxRecordTest` for the pattern).
6. **CLI/report output is not logging.** `tandem-benchmark`'s scenario results and
   `tandem-sample`'s walkthrough narration are the tool's product, meant to be read or piped —
   keep them as `System.out`. Reserve the logger for genuine diagnostics an operator would
   filter by level.
7. **Every log message starts with a capital letter** — a style rule, applied regardless of
   level or logging API (`System.Logger` or SLF4J).

## Javadoc

Applies to the four published modules (`tandem-core`, `tandem-jdbc`, `tandem-kafka`,
`tandem-test`) — their javadoc jar is generated automatically by the
`com.vanniktech.maven.publish` plugin and is the first thing an external consumer sees
in their IDE. Package-private/internal classes (e.g. `RelayWorker`, `MiniJson`,
`OutboxRowMapper`, `CloudEventEncoder`) are out of scope — they never reach the
published jar.

1. **Every public class/interface gets a javadoc comment** — what it is and, where
   relevant, a pointer to the HLD/LLD section it implements (e.g.
   `(LLD-jdbc §3.3–§3.7)`).
2. **No javadoc on self-explanatory members.** Skip getters/setters/`toString`/
   `equals`/`hashCode` whose name already says everything, and any default-interface
   method whose behaviour is obvious from its name. Same rule as inline code comments:
   document the *why*, not the *what* — never restate the method name in prose.
3. **Public constructors are always documented**: what each parameter represents, and
   `@throws IllegalArgumentException`/`@throws NullPointerException` for any validated
   constraint (e.g. `bucketCount` must be positive). A constructor is the contract a
   consumer hits first — it never gets a free pass.
4. **Builder/fluent setters get one line only when the default or a constraint isn't
   obvious from the name** (e.g. `RelayConfig.Builder.rowLease` must be `>
   deliveryTimeoutMs`, with the default value stated). A setter whose name is fully
   self-describing (`aggregateType(String)`) stays undocumented — ten near-identical
   one-liners are noise, not signal.
5. **Public interface (port) methods always get `@param`/`@return`/`@throws`** —
   ports are the hexagonal architecture's contract surface, so their methods must be
   fully specified even when the interface-level javadoc already explains the port's
   purpose.
6. **Don't chase doclint to zero.** The Java 17 javadoc linter flags any method with a
   partial doc comment (missing `@return` on a fluent setter, for instance) as a
   warning. That's expected here — rules 2 and 4 deliberately favor terse, accurate
   javadoc over exhaustively tagged boilerplate. Do not add `@return this` or similar
   filler just to silence the linter.

## Testing

**Framework:** JUnit 6 + AssertJ. Run with `./gradlew test` (coverage report at
`build/reports/jacoco/test/jacocoTestReport.xml`).

1. **BDD method names**, literal form `GIVEN_..._WHEN_..._THEN_...` — the markers
   `GIVEN`/`WHEN`/`THEN` in uppercase, the descriptive parts in snake_case, e.g.
   `GIVEN_a_pending_message_WHEN_relay_publishes_THEN_status_is_done`.
   Long, non-idiomatic-for-Java names are fine; readability of the scenario wins.
   **Describe the scenario in the use case's business terms, never with Java method
   or class names.** A test name states the *behaviour* (`WHEN_the_relay_claims_work`,
   `WHEN_a_lease_expires`), not the API call that happens to implement it today
   (`WHEN_claimBatch_called`, `WHEN_reclaimExpiredLeases_called`). Names that embed a
   method or type are fragile: renaming the method or refactoring the seam forces a
   rename of every test that still passes, and the name no longer tells the reader
   *why* the behaviour matters. If you cannot phrase the scenario without naming a Java
   symbol, the test is probably pinned to the implementation rather than the behaviour.

2. **Classical / Detroit school — no mocks of any kind.** Use real domain objects
   and real collaborators:
   - **Unit tests:** use `InMemoryOutbox` from `tandem-test` as the real outbox
     collaborator — no database required.
   - **Integration tests:** use `TandemTestContainer` from `tandem-test`, which
     wires up a real PostgreSQL and a real Kafka broker via Testcontainers.
   Refactoring the production code under test to make it testable without mocks is
   explicitly allowed — extract a pure function, change visibility, split a class,
   introduce a seam. Prefer a behaviour-preserving refactor over reaching for a mock.

3. **Test behaviours, not coverage.** Aim for assertions that would fail under
   mutation: cover empty *and* populated inputs, single *and* multiple items,
   ordering, per-aggregate isolation, and failure/retry paths. 100% line coverage
   is not the goal — pinned behaviour is. If a surviving mutant has no possible
   killing input, that signals redundant/dead code to remove, not a test to add.
   **A useless test is a wrong test — don't write it, and delete it if it exists.**
   Useless means it tests the obvious, merely restates the implementation
   (tautology), or exists only to push the coverage number up.

4. **Avoid fragile hardcoded strings**, in three tiers:
   - **Format field names** (JSON/Kafka header names) → use typed constants or
     deserialize into typed records so each name is declared once.
   - **Pass-through values** where the output must equal an input → assert against
     the input object, never repeat the literal.
   - **Test-data strings reused as both input and lookup key** → extract a named
     constant so input and assertion cannot drift.
   Keep a literal only for a genuine *transform output* (e.g. topic routing:
   `router.topicFor(record(aggregateType="OrderLine")) == "order-line-topic"`) —
   deriving it via production code would be tautological.

### Integration tests and Docker

Integration tests in `tandem-test` require Docker (Testcontainers). They run
automatically in CI (GitHub Actions ubuntu-latest has Docker available). Locally,
Docker Desktop or Colima must be running. Integration tests are tagged
`@Tag("integration")` and run as part of `./gradlew check`; skip them with
`./gradlew test -x integrationTest` if Docker is unavailable.

## Commit messages

- Describe **what** changed and why, not **how** it was implemented.
- Do **not** add a `Co-Authored-By` trailer.
- **Before every commit, run the full test suite and make sure it is green**
  (`./gradlew test`, or `./gradlew check` to include the coverage gate). Never
  commit with failing or unrun tests.
- **Before every commit, check that the docs (`docs/`, `README.md`, `AGENTS.md`,
  …) are consistent with what is being committed** — update them in the same change
  if the code, conventions, structure, or commands they describe have moved. Treat
  stale docs as part of the diff, not a follow-up.

## Releases

Tags follow `v<semver>` and pushing one publishes all modules to Maven Central.
Before creating a release tag:

1. **Check for breaking changes** since the previous release tag — diff the
   public API surface (`tandem-core` interfaces and public types).
2. If there are breaking changes, **verify the requested version is bumped per
   semantic versioning** (a breaking change requires a major bump; in `0.x`, a
   minor bump conventionally signals it).
3. If the requested version does **not** match what semver requires, do not tag
   silently — ask the user to choose between:
   1. proceed with the version as given,
   2. use the semver-correct version you propose, or
   3. cancel.
