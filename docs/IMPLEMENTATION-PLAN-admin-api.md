# Tandem — Implementation Plan: Admin API (`tandem-admin`)

**Version:** 1.0
**Status:** Active
**Scope:** the Admin API module, shipped in slices. **Slice 1 (reads) is done — see §6.
Slice 2 (replay/discard) is done — see §8/§9.**
**Contract:** [admin-api.openapi.yaml](admin-api.openapi.yaml) — frozen and reviewed (API-first).
**Design:** [HLD-admin-api.md](HLD-admin-api.md).

The *design* is fixed in the HLD and the OpenAPI; this document only orders the work, fences
the scope, and defines done-ness per slice. Read it first, then the HLD. Follow
[AGENTS.md](../AGENTS.md) for every change.

---

## 1. Starting position — what exists and what does not

Established by reading the code, because the HLD's architecture section overstated the reuse:
it said the admin use cases "delegate to existing core ports (`OutboxRepository`,
`ReplayService`) […] they introduce no new persistence path". That is true for replay and
false for every read.

| Capability the contract needs | State today |
|---|---|
| Write outbox rows | `OutboxRepository` — **insert-only** (`insert`, `insertAll`). No reads at all. |
| Relay-side row access | `OutboxStore` — relay-shaped: `claimBatch` is bucket-scoped and *mutates* rows to `IN_FLIGHT`; plus `markDone`/`markForRetry`/`markFailed`, `reclaimExpiredLeases`, `cleanup`. |
| Backlog readings | `OutboxStore.lag()` → `LagSnapshot(pending, oldestSince)`, `failedCount()`, `blockedCount()`. **Reusable** for `OutboxSummary`'s lag fields. |
| Read one row by id / search by criteria / count per status | **Nothing.** No port exposes any of these. |
| Replay | `ReplayService` + `JdbcReplayService` + `ReplayCriteria`/`ReplayResult` — **reusable almost 1:1** for slice 2. |
| Discard (`FAILED` → `DISCARDED`) | **No code path anywhere.** `OutboxStatus.DISCARDED` is documented as "reachable only via the Admin API". Genuinely new verb. |
| Attempt archive | **Ghost port.** `AttemptRecorder` is an interface plus `NOOP` with `isEnabled()` returning `false`; no JDBC adapter, no `tandem_outbox_attempt` table in the baseline DDL, no relay call site. `AttemptOutcome`/`AttemptStatus` have zero product references. |
| Relay control state | `tandem_relay_control` **does not exist** in the baseline schema. |
| In-process relay status | `WorkerPool.status()` → `RelayStatus` (`tandem-jdbc`). Database-free, so it answers only for *this* JVM — the contract's `/relay/*` endpoints must work standalone, which needs the DB-mediated path (HLD-admin-api §4.1). Note the name collides with the OpenAPI `RelayStatus` schema; the two are different shapes. |

**Consequence for slice 1: it does introduce a new persistence path** — a read port plus its
JDBC adapter. That is the substance of the slice, not incidental plumbing. [HLD-admin-api.md](HLD-admin-api.md)
§4 has been corrected accordingly.

### 1.1 Attempt archive — deferred, and excluded from every slice

**User decision, 2026-08-02: the attempt archive is future, low-priority work. Until it is
explicitly requested, it is not to be considered in the scope of any other feature.**

The two attempt `operationId`s — `getMessageAttempts`, `searchAttempts` — are therefore **not
implemented in any slice below**, and no other slice may grow scope to accommodate them.

They stay in the published contract (removing them would be a breaking contract change, §1 of
the HLD). The gap is deliberate and must be **visible rather than silent**: the Specmatic
conformance run keeps an explicit allowlist of not-yet-implemented `operationId`s, which
shrinks as slices land. An unimplemented operation that is *not* on that list fails the build.

---

## 2. Slices

| Slice | Content | State |
|---|---|---|
| **1** | **Reads** — `getOutboxSummary`, `searchOutboxMessages`, `getOutboxMessage`. No state change; the safe first cut that answers "what is stuck and why". | **Done** |
| 2 | **Replay + discard** — `replayMessage`, `replayBulk`, `discardMessage`. The transition that unblocks an aggregate; closes the product's biggest hole. Fold in `JdbcReplayService`'s 4 uncovered `ReplayCriteria` branches. | **Done** |
| 3 | **Relay control + observability** — `getRelayStatus`, `pauseRelay`, `resumeRelay`, `getRelayBuckets`, `releaseBucket`, `getRelayWorkers`. Needs the `tandem_relay_control` DDL (additive) and the relay honouring it once per poll cycle, plus the `paused` flag on `BucketStatus`. | Later |
| 4 | Runtime metrics knobs (Q30): change `metricsInterval` during an incident; on-demand lag reading. | Later |
| — | Attempt endpoints | **Deferred (§1.1)** |

Slice order is by operator value and by risk: reads cannot corrupt anything, so they land
first and prove the module's plumbing (autoconfiguration, base path, problem+json, contract
tests) before any endpoint mutates delivery state.

---

## 3. Slice 1 — reads

### 3.1 In scope

Three `operationId`s, exactly as the contract specifies them:

- **`getOutboxSummary`** — `GET /outbox/summary` → `OutboxSummary`: row count per status, plus
  `lagCount` and `lagAgeSeconds`.
- **`searchOutboxMessages`** — `GET /outbox/messages` → `OutboxMessagePage`: cursor-paginated,
  filters `status` / `aggregateId` / `aggregateType` / `type` / `createdFrom` / `createdTo`,
  `limit` 1–500 (default 50). **Payload omitted** in the list view.
- **`getOutboxMessage`** — `GET /outbox/messages/{id}` → `OutboxMessage` with payload and
  headers; `404` (problem+json) when absent.

### 3.2 Out of scope — stop and flag if a task seems to need it

Replay, discard, any `/relay/*` endpoint, the `tandem_relay_control` table, **anything touching
the attempt archive** (§1.1), MySQL, and the CLI (a later backlog item, a frontend over these
same endpoints — never a second control path).

**Slice 1 does not touch the schema.** If a task appears to need a DDL change, that is the
signal to stop: the read path is served entirely by the existing `tandem_outbox` columns.

### 3.3 New in `tandem-core`

Zero-dependency, as always.

**The read model is its own model — it does not reuse the write/relay model** (decision,
2026-08-02). `OutboxMessage`/`OutboxRecord` describe a row *being written and delivered*:
`payload` is mandatory there because a message without one cannot be published. The read side
has different needs — the list view must not carry a payload at all — and welding the two
makes both rigid: every read-side requirement would push a change into the type the write path
and the relay depend on. Keeping them independent is also what dissolves the payload problem
this plan previously listed as an open decision (§3.9).

- **`OutboxQuery`** (port) — the read side the admin needs:
  - `Map<OutboxStatus, Long> statusCounts()`
  - `List<OutboxRowView> search(OutboxSearchCriteria criteria)`
  - `Optional<OutboxRowDetail> findById(long id)`
- **`OutboxRowView`** (record) — the list row: `id`, `aggregateId`, `aggregateType`, `type`,
  `seq`, `status`, `attempts`, `lastError`, `nextAttemptAt`, `lockedBy`, `lockedUntil`,
  `createdAt`. **No `payload`, no `headers`** — the columns the list view must not read.
- **`OutboxRowDetail`** (record) — `OutboxRowView` plus `payload` and `headers`, by composition
  rather than by repeating twelve fields. Returned only by `findById`.
  Each query returns exactly what it reads: no "present only sometimes" field, so neither type
  can lie about itself.
- **`OutboxSearchCriteria`** (record) — `status`, `aggregateId`, `aggregateType`, `type`,
  `createdFrom`, `createdTo`, `limit`, `afterId` (the cursor).
  **Unlike `ReplayCriteria`, a selector-less search is legitimate** — it is bounded by `limit`,
  and "show me the newest rows" is the first thing an operator asks. Validate `limit` against
  the contract's 1–500 range instead.

Both row types are reachable from a log statement, so per AGENTS logging §5 each gets a
`toString()` that prints `payloadBytes=<length>`/`headerNames=<keySet()>` — never the values —
and a unit test asserting a fake-sensitive payload and header value do **not** appear in the
rendered string.

**Port placement — decided 2026-08-02: `tandem-core`, not `tandem-admin`.** Both types widen
core's permanently-frozen surface, deliberately: it is what lets the JDBC adapter and
`InMemoryOutbox` share one contract, which the no-mocks unit tests depend on. It is also
consistent with `ReplayService` — already an operator-facing operation, already a core port
with a `tandem-jdbc` adapter. The alternatives were an admin-local port (rejected: forces
duplicating the package-private `OutboxRowMapper` and puts `InMemoryOutbox` out of reach as a
test collaborator) and adding the methods to `OutboxStore` (rejected: mixes non-bucket-scoped
admin queries into the relay's claim/mark/cleanup contract that every adapter must implement).

### 3.4 New in `tandem-jdbc`

- **`JdbcOutboxQuery implements OutboxQuery`** — same package as `OutboxRowMapper`. It needs
  its **own** mapping to the read types; `OutboxRowMapper` maps to `OutboxRecord` (the write/relay
  model) and stays package-private and untouched.
  - **Named columns only, never `SELECT *`**, tolerating extra columns (AGENTS, HLD §1.4).
  - The list query selects the `OutboxRowView` columns and **genuinely does not select
    `payload`/`headers`** — the point of the separate read type is that those JSONB columns are
    never read for a list page, not merely dropped afterwards.
  - Search: dynamic `WHERE` over the six filters + `id > :afterId ORDER BY id LIMIT :n`.
  - `statusCounts()`: `SELECT status, count(*) … GROUP BY status`. Absent statuses must appear
    as `0`, not be missing from the map — the contract's `counts` object names all five.
  - **No new index.** Verify the filter combinations against the existing indexes and record
    what a large-outbox search actually costs; if a filter needs an index the schema does not
    have, that is a §3.2 stop-and-flag, not a silent DDL addition.

### 3.5 New in `tandem-test`

- **`InMemoryOutbox` also implements `OutboxQuery`**, which is what lets the admin use cases be
  unit-tested against a real collaborator with no database (AGENTS: no mocks).
  It already holds everything the port needs and has `byId()`, `byStatus()`, `statusCounts()`
  and `all()` as test helpers — but those return `OutboxRecord`, the write model, so the port
  methods are new code projecting its rows onto the read types, not a rename of the helpers.
  `statusCounts()` is the one that carries over directly, which closes backlog item 17 (it and
  `occupiedBuckets()` are flagged there as zero-user, zero-coverage methods in a *published*
  module).
- Its bucket/status semantics must match the JDBC adapter's, the same way it already mirrors
  `BucketHash` — a helper that disagrees with the real store makes every unit test above it a
  lie.

### 3.6 New module — `tandem-admin`

Published, optional, off by default.

- **Dependencies:** `api(project(":tandem-jdbc"))`; Spring `compileOnly` on the Boot 3.x
  baseline, exactly like `tandem-spring-relay` — no Spring version propagates to consumers, and
  Jackson arrives with the application's Boot rather than being redistributed. Contract-test
  tooling (swagger-request-validator, Specmatic) is **test-only** and never shipped.
- **⚠️ The dual-generation gate applies.** `tandem-admin` is a Spring module, so it needs its own
  **`bootFourTest`** task wired into `check` (LLD-spring-config §1.2), and both rules that keep
  one artifact valid on Boot 3.x and 4.x: order autoconfigurations **by name** (`afterName`),
  never by class literal, and put class conditions on **`@Bean` methods**, never on a nested
  `@Configuration`. Neither fails loudly when broken. **The pre-commit gate is `./gradlew check`,
  not `./gradlew test`** — a green `test` alone can hide a 4.x regression until CI.
- **Enablement:** autoconfiguration gated on `tandem.admin.enabled` (default `false`) — when
  off, no controller bean and no cost.
- **Base path:** configurable (default `/tandem/admin`) + the fixed `/v1` segment.
- **Errors:** one `@RestControllerAdvice` rendering RFC 9457 `application/problem+json`, with
  `type` always the canonical `https://tandem.codingful.com/problems/{slug}` URL. Slice 1 needs
  `not-found` and `internal-error`; `unauthorized` is returned once the host wires authentication
  (Tandem ships endpoints, not authentication — HLD §3).
- **Structure:** framework-agnostic use cases (the admin's own core) with the REST controllers
  as a driving adapter over them, per HLD §4. One module — not a core/adapter module split,
  which would be ceremony without a swappable boundary (Pareto).
- **The wire model belongs to `tandem-admin`, never to `tandem-core`** (decision, 2026-08-02).
  The controllers own DTOs mapped from the read model; **no core type is ever serialized onto
  the wire.** The OpenAPI document and `tandem-core`'s Java surface are two independently
  published, independently frozen contracts, with different consumers (REST clients that Tandem
  never sees vs. adopters compiling against the library) and different evolution rules. Welding
  them means an additive change on one side forces a change on the other, and it drags
  serialization concerns — annotations, naming, JSON shape — into a module whose defining
  property is having no dependencies.
  Two consequences to keep in mind while implementing:
  - **The contract's vocabulary is its own.** The API schema is `OutboxEntry` /
    `OutboxEntryPage` — renamed from `OutboxMessage` on 2026-08-02 precisely so it cannot be
    mistaken for the core's write model. Name the DTOs after the *contract*, and do not
    reintroduce a core name on the wire side.
  - **`payload` crosses the boundary as JSON, not as bytes.** The column is `JSONB`, the
    contract types the field as object-or-string, and the core read model holds `byte[]`. The
    mapping to the wire happens in the DTO layer — the one place allowed to know how the API
    renders a payload.

### 3.7 Registration checklist — all six lists, in the same change

Each omission fails **silently** (AGENTS, "Adding a module"):

`settings.gradle.kts` · `tandem-bom` · `tandem-coverage`'s `coveredProjects` ·
`README.md` module table · `CONTRIBUTING.md` project layout · `docs/LLD-base.md` (artifactId +
package) · `THIRD-PARTY-NOTICES.md` per-module table (state "none beyond …" — Spring and the
contract-test tooling are `compileOnly`/test-only and are not redistributed).

`tandem-admin` is **published**, so it does *not* go in `unpublishedModules`.

### 3.8 Tests

- **Unit** — the use cases against `InMemoryOutbox` as the real `OutboxQuery` (no mocks).
  Behaviours worth pinning, all of which would survive a coverage-only test: empty *and*
  populated outbox; `statusCounts` reporting `0` for absent statuses; cursor paging across a
  boundary with rows inserted between pages; `limit` at both ends of 1–500; every filter alone
  and in combination; a search matching nothing.
- **Integration** — `TandemTestContainer` (real PostgreSQL), each endpoint test doubling as a
  conformance test via swagger-request-validator against the OpenAPI.
- **Contract** — Specmatic for generative conformance and the spec backward-compatibility gate,
  with the §1.1 allowlist for the deferred attempt operations.
- **Security of the response body** — assert the list view does not carry `payload`, and that no
  log line or `toString()` on the new types prints payload or header *values* (AGENTS logging §5).
- **The model boundaries hold** — a test that fails if a core type is serialized onto the wire.
  This is the kind of separation that erodes silently under a later "just return the record
  here", and nothing else in the build would notice.

### 3.9 Resolved — three models, deliberately independent

This section previously held an open decision: `OutboxMessage.payload` is
`Objects.requireNonNull`, so an `OutboxRecord` cannot exist without a payload, yet
`searchOutboxMessages` must not return one. **The question dissolved once the models were
separated** (user decision, 2026-08-02) — it was only ever a symptom of reusing one model for
three jobs.

| Model | Owner | Job |
|---|---|---|
| `OutboxMessage` / `OutboxRecord` | `tandem-core` | The row being **written and delivered**. `payload` mandatory — a message without one cannot be published. |
| `OutboxRowView` / `OutboxRowDetail` | `tandem-core` | The row being **read**. Two shapes because the two queries genuinely read different columns. |
| The REST DTOs | `tandem-admin` | The row **on the wire**, evolving with the OpenAPI. |

Each boundary earns its keep: read/write independence means a read-side need never pushes a
change into the type the relay depends on, and API/core independence keeps two separately
frozen contracts from forcing changes on each other. What this deliberately is **not** is a
mapping layer per architectural reflex — a fourth model, or ports-and-adapters ceremony around
a boundary that does not swap, would be exactly the complexity Pareto (§1.1) rejects.

---

## 4. Cross-cutting reminders (from AGENTS.md / the HLD)

- **API-first.** Every API change starts in the OpenAPI, is reviewed, and *then* implemented.
  Never back-derive the spec from the code. The implementation conforms to the committed
  contract — including its error `type` slugs, which never change once published.
- **Tolerant readers, open schemas.** No `additionalProperties: false`, no validation layer that
  rejects unknown fields — it breaks consumers the moment the contract grows additively.
- **No mocks.** `InMemoryOutbox` for unit, `TandemTestContainer` for integration; refactor for
  testability rather than reaching for a mock.
- **BDD test names** in business terms — `GIVEN_an_outbox_with_a_failed_row_WHEN_the_operator_
  searches_by_status_THEN_only_that_row_is_returned`, never naming a Java method or type.
- **Logging** — `tandem-admin` is relay/ops-side, but it sits in the same repo conventions:
  fixed message text then a flat `name:value` tail, capital first letter, level by frequency,
  the `Throwable` always passed, and **never** payload/header values or credentials.
- **Javadoc** — `tandem-admin` is published, so its public types are documented, and the new
  `OutboxQuery` port methods get full `@param`/`@return`/`@throws` (ports are the contract
  surface, AGENTS Javadoc §5).
- **Minimal client footprint** — nothing added here may reach the client write-side. The new
  core types are interfaces and records with no dependencies; Spring stays `compileOnly` in
  `tandem-admin`.
- **Checkpoint at the end of the slice** for a spec-adherence review before starting slice 2.

---

## 5. Definition of done — slice 1

- The three `operationId`s implemented and conforming to the OpenAPI, verified by
  `openapi-request-validator-core` against every test's real MockMvc request/response.
- `./gradlew check` green — **including `:tandem-admin:bootFourTest`** and the Docker-bound
  integration tests.
- Coverage of the new code reviewed line-by-line, not by percentage: genuine gaps closed,
  acceptable ones (defensive guards, unkillable branches) named as such.
- `tandem-admin` registered in all six lists (§3.7).
- The module is genuinely off by default: with `tandem.admin.enabled` unset, no controller bean
  exists and no endpoint answers.
- Docs consistent — HLD-admin-api, README, CONTRIBUTING, LLD-base, THIRD-PARTY-NOTICES.
- The three models stayed independent (§3.9): no core type on the wire, no payload requirement
  leaking into the read path.
- No out-of-scope decision silently invented: any need for a schema change or an attempt-archive
  touch surfaced rather than absorbed.

---

## 6. Landed — slice 1 (2026-08-02)

All three read operations implemented and verified — unit tests against `InMemoryOutbox`, MockMvc
tests exercising real Spring MVC dispatch, an end-to-end `TandemTestContainer` test writing through
the real `JdbcOutboxRepository` and reading back through the real `JdbcOutboxQuery`/`JdbcOutboxStore`
and REST layer, and every one of those HTTP tests validated against the committed OpenAPI document.
`./gradlew check` is green project-wide, including `:tandem-admin:bootFourTest`.

**Contract-validation tooling ended up different from the plan, for reasons worth recording:**

- **Specmatic was not wired.** Given the scope already delivered (real request/response conformance
  on every test), it is separate scope from this slice, not a rejection — tracked as a backlog item
  (item 1.5) rather than "deferred under time pressure" as an earlier draft of this note put it:
  there was never an actual deadline, just an already-long session that had delivered enough for one
  sitting. **Evaluate both Specmatic (JVM-native, fits the Java/CI stack without a Python toolchain)
  and Schemathesis (Python, property-based) before committing to either** — a focused follow-up for
  when generative conformance and the spec backward-compatibility gate are actually needed (e.g.
  before a `/v2`, or once slice 3 adds more surface).
- **swagger-request-validator's `-mockmvc` module cannot be used at all on this project's Spring
  generation.** Verified against its published POM: even its latest release line targets
  `javax.servlet` and Spring Framework 5.3 — it has never been ported to Jakarta/Spring 6. Using it
  would mean downgrading the whole test's servlet types, which is not on the table.
- **The library was also renamed and relocated mid-2026** (`com.atlassian.oai:swagger-request-validator-*`
  → `com.atlassian.oai:openapi-request-validator-*`), and the new `3.0.0` line under the new name
  compiles to **Java 21 class files** (major version 65) — incompatible with this project's Java 17
  toolchain (major version 61), which applies to every module including test compilation. The fix:
  depend on `openapi-request-validator-core` **under the pre-rename artifact id**
  (`com.atlassian.oai:swagger-request-validator-core`) at its **last release before the rename**
  (`2.46.1`), which still targets Java 8 bytecode and is otherwise the identical library (same
  packages, same classes) one release earlier. Only the core module is used — it has no servlet
  dependency at all — behind a small hand-written adapter (`OpenApiConformance`, test-only) mapping
  Spring's real Jakarta `MockHttpServletRequest`/`MockHttpServletResponse` onto the library's
  `SimpleRequest`/`SimpleResponse`. **Validates the response only** (`validateResponse`, not
  `validate`): several tests deliberately send an invalid parameter to exercise the 400 path, and
  what must conform there is Tandem's own error response, not the intentionally-bad request.

**Two genuine implementation bugs the conformance check caught, not code review:**

- **Jackson's default `Instant` serialization is a numeric timestamp**, violating the OpenAPI's
  `string`/`date-time` schema for `createdAt` etc. Fixed by disabling
  `SerializationFeature.WRITE_DATES_AS_TIMESTAMPS` — centralized in `TandemAdminObjectMappers`, the
  one place both the autoconfiguration's fallback bean and every test build their `ObjectMapper`, so
  production and tests can never drift apart on this. (A real Spring Boot application's own
  autoconfigured `ObjectMapper` already disables this by default; only the fallback bean and raw
  test-constructed mappers were at risk.)
- **An explicit JSON `null` fails an OpenAPI 3.0 `oneOf` schema even when the field is `nullable`** —
  a known OpenAPI 3.0 limitation (fixed by `type: [..., 'null']` unions in 3.1, not available here).
  `OutboxEntry.payload`'s schema is `oneOf: [object, string]`, so a list-view row serializing
  `"payload": null` failed conformance outright. Fixed with `@JsonInclude(NON_NULL)` on
  `OutboxEntryResponse` — which also matches the contract's own prose ("omitted in list view", not
  "null in list view") better than the original implementation did.

**One deliberate, additive contract change, made because the conformance check surfaced a real
gap:** `searchOutboxMessages` and `getOutboxMessage` did not declare a `400` response, yet both
operations can genuinely return one (an unparseable `status`/`cursor` query value, or a non-numeric
`{id}`). Added a shared `InvalidParameter` response (slug `invalid-parameter`) referenced from both
— additive, so not a breaking change under the compatibility rules (§1). `HLD-admin-api.md`'s slug
list updated to match. `TandemAdminExceptionHandler` also now catches
`MethodArgumentTypeMismatchException` (Spring's own path-variable conversion failure), which
previously fell through to the generic 500 handler.

**A third bug the conformance check caught, one commit later:** `ProblemDetail.forStatus(status)`
(used whenever the internal-error handler had no detail message to give) serializes
`"detail": null`. Same OpenAPI 3.0 limitation as the payload bug above, but on a Spring framework
type this module doesn't own, so `@JsonInclude` isn't an option here. Fixed at the source instead:
`handleUnexpected` now always supplies a static, non-leaking detail string ("see server logs"), so
`problem()` always calls `forStatusAndDetail` and the null-detail code path — and the bug it could
produce — no longer exists.

**Coverage review (AGENTS' pre-commit rule), read line-by-line against the aggregated cross-module
report (`tandem-coverage:aggregatedCoverageReport` — the per-module `jacocoTestReport`/
`jacocoMergedReport` outputs undercount anything a *different* module's tests exercise, e.g.
`OutboxRowDetail`'s convenience delegates, which only `tandem-admin`'s tests call). Findings:**
- **Two genuine dead-code removals**, not gaps to fill: `OutboxMessageNotFoundException.id()` had
  zero callers (the handler uses `getMessage()`); and `OutboxAdminService.search()`'s cursor
  condition carried a redundant `&& !rows.isEmpty()` — since `criteria.limit()` is validated `>= 1`,
  `rows.size() == criteria.limit()` already implies `rows` is non-empty, so the second clause could
  never change the outcome. Both removed rather than tested around (Testing §3: a mutant with no
  killing input means dead code, not a missing test).
- **Five genuine gaps closed** with new tests: the generic 500 handler was never exercised end to
  end (added a throwing `OutboxQuery` test double and a MockMvc test asserting the problem+json
  shape); a non-numeric `{id}` path segment was untested (the exact case
  `MethodArgumentTypeMismatchException` handling exists for); a *valid* cursor was never round-tripped
  through the controller (every prior cursor test was the invalid-input path); the JDBC `type` filter
  had no integration-test coverage though every other filter did; and `OutboxRowDetail.equals()`'s
  reflexive/wrong-type/headers-differ branches were never hit (standard equals-contract gaps).
  `OutboxAdminService`'s `OutboxStore.lag()`-returns-empty fallback also got a real collaborator test
  (a minimal hand-written `OutboxStore` overriding nothing but the default `lag()` — a real
  implementation, not a mock).
- **Accepted, left uncovered, matching existing precedent:** every `catch (SQLException e)` in
  `JdbcOutboxQuery` — checked against `JdbcReplayService`'s own identical, already-accepted gaps in
  the pre-existing codebase — and the null-payload defensive branch in `findById`, unreachable given
  `tandem_outbox.payload JSONB NOT NULL` (same reasoning `OutboxRowMapper` already relies on).

**Process note:** the aggregated coverage report and the project-wide `./gradlew check` both hit
transient Testcontainers/Docker resource contention repeatedly during this session, purely from
running many container-based integration test suites back to back — never a real regression
(confirmed each time by re-running the flagged task alone, which passed). Isolate before concluding
a genuine failure when several unrelated modules' container tests fail in the same broad run.

---

## 7. Package structure (2026-08-02) — split by feature ahead of slice 2

A single flat package stopped being enough once slice 2 (replay/discard) and slice 3 (relay
control) came into view — three feature areas' controllers, DTOs, and error mappings in one
package would already be cluttered with just the first one built. Restructured:

- **`com.codingful.tandem.admin`** (root) — cross-cutting infrastructure only: the
  `TandemAdminAutoConfiguration` entry point; the DB-derived adapter beans (`OutboxQuery`,
  `OutboxStore`) a future feature package may also need (kept here rather than inside `outbox`,
  since relay control's own endpoints may need `OutboxStore` too); the shared `ProblemDetails`
  RFC 9457 builder; and `TandemAdminExceptionHandler`, the generic 400/500 mapping every feature
  composes alongside its own.
- **`com.codingful.tandem.admin.outbox`** — slice 1 (reads) today; replay/discard (slice 2) joins
  it, since both act on the same resource. `OutboxAdminConfiguration` is the package's one public
  type (the wiring entry point the root `@Import`s); everything else — the use case, the
  controller, the DTOs, `OutboxExceptionHandler` — is package-private. Nothing outside this
  package had a genuine reason to reference them, so the earlier, more permissive visibility
  (`OutboxAdminService` and the three DTOs were `public`) was tightened at the same time — pure
  surface-area cleanup, not a behavior change.
- **A future `com.codingful.tandem.admin.relay` package** for slice 3, following the same shape.
  Not created yet — an empty package ahead of the slice that needs it would be exactly the
  premature structure Pareto (§1.1) warns against.

**Two real bugs found while making this change, not invented ahead of it — both explained by the
same root cause:**

- **Registering one multi-interface object under two different bean types is ambiguous to Spring's
  by-type autowiring.** Attempting `withBean(OutboxQuery.class, () -> outbox)` and
  `withBean(OutboxStore.class, () -> outbox)` for the *same* `InMemoryOutbox` instance (it
  implements both) made `OutboxQuery` injection ambiguous — Spring's type-matching inspects the
  produced object's actual class, not just the type token the bean was registered under. Fixed by
  using two single-purpose stub types instead (`PlainOutboxQuery`/`PlainOutboxStore` in
  `OutboxAdminConfigurationTest`, mirroring `OutboxAdminServiceTest`'s pre-existing
  `NoLagOutboxStore`) — using two *separate* `InMemoryOutbox` instances would **not** have fixed
  it, since the ambiguity is about interface overlap between bean definitions, not object identity.
- **`ExceptionHandlerExceptionResolver` does not rank `@ExceptionHandler` methods by exception-type
  specificity *across* different `@ControllerAdvice` beans — only within one bean's own methods.**
  Across beans it tries each advice bean in `@Order` sequence and uses the first one with *any*
  applicable method, even a less specific one. Splitting the single, all-cases
  `TandemAdminExceptionHandler` into a generic root handler plus `outbox`'s own
  `OutboxExceptionHandler` (404) silently broke the 404: the generic handler's
  `@ExceptionHandler(Exception.class)` matched first and shadowed the more specific one. This is
  **not test-detectable by every kind of test** — `OutboxAdminControllerTest`/`OutboxAdminIT`
  (`MockMvcBuilders.standaloneSetup(...)`) kept passing throughout, because standalone MockMvc does
  not even honour `@Order` for manually-supplied advice instances (registration order decides,
  full stop); the bug was only caught by manually curling the live `tandem-sample-spring` demo.
  Fixed with two changes, both required: `@Order(Ordered.LOWEST_PRECEDENCE)` on
  `TandemAdminExceptionHandler` (so a real Spring context always tries feature-specific advice
  first) **and** `@Order(0)` on `OutboxExceptionHandler` (an advice bean with no `@Order` also
  defaults to `LOWEST_PRECEDENCE` — tied with the generic one, and ties resolve by bean
  *registration* order, which favoured the generic advice since it is `@Import`ed first). Every
  future feature-specific advice needs the same explicit, higher-precedence `@Order`.
  Regression-tested by `TandemAdminEndToEndTest` (root test package) — the one test in this module
  using a **real** `WebApplicationContext`/`@AutoConfigureMockMvc` instead of standalone setup,
  specifically because standalone setup cannot detect this class of bug. Verified the test is
  genuine by reverting the `@Order(0)` fix and confirming it fails. Excluded from `bootFourTest`
  (tagged `boot3-only`): on the Boot 4.x line, `@AutoConfigureMockMvc` did not contribute a
  `MockMvc` bean under this setup — a `spring-boot-test-autoconfigure` discrepancy, not a
  `tandem-admin` production-code compatibility gap (the actual autoconfiguration wiring is already
  verified on both generations by `TandemAdminAutoConfigurationTest`/`OutboxAdminConfigurationTest`).

**Takeaway for slice 2/3:** every new feature-specific `@RestControllerAdvice` needs an explicit
`@Order` lower than `Ordered.LOWEST_PRECEDENCE` (e.g. `@Order(0)`), and any test asserting on error
dispatch *across* advice beans should prefer a real `@SpringBootTest` +
`@AutoConfigureMockMvc` context over `MockMvcBuilders.standaloneSetup(...)` — the latter's
exception-resolution behavior provably diverges from production the moment more than one advice
bean is involved.

---

## 8. Slice 2 — replay + discard

### 8.1 In scope

Three `operationId`s, exactly as the contract specifies them:

- **`replayMessage`** — `POST /outbox/messages/{id}/replay`: resets one `DONE`/`FAILED` row to
  `PENDING`. `404` if absent, `409 message-not-replayable` if present but neither `DONE` nor
  `FAILED`.
- **`discardMessage`** — `POST /outbox/messages/{id}/discard`: marks one `FAILED` row
  `DISCARDED`, recording the operator's reason. Requires `acknowledgeOrderingBreak: true`
  (`400 ordering-break-not-acknowledged` otherwise). `404` if absent, `409
  message-not-discardable` if present but not `FAILED`.
- **`replayBulk`** — `POST /outbox/replay`: replays every `DONE`/`FAILED` row matching a
  criteria (aggregate/id-range/status), with `dryRun` to preview the count. `400
  replay-no-selector` if no selector is given.

### 8.2 Out of scope — stop and flag if a task seems to need it

Same boundary as slice 1: any `/relay/*` endpoint, `tandem_relay_control`, the attempt archive
(§1.1), MySQL, and the CLI. **Bulk discard is also out of scope** — the contract has no
`discardBulk` operation, only `discardMessage` (single id); do not invent one.

### 8.3 Decided today (2026-08-02) — see HLD-admin-api §6.1 for the full rationale

- **`409 message-not-discardable`** for `discardMessage` on a non-`FAILED` row — additive
  response, same pattern as `replayMessage`'s existing `409 message-not-replayable`.
- **New, additive `discard_reason` column** on `tandem_outbox` — keeps `DiscardRequest.reason`
  actually persisted ("recorded for audit") without overwriting `last_error`, which stays the
  original delivery-failure text.
- **`DiscardService` — a new core port, not a method on `OutboxStore`.** Same reasoning §3.3
  already used to keep `OutboxQuery` off `OutboxStore`: discard is admin-only, never called by
  the relay, so it does not belong in the relay's claim/mark/cleanup contract every `OutboxStore`
  adapter must implement. It follows `ReplayService`'s existing shape instead — a small,
  dedicated port with a `tandem-jdbc` adapter and an `InMemoryOutbox` implementation for
  no-mocks unit tests.

### 8.4 New in `tandem-core`

- **`DiscardService`** (port) — `boolean discard(long id, String reason)`. Returns `false` when
  no row exists with that id **or** the row exists but is not `FAILED`; the caller (the admin
  service) disambiguates the two via `OutboxQuery.findById` only on the failure path, so the
  success path stays a single statement, same division of responsibility slice 1 established
  between the read port and the mutating one.
- **`OutboxRecord`** gains a nullable `discardReason` field (+ builder method) — mirrors how
  `lastError` already lives there even though only the relay's `markFailed`/`markForRetry` set
  it; `discardReason` is set only by `DiscardService`, never by the relay.
- **`OutboxRowView`** gains `discardReason` (after `lastError`) — read side mirrors the new
  column, same as every other `tandem_outbox` field it already projects.
- **`OutboxRowDetail`** gains the matching delegate accessor. Not sensitive (an operator-entered
  admin reason, not payload/header data), so no redaction needed in its existing `toString()`.
- **`ReplayService`/`ReplayCriteria`/`ReplayResult` are reused as-is — no core changes.**
  `replayMessage` is `ReplayService.replay(criteria)` with `fromId = toId = id` (no status
  filter needed: `JdbcReplayService` already restricts to `{DONE, FAILED}`), which also
  satisfies `ReplayCriteria`'s "at least one selector" invariant for free. `replayBulk` maps its
  request fields onto `ReplayCriteria` directly.

### 8.5 New in `tandem-jdbc`

- **`JdbcDiscardService implements DiscardService`** — same package as `JdbcReplayService`:
  `UPDATE tandem_outbox SET status = 4, discard_reason = ? WHERE id = ? AND status = 3`,
  returning whether a row was actually updated. A single conditional `UPDATE` avoids a
  read-then-write race between checking the precondition and applying it.
- **`JdbcOutboxQuery`** — add `discard_reason` to `VIEW_COLUMNS` and `mapView`.

### 8.6 New in `tandem-test`

- **`InMemoryOutbox` also implements `ReplayService` and `DiscardService`** — required for the
  admin use cases to stay unit-testable without a database (AGENTS: no mocks). `replay` mirrors
  `JdbcReplayService`'s filter/status logic against the in-memory rows; `discard` mirrors
  `JdbcDiscardService`'s conditional transition. `toRowView`/`toRowDetail` map the new
  `discardReason` field through.

### 8.7 New/changed in `tandem-admin.outbox`

- **`OutboxAdminConfiguration`** — two new `@Bean`s, `JdbcReplayService`/`JdbcDiscardService`,
  DataSource-derived like the existing `OutboxQuery`/`OutboxStore` beans (feature-local, not
  root, since no other feature package needs them — same placement rule as §3.6/§7).
- **`OutboxAdminService`** — `replayMessage(id)`, `discardMessage(id, request)`,
  `replayBulk(request)`, alongside the existing read methods.
- **New exceptions**, each mapped in `OutboxExceptionHandler` (which needs the same `@Order(0)`
  every feature-specific advice requires, §7): `MessageNotReplayableException` (409),
  `MessageNotDiscardableException` (409), `OrderingBreakNotAcknowledgedException` (400),
  `ReplayNoSelectorException` (400, thrown by the service before constructing `ReplayCriteria`
  when the request carries no selector at all — kept distinct from the generic
  `invalid-parameter` 400 so the slug matches the contract).
- **New DTOs**, wire-owned per §3.6/§3.9 (never the core `ReplayCriteria`/`ReplayResult` on the
  wire): a replay-bulk request record, a `ReplayResultResponse` (matched/replayed/dryRun), and a
  discard-request record (acknowledgeOrderingBreak/reason). `OutboxEntryResponse` gains
  `discardReason`.
- **`OutboxAdminController`** — three new `@PostMapping` endpoints for the three operations.

### 8.8 Tests

Same shape as slice 1 (§3.8): unit tests for the service against `InMemoryOutbox` (both
success and every error branch — not-found, wrong-state, missing-ack, no-selector, dry-run
matched-but-not-replayed), MockMvc controller tests double as conformance tests via
`OpenApiConformance`, and a `TandemTestContainer` IT exercising replay/discard against real
PostgreSQL end to end (write → fail → discard → verify `discard_reason` persisted and
`last_error` untouched; write → fail → replay → verify reset to `PENDING`).

### 8.9 Definition of done — slice 2

Same bar as §5, plus: `discard_reason` never overwrites `last_error`; a discard/replay attempt
on a non-existent id is `404`, never `409`; bulk discard is not implemented (§8.2); every new
exception's problem `type` slug matches the contract exactly.

---

## 9. Landed — slice 2 (2026-08-02)

All three operations implemented and verified against the same layered test strategy as slice 1:
unit tests against `InMemoryOutbox` (including its new `ReplayService`/`DiscardService`
implementations), MockMvc tests over real Spring MVC dispatch, a `TandemTestContainer` IT writing
through the real repository and acting through the real `JdbcReplayService`/`JdbcDiscardService`,
and every HTTP test validated against the committed OpenAPI. `./gradlew check` is green
project-wide, including `:tandem-admin:bootFourTest`.

**Two contract decisions resolved before implementation (HLD-admin-api §6.1 has the full
rationale):**

- **`409 message-not-discardable`** added to `discardMessage` — the contract had no response at
  all for discarding a non-`FAILED` row, even though `DISCARDED` is documented (LLD-core §1.2) as
  reachable only from `FAILED`. Additive, mirrors `message-not-replayable`.
- **New, additive `discard_reason` column** on `tandem_outbox` — keeps `DiscardRequest.reason`
  actually persisted without overwriting `last_error`.

**Architecture decision, following the precedent slice 1 already set:** `DiscardService` is a new
core port (`tandem-core`) with a `JdbcDiscardService` adapter (`tandem-jdbc`) and an
`InMemoryOutbox` implementation — not a method on `OutboxStore` — for the same reason `OutboxQuery`
was kept off `OutboxStore` in slice 1: admin-only operations don't belong in the relay's
claim/mark/cleanup contract every `OutboxStore` adapter must implement. `ReplayService` needed no
core changes at all — `replayMessage` reuses it as-is with `fromId = toId = id`, and `replayBulk`
maps its request fields onto `ReplayCriteria` directly, exactly as planned in §8.4.

**A significant, previously-undocumented Boot 4/Spring 7 `MockMvc` incompatibility, found while
writing the discard/bulk-replay tests — bigger in scope than the `.param()` removal slice 1
found:** `MockHttpServletRequestBuilder`'s fluent setters (`content`, `contentType`, `header`, …)
moved from being declared directly on that class to a new generic supertype,
`AbstractMockHttpServletRequestBuilder<B>`, in Spring Framework 7. A test class compiled once
against the Boot 3.x baseline (this module's dual-generation gate, LLD-spring-config §1.2) invokes
these via `invokevirtual` against the Boot-3-shaped class; re-running that same compiled bytecode
against the Boot 4.x/Spring 7 classpath throws `NoSuchMethodError` at the *first* chained builder
call after the bare static factory (`post(uri)`), regardless of which method — verified by trying
three different methods (`contentType`, `header`) and getting the identical failure shape each
time, and by confirming every already-passing test (slice 1 and slice 2's body-less requests)
never chains anything beyond the factory call itself. Fixed the same way as the `.param()` case:
the six new tests that POST a JSON body are tagged `boot3-only` and excluded from `bootFourTest`,
documented in `OutboxAdminControllerTest`'s class javadoc. **Takeaway for slice 3:** any MockMvc
test needs a request body or additional headers is `boot3-only` by construction under this
compile-once/run-twice test architecture — only bare `get(uri)`/`post(uri)` calls and
`MockMvcResultMatchers`-side assertions (a different, unaffected class hierarchy) are safe to run
on both generations.

**Coverage review closed every genuine gap the aggregated report surfaced:**
- `OutboxAdminService.replayBulk`'s "at least one selector" `||` chain had three operands
  (`fromId`, `toId`, `statuses`) whose *own* true-branch was never the deciding factor, because an
  earlier operand being true always short-circuited past them in every existing test. Closed with
  dedicated id-range-only, `toId`-only, and statuses-only selector tests — the last one also pins
  the earlier design decision that a non-empty `statuses` alone is a valid selector, independent of
  aggregate/id-range.
- `InMemoryOutbox.matchesReplay`'s three exclusion branches (aggregate-id mismatch,
  aggregate-type mismatch, below-`fromId`) were only ever exercised via the *fallthrough* accept
  path, never as the deciding `false`. Closed with one two-aggregate test per filter.
- `JdbcReplayService`'s "no replayable status requested" early return (matched=0 when the
  selector's statuses don't intersect `{DONE, FAILED}`) — pre-existing, uncovered code that slice 2
  makes genuinely reachable for the first time (an operator can send `"statuses": ["PENDING"]` to
  `POST /outbox/replay`). Closed via `JdbcReplayServiceIT`, which also brings `ReplayCriteria` to
  full branch coverage — the "4 uncovered `ReplayCriteria` branches" this plan flagged as a slice-2
  goal (§2) are closed as a side effect.
- **Accepted, unchanged from slice 1's precedent:** every `catch (SQLException e)` (now including
  `JdbcDiscardService`'s), and `OutboxRowDetail.equals()`'s remaining combinatorial branch —
  neither touched by slice 2's changes.
