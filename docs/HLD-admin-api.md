# Tandem — Admin API (Design Note)

**Version:** 1.0  
**Status:** Draft  
**Companion to:** HLD §7.2 (Admin API)  
**Contract:** [admin-api.openapi.yaml](admin-api.openapi.yaml)

A REST operations layer over the outbox (the *sends*), plus operational control of the
relay. Built **API-first**: the OpenAPI document is the source of truth and is defined and
reviewed *before* the implementation.

---

## 1. API-first approach

The contract leads, the code follows:

1. **The OpenAPI document is authored and reviewed first** ([admin-api.openapi.yaml](admin-api.openapi.yaml)).
2. **The implementation conforms to it** — controllers/DTOs are generated from or validated
   against the spec; the spec is not back-derived from the code.
3. **Every API change starts in the spec** — modify the OpenAPI, review it, then implement.
   A breaking change to the contract is a breaking change to the library (semver).
4. **The contract is the shared artifact** — the future Admin Web UI (backlog) and any CLI
   or third-party tooling consume this same contract; nothing depends on implementation
   internals.

This approach is recorded as a project convention in [AGENTS.md](../AGENTS.md) and applies
to any future external API Tandem exposes, not only the Admin API.

**CI contract validation (to wire when `tandem-admin` is implemented).** The OpenAPI
document is validated in CI on every change — independent of any editor — so real spec
problems are caught at build time. **Specmatic consolidates this**: running it against
[admin-api.openapi.yaml](admin-api.openapi.yaml) fails on a malformed/unusable contract as
part of its conformance and backward-compatibility checks (§5), so a separate validation
step is largely redundant. A dedicated linter (`redocly lint`) remains *optional* for style
and governance rules (naming, descriptions, examples) that Specmatic does not enforce. See
§5 for the full testing strategy.

**Backward *and* forward compatibility.** Two distinct, equally important promises:

- **Backward compatibility (provider side).** Within a major version (`/v1`), changes are
  **additive only**: new optional fields, new endpoints/operations, never a removal, rename,
  type change, newly-required field, narrowed range, or changed error `type` slug. Breaking
  changes go to `/v2`. Specmatic's backward-compatibility check (§5) enforces this on every
  spec change.
- **Forward compatibility (consumer side).** Older consumers must keep working against a
  newer provider, so consumers must be **tolerant readers** — ignore unknown response fields
  and unknown enum values, never reject a payload for extra properties. Response schemas are
  deliberately **open** (no `additionalProperties: false`). **Over-strict JSON validation is a
  forward-compatibility hazard**: a client (or a validation layer) configured to reject
  unknown fields breaks the moment the provider adds an additive field — which is exactly why
  we do *not* lock request/response bodies to `additionalProperties: false`.

---

## 2. Operations covered

| Area | Operations |
|---|---|
| **Outbox — read** | health summary (counts per status, lag count/age); search messages (by status / aggregate / type / time / **correlation id**); get one message with full payload + headers |
| **Outbox — act** | replay one message; bulk replay by criteria (with `dryRun` preview); discard a FAILED message (explicit ordering-break acknowledgement required) |
| **Relay** | status (state, bucket coverage, worker count); pause / resume (whole relay or a single bucket); **per-bucket ownership + lag**, listed or one at a time (spot uncovered/hot buckets); **active workers**; **force-release a bucket** for reassignment (zombie owner recovery) |

Replay builds on the per-aggregate `ReplayService` (HLD §8).

**Search by correlation id is the incident-time entry point**, not just one filter among the
others. The correlation id typically originates *outside* the application (an inbound request
header, a consumed message) and is what an operator actually holds when an investigation starts —
from a log line, an alert, or a customer ticket — whereas the aggregate id the other filters are
built around is usually not known yet. Since production database access is normally forbidden (§1),
this API is the only place the question can be asked at all. It is backed by its own indexed column
rather than a scan over the `headers` JSONB (HLD-tracing §4.1), and it returns **many** rows —
one correlation id groups the whole business operation, typically across several aggregates — so it
is paginated like any other search and usually combined with `status`.

The Admin API exposes no attempt-level forensic history (a timeline of every delivery attempt
per message). It is **designed but not built**, and its two operations were removed from this
contract rather than published unimplemented — [HLD-attempt-archive.md](HLD-attempt-archive.md)
§8 holds the endpoint and schema definitions, so restoring them is an additive `/v1` change.

---

## 3. Opt-in and security (Pareto, §1.1)

- **Off by default.** The Admin API is an attack surface; it is not exposed unless explicitly
  enabled (`tandem.admin.enabled=true`). When off there is no endpoint, no controller bean,
  and no cost.
- **Isolated in embedded mode.** Every `@RestControllerAdvice` in `tandem-admin` is scoped with
  `basePackages` to its own package tree, never left unscoped. An unscoped advice applies to
  every controller in the whole `ApplicationContext` — in embedded mode that includes the host
  application's own controllers, which have nothing to do with Tandem; an unrelated exception
  from the host's own endpoint would otherwise be rendered as one of this module's RFC 9457
  problems. Scoping keeps the Admin API's error handling from ever touching the host's.
- **Security is the host's responsibility.** Tandem ships the endpoints, *not* the
  authentication. The OpenAPI declares `bearerAuth` / `apiKeyAuth` security schemes as the
  expected shape, but wiring real authentication/authorization (e.g. Spring Security) is the
  application's job. Tandem documents this prominently and does not ship an open-by-default
  management surface.
- **Write operations are audit-logged, including the caller when the host authenticates one.**
  Every mutating endpoint (replay, bulk replay, discard, pause, resume, release) logs at `INFO`
  once it succeeds, with the affected identifiers and an `actor` field. `actor` is read from the
  servlet `java.security.Principal` Spring MVC binds automatically on the controller method —
  populated by *whatever* mechanism the host installs (Basic auth, an OAuth2 resource server, a
  custom filter), never a hard dependency on Spring Security specifically. When the host runs no
  authentication, `actor` is absent from the log rather than an invented value — consistent with
  "security is the host's responsibility" above. Reads are not logged: an operator's own
  dashboard polling `GET /relay/status` every few seconds would otherwise flood the log with
  traffic that never mutates anything.
- **Configurable base path + versioned** — a configurable base (default `/tandem/admin`)
  plus a fixed major-version segment `/v1`, so the effective default is `/tandem/admin/v1`.
  A breaking change ships under `/v2` alongside `/v1` (matching the semver release rule in
  AGENTS.md).
- **Errors are RFC 9457 Problem Details** — every 4xx/5xx returns
  `application/problem+json` with the standard `type` / `title` / `status` / `detail` /
  `instance` fields (extension members allowed), per the `ProblemDetail` schema in the contract.
  The **`type` is always a canonical `https://tandem.codingful.com/problems/{slug}` URL**
  (never `about:blank`). The `{slug}` (kebab-case) is the **stable machine identifier** consumers
  match on — it never changes once published. The URL need not be dereferenceable today: it is an
  identifier first; if docs are published later, the same URLs resolve, with **no contract change**
  (the reason a URL is chosen now over `about:blank`/URN). Current slugs: `unauthorized`,
  `not-found`, `internal-error`, `invalid-parameter`, `message-not-replayable`, `ordering-break-not-acknowledged`,
  `replay-no-selector`, `message-not-discardable`, `relay-coordination-unsupported`.

---

## 4. Architecture (Hexagonal, §1.2)

The admin operations are framework-agnostic **use cases** (a port, `AdminService`); REST is
a **driving adapter** implementing the OpenAPI on top of them. This keeps the operations
testable without HTTP and lets the future Admin Web UI reuse the same REST contract rather
than the internals.

**Reuse is partial — the read side is a new persistence path.** Write-side and act-side
operations do delegate to existing core ports: replay is `ReplayService` (already
implemented as `JdbcReplayService`), and the lag figures in `OutboxSummary` come from
`OutboxStore.lag()`. **The reads do not.** `OutboxRepository` is insert-only, and
`OutboxStore` is relay-shaped — its `claimBatch` is bucket-scoped and *mutates* rows to
`IN_FLIGHT`. Nothing exposes read-one-by-id, search-by-criteria, or count-per-status. Those
arrive as a new core port (`OutboxQuery`) with a `tandem-jdbc` adapter, implemented by
`InMemoryOutbox` as well so the use cases stay unit-testable without a database. Discard
(`FAILED` → `DISCARDED`) is likewise a genuinely new verb with no existing code path: it gets
its own core port, `DiscardService` (a single `discard(id, reason)` method), following the same
shape as `ReplayService` rather than being folded into `OutboxStore` — the admin-only ops never
belong in the relay's claim/mark/cleanup contract every adapter must implement (the same
reasoning that kept `OutboxQuery` out of `OutboxStore`, IMPLEMENTATION-PLAN-admin-api.md §3.3).
A discarded row also records the operator's reason in a new, additive `discard_reason` column —
kept separate from `last_error` so discarding a `FAILED` row does not erase why it failed in the
first place.
See [IMPLEMENTATION-PLAN-admin-api.md](IMPLEMENTATION-PLAN-admin-api.md) §1.

**Three independent models, on purpose.** The read port does **not** return the write/relay
model, and the REST layer does **not** serialize core types:

| Model | Owner | Job |
|---|---|---|
| `OutboxMessage` / `OutboxRecord` | `tandem-core` | The row being **written and delivered** (`payload` mandatory — a message without one cannot be published). |
| The read model behind `OutboxQuery` | `tandem-core` | The row being **read**; the list shape deliberately carries no payload. |
| The REST DTOs | `tandem-admin` | The row **on the wire**, evolving with the OpenAPI. |

Each split answers a different rigidity. Read/write independence stops a read-side need from
pushing a change into the type the relay depends on — and it is what lets the list view avoid
reading the `payload` column at all, rather than loading it and discarding it. API/core
independence keeps two **separately published, separately frozen** contracts — the OpenAPI,
consumed by REST clients Tandem never sees, and the Java surface an adopter compiles against —
from forcing changes on each other, and keeps serialization concerns out of a module whose
defining property is having no dependencies.

The contract's names are kept **deliberately distinct** from the library's for the same reason:
the API schema is `OutboxEntry` (paginated as `OutboxEntryPage`), not `OutboxMessage`, which is
the core's *write* model. Sharing a name would make two independently-evolving types read as one.

- **Module:** `tandem-admin` (optional). Contains the use-case logic and the Spring-based
  REST adapter that realises [admin-api.openapi.yaml](admin-api.openapi.yaml). It depends
  only on `tandem-core` (ports/models) and `tandem-jdbc` (DB access) — **never on the client
  application's domain code**.
- **Enablement:** Spring autoconfiguration gated by `tandem.admin.enabled` and the module's
  presence on the classpath.

### 4.1 Deployment models — embedded or fully standalone

Because **the database is Tandem's coordination point**, the Admin API needs nothing from
the client service at runtime except access to its outbox database. It supports two models:

- **Embedded** — added as a module inside the client Spring application; same process,
  simplest setup.
- **Standalone (fully independent)** — its own deployable Spring Boot service, pointed at the
  client application's datasource. **No runtime dependency on the client service**:
  independent lifecycle, independent scaling, and a separate security boundary (the
  management surface can be isolated on an internal network instead of being exposed from the
  client app). It needs only DB credentials.

**Why this works — the operations split cleanly:**

| Operation group | How it acts | Standalone-safe? |
|---|---|---|
| Reads (summary, search, detail) | DB queries | ✅ DB-only |
| Replay (single / bulk) | `UPDATE` rows back to `PENDING`; the client's relay picks them up on its next poll | ✅ DB-only |
| Discard | `UPDATE` the FAILED row to `DISCARDED`, which the head-of-chain check skips (HLD §5.3) | ✅ DB-only |
| Bucket ownership / lag / workers (read) | Query the `tandem_bucket_lease` table + outbox lag per bucket | ✅ DB-only |
| Force-release a bucket | `UPDATE tandem_bucket_lease` to clear the lease → reassigned next cycle | ✅ DB-only |
| **Relay control (pause / resume / status)** | Runtime state of the relay *process* | ⚠️ needs DB mediation (below) |

**The control tables (concrete).** Relay control and observability are mediated through tables the
schema already has — **no `tandem_relay_control` table was added** (decision, §6.1):

- **`tandem_meta`** (`key`, `value`, `updated_at`) — the existing generic key/value table (it already
  carries `bucket_count`, LLD-bucket-count-guard §5) holds the **whole-relay** desired state under a
  `relay_paused` key. Every relay instance re-reads it on its control-refresh tick and honours it;
  `POST /relay/pause|resume` without a bucket selector writes it. This is the only control path that
  works in **both** coordination modes, which is why it lives in a core table rather than a
  LEASE-only one.
- **`tandem_bucket_lease`** (`bucket`, `owner`, `lease_until`, `paused`, `updated_at`) — written by the
  relay as workers claim/renew bucket leases. The admin reads it for `GET /relay/status` (covered vs
  uncovered) and `GET /relay/buckets` (owner + lag per bucket), and writes its **additive `paused`
  column** for a per-bucket pause. `POST /relay/buckets/{bucket}/release` clears a row's lease.
- **`tandem_relay_member`** (`owner`, `lease_until`, `updated_at`) — the presence table the fair-share
  divisor already maintains (LLD-jdbc §3.2), read by `GET /relay/workers`.

**Coordination mode changes what these endpoints can answer (decision, §6.1).**
`tandem_bucket_lease` and `tandem_relay_member` are **`LEASE`-only** — under `SINGLE` the relay owns
every bucket in-process and never reads or writes either table, by design (that is what makes
`SINGLE` the zero-cost default). The admin therefore **must know the mode before it queries**, which
is why the relay records it (below):

| Endpoint | `SINGLE` | `LEASE` |
|---|---|---|
| `pause`/`resume`, **no** bucket selector | ✅ works (via `tandem_meta`) | ✅ works |
| `pause`/`resume`, **naming a bucket** | `409 relay-coordination-unsupported` | ✅ works (`404` outside `[0, B)`) |
| `GET /relay/buckets` | `409` | ✅ works |
| `GET /relay/buckets/{bucket}` | `409` | ✅ works (`404` outside `[0, B)`) |
| `GET /relay/workers` | `409` | ✅ works |
| `POST /relay/buckets/{bucket}/release` | `409` | ✅ works (`404` outside `[0, B)`) |
| `GET /relay/status` | ✅ works — `workers` and `uncoveredBuckets` are `0` by definition, `state` covers `DOWN` too (below) | ✅ works |

**`409`, not an empty list or a `404`**, because the lease tables' *presence* says nothing about
whether the relay maintains them. Under `SINGLE` those tables are either absent (any query is then a
`500`) or present-but-unused, in which case a bucket listing shows every bucket unowned — a healthy
relay rendered as a total coverage stall — and a per-bucket `pause` updates a seeded row and returns
`200` for something the relay will never act on. `409` cannot be mistaken for data.

**The relay records its mode in `tandem_meta` under a `coordination` key**, written once on every
`WorkerPool.start()` — the one entry point every assembly (Spring, plain Java, benchmark) goes
through — because the admin cannot infer it: coordination is the *relay's* configuration and a
standalone admin has its own. Absent key — no relay of this version has started here — is treated
as `SINGLE`, since nothing is known to be maintaining the coordination state.

**The same `coordination` row doubles as a heartbeat, so `RelayStatus.state` can say `DOWN`.**
Before this, liveness only existed under `LEASE` (`tandem_relay_member`'s per-instance
heartbeats) — under `SINGLE`, the default mode, `coordination` was written once and never
touched again, so "the relay died an hour ago" and "the relay never died" were indistinguishable
from the data (`docs/open-questions-lld.md` Q30). Every relay instance's `WorkerPool` now
re-touches `coordination.updated_at` on the same cadence it already re-reads pause state
(`reclaimInterval`, via `RelayControlSource.heartbeat()`), under **both** coordination modes —
one shared code path, not "check `tandem_relay_member` under `LEASE`, something else under
`SINGLE`". The relay also publishes its own heartbeat cadence once at startup
(`relay_heartbeat_interval_seconds`), so the admin computes staleness (missing more than **3×**
that interval) without guessing a threshold that might not match the deployment's actual
configuration. `state` reports `DOWN` whenever this staleness check fails — taking priority over
`PAUSED`, since "is anything running at all" matters more than the desired-state flag once
nothing is heartbeating.

Everything else stays DB-only and works across multiple relay instances and admin restarts, and
behaves identically embedded or standalone.

### 4.2 What the independence does and does not imply

- **No runtime service dependency** on the client — the admin service never calls the client,
  and the client never calls the admin service.
- **There is a schema-level contract.** The admin service operates on the same
  `outbox` / relay-control tables, so it must be **schema-compatible** with the client's
  Tandem version. This is a shared *DB contract*, not a runtime coupling — treat schema
  changes as a versioned contract between the two.
- **It has direct DB write access** (replay/discard mutate the outbox), so the standalone
  service is a privileged surface: isolate it (internal network) and secure it (§3). Security
  remains the host's responsibility.

---

## 5. Contract testing strategy

Contract testing here is **provider-side conformance to this OpenAPI document — never
consumer-driven.** The OpenAPI is the authoritative, *published* contract (API-first, §1);
Tandem is a library exposing a management API to **unknown/external** consumers (ops teams,
the future Admin Web UI, third-party tooling), not a service in a closed mesh of known
consumers. Consumer-driven contracts (Pact-style) assume you control and co-evolve with the
consumers and let them *shape* the contract — the opposite of a stable published contract.
They are therefore **out of scope by design**.

Tools (to wire when `tandem-admin` is implemented):

- **swagger-request-validator (Atlassian)** — *primary*. Validates that the real HTTP
  interactions in the Spring MockMvc / REST Assured integration tests conform to
  [admin-api.openapi.yaml](admin-api.openapi.yaml). Each endpoint test doubles as a
  conformance test; the OpenAPI stays the single source of truth. Fits the no-mocks,
  Testcontainers-based approach in [AGENTS.md](../AGENTS.md).
- **Specmatic** — *complement*. Contract-driven (not consumer-driven): uses the OpenAPI as
  an executable contract to generate requests and validate the provider's responses, and —
  uniquely among these tools — performs **backward-compatibility checking** between spec
  versions, directly enforcing the "breaking contract change = breaking library change
  (semver)" rule. JVM-native (Kotlin + JUnit), so it fits the Java/CI stack without a Python
  toolchain. *Note:* it has an open-source core plus commercial features — confirm the OSS
  tier covers CLI/JUnit usage; as a test-only dependency it is never shipped in the published
  artifact, so license impact on Tandem's Apache 2.0 distribution is minimal but should be
  verified.

The two are complementary: `swagger-request-validator` asserts conformance *inside* the
hand-written behavior tests (no-mocks style); Specmatic adds generative conformance and the
spec backward-compatibility gate.

**One tool, three jobs.** Specmatic consolidates what would otherwise be three separate
steps — contract **validation** (well-formedness; replaces the standalone `swagger-cli` /
`redocly` validate step, see §1), generative **conformance** testing, and **backward-
compatibility** checking between spec versions. A dedicated linter (`redocly lint`) stays
*optional*, only for style/governance rules Specmatic does not cover.

**Explicitly not used:** Pact / Spring Cloud Contract or any other consumer-driven tooling —
architecturally mismatched for a published, provider-authoritative contract. (Specmatic is
contract-*driven*, with the OpenAPI authoritative, so it is consistent with this stance.)

## 6. Decisions

### 6.1 Resolved

- **Pagination — cursor, not page/offset.** The contract keeps `cursor`/`nextCursor`
  (`afterId`-based). The outbox is written continuously by the client and the relay while an
  operator pages through it; `OFFSET N` both degrades linearly with
  `N` on a large table and can skip or duplicate rows as the underlying set shifts between
  page fetches. `WHERE id > :cursor ORDER BY id LIMIT :n` stays O(1) per page regardless of
  depth and is stable under concurrent writes. The tradeoff accepted: no jump-to-arbitrary-page
  and no free "page X of Y" — acceptable, since operators page forward through time/status/
  aggregate, not to a specific page number.
- **Discard semantics — hard skip only, no tombstone.** `POST
  /outbox/messages/{id}/discard` marks the row `DISCARDED` and publishes nothing in its
  place; it does not emit a compensating/tombstone event. A tombstone would need a new
  public, permanently-versioned event type/schema (a new contract surface under the
  additive-compatibility rules, §1) purely to serve an already-rare operator action, and
  every consumer would still need to tolerate it as an unknown type if unhandled — a
  minority-case cost against the common path (Pareto, §1.1). If real operational need for a
  compensating signal emerges later, add it as an explicit opt-in (e.g. an `emitTombstone`
  flag on `DiscardRequest`) rather than changing the default behaviour.
- **Relay pause scope — whole relay and per-bucket, both in scope.** The contract already
  allows both via the optional `bucket` field on `BucketSelector`. Pause is DB-mediated (storage
  and latency below), so its effect lags by one control-refresh tick — an acceptable, documented
  latency for an admin action, not a hard requirement to take effect mid-cycle. Per-bucket pause
  needs `LEASE` coordination. During a bucket pause the owning worker **keeps renewing its lease**
  and simply skips dispatching sends for that bucket; pause is orthogonal to release (the
  existing zombie-recovery endpoint), so a paused bucket is never involuntarily reassigned.
  `BucketStatus` carries a `paused` flag so a deliberately-idle bucket is distinguishable from a
  stalled one — without it, a paused bucket's growing `pendingCount`/`lagAgeSeconds` reads as a
  real problem on `GET /relay/buckets`.
- **Spec ↔ code binding — hand-write and validate in CI, no codegen.** Server-stub
  generation from the OpenAPI document is rejected in favour of hand-written controllers/DTOs
  verified by the two tools already committed to in §5 (swagger-request-validator +
  Specmatic), which catch spec drift at test time regardless of how the implementation was
  authored. Generated code would fight this project's specific conventions — the javadoc
  rules (AGENTS.md), the ban on inline comments, and especially the logging rule that every
  `toString()` reachable from a log statement (`OutboxMessage`, `OutboxRecord`, …) must never
  print `payload`/`headers` values, each backed by a dedicated unit test. Getting a generator
  to honour that would need custom templates that become their own artifact to maintain, for a
  problem (drift) the CI conformance gate already solves. No other Tandem module uses codegen;
  hand-writing keeps `tandem-admin` consistent with the rest of the codebase.
- **Discard on a non-`FAILED` row — `409 message-not-discardable`,** mirroring `replayMessage`'s
  `409 message-not-replayable`. `DISCARDED` is reachable only from `FAILED` (LLD-core §1.2), and an
  attempt from any other state is worth surfacing. Rejected: a silent no-op (hides a state the
  operator would want to see) and forcing the transition regardless (breaks the precondition).
- **Relay control storage — reuse `tandem_meta`, no `tandem_relay_control` table.** `tandem_meta` is
  already a generic `key`/`value` table serving exactly this purpose for `bucket_count`, and is a
  **core** table present in every deployment — which whole-relay pause must be, unlike
  `tandem_bucket_lease`, which only `LEASE` maintains. A dedicated flag table would add a migration
  and a schema surface for no capability the first one lacks (Pareto, §1.1). Per-bucket pause is an
  additive `paused` column on the `tandem_bucket_lease` row it naturally belongs to, rather than a
  second place to look for one bucket's state.
- **Coordination mode limits what relay endpoints can answer.** Under `SINGLE` (the zero-cost
  default) the relay owns all buckets in-process and never touches
  `tandem_bucket_lease`/`tandem_relay_member`, so per-bucket and per-worker endpoints have nothing
  truthful to report and return **`409 relay-coordination-unsupported`** there. Whole-relay
  pause/resume and `GET /relay/status` work in both modes, their state being in `tandem_meta` —
  requiring `LEASE` for pause/resume would deny the incident lever to the most common deployment.
  The relay publishes its mode in `tandem_meta` so the admin can tell the two apart; §4.1 has the
  mechanics and the per-endpoint table. Rejected: probing `information_schema` for the lease tables
  (answers whether they exist, not whether anything maintains them), and making `SINGLE` write them
  purely for observability (a DB dependency on the path whose defining property is not having one).
- **Pause latency — one control-refresh tick (`reclaimInterval`, 5 s by default), not one poll
  cycle.** Per-cycle re-reading is not affordable: `pollInterval` defaults to 100 ms and a busy
  worker loops faster still, so it would put a query on the hot path. The relay refreshes the
  desired state on its existing maintenance cadence into an in-memory flag the workers read for
  free.
- **Discard reason storage — additive `discard_reason` column.** `DiscardRequest.reason` is
  documented as "recorded for audit," so it must actually be persisted somewhere durable to
  keep that promise. Overwriting `last_error` was rejected: it would erase the original
  delivery-failure reason the moment a row is discarded, which is exactly the context an
  operator most wants preserved. A new nullable `discard_reason` column keeps the two
  independent, mirrors the additive-only schema-evolution rule (§1), and is exposed as an
  optional field on `OutboxEntry`/the core read model, visible via `getOutboxMessage`.
