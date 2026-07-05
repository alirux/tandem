# Tandem — Admin API (Design Note)

**Version:** 1.0  
**Status:** Draft  
**Companion to:** HLD §7.3 (Admin API)  
**Contract:** [admin-api.openapi.yaml](admin-api.openapi.yaml)

A REST operations layer over the outbox (the *sends*) and the send-attempt archive (the
*trace of attempts*), plus operational control of the relay. Built **API-first**: the
OpenAPI document is the source of truth and is defined and reviewed *before* the
implementation.

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
| **Outbox — read** | health summary (counts per status, lag count/age); search messages (by status / aggregate / type / time); get one message with full payload + headers |
| **Outbox — act** | replay one message; bulk replay by criteria (with `dryRun` preview); discard a FAILED message (explicit ordering-break acknowledgement required) |
| **Attempts** | attempt timeline of one message; search the attempt archive (by aggregate / status / `traceId` / `correlationId` / time) |
| **Relay** | status (state, bucket coverage, worker count); pause / resume (whole relay or a single bucket); **per-bucket ownership + lag** (spot uncovered/hot buckets); **active workers**; **force-release a bucket** for reassignment (zombie owner recovery) |

Replay builds on the per-aggregate `ReplayService` (HLD §8); the attempt endpoints read the
optional attempt archive (HLD §7.1) and return empty / `503` when it is disabled.

---

## 3. Opt-in and security (Pareto, §1.1)

- **Off by default.** The Admin API is an attack surface; it is not exposed unless explicitly
  enabled (`tandem.admin.enabled=true`). When off there is no endpoint, no controller bean,
  and no cost.
- **Security is the host's responsibility.** Tandem ships the endpoints, *not* the
  authentication. The OpenAPI declares `bearerAuth` / `apiKeyAuth` security schemes as the
  expected shape, but wiring real authentication/authorization (e.g. Spring Security) is the
  application's job. Tandem documents this prominently and does not ship an open-by-default
  management surface.
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
  `not-found`, `internal-error`, `message-not-replayable`, `ordering-break-not-acknowledged`,
  `replay-no-selector`, `attempt-archive-disabled`.

---

## 4. Architecture (Hexagonal, §1.2)

The admin operations are framework-agnostic **use cases** (a port, `AdminService`); REST is
a **driving adapter** implementing the OpenAPI on top of them. The use cases delegate to
existing core ports (`OutboxRepository`, `ReplayService`) and the attempt-archive query —
they introduce no new persistence path. This keeps the operations testable without HTTP and
lets the future Admin Web UI reuse the same REST contract rather than the internals.

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
| Reads (summary, search, detail, attempts) | DB queries | ✅ DB-only |
| Replay (single / bulk) | `UPDATE` rows back to `PENDING`; the client's relay picks them up on its next poll | ✅ DB-only |
| Discard | `UPDATE` the FAILED row to `DISCARDED`, which the head-of-chain check skips (HLD §5.3) | ✅ DB-only |
| Bucket ownership / lag / workers (read) | Query the `tandem_bucket_lease` table + outbox lag per bucket | ✅ DB-only |
| Force-release a bucket | `UPDATE tandem_bucket_lease` to clear the lease → reassigned next cycle | ✅ DB-only |
| **Relay control (pause / resume / status)** | Runtime state of the relay *process* | ⚠️ needs DB mediation (below) |

**The control tables (concrete).** Relay control and observability are mediated through two
tables — the same `tandem_bucket_lease` table the relay already uses for bucket assignment (HLD §4.3,
LLD-jdbc §3.2), plus a tiny `tandem_relay_control` flag table:

- **`tandem_bucket_lease`** (`bucket`, `owner`, `lease_until`, `updated_at`) — written by the relay as
  workers claim/renew bucket leases. The admin reads it for `GET /relay/status` (covered vs
  uncovered), `GET /relay/buckets` (owner + lag per bucket), and `GET /relay/workers` (distinct
  live owners). `POST /relay/buckets/{bucket}/release` clears a row's lease.
- **`tandem_relay_control`** — a small key/value (or single-row) table holding the desired state
  (RUNNING / PAUSED, optionally per bucket). The relay reads it each poll cycle and honours it;
  `POST /relay/pause|resume` writes it.

This keeps every endpoint DB-only and works across multiple relay instances and admin restarts.
(In embedded mode the same DB-mediated mechanism is used, so behaviour is identical regardless
of deployment model. The single-instance embedded case may keep these in-process, but the
table-mediated path is the default so adding a second instance just works.)

### 4.2 What the independence does and does not imply

- **No runtime service dependency** on the client — the admin service never calls the client,
  and the client never calls the admin service.
- **There is a schema-level contract.** The admin service operates on the same
  `outbox` / `tandem_outbox_attempt` / relay-control tables, so it must be **schema-compatible** with
  the client's Tandem version. This is a shared *DB contract*, not a runtime coupling — treat
  schema changes as a versioned contract between the two.
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

## 6. Open decisions

| Area | Options |
|---|---|
| Pagination | Cursor (afterId, in the contract) vs. page/offset | 
| Discard semantics | Hard "skip the event" (ordering break, acknowledged) vs. also support "discard + emit a tombstone/compensation" |
| Relay pause scope | Whole relay only vs. per-shard (the contract allows both) — confirm per-shard pause is operationally safe with lease reclaim |
| Spec ↔ code binding | Generate server stubs from the spec at build time vs. hand-write and validate against the spec in CI |
