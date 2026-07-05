# Tandem — Implementation Plan: Basic Round

**Version:** 1.0
**Status:** Active
**Scope:** the first implementation milestone — `tandem-core` → `tandem-jdbc` → `tandem-kafka` → `tandem-test`.

This is the execution plan for the basic round. The *design* is fixed in the HLD/LLDs;
this document only orders the work, fences the scope, and defines done-ness per module.
Read it first, then the relevant module LLD. Follow [AGENTS.md](../AGENTS.md) for every change.

---

## 1. Scope

### In scope (build now)
- **Gradle scaffold** — multi-project Kotlin-DSL build (LLD-base): root + `settings.gradle.kts`,
  `tandem-bom`, the four basic-round subprojects, the Maven-Central publish plugin. CI already exists.
- **`tandem-core`** — models, ports, exceptions, pure logic (`BucketHash`, `LamportClock`,
  header constants). Zero runtime dependencies (LLD-core).
- **`tandem-jdbc`** — write-side insert + relay engine (poll/claim/mark/lease/cleanup), `ReplayService`,
  config fail-fast. PostgreSQL baseline only (LLD-jdbc).
- **`tandem-kafka`** — `OutboxDispatcher` (async), producer-config fail-fast, error classifier,
  CloudEvents **binary** binding, default `TopicRouter` (LLD-kafka).
- **`tandem-test`** — `InMemoryOutbox`, `RecordingDispatcher`, `TandemTestContainer` (LLD-test).

### Out of scope (do NOT build; stop and flag if a task seems to need it)
- **Spring** anything (`tandem-spring*`, `tandem-relay` runnable) — **Q21, Q22, Q23 open**.
- **Admin API** (`tandem-admin`) — **Q25 open**; the OpenAPI exists but the impl is deferred.
- **Optional capability features** even though their ports exist in core: attempt archive,
  trace/correlation propagation, causal ordering / Lamport, Micrometer adapter. In the basic round
  their ports stay **no-op defaults** (built, but no real adapter wired).
- **MySQL DDL / engine** — **Q28 open**; PostgreSQL only for now.
- **Benchmark harness** (`tandem-benchmark`) — separate, later.

> **Hard rule:** if implementing a basic-round task appears to require an out-of-scope decision
> (a Spring detail, the admin control-table schema, MySQL specifics, a causal/tracing/archive
> behaviour), **stop and surface it** — do not invent a design. The basic round must compile and
> pass with the optional features off.

---

## 2. Module order & dependencies

```
0. scaffold ──▶ 1. core ──▶ 2. test (in-memory parts) ──▶ 3. jdbc ──▶ 4. kafka ──▶ 5. test (container) + e2e
```

- **`InMemoryOutbox` + `RecordingDispatcher` come right after core** (they implement only core ports),
  so the relay engine in `tandem-jdbc` can be unit-tested with real in-memory collaborators (no DB, no
  Kafka) per the no-mocks rule.
- **`TandemTestContainer` comes last** (it needs the JDBC + Kafka adapters and the baseline DDL).
- **The relay engine lives in `tandem-jdbc` but depends only on the `OutboxDispatcher` *port*** — never
  on `tandem-kafka`. Unit-test it with `RecordingDispatcher`; wire the real Kafka dispatcher only in the
  end-to-end test.

---

## 3. Per-module definition of done

A module is done when: code matches its LLD, tests are green (`./gradlew check`), docs are still
consistent (AGENTS.md), and the module-specific invariant below holds.

### 0. Scaffold
- `./gradlew build` succeeds with empty subprojects; BOM aligns versions; publish config present
  (not executed). Java 17 toolchain, Kotlin DSL.

### 1. `tandem-core`
- Types: `AggregateId`, `OutboxStatus` (+`fromCode`), `OutboxMessage` (+builder), `OutboxRecord`,
  `ReplayCriteria`/`ReplayResult`.
- Ports: `OutboxRepository`, `OutboxStore`, `OutboxDispatcher`, `PayloadSerializer`, `TopicRouter`,
  `ReplayService`, `TandemAggregate`, and the optional ports as **no-op defaults**
  (`TandemMetrics`, `AttemptRecorder`, `TracePropagator`, `CausalContext`).
- Pure logic: `BucketHash.bucketFor` (FNV-1a 64-bit + `Math.floorMod`), `LamportClock.merge`,
  header-name constants. Exception hierarchy (LLD-core §3).
- **Invariant: zero external runtime dependencies** — verify the dependency graph has nothing but the JDK.
- Tests: pure-logic behaviours (bucket distribution + determinism, `floorMod` covers the full hash range
  incl. the negative/min case, status round-trip, Lamport merge, builder validation).

### 2. `tandem-test` (in-memory parts)
- `InMemoryOutbox` implements **both** `OutboxRepository` + `OutboxStore` faithfully (head-of-chain claim,
  lease, retry/fail, reclaim, cleanup), computing `bucket` via the **same core `BucketHash`** so it
  matches the real DB. Controllable clock for deterministic backoff/lease tests.
- `RecordingDispatcher` implements `OutboxDispatcher`, records sends, can force retriable/permanent
  failures, and exposes a controllable variant that completes futures on demand (to exercise overlapping
  in-flight dispatch).
- Tests: the helpers' own behaviour (they are real collaborators, so they must be correct).

### 3. `tandem-jdbc` (PostgreSQL)
- `JdbcOutboxRepository.insert/insertAll` — bucket from `BucketHash`, `contentType`→`headers`,
  `DuplicateSeqException` on unique violation. **Invariant: no Kafka on this path** (write-side footprint).
- `JdbcOutboxStore` — head-of-chain `claimBatch`, `markDoneBatch`, `markForRetry`, `markFailed`,
  `reclaimExpiredLeases` (**increments `attempts`**, quarantines to FAILED at max — LLD-jdbc §3.5),
  `cleanup`.
- `WorkerPool` — supervised threads (restart on death; LLD-jdbc §3.1), continuous claim-while-busy loop,
  idle backoff, embedded bucket assignment + standalone `tandem_bucket_lease` assignment, graceful
  shutdown. `BackoffStrategy` (full-jitter). `ReplayService`.
- Config fail-fast: `rowLease > delivery.timeout.ms` (the relay receives the effective
  `deliveryTimeoutMs` in its config; canonical message/log/metric, LLD-jdbc §3.5).
- Tests: relay loop against `RecordingDispatcher` (ordering, head-of-chain blocking, poison isolation,
  retry/backoff, reclaim/attempt-bump); integration against Postgres via Testcontainers (claim SQL,
  unique constraint, indexes, cleanup).

### 4. `tandem-kafka`
- `KafkaRelay implements OutboxDispatcher` — async `send` + callback → `CompletableFuture<Void>`;
  error `classify` → `OutboxDispatchException.isRetriable()`.
- Producer-config fail-fast on unsafe overrides (idempotence/acks/max.in.flight; LLD-kafka §1).
- CloudEvents **binary** binding (id/source/type/subject/time/datacontenttype, `ce_seq`/`ce_partitionkey`;
  null-`type` fallback to `aggregate_type`). Default `TopicRouter` = `kebab(aggregateType)+suffix`.
- Tests: dispatcher success/failure + classification (RecordingDispatcher analog at unit level);
  integration against a Kafka (KRaft) container asserting the CloudEvent on the topic.

### 5. `tandem-test` (container) + end-to-end
- `TandemTestContainer` starts Postgres + Kafka, applies `schema/postgres/tandem-baseline.sql`, exposes
  factories for `JdbcOutboxRepository`, the `WorkerPool` relay, and a consumer.
- **End-to-end test:** insert in a transaction → run the relay → assert the CloudEvent lands on the topic
  in per-aggregate `seq` order, with **zero ordering violations** and **zero lost events**.

---

## 4. Cross-cutting reminders (from AGENTS.md / HLD)

- **No mocks.** `InMemoryOutbox` for unit, `TandemTestContainer` for integration. Refactor for testability
  rather than mocking.
- **BDD test names** `GIVEN_..._WHEN_..._THEN_...`; test behaviours that would fail under mutation; delete
  useless tests.
- **Minimal client footprint:** the write-side (`core` + the JDBC insert) must not pull Kafka, CloudEvents,
  tracing, a forced JSON binding, or Spring.
- **Compatibility:** named columns (never `SELECT *`), additive schema, tolerant readers.
- **Optional features off:** their ports resolve to no-op defaults; guard any capability behind
  `isEnabled()` so the off-path costs nothing.
- **Checkpoint per module:** stop after each module for a spec-adherence review before starting the next.

---

## 5. Definition of done — basic round
- All four modules implemented, `./gradlew check` green (incl. the `@Tag("integration")` Testcontainers
  tests with Docker available).
- The manual no-Spring wiring of LLD-jdbc §7 works end-to-end on PostgreSQL.
- `tandem-core` proven zero-dependency; the write-side proven Kafka-free.
- Docs still consistent; no out-of-scope decision was silently invented (all such points surfaced).
