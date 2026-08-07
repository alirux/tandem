# Tandem — Open Questions before the LLDs

**Version:** 1.0  
**Status:** Living checklist  
**Purpose:** Track the gaps and ambiguities in the HLD + companion specs that must be
resolved (or consciously deferred) to write correct per-module LLDs.

**Priority:** **P1** = blocks the relevant LLD · **P2** = important ambiguity · **P3** = minor / cleanup.  
**Status:** `[ ]` open · `[x]` resolved (link the resolving doc/section).

---

## A. Cross-cutting / model

- [x] **Q1 (P1)** — **Port contracts.** ✅ Resolved in [LLD-core.md](LLD-core.md) §2: signatures for
  `OutboxRepository`, `OutboxStore`, `OutboxDispatcher`, `PayloadSerializer`, `TopicRouter`,
  `CausalContext`, `TracePropagator`, `TandemMetrics`, `ReplayService`,
  `TandemAggregate`. (`OutboxEventMapper` → tandem-spring-producer; `AdminService` → tandem-admin.) *(tandem-core)*
- [x] **Q2 (P1)** — **`OutboxMessage` model.** ✅ Resolved: `AggregateId` = **typed value object**;
  **`OutboxMessage`** = write-side value (immutable + builder), **`OutboxRecord`** = stored row
  with delivery state; **`OutboxStatus`** enum ↔ `SMALLINT`. Detail → `LLD-core.md`. *(tandem-core; HLD §5)*
- [x] **Q3 (P1)** — **`payload` type.** ✅ Resolved: core treats payload as **`byte[]`** via a
  pluggable **`PayloadSerializer`** (no forced JSON lib — minimal footprint §1.3); column is
  **`JSONB` by default** (readable/inspectable) and **`BYTEA`** for binary serializers
  (Avro/Protobuf). *(tandem-core / jdbc / kafka; HLD §5.1, §4.8)*
- [x] **Q4 (P1)** — **Metrics vs zero-dependency core.** ✅ Resolved: a **`TandemMetrics` port**
  in `tandem-core` (no-op default); the relay/poller calls it without knowing Micrometer. The
  **Micrometer adapter lives in an optional relay-side module `tandem-micrometer`**, so the
  client write-side never depends on Micrometer and `tandem-jdbc` stays Micrometer-free.
  *(tandem-core / tandem-micrometer; HLD §7, §1.2, §1.3)*
- [x] **Q5 (P2)** — **Exception model.** ✅ Resolved: an **unchecked** hierarchy rooted at
  `TandemException` — `OutboxInsertException` → `DuplicateSeqException` (UNIQUE violation =
  optimistic-conflict signal), `PayloadSerializationException`, `OutboxDispatchException`,
  `TandemConfigurationException`. Relay classifies Kafka errors retriable vs non-retriable (Q17).
  Detail → `LLD-core.md`. *(tandem-core)*
- [~] **Q6 (P2)** — **Consolidated configuration reference.** *Basic-round subset done:* the defaults
  the round needs are tabulated in [LLD-jdbc.md](LLD-jdbc.md) §6. *Still open (full):* one table of every
  `TandemProperties` key (name, type, default, scope) incl. feature flags
  (`tandem.admin.enabled`, `tandem.tracing.enabled`). The core
  `tandem.*` contract (outbox/relay/kafka) is specified in [LLD-spring-config.md](LLD-spring-config.md) §2;
  the feature-flag keys land with their own modules. *(tandem-spring-producer / tandem-spring-relay)*
- [~] **Q7 (P2)** — **Schema migration strategy.** *Basic-round decided:* the baseline DDL
  (`tandem_outbox` + indexes; `tandem_bucket_lease` for standalone) ships as a **hand-written,
  versioned SQL script per DB** that the operator applies (LLD-jdbc §6, HLD §5.1); a migration tool
  (Liquibase/Flyway) is **deferred**. *Still open (full):* conditional optional columns/tables
  (`type`, `lamport`, relay-control); versioned schema contract for standalone
  relay/admin. **Must satisfy backward + forward compatibility (§1.4)** — additive scripts only. *(all)*

## B. tandem-jdbc (relay + write-side persistence)

- [x] **Q8 (P1)** — **Worker model / shard assignment.** ✅ Resolved: **virtual-bucket sharding**.
  A fixed, large bucket count `B` (e.g. 256, never changed) computed **in Java** at insert
  (`BucketHash.bucketFor` = 64-bit FNV-1a + `Math.floorMod`; engine-independent, LLD-core §4); each bucket owned by one
  worker; workers own bucket *subsets* — in-process (embedded) or `tandem_bucket_lease` table (standalone, self-healing
  coverage). **Structural** per-aggregate exclusivity (no per-aggregate lock); changing worker count needs
  no migration (`B` fixed). Chosen over per-aggregate claim after the devil's-advocate analysis
  ([q8-worker-model-decision.md](q8-worker-model-decision.md)): structural exclusivity + *loud*
  failure (coverage stall) beats lock-based exclusivity + *silent* reorder risk. HLD §4.3/§6. *(tandem-jdbc)*
- [x] **Q9 (P1)** — **Transaction boundaries.** ✅ [LLD-jdbc.md](LLD-jdbc.md) §3.3/§3.4: tx1 =
  head-of-chain claim + `UPDATE IN_FLIGHT` (commit; lease takes over), publish outside any tx
  (async, overlapping in-flight across the batch's distinct aggregates), tx2 = `markDoneBatch`
  (acked ids across aggregates). *(tandem-jdbc)*
- [x] **Q10 (P1)** — **Batch ordering on partial failure.** ✅ §3.4: a batch is one head per
  aggregate, dispatched with overlapping in-flight sends; per-aggregate order is **structural**
  (`seq(N+1)` only becomes a claimable head once `seq(N)` is DONE, E6), and a failed row stays
  non-DONE so its aggregate's later rows stay blocked next cycle — no explicit per-aggregate loop.
  *(tandem-jdbc / tandem-kafka)*
- [x] **Q11 (P2)** — **Poison-gate exact query.** ✅ §3.3: subsumed by the **head-of-chain `NOT EXISTS`**
  predicate (status 0/1/3), with the supporting index. *(tandem-jdbc)*
- [x] **Q12 (P2)** — **Cleanup.** ✅ §3.7: chunked batch `DELETE` of DONE/DISCARDED past retention
  (default 14 days); time-partitioning as an opt-in high-volume alternative. *(tandem-jdbc)*
- [x] **Q13 (P2)** — **`BackoffStrategy`.** ✅ §3.6: exponential **full-jitter**, base 1s, cap ~5min,
  max 10 attempts; DB-clock `next_attempt_at`. *(tandem-jdbc)*
  > **Drift found and corrected (2026-07-23, during a relay-lease review).** The decision above was
  > right but the **implementation had diverged**: `RelayWorker` anchored the deadline on its own wall
  > clock (`clock.instant() + backoff`) and passed an absolute `Instant`, while the claim compares
  > `next_attempt_at` with the DB's `now()`. A relay-to-DB clock offset therefore shifted when a row
  > became due — early enough to burn `attempts` and quarantine it prematurely, or late enough to delay
  > delivery. Safety (ordering, exclusivity, bucket ownership) was never affected: those were already
  > DB-anchored. Realigned to the documented decision — `markForRetry(id, error, retryDelay)` now takes
  > the **relative** backoff and the adapter anchors it (`now() + retryDelay`), mirroring how
  > `claimBatch` passes `rowLease`; `RelayWorker` holds **no `Clock`** so a locally-anchored deadline
  > cannot reappear. The invariant is now stated in [LLD-jdbc.md](LLD-jdbc.md) §3.2, with cleanup's
  > retention cutoff (Q12, §3.7) recorded as the single deliberate exception — relay-computed and
  > benign over a window of days. *Lesson: this doc records decisions, not what the code does; a
  > resolved question is not evidence the code still matches it.*
- [x] **Q14 (P2)** — **WorkerPool lifecycle.** ✅ §3.1: thread pool (`cores×2`), poll loop, graceful
  shutdown (in-flight recovered by lease). *(tandem-jdbc)*
- [x] **Q15 (P2)** — **Lease reclaim.** ✅ §3.5: periodic (~5s) `UPDATE … WHERE status=1 AND locked_until<now()`.
  *(tandem-jdbc)*
- [x] **Q16 (P2)** — **Remaining SQL.** ✅ INSERT/claim/markDone/reclaim/cleanup in LLD-jdbc; the
  `tandem_bucket_lease` ↔ relay heartbeat/control reconciliation done (HLD-admin-api §4.1); Lamport store
  resolved — **Tandem-managed `tandem_aggregate_clock` table** with an atomic upsert advance (HLD §9.3,
  LLD-jdbc §2; clean boundary, no domain-table writes). *(tandem-jdbc)*

## C. tandem-kafka

- [x] **Q17 (P1)** — **Producer failure semantics.** ✅ [LLD-kafka.md](LLD-kafka.md) §1/§2/§4:
  `dispatch` = **async** (`send` + callback → `CompletableFuture<Void>`; the future completes on the
  ack or exceptionally with the verdict) so the relay overlaps `batch_size` records in flight per shard;
  mandated safe producer config (idempotence/acks=all/max.in.flight≤5, fail-fast on unsafe override);
  error classifier → **retriable** = `markForRetry`, **permanent** (RecordTooLarge, Serialization,
  auth, InvalidTopic) = `markFailed`; verdict carried in `OutboxDispatchException`. *(tandem-kafka)*
- [x] **Q18 (P2)** — **`TopicRouter` default.** ✅ §5: source = **`aggregate_type`**; rule =
  `kebab-case(aggregate_type)` + suffix (default `-topic`, configurable), **no pluralization**
  (`Order` → `order-topic`). Override via custom router or a static map. HLD examples corrected. *(tandem-kafka)*
- [x] **Q19 (P2)** — **CloudEvents binding.** ✅ §3: `CloudEventBuilder` mapping; binary/structured/raw
  modes via the SDK; `datacontenttype` = `headers["content-type"]` else config default; extensions
  become `ce_seq`/`ce_logicalclock`/`ce_partitionkey`; the Lamport header is reconciled to **`ce_logicalclock`** (§9 updated). *(tandem-kafka)*
- [x] **Q20 (P2)** — **Null `type`.** ✅ §3.4: fall back to **`aggregate_type`** (configurable) so the
  required CloudEvents `type` is always valid; raw mode needs no `type`. *(tandem-kafka)*

## D. tandem-spring (producer / relay / tandem-relay runnable — no aggregator, Q21)

- [x] **Q21 (P1)** — **Reconcile the two split axes.** ✅ Resolved: split by **role only** —
  `tandem-spring-producer` + `tandem-spring-relay`, each a **single artifact** compiled against the
  Spring Framework 6.x/7.x common API subset (no `-boot3`/`-boot4` split) and validated by a CI
  version matrix. The role split is structural enforcement of the minimal client footprint; the
  version split would protect no invariant, so it stays the §10.1 fallback. No all-in-one aggregator.
  *(tandem-spring; HLD §10.1, §3.2; LLD-spring-config §1)*
- [x] **Q22 (P2)** — **Spring write-side ergonomics.** ✅ [LLD-spring-producer.md](LLD-spring-producer.md):
  Template = `execute(Function<OutboxCollector,T>)` (collector owns the tx + optional-Jackson object
  payloads); `@TransactionalOutbox` = composed `@Transactional` aspect extracting `TandemAggregate`
  after `proceed()`, inside the tx, with an active-tx fail-fast backstop; Spring-events = synchronous
  `@EventListener` scoped to `OutboxMessage` + registered `OutboxEventMapper<T>` types, fail-fast
  without a tx. Serializer optional (never forced); `byte[]` path always dependency-free.
  Micrometer-Tracing left to the tracing increment (HLD-tracing §8). *(tandem-spring-producer; HLD §3.1)*
- [ ] **Q23 (P2)** — **`tandem-relay` runnable.** Main class, config binding, packaging
  (JAR/Docker), how it receives N / shard assignment (ties to Q8). *(tandem-relay; HLD §3.2)*

## E. tandem-admin

- [x] **Q24 (P1)** — **`DISCARDED` state.** ✅ Resolved: add **`DISCARDED` (status=4) now**;
  transition is `FAILED → DISCARDED` (admin only); DISCARDED rows are not polled and do **not**
  block the aggregate (excluded from the poison-gate). *(tandem-core / jdbc / admin; HLD §5.3, HLD-admin-api)*
- [ ] **Q25 (P2)** — **`AdminService` signatures** (1:1 with the OpenAPI `operationId`s), cursor
  pagination encoding, relay-control table schema (see Q16). *(tandem-admin; admin-api.openapi.yaml)*
- [x] **Q30 (P2)** — **Three admin additions around the lag gauges.** ✅ Resolved 2026-08-05:
  1. **Change `metricsInterval` at runtime** — **demoted to the backlog at very low priority, not
     built.** It would require restructuring `WorkerPool`'s scheduling from a single
     `scheduleWithFixedDelay` call to a self-rescheduling task (`ScheduledExecutorService` has no
     API to change a pending fixed-delay task's period) — the only place in the codebase needing
     that pattern, disproportionate to how rarely "raise resolution during an incident" would
     actually get used. See the backlog for the demoted entry.
  2. **Take a lag reading on demand** — **already built, nothing was needed.** `GET
     /outbox/summary` already calls `OutboxStore.lag()` synchronously on every request
     (`OutboxAdminService.summary()`), independent of any relay process being alive, and does not
     push through `TandemMetrics`. The query is bounded to backlog size, not table size, via the
     existing partial index `idx_tandem_outbox_dispatch (bucket, id) WHERE status = 0` — this
     question's original "unindexed, proportional to backlog" framing predated that index.
  3. **`RelayStatus` can now say "no relay is alive"** — **done.** `state` gains `DOWN`
     (additive, §1.4). Resolved by giving `SINGLE` a lightweight heartbeat too, closing the
     asymmetry this question described: `RelayControlSource.heartbeat()` re-touches
     `tandem_meta.coordination.updated_at` on the same cadence `refresh()` already runs on
     (`WorkerPool`'s `controlTick`, every `reclaimInterval`) — under both coordination modes, not
     just `LEASE`'s `tandem_relay_member`. The relay also publishes its own heartbeat cadence once
     at startup (`relay_heartbeat_interval_seconds`), so the admin computes staleness
     (`> 3× that interval`) without guessing it. *(tandem-core: `RelayStatusView.alive`;
     tandem-jdbc: `RelayControlSource.heartbeat()`, `JdbcRelayQuery.status()`; tandem-admin:
     `RelayStatusResponse.from`; admin-api.openapi.yaml)*

## F. tandem-test

- [x] **Q26 (P2)** — **`InMemoryOutbox` scope.** ✅ [LLD-test.md](LLD-test.md): minimal scope for the
  basic round — `InMemoryOutbox` implements **both** `OutboxRepository` + `OutboxStore` (real
  collaborator), `RecordingDispatcher` (in-memory `OutboxDispatcher` with forced failures), and
  `TandemTestContainer` (Postgres + Kafka via Testcontainers, applies baseline DDL). *(tandem-test)*

## G. Optional adapters (kafka-streams, flink, tracing-otel, micrometer)

- [ ] **Q27 (P3)** — **`lamport` (BIGINT) → engine timestamp (long ms).** Representation/overflow,
  header naming (`ce_logicalclock` vs `ce_*`), concrete extractor/assigner classes. *(tandem-kafka-streams, tandem-flink; HLD §9.4)*
- [ ] **Q31 (P2)** — **`tandem-micrometer` design.** The module is reserved (`docs/LLD-base.md`:
  artifactId + package `com.codingful.tandem.micrometer`) but has no LLD, no dependency-catalog entry,
  and no wiring decision — it survived only as a trailing clause of the metrics work (HLD §7,
  LLD-jdbc §4) until 2026-07-28.
  - **✅ Resolved 2026-07-29: `tandem-spring-relay` autoconfigures the adapter bean; the user does not
    wire it by hand.** The precedent is `JacksonPayloadSerializer` (`tandem-spring-producer`) — an
    optional-library-backed port implementation gated by `@ConditionalOnClass`, contributed by the
    Spring module itself, never left to manual `@Bean` wiring — and every other capability in this
    Spring integration follows the same "classpath presence is the opt-in signal, zero config once
    present" shape (`TopicRouter`, `WorkerPool`, the Template, the annotation and events tiers). No
    minimal-footprint concern either way: `tandem-micrometer` is relay-side only, so the write-side is
    unaffected regardless (HLD §1.3, "wired only where the relay runs").
  - **New structural fact this creates, to design carefully, not just repeat:** unlike Jackson —
    where `JacksonPayloadSerializer` lives *inside* the module that conditions on it, so only one
    `@ConditionalOnClass` is needed — the Micrometer adapter lives in a **separate** module.
    `tandem-spring-relay` therefore needs a new **optional (`compileOnly`) dependency on a sibling
    Tandem module**, gated by a **second** condition (Micrometer's `MeterRegistry` present *and* the
    adapter class itself present) — the first time any Tandem module optionally depends on another;
    every existing sibling dependency (`tandem-jdbc`, `tandem-kafka`) is mandatory (`api`). Confirm the
    two-condition ordering doesn't reopen the nested-`@Configuration`/class-literal traps already
    documented in LLD-spring-config §1.1, and that it's included in the dual-generation `bootFourTest`
    classpath once built.
  - **Still needed before code:** the LLD itself — mapping each `TandemMetrics` method to a Micrometer
    meter name/type/tags (HLD §7's own type column needs the `failed.count` correction folded in,
    2026-07-29), the gauge-registration mechanics consistent with the push-on-timer decision (the
    adapter keeps the last value the relay pushed, never queries on scrape), and `recordUncoveredBuckets`
    — the one signal LLD-jdbc §4 defers here explicitly.
  - **Verified, not assumed:** Micrometer stays on the same major across both Spring Boot lines this
    project supports — Boot 3.3.13 manages **1.13.15**, Boot 4.1.0 manages **1.17.0** (read from the real
    `spring-boot-dependencies` POMs). Lower cross-generation risk than the Framework 6→7 jump, but per
    this repo's own 2026-07-27 lesson (a green single-version run doesn't prove compatibility with the
    other), that still wants a real dual-generation test once the module exists, not just this
    version-number argument. *(tandem-micrometer, tandem-spring-relay; HLD §1.3/§7, LLD-jdbc §4,
    LLD-spring-config §1.1/§4.4, LLD-base)*

## H. Minor inconsistencies / cleanup

- [ ] **Q28 (P3)** — **MySQL DDL incomplete (§5.4).** No `TIMESTAMPTZ` in MySQL
  (`DATETIME`/`TIMESTAMP`), `JSONB`→`JSON`, partial indexes unsupported (need a workaround).
  *(tandem-jdbc; HLD §5.4)*
- [ ] **Q29 (P3)** — **`headers` naming sweep.** Confirm `traceparent`/`correlation-id` everywhere;
  no stale `trace-id`. *(docs)*

---

## Blocker summary (resolve first)

The genuine blockers before starting the LLDs cluster into:
1. **Q1 + Q2** — port signatures + `OutboxMessage` / `OutboxStatus` (core).
2. **Q3 + Q4** — payload type, and metrics vs zero-dep core.
3. **Q8 + Q9 + Q10 + Q17** — multi-instance shard assignment, transaction boundaries, batch
   ordering on failure (jdbc/kafka).
4. **Q24 + Q7** — `DISCARDED` state and the schema migration strategy.

Resolving these unblocks `tandem-core` first, then `tandem-jdbc` / `tandem-kafka`.
