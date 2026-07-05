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
  `CausalContext`, `AttemptRecorder`, `TracePropagator`, `TandemMetrics`, `ReplayService`,
  `TandemAggregate`. (`OutboxEventMapper` → tandem-spring; `AdminService` → tandem-admin.) *(tandem-core)*
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
  (`tandem.attempt-archive.enabled`, `tandem.admin.enabled`, `tandem.tracing.enabled`). *(tandem-spring)*
- [~] **Q7 (P2)** — **Schema migration strategy.** *Basic-round decided:* the baseline DDL
  (`tandem_outbox` + indexes; `tandem_bucket_lease` for standalone) ships as a **hand-written,
  versioned SQL script per DB** that the operator applies (LLD-jdbc §6, HLD §5.1); a migration tool
  (Liquibase/Flyway) is **deferred**. *Still open (full):* conditional optional columns/tables
  (`type`, `lamport`, `tandem_outbox_attempt`, relay-control); versioned schema contract for standalone
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

## D. tandem-spring (producer / relay / aggregator / tandem-relay)

- [ ] **Q21 (P1)** — **Reconcile the two split axes.** §10.1 proposes a *version* split
  (`tandem-spring-core` / `-boot3` / `-boot4`); §3.2 introduces a *producer/relay* split.
  Combined they risk module explosion — decide how they coexist. *(tandem-spring; HLD §10.1, §3.2)*
- [ ] **Q22 (P2)** — **Spring component details.** `@TransactionalOutbox` aspect impl;
  `TransactionalOutboxTemplate` API; `OutboxEventMapper<T>` signature; Micrometer-Tracing adapter
  wiring. *(tandem-spring; HLD §3.1)*
- [ ] **Q23 (P2)** — **`tandem-relay` runnable.** Main class, config binding, packaging
  (JAR/Docker), how it receives N / shard assignment (ties to Q8). *(tandem-relay; HLD §3.2)*

## E. tandem-admin

- [x] **Q24 (P1)** — **`DISCARDED` state.** ✅ Resolved: add **`DISCARDED` (status=4) now**;
  transition is `FAILED → DISCARDED` (admin only); DISCARDED rows are not polled and do **not**
  block the aggregate (excluded from the poison-gate). *(tandem-core / jdbc / admin; HLD §5.3, HLD-admin-api)*
- [ ] **Q25 (P2)** — **`AdminService` signatures** (1:1 with the OpenAPI `operationId`s), cursor
  pagination encoding, relay-control table schema (see Q16). *(tandem-admin; admin-api.openapi.yaml)*

## F. tandem-test

- [x] **Q26 (P2)** — **`InMemoryOutbox` scope.** ✅ [LLD-test.md](LLD-test.md): minimal scope for the
  basic round — `InMemoryOutbox` implements **both** `OutboxRepository` + `OutboxStore` (real
  collaborator), `RecordingDispatcher` (in-memory `OutboxDispatcher` with forced failures), and
  `TandemTestContainer` (Postgres + Kafka via Testcontainers, applies baseline DDL). *(tandem-test)*

## G. Optional adapters (kafka-streams, flink, tracing-otel)

- [ ] **Q27 (P3)** — **`lamport` (BIGINT) → engine timestamp (long ms).** Representation/overflow,
  header naming (`ce_logicalclock` vs `ce_*`), concrete extractor/assigner classes. *(tandem-kafka-streams, tandem-flink; HLD §9.4)*

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
