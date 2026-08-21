# Tandem — `tandem-test` LLD (minimal)

**Version:** 0.1 (Draft)  
**Module:** `tandem-test` · package `com.codingful.tandem.test`  
**Depends on:** `tandem-core` (+ `tandem-jdbc`/`tandem-kafka` for the container helper); Testcontainers
+ JUnit 6 + AssertJ (test-scope). **Resolves:** Q26 (minimal scope for the basic round).

Test utilities supporting the **no-mocks / Detroit-school** approach (AGENTS.md): real in-memory
collaborators for unit tests, and a Testcontainers helper for integration tests. Just enough to test
the basic round (write-side + relay + end-to-end); richer helpers come later.

---

## 1. `InMemoryOutbox` — unit tests without a database

A faithful in-memory implementation of **both** `OutboxRepository` (write-side) and `OutboxStore`
(relay-side) — a real collaborator, not a mock. Backed by a thread-safe map of `OutboxRecord`.

- `insert` / `insertAll`: assign `id` (atomic counter), compute `bucket` via the **same core
  `BucketHash.bucketFor`** the JDBC adapter uses (LLD-core §4) so in-memory and real-DB buckets match,
  store as `PENDING`; enforce `UNIQUE(aggregate_id, seq)` → `DuplicateSeqException`. A message that
  leaves `seq` to Tandem (`managedSeq()`, [HLD-managed-seq](HLD-managed-seq.md) §4.1) is numbered
  from an **outbox-wide counter**, standing in for the `tandem_seq` sequence — one counter for the
  whole outbox, not one per aggregate, so the numbers are sparse per aggregate exactly as the
  database's are. `lockedWrite()` (`HLD-managed-seq` §4.2) needs no mechanism here: every insert
  already serialises under one internal lock, stricter than the advisory lock asks for — the flag is
  carried through onto the stored message rather than acted on.
- `claimBatch(buckets, worker, lease, n)`: return the **head of each aggregate's pending chain** in the
  given buckets (earliest not-DONE row, eligible by `next_attempt_at`), mark `IN_FLIGHT` — mirrors the
  SQL semantics of LLD-jdbc §3.3.
- `markDone` / `markForRetry` / `markFailed` / `reclaimExpiredLeases` / `cleanup`: mutate the map.
- Test affordances: inspect rows by status, advance a controllable clock (to test backoff/lease
  expiry deterministically).

This lets unit tests exercise the write-side and the relay loop with zero I/O.

## 2. `RecordingDispatcher` — in-memory `OutboxDispatcher`

An in-memory `OutboxDispatcher` that **records** the dispatched records (the "published" events) for
assertions, and can be told to **fail** a given record retriably or permanently — so the relay's
retry/backoff and fail-fast paths (LLD-kafka §4) are testable without Kafka. A real collaborator, not
a mock. Its `dispatch` returns a `CompletableFuture<Void>` matching the port (LLD-core §2.3):
already-completed for recorded successes, completed-exceptionally with the chosen retriable/permanent
`OutboxDispatchException` for forced failures — and a controllable variant that completes futures on
demand, so the relay's **overlapping in-flight dispatch** (LLD-jdbc §3.4) is exercised as in production.

## 3. `RecordingMetrics` — enabled `TandemMetrics`

An **enabled** `TandemMetrics` that keeps what it was told: counters accumulate, gauges keep their
latest reading. Being enabled is the point — every metric call in the relay sits behind
`isEnabled()`, and the lag reading is not even scheduled without an adapter (LLD-jdbc §4), so a test
running on the no-op default exercises none of that code. A real collaborator, not a mock; also
usable by an adopter to check their own adapter is reached.

## 4. `TandemTestContainer` — integration tests

A Testcontainers helper (tagged `@Tag("integration")`) that:
- starts a real **PostgreSQL** and a real **Kafka** (KRaft) container,
- applies the committed **baseline DDL** ([`schema/postgres/tandem-baseline.sql`](../schema/postgres/tandem-baseline.sql), generated from the changelog; LLD-jdbc §1/§6) — so every integration test also proves the generated schema applies to a real PostgreSQL,
- exposes the `DataSource` + bootstrap servers and convenience factories for `JdbcOutboxRepository`,
  the `WorkerPool` relay, and a Kafka consumer,

so an end-to-end test can: insert in a transaction → run the relay → assert the CloudEvent landed on
the topic in per-aggregate order.

## 5. Scope (minimal, for the basic round)

In: the four helpers above — enough to unit-test write-side + relay loop and integration-test the
full path. Out (later): MySQL container variants, causal-ordering/admin test fixtures,
property-based/fuzz harnesses.
