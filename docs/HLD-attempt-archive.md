# Tandem — Send Attempt Archive (Design Note)

**Version:** 1.0  
**Status:** Draft  
**Companion to:** HLD §7.1 (Attempt Archive)

A durable, append-only history of **every** publish attempt — temporal data, outcome,
error detail, attempt number, trace/correlation id, and the destination coordinates — for
forensic debugging and operations. The feature is **opt-in and off by default**; when off
it adds **no performance cost** and **no setup or configuration burden**.

---

## 1. Motivation

The outbox row carries only the *latest* attempt state (`attempts`, `last_error`,
`next_attempt_at`). When an event misbehaves in production — flapping retries, a transient
broker error, a poison message — an operator cannot reconstruct *what happened on each
attempt*: when it ran, how long it took, which worker, which error, under which trace.

The attempt archive records one immutable row per attempt, so the full timeline of an
event's delivery is queryable after the fact.

---

## 2. What is recorded (per attempt)

| Field | Purpose |
|---|---|
| `outbox_id` | The event this attempt belongs to (join back to `outbox`) |
| `aggregate_id`, `aggregate_type` | Denormalized for ops queries without a join |
| `attempt_number` | Which attempt this was (mirrors `outbox.attempts` at the time) |
| `status` | Outcome: `SUCCESS` or `FAILED` |
| `started_at`, `finished_at` | When the attempt began and completed |
| `latency_ms` | Attempt duration (`finished_at − started_at`) |
| `worker_id` | The worker/`locked_by` that ran the attempt |
| `topic`, `partition`, `kafka_offset` | Destination coordinates (on success) |
| `error_class`, `error_message`, `error_detail` | Exception type, message, full detail/stacktrace (on failure) |
| `trace_id`, `correlation_id` | Distributed-tracing / correlation identifiers (from headers; see §7) |
| `created_at` | Row insertion time |

The intent is **everything useful to debug an event and help an operator** — extendable as
new diagnostic needs arise, without touching the hot outbox table.

---

## 3. Data model (created only when enabled)

```sql
-- Created ONLY when the attempt archive is enabled.
-- The base Tandem setup does NOT include this table.
CREATE TABLE tandem_outbox_attempt (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    outbox_id      BIGINT       NOT NULL,
    aggregate_id   VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    attempt_number INT          NOT NULL,
    status         SMALLINT     NOT NULL,        -- 1 = SUCCESS, 2 = FAILED
    started_at     TIMESTAMPTZ  NOT NULL,
    finished_at    TIMESTAMPTZ  NOT NULL,
    latency_ms     INT,
    worker_id      VARCHAR(64),
    topic          VARCHAR(255),                 -- on success
    partition      INT,                          -- on success
    kafka_offset   BIGINT,                       -- on success
    error_class    VARCHAR(255),                 -- on failure
    error_message  TEXT,                         -- on failure
    error_detail   TEXT,                         -- stacktrace / full detail, on failure
    trace_id       VARCHAR(128),
    correlation_id VARCHAR(128),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Full attempt timeline of one event
CREATE INDEX idx_tandem_outbox_attempt_outbox    ON tandem_outbox_attempt (outbox_id, attempt_number);
-- Per-aggregate forensic queries
CREATE INDEX idx_tandem_outbox_attempt_aggregate ON tandem_outbox_attempt (aggregate_id, created_at);
-- Failure investigation over time
CREATE INDEX idx_tandem_outbox_attempt_failed    ON tandem_outbox_attempt (created_at) WHERE status = 2;
```

The table is append-only; it is never updated, only inserted and purged (see §6).

---

## 4. Lifecycle — when an attempt is recorded

The relay invokes the recorder after each publish attempt completes, on **both** paths. Because the
async relay marks **success in batches across aggregates** but handles **failure per record** (HLD §6,
LLD-jdbc §3.4), the archive write follows the same shape — always inside the status-update transaction:

```
[KafkaRelay] publish attempts (async; batch_size records of distinct aggregates in flight)
   │
   ├── success (ack): the per-record ack data (topic/partition/offset, latency, trace…) is captured
   │        in the send's completion handler and buffered with the DONE id; at the markDoneBatch
   │        flush, the SAME transaction does
   │            UPDATE tandem_outbox SET status = DONE WHERE id = ANY(:ids)
   │            INSERT INTO tandem_outbox_attempt VALUES (…), (…), …   -- one row per acked record
   │
   └── failure: in the SAME per-record transaction that updates attempts/last_error/next_attempt_at
            (or marks FAILED),
            INSERT tandem_outbox_attempt(status=FAILED, error_class/message/detail, attempt#, trace…)
```

The archive insert **piggybacks on the status-update transaction the relay already performs** — no new
round-trip transaction (a **multi-row** insert on the batched success flush, a **single** insert on a
per-record failure). It is therefore as consistent as the status update itself: if that transaction
rolls back, so do its archive rows, and the subsequent retry records a fresh attempt (correct — a
re-attempt *is* a new attempt).

---

## 5. Opt-in, off by default, zero cost when off

This is the defining constraint. Realized via the Hexagonal pattern (HLD §1.2):

- **Port:** `AttemptRecorder` (in `tandem-core`).
- **Default adapter:** `NoOpAttemptRecorder` — does nothing. Wired by default.
- **Opt-in adapter:** `JdbcAttemptArchive` (in `tandem-jdbc`) — the table writer, wired
  only when the feature is enabled.

What "zero cost / zero setup when off" means concretely:

| Concern | When **off** (default) | When **on** |
|---|---|---|
| Schema | **No table.** Base setup is just the `outbox` table. | `tandem_outbox_attempt` table created (separate, optional DDL/migration) |
| Configuration | **Nothing to set.** | A single flag, e.g. `tandem.attempt-archive.enabled=true` |
| Write path | **No insert, no object built** (see guard below) | Archive rows inside the existing status-update tx — multi-row on the batched success flush, single on a per-record failure (§4) |
| Module | No extra module — the no-op lives in core | Uses the `tandem-jdbc` adapter |

**The guard (true zero-cost).** The relay checks the enabled flag *before* assembling the
attempt record, so when disabled there is **no timing capture, no error-detail extraction,
and no object allocation** — only a boolean check that the JIT renders negligible:

```
if (attemptRecorder.isEnabled()) {      // false by default → nothing below runs
    var record = buildAttemptRecord(...);   // timestamps, error detail, trace ids
    attemptRecorder.record(record);
}
```

A no-op recorder alone is not enough — the *construction* of the record must also be
skipped. The guard guarantees the off path adds nothing measurable.

---

## 6. Retention

The archive grows **N× faster** than the outbox (one row per attempt, not per event), so
it needs its own retention policy — independent of the outbox cleanup:

- **Configurable retention window** (e.g. keep 7–30 days).
- **Recommended: time-partitioning** on `created_at` so old data is dropped by detaching a
  partition rather than row-by-row `DELETE`.
- Retention is **only relevant when the feature is on** — another reason it carries zero
  burden by default.

---

## 7. Relation to trace / correlation id propagation

`trace_id` and `correlation_id` are read from the event's headers/context at attempt time.
Fully populating them depends on those identifiers being propagated into the outbox
`headers` at write time — the **trace & correlation propagation** feature
([HLD-tracing.md](HLD-tracing.md), HLD §7.2). With propagation off, the archive simply captures
whatever identifiers the application already placed in the headers; with it on, capture is
automatic. The archive is the primary *consumer* of those ids, which is why the two
features are designed together.

---

## 8. Open decisions

| Area | Options |
|---|---|
| Write timing | **Synchronous in the status-update tx** (consistent, one extra INSERT — preferred) vs. asynchronous/batched writer (lower latency impact, weaker consistency, more complexity) |
| Retention default | Disabled / unbounded vs. a sane default window (e.g. 14 days) when the feature is enabled |
| Module placement | Adapter in `tandem-jdbc` (no new module — preferred, Pareto) vs. a dedicated `tandem-archive` module |
| Success rows | Record **every** attempt incl. the single successful one (full timeline) vs. only failures + final success (smaller archive) |
