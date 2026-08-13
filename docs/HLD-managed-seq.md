# Tandem — Managed `seq` (Design Note)

**Version:** 1.0  
**Status:** Draft — **not designed to completion, not built, not decided**  
**Companion to:** [HLD.md](HLD.md) §4.2 (Ordering Established at Write Time)

**This document is the design.** [HLD §4.2](HLD.md) states the shipped contract — `seq` is
app-assigned, from the aggregate's `version`, and Tandem never generates it — and that contract is
unchanged by anything written here. This note records what that contract costs an existing
application, the measurements behind that claim, and an **opt-in** alternative in which Tandem
assigns `seq` itself.

---

## 0. Implementation status — read this first

**Nothing in this document is built.** There is no counter table, no flag, no property, no port and
no reserved surface. `seq` is always supplied by the caller, on every tier, and a message whose
`seq` is wrong is rejected by `UNIQUE (aggregate_id, seq)` at insert.

§3 is different in kind from the rest: it records **measurements of the shipped product**, not a
design. Those facts hold today.

---

## 1. The problem: what `seq` costs an existing application

Adopting Tandem in an application that already has aggregates carries two costs that the write-side
API cannot absorb, both traceable to `seq` being app-assigned (HLD §4.2):

1. **The aggregate must have a version.** An aggregate with no version field has no `seq` source, so
   adoption reaches into the *domain* schema rather than only adding the `tandem_*` tables.
2. **Writes to one aggregate must be serialised.** HLD §4.2 states this as a hard precondition:
   within an aggregate, commit order must equal `seq` order must equal `id` order. An existing
   application that writes an aggregate from several code paths — a service, a batch job, an admin
   action — frequently does not satisfy it, and nothing in the application fails when it does not.

The second cost is the severe one, because its failure mode is silent. §3.1 measures it.

---

## 2. What `seq` actually does today

`seq` is **not** the relay's ordering key. The claim (`JdbcOutboxStore`, LLD-jdbc §3.3) selects
`ORDER BY o.id` and gates on a head-of-chain `NOT EXISTS` over earlier rows of the same aggregate;
`seq` appears in no dispatch query. It has exactly two jobs:

1. **`UNIQUE (aggregate_id, seq)`** — the safety net against a write-side bug, as HLD §4.2 says.
2. **The `seq` CloudEvents extension** (`ce_seq`), for consumers that want the aggregate's own
   sequence number alongside the event.

Job 2 is replaceable: consumers already receive `ce_id` (the outbox `id`), unique by construction and
sufficient for deduplication. Job 1 is not replaceable — it is the only mechanism that makes a
write-side ordering defect *loud* instead of silent.

This matters for §4: auto-assigning `seq` from a source that is not itself serialised would remove
the safety net without replacing what it protects against, which is strictly worse than the status
quo. The design in §4 is worth considering only because the mechanism that assigns the number is
also the mechanism that provides the lock.

---

## 3. Measurements

Both sets of results were established by experiment against a real PostgreSQL 16 and a real Spring
Data JPA / Hibernate domain.

### 3.1 Commit order, not `seq`, decides publication order

Two concurrent transactions write the same aggregate. `seq = 1` is inserted first and so receives the
lower `id`; `seq = 2` is inserted second and commits first.

- The claim issued while the first transaction is still open returns **only the `seq = 2` row**, which
  is therefore published first. The `seq = 1` row is published only after its own commit. Publication
  order is **`[2, 1]`**.
- Neither guard fires. `UNIQUE (aggregate_id, seq)` sees two *different* `seq` values, so it has
  nothing to reject; the head-of-chain `NOT EXISTS` cannot see the earlier row at all, because an
  uncommitted row is invisible to the claim's snapshot.
- **The result is identical at `bucketCount` 1 and 256.** A single bucket serialises the *relay*, not
  the *writes*, and the defect is upstream of the relay. This is structural rather than incidental:
  one aggregate always hashes to one bucket (HLD §4.3), so `B` never affects per-aggregate ordering.
- Control: the same two writes committed in insert order are published `[1, 2]`. The cause is the
  commit order, not the concurrency.

This is pinned by `CommitOrderReorderIT` (`tandem-jdbc`), which runs both the reordering case and the
control at `bucketCount` 1 and 256.

**Consequence for §1's second cost:** an application that does not serialise its writers gets
reordered events with no error, no failed insert, and no signal of any kind. See §6.

### 3.2 A JPA `@Version` is stale on every write-side tier

Hibernate increments `@Version` at flush. Every Tandem write-side tier runs *before* the flush —
inside the caller's transaction, which is the whole point of the outbox — so every tier observes the
pre-increment value. Measured across all three Spring tiers (the application-events tier with
`AbstractAggregateRoot` + `OutboxEventMapper`; the annotation tier with `@TransactionalOutbox` +
`TandemAggregate`; and the Template tier with `TransactionalOutboxTemplate`), the results were
**identical in all three**:

| Scenario | Observed | Outcome |
|---|---|---|
| One state-changing mutation per transaction | tier sees version `N`, flush persists `N+1` | works, but `seq` is always the persisted version minus one |
| Two mutations in one transaction | both mutations see the same pre-flush version | `DuplicateSeqException` — **the business transaction aborts** |
| A mutation that changes no persistent state | Hibernate issues no `UPDATE`, the version does not advance | the next transaction reuses the `seq` → `DuplicateSeqException` |

The plain `OutboxRepository.insert()` tier is not exempt: it fails the same way whenever the caller
passes `entity.getVersion()`.

**The tier is irrelevant.** Choosing a different tier changes how much of the domain Tandem touches;
it does not change where `seq` comes from. These are independent decisions, and only the second one
is affected by this.

The third row is the dangerous one. It is deterministic rather than racy, but it depends on the
*data* — whether a given mutation happens to dirty a persistent field — so it survives a test suite
built on state-changing fixtures and surfaces in production.

---

## 4. The design: a counter that is also the lock

Tandem assigns `seq` from a per-aggregate counter, read and advanced **inside the caller's
transaction**, immediately before the outbox insert:

```sql
INSERT INTO tandem_aggregate_seq (aggregate_id, next_seq) VALUES (?, 1)
    ON CONFLICT (aggregate_id) DO UPDATE SET next_seq = tandem_aggregate_seq.next_seq + 1
    RETURNING next_seq
```

**The number is the lesser half of what this buys.** The row lock this statement takes on
`tandem_aggregate_seq(aggregate_id)` is held for the remainder of the caller's transaction, so any
concurrent transaction writing the same aggregate blocks until the first commits. That makes
`commit order = seq order = id order` a property **Tandem enforces**, rather than a precondition the
application is asked to satisfy and given no way to verify.

Both costs in §1 therefore close together: the aggregate needs no version field, and the
serialisation requirement stops being the adopter's problem. Every failure mode in §3.2 disappears by
construction, because the counter advances **per event** rather than per state change — including the
third row, which no amount of care with `@Version` fully prevents.

### 4.1 Why a table rather than a computed value

`SELECT COALESCE(MAX(seq), 0) + 1 FROM tandem_outbox WHERE aggregate_id = ?` is the obvious
alternative and is wrong twice. Cleanup deletes terminal rows after the retention window (HLD §4.7),
so the counter resets and collides with history already published; and without a lock the race
remains — it merely changes shape, from a silent reorder into a constraint violation. A dedicated
table survives cleanup and carries the lock.

---

## 5. Costs

Stated in full, because they are the reason this is opt-in rather than the default:

- **One extra statement and one row lock on the client's hot path**, inside the business
  transaction — latency added to every write, not to the relay.
- **Concurrent writers to one aggregate become waiters.** For an application that currently tolerates
  concurrent writes on an aggregate, this is a real behavioural change, not a transparent addition.
  It is the *point* of the design, but it must be a decision rather than a surprise.
- **Deadlock risk across aggregates.** A transaction touching several aggregates takes several
  counter locks; two transactions taking them in different orders deadlock. This requires a stated
  lock-ordering rule — take the counters in sorted `aggregate_id` order — as part of the contract,
  not as folklore.
- **`seq` loses its domain meaning.** It becomes Tandem's counter, not the aggregate's version, so a
  consumer can no longer read `ce_seq` as "version 7 of `Order#42`". Acceptable for the intended
  audience, which does not have that version to begin with, but it is a contract change for
  consumers and must not be switched on silently under an existing stream.
- **Unbounded growth.** `tandem_aggregate_seq` holds one row per aggregate, forever; it cannot be
  cleaned up on the outbox's retention schedule without resetting counters (§4.1).

The default stays app-assigned `seq`: no extra table, no extra lock, no added latency — Pareto
(HLD §1.1).

---

## 6. The gap this does not close

Managed `seq` prevents the §3.1 defect for applications that adopt it. It does nothing for
applications that keep app-assigned `seq`, and those remain unable to *detect* a violation: a row
published out of order is indistinguishable from a correctly published one, with no metric, no log
and no counter.

Detection is cheap and independent of everything above. The relay knows the last `seq` it published
per aggregate; a `seq` that goes **backwards** is never legitimate. (A *gap* may be legitimate — not
every event of an aggregate necessarily passes through the outbox — so only the backwards case is a
sound signal.) Surfacing that as a `TandemMetrics` counter would turn the single silent failure mode
in the design into an alertable one, for every deployment rather than only for adopters of this
feature.

---

## 7. Compatibility

Additive throughout, per HLD §1.4:

- **DDL:** a new table in its own optional script, like every other opt-in capability. The baseline
  schema is untouched.
- **Rows:** the `seq` column, its type and its `UNIQUE` constraint are unchanged. A managed `seq` is
  an ordinary `seq` as far as every reader is concerned.
- **Kafka:** `ce_seq` keeps its meaning as "the aggregate's sequence number", with a different
  generator behind it. Consumers that treat it as an opaque monotonic counter are unaffected;
  consumers that join it against the aggregate's persisted version are not, which is why §5 lists it
  as a contract change for an existing stream.
- **API:** no change.
