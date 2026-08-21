# Tandem — Managed `seq` (Design Note)

**Version:** 1.1  
**Status:** Draft — **not built, not decided.** §6 records shipped behaviour that weakens the case for
building §4 at all; read §0 first.  
**Companion to:** [HLD.md](HLD.md) §4.2 (Ordering Established at Write Time)

**This document is the design.** [HLD §4.2](HLD.md) states the shipped contract — `seq` is
app-assigned, from the aggregate's `version`, and Tandem never generates it — and that contract is
unchanged by anything written here. This note records what that contract costs an existing
application, the measurements behind that claim, and an **opt-in** alternative in which Tandem
assigns `seq` itself.

---

## 0. Implementation status — read this first

**The design in §4 is not built.** There is no sequence, no lock, no flag, no property, no port and no
reserved surface. `seq` is always supplied by the caller, on every tier, and a message whose `seq` is
wrong is rejected by `UNIQUE (aggregate_id, seq)` at insert.

Two sections are different in kind and describe the **shipped product**, not a design: §3 records
measurements, and §6 the write-side ordering detector — which materially weakens the case for §4.
Read §6 before acting on anything in §4.

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
sufficient for deduplication.

**Job 1 is worth looking at precisely, because what the constraint actually catches is narrower than
"ordering defects".** `UNIQUE (aggregate_id, seq)` fires only on a **duplicate** `seq` — and §3.1
measures that the reorder hazard produces two *different* `seq` values, so the constraint never sees
it. The two guards cover disjoint failures:

| Failure | `UNIQUE (aggregate_id, seq)` | Detector (§6) |
|---|---|---|
| Two events given the **same** `seq` (a stale `@Version`, §3.2) | catches it — the business transaction aborts | does not see it: the rows publish in `id` order |
| Concurrent writers, **distinct** `seq`, commits inverted (§3.1) | **never fires** | catches it, subject to §6's limits |

This matters for §4, and it is easy to get wrong: auto-assigning `seq` looks like it removes the
safety net without replacing what it protects against. It does not. The constraint protects against
the *duplicate* case — which is exactly the `@Version` staleness that a Tandem-assigned `seq`
**eliminates by construction**, since a monotonic source cannot hand out the same number twice.
Auto-assignment does not leave the net's job undone; it removes the job. What remains for the
constraint is guarding against a bug in Tandem's own assignment: weaker, but not nothing.

So **the number and the lock do not have to be bundled for either to be worth having.** They are
separate problems with separate mechanisms, and §4 treats them separately.

---

## 3. Measurements

Both sets of results were established by experiment against a real PostgreSQL 16 and a real Spring
Data JPA / Hibernate domain.

### 3.1 Commit order, not `seq`, decides publication order

Two concurrent transactions write the same aggregate. `seq = 1` is inserted first and so receives the
lower `id`; `seq = 2` is inserted second and commits first.

- **The claim** — the relay's own poll query, which selects and locks the next batch of rows to publish
  (§2 above; `JdbcOutboxStore`, LLD-jdbc §3.3) — **issued while the first transaction is still open**
  returns **only the `seq = 2` row**. Not because the query is wrong: `seq = 1`'s transaction has not
  committed yet, and an uncommitted row is invisible to any other session's snapshot under ordinary
  PostgreSQL transaction isolation (MVCC) — the same rule that keeps a `SELECT` in one session from
  seeing another session's in-progress, unflushed writes. The relay is not special-cased here; it sees
  exactly what any other query would see at that instant. `seq = 2` is therefore published first. The
  `seq = 1` row only becomes visible — and only then claimable — once its own transaction commits.
  Publication order is **`[2, 1]`**.
- Neither guard fires. `UNIQUE (aggregate_id, seq)` sees two *different* `seq` values, so it has
  nothing to reject; the head-of-chain `NOT EXISTS` cannot see the earlier row at all, for the same
  MVCC reason — an uncommitted row is invisible to the claim's snapshot.
- **The result is identical at `bucketCount` 1 and 256.** A single bucket serialises the *relay*, not
  the *writes*, and the defect is upstream of the relay. This is structural rather than incidental:
  one aggregate always hashes to one bucket (HLD §4.3), so `B` never affects per-aggregate ordering.
- Control: the same two writes committed in insert order are published `[1, 2]`. The cause is the
  commit order, not the concurrency.

This is pinned by `CommitOrderReorderIT` (`tandem-jdbc`), which runs both the reordering case and the
control at `bucketCount` 1 and 256.

**Consequence for §1's second cost:** an application that does not serialise its writers gets
reordered events with no error and no failed insert — nothing in the data, and nothing the caller can
observe. The relay does report it as it publishes; see §6 for what that signal covers and what it
does not.

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

### 3.3 Whether the aggregate's own lock serialises writers at all depends on flush order

§3.1 shows the reorder with no domain table in the picture. A real JPA application has one — an
`UPDATE` on the aggregate row, guarded by whatever lock the domain uses — so it is natural to expect
that row lock to already prevent the reorder. It does not, by default, and the reason is the same
flush timing as §3.2: Hibernate defers the `UPDATE` to flush, and every write-side tier runs *before*
flush, so **the outbox row is inserted before the lock is ever taken**.

Two transactions, `T1` first to insert (lower `id`), `T2` first to commit — the same shape as §3.1,
now with a domain mutation ahead of the outbox insert:

```
-- No explicit flush: the domain UPDATE and the lock both happen at commit, after the insert.
T1: begin; UPDATE order SET status=? WHERE id=?   -- deferred by the ORM to flush/commit, not yet issued
T1: INSERT INTO tandem_outbox (seq=1, ...)         -- issued immediately, no lock held
                                                     -- (T1 left open)
T2: begin; UPDATE order SET status=? WHERE id=?    -- no lock from T1 exists yet — proceeds
T2: INSERT INTO tandem_outbox (seq=2, ...)
T2: commit                                          -- T2's UPDATE flushes and commits first
T1: commit                                          -- T1's UPDATE flushes now, too late
                                                     -- published: [2, 1] — same reorder as §3.1

-- With an explicit flush before the outbox insert: the UPDATE, and its lock, come first.
T1: begin; UPDATE order SET status=? WHERE id=?; flush   -- lock taken now, held to commit
T1: INSERT INTO tandem_outbox (seq=1, ...)
                                                     -- (T1 left open, holding the row lock)
T2: begin; UPDATE order SET status=? WHERE id=?; flush   -- BLOCKS on T1's row lock
T1: commit                                          -- releases the lock
T2: (unblocks) INSERT INTO tandem_outbox (seq=2, ...); commit
                                                     -- published: [1, 2] — in order
```

Measured against a real Hibernate domain, three shapes of writer:

| Writer touches | No explicit flush | Explicit flush before the outbox insert |
|---|---|---|
| The aggregate row itself, with `@Version` | reorders (`T1`'s commit fails its optimistic check instead — a lost event, not a reorder) | serialises: `T2` blocks on `T1`'s row lock |
| The aggregate row itself, no version | reorders | serialises |
| Only a *child* of the aggregate (e.g. an order line), never the root | reorders | **still reorders — no shared row to lock** |

**The explicit flush recommended in §3.2 for the stale `seq` is therefore doing two jobs, not one**: it
is also what makes the aggregate's write lock real. Skipping it does not make `@Version` merely
ineffective at protecting order — with a version, the *earlier* writer's commit fails outright,
discarding that transaction's outbox row with it.

**The flush does not help when there is no shared row.** Writers that only insert children of the
aggregate reorder regardless, because there is nothing for either transaction to lock. That case needs
its own serialisation — an explicit `SELECT … FOR UPDATE` on the aggregate row, or an aggregate
boundary drawn so concurrent writers meet on one row — which no `seq` source and no tier substitutes
for.

**Not a Tandem defect — and not something a CDC-based outbox avoids either.** The Hibernate
flush-timing fact above is general and holds for any JPA application, with or without an outbox. It is
tempting to think a log-tailing design (Debezium reading the WAL, [comparison.md](comparison.md) §2)
sidesteps §3.1's reorder, since it reads the database's own commit order instead of reconstructing one
from `id`. It does not: commit order **is** what produces the reorder in the first place. Logical
decoding emits a transaction only once its `COMMIT` record is read, in the order those `COMMIT`
records appear in the WAL — so if `T2` (seq 2) commits before `T1` (seq 1), a WAL tailer emits `T2`'s
change first for exactly the same reason the claim does: that is, factually, what happened first. No
downstream reader — polling or log-tailing — can recover an order the write side never established
in the first place, because once two writers race with no lock between them, there is no single
"correct" order left to recover; whichever commits first *is* first, for anyone watching.

The precondition in HLD §4.2 is therefore not a gap particular to this library, or to the poll-vs-CDC
choice: **any** consumer of committed database state — Tandem's relay, a hand-rolled poll-and-claim
publisher, or a CDC pipeline — depends on the write side actually serialising per-aggregate writers.
What CDC changes is unrelated to this hazard: it removes Tandem's *own* approximation of order (`id`
plus polling visibility, §2 above) in favour of reading the database's serialization history directly
— which is a real strength ([comparison.md](comparison.md) §2: "Debezium never has to reconstruct
ordering — it reads it") against Tandem's polling mechanics, not against an unserialised write side.

**And that approximation has a sharper consequence than "the relay is a step behind": whether the
same violation is even *visible* in the published order depends on poll timing, not on whether it
happened.** The head-of-chain gate (`NOT EXISTS ... e.id < o.id AND e.status IN (0,1,3)`) enforces
strict ascending `id` per aggregate **whenever a claim sees both rows already committed together** — a
later `id` is held back until the earlier one is `DONE`, regardless of which one actually committed
first. The reorder in §3.1 is only visible because the claim happened to run **inside** the narrow
window between the two commits, catching only the later row. The identical write-side race, with the
same two commits in the same order, produces no visible reorder at all if the next claim instead runs
**after both have committed**:

| | `T2` commits, then `T1` commits (identical in both) | Relay poll | Published |
|---|---|---|---|
| Timeline A (measured, §3.1) | `T2` at 8ms, `T1` at 40ms | runs at 10ms — sees only `T2` | `[seq 2, seq 1]` — reorder visible, counter +1 |
| Timeline B (same race) | `T2` at 8ms, `T1` at 15ms | next poll runs at 100ms — sees both, already committed | `[seq 1, seq 2]` — head-of-chain gate enforces order; **counter stays 0** |

The write-side defect is byte-for-byte identical in both rows of the table. A WAL tailer would show
`[2, 1]` in **both** timelines — it processes each `COMMIT` record as it appears in the log, with no
polling cadence to land inside or outside of. Tandem's detector, by construction, only fires when a
claim's timing happens to fall inside the race window — narrower than one `pollInterval` in practice,
so a race that resolves faster than that is invisible **by construction**, not by bad luck or a lost
watermark. This is in addition to, and structurally more common than, the watermark-eviction and
relay-restart causes already noted in [HLD.md](HLD.md) §7 — a zero reading is evidence of nothing.

---

## 4. The design: two independent halves, neither of which persists state per aggregate

§1 lists two costs and they are **separate problems**, solved by separate mechanisms:

| | Problem | Mechanism | Persistent state |
|---|---|---|---|
| **4.1** | The aggregate has no `version` to take `seq` from | a `SEQUENCE` as the column default | one catalog row, fixed |
| **4.2** | Concurrent writers to one aggregate are not serialised | `pg_advisory_xact_lock` in the caller's transaction | **none** — shared memory, released at commit |

They ship behind one opt-in switch (§4.3 says why one and not two), but they are independent: either
can be reasoned about, and if necessary built, without the other.

> **§4.4 records the obvious alternative and why it is rejected** — a `tandem_aggregate_seq` table
> holding one counter row per aggregate, solving both halves with a single `INSERT … ON CONFLICT`
> whose row lock also does the serialising. It is the design anyone reaches for first, so the reason
> against it is worth having written down.

### 4.1 The number: a sequence as the column default

```sql
CREATE SEQUENCE tandem_seq;   -- CACHE 1 (the default) is mandatory, see below
ALTER TABLE tandem_outbox ALTER COLUMN seq SET DEFAULT nextval('tandem_seq');
```

In managed mode the write side simply **omits `seq` from the INSERT**; the default supplies it. That
is the whole mechanism — no extra statement, no extra round trip, no lock. `seq` stops being dense
per aggregate (`Order#42`'s first event might be `8347283`), which §5 addresses.

Purely additive: `seq` is `NOT NULL` with no default today, so **nothing can currently omit the
column** — adding a default changes the behaviour of no existing writer.

Three constraints on it:

- **`CACHE` must stay 1.** With a per-session cache, session A pre-allocates 1–100 and session B
  101–200; if B inserts first, `seq` moves backwards for that aggregate and the §6 detector reports
  false regressions. PostgreSQL's default is `CACHE 1`; it must be pinned in the changelog, not left
  to whoever creates the sequence.
- **Turning it on over existing data needs `START WITH` above the highest `seq` already emitted**,
  or new values fall below history already published for the same aggregates. The app-assigned →
  managed direction is safe once; the reverse is not.
- **PostgreSQL only.** MySQL has no `SEQUENCE` object (MariaDB does), so the MySQL port (backlog
  item 7) needs its own answer here. Acceptable while this is opt-in.

> **Managed and app-assigned `seq` can coexist per aggregate, and this needs no mechanism to allow —
> it is a property of the design.** The default fires only when an INSERT *omits* the column, so the
> granularity is already per-message: bind `seq` and it is app-assigned, omit it and Tandem assigns it.
> Nothing has to be configured, and the sequence knows nothing about aggregate types.
>
> It is sound because **no invariant in this design spans aggregates**: `seq`, `UNIQUE (aggregate_id,
> seq)`, the §6 watermark and the §4.2 lock are all per-aggregate, so `Order` on a generated `seq`
> (`8347283`, `8347291`, …) and `Customer` on its `@Version` (`1`, `2`, `3`, …) never meet in any
> comparison.
>
> The consequence worth planning for is **migration**, since §5 makes switching a consumer's `ce_seq`
> meaning a contract change: a per-aggregate-type rollout lets managed mode be turned on for a *new*
> aggregate type, when no consumer yet reads its `ce_seq` as a domain version, leaving existing streams
> untouched indefinitely. That is a gentler adoption path than a single global switch.
>
> **What this must not become is an implicit rule.** "No `seq` supplied ⇒ managed" would be elegant and
> is the wrong default: `OutboxMessage.seq` is a primitive `long`, so a caller who simply *forgets* to
> set it passes `0` and today fails loudly on `UNIQUE (aggregate_id, seq)` at the second event. Under an
> implicit rule that same mistake would silently switch the aggregate's wire contract instead. Whatever
> selects the mode has to be explicit at the call site, and per-aggregate-type configuration would be a
> second place to state what the call site already decides — a source of drift, not of control.

### 4.2 The order: a transaction-scoped advisory lock

```sql
SELECT pg_advisory_xact_lock(hashtext(?));   -- ? = aggregate_id, before the outbox insert
```

Held for the remainder of the caller's transaction and released automatically at commit or rollback,
so a concurrent transaction writing the same aggregate blocks until the first commits. That makes
`commit order = seq order = id order` a property **Tandem enforces**, rather than a precondition the
application is asked to satisfy and given no way to verify.

**Same guarantee as the rejected counter row (§4.4), no persistent state.** Both are a lock held to
commit; the difference is only where the lock lives — a table row that must exist forever, versus
shared memory that cleans itself up.

Two properties to state rather than discover: `hashtext` collisions make two unrelated aggregates
occasionally serialise (a throughput effect, never a correctness one — narrow it with the two-key
`pg_advisory_xact_lock(int4, int4)` form if it matters), and this is PostgreSQL-specific in a stronger
sense than §4.1 — MySQL's `GET_LOCK` is **session**-scoped, not transaction-scoped, so it does not
transfer.

### 4.3 Why one switch and not two

The two halves look independently useful, and §3.3 shows the audience for §4.2 is narrower than it
first appears: an application that updates the aggregate root gets the serialisation **for free** from
an explicit flush, no Tandem lock required. But §3.3 also shows who is left — writers that only touch
*children* of the aggregate, who reorder even with a flush — and that is the same population that has
**no aggregate `version` to take `seq` from**, since nothing is bumping a version on the root. The two
audiences largely coincide, so splitting them into two knobs would ask adopters to reason about a
distinction that mostly does not exist for them. One switch, both halves.

"One switch" is about the two halves travelling together, not about its granularity: per §4.1 the
choice is made per write, so it can be applied to one aggregate type and not another.

### 4.4 Rejected: a counter table (`tandem_aggregate_seq`)

The natural alternative assigns `seq` from a dedicated **table**, one counter row per aggregate:

```sql
INSERT INTO tandem_aggregate_seq (aggregate_id, next_seq) VALUES (?, 1)
    ON CONFLICT (aggregate_id) DO UPDATE SET next_seq = tandem_aggregate_seq.next_seq + 1
    RETURNING next_seq
```

Elegant in one respect — the row lock it takes on `tandem_aggregate_seq(aggregate_id)` *is* the
serialisation, so one statement buys both halves — and it keeps `seq` dense per aggregate, which
§4.1 gives up.

**Rejected on unbounded growth.** The table holds one row per aggregate **forever**. It cannot be
pruned on the outbox's retention schedule: deleting `Order#42`'s counter makes its next event restart
at `1`, colliding with history already published to Kafka months earlier — the same argument that
rules out computing `MAX(seq) + 1` from `tandem_outbox` itself, whose rows the cleanup does delete
(HLD §4.7). So the table grows monotonically with the *cumulative cardinality of aggregates*, never
with anything that shrinks. Negligible for low-cardinality domains (accounts, products); for
per-order, per-session or per-request aggregates it becomes larger than the outbox itself, permanently
and unprunably, in the adopter's production database.

Since §4.1 + §4.2 deliver the same two guarantees with fixed state and no state respectively, the
table is dominated: it costs a permanent unprunable table and buys only the denseness of `seq`.

---

## 5. Costs

Stated in full, because they are the reason this is opt-in rather than the default:

- **One extra statement on the client's hot path — the lock (§4.2), not the number.** §4.1 costs
  nothing: the default is evaluated inside the INSERT that already happens. Only §4.2 adds a round
  trip, inside the business transaction, so the latency lands on every write rather than on the relay.
- **Concurrent writers to one aggregate become waiters.** For an application that currently tolerates
  concurrent writes on an aggregate, this is a real behavioural change, not a transparent addition.
  It is the *point* of §4.2, but it must be a decision rather than a surprise.
- **Deadlock risk across aggregates.** A transaction touching several aggregates takes several
  advisory locks; two transactions taking them in different orders deadlock. This requires a stated
  lock-ordering rule — take them in sorted `aggregate_id` order — as part of the contract, not as
  folklore. (PostgreSQL detects and breaks the deadlock, so the failure is loud, not a hang.)
- **`seq` loses its domain meaning, and its denseness.** It becomes Tandem's counter, not the
  aggregate's version, so a consumer can no longer read `ce_seq` as "version 7 of `Order#42`" — and
  with §4.1 it is not even `7`, it is a large global number with gaps. Acceptable for the intended
  audience, which has no such version to begin with, but it is a contract change for consumers and
  must not be switched on silently under an existing stream.
- **PostgreSQL only**, both halves, for different reasons (§4.1, §4.2). The MySQL port (backlog
  item 7) would need its own mechanism for each.

**Not on this list, deliberately:** unbounded storage growth. §4.1 holds one catalog row and §4.2
holds nothing at all — which is the whole reason the counter table is rejected (§4.4).

The default stays app-assigned `seq`: no sequence, no lock, no added latency — Pareto (HLD §1.1).

---

## 6. Detection is shipped, and it is why §4 is a weak proposition

Managed `seq` would *prevent* the §3.1 defect for applications that adopt it. **Detecting** the same
defect is cheap, independent of everything above, and helps every deployment rather than only adopters
of this feature — which is why it is what got built.

The relay knows the last `seq` it published per aggregate; a `seq` that goes **backwards** is never
legitimate. (A *gap* may be legitimate — not every event of an aggregate necessarily passes through
the outbox — so only the backwards case is a sound signal.) That is
`tandem.outbox.seq_regression.count`, specified in [HLD.md](HLD.md) §7 and §8: an in-process check at
publish time, because the rows are left in the table in perfect order and no later query can find the
violation. Operator replays are excluded at the source via the row's `replays` count, so a non-zero
value always means writers to one aggregate are not serialised.

**It under-reports and never over-reports.** The watermarks are bounded (an LRU per worker) and live
only in memory, so a relay restart or an eviction loses them. A non-zero reading is therefore always
a real violation, while zero is not proof of absence.

**This is what makes §4 a weak proposition.** The strongest argument for it would be §1's — that the
second cost is severe *because its failure mode is silent*. That argument is not available: the
failure mode is reported, imperfectly but alertably. What §4 still offers is the difference between
detecting a violated precondition and enforcing it — a real difference, but a much smaller one than
"the only way to know". §4 has to be justified on prevention alone.

---

## 7. Compatibility

Additive throughout, per HLD §1.4:

- **DDL:** a `CREATE SEQUENCE` plus a `DEFAULT` on an existing column (§4.1) — no new table. Strictly
  additive: `seq` is `NOT NULL` with no default today, so no existing writer can omit it, and adding a
  default changes nothing for anyone already binding the column. **Where it lives needs deciding**: the
  schema's source of truth is the Liquibase changelog (LLD-jdbc §6, append-only, with a generated flat
  baseline under a CI drift gate), which leaves no clean place for an optional script outside it.
  Either this becomes a changelog version every adopter applies (harmless: an unused sequence and a
  default nothing triggers), or the changelog gains a genuinely optional context, which has no
  precedent here.
- **Rows:** the `seq` column, its type and its `UNIQUE` constraint are unchanged. A managed `seq` is
  an ordinary `seq` as far as every reader is concerned — larger and sparser (§4.1), but readers
  treat it as opaque.
- **Kafka:** `ce_seq` keeps its meaning as "the aggregate's sequence number", with a different
  generator behind it. Consumers that treat it as an opaque monotonic counter are unaffected;
  consumers that join it against the aggregate's persisted version are not, which is why §5 lists it
  as a contract change for an existing stream.
- **API:** no change.
