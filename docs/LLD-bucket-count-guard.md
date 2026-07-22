# Tandem — LLD: bucket-count consistency guard (`tandem-core` + `tandem-jdbc`)

**Version:** 0.2
**Status:** Proposed
**Companion to:** [HLD.md](HLD.md) §4.3; [LLD-jdbc.md](LLD-jdbc.md); consumed by [LLD-spring.md](LLD-spring.md) §2.1

A guard that makes a divergent `bucketCount` between the write-side and the relay impossible to miss.
It is a core-adapter concern, not a Spring one: it protects the manual assembly path Tandem ships
today, and every higher-level integration (`tandem-spring-producer` / `tandem-spring-relay`) inherits
it by wiring the same components. The design follows the project's hexagonal rule — a pure decision
**strategy** and a storage **port** in `tandem-core`, a JDBC **adapter** in `tandem-jdbc` — so the
algorithm is reimplementable and every side effect is isolated behind the port.

---

## 1. The hazard

`bucketCount` is consumed by two independent sides:

- the **write-side**, `JdbcOutboxRepository(dataSource, bucketCount)`, which computes each row's
  `bucket` at INSERT time (HLD §4.3);
- the **relay**, `RelayConfig.bucketCount`, which determines the bucket range workers poll and own.

The value must be identical on both sides and must never change after first deployment — it is baked
into every stored row's `bucket` column. Under the split deployment topology these two sides are
usually **two separate processes with separate configuration**, so nothing structurally forces them
to agree.

**Failure shape.** If the write-side inserts with `256` while the relay polls `512`, every new row
lands in a bucket no worker owns. Nothing throws. Rows accumulate as `PENDING` and delivery silently
stops — a stable, silent equilibrium. This is the same class of failure as the bucket-lease
starvation found under load testing, and the same conclusion applies: **detect it structurally
instead of documenting it.** A comment in a config file is not a match for a failure mode that
produces no error and no log.

---

## 2. Architecture (hexagonal)

Three collaborators, split along the core/port/adapter boundary the project already uses for
`OutboxStore` and `BucketSource`:

| Collaborator | Kind | Module | Responsibility |
|---|---|---|---|
| `BucketCountReconciliation` | **strategy** (pure) | `tandem-core` | Decide, from the stored value and the configured value, what to do. No I/O. Reimplementable. |
| `BucketCountStore` | **port** (side-effect boundary) | `tandem-core` | Read and atomically seed the stored value. The *only* interface that performs I/O. |
| `JdbcBucketCountStore` | **adapter** | `tandem-jdbc` | Implements the port over the metadata table; owns the DB-specific SQL and the atomic seed. |
| `BucketCountGuard` | **orchestrator** | `tandem-jdbc` | Wires the port to the strategy; performs the side effect the strategy selects. Thin — no algorithm of its own. |

The dependency arrows point inward only: the adapter and orchestrator depend on the core strategy and
port; the core never depends on JDBC. The strategy is a pure function of two values, so it is
unit-testable with no database; the port confines every read, write and the atomic seed to one place;
the orchestrator holds no policy — it asks the strategy what to do and uses the port to do it.

```
        tandem-core (pure)                         tandem-jdbc (adapter)
  ┌──────────────────────────────┐        ┌──────────────────────────────────┐
  │ BucketCountReconciliation     │◀───────│ BucketCountGuard (orchestrator)   │
  │   decide(stored, configured)  │        │   read → decide → act             │
  │        → Decision             │        │                                   │
  │                               │        │            uses                   │
  │ BucketCountStore  (port)      │◀───────│ JdbcBucketCountStore (adapter)    │
  │   read() / seedIfAbsent()     │        │   INSERT … ON CONFLICT DO NOTHING │
  └──────────────────────────────┘        └──────────────────────────────────┘
```

---

## 3. The reconciliation strategy (pure algorithm)

The algorithm is a pure function `(OptionalInt stored, int configured) → Decision`, with no I/O, so
that *what to do* is separable from *doing it* and can be reimplemented without touching storage.

```java
// tandem-core
public interface BucketCountReconciliation {
    Decision decide(OptionalInt stored, int configured);

    enum Kind { SEED, PROCEED, CONFLICT }
    record Decision(Kind kind, String message) { /* message set only for CONFLICT */ }

    /** The default policy (§3.1). */
    static BucketCountReconciliation seedOrValidate() { … }
}
```

### 3.1 Default policy — `seedOrValidate`

| `stored` | vs. `configured` | `Decision` | Meaning |
|---|---|---|---|
| absent | — | `SEED` | First initialisation (or upgrade from a pre-guard database). |
| present | equal | `PROCEED` | Both sides agree. |
| present | different | `CONFLICT` | Fail fast. The message names both values, which side is misconfigured, and that the stored value cannot change after first deployment. |

This is a pure decision table — it does no seeding itself; it *selects* `SEED` and lets the
orchestrator perform it via the port (§4). That separation is what keeps the side effect out of the
algorithm.

### 3.2 Why a strategy

The policy is the one part a future requirement is most likely to change, and each alternative is a
different pure function over the same two inputs — the textbook case for the pattern:

- a **strict** policy that returns `CONFLICT` when `stored` is absent (refuse to auto-seed; require an
  explicit provisioning step);
- a **warn-only** policy for a controlled rollout, logging the mismatch instead of throwing.

Selection mirrors `BucketSource.forCoordination`: a factory picks the implementation, defaulting to
`seedOrValidate()`, and an assembler may pass a different one. The default is the only one shipped in
this increment.

**Re-sharding is deliberately *not* one of these policies.** Treating a mismatch as an intentional
change of `bucketCount` is a feature in its own right — it means re-bucketing already-stored rows and
coordinating the cut-over across every write-side and relay instance, none of which a startup
reconciliation strategy can do safely. It is out of scope for this guard and is recorded as a separate
future topic (§8); the guard's job is strictly to *detect* divergence, never to accept it.

---

## 4. Side-effect isolation — the storage port

All I/O lives behind one port, so the strategy and the orchestration logic are storage-agnostic and
the atomic concurrency handling has exactly one home:

```java
// tandem-core
public interface BucketCountStore {
    /** The stored bucket count, or empty if none has been established yet. */
    OptionalInt read();

    /**
     * Atomically store {@code candidate} only if no value exists yet, then return the value now in
     * effect — {@code candidate} if this call established it, or the pre-existing value if another
     * process won the race. Never overwrites an existing value.
     */
    int seedIfAbsent(int candidate);
}
```

`seedIfAbsent` is the single primitive that must be atomic; isolating it here means neither the
strategy nor the orchestrator contains DB-specific or race-sensitive code. The JDBC adapter
implements it with `INSERT … ON CONFLICT DO NOTHING` followed by a read (or `RETURNING` where
available), so a concurrent first-start collapses to one persisted value.

### 4.1 Orchestration

`BucketCountGuard` is the only collaborator that both touches the port and acts on a verdict, and it
holds no policy of its own:

1. `stored = store.read()`
2. `decision = reconciliation.decide(stored, configured)`
3. dispatch on `decision.kind()`:
   - `PROCEED` → return.
   - `CONFLICT` → throw `TandemConfigurationException(decision.message())` before any traffic is served.
   - `SEED` → `effective = store.seedIfAbsent(configured)`; then **re-decide**
     `reconciliation.decide(OptionalInt.of(effective), configured)` — which is `PROCEED` if this
     process seeded, or `CONFLICT` if a concurrent process seeded a *different* value. This second
     pass is what closes the race: `seedIfAbsent` guarantees a single persisted value, and re-deciding
     against it guarantees the loser of a mismatched race still fails loudly. There is no window in
     which a mismatch is accepted.

The guard runs once per side, at construction/startup, on the `DataSource` already in hand — one
trivial read (plus, on first init only, one seed) and nothing on the hot path.

---

## 5. Storage / schema

A single stored value keyed by name, additive to the baseline schema — a new metadata row/table only,
with **no change to `tandem_outbox`**. The natural shape is a small key/value metadata table
(`tandem_meta(key TEXT PRIMARY KEY, value TEXT)`) holding `bucket_count`, which also gives later
cross-cutting settings a home without another migration. The exact DDL is settled when this lands and
is mirrored into `schema/postgres/tandem-baseline.sql` and LLD-jdbc; the MySQL equivalent follows the
same shape under Q28.

Whichever shape is chosen, it must not reintroduce a per-DB hash or any value that changes across DB
major-version upgrades — the same constraint the `bucket` computation already honours ("stable across
DB major-version upgrades", LLD-jdbc §-). The `value` is stored as text and parsed by the adapter, so
the metadata table is not typed to this one setting.

---

## 6. Compatibility

Databases provisioned by an earlier Tandem version (≤ 0.1.1) have no stored value. The default policy
returns `SEED`, so the guard **seeds** the value on first start under the new version rather than
rejecting the database: the first component to start writes the current `bucketCount` — which, for an
existing deployment, is by definition the value already baked into its rows — and every later start
validates against it. Upgrading is a no-op.

An older Tandem instance that predates the metadata row keeps working unchanged against a schema that
has it: it never reads the row, and the extra table is inert to it. This is the project's
backward-*and*-forward compatibility rule applied literally — every contract stays readable by both an
older and a newer peer, and readers tolerate what they do not recognise. The metadata table is
therefore safe to add in a minor release.

---

## 7. Placement / wiring

The port and strategy live in `tandem-core` (dependency-free, already a dependency of `tandem-jdbc`);
the adapter and orchestrator live in `tandem-jdbc` and the baseline schema — **not** in any Spring
autoconfiguration. The manual assembly path (`new JdbcOutboxRepository(...)` + `new WorkerPool(...)`)
is the one Tandem ships and documents today; a guard that lived in the Spring layer would leave the
primary path unprotected. The write-side runs the guard inside `JdbcOutboxRepository` construction;
the relay runs it at `WorkerPool` start, alongside the existing row-lease invariant check (LLD-jdbc
§3.5), so both fail-fast paths sit together. `tandem-spring-producer` and `tandem-spring-relay`
acquire the guard purely by constructing these same components — and, because selection is a factory,
an assembler that needs a non-default policy passes one without the guard changing.

---

## 8. Open points

- **Exact DDL and table name** — `tandem_meta` key/value vs. a dedicated single-column table; settled
  when implemented and mirrored into the baseline schema + LLD-jdbc.
- **Re-sharding (separate future feature).** Changing `bucketCount` after first deployment is a
  distinct feature to be designed on its own, **not** a variant of this guard or a reconciliation
  policy. It requires re-bucketing already-stored rows and a coordinated cut-over across all
  write-side and relay instances (candidate approaches: drain-and-re-bucket, or dual-write during a
  migration window) — analysis deferred. Until that feature exists, `bucketCount` is fixed at first
  deployment and this guard only ever *detects* a mismatch, never accepts one.
- **MySQL** — the metadata table's MySQL DDL rides along with Q28.
- **Reuse for other cross-cutting settings** — once `tandem_meta` and `BucketCountStore` exist, a
  more general `MetadataStore` port could back other shared values; deferred until a second use case
  actually appears (avoid speculative generality).
