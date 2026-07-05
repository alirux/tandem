-- Tandem — baseline schema (PostgreSQL)
-- Schema version: v1
--
-- This is the hand-written, versioned baseline DDL the operator applies (Q7 / LLD-jdbc §6).
-- The library does NOT run migrations itself. The schema is a long-lived contract shared by
-- the client write-side, the relay, and the Admin API (possibly at different Tandem versions
-- on the same DB), so it MUST evolve ADDITIVELY only: new optional/nullable columns, new
-- indexes or tables — never a removal, rename, type change, or newly-required column (HLD §1.4).
-- Optional features (attempt archive, causal-ordering clock) ship their own separate DDL and
-- are NOT part of this baseline.
--
-- Targets PostgreSQL 13+. For MySQL, see schema/mysql (pending Q28: partial-index workaround,
-- type mappings). The `bucket` value is computed in Java by tandem-jdbc (engine-independent),
-- so there is no DB-specific bucket/hash function to port.

-- ---------------------------------------------------------------------------
-- Core (always required)
-- ---------------------------------------------------------------------------

CREATE TABLE tandem_outbox (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_id    VARCHAR(255) NOT NULL,
    aggregate_type  VARCHAR(255) NOT NULL,
    type            VARCHAR(255),            -- CloudEvents `type`, e.g. com.acme.order.placed; nullable (Q20)
    bucket          SMALLINT     NOT NULL,   -- virtual bucket = Math.floorMod(fnv1a64(aggregate_id), B); computed in Java by tandem-jdbc at insert (HLD §4.3)
    seq             BIGINT       NOT NULL,
    payload         JSONB        NOT NULL,   -- JSONB by default; switch to BYTEA only if a binary serializer (Avro/Protobuf) is used (HLD §5.2)
    headers         JSONB,
    status          SMALLINT     NOT NULL DEFAULT 0,
    -- 0 = PENDING, 1 = IN_FLIGHT, 2 = DONE, 3 = FAILED, 4 = DISCARDED
    locked_by       VARCHAR(64),
    locked_until    TIMESTAMPTZ,
    attempts        INT          NOT NULL DEFAULT 0,
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (aggregate_id, seq)               -- per-aggregate ordering safety net (HLD §4.2)
);

-- Partial index driving the bucket poll (only PENDING rows), by bucket then id.
CREATE INDEX idx_tandem_outbox_dispatch
    ON tandem_outbox (bucket, id)
    WHERE status = 0;

-- Supports the head-of-chain / poison-gate NOT EXISTS check per aggregate (HLD §6; LLD-jdbc §3.3).
CREATE INDEX idx_tandem_outbox_aggregate
    ON tandem_outbox (aggregate_id, id)
    WHERE status IN (0, 1, 3);

-- Supports the periodic lease-reclaim (LLD-jdbc §3.5): "status = 1 AND locked_until < now()", run
-- every reclaimInterval (~5s) on every instance. Partial on IN_FLIGHT only, so it stays tiny (in-flight
-- rows are transient and few) and the reclaim scans expired leases, not the whole table — which matters
-- once DONE rows accumulate between cleanup passes on a large, high-throughput outbox.
CREATE INDEX idx_tandem_outbox_inflight
    ON tandem_outbox (locked_until)
    WHERE status = 1;

-- ---------------------------------------------------------------------------
-- LEASE coordination mode only (multi-instance)
--
-- The SINGLE coordination mode (one relay instance owns all buckets in-process) does NOT need
-- these tables. Create them only under LEASE — a horizontally-scaled client with an embedded
-- relay, or one or more standalone relay processes (LLD-jdbc §1/§3.2).
--
-- B (virtual bucket count) is fixed at first deploy and IMMUTABLE thereafter (B5): changing it
-- re-maps aggregates across buckets and would split an aggregate's events across workers. The
-- seed below matches the default B = 256 (buckets 0..255); if you configure a different B, seed
-- exactly that many rows and keep the config and seed in sync.
-- ---------------------------------------------------------------------------

CREATE TABLE tandem_bucket_lease (
    bucket       SMALLINT     PRIMARY KEY,   -- 0 .. B-1
    owner        VARCHAR(64),                -- worker id; NULL = free
    lease_until  TIMESTAMPTZ,                -- ownership expiry; renewed on heartbeat
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Seed one row per virtual bucket (default B = 256 → 0..255).
INSERT INTO tandem_bucket_lease (bucket)
SELECT generate_series(0, 255)::smallint;

-- Relay-instance membership (presence), decoupled from bucket ownership (LLD-jdbc §3.2). One row per
-- live relay instance, renewed each heartbeat. Its purpose is fair-share correctness: an instance that
-- currently owns ZERO buckets has no row in tandem_bucket_lease and would otherwise be invisible to
-- peers' live-owner count, so an incumbent holding every bucket would never learn a newcomer exists and
-- never release its fair share (a stable scale-up starvation). Counting live members here instead makes
-- a zero-owned joiner visible, so the incumbent releases and the fleet rebalances. A dead instance's row
-- simply expires (and is pruned on the next heartbeat). Not seeded — instances self-register at runtime.
CREATE TABLE tandem_relay_member (
    owner        VARCHAR(64)  PRIMARY KEY,   -- matches tandem_bucket_lease.owner
    lease_until  TIMESTAMPTZ  NOT NULL,      -- presence expiry; renewed on heartbeat
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
