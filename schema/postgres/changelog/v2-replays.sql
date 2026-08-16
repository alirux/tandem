--liquibase formatted sql

-- Schema version v2 — records on the row how many times an operator has replayed it.
--
-- Everything here has shipped and is IMMUTABLE: an operator's DATABASECHANGELOG already records the
-- checksums, so a later schema change appends a new v<n> file instead of editing this one.

--changeset tandem:v2-add-tandem-outbox-replays stripComments:false
-- A replay rewrites the row's delivery state (status, attempts, lease columns) and leaves nothing
-- behind, so a replayed row has been indistinguishable from one that never was. That mattered twice:
-- the Admin API's own justification is that replay mutates delivery state and therefore needs an
-- audit trail (HLD-admin-api §4), yet the only trace was a log line in a different process; and the
-- relay cannot otherwise tell a legitimate replay from a genuine write-side ordering violation, since
-- the two present identically on the row (HLD §8).
--
-- Counted, not a boolean: "replayed twice" is a materially different story from "replayed once" when
-- reconstructing an incident. Distinct from `attempts`, which a replay resets because it is the
-- delivery budget of the current round — this survives for the lifetime of the row.
--
-- NOT NULL DEFAULT 0 so existing rows read as never-replayed without a backfill, and additive per
-- HLD §1.4: the relay's claim query does not select it, so a relay older than this column keeps
-- working against a migrated database unchanged.
ALTER TABLE tandem_outbox
    ADD COLUMN replays INT NOT NULL DEFAULT 0;
