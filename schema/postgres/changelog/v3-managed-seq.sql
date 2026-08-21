--liquibase formatted sql

-- Schema version v3 — lets Tandem assign `seq` instead of the application (HLD-managed-seq §4.1).
--
-- Everything here has shipped and is IMMUTABLE: an operator's DATABASECHANGELOG already records the
-- checksums, so a later schema change appends a new v<n> file instead of editing this one.

--changeset tandem:v3-create-tandem-seq stripComments:false
-- An aggregate with no version field has no source for `seq`, so adopting Tandem otherwise reaches
-- into the application's own domain schema rather than only adding the `tandem_*` tables
-- (HLD-managed-seq §1). This sequence is that source: in managed mode the write side omits the
-- column and the DEFAULT below supplies the value, so there is no extra statement, no round trip and
-- no lock on the caller's hot path.
--
-- CACHE 1 is a correctness constraint, not a performance default, and is stated explicitly rather
-- than inherited: with a per-session cache two sessions pre-allocate disjoint ranges (1-100,
-- 101-200), so a later-committing session can emit a LOWER `seq` for the same aggregate — which the
-- relay's seq-regression detector (HLD §7, §8) reports as a write-side ordering violation that never
-- happened.
--
-- The sequence is deliberately NOT tied to the column with OWNED BY: its lifecycle stays independent
-- of `tandem_outbox`.
--
-- START WITH 1 is right for a database whose aggregate types are all new to managed mode. Switching
-- an aggregate type that has ALREADY emitted app-assigned values needs the sequence moved above that
-- type's maximum (`ALTER SEQUENCE tandem_seq RESTART WITH ...`) or the next event collides with
-- UNIQUE (aggregate_id, seq). That belongs to enabling the feature, not to this migration, which
-- cannot know which types will ever be converted.
CREATE SEQUENCE tandem_seq AS BIGINT START WITH 1 INCREMENT BY 1 CACHE 1;

--changeset tandem:v3-default-tandem-outbox-seq stripComments:false
-- Strictly additive (HLD §1.4): `seq` is NOT NULL with no default today, so no existing writer can
-- omit the column, and a DEFAULT changes the behaviour of none of them. An adopter who never opts in
-- carries a sequence that is never advanced and a default that never fires.
ALTER TABLE tandem_outbox
    ALTER COLUMN seq SET DEFAULT nextval('tandem_seq');
