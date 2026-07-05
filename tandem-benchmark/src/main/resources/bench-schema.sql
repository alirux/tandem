-- Benchmark-owned table (not part of the Tandem schema): a synthetic aggregate that the
-- LoadGenerator locks with SELECT ... FOR UPDATE and bumps on every insert, mimicking the
-- domain-row contention the outbox pattern is meant to sit inside (LLD-benchmark §4.1).
CREATE TABLE IF NOT EXISTS bench_aggregate (
    aggregate_id VARCHAR(255) PRIMARY KEY,
    version      BIGINT NOT NULL DEFAULT 0
);
