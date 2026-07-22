package com.codingful.tandem.core.port;

import java.util.OptionalInt;

/**
 * The side-effect boundary for the bucket-count guard (LLD-bucket-count-guard §4): reads and
 * atomically seeds the single bucket-count value the write-side and the relay must agree on. This is
 * the <b>only</b> interface in the guard that performs I/O, so the reconciliation strategy
 * ({@link com.codingful.tandem.core.BucketCountReconciliation}) and the orchestration stay
 * storage-agnostic and the race-sensitive atomic seed has exactly one home. Implemented by
 * {@code tandem-jdbc}'s {@code JdbcBucketCountStore} over the {@code tandem_meta} table.
 */
public interface BucketCountStore {

    /** The bucket count established in the database, or empty if none has been established yet. */
    OptionalInt read();

    /**
     * Atomically store {@code candidate} <b>only if no value exists yet</b>, then return the value now
     * in effect — {@code candidate} if this call established it, or the pre-existing value if another
     * process won a concurrent first-initialisation race. Never overwrites an existing value. This is
     * the single primitive that must be atomic; the orchestrator re-reads its result and re-decides,
     * so a mismatched race still fails loudly (§4.1).
     *
     * @param candidate the bucket count to seed if none is stored (must be positive)
     * @return the bucket count now stored
     */
    int seedIfAbsent(int candidate);
}
