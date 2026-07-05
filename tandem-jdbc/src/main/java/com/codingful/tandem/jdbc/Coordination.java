package com.codingful.tandem.jdbc;

/**
 * How concurrent relay instances share the virtual buckets (LLD-jdbc §3.2, HLD §3.2 axis 2). Chosen
 * statically via {@link RelayConfig#coordination()} — it is orthogonal to <i>where</i> the relay runs
 * (embedded in the client or standalone).
 */
public enum Coordination {

    /**
     * The relay instance owns <b>all</b> buckets in-process; no coordination table. Correct only when
     * exactly one relay instance runs against the outbox. The zero-cost default.
     */
    SINGLE,

    /**
     * Bucket ownership is partitioned across instances via the {@code tandem_bucket_lease} table
     * ({@link BucketLeaseManager}). Correct for any number of concurrent instances — a horizontally-
     * scaled client with an embedded relay, or multiple standalone relay processes.
     */
    LEASE
}
