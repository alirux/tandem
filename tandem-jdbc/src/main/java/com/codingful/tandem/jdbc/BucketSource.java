package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.port.TandemMetrics;
import java.lang.System.Logger;
import java.util.HashSet;
import java.util.OptionalInt;
import java.util.Set;
import javax.sql.DataSource;

/**
 * Supplies the virtual buckets this relay <b>instance</b> currently owns (LLD-jdbc §3.2). The
 * {@link WorkerPool} splits them across its worker threads.
 *
 * <ul>
 *   <li><b>Embedded</b> (single process): the instance owns <i>all</i> buckets — see
 *       {@link #embedded(int)}. No coordination table.</li>
 *   <li><b>Standalone</b> (multi-instance): the instance owns a subset assigned via the
 *       {@code tandem_bucket_lease} table — see {@link BucketLeaseManager}.</li>
 * </ul>
 */
public interface BucketSource {

    /** The buckets this instance currently owns. */
    Set<Integer> ownedBuckets();

    /** Periodic maintenance (renew/reconcile leases). No-op under {@link Coordination#SINGLE}. */
    default void heartbeat() {
    }

    /** Release owned buckets on shutdown. No-op under {@link Coordination#SINGLE}. */
    default void release() {
    }

    /**
     * One-time startup precondition check (relay fail-fast), mirroring {@link RelayConfig#checkRowLeaseSafe}.
     * No-op under {@link Coordination#SINGLE}; {@link BucketLeaseManager} verifies the lease table is
     * present and seeded to {@code bucketCount}.
     */
    default void validateOnStart(TandemMetrics metrics, Logger logger) {
    }

    /**
     * Count of buckets with {@code PENDING} rows but no live owner — a coverage stall — for the
     * {@code bucket.uncovered} gauge (HLD §7). Empty under {@link Coordination#SINGLE}: there is no
     * lease table to stall, and supervised worker-thread restart (LLD-jdbc §3.1) keeps coverage there;
     * {@code lag.age_seconds} is that mode's backstop signal instead.
     *
     * @return the count, or empty if this mode has no such notion — the relay then emits nothing
     */
    default OptionalInt uncoveredBuckets() {
        return OptionalInt.empty();
    }

    /**
     * Select the {@link BucketSource} for the configured {@link Coordination} mode (§3.2): {@link #embedded}
     * for {@code SINGLE}, a {@link BucketLeaseManager} (backed by {@code dataSource}) for {@code LEASE}.
     *
     * <p>Pure selection — it does not query {@code dataSource} (the {@code SINGLE} branch ignores it and
     * the {@code LEASE} branch only hands it to the manager). The bucket-count guard is a separate,
     * explicit relay-startup step ({@link BucketCountGuard}, LLD-bucket-count-guard §7), not folded in
     * here, so this stays a side-effect-free factory.
     */
    static BucketSource forCoordination(RelayConfig cfg, DataSource dataSource) {
        return switch (cfg.coordination()) {
            case SINGLE -> embedded(cfg.bucketCount());
            case LEASE -> new BucketLeaseManager(dataSource, cfg.instanceId(), cfg.bucketCount(), cfg.bucketLease());
        };
    }

    /** {@link Coordination#SINGLE}: the single instance owns every bucket {@code [0, bucketCount)}. */
    static BucketSource embedded(int bucketCount) {
        if (bucketCount <= 0) {
            throw new IllegalArgumentException("bucketCount must be positive");
        }
        Set<Integer> all = new HashSet<>(bucketCount);
        for (int b = 0; b < bucketCount; b++) {
            all.add(b);
        }
        Set<Integer> immutable = Set.copyOf(all);
        return () -> immutable;
    }
}
