package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.port.TandemMetrics;
import java.lang.System.Logger;
import java.util.HashSet;
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
