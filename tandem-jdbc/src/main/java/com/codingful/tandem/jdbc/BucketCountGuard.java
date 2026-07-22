package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.BucketCountReconciliation;
import com.codingful.tandem.core.BucketCountReconciliation.Decision;
import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.port.BucketCountStore;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.OptionalInt;
import javax.sql.DataSource;

/**
 * Fails fast when the write-side and the relay are configured with different bucket counts
 * (LLD-bucket-count-guard). A divergent {@code bucketCount} makes the write-side insert into buckets
 * the relay never polls, silently stopping delivery — so the guard persists the value on first startup
 * and validates every later startup against it, on <b>both</b> sides.
 *
 * <p>The normal call is the static {@link #check(DataSource, int)} — an explicit startup step the
 * assembly runs against a plain {@link DataSource}. The instance form
 * ({@link #BucketCountGuard(BucketCountStore, BucketCountReconciliation)} + {@link #check(int)}) is the
 * seam for a non-default {@link BucketCountReconciliation} policy or an injected {@link BucketCountStore}
 * test double; it holds no policy of its own, delegating the decision to the strategy and the I/O to the
 * port.
 */
public final class BucketCountGuard {

    private static final Logger LOG = System.getLogger(BucketCountGuard.class.getName());

    private final BucketCountStore store;
    private final BucketCountReconciliation reconciliation;

    /**
     * Reconcile {@code bucketCount} against the value stored in {@code dataSource}, with the default
     * {@link BucketCountReconciliation#seedOrValidate()} policy: seed it if none is stored, proceed if
     * they agree, fail fast if they differ. The normal startup call.
     *
     * @throws TandemConfigurationException if a different bucket count is already established
     */
    public static void check(DataSource dataSource, int bucketCount) {
        check(dataSource, bucketCount, BucketCountReconciliation.seedOrValidate());
    }

    /** As {@link #check(DataSource, int)}, with an explicit reconciliation policy. */
    public static void check(DataSource dataSource, int bucketCount, BucketCountReconciliation policy) {
        new BucketCountGuard(new JdbcBucketCountStore(dataSource), policy).check(bucketCount);
    }

    /** The seam for an injected store and policy — e.g. a non-default strategy or a test double. */
    public BucketCountGuard(BucketCountStore store, BucketCountReconciliation reconciliation) {
        this.store = Objects.requireNonNull(store, "store");
        this.reconciliation = Objects.requireNonNull(reconciliation, "reconciliation");
    }

    /**
     * Reconcile {@code configuredBucketCount} against the stored value: seed it if none is stored,
     * proceed if they agree, or throw {@link TandemConfigurationException} if they differ. Idempotent
     * and cheap — one read (plus, on first initialisation only, one seed). Safe to call from more than
     * one process concurrently (§4.1).
     *
     * @throws TandemConfigurationException if the stored and configured bucket counts differ
     */
    public void check(int configuredBucketCount) {
        Decision decision = reconciliation.decide(store.read(), configuredBucketCount);
        switch (decision.kind()) {
            case PROCEED -> { /* both sides agree */ }
            case CONFLICT -> throw conflict(decision);
            case SEED -> {
                // Seed atomically, then re-decide against the value now in effect: this is PROCEED if we
                // seeded, or CONFLICT if a concurrent process seeded a different value. seedIfAbsent
                // guarantees a single persisted value, so the race loser still fails loudly.
                int effective = store.seedIfAbsent(configuredBucketCount);
                Decision reDecision = reconciliation.decide(OptionalInt.of(effective), configuredBucketCount);
                if (reDecision.kind() == Decision.Kind.CONFLICT) {
                    throw conflict(reDecision);
                }
            }
        }
    }

    /** Logs the mismatch and builds the failure to throw (returned, so the {@code throw} stays at the call site). */
    private static TandemConfigurationException conflict(Decision decision) {
        LOG.log(Level.ERROR, decision.message());
        return new TandemConfigurationException(decision.message());
    }
}
