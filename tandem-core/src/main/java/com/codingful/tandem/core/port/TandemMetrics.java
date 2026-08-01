package com.codingful.tandem.core.port;

/**
 * Optional metrics port (LLD-core §2.5, §7). The default is a no-op: {@link #isEnabled()} returns
 * {@code false} and callers guard on it so the off-path costs nothing. A real adapter ships in
 * {@code tandem-micrometer}.
 */
public interface TandemMetrics {

    /** A no-op metrics sink — the default when no adapter is wired. */
    TandemMetrics NOOP = new TandemMetrics() {
    };

    /** {@code true} once a real adapter is wired; callers guard on this so the off-path costs nothing. */
    default boolean isEnabled() {
        return false;
    }

    /** @param pending the current count of {@code PENDING} rows */
    default void recordLag(long pending) {
    }

    /** @param age seconds since the oldest {@code PENDING} row was created */
    default void recordLagAgeSeconds(double age) {
    }

    /** @param n rows successfully published since the last call */
    default void incrementPublished(long n) {
    }

    /** @param count the current count of {@code FAILED} rows — a live reading, not a delta since the last call */
    default void recordFailed(long count) {
    }

    /**
     * @param count {@code PENDING} rows stuck behind a {@code FAILED} row of the same aggregate — a live
     *              reading. Included in the lag gauges too, since they are genuinely waiting; reported
     *              separately because no relay will claim them until an operator intervenes, so a rising
     *              lag with {@code blocked} rising with it means something quite different from a rising
     *              lag with {@code blocked} at zero (§7).
     */
    default void recordBlocked(long count) {
    }

    /** A retriable dispatch failure was retried. */
    default void incrementRetry() {
    }

    /** @param n rows reclaimed because their lease expired */
    default void incrementLeaseExpired(long n) {
    }

    /** @param n worker threads currently running across this instance */
    default void recordActiveWorkers(int n) {
    }

    /** Buckets with PENDING rows but no live owner (§7). */
    default void recordUncoveredBuckets(int n) {
    }

    /** A startup config invariant was violated, e.g. {@code rowLease <= delivery.timeout.ms} (LLD-jdbc §3.5). */
    default void recordConfigInvalid(String check) {
    }
}
