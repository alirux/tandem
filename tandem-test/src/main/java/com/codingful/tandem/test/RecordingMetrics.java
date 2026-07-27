package com.codingful.tandem.test;

import com.codingful.tandem.core.port.TandemMetrics;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An <b>enabled</b> {@link TandemMetrics} that keeps what it was told — a real collaborator for
 * asserting what the relay emits, without a metrics backend and without a mocking framework
 * (LLD-test §1).
 *
 * <p>Being enabled is the point: every metric in the relay sits behind {@code isEnabled()}, so a test
 * using the no-op default exercises none of that code. Wire this one to assert the emissions, or to
 * check your own adapter is reached.
 *
 * <p>Gauges keep their <b>latest</b> reading, counters accumulate. Safe to read from another thread
 * than the relay's.
 */
public final class RecordingMetrics implements TandemMetrics {

    private final List<String> configInvalidChecks = new CopyOnWriteArrayList<>();
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong leaseExpired = new AtomicLong();
    private final AtomicLong lag = new AtomicLong(-1);
    private final AtomicLong lagAgeMillis = new AtomicLong(-1);
    private final AtomicInteger activeWorkers = new AtomicInteger(-1);
    private final AtomicInteger uncoveredBuckets = new AtomicInteger(-1);

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void recordLag(long pending) {
        lag.set(pending);
    }

    @Override
    public void recordLagAgeSeconds(double age) {
        lagAgeMillis.set(Math.round(age * 1000));
    }

    @Override
    public void incrementPublished(long n) {
        published.addAndGet(n);
    }

    @Override
    public void recordFailed(long count) {
        failed.addAndGet(count);
    }

    @Override
    public void incrementRetry() {
        retries.incrementAndGet();
    }

    @Override
    public void incrementLeaseExpired(long n) {
        leaseExpired.addAndGet(n);
    }

    @Override
    public void recordActiveWorkers(int n) {
        activeWorkers.set(n);
    }

    @Override
    public void recordUncoveredBuckets(int n) {
        uncoveredBuckets.set(n);
    }

    @Override
    public void recordConfigInvalid(String check) {
        configInvalidChecks.add(check);
    }

    public long published() {
        return published.get();
    }

    public long retries() {
        return retries.get();
    }

    public long failed() {
        return failed.get();
    }

    public long leaseExpired() {
        return leaseExpired.get();
    }

    /** The names of the failed startup checks, in the order they were reported. */
    public List<String> configInvalidChecks() {
        return configInvalidChecks;
    }

    /** The latest pending count, or {@code -1} if the gauge was never recorded. */
    public long lag() {
        return lag.get();
    }

    /** The latest lag age in seconds, or {@code -1} if the gauge was never recorded. */
    public double lagAgeSeconds() {
        long millis = lagAgeMillis.get();
        return millis < 0 ? -1 : millis / 1000d;
    }

    /** The latest live-worker count, or {@code -1} if the gauge was never recorded. */
    public int activeWorkers() {
        return activeWorkers.get();
    }

    /** The latest uncovered-bucket count, or {@code -1} if the gauge was never recorded. */
    public int uncoveredBuckets() {
        return uncoveredBuckets.get();
    }
}
