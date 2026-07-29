package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.port.TandemMetrics;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * In-process {@link TandemMetrics} sink registered with the relay (HLD-load-testing.md §2.1): the
 * real {@code tandem-micrometer} adapter stays a future module, so the harness counts the relay's
 * published/retry/lease-expired events itself.
 *
 * <p>It also keeps the <b>gauges</b> the relay now reports — backlog, backlog age, failed-row count,
 * live workers (LLD-jdbc §4). Those are read from the relay's own periodic reading, which makes them
 * directly comparable with {@link LagProbe}'s independent SQL over the same table: two computations of
 * the same figure, and a benchmark run is where they can be checked against each other.
 */
public final class BenchmarkMetrics implements TandemMetrics {

    private final LongAdder published = new LongAdder();
    private final LongAdder retries = new LongAdder();
    private final LongAdder leaseExpired = new LongAdder();
    private final LongAdder configInvalid = new LongAdder();

    // Gauges: latest reading wins, -1 until the relay reports one.
    private final AtomicLong lag = new AtomicLong(-1);
    private final AtomicLong lagAgeMillis = new AtomicLong(-1);
    private final AtomicInteger activeWorkers = new AtomicInteger(-1);
    private final AtomicLong failed = new AtomicLong(-1);

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
    public void recordActiveWorkers(int n) {
        activeWorkers.set(n);
    }

    @Override
    public void incrementPublished(long n) {
        published.add(n);
    }

    @Override
    public void recordFailed(long count) {
        failed.set(count);
    }

    @Override
    public void incrementRetry() {
        retries.increment();
    }

    @Override
    public void incrementLeaseExpired(long n) {
        leaseExpired.add(n);
    }

    @Override
    public void recordConfigInvalid(String check) {
        configInvalid.increment();
    }

    public long publishedCount() {
        return published.sum();
    }

    /** Latest live count of {@code FAILED} rows the relay reported, or {@code -1} before its first reading. */
    public long failedCount() {
        return failed.get();
    }

    public long retryCount() {
        return retries.sum();
    }

    public long leaseExpiredCount() {
        return leaseExpired.sum();
    }

    public long configInvalidCount() {
        return configInvalid.sum();
    }

    /** Latest backlog the relay reported, or {@code -1} before its first reading. */
    public long lag() {
        return lag.get();
    }

    /** Latest backlog age the relay reported in seconds, or {@code -1} before its first reading. */
    public double lagAgeSeconds() {
        long millis = lagAgeMillis.get();
        return millis < 0 ? -1 : millis / 1000d;
    }

    /** Latest live-worker count the relay reported, or {@code -1} before its first reading. */
    public int activeWorkers() {
        return activeWorkers.get();
    }

    /** Returns the count published since the last call, resetting the counter — a throughput sampling window. */
    public long publishedSinceLast() {
        return published.sumThenReset();
    }
}
