package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.port.TandemMetrics;
import java.util.concurrent.atomic.LongAdder;

/**
 * In-process {@link TandemMetrics} sink registered with the relay (HLD-load-testing.md §2.1): the
 * real {@code tandem-micrometer} adapter stays a future module, so the harness counts the relay's
 * published/retry/failed/lease-expired events itself. It does <b>not</b> compute lag or lag age —
 * nothing in the product ever calls {@link #recordLagAgeSeconds}, so that signal comes from
 * {@link LagProbe}'s direct SQL instead (LLD-benchmark §6, §6.1).
 */
public final class BenchmarkMetrics implements TandemMetrics {

    private final LongAdder published = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder retries = new LongAdder();
    private final LongAdder leaseExpired = new LongAdder();
    private final LongAdder configInvalid = new LongAdder();

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void incrementPublished(long n) {
        published.add(n);
    }

    @Override
    public void recordFailed(long count) {
        failed.add(count);
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

    public long failedCount() {
        return failed.sum();
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

    /** Returns the count published since the last call, resetting the counter — a throughput sampling window. */
    public long publishedSinceLast() {
        return published.sumThenReset();
    }
}
