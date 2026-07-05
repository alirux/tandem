package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.port.TandemMetrics;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An <b>enabled</b> {@link TandemMetrics} that records what it was told — a real collaborator for
 * asserting the relay's metric calls without a metrics backend.
 */
final class RecordingMetrics implements TandemMetrics {

    private final List<String> configInvalidChecks = new CopyOnWriteArrayList<>();
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong leaseExpired = new AtomicLong();

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void incrementPublished(long n) {
        published.addAndGet(n);
    }

    @Override
    public void incrementRetry() {
        retries.incrementAndGet();
    }

    @Override
    public void recordFailed(long count) {
        failed.addAndGet(count);
    }

    @Override
    public void incrementLeaseExpired(long n) {
        leaseExpired.addAndGet(n);
    }

    @Override
    public void recordConfigInvalid(String check) {
        configInvalidChecks.add(check);
    }

    List<String> configInvalidChecks() {
        return configInvalidChecks;
    }

    long published() {
        return published.get();
    }

    long retries() {
        return retries.get();
    }

    long failed() {
        return failed.get();
    }
}
