package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.LagSnapshot;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.exception.TandemException;
import com.codingful.tandem.core.port.OutboxStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Wraps the real store and makes a targeted worker's {@link #claimBatch} throw whenever
 * {@link FaultInjector} currently stalls it — the claim-side counterpart to
 * {@link FaultInjectingDispatcher}. Has no dedicated scenario; wired only by
 * {@link MetricsDashboardDemo} (LLD-benchmark §6.3). Every other method delegates unchanged: this
 * fault must not disturb the lag/failed/blocked readings the other panels depend on.
 *
 * <p><b>Why a thrown exception, not a hung call.</b> {@code claimAndDispatch} runs synchronously on
 * the worker thread, so an exception here reproduces exactly what a stuck JDBC call looks like from
 * {@code WorkerPool}'s point of view: {@code runWorker}'s catch block logs it and neither flushes nor
 * stamps a progress cycle (LLD-jdbc §3.8), so the thread stays alive while its
 * {@code workers.cycle_age_seconds} reading stops advancing — the one failure mode
 * {@code workers.active} cannot show on its own. A dispatch-level fault ({@link FaultInjector.Fault#STALL})
 * cannot demonstrate this: it leaves a {@link java.util.concurrent.CompletableFuture} pending, which
 * {@code claimAndDispatch} returns from immediately, so the worker's cycle keeps advancing regardless.
 */
final class FaultInjectingOutboxStore implements OutboxStore {

    private final OutboxStore delegate;
    private final FaultInjector faultInjector;

    FaultInjectingOutboxStore(OutboxStore delegate, FaultInjector faultInjector) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.faultInjector = Objects.requireNonNull(faultInjector, "faultInjector");
    }

    @Override
    public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
        if (faultInjector.claimsStalledFor(workerId)) {
            throw new TandemException("injected claim stall workerId:" + workerId);
        }
        return delegate.claimBatch(buckets, workerId, lease, batchSize);
    }

    @Override
    public void markDone(long id) {
        delegate.markDone(id);
    }

    @Override
    public void markDoneBatch(Collection<Long> ids) {
        delegate.markDoneBatch(ids);
    }

    @Override
    public void markForRetry(long id, String error, Duration retryDelay) {
        delegate.markForRetry(id, error, retryDelay);
    }

    @Override
    public void markFailed(long id, String error) {
        delegate.markFailed(id, error);
    }

    @Override
    public int reclaimExpiredLeases() {
        return delegate.reclaimExpiredLeases();
    }

    @Override
    public int cleanup(Instant doneBefore, int batchSize) {
        return delegate.cleanup(doneBefore, batchSize);
    }

    @Override
    public Optional<LagSnapshot> lag() {
        return delegate.lag();
    }

    @Override
    public OptionalLong failedCount() {
        return delegate.failedCount();
    }

    @Override
    public OptionalLong blockedCount() {
        return delegate.blockedCount();
    }
}
