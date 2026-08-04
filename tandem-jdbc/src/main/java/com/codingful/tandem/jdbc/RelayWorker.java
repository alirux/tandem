package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.exception.OutboxDispatchException;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.TandemMetrics;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * The poll/dispatch/mark logic for one worker (LLD-jdbc §3.3/§3.4), extracted from the threading so it
 * can be unit-tested deterministically against {@code InMemoryOutbox} + {@code RecordingDispatcher}.
 *
 * <p>Per-aggregate ordering and the poison gate hold <b>structurally</b> (§3.4): the head-of-chain
 * claim returns at most one row per aggregate, and {@code seq(N+1)} only becomes claimable once
 * {@code seq(N)} is {@code DONE}. So the worker overlaps {@code batchSize} sends of <i>distinct</i>
 * aggregates on one async dispatcher without any per-record await.
 *
 * <p><b>Completions never touch the database.</b> The dispatcher settles its futures on its own I/O
 * thread (for Kafka, the producer's single sender thread, which every worker's sends share), so the
 * completion handler only enqueues — acks into {@code doneIds}, failures into {@code failures} — and
 * both queues are drained by the worker's own loop ({@link #flushDone()} / {@link #flushFailures()}),
 * where the JDBC writes happen. A slow store can then never stall the dispatcher's I/O thread and,
 * with it, every other in-flight send.
 */
final class RelayWorker {

    private static final Logger LOG = System.getLogger(RelayWorker.class.getName());

    private final OutboxStore store;
    private final OutboxDispatcher dispatcher;
    private final RelayConfig cfg;
    private final BackoffStrategy backoff;
    private final TandemMetrics metrics;
    private final Clock clock;
    private final String workerId;
    private final Supplier<Set<Integer>> ownedBuckets;

    private final AtomicInteger inFlight = new AtomicInteger();
    private final ConcurrentLinkedQueue<Long> doneIds = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<FailedDispatch> failures = new ConcurrentLinkedQueue<>();

    // Coarse-grained throughput visibility independent of any metrics adapter — see recordProgress().
    private final AtomicLong outcomeCount = new AtomicLong();
    private final AtomicLong okCount = new AtomicLong();
    private final AtomicLong koCount = new AtomicLong();

    /** A failed publish, captured on the completion thread and processed by {@link #flushFailures()}. */
    private record FailedDispatch(OutboxRecord record, Throwable error) {
    }

    // Backoff still stays *relative*, handed to the store to anchor on the DB clock (§3.2/§3.6) — the
    // Clock below is used for exactly one thing, timestamping the ack instant for recordPublishLatency,
    // and must never be used to compute a locally-anchored retry deadline.
    RelayWorker(OutboxStore store, OutboxDispatcher dispatcher, RelayConfig cfg, BackoffStrategy backoff,
                TandemMetrics metrics, Clock clock, String workerId, Supplier<Set<Integer>> ownedBuckets) {
        this.store = store;
        this.dispatcher = dispatcher;
        this.cfg = cfg;
        this.backoff = backoff;
        this.metrics = metrics;
        this.clock = clock;
        this.workerId = workerId;
        this.ownedBuckets = ownedBuckets;
    }

    /**
     * Claim up to the remaining in-flight capacity and dispatch each head asynchronously, wiring the
     * completion handler. Returns the number of rows claimed (0 means no work was available).
     */
    int claimAndDispatch() {
        int capacity = cfg.batchSize() - inFlight.get();
        if (capacity <= 0) {
            return 0;
        }
        Set<Integer> buckets = ownedBuckets.get();
        if (buckets.isEmpty()) {
            return 0;
        }
        List<OutboxRecord> batch = store.claimBatch(buckets, workerId, cfg.rowLease(), capacity);
        for (OutboxRecord record : batch) {
            inFlight.incrementAndGet();
            CompletableFuture<Void> ack;
            try {
                ack = dispatcher.dispatch(record);
            } catch (RuntimeException synchronousFailure) {
                onComplete(record, synchronousFailure);
                continue;
            }
            ack.whenComplete((ignored, error) -> onComplete(record, error));
        }
        return batch.size();
    }

    /**
     * Runs on the dispatcher's completion thread — <b>must not block</b> (see the class doc): it only
     * enqueues the outcome; the store writes happen in {@link #flushDone()} / {@link #flushFailures()}.
     */
    private void onComplete(OutboxRecord record, Throwable error) {
        try {
            if (error == null) {
                doneIds.add(record.id());
                if (metrics.isEnabled()) {
                    metrics.incrementPublished(1);
                    metrics.recordPublishLatency(Duration.between(record.createdAt(), clock.instant()));
                }
                recordProgress(true);
            } else {
                failures.add(new FailedDispatch(record, error));
                recordProgress(false);
            }
        } finally {
            inFlight.decrementAndGet();
        }
    }

    /**
     * One {@code INFO} line every {@code cfg.logEveryRows()} dispatch outcomes on this worker, ok and ko
     * combined — a row-count cadence rather than a clock one, so an idle relay logs nothing and a busy
     * one gets a bounded number of lines regardless of throughput. Always on: unlike the metrics port
     * (§4), this needs no adapter wired.
     */
    private void recordProgress(boolean ok) {
        long total = outcomeCount.incrementAndGet();
        (ok ? okCount : koCount).incrementAndGet();
        if (crossesLogThreshold(total, cfg.logEveryRows())) {
            LOG.log(Level.INFO, "Relay worker progress workerId:" + workerId
                    + ", processed:" + total + ", ok:" + okCount.get() + ", ko:" + koCount.get());
        }
    }

    /** Whether {@code total} is exactly on a {@code every}-row boundary — pulled out so it is testable without a logger. */
    static boolean crossesLogThreshold(long total, long every) {
        return total % every == 0;
    }

    /** Route a failed publish to retry-with-backoff or permanent FAILED (§3.4.2), per the verdict. */
    private void handleFailure(OutboxRecord record, Throwable error) {
        Throwable cause = unwrap(error);
        boolean retriable = !(cause instanceof OutboxDispatchException ode) || ode.isRetriable();
        String message = cause.getMessage();
        int attemptsAfter = record.attempts() + 1;
        if (retriable && attemptsAfter < cfg.maxAttempts()) {
            Duration delay = backoff.delayFor(record.attempts());
            LOG.log(Level.WARNING, "Outbox row dispatch failed, retrying rowId:" + record.id()
                    + ", aggregateType:" + record.aggregateType() + ", aggregateId:" + record.aggregateId()
                    + ", attempts:" + attemptsAfter + ", delayMs:" + delay.toMillis(), cause);
            store.markForRetry(record.id(), message, delay);
            if (metrics.isEnabled()) {
                metrics.incrementRetry();
            }
        } else {
            LOG.log(Level.ERROR, "Outbox row dispatch failed permanently rowId:" + record.id()
                    + ", aggregateType:" + record.aggregateType() + ", aggregateId:" + record.aggregateId()
                    + ", attempts:" + attemptsAfter, cause);
            store.markFailed(record.id(), message);
            // No metrics call here: failed.count is a live count of FAILED rows (WorkerPool.metricsTick,
            // via OutboxStore.failedCount()), not a tally of failure events — a row can later leave
            // FAILED (moved to DISCARDED by an operator), and only a fresh read reflects that.
        }
    }

    /** Flush the acked ids accumulated across aggregates in one batched mark-DONE (§3.4.1). */
    int flushDone() {
        if (doneIds.isEmpty()) {
            return 0;
        }
        List<Long> batch = new ArrayList<>();
        Long id;
        while ((id = doneIds.poll()) != null) {
            batch.add(id);
        }
        if (batch.isEmpty()) {
            return 0;
        }
        store.markDoneBatch(batch);
        return batch.size();
    }

    /**
     * Process the failures captured by the completion handler: mark each row for retry or FAILED on
     * <b>this</b> (worker) thread, keeping the JDBC writes off the dispatcher's I/O thread. Returns the
     * number of failures processed.
     */
    int flushFailures() {
        int processed = 0;
        FailedDispatch failure;
        while ((failure = failures.poll()) != null) {
            handleFailure(failure.record(), failure.error());
            processed++;
        }
        return processed;
    }

    int inFlight() {
        return inFlight.get();
    }

    long okCount() {
        return okCount.get();
    }

    long koCount() {
        return koCount.get();
    }

    private static Throwable unwrap(Throwable error) {
        if (error instanceof CompletionException && error.getCause() != null) {
            return error.getCause();
        }
        return error;
    }
}
