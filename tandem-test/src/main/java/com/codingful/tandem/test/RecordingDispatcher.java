package com.codingful.tandem.test;

import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.exception.OutboxDispatchException;
import com.codingful.tandem.core.port.OutboxDispatcher;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An in-memory {@link OutboxDispatcher} that <b>records</b> every dispatched record for assertions and
 * can be told to <b>fail</b> a record retriably or permanently — a real collaborator, not a mock
 * (LLD-test §2). It exercises the relay's retry/backoff and fail-fast paths (LLD-kafka §4) without
 * Kafka.
 *
 * <p>Two completion modes:
 * <ul>
 *   <li><b>Auto</b> (default): {@link #dispatch} returns an already-settled future — completed for a
 *       success, completed-exceptionally with the chosen {@link OutboxDispatchException} for a forced
 *       failure.</li>
 *   <li><b>Manual</b> ({@link #manualCompletion()}): {@link #dispatch} returns an <i>incomplete</i>
 *       future and queues it, so the test can keep several sends of distinct aggregates in flight and
 *       settle them on demand via {@link #completeNext()} / {@link #completeAll()} — the overlapping
 *       in-flight dispatch the relay relies on (LLD-jdbc §3.4).</li>
 * </ul>
 */
public final class RecordingDispatcher implements OutboxDispatcher {

    /** A queued, not-yet-settled send in manual mode. */
    private static final class Pending {
        final OutboxRecord record;
        final CompletableFuture<Void> future;

        Pending(OutboxRecord record, CompletableFuture<Void> future) {
            this.record = record;
            this.future = future;
        }
    }

    private final List<OutboxRecord> dispatched = new CopyOnWriteArrayList<>();
    // id -> remaining forced failures (and the verdict).
    private final Map<Long, FailureSpec> failures = new ConcurrentHashMap<>();
    private volatile Boolean failAllRetriable;   // null = no global failure

    private volatile boolean manual;
    private final Deque<Pending> pending = new ArrayDeque<>();
    private final Object pendingLock = new Object();

    private record FailureSpec(boolean retriable, int remaining) {
    }

    @Override
    public CompletableFuture<Void> dispatch(OutboxRecord record) {
        dispatched.add(record);
        if (manual) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            synchronized (pendingLock) {
                pending.add(new Pending(record, future));
            }
            return future;
        }
        return settle(record, new CompletableFuture<>());
    }

    /** Settle a future per the configured outcome for {@code record}. */
    private CompletableFuture<Void> settle(OutboxRecord record, CompletableFuture<Void> future) {
        OutboxDispatchException failure = resolveFailure(record);
        if (failure == null) {
            future.complete(null);
        } else {
            future.completeExceptionally(failure);
        }
        return future;
    }

    /** Returns the exception to fail {@code record} with, or {@code null} for success; decrements counters. */
    private OutboxDispatchException resolveFailure(OutboxRecord record) {
        FailureSpec spec = failures.get(record.id());
        if (spec != null && spec.remaining() > 0) {
            if (spec.remaining() == Integer.MAX_VALUE) {
                return forced(spec.retriable());
            }
            int left = spec.remaining() - 1;
            if (left == 0) {
                failures.remove(record.id());
            } else {
                failures.put(record.id(), new FailureSpec(spec.retriable(), left));
            }
            return forced(spec.retriable());
        }
        if (failAllRetriable != null) {
            return forced(failAllRetriable);
        }
        return null;
    }

    private static OutboxDispatchException forced(boolean retriable) {
        return new OutboxDispatchException(
                "forced " + (retriable ? "retriable" : "permanent") + " dispatch failure", retriable);
    }

    // --- failure configuration (fluent) ---

    /** Fail every dispatch of {@code id} with the given verdict, until cleared. */
    public RecordingDispatcher failRecord(long id, boolean retriable) {
        failures.put(id, new FailureSpec(retriable, Integer.MAX_VALUE));
        return this;
    }

    /** Fail the first {@code times} dispatches of {@code id}, then let it succeed. */
    public RecordingDispatcher failRecord(long id, boolean retriable, int times) {
        if (times <= 0) {
            throw new IllegalArgumentException("times must be positive");
        }
        failures.put(id, new FailureSpec(retriable, times));
        return this;
    }

    /** Fail every dispatch (any record) with the given verdict. */
    public RecordingDispatcher failAll(boolean retriable) {
        failAllRetriable = retriable;
        return this;
    }

    /** Clear all forced failures; subsequent dispatches succeed. */
    public RecordingDispatcher succeedAll() {
        failures.clear();
        failAllRetriable = null;
        return this;
    }

    // --- manual completion mode ---

    /** Switch to manual mode: dispatched futures stay pending until completed explicitly. */
    public RecordingDispatcher manualCompletion() {
        this.manual = true;
        return this;
    }

    /** Number of futures currently in flight (manual mode). */
    public int pendingCount() {
        synchronized (pendingLock) {
            return pending.size();
        }
    }

    /** Settle the oldest in-flight send (FIFO) per its configured outcome. Returns its record. */
    public OutboxRecord completeNext() {
        Pending next;
        synchronized (pendingLock) {
            next = pending.poll();
        }
        if (next == null) {
            throw new IllegalStateException("no pending dispatch to complete");
        }
        settle(next.record, next.future);
        return next.record;
    }

    /** Settle all in-flight sends, oldest first. */
    public void completeAll() {
        while (true) {
            Pending next;
            synchronized (pendingLock) {
                next = pending.poll();
            }
            if (next == null) {
                return;
            }
            settle(next.record, next.future);
        }
    }

    // --- recordings ---

    /** Every record passed to {@link #dispatch}, in call order. */
    public List<OutboxRecord> dispatched() {
        return new ArrayList<>(dispatched);
    }

    /** The ids of every dispatched record, in call order. */
    public List<Long> dispatchedIds() {
        return dispatched.stream().map(OutboxRecord::id).toList();
    }

    public int dispatchCount() {
        return dispatched.size();
    }
}
