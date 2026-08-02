package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.OutboxRecord;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * A mutable, at-runtime-toggleable fault rule for {@link FaultInjectingDispatcher} and
 * {@link FaultInjectingOutboxStore}. Held once per {@link BenchmarkEnvironment} so the relay pool can
 * be built a single time while still letting a scenario turn fault injection on/off mid-run.
 *
 * <p>Two independent axes: which {@code OutboxRecord} a dispatch fault applies to (per-aggregate), and
 * which relay instance a claim stall applies to (per-worker) — orthogonal because a claim never sees a
 * record at all, so it cannot be selected by aggregate. The dispatch axis is what S6 (LLD-benchmark §8)
 * drives; the claim axis has no dedicated scenario and is used only by {@link MetricsDashboardDemo}
 * (§6.3).
 */
public final class FaultInjector {

    /** What the dispatcher should do with a record the rule matches. */
    public enum Fault {
        /** Dispatch normally. */
        NONE,
        /** Fail in a way the relay will retry — the path that drives {@code retry.count}. */
        RETRIABLE,
        /** Fail terminally: the row goes straight to {@code FAILED}, no attempts left to burn. */
        PERMANENT,
        /**
         * Never complete the dispatch at all. The row stays {@code IN_FLIGHT} holding a row lease, which
         * is the only reliable way to have rows in that state when an instance is killed — and therefore
         * the only reliable way to exercise lease reclaim ({@code lease_expired.count}). A dispatch that
         * merely fails would have released the row long before the crash.
         */
        STALL
    }

    private final AtomicReference<Function<OutboxRecord, Fault>> rule = new AtomicReference<>(record -> Fault.NONE);
    // null = no worker currently stalled. Matched by substring against WorkerPool's own worker-id
    // format ("tandem-relay-" + instanceId + "-w" + index, LLD-jdbc §3.1) so a scenario can name the
    // instanceId it already configured rather than reconstructing the worker-id scheme itself.
    private final AtomicReference<String> stalledInstanceId = new AtomicReference<>();

    /** Every dispatch of {@code aggregateId}'s rows fails permanently from now on (S6: the poison message). */
    public void poisonAggregate(String aggregateId) {
        applyToAggregate(aggregateId, Fault.PERMANENT);
    }

    /** Every dispatch of {@code aggregateId}'s rows fails retriably from now on. */
    public void flakeAggregate(String aggregateId) {
        applyToAggregate(aggregateId, Fault.RETRIABLE);
    }

    /** Every dispatch of {@code aggregateId}'s rows hangs from now on, pinning the row {@code IN_FLIGHT}. */
    public void stallAggregate(String aggregateId) {
        applyToAggregate(aggregateId, Fault.STALL);
    }

    /**
     * Every claim attempt by any worker of {@code instanceId} throws from now on: the instance's threads
     * stay alive but stop progressing, driving {@code workers.cycle_age_seconds} up while
     * {@code workers.active} does not move.
     */
    public void stallWorkerClaims(String instanceId) {
        stalledInstanceId.set(Objects.requireNonNull(instanceId, "instanceId"));
    }

    /** Back to pass-through — no record fails and no worker's claims are stalled. */
    public void clear() {
        rule.set(record -> Fault.NONE);
        stalledInstanceId.set(null);
    }

    Fault faultFor(OutboxRecord record) {
        return rule.get().apply(record);
    }

    boolean claimsStalledFor(String workerId) {
        String instanceId = stalledInstanceId.get();
        return instanceId != null && workerId.contains(instanceId);
    }

    private void applyToAggregate(String aggregateId, Fault fault) {
        rule.set(record -> record.aggregateId().value().equals(aggregateId) ? fault : Fault.NONE);
    }
}
