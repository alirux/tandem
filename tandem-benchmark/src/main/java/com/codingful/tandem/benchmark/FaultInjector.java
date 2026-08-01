package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.OutboxRecord;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * A mutable, at-runtime-toggleable fault rule for {@link FaultInjectingDispatcher} (S6, LLD-benchmark
 * §8). Held once per {@link BenchmarkEnvironment} so the relay pool can be built a single time while
 * still letting a scenario turn fault injection on/off mid-run.
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

    /** Back to pass-through — no record fails. */
    public void clear() {
        rule.set(record -> Fault.NONE);
    }

    Fault faultFor(OutboxRecord record) {
        return rule.get().apply(record);
    }

    private void applyToAggregate(String aggregateId, Fault fault) {
        rule.set(record -> record.aggregateId().value().equals(aggregateId) ? fault : Fault.NONE);
    }
}
