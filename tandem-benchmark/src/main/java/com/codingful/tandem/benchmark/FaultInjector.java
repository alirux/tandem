package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.OutboxRecord;
import java.util.function.Predicate;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A mutable, at-runtime-toggleable predicate for {@link FaultInjectingDispatcher} (S6, LLD-benchmark
 * §8). Held once per {@link BenchmarkEnvironment} so the relay pool can be built a single time while
 * still letting a scenario turn fault injection on/off mid-run.
 */
public final class FaultInjector {

    private final AtomicReference<Predicate<OutboxRecord>> predicate = new AtomicReference<>(record -> false);

    boolean test(OutboxRecord record) {
        return predicate.get().test(record);
    }

    /** Every dispatch of {@code aggregateId}'s rows fails permanently from now on (S6: the poison message). */
    public void poisonAggregate(String aggregateId) {
        predicate.set(record -> record.aggregateId().value().equals(aggregateId));
    }

    /** Back to pass-through — no record fails. */
    public void clear() {
        predicate.set(record -> false);
    }
}
