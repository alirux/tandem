package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.CorrelationConsumer;
import com.codingful.tandem.benchmark.LagProbe;
import com.codingful.tandem.benchmark.LoadGenerator;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BooleanSupplier;

/** Correctness assertions and small timing helpers every scenario shares (HLD-load-testing.md §4). */
final class ScenarioSupport {

    private ScenarioSupport() {
    }

    record CorrectnessReport(long orderingViolations, Set<String> missingKeys, long duplicateCount) {
        boolean passed() {
            return orderingViolations == 0 && missingKeys.isEmpty();
        }
    }

    /** Zero ordering violations, and every inserted {@code (aggregateId, seq)} eventually arrived (duplicates allowed). */
    static CorrectnessReport verify(LoadGenerator generator, CorrelationConsumer consumer) {
        Set<String> missing = new HashSet<>(generator.insertedKeys());
        missing.removeAll(consumer.receivedKeys());
        return new CorrectnessReport(consumer.orderingViolations(), missing, consumer.duplicateCount());
    }

    /**
     * Polls the lag probe until {@code namespace}'s own PENDING/IN_FLIGHT backlog drains to zero, or
     * throws on timeout. Deliberately excludes {@code FAILED} rows ({@link LagProbe#inProgressForNamespace}):
     * a permanently failed row is terminal and will never drain on its own, so it must not stall this
     * wait — it surfaces instead as a missing key in {@link #verify}. Scoped to {@code namespace}
     * (a scenario's own aggregate-id prefix, from {@link com.codingful.tandem.benchmark.AggregateSelector})
     * so another scenario's permanently-stuck poisoned backlog (S6, sharing one {@code tandem_outbox}
     * table across a test class) can never make this wait hang forever.
     */
    static void waitForDrain(LagProbe lagProbe, String namespace, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (lagProbe.inProgressForNamespace(namespace).pending() == 0) {
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("backlog did not drain within " + timeout);
    }

    /** Like {@link #waitForDrain}, but ignoring one aggregate's own rows within {@code namespace} (S6: the poisoned one never drains). */
    static void waitForOthersToDrain(LagProbe lagProbe, String namespace, String excludedAggregateId, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (lagProbe.pendingExcludingAggregate(namespace, excludedAggregateId) == 0) {
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("other aggregates did not drain within " + timeout);
    }

    /** A ramp observation window scaled to a short (smoke) or long (full) {@code duration}, floored at 1s. */
    static Duration observationWindowFor(BenchmarkConfig cfg) {
        Duration tenth = cfg.duration().dividedBy(10);
        return tenth.compareTo(Duration.ofSeconds(1)) < 0 ? Duration.ofSeconds(1) : tenth;
    }

    static Duration maxDuration(Duration a, Duration b) {
        return a.compareTo(b) >= 0 ? a : b;
    }

    static Duration minDuration(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    /**
     * Polls {@code condition} until it's true, or throws on timeout — the same pattern
     * {@code EmbeddedLeaseIT}/{@code BucketLeaseManagerIT} use in {@code tandem-jdbc} (S8: awaiting a
     * bucket-ownership partition to stabilize, before and after a simulated instance kill).
     */
    static void awaitUpTo(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("condition not met within " + timeout);
    }
}
