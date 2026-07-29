package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.LagSnapshot;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.test.ControllableClock;
import com.codingful.tandem.test.InMemoryOutbox;
import com.codingful.tandem.test.RecordingDispatcher;
import com.codingful.tandem.test.RecordingMetrics;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class WorkerPoolTest {

    private static final int BUCKETS = 256;

    @Test
    void GIVEN_an_unsafe_row_lease_WHEN_the_relay_starts_THEN_it_aborts_startup() {
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).rowLease(Duration.ofSeconds(20)).deliveryTimeoutMs(30_000).build();
        WorkerPool pool = new WorkerPool(new InMemoryOutbox(), new RecordingDispatcher(), cfg);

        assertThatThrownBy(pool::start).isInstanceOf(TandemConfigurationException.class);
    }

    @Test
    void GIVEN_a_config_that_looks_safe_but_a_dispatcher_reporting_a_larger_timeout_WHEN_the_relay_starts_THEN_it_aborts() {
        // The footgun: config says deliveryTimeout=10s (rowLease 60s > 10s → looks safe), but the
        // dispatcher actually enforces 90s. The relay must validate against the dispatcher's reported
        // value, not the stale configured one, and abort.
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).rowLease(Duration.ofSeconds(60)).deliveryTimeoutMs(10_000).build();
        OutboxDispatcher reportsUnsafeTimeout = new OutboxDispatcher() {
            @Override
            public CompletableFuture<Void> dispatch(OutboxRecord record) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public OptionalLong deliveryTimeoutMillis() {
                return OptionalLong.of(90_000);
            }
        };
        WorkerPool pool = new WorkerPool(new InMemoryOutbox(), reportsUnsafeTimeout, cfg);

        assertThatThrownBy(pool::start).isInstanceOf(TandemConfigurationException.class);
    }

    @Test
    void GIVEN_many_events_across_aggregates_WHEN_the_running_relay_drains_them_THEN_all_are_delivered_in_per_aggregate_order() {
        InMemoryOutbox outbox = new InMemoryOutbox();   // system clock, B=256
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        int aggregates = 20;
        int perAggregate = 10;
        for (int a = 0; a < aggregates; a++) {
            for (int seq = 1; seq <= perAggregate; seq++) {
                outbox.insert(OutboxMessage.builder()
                        .aggregateId("order-" + a).aggregateType("Order").seq(seq)
                        .payload(("p-" + a + '-' + seq).getBytes()).build());
            }
        }
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(4).pollInterval(Duration.ofMillis(10)).build();
        WorkerPool pool = new WorkerPool(outbox, dispatcher, cfg);

        pool.start();
        try {
            int total = aggregates * perAggregate;
            awaitUpTo(Duration.ofSeconds(20),
                    () -> "all " + total + " events delivered, got " + outbox.statusCounts(),
                    () -> outbox.byStatus(OutboxStatus.DONE).size() == total);
        } finally {
            pool.stop();
        }

        // Every event delivered exactly once, and per aggregate the published seqs are increasing.
        assertThat(dispatcher.dispatchCount()).isEqualTo(aggregates * perAggregate);
        Map<String, List<Long>> seqsByAggregate = dispatcher.dispatched().stream()
                .collect(Collectors.groupingBy(r -> r.aggregateId().value(),
                        Collectors.mapping(OutboxRecord::seq, Collectors.toList())));
        assertThat(seqsByAggregate).hasSize(aggregates);
        seqsByAggregate.forEach((aggregate, seqs) ->
                assertThat(seqs).as("order within %s", aggregate).isSorted());
    }

    @Test
    void GIVEN_a_running_relay_WHEN_stopped_THEN_it_shuts_down_cleanly() {
        WorkerPool pool = new WorkerPool(new InMemoryOutbox(), new RecordingDispatcher(),
                RelayConfig.builder().bucketCount(BUCKETS).workersPerInstance(2).build());

        pool.start();
        pool.stop();   // must return without hanging; idempotent
        pool.stop();
    }

    @Test
    void GIVEN_a_running_relay_WHEN_stopped_gracefully_THEN_the_bucket_source_is_released() {
        RecordingBucketSource buckets = new RecordingBucketSource(BUCKETS);
        WorkerPool pool = new WorkerPool(new InMemoryOutbox(), new RecordingDispatcher(),
                RelayConfig.builder().bucketCount(BUCKETS).workersPerInstance(2).build(),
                TandemMetrics.NOOP, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), buckets);

        pool.start();
        pool.stop();

        assertThat(buckets.releaseCalls()).isEqualTo(1);
    }

    @Test
    void GIVEN_a_running_relay_WHEN_killed_THEN_threads_stop_but_the_bucket_source_is_NOT_released() {
        // The distinction kill() exists for: a graceful stop() releases immediately (LEASE: buckets +
        // presence freed at once); kill() simulates a crash, where nothing is released explicitly —
        // ownership is only discovered stale once the lease itself expires (§3.2).
        RecordingBucketSource buckets = new RecordingBucketSource(BUCKETS);
        WorkerPool pool = new WorkerPool(new InMemoryOutbox(), new RecordingDispatcher(),
                RelayConfig.builder().bucketCount(BUCKETS).workersPerInstance(2).build(),
                TandemMetrics.NOOP, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), buckets);

        pool.start();
        pool.kill();   // must return without hanging; idempotent
        pool.kill();

        assertThat(buckets.releaseCalls()).isZero();
    }

    @Test
    void GIVEN_a_worker_killed_by_a_fatal_error_WHEN_events_are_waiting_THEN_the_relay_recovers_and_delivers_them() {
        // The supervision contract (§3.1): an Error escaping a worker kills its thread, and with it the
        // buckets it owns — nothing else would ever pick them up. A single worker makes that fatal if
        // the restart does not happen: the event below would simply never be delivered.
        InMemoryOutbox outbox = new InMemoryOutbox();
        outbox.insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build());
        CrashingOnceStore store = new CrashingOnceStore(outbox);
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(1).pollInterval(Duration.ofMillis(10)).build();
        WorkerPool pool = new WorkerPool(store, dispatcher, cfg);

        pool.start();
        try {
            awaitUpTo(Duration.ofSeconds(20),
                    () -> "the waiting event to be delivered after the worker restart, got "
                            + outbox.statusCounts() + ", crashed:" + store.crashed(),
                    () -> outbox.byStatus(OutboxStatus.DONE).size() == 1);
        } finally {
            pool.stop();
        }

        assertThat(store.crashed()).as("the worker really did die").isTrue();
        assertThat(dispatcher.dispatchCount()).isEqualTo(1);
    }

    @Test
    void GIVEN_delivered_events_older_than_the_retention_window_WHEN_the_relay_runs_THEN_they_are_purged_from_the_outbox() {
        InMemoryOutbox outbox = new InMemoryOutbox();
        for (int seq = 1; seq <= 3; seq++) {
            outbox.insert(OutboxMessage.builder()
                    .aggregateId("order-1").aggregateType("Order").seq(seq).payload("{}".getBytes()).build());
        }
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(2).pollInterval(Duration.ofMillis(10))
                .retention(Duration.ZERO).cleanupInterval(Duration.ofMillis(50)).build();
        WorkerPool pool = new WorkerPool(outbox, new RecordingDispatcher(), cfg);

        pool.start();
        try {
            // Delivered is not enough — the rows must actually leave the table, or the outbox grows forever.
            awaitUpTo(Duration.ofSeconds(20),
                    () -> "an empty outbox, still holding " + outbox.statusCounts(),
                    () -> outbox.size() == 0);
        } finally {
            pool.stop();
        }
    }

    @Test
    void GIVEN_a_delivery_that_never_completes_WHEN_the_lease_expires_THEN_the_event_is_claimed_again_and_delivered() {
        ControllableClock clock = ControllableClock.atEpochDay();
        InMemoryOutbox outbox = new InMemoryOutbox(BUCKETS, InMemoryOutbox.DEFAULT_MAX_ATTEMPTS, clock);
        outbox.insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build());
        AtomicInteger attempts = new AtomicInteger();
        // First attempt never settles — the relay instance that owned it is, from the outbox's point of
        // view, gone; the row would stay IN_FLIGHT forever without reclaim.
        OutboxDispatcher stallsOnce = record -> attempts.incrementAndGet() == 1
                ? new CompletableFuture<>()
                : CompletableFuture.completedFuture(null);
        Duration rowLease = Duration.ofSeconds(60);
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(1).pollInterval(Duration.ofMillis(10))
                .rowLease(rowLease).reclaimInterval(Duration.ofMillis(50)).build();
        WorkerPool pool = new WorkerPool(outbox, stallsOnce, cfg);

        pool.start();
        try {
            awaitUpTo(Duration.ofSeconds(20),
                    () -> "the event to be claimed, got " + outbox.statusCounts(),
                    () -> outbox.byStatus(OutboxStatus.IN_FLIGHT).size() == 1);
            clock.advance(rowLease.plusSeconds(1));
            awaitUpTo(Duration.ofSeconds(20),
                    () -> "the reclaimed event to be delivered, got " + outbox.statusCounts()
                            + ", attempts:" + attempts.get(),
                    () -> outbox.byStatus(OutboxStatus.DONE).size() == 1);
        } finally {
            pool.stop();
        }

        assertThat(attempts).hasValueGreaterThan(1);   // at-least-once: the stalled attempt was retried
    }

    @Test
    void GIVEN_a_backlog_and_a_metrics_adapter_WHEN_the_relay_runs_THEN_it_reports_the_backlog_its_age_and_the_live_workers() {
        ControllableClock clock = ControllableClock.atEpochDay();
        InMemoryOutbox outbox = new InMemoryOutbox(BUCKETS, InMemoryOutbox.DEFAULT_MAX_ATTEMPTS, clock);
        // One chain: only its head is claimable, so the two successors stay PENDING no matter how many
        // workers run — a backlog that does not depend on claim timing.
        for (int seq = 1; seq <= 3; seq++) {
            outbox.insert(OutboxMessage.builder()
                    .aggregateId("order-1").aggregateType("Order").seq(seq).payload("{}".getBytes()).build());
        }
        clock.advance(Duration.ofSeconds(30));   // the backlog has been waiting half a minute
        RecordingMetrics metrics = new RecordingMetrics();
        // Nothing ever completes, so the backlog stays put and the reading is stable to assert.
        OutboxDispatcher stalls = record -> new CompletableFuture<>();
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(2).pollInterval(Duration.ofMillis(10))
                .metricsInterval(Duration.ofMillis(50)).build();
        WorkerPool pool = new WorkerPool(outbox, stalls, cfg, metrics, clock,
                BackoffStrategy.fullJitter(), BucketSource.embedded(BUCKETS));

        pool.start();
        try {
            // Wait for a *complete* reading taken after the head was claimed. Two things make a
            // partial one visible: the first reading is taken the instant the relay starts, which can
            // be before the head is claimed (all three rows still waiting), and a reading is three
            // separate gauge writes, so a tick can be caught half-written — every gauge must have
            // been set at least once before any of them is worth asserting.
            awaitUpTo(Duration.ofSeconds(20),
                    () -> "a complete metrics reading of the claimed backlog, observed lag:" + metrics.lag()
                            + ", lagAgeSeconds:" + metrics.lagAgeSeconds()
                            + ", activeWorkers:" + metrics.activeWorkers()
                            + ", inFlight:" + outbox.byStatus(OutboxStatus.IN_FLIGHT).size(),
                    () -> outbox.byStatus(OutboxStatus.IN_FLIGHT).size() == 1
                            && metrics.lag() == 2
                            && metrics.lagAgeSeconds() >= 0
                            && metrics.activeWorkers() >= 0);

            // Assert on the running relay, never after the stop: the gauges keep their latest reading,
            // and a metrics tick that races stop() reports the workers it finds already interrupted —
            // a truthful reading of a stopping relay, but not the one under test here.
            // The head is in flight; its two successors are still waiting, and have been for 30s.
            assertThat(metrics.lag()).isEqualTo(2);
            assertThat(metrics.lagAgeSeconds()).isEqualTo(30);
            assertThat(metrics.activeWorkers()).isEqualTo(2);
        } finally {
            pool.stop();
        }
    }

    @Test
    void GIVEN_a_permanently_failing_dispatch_WHEN_the_relay_runs_THEN_it_reports_the_row_as_failed() {
        InMemoryOutbox outbox = new InMemoryOutbox();
        outbox.insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build());
        RecordingDispatcher dispatcher = new RecordingDispatcher().failAll(false);   // permanent
        RecordingMetrics metrics = new RecordingMetrics();
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(1).pollInterval(Duration.ofMillis(10))
                .metricsInterval(Duration.ofMillis(20)).build();
        WorkerPool pool = new WorkerPool(outbox, dispatcher, cfg, metrics, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), BucketSource.embedded(BUCKETS));

        pool.start();
        try {
            awaitUpTo(Duration.ofSeconds(20), () -> "failed:" + metrics.failed(), () -> metrics.failed() == 1);
        } finally {
            pool.stop();
        }
    }

    @Test
    void GIVEN_the_stores_failed_count_drops_WHEN_the_relay_reads_it_again_THEN_the_reported_value_drops_too() {
        // The property a tally-of-events implementation cannot have: a live read must be able to go
        // down, exactly as it would once an operator resolves a stuck row (moved out of FAILED,
        // LLD-core §1.2) — a count that only ever grows would misreport a resolved incident forever.
        StubFailedCountStore store = new StubFailedCountStore(new InMemoryOutbox());
        store.setFailedCount(3);
        RecordingMetrics metrics = new RecordingMetrics();
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(1).pollInterval(Duration.ofMillis(10))
                .metricsInterval(Duration.ofMillis(20)).build();
        WorkerPool pool = new WorkerPool(store, new RecordingDispatcher(), cfg, metrics, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), BucketSource.embedded(BUCKETS));

        pool.start();
        try {
            awaitUpTo(Duration.ofSeconds(20), () -> "failed:" + metrics.failed(), () -> metrics.failed() == 3);

            store.setFailedCount(0);
            awaitUpTo(Duration.ofSeconds(20), () -> "failed:" + metrics.failed(), () -> metrics.failed() == 0);
        } finally {
            pool.stop();
        }
    }

    @Test
    void GIVEN_the_bucket_sources_uncovered_count_changes_WHEN_the_relay_reads_it_again_THEN_it_reports_the_new_value() {
        StubUncoveredBucketsSource buckets = new StubUncoveredBucketsSource(BUCKETS);
        buckets.setUncoveredBuckets(2);
        RecordingMetrics metrics = new RecordingMetrics();
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(1).pollInterval(Duration.ofMillis(10))
                .metricsInterval(Duration.ofMillis(20)).build();
        WorkerPool pool = new WorkerPool(new InMemoryOutbox(), new RecordingDispatcher(), cfg, metrics,
                Clock.systemUTC(), BackoffStrategy.fullJitter(), buckets);

        pool.start();
        try {
            awaitUpTo(Duration.ofSeconds(20), () -> "uncoveredBuckets:" + metrics.uncoveredBuckets(),
                    () -> metrics.uncoveredBuckets() == 2);

            // Coverage stalls resolve (a heartbeat rebalances, or an operator scales the fleet) — the
            // reported value must follow, not stick at whatever it first observed.
            buckets.setUncoveredBuckets(0);
            awaitUpTo(Duration.ofSeconds(20), () -> "uncoveredBuckets:" + metrics.uncoveredBuckets(),
                    () -> metrics.uncoveredBuckets() == 0);
        } finally {
            pool.stop();
        }
    }

    @Test
    void GIVEN_no_metrics_adapter_WHEN_the_relay_runs_THEN_the_backlog_is_never_queried() {
        CountingStore store = new CountingStore(new InMemoryOutbox());
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(1).pollInterval(Duration.ofMillis(10))
                .metricsInterval(Duration.ofMillis(10)).build();
        WorkerPool pool = new WorkerPool(store, new RecordingDispatcher(), cfg);   // metrics: the no-op default

        pool.start();
        try {
            // 20 poll cycles at a 10ms interval leave ample room for a 10ms metrics tick to have fired.
            awaitUpTo(Duration.ofSeconds(20),
                    () -> "20 poll cycles, reached only " + store.claims(),
                    () -> store.claims() >= 20);
        } finally {
            pool.stop();
        }

        // An outbox nobody is watching must not pay for the gauges — not even one query.
        assertThat(store.lagQueries()).isZero();
    }

    /** Delegates every call to a real {@link InMemoryOutbox}; each subclass bends exactly one of them. */
    private abstract static class DelegatingStore implements OutboxStore {
        private final InMemoryOutbox delegate;

        DelegatingStore(InMemoryOutbox delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
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
    }

    /** Counts how often the relay claimed work and how often it read the lag gauge. */
    private static final class CountingStore extends DelegatingStore {
        private final AtomicInteger claims = new AtomicInteger();
        private final AtomicInteger lagQueries = new AtomicInteger();

        CountingStore(InMemoryOutbox delegate) {
            super(delegate);
        }

        @Override
        public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
            claims.incrementAndGet();
            return super.claimBatch(buckets, workerId, lease, batchSize);
        }

        @Override
        public Optional<LagSnapshot> lag() {
            lagQueries.incrementAndGet();
            return super.lag();
        }

        int claims() {
            return claims.get();
        }

        int lagQueries() {
            return lagQueries.get();
        }
    }

    /** Lets the first claim kill its worker thread with an {@link Error}, then behaves normally. */
    private static final class CrashingOnceStore extends DelegatingStore {
        private final AtomicBoolean crashed = new AtomicBoolean();

        CrashingOnceStore(InMemoryOutbox delegate) {
            super(delegate);
        }

        @Override
        public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
            if (crashed.compareAndSet(false, true)) {
                throw new Error("simulated fatal failure while claiming");
            }
            return super.claimBatch(buckets, workerId, lease, batchSize);
        }

        boolean crashed() {
            return crashed.get();
        }
    }

    /** Reports whatever {@code failedCount} was last set to, independent of the delegate's own rows. */
    private static final class StubFailedCountStore extends DelegatingStore {
        private final AtomicLong failedCount = new AtomicLong();

        StubFailedCountStore(InMemoryOutbox delegate) {
            super(delegate);
        }

        @Override
        public OptionalLong failedCount() {
            return OptionalLong.of(failedCount.get());
        }

        void setFailedCount(long value) {
            failedCount.set(value);
        }
    }

    /** Reports whatever {@code uncoveredBuckets} was last set to; owns every bucket unconditionally. */
    private static final class StubUncoveredBucketsSource implements BucketSource {
        private final Set<Integer> all;
        private final AtomicInteger uncovered = new AtomicInteger();

        StubUncoveredBucketsSource(int bucketCount) {
            all = new HashSet<>();
            for (int b = 0; b < bucketCount; b++) {
                all.add(b);
            }
        }

        @Override
        public Set<Integer> ownedBuckets() {
            return all;
        }

        @Override
        public OptionalInt uncoveredBuckets() {
            return OptionalInt.of(uncovered.get());
        }

        void setUncoveredBuckets(int value) {
            uncovered.set(value);
        }
    }

    /** A real, in-memory {@link BucketSource} that counts {@link #release()} calls — no mocks. */
    private static final class RecordingBucketSource implements BucketSource {
        private final Set<Integer> all;
        private final AtomicInteger releaseCalls = new AtomicInteger();

        RecordingBucketSource(int bucketCount) {
            all = new HashSet<>();
            for (int b = 0; b < bucketCount; b++) {
                all.add(b);
            }
        }

        @Override
        public Set<Integer> ownedBuckets() {
            return all;
        }

        @Override
        public void release() {
            releaseCalls.incrementAndGet();
        }

        int releaseCalls() {
            return releaseCalls.get();
        }
    }

    /**
     * Waits for {@code condition}, and on timeout fails naming what was awaited and what was actually
     * observed — {@code expected} is a supplier so it can render live state at the moment of failure.
     * A bare "condition not met" is what turns a timing flake into a mystery; worse, a caller that
     * asserts the same state afterwards reports the mismatch, not the timeout that caused it.
     */
    private static void awaitUpTo(Duration timeout, Supplier<String> expected, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            if (condition.getAsBoolean()) {
                return;
            }
            if (System.nanoTime() >= deadline) {   // checked after the condition: never time out unseen
                throw new AssertionError("Timed out after " + timeout + " awaiting " + expected.get());
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while awaiting " + expected.get(), e);
            }
        }
    }
}
