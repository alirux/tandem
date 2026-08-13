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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    void GIVEN_the_relay_is_paused_WHEN_events_are_pending_THEN_nothing_is_claimed() throws InterruptedException {
        InMemoryOutbox outbox = new InMemoryOutbox();
        outbox.insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload("p".getBytes()).build());
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        RelayConfig cfg = RelayConfig.builder().bucketCount(BUCKETS).pollInterval(Duration.ofMillis(10)).build();
        MutableControlSource control = new MutableControlSource();
        control.setWholeRelayPaused(true);
        WorkerPool pool = new WorkerPool(outbox, dispatcher, cfg, TandemMetrics.NOOP, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), BucketSource.embedded(BUCKETS), control);

        pool.start();
        try {
            Thread.sleep(200);   // several poll intervals - nothing should ever be claimed
            assertThat(dispatcher.dispatchCount()).isZero();
            assertThat(outbox.byStatus(OutboxStatus.PENDING)).hasSize(1);
        } finally {
            pool.stop();
        }
    }

    @Test
    void GIVEN_a_paused_bucket_WHEN_the_relay_runs_THEN_only_that_buckets_events_stay_pending() {
        InMemoryOutbox outbox = new InMemoryOutbox();
        outbox.insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload("p".getBytes()).build());
        outbox.insert(OutboxMessage.builder()
                .aggregateId("order-2").aggregateType("Order").seq(1).payload("p".getBytes()).build());
        long pausedId = outbox.all().get(0).id();
        int pausedBucket = outbox.bucketOf(pausedId);
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        RelayConfig cfg = RelayConfig.builder().bucketCount(BUCKETS).pollInterval(Duration.ofMillis(10)).build();
        MutableControlSource control = new MutableControlSource();
        control.pauseBucket(pausedBucket);
        WorkerPool pool = new WorkerPool(outbox, dispatcher, cfg, TandemMetrics.NOOP, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), BucketSource.embedded(BUCKETS), control);

        pool.start();
        try {
            awaitUpTo(Duration.ofSeconds(10),
                    () -> "the unpaused aggregate delivered, got " + outbox.statusCounts(),
                    () -> outbox.byStatus(OutboxStatus.DONE).size() == 1);
            assertThat(outbox.byId(pausedId).status()).isEqualTo(OutboxStatus.PENDING);
        } finally {
            pool.stop();
        }
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
                            + ", workerCycleAgeSeconds:" + metrics.workerCycleAgeSeconds()
                            + ", inFlight:" + outbox.byStatus(OutboxStatus.IN_FLIGHT).size(),
                    () -> outbox.byStatus(OutboxStatus.IN_FLIGHT).size() == 1
                            && metrics.lag() == 2
                            && metrics.lagAgeSeconds() >= 0
                            && metrics.activeWorkers() >= 0
                            && metrics.workerCycleAgeSeconds() >= 0);

            // Assert on the running relay, never after the stop: the gauges keep their latest reading,
            // and a metrics tick that races stop() reports the workers it finds already interrupted —
            // a truthful reading of a stopping relay, but not the one under test here.
            // The head is in flight; its two successors are still waiting, and have been for 30s.
            assertThat(metrics.lag()).isEqualTo(2);
            assertThat(metrics.lagAgeSeconds()).isEqualTo(30);
            assertThat(metrics.activeWorkers()).isEqualTo(2);
            // Both workers keep completing cycles against the same frozen clock used to stamp them, so
            // a healthy, busy relay reads exactly zero here — never a stale, ever-growing age.
            assertThat(metrics.workerCycleAgeSeconds()).isZero();
        } finally {
            pool.stop();
        }
    }

    @Test
    void GIVEN_a_relay_never_started_WHEN_its_status_is_read_THEN_it_reports_stopped_with_no_live_workers() {
        RelayConfig cfg = RelayConfig.builder().bucketCount(BUCKETS).workersPerInstance(3).build();
        WorkerPool pool = new WorkerPool(new InMemoryOutbox(), new RecordingDispatcher(), cfg);

        RelayStatus status = pool.status();

        assertThat(status.state()).isEqualTo(RelayStatus.State.STOPPED);
        assertThat(status.workersConfigured()).isEqualTo(3);
        assertThat(status.workersAlive()).isZero();
        assertThat(status.oldestWorkerCycle()).isEmpty();
    }

    @Test
    void GIVEN_a_running_relay_WHEN_its_status_is_read_THEN_it_reports_every_worker_alive_and_progressing() {
        InMemoryOutbox outbox = new InMemoryOutbox();
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(2).instanceId("relay-under-test")
                .pollInterval(Duration.ofMillis(10)).build();
        WorkerPool pool = new WorkerPool(outbox, new RecordingDispatcher(), cfg);
        Instant beforeStart = Instant.now();

        pool.start();
        try {
            awaitUpTo(Duration.ofSeconds(20), () -> "every worker to have completed a cycle, got " + pool.status(),
                    () -> pool.status().workersAlive() == 2);

            RelayStatus status = pool.status();

            assertThat(status.instanceId()).isEqualTo("relay-under-test");
            assertThat(status.state()).isEqualTo(RelayStatus.State.RUNNING);
            assertThat(status.coordination()).isEqualTo(Coordination.SINGLE);
            assertThat(status.workersConfigured()).isEqualTo(2);
            assertThat(status.workersAlive()).isEqualTo(2);
            // Not before pool.start() was called: a worker's first cycle timestamp is set no earlier
            // than the moment its thread was started (§3.8), never left over from construction.
            assertThat(status.oldestWorkerCycle()).isPresent().get().satisfies(cycleAt ->
                    assertThat(cycleAt).isAfterOrEqualTo(beforeStart));
        } finally {
            pool.stop();
        }
    }

    @Test
    void GIVEN_a_worker_thread_alive_but_stuck_failing_every_claim_WHEN_its_status_is_read_THEN_its_cycle_timestamp_does_not_advance() {
        // A live thread is not a working one: mark_only-on-success (§3.8) means a worker whose every
        // iteration throws before completing keeps its start-time cycle forever, even while its thread
        // stays alive and its uncaught-exception handler never fires (the exception is caught per
        // iteration inside runWorker, not escaping the thread).
        ControllableClock clock = ControllableClock.atEpochDay();
        OutboxStore alwaysThrows = new OutboxStore() {
            @Override
            public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
                throw new RuntimeException("simulated stuck claim");
            }

            @Override
            public void markDone(long id) {
            }

            @Override
            public void markForRetry(long id, String error, Duration retryDelay) {
            }

            @Override
            public void markFailed(long id, String error) {
            }

            @Override
            public int reclaimExpiredLeases() {
                return 0;
            }

            @Override
            public int cleanup(Instant doneBefore, int batchSize) {
                return 0;
            }
        };
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(1).pollInterval(Duration.ofMillis(10)).build();
        WorkerPool pool = new WorkerPool(alwaysThrows, new RecordingDispatcher(), cfg, TandemMetrics.NOOP, clock,
                BackoffStrategy.fullJitter(), BucketSource.embedded(BUCKETS));
        Instant startedAt = clock.instant();

        pool.start();
        try {
            // The thread is alive from the first status() read onward; give it several failing
            // iterations to prove the timestamp really is pinned, not merely not-yet-updated.
            awaitUpTo(Duration.ofSeconds(20), () -> "the worker thread to be alive, got " + pool.status(),
                    () -> pool.status().workersAlive() == 1);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            RelayStatus status = pool.status();

            assertThat(status.workersAlive()).isEqualTo(1);
            assertThat(status.oldestWorkerCycle()).contains(startedAt);
        } finally {
            pool.stop();
        }
    }

    @Test
    void GIVEN_a_worker_stuck_failing_every_claim_WHEN_the_metrics_adapter_reads_THEN_the_cycle_age_gauge_climbs() {
        // The behaviour recordWorkerCycleAgeSeconds exists for: a relay running but not making progress
        // (HLD §7) must be visible as a single climbing gauge, not only inferred by correlating four
        // others. The clock is frozen except for the explicit advance below, so the reading is exact,
        // not merely "eventually positive".
        ControllableClock clock = ControllableClock.atEpochDay();
        OutboxStore alwaysThrows = new OutboxStore() {
            @Override
            public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
                throw new RuntimeException("simulated stuck claim");
            }

            @Override
            public void markDone(long id) {
            }

            @Override
            public void markForRetry(long id, String error, Duration retryDelay) {
            }

            @Override
            public void markFailed(long id, String error) {
            }

            @Override
            public int reclaimExpiredLeases() {
                return 0;
            }

            @Override
            public int cleanup(Instant doneBefore, int batchSize) {
                return 0;
            }
        };
        RecordingMetrics metrics = new RecordingMetrics();
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(1).pollInterval(Duration.ofMillis(10))
                .metricsInterval(Duration.ofMillis(20)).build();
        WorkerPool pool = new WorkerPool(alwaysThrows, new RecordingDispatcher(), cfg, metrics, clock,
                BackoffStrategy.fullJitter(), BucketSource.embedded(BUCKETS));

        pool.start();
        try {
            // A first, unstuck reading: the clock has not moved since the worker's start-time stamp.
            awaitUpTo(Duration.ofSeconds(20),
                    () -> "a first reading of a not-yet-stuck worker, got " + metrics.workerCycleAgeSeconds(),
                    () -> metrics.workerCycleAgeSeconds() == 0);

            clock.advance(Duration.ofSeconds(45));   // the worker has been failing to progress ever since

            awaitUpTo(Duration.ofSeconds(20),
                    () -> "the cycle-age gauge to reflect 45s of no progress, got " + metrics.workerCycleAgeSeconds(),
                    () -> metrics.workerCycleAgeSeconds() == 45);
        } finally {
            pool.stop();
        }
    }

    @Test
    void GIVEN_a_database_that_keeps_failing_WHEN_the_relay_retries_THEN_it_slows_down_instead_of_hammering_it()
            throws InterruptedException {
        // Without a growing backoff a worker retries at pollInterval forever — here ~100 times a
        // second, each writing an ERROR with a stack trace, so an outage becomes a log flood on top
        // of itself (§3.1). The bound is deliberately loose: this pins the order of magnitude, not a
        // schedule, so it cannot go flaky on a slow machine.
        AlwaysFailingStore store = new AlwaysFailingStore(new InMemoryOutbox());
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(1).pollInterval(Duration.ofMillis(10))
                .reclaimInterval(Duration.ofMillis(500)).build();
        WorkerPool pool = new WorkerPool(store, new RecordingDispatcher(), cfg);

        pool.start();
        try {
            awaitUpTo(Duration.ofSeconds(20), () -> "the worker to have tried at least once",
                    () -> store.claimAttempts() >= 1);
            Thread.sleep(1_000);

            // A fixed 10 ms retry would be ~100 attempts here; the backoff reaches its 500 ms cap
            // (reclaimInterval) after six doublings, so a handful is what a second buys.
            assertThat(store.claimAttempts()).isLessThan(20);
            // ...and it must still be retrying: backing off is not the same as giving up, or the
            // relay would never notice the database coming back.
            int soFar = store.claimAttempts();
            awaitUpTo(Duration.ofSeconds(20), () -> "the worker to keep retrying after backing off",
                    () -> store.claimAttempts() > soFar);
        } finally {
            pool.stop();
        }
    }

    @Test
    void GIVEN_a_shutdown_in_progress_WHEN_its_status_is_read_THEN_it_is_reported_as_draining_not_stopped() {
        // Deliberately latch-driven, not timing-driven: a worker is parked inside claimBatch() until
        // released, so the window in which stop() is mid-join is fully under the test's control —
        // this is the one status a caller must be able to tell apart from a hard STOPPED (§3.8), since
        // an application draining an instance out of a load balancer must not report it as a failure.
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        OutboxStore parksOnClaim = new OutboxStore() {
            @Override
            public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    // interrupt() is exactly how stop() wakes this worker; clear the flag and hold the
                    // window open a little longer so the assertion below cannot race the actual exit.
                    Thread.interrupted();
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }
                return List.of();
            }

            @Override
            public void markDone(long id) {
            }

            @Override
            public void markForRetry(long id, String error, Duration retryDelay) {
            }

            @Override
            public void markFailed(long id, String error) {
            }

            @Override
            public int reclaimExpiredLeases() {
                return 0;
            }

            @Override
            public int cleanup(Instant doneBefore, int batchSize) {
                return 0;
            }
        };
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(1).pollInterval(Duration.ofMillis(10)).build();
        WorkerPool pool = new WorkerPool(parksOnClaim, new RecordingDispatcher(), cfg, TandemMetrics.NOOP,
                Clock.systemUTC(), BackoffStrategy.fullJitter(), BucketSource.embedded(BUCKETS));

        pool.start();
        try {
            entered.await();   // the worker is now parked inside claimBatch()

            Thread stopper = new Thread(pool::stop);
            stopper.start();
            try {
                awaitUpTo(Duration.ofSeconds(5), () -> "status to report a draining shutdown, got " + pool.status(),
                        () -> pool.status().state() == RelayStatus.State.STOPPING);
                assertThat(pool.status().state()).isEqualTo(RelayStatus.State.STOPPING);
            } finally {
                release.countDown();
                stopper.join(TimeUnit.SECONDS.toMillis(20));
            }

            assertThat(pool.status().state()).isEqualTo(RelayStatus.State.STOPPED);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while awaiting the worker to park", e);
        }
    }

    @Test
    void GIVEN_a_stopped_relay_WHEN_its_status_is_read_after_shutdown_THEN_no_worker_is_reported_alive() {
        InMemoryOutbox outbox = new InMemoryOutbox();
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(2).pollInterval(Duration.ofMillis(10)).build();
        WorkerPool pool = new WorkerPool(outbox, new RecordingDispatcher(), cfg);
        pool.start();
        awaitUpTo(Duration.ofSeconds(20), () -> "every worker to have started", () -> pool.status().workersAlive() == 2);

        pool.stop();

        RelayStatus status = pool.status();
        assertThat(status.state()).isEqualTo(RelayStatus.State.STOPPED);
        assertThat(status.workersAlive()).isZero();
        assertThat(status.oldestWorkerCycle()).isEmpty();
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
    void GIVEN_a_chain_stuck_behind_a_failure_WHEN_the_relay_reports_metrics_THEN_the_blocked_rows_are_counted_apart() {
        InMemoryOutbox outbox = new InMemoryOutbox();
        outbox.insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build());
        outbox.insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(2).payload("{}".getBytes()).build());
        RecordingDispatcher dispatcher = new RecordingDispatcher().failAll(false);   // permanent
        RecordingMetrics metrics = new RecordingMetrics();
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(BUCKETS).workersPerInstance(1).pollInterval(Duration.ofMillis(10))
                .metricsInterval(Duration.ofMillis(20)).build();
        WorkerPool pool = new WorkerPool(outbox, dispatcher, cfg, metrics, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), BucketSource.embedded(BUCKETS));

        pool.start();
        try {
            awaitUpTo(Duration.ofSeconds(20), () -> "blocked:" + metrics.blocked(), () -> metrics.blocked() == 1);

            // The backlog gauge keeps counting the same row, on purpose: it is an undelivered event and
            // hiding it would report an empty outbox while the aggregate is stalled. What the two
            // readings together say is "everything still waiting is waiting on a failure" — which is a
            // different incident from a relay that cannot keep up, and needs a different response.
            assertThat(metrics.lag()).isEqualTo(1);
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

    /** A database that is down: every claim throws, and the attempts are counted. */
    private static final class AlwaysFailingStore extends DelegatingStore {
        private final AtomicInteger claimAttempts = new AtomicInteger();

        AlwaysFailingStore(InMemoryOutbox delegate) {
            super(delegate);
        }

        @Override
        public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
            claimAttempts.incrementAndGet();
            throw new RuntimeException("simulated database outage");
        }

        int claimAttempts() {
            return claimAttempts.get();
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

    /** A real, mutable {@link RelayControlSource} for tests — no scheduled refresh, set directly. */
    private static final class MutableControlSource implements RelayControlSource {
        private volatile boolean wholeRelayPaused;
        private final Set<Integer> pausedBuckets = ConcurrentHashMap.newKeySet();

        void setWholeRelayPaused(boolean paused) {
            this.wholeRelayPaused = paused;
        }

        void pauseBucket(int bucket) {
            pausedBuckets.add(bucket);
        }

        @Override
        public boolean wholeRelayPaused() {
            return wholeRelayPaused;
        }

        @Override
        public boolean bucketPaused(int bucket) {
            return pausedBuckets.contains(bucket);
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
