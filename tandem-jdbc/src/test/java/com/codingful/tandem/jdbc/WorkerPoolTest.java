package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.test.InMemoryOutbox;
import com.codingful.tandem.test.RecordingDispatcher;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
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
            awaitUpTo(Duration.ofSeconds(20), () -> outbox.byStatus(OutboxStatus.DONE).size() == total);
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

    private static void awaitUpTo(Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while awaiting condition", e);
            }
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
