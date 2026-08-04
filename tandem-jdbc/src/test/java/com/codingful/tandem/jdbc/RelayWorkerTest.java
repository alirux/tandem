package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.test.ControllableClock;
import com.codingful.tandem.test.InMemoryOutbox;
import com.codingful.tandem.test.RecordingDispatcher;
import com.codingful.tandem.test.RecordingMetrics;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RelayWorkerTest {

    private static final int BUCKETS = 256;
    private static final int MAX_ATTEMPTS = 3;

    private ControllableClock clock;
    private InMemoryOutbox outbox;
    private RecordingDispatcher dispatcher;
    private RelayConfig cfg;

    @BeforeEach
    void setUp() {
        clock = ControllableClock.atEpochDay();
        outbox = new InMemoryOutbox(BUCKETS, MAX_ATTEMPTS, clock);
        dispatcher = new RecordingDispatcher();
        cfg = RelayConfig.builder().bucketCount(BUCKETS).batchSize(100).maxAttempts(MAX_ATTEMPTS).build();
    }

    /** A worker owning every bucket, with a fixed 10s backoff so retry timing is deterministic. */
    private RelayWorker worker() {
        Supplier<Set<Integer>> allBuckets = outbox::allBuckets;
        return new RelayWorker(outbox, dispatcher, cfg, attempts -> Duration.ofSeconds(10),
                TandemMetrics.NOOP, clock, "worker-1", allBuckets);
    }

    private RelayWorker worker(BackoffStrategy backoff, TandemMetrics metrics) {
        return new RelayWorker(outbox, dispatcher, cfg, backoff, metrics, clock, "worker-1", outbox::allBuckets);
    }

    private void insert(String aggregateId, long seq) {
        outbox.insert(OutboxMessage.builder()
                .aggregateId(aggregateId).aggregateType("Order").seq(seq)
                .payload(("p-" + aggregateId + '-' + seq).getBytes()).build());
    }

    /** Drive the worker to quiescence (auto-completing dispatcher settles synchronously). */
    private void drain(RelayWorker worker) {
        int guard = 0;
        while (true) {
            int claimed = worker.claimAndDispatch();
            worker.flushDone();
            worker.flushFailures();
            if (claimed == 0 && worker.inFlight() == 0) {
                return;
            }
            if (++guard > 10_000) {
                throw new IllegalStateException("relay did not quiesce");
            }
        }
    }

    @Test
    void GIVEN_chains_across_aggregates_WHEN_the_relay_runs_THEN_every_event_is_delivered_in_per_aggregate_order() {
        insert("order-1", 1);
        insert("order-1", 2);
        insert("order-1", 3);
        insert("order-2", 1);
        insert("order-2", 2);

        drain(worker());

        assertThat(outbox.byStatus(OutboxStatus.DONE)).hasSize(5);
        // Within each aggregate, the published seqs are strictly increasing.
        Map<String, List<Long>> seqsByAggregate = dispatcher.dispatched().stream()
                .collect(Collectors.groupingBy(r -> r.aggregateId().value(),
                        Collectors.mapping(OutboxRecord::seq, Collectors.toList())));
        assertThat(seqsByAggregate.get("order-1")).containsExactly(1L, 2L, 3L);
        assertThat(seqsByAggregate.get("order-2")).containsExactly(1L, 2L);
    }

    @Test
    void GIVEN_a_pending_chain_WHEN_one_relay_cycle_runs_THEN_only_the_head_of_each_aggregate_is_published() {
        insert("order-1", 1);   // id 1 — head
        insert("order-1", 2);   // id 2 — blocked
        insert("order-2", 1);   // id 3 — head

        RelayWorker worker = worker();
        worker.claimAndDispatch();
        worker.flushDone();

        // Only the two heads were published in the first cycle; the blocked successor was not.
        assertThat(dispatcher.dispatchedIds()).containsExactly(1L, 3L);
    }

    @Test
    void GIVEN_a_permanently_failing_head_WHEN_the_relay_runs_THEN_only_that_aggregate_is_blocked() {
        insert("order-1", 1);   // id 1 — will fail permanently
        insert("order-1", 2);   // id 2 — blocked behind the poison head
        insert("order-2", 1);   // id 3 — unaffected
        dispatcher.failRecord(1, false);

        drain(worker());

        assertThat(outbox.byId(1).status()).isEqualTo(OutboxStatus.FAILED);
        assertThat(outbox.byId(2).status()).isEqualTo(OutboxStatus.PENDING);   // gated by the FAILED head
        assertThat(outbox.byId(3).status()).isEqualTo(OutboxStatus.DONE);
    }

    @Test
    void GIVEN_a_retriable_failure_WHEN_the_relay_runs_THEN_the_row_backs_off_then_succeeds_once_due() {
        insert("order-1", 1);
        dispatcher.failRecord(1, true, 1);   // fail once retriably, then succeed

        RelayWorker worker = worker();
        drain(worker);

        // After the first (failed) attempt it is PENDING again, scheduled 10s out — not yet retried.
        OutboxRecord afterFailure = outbox.byId(1);
        assertThat(afterFailure.status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(afterFailure.attempts()).isEqualTo(1);
        assertThat(afterFailure.nextAttemptAt()).isEqualTo(clock.instant().plus(Duration.ofSeconds(10)));

        // Once the backoff elapses, the retry succeeds.
        clock.advance(Duration.ofSeconds(11));
        drain(worker);
        assertThat(outbox.byId(1).status()).isEqualTo(OutboxStatus.DONE);
    }

    @Test
    void GIVEN_a_row_that_keeps_failing_retriably_WHEN_the_attempt_ceiling_is_reached_THEN_it_is_failed() {
        insert("order-1", 1);
        dispatcher.failRecord(1, true);   // always retriable

        // Zero backoff so each retry is immediately due within a single drain.
        RelayWorker worker = worker(attempts -> Duration.ZERO, TandemMetrics.NOOP);
        drain(worker);

        OutboxRecord row = outbox.byId(1);
        assertThat(row.status()).isEqualTo(OutboxStatus.FAILED);
        assertThat(row.attempts()).isEqualTo(MAX_ATTEMPTS);
        assertThat(dispatcher.dispatchCount()).isEqualTo(MAX_ATTEMPTS);
    }

    @Test
    void GIVEN_a_batch_of_distinct_aggregates_WHEN_dispatch_is_in_flight_THEN_they_overlap_before_any_is_marked_done() {
        insert("order-1", 1);
        insert("order-2", 1);
        insert("order-3", 1);
        dispatcher.manualCompletion();

        RelayWorker worker = worker();
        int claimed = worker.claimAndDispatch();

        // All three distinct aggregates are in flight at once (the per-shard concurrency window).
        assertThat(claimed).isEqualTo(3);
        assertThat(worker.inFlight()).isEqualTo(3);
        assertThat(dispatcher.pendingCount()).isEqualTo(3);
        assertThat(worker.flushDone()).isZero();   // nothing acked yet
        assertThat(outbox.byStatus(OutboxStatus.IN_FLIGHT)).hasSize(3);

        dispatcher.completeAll();
        assertThat(worker.inFlight()).isZero();
        assertThat(worker.flushDone()).isEqualTo(3);
        assertThat(outbox.byStatus(OutboxStatus.DONE)).hasSize(3);
    }

    @Test
    void GIVEN_a_failed_dispatch_WHEN_not_yet_flushed_THEN_the_store_is_untouched_until_flushFailures() {
        // The completion handler runs on the dispatcher's I/O thread and must not do JDBC: it only
        // enqueues the failure; the store write happens in flushFailures() on the worker thread.
        insert("order-1", 1);
        dispatcher.failRecord(1, true);

        RelayWorker worker = worker();
        worker.claimAndDispatch();   // auto mode settles the future (exceptionally) synchronously

        assertThat(worker.inFlight()).isZero();                                  // completion already ran…
        assertThat(outbox.byId(1).status()).isEqualTo(OutboxStatus.IN_FLIGHT);   // …but wrote nothing yet

        assertThat(worker.flushFailures()).isEqualTo(1);
        OutboxRecord afterFlush = outbox.byId(1);
        assertThat(afterFlush.status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(afterFlush.attempts()).isEqualTo(1);
    }

    @Test
    void GIVEN_metrics_enabled_WHEN_the_relay_publishes_and_retries_THEN_the_counts_are_recorded() {
        insert("order-1", 1);   // succeeds
        insert("order-2", 1);   // fails permanently
        dispatcher.failRecord(2, false);
        RecordingMetrics metrics = new RecordingMetrics();

        drain(worker(attempts -> Duration.ZERO, metrics));

        assertThat(metrics.published()).isEqualTo(1);
        // Not retried: a permanent failure never calls incrementRetry. failed.count itself is not
        // this worker's job — it's a live read of the store (WorkerPool.metricsTick, LLD-jdbc §4),
        // not something RelayWorker reports per event, so it is exercised at that level instead.
        assertThat(metrics.retries()).isZero();
    }

    @Test
    void GIVEN_metrics_enabled_WHEN_a_row_is_published_THEN_publish_latency_is_the_gap_from_created_at_to_ack() {
        insert("order-1", 1);   // createdAt stamped at the clock's current instant
        clock.advance(Duration.ofMillis(250));   // dispatch settles synchronously at the advanced instant
        RecordingMetrics metrics = new RecordingMetrics();

        drain(worker(attempts -> Duration.ZERO, metrics));

        assertThat(metrics.publishLatencies()).containsExactly(Duration.ofMillis(250));
    }

    @Test
    void GIVEN_metrics_enabled_WHEN_a_dispatch_fails_permanently_THEN_no_publish_latency_is_recorded() {
        insert("order-1", 1);
        dispatcher.failRecord(1, false);
        RecordingMetrics metrics = new RecordingMetrics();

        drain(worker(attempts -> Duration.ZERO, metrics));

        assertThat(metrics.publishLatencies()).isEmpty();
    }

    @Test
    void GIVEN_a_mix_of_successes_and_failures_WHEN_the_relay_runs_THEN_ok_and_ko_are_tallied_separately() {
        insert("order-1", 1);   // succeeds
        insert("order-2", 1);   // fails permanently
        insert("order-3", 1);   // succeeds
        dispatcher.failRecord(2, false);

        RelayWorker worker = worker(attempts -> Duration.ZERO, TandemMetrics.NOOP);
        drain(worker);

        assertThat(worker.okCount()).isEqualTo(2);
        assertThat(worker.koCount()).isEqualTo(1);
    }

    @Test
    void GIVEN_a_row_that_is_retried_before_succeeding_WHEN_the_relay_runs_THEN_every_attempt_counts_toward_ko_or_ok() {
        insert("order-1", 1);
        dispatcher.failRecord(1, true, 1);   // fail once retriably, then succeed

        RelayWorker worker = worker(attempts -> Duration.ZERO, TandemMetrics.NOOP);
        drain(worker);

        // Two attempts total: the failed one and the retry that succeeded.
        assertThat(worker.koCount()).isEqualTo(1);
        assertThat(worker.okCount()).isEqualTo(1);
    }

    @Test
    void GIVEN_only_a_multiple_of_the_configured_threshold_WHEN_checked_THEN_it_crosses_the_log_boundary() {
        assertThat(RelayWorker.crossesLogThreshold(9, 10)).isFalse();
        assertThat(RelayWorker.crossesLogThreshold(10, 10)).isTrue();
        assertThat(RelayWorker.crossesLogThreshold(11, 10)).isFalse();
        assertThat(RelayWorker.crossesLogThreshold(20, 10)).isTrue();
        assertThat(RelayWorker.crossesLogThreshold(1, 1)).isTrue();
    }

    @Test
    void GIVEN_a_progress_log_threshold_of_one_row_WHEN_the_relay_runs_THEN_every_outcome_still_crosses_it_without_losing_the_tally() {
        insert("order-1", 1);   // succeeds
        insert("order-2", 1);   // fails permanently
        insert("order-3", 1);   // succeeds
        dispatcher.failRecord(2, false);
        RelayConfig lowThreshold = RelayConfig.builder()
                .bucketCount(BUCKETS).batchSize(100).maxAttempts(MAX_ATTEMPTS).logEveryRows(1).build();
        RelayWorker worker = new RelayWorker(outbox, dispatcher, lowThreshold,
                attempts -> Duration.ZERO, TandemMetrics.NOOP, clock, "worker-1", outbox::allBuckets);

        drain(worker);

        assertThat(worker.okCount()).isEqualTo(2);
        assertThat(worker.koCount()).isEqualTo(1);
    }
}
