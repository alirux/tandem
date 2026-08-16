package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.test.ControllableClock;
import com.codingful.tandem.test.InMemoryOutbox;
import com.codingful.tandem.test.RecordingDispatcher;
import com.codingful.tandem.test.RecordingMetrics;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The write-side ordering detector (§3.9, HLD §8): the relay is the only witness to the {@code seq}
 * order events actually went out in, because the rows themselves are left perfectly ordered.
 *
 * <p>The out-of-order publish is staged here by inserting the higher {@code seq} first, so it takes the
 * lower {@code id} and the head-of-chain claim reaches it first. That is the same thing the real hazard
 * produces at the relay — two unserialised writers whose commit order inverts their insert order — with
 * the visibility race replaced by something a unit test can control; {@code CommitOrderReorderIT} pins
 * the genuine mechanism against a real database.
 */
class RelayWorkerSeqRegressionTest {

    private static final int BUCKETS = 256;
    private static final AggregateId ORDER_1 = AggregateId.of("order-1");

    private ControllableClock clock;
    private InMemoryOutbox outbox;
    private RecordingDispatcher dispatcher;
    private RecordingMetrics metrics;

    @BeforeEach
    void setUp() {
        clock = ControllableClock.atEpochDay();
        outbox = new InMemoryOutbox(BUCKETS, 3, clock);
        dispatcher = new RecordingDispatcher();
        metrics = new RecordingMetrics();
    }

    @Test
    void GIVEN_events_published_in_order_WHEN_the_relay_runs_THEN_no_ordering_violation_is_reported() {
        insert(ORDER_1, 1);
        insert(ORDER_1, 2);
        insert(ORDER_1, 3);

        drain(worker(outbox));

        assertThat(publishedSeqs()).containsExactly(1L, 2L, 3L);
        assertThat(metrics.seqRegressions()).isZero();
    }

    @Test
    void GIVEN_writers_to_one_aggregate_were_not_serialised_WHEN_the_relay_publishes_them_out_of_order_THEN_the_violation_is_reported() {
        insert(ORDER_1, 2);
        insert(ORDER_1, 1);

        drain(worker(outbox));

        assertThat(publishedSeqs()).containsExactly(2L, 1L);
        assertThat(metrics.seqRegressions()).isEqualTo(1);
    }

    /**
     * The false positive that got the first implementation of this detector reverted: an operator replay
     * re-publishes an event the relay already sent, which is indistinguishable from a reordered write on
     * every field except {@code replays}. Firing here would send an operator after a write-side bug
     * during the very replay they are running to recover from one.
     */
    @Test
    void GIVEN_an_operator_replays_an_already_published_event_WHEN_it_goes_out_again_THEN_no_violation_is_reported() {
        insert(ORDER_1, 1);
        insert(ORDER_1, 2);
        insert(ORDER_1, 3);
        RelayWorker worker = worker(outbox);
        drain(worker);

        outbox.replay(new ReplayCriteria(ORDER_1, null, 1L, 1L, Set.of(), false));
        drain(worker);

        assertThat(publishedSeqs()).containsExactly(1L, 2L, 3L, 1L);
        assertThat(metrics.seqRegressions()).isZero();
    }

    /**
     * A replayed row stays suppressed, but only that row: the watermark is never lowered to it, so the
     * aggregate keeps its history and a genuine regression after a replay is still caught.
     */
    @Test
    void GIVEN_an_aggregate_was_replayed_WHEN_a_never_replayed_event_later_goes_out_backwards_THEN_the_violation_is_reported() {
        insert(ORDER_1, 5);
        insert(ORDER_1, 6);
        RelayWorker worker = worker(outbox);
        drain(worker);
        outbox.replay(new ReplayCriteria(ORDER_1, null, 1L, 1L, Set.of(), false));
        drain(worker);

        insert(ORDER_1, 4);
        drain(worker);

        assertThat(publishedSeqs()).containsExactly(5L, 6L, 5L, 4L);
        assertThat(metrics.seqRegressions()).isEqualTo(1);
    }

    /**
     * An at-least-once redelivery of the same event is not an ordering fault (§3.9) — and unlike a
     * replay it leaves {@code replays} at zero, so nothing suppresses it downstream: the equal-{@code seq}
     * case has to be right on its own.
     */
    @Test
    void GIVEN_a_lease_expired_before_the_ack_WHEN_the_same_event_is_published_again_THEN_no_violation_is_reported() {
        dispatcher.manualCompletion();
        insert(ORDER_1, 1);
        RelayWorker worker = worker(outbox);
        worker.claimAndDispatch();                 // in flight, ack outstanding
        clock.advance(Duration.ofMinutes(5));
        assertThat(outbox.reclaimExpiredLeases()).isEqualTo(1);
        worker.claimAndDispatch();                 // the same row, claimed and sent a second time
        dispatcher.completeAll();

        worker.flushDone();

        assertThat(publishedSeqs()).containsExactly(1L, 1L);
        assertThat(outbox.replaysOf(1L)).hasValue(0);
        assertThat(metrics.seqRegressions()).isZero();
    }

    /**
     * A store that cannot answer "was this row replayed?" cannot rule a replay out either, so the relay
     * stays silent rather than raise an incident it is unable to substantiate.
     */
    @Test
    void GIVEN_a_store_that_does_not_record_replays_WHEN_events_go_out_backwards_THEN_no_violation_is_reported() {
        insert(ORDER_1, 2);
        insert(ORDER_1, 1);

        drain(worker(new ReplayAgnosticStore(outbox)));

        assertThat(publishedSeqs()).containsExactly(2L, 1L);
        assertThat(metrics.seqRegressions()).isZero();
    }

    /**
     * The report has two channels and only one of them needs an adapter: the {@code ERROR} is always on
     * (a violated ordering precondition must not be invisible to an adopter who wired no metrics), while
     * the counter is guarded like every other meter. Delivery must be untouched either way.
     */
    @Test
    void GIVEN_no_metrics_adapter_is_wired_WHEN_events_go_out_backwards_THEN_delivery_still_completes() {
        insert(ORDER_1, 2);
        insert(ORDER_1, 1);
        RelayConfig cfg = RelayConfig.builder().bucketCount(BUCKETS).maxAttempts(3).build();

        drain(new RelayWorker(outbox, dispatcher, cfg, attempts -> Duration.ofSeconds(10),
                TandemMetrics.NOOP, clock, "worker-1", outbox::allBuckets));

        assertThat(publishedSeqs()).containsExactly(2L, 1L);
        assertThat(outbox.byStatus(OutboxStatus.DONE)).hasSize(2);
    }

    @Test
    void GIVEN_detection_is_turned_off_WHEN_events_go_out_backwards_THEN_nothing_is_reported_and_delivery_is_unaffected() {
        insert(ORDER_1, 2);
        insert(ORDER_1, 1);

        drain(worker(outbox, RelayConfig.builder()
                .bucketCount(BUCKETS).maxAttempts(3).seqRegressionDetection(false).build()));

        assertThat(publishedSeqs()).containsExactly(2L, 1L);
        assertThat(outbox.byStatus(OutboxStatus.DONE)).hasSize(2);
        assertThat(metrics.seqRegressions()).isZero();
    }

    // --- fixture ---

    private RelayWorker worker(OutboxStore store) {
        return worker(store, RelayConfig.builder().bucketCount(BUCKETS).maxAttempts(3).build());
    }

    private RelayWorker worker(OutboxStore store, RelayConfig cfg) {
        return new RelayWorker(store, dispatcher, cfg, attempts -> Duration.ofSeconds(10),
                metrics, clock, "worker-1", outbox::allBuckets);
    }

    private void insert(AggregateId aggregateId, long seq) {
        outbox.insert(OutboxMessage.builder()
                .aggregateId(aggregateId.value()).aggregateType("Order").seq(seq)
                .payload(("p-" + seq).getBytes()).build());
    }

    private List<Long> publishedSeqs() {
        return dispatcher.dispatched().stream().map(OutboxRecord::seq).toList();
    }

    private void drain(RelayWorker worker) {
        int guard = 0;
        while (true) {
            int claimed = worker.claimAndDispatch();
            worker.flushDone();
            worker.flushFailures();
            if (claimed == 0 && worker.inFlight() == 0) {
                return;
            }
            if (++guard > 1_000) {
                throw new IllegalStateException("relay did not quiesce");
            }
        }
    }

    /**
     * A real store that simply does not keep the replay count — the port's default, and what any
     * implementation predating the {@code replays} column looks like to the relay.
     */
    private record ReplayAgnosticStore(OutboxStore delegate) implements OutboxStore {

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
        public int cleanup(java.time.Instant doneBefore, int batchSize) {
            return delegate.cleanup(doneBefore, batchSize);
        }

        @Override
        public OptionalInt replaysOf(long id) {
            return OptionalInt.empty();
        }
    }
}
