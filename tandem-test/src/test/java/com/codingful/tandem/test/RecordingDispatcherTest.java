package com.codingful.tandem.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.exception.OutboxDispatchException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class RecordingDispatcherTest {

    private static OutboxRecord record(long id) {
        OutboxMessage message = OutboxMessage.builder()
                .aggregateId("order-" + id)
                .aggregateType("Order")
                .seq(1)
                .payload(new byte[] {(byte) id})
                .build();
        return OutboxRecord.builder().id(id).message(message).createdAt(Instant.EPOCH).build();
    }

    @Test
    void GIVEN_records_dispatched_WHEN_no_failures_configured_THEN_they_are_recorded_in_order_and_succeed() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();

        CompletableFuture<Void> f1 = dispatcher.dispatch(record(1));
        CompletableFuture<Void> f2 = dispatcher.dispatch(record(2));

        assertThat(f1).isCompleted();
        assertThat(f2).isCompleted();
        assertThat(dispatcher.dispatchedIds()).containsExactly(1L, 2L);
    }

    @Test
    void GIVEN_a_record_forced_to_fail_retriably_WHEN_dispatched_THEN_the_future_carries_a_retriable_verdict() {
        RecordingDispatcher dispatcher = new RecordingDispatcher().failRecord(1, true);

        CompletableFuture<Void> future = dispatcher.dispatch(record(1));

        assertThat(future).isCompletedExceptionally();
        assertThat(catchDispatchException(future).isRetriable()).isTrue();
    }

    @Test
    void GIVEN_a_record_forced_to_fail_permanently_WHEN_dispatched_THEN_the_future_carries_a_permanent_verdict() {
        RecordingDispatcher dispatcher = new RecordingDispatcher().failRecord(1, false);

        assertThat(catchDispatchException(dispatcher.dispatch(record(1))).isRetriable()).isFalse();
    }

    @Test
    void GIVEN_a_record_set_to_fail_a_fixed_number_of_times_WHEN_dispatched_repeatedly_THEN_it_recovers() {
        RecordingDispatcher dispatcher = new RecordingDispatcher().failRecord(1, true, 2);

        assertThat(dispatcher.dispatch(record(1))).isCompletedExceptionally();
        assertThat(dispatcher.dispatch(record(1))).isCompletedExceptionally();
        assertThat(dispatcher.dispatch(record(1))).isCompleted();   // third attempt succeeds
    }

    @Test
    void GIVEN_fail_all_configured_WHEN_any_record_dispatched_THEN_it_fails() {
        RecordingDispatcher dispatcher = new RecordingDispatcher().failAll(true);

        assertThat(dispatcher.dispatch(record(1))).isCompletedExceptionally();
        assertThat(dispatcher.dispatch(record(2))).isCompletedExceptionally();

        dispatcher.succeedAll();
        assertThat(dispatcher.dispatch(record(3))).isCompleted();
    }

    @Test
    void GIVEN_manual_mode_WHEN_records_dispatched_THEN_futures_stay_in_flight_until_completed_fifo() {
        RecordingDispatcher dispatcher = new RecordingDispatcher().manualCompletion();

        CompletableFuture<Void> f1 = dispatcher.dispatch(record(1));
        CompletableFuture<Void> f2 = dispatcher.dispatch(record(2));

        // Both overlap in flight (the relay's per-shard concurrency window).
        assertThat(f1).isNotDone();
        assertThat(f2).isNotDone();
        assertThat(dispatcher.pendingCount()).isEqualTo(2);

        assertThat(dispatcher.completeNext().id()).isEqualTo(1L);   // FIFO
        assertThat(f1).isCompleted();
        assertThat(f2).isNotDone();

        dispatcher.completeAll();
        assertThat(f2).isCompleted();
        assertThat(dispatcher.pendingCount()).isZero();
    }

    @Test
    void GIVEN_manual_mode_with_a_forced_failure_WHEN_completed_THEN_the_outcome_honours_the_failure_config() {
        RecordingDispatcher dispatcher = new RecordingDispatcher().manualCompletion().failRecord(1, false);

        CompletableFuture<Void> future = dispatcher.dispatch(record(1));
        assertThat(future).isNotDone();

        dispatcher.completeNext();
        assertThat(catchDispatchException(future).isRetriable()).isFalse();
    }

    private static OutboxDispatchException catchDispatchException(CompletableFuture<Void> future) {
        try {
            future.join();
            throw new AssertionError("expected the future to complete exceptionally");
        } catch (CompletionException e) {
            assertThat(e.getCause()).isInstanceOf(OutboxDispatchException.class);
            return (OutboxDispatchException) e.getCause();
        }
    }
}
