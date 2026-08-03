package com.codingful.tandem.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.ReplayResult;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour of {@link InMemoryOutbox}'s {@code ReplayService}/{@code DiscardService} implementations —
 * the Admin API's mutating actions (slice 2), unit-tested against a real collaborator (no mocks, AGENTS).
 */
class InMemoryOutboxReplayAndDiscardTest {

    private InMemoryOutbox outbox;

    @BeforeEach
    void setUp() {
        outbox = new InMemoryOutbox(256, 3, ControllableClock.atEpochDay());
    }

    private long insert(String aggregateId) {
        outbox.insert(OutboxMessage.builder()
                .aggregateId(aggregateId).aggregateType("Order").seq(1).payload("{}".getBytes()).build());
        return outbox.byId(outbox.size()).id();
    }

    // --- replay ---

    @Test
    void GIVEN_done_and_failed_and_discarded_rows_WHEN_replayed_by_aggregate_THEN_only_the_replayable_ones_reset() {
        long done = insert("order-1");
        outbox.markDone(done);
        long failed = insert("order-2");
        outbox.markFailed(failed, "boom");
        long discarded = insert("order-3");
        outbox.markFailed(discarded, "boom");
        outbox.discard(discarded, "no longer needed");

        ReplayResult result = outbox.replay(new ReplayCriteria(null, "Order", null, null, Set.of(), false));

        assertThat(result.matched()).isEqualTo(2);
        assertThat(result.replayed()).isEqualTo(2);
        assertThat(outbox.byId(done).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outbox.byId(failed).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outbox.byId(discarded).status()).isEqualTo(OutboxStatus.DISCARDED);   // terminal, untouched
    }

    @Test
    void GIVEN_a_replayed_row_WHEN_inspected_THEN_attempts_and_last_error_are_reset() {
        long id = insert("order-1");
        outbox.markFailed(id, "boom");
        outbox.markFailed(id, "boom again");

        outbox.replay(new ReplayCriteria(AggregateId.of("order-1"), null, null, null, Set.of(), false));

        OutboxRecord replayed = outbox.byId(id);
        assertThat(replayed.status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(replayed.attempts()).isZero();
        assertThat(replayed.lastError()).isNull();
    }

    @Test
    void GIVEN_a_dry_run_WHEN_replayed_THEN_it_reports_the_matches_without_changing_any_row() {
        long id = insert("order-1");
        outbox.markFailed(id, "boom");

        ReplayResult result = outbox.replay(new ReplayCriteria(AggregateId.of("order-1"), null, null, null, Set.of(), true));

        assertThat(result.matched()).isEqualTo(1);
        assertThat(result.replayed()).isZero();
        assertThat(result.dryRun()).isTrue();
        assertThat(outbox.byId(id).status()).isEqualTo(OutboxStatus.FAILED);   // unchanged
    }

    @Test
    void GIVEN_a_status_selector_WHEN_replaying_by_type_THEN_only_rows_in_that_status_reset() {
        long stayDone = insert("order-1");
        outbox.markDone(stayDone);
        long resetFailed = insert("order-2");
        outbox.markFailed(resetFailed, "boom");

        ReplayResult result = outbox.replay(
                new ReplayCriteria(null, "Order", null, null, Set.of(OutboxStatus.FAILED), false));

        assertThat(result.matched()).isEqualTo(1);
        assertThat(outbox.byId(stayDone).status()).isEqualTo(OutboxStatus.DONE);
        assertThat(outbox.byId(resetFailed).status()).isEqualTo(OutboxStatus.PENDING);
    }

    @Test
    void GIVEN_an_id_range_WHEN_replayed_THEN_only_rows_inside_the_range_are_replayed() {
        long first = insert("order-1");
        outbox.markFailed(first, "boom");
        long second = insert("order-2");
        outbox.markFailed(second, "boom");

        ReplayResult result = outbox.replay(new ReplayCriteria(null, null, first, first, Set.of(), false));

        assertThat(result.matched()).isEqualTo(1);
        assertThat(outbox.byId(first).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outbox.byId(second).status()).isEqualTo(OutboxStatus.FAILED);
    }

    @Test
    void GIVEN_no_replayable_rows_match_WHEN_replayed_THEN_nothing_is_matched_or_replayed() {
        insert("order-1");   // stays PENDING, not replayable

        ReplayResult result = outbox.replay(new ReplayCriteria(AggregateId.of("order-1"), null, null, null, Set.of(), false));

        assertThat(result.matched()).isZero();
        assertThat(result.replayed()).isZero();
    }

    @Test
    void GIVEN_a_status_selector_with_no_replayable_status_WHEN_replayed_THEN_nothing_matches() {
        long id = insert("order-1");
        outbox.markFailed(id, "boom");

        // PENDING is not a replayable status - intersecting it with {DONE, FAILED} leaves nothing eligible.
        ReplayResult result = outbox.replay(new ReplayCriteria(null, null, null, null, Set.of(OutboxStatus.PENDING), false));

        assertThat(result.matched()).isZero();
        assertThat(result.replayed()).isZero();
        assertThat(outbox.byId(id).status()).isEqualTo(OutboxStatus.FAILED);   // unchanged
    }

    @Test
    void GIVEN_a_row_from_a_different_aggregate_id_WHEN_replayed_by_aggregate_id_THEN_it_is_excluded() {
        long matching = insert("order-1");
        outbox.markFailed(matching, "boom");
        long other = insert("order-2");
        outbox.markFailed(other, "boom");

        ReplayResult result = outbox.replay(new ReplayCriteria(AggregateId.of("order-1"), null, null, null, Set.of(), false));

        assertThat(result.matched()).isEqualTo(1);
        assertThat(outbox.byId(matching).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outbox.byId(other).status()).isEqualTo(OutboxStatus.FAILED);   // different aggregate, excluded
    }

    @Test
    void GIVEN_a_row_of_a_different_aggregate_type_WHEN_replayed_by_aggregate_type_THEN_it_is_excluded() {
        long order = insert("order-1");
        outbox.markFailed(order, "boom");
        outbox.insert(OutboxMessage.builder()
                .aggregateId("cust-1").aggregateType("Customer").seq(1).payload("{}".getBytes()).build());
        long customer = outbox.byId(outbox.size()).id();
        outbox.markFailed(customer, "boom");

        ReplayResult result = outbox.replay(new ReplayCriteria(null, "Order", null, null, Set.of(), false));

        assertThat(result.matched()).isEqualTo(1);
        assertThat(outbox.byId(order).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outbox.byId(customer).status()).isEqualTo(OutboxStatus.FAILED);   // different type, excluded
    }

    @Test
    void GIVEN_a_row_below_the_from_id_WHEN_replayed_by_id_range_THEN_it_is_excluded() {
        long below = insert("order-1");
        outbox.markFailed(below, "boom");
        long inRange = insert("order-2");
        outbox.markFailed(inRange, "boom");

        ReplayResult result = outbox.replay(new ReplayCriteria(null, null, inRange, inRange, Set.of(), false));

        assertThat(result.matched()).isEqualTo(1);
        assertThat(outbox.byId(inRange).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outbox.byId(below).status()).isEqualTo(OutboxStatus.FAILED);   // below fromId, excluded
    }

    // --- discard ---

    @Test
    void GIVEN_a_failed_row_WHEN_discarded_THEN_it_becomes_discarded_with_the_reason_recorded() {
        long id = insert("order-1");
        outbox.markFailed(id, "boom");

        boolean discarded = outbox.discard(id, "duplicate event, safe to skip");

        assertThat(discarded).isTrue();
        OutboxRecord record = outbox.byId(id);
        assertThat(record.status()).isEqualTo(OutboxStatus.DISCARDED);
        assertThat(record.discardReason()).isEqualTo("duplicate event, safe to skip");
        assertThat(record.lastError()).isEqualTo("boom");   // original failure reason untouched
    }

    @Test
    void GIVEN_a_pending_row_WHEN_discarded_THEN_it_is_refused_and_left_unchanged() {
        long id = insert("order-1");   // still PENDING

        boolean discarded = outbox.discard(id, "irrelevant");

        assertThat(discarded).isFalse();
        assertThat(outbox.byId(id).status()).isEqualTo(OutboxStatus.PENDING);
    }

    @Test
    void GIVEN_a_missing_id_WHEN_discarded_THEN_it_is_refused() {
        assertThat(outbox.discard(999_999L, "irrelevant")).isFalse();
    }
}
