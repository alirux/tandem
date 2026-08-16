package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.AggregateId;
import org.junit.jupiter.api.Test;

class SeqWatermarksTest {

    private static final AggregateId ORDER_1 = AggregateId.of("order-1");
    private static final AggregateId ORDER_2 = AggregateId.of("order-2");

    private final SeqWatermarks watermarks = new SeqWatermarks();

    @Test
    void GIVEN_an_aggregate_never_published_before_WHEN_its_first_event_goes_out_THEN_nothing_is_suspected() {
        assertThat(watermarks.record(ORDER_1, 7)).isEqualTo(SeqWatermarks.Verdict.ADVANCED);
    }

    @Test
    void GIVEN_events_published_in_order_WHEN_each_one_goes_out_THEN_nothing_is_suspected() {
        assertThat(watermarks.record(ORDER_1, 1)).isEqualTo(SeqWatermarks.Verdict.ADVANCED);
        assertThat(watermarks.record(ORDER_1, 2)).isEqualTo(SeqWatermarks.Verdict.ADVANCED);
        assertThat(watermarks.record(ORDER_1, 3)).isEqualTo(SeqWatermarks.Verdict.ADVANCED);
    }

    /** A gap is legitimate: not every domain event need pass through the outbox. */
    @Test
    void GIVEN_a_gap_in_the_sequence_WHEN_the_later_event_goes_out_THEN_nothing_is_suspected() {
        watermarks.record(ORDER_1, 1);

        assertThat(watermarks.record(ORDER_1, 50)).isEqualTo(SeqWatermarks.Verdict.ADVANCED);
    }

    /** At-least-once delivery: the same row re-published after a lease reclaim is not an ordering fault. */
    @Test
    void GIVEN_an_event_already_published_WHEN_the_very_same_one_goes_out_again_THEN_it_is_a_duplicate_not_a_regression() {
        watermarks.record(ORDER_1, 4);

        assertThat(watermarks.record(ORDER_1, 4)).isEqualTo(SeqWatermarks.Verdict.DUPLICATE);
    }

    @Test
    void GIVEN_an_event_already_published_WHEN_an_earlier_one_goes_out_after_it_THEN_it_is_suspected() {
        watermarks.record(ORDER_1, 2);

        assertThat(watermarks.record(ORDER_1, 1)).isEqualTo(SeqWatermarks.Verdict.REGRESSED);
    }

    @Test
    void GIVEN_two_aggregates_WHEN_one_goes_backwards_THEN_the_other_is_judged_on_its_own_history() {
        watermarks.record(ORDER_1, 9);
        watermarks.record(ORDER_2, 1);

        assertThat(watermarks.record(ORDER_2, 2)).isEqualTo(SeqWatermarks.Verdict.ADVANCED);
        assertThat(watermarks.record(ORDER_1, 8)).isEqualTo(SeqWatermarks.Verdict.REGRESSED);
    }

    /**
     * The trap HLD §8 names explicitly: had the suspected event lowered the watermark, everything after
     * it would read as an advance and the next genuine regression would go unseen.
     */
    @Test
    void GIVEN_an_event_went_backwards_WHEN_a_later_one_goes_backwards_too_THEN_it_is_still_suspected() {
        watermarks.record(ORDER_1, 10);
        watermarks.record(ORDER_1, 3);

        assertThat(watermarks.record(ORDER_1, 4)).isEqualTo(SeqWatermarks.Verdict.REGRESSED);
        assertThat(watermarks.record(ORDER_1, 11)).isEqualTo(SeqWatermarks.Verdict.ADVANCED);
    }

    @Test
    void GIVEN_more_aggregates_than_the_cap_WHEN_they_all_publish_THEN_the_tracked_set_stays_bounded() {
        SeqWatermarks bounded = new SeqWatermarks(3);

        for (int i = 0; i < 100; i++) {
            bounded.record(AggregateId.of("order-" + i), 1);
        }

        assertThat(bounded.size()).isEqualTo(3);
    }

    /**
     * Eviction degrades detection exactly as a relay restart does, and the design accepts that — pinned
     * so the trade-off is a decision on record rather than a surprise.
     */
    @Test
    void GIVEN_an_aggregate_evicted_by_newer_traffic_WHEN_it_goes_backwards_THEN_the_regression_is_missed() {
        SeqWatermarks bounded = new SeqWatermarks(2);
        bounded.record(ORDER_1, 10);
        bounded.record(AggregateId.of("other-1"), 1);
        bounded.record(AggregateId.of("other-2"), 1);

        assertThat(bounded.record(ORDER_1, 2)).isEqualTo(SeqWatermarks.Verdict.ADVANCED);
    }

    /** Recency is by publish, not by insertion: a busy aggregate must not be evicted by newcomers. */
    @Test
    void GIVEN_an_aggregate_kept_busy_WHEN_newer_aggregates_arrive_THEN_the_busy_one_survives_eviction() {
        SeqWatermarks bounded = new SeqWatermarks(2);
        bounded.record(ORDER_1, 10);
        bounded.record(AggregateId.of("other-1"), 1);
        bounded.record(ORDER_1, 11);
        bounded.record(AggregateId.of("other-2"), 1);

        assertThat(bounded.record(ORDER_1, 5)).isEqualTo(SeqWatermarks.Verdict.REGRESSED);
    }
}
