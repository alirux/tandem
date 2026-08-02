package com.codingful.tandem.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRowDetail;
import com.codingful.tandem.core.OutboxRowView;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour of {@link InMemoryOutbox}'s {@code OutboxQuery} implementation — the read side the Admin
 * API's use cases are unit-tested against (no mocks, AGENTS).
 */
class InMemoryOutboxQueryTest {

    private ControllableClock clock;
    private InMemoryOutbox outbox;

    @BeforeEach
    void setUp() {
        clock = ControllableClock.atEpochDay();
        outbox = new InMemoryOutbox(256, 3, clock);
    }

    private long insert(String aggregateId, String aggregateType, String type, long seq) {
        outbox.insert(OutboxMessage.builder()
                .aggregateId(aggregateId)
                .aggregateType(aggregateType)
                .type(type)
                .seq(seq)
                .payload(("payload-" + aggregateId + '-' + seq).getBytes())
                .header("correlation-id", "abc")
                .build());
        return outbox.byId(outbox.size()).id();   // ids are 1-based and sequential, per insert order
    }

    @Test
    void GIVEN_an_empty_outbox_WHEN_status_counts_read_THEN_every_status_is_zero() {
        Map<OutboxStatus, Long> counts = outbox.statusCounts();

        assertThat(counts).hasSize(OutboxStatus.values().length);
        assertThat(counts.values()).allMatch(count -> count == 0L);
    }

    @Test
    void GIVEN_rows_in_some_statuses_WHEN_status_counts_read_THEN_absent_statuses_are_zero_not_missing() {
        insert("order-1", "Order", null, 1);   // PENDING

        Map<OutboxStatus, Long> counts = outbox.statusCounts();

        assertThat(counts.get(OutboxStatus.PENDING)).isEqualTo(1L);
        assertThat(counts.get(OutboxStatus.DONE)).isZero();
        assertThat(counts.get(OutboxStatus.FAILED)).isZero();
        assertThat(counts.get(OutboxStatus.IN_FLIGHT)).isZero();
        assertThat(counts.get(OutboxStatus.DISCARDED)).isZero();
    }

    @Test
    void GIVEN_an_empty_outbox_WHEN_searched_THEN_an_empty_list_is_returned() {
        assertThat(outbox.search(OutboxSearchCriteria.builder().build())).isEmpty();
    }

    @Test
    void GIVEN_no_selector_WHEN_searched_THEN_all_rows_come_back_in_ascending_id_order() {
        insert("order-1", "Order", null, 1);
        insert("order-2", "Order", null, 1);
        insert("order-3", "Order", null, 1);

        List<OutboxRowView> rows = outbox.search(OutboxSearchCriteria.builder().build());

        assertThat(rows).extracting(OutboxRowView::aggregateId)
                .containsExactly(AggregateId.of("order-1"), AggregateId.of("order-2"), AggregateId.of("order-3"));
    }

    @Test
    void GIVEN_a_status_filter_WHEN_searched_THEN_only_matching_rows_are_returned() {
        insert("order-1", "Order", null, 1);
        long failedId = insert("order-2", "Order", null, 1);
        outbox.markFailed(failedId, "boom");

        List<OutboxRowView> rows =
                outbox.search(OutboxSearchCriteria.builder().status(OutboxStatus.FAILED).build());

        assertThat(rows).extracting(OutboxRowView::id).containsExactly(failedId);
    }

    @Test
    void GIVEN_an_aggregate_id_filter_WHEN_searched_THEN_only_that_aggregates_rows_are_returned() {
        insert("order-1", "Order", null, 1);
        insert("order-2", "Order", null, 1);

        List<OutboxRowView> rows = outbox.search(
                OutboxSearchCriteria.builder().aggregateId(AggregateId.of("order-1")).build());

        assertThat(rows).extracting(OutboxRowView::aggregateId).containsExactly(AggregateId.of("order-1"));
    }

    @Test
    void GIVEN_an_aggregate_type_filter_WHEN_searched_THEN_only_matching_rows_are_returned() {
        insert("order-1", "Order", null, 1);
        insert("cust-1", "Customer", null, 1);

        List<OutboxRowView> rows =
                outbox.search(OutboxSearchCriteria.builder().aggregateType("Customer").build());

        assertThat(rows).extracting(OutboxRowView::aggregateType).containsExactly("Customer");
    }

    @Test
    void GIVEN_a_type_filter_WHEN_searched_THEN_only_matching_rows_are_returned() {
        insert("order-1", "Order", "com.acme.order.placed", 1);
        insert("order-2", "Order", "com.acme.order.cancelled", 1);

        List<OutboxRowView> rows =
                outbox.search(OutboxSearchCriteria.builder().type("com.acme.order.cancelled").build());

        assertThat(rows).extracting(OutboxRowView::type).containsExactly("com.acme.order.cancelled");
    }

    @Test
    void GIVEN_a_created_time_range_WHEN_searched_THEN_only_rows_inside_the_range_are_returned() {
        clock.set(Instant.parse("2020-01-01T00:00:00Z"));
        insert("order-1", "Order", null, 1);   // outside the range below
        clock.set(Instant.parse("2026-06-01T00:00:00Z"));
        insert("order-2", "Order", null, 1);   // inside

        List<OutboxRowView> rows = outbox.search(OutboxSearchCriteria.builder()
                .createdFrom(Instant.parse("2026-01-01T00:00:00Z"))
                .createdTo(Instant.parse("2026-12-31T00:00:00Z"))
                .build());

        assertThat(rows).extracting(OutboxRowView::aggregateId).containsExactly(AggregateId.of("order-2"));
    }

    @Test
    void GIVEN_only_a_created_from_bound_WHEN_searched_THEN_rows_from_that_point_onward_are_returned() {
        clock.set(Instant.parse("2020-01-01T00:00:00Z"));
        insert("order-1", "Order", null, 1);   // before the bound
        clock.set(Instant.parse("2026-06-01T00:00:00Z"));
        insert("order-2", "Order", null, 1);   // after the bound, no upper bound set

        List<OutboxRowView> rows = outbox.search(OutboxSearchCriteria.builder()
                .createdFrom(Instant.parse("2026-01-01T00:00:00Z"))
                .build());

        assertThat(rows).extracting(OutboxRowView::aggregateId).containsExactly(AggregateId.of("order-2"));
    }

    @Test
    void GIVEN_combined_filters_WHEN_searched_THEN_only_rows_matching_all_of_them_are_returned() {
        insert("order-1", "Order", "com.acme.order.placed", 1);
        insert("order-2", "Order", "com.acme.order.cancelled", 1);
        insert("cust-1", "Customer", "com.acme.order.placed", 1);

        List<OutboxRowView> rows = outbox.search(OutboxSearchCriteria.builder()
                .aggregateType("Order")
                .type("com.acme.order.placed")
                .build());

        assertThat(rows).extracting(OutboxRowView::aggregateId).containsExactly(AggregateId.of("order-1"));
    }

    @Test
    void GIVEN_a_search_matching_nothing_WHEN_searched_THEN_an_empty_list_is_returned() {
        insert("order-1", "Order", null, 1);

        List<OutboxRowView> rows =
                outbox.search(OutboxSearchCriteria.builder().aggregateType("Nonexistent").build());

        assertThat(rows).isEmpty();
    }

    @Test
    void GIVEN_more_rows_than_the_limit_WHEN_searched_with_the_smallest_limit_THEN_exactly_one_row_is_returned() {
        insert("order-1", "Order", null, 1);
        insert("order-2", "Order", null, 1);
        insert("order-3", "Order", null, 1);

        assertThat(outbox.search(OutboxSearchCriteria.builder().limit(1).build())).hasSize(1);
    }

    @Test
    void GIVEN_a_cursor_WHEN_searched_THEN_only_rows_after_it_are_returned() {
        long first = insert("order-1", "Order", null, 1);
        long second = insert("order-2", "Order", null, 1);

        List<OutboxRowView> rows = outbox.search(OutboxSearchCriteria.builder().afterId(first).build());

        assertThat(rows).extracting(OutboxRowView::id).containsExactly(second);
    }

    @Test
    void GIVEN_a_row_inserted_between_two_pages_WHEN_the_second_page_is_fetched_THEN_the_new_row_does_not_shift_it() {
        long first = insert("order-1", "Order", null, 1);
        long second = insert("order-2", "Order", null, 1);

        // A row lands "between pages" — after the first page was read, before the second is fetched.
        insert("order-3", "Order", null, 1);

        List<OutboxRowView> secondPage =
                outbox.search(OutboxSearchCriteria.builder().afterId(first).limit(1).build());

        assertThat(secondPage).extracting(OutboxRowView::id).containsExactly(second);
    }

    @Test
    void GIVEN_an_existing_id_WHEN_found_by_id_THEN_the_full_detail_including_payload_and_headers_is_returned() {
        long id = insert("order-1", "Order", null, 1);

        Optional<OutboxRowDetail> detail = outbox.findById(id);

        assertThat(detail).isPresent();
        assertThat(detail.get().id()).isEqualTo(id);
        assertThat(new String(detail.get().payload())).isEqualTo("payload-order-1-1");
        assertThat(detail.get().headers()).containsEntry("correlation-id", "abc");
    }

    @Test
    void GIVEN_a_missing_id_WHEN_found_by_id_THEN_it_is_empty() {
        assertThat(outbox.findById(999_999L)).isEmpty();
    }
}
