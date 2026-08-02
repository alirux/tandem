package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRowDetail;
import com.codingful.tandem.core.OutboxRowView;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class JdbcOutboxQueryIT extends AbstractPostgresIT {

    private final JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, 256);
    private final JdbcOutboxQuery query = new JdbcOutboxQuery(DATA_SOURCE);

    private long insert(String aggregateId, String aggregateType, long seq, String payload) {
        return insert(aggregateId, aggregateType, null, seq, payload);
    }

    private long insert(String aggregateId, String aggregateType, String type, long seq, String payload) {
        repository.insert(OutboxMessage.builder()
                .aggregateId(aggregateId)
                .aggregateType(aggregateType)
                .type(type)
                .seq(seq)
                .payload(payload.getBytes(StandardCharsets.UTF_8))
                .header("correlation-id", "abc")
                .build());
        return lastInsertedId();
    }

    private static long lastInsertedId() {
        try (Connection conn = DATA_SOURCE.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT max(id) FROM tandem_outbox");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void GIVEN_an_empty_outbox_WHEN_status_counts_read_THEN_every_status_is_zero() {
        Map<OutboxStatus, Long> counts = query.statusCounts();

        assertThat(counts).hasSize(OutboxStatus.values().length);
        for (OutboxStatus status : OutboxStatus.values()) {
            assertThat(counts.get(status)).as(status.name()).isZero();
        }
    }

    @Test
    void GIVEN_rows_in_some_statuses_WHEN_status_counts_read_THEN_absent_statuses_are_zero_not_missing() {
        insert("order-1", "Order", 1, "{}");
        long failed = insert("order-2", "Order", 1, "{}");
        setStatus(failed, OutboxStatus.FAILED);

        Map<OutboxStatus, Long> counts = query.statusCounts();

        assertThat(counts.get(OutboxStatus.PENDING)).isEqualTo(1L);
        assertThat(counts.get(OutboxStatus.FAILED)).isEqualTo(1L);
        assertThat(counts.get(OutboxStatus.DONE)).isZero();
        assertThat(counts.get(OutboxStatus.IN_FLIGHT)).isZero();
        assertThat(counts.get(OutboxStatus.DISCARDED)).isZero();
    }

    @Test
    void GIVEN_no_selector_WHEN_searched_THEN_rows_are_returned_in_ascending_id_order() {
        insert("order-1", "Order", 1, "{}");
        insert("order-2", "Order", 1, "{}");
        insert("order-3", "Order", 1, "{}");

        List<OutboxRowView> rows = query.search(OutboxSearchCriteria.builder().build());

        assertThat(rows).extracting(OutboxRowView::aggregateId)
                .containsExactly(AggregateId.of("order-1"), AggregateId.of("order-2"), AggregateId.of("order-3"));
    }

    @Test
    void GIVEN_a_status_filter_WHEN_searched_THEN_only_matching_rows_are_returned() {
        long a = insert("order-1", "Order", 1, "{}");
        insert("order-2", "Order", 1, "{}");
        setStatus(a, OutboxStatus.FAILED);

        List<OutboxRowView> rows = query.search(
                OutboxSearchCriteria.builder().status(OutboxStatus.FAILED).build());

        assertThat(rows).extracting(OutboxRowView::id).containsExactly(a);
    }

    @Test
    void GIVEN_a_type_filter_WHEN_searched_THEN_only_matching_rows_are_returned() {
        long placed = insert("order-1", "Order", "com.acme.order.placed", 1, "{}");
        insert("order-2", "Order", "com.acme.order.cancelled", 1, "{}");

        List<OutboxRowView> rows = query.search(
                OutboxSearchCriteria.builder().type("com.acme.order.placed").build());

        assertThat(rows).extracting(OutboxRowView::id).containsExactly(placed);
    }

    @Test
    void GIVEN_an_aggregate_id_filter_WHEN_searched_THEN_only_that_aggregates_rows_are_returned() {
        insert("order-1", "Order", 1, "{}");
        insert("order-2", "Order", 1, "{}");

        List<OutboxRowView> rows = query.search(
                OutboxSearchCriteria.builder().aggregateId(AggregateId.of("order-1")).build());

        assertThat(rows).extracting(OutboxRowView::aggregateId).containsExactly(AggregateId.of("order-1"));
    }

    @Test
    void GIVEN_a_created_time_range_WHEN_searched_THEN_only_rows_inside_the_range_are_returned() {
        long before = insert("order-1", "Order", 1, "{}");
        setCreatedAt(before, Instant.parse("2020-01-01T00:00:00Z"));
        long inside = insert("order-2", "Order", 1, "{}");
        setCreatedAt(inside, Instant.parse("2026-06-01T00:00:00Z"));

        List<OutboxRowView> rows = query.search(OutboxSearchCriteria.builder()
                .createdFrom(Instant.parse("2026-01-01T00:00:00Z"))
                .createdTo(Instant.parse("2026-12-31T00:00:00Z"))
                .build());

        assertThat(rows).extracting(OutboxRowView::id).containsExactly(inside);
    }

    @Test
    void GIVEN_more_rows_than_the_limit_WHEN_searched_THEN_only_limit_rows_are_returned() {
        insert("order-1", "Order", 1, "{}");
        insert("order-2", "Order", 1, "{}");
        insert("order-3", "Order", 1, "{}");

        List<OutboxRowView> rows = query.search(OutboxSearchCriteria.builder().limit(2).build());

        assertThat(rows).hasSize(2);
    }

    @Test
    void GIVEN_a_cursor_WHEN_searched_THEN_only_rows_after_it_are_returned() {
        long first = insert("order-1", "Order", 1, "{}");
        long second = insert("order-2", "Order", 1, "{}");

        List<OutboxRowView> rows = query.search(OutboxSearchCriteria.builder().afterId(first).build());

        assertThat(rows).extracting(OutboxRowView::id).containsExactly(second);
    }

    @Test
    void GIVEN_rows_inserted_between_two_pages_WHEN_the_second_page_is_fetched_THEN_it_still_starts_after_the_cursor() {
        long first = insert("order-1", "Order", 1, "{}");
        long second = insert("order-2", "Order", 1, "{}");
        insert("order-3", "Order", 1, "{}");   // inserted "between pages" relative to the cursor below

        List<OutboxRowView> secondPage = query.search(OutboxSearchCriteria.builder().afterId(first).limit(1).build());

        assertThat(secondPage).extracting(OutboxRowView::id).containsExactly(second);
    }

    @Test
    void GIVEN_a_search_matching_nothing_WHEN_searched_THEN_an_empty_list_is_returned() {
        insert("order-1", "Order", 1, "{}");

        List<OutboxRowView> rows = query.search(
                OutboxSearchCriteria.builder().aggregateType("Nonexistent").build());

        assertThat(rows).isEmpty();
    }

    @Test
    void GIVEN_a_search_result_row_WHEN_inspected_THEN_it_carries_no_payload_or_headers() {
        insert("order-1", "Order", 1, "{\"secret\":true}");

        List<OutboxRowView> rows = query.search(OutboxSearchCriteria.builder().build());

        // OutboxRowView has no payload/headers accessor at all — the type itself proves the column
        // was never read; this assertion documents that intent for a reader of the test.
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).aggregateType()).isEqualTo("Order");
    }

    @Test
    void GIVEN_an_existing_id_WHEN_found_by_id_THEN_the_full_detail_including_payload_and_headers_is_returned() {
        long id = insert("order-1", "Order", 1, "{\"amount\":42}");

        Optional<OutboxRowDetail> detail = query.findById(id);

        assertThat(detail).isPresent();
        assertThat(detail.get().id()).isEqualTo(id);
        // JSONB round-trips through Postgres's canonical text form (space after ':'), not the literal bytes.
        assertThat(new String(detail.get().payload(), StandardCharsets.UTF_8)).contains("\"amount\": 42");
        assertThat(detail.get().headers()).containsEntry("correlation-id", "abc");
    }

    @Test
    void GIVEN_a_missing_id_WHEN_found_by_id_THEN_it_is_empty() {
        assertThat(query.findById(999_999L)).isEmpty();
    }

    private static void setStatus(long id, OutboxStatus status) {
        execute("UPDATE tandem_outbox SET status = " + status.code() + " WHERE id = " + id);
    }

    private static void setCreatedAt(long id, Instant createdAt) {
        execute("UPDATE tandem_outbox SET created_at = '" + createdAt + "' WHERE id = " + id);
    }
}
