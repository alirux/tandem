package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.exception.DuplicateSeqException;
import com.codingful.tandem.core.exception.OutboxInsertException;
import com.codingful.tandem.core.port.TracePropagator;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JdbcOutboxRepositoryIT extends AbstractPostgresIT {

    private static final int BUCKETS = 256;

    /** A real, non-NOOP propagator standing in for tandem-spring-producer/tandem-tracing-otel. */
    private static final TracePropagator STUB_PROPAGATOR = new TracePropagator() {
        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public Map<String, String> capture() {
            return Map.of(
                    TandemHeaders.TRACEPARENT, "00-stub-trace-01",
                    TandemHeaders.CORRELATION_ID, "captured-corr");
        }
    };

    private final JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, BUCKETS);
    private final JdbcOutboxRepository tracingRepository =
            new JdbcOutboxRepository(DATA_SOURCE, BUCKETS, STUB_PROPAGATOR);

    @Test
    void GIVEN_a_message_WHEN_inserted_THEN_the_row_holds_the_mapped_columns_and_the_java_computed_bucket() {
        String aggregateId = "order-1";
        repository.insert(OutboxMessage.builder()
                .aggregateId(aggregateId).aggregateType("Order").type("com.acme.order.placed").seq(7)
                .payload("{\"amount\":42}".getBytes(StandardCharsets.UTF_8))
                .contentType("application/json").header("correlation-id", "corr-1").build());

        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT aggregate_type, type, bucket, seq, status, attempts,"
                             + " payload = '{\"amount\":42}'::jsonb AS payload_match,"
                             + " headers->>'content-type' AS content_type,"
                             + " headers->>'correlation-id' AS correlation_id"
                             + " FROM tandem_outbox WHERE aggregate_id = ?")) {
            ps.setString(1, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("aggregate_type")).isEqualTo("Order");
                assertThat(rs.getString("type")).isEqualTo("com.acme.order.placed");
                assertThat(rs.getInt("bucket")).isEqualTo(BucketHash.bucketFor(aggregateId, BUCKETS));
                assertThat(rs.getLong("seq")).isEqualTo(7);
                assertThat(rs.getInt("status")).isZero();   // PENDING
                assertThat(rs.getInt("attempts")).isZero();
                assertThat(rs.getBoolean("payload_match")).isTrue();
                assertThat(rs.getString("content_type")).isEqualTo("application/json");
                assertThat(rs.getString("correlation_id")).isEqualTo("corr-1");
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void GIVEN_a_content_type_field_and_a_header_WHEN_inserted_THEN_the_typed_field_wins() {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-2").aggregateType("Order").seq(1).payload("{}".getBytes())
                .header(TandemHeaders.CONTENT_TYPE, "text/plain")   // should be overridden
                .contentType("application/json").build());

        assertThat(contentTypeOf("order-2")).isEqualTo("application/json");
    }

    @Test
    void GIVEN_an_existing_aggregate_id_and_seq_WHEN_inserted_again_THEN_the_unique_violation_surfaces_as_a_duplicate() {
        OutboxMessage message = OutboxMessage.builder()
                .aggregateId("order-3").aggregateType("Order").seq(1).payload("{}".getBytes()).build();
        repository.insert(message);

        assertThatThrownBy(() -> repository.insert(message)).isInstanceOf(DuplicateSeqException.class);
    }

    @Test
    void GIVEN_several_messages_WHEN_inserted_in_one_batch_THEN_all_rows_are_persisted() {
        repository.insertAll(List.of(
                OutboxMessage.builder().aggregateId("order-4").aggregateType("Order").seq(1).payload("{}".getBytes()).build(),
                OutboxMessage.builder().aggregateId("order-4").aggregateType("Order").seq(2).payload("{}".getBytes()).build(),
                OutboxMessage.builder().aggregateId("order-5").aggregateType("Order").seq(1).payload("{}".getBytes()).build()));

        assertThat(rowCount()).isEqualTo(3);
    }

    @Test
    void GIVEN_no_messages_WHEN_inserted_in_a_batch_THEN_it_returns_without_touching_the_database() {
        repository.insertAll(List.of());   // would open a connection/statement if it reached the database

        assertThat(rowCount()).isZero();
    }

    @Test
    void GIVEN_tracing_enabled_and_no_explicit_trace_headers_WHEN_inserted_THEN_the_captured_headers_are_stored() {
        tracingRepository.insert(OutboxMessage.builder()
                .aggregateId("order-trace-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build());

        assertThat(headerOf("order-trace-1", TandemHeaders.TRACEPARENT)).isEqualTo("00-stub-trace-01");
        assertThat(headerOf("order-trace-1", TandemHeaders.CORRELATION_ID)).isEqualTo("captured-corr");
    }

    @Test
    void GIVEN_tracing_enabled_and_an_explicit_correlation_id_header_WHEN_inserted_THEN_the_explicit_value_wins() {
        tracingRepository.insert(OutboxMessage.builder()
                .aggregateId("order-trace-2").aggregateType("Order").seq(1).payload("{}".getBytes())
                .header(TandemHeaders.CORRELATION_ID, "explicit-corr").build());

        assertThat(headerOf("order-trace-2", TandemHeaders.CORRELATION_ID)).isEqualTo("explicit-corr");
        assertThat(headerOf("order-trace-2", TandemHeaders.TRACEPARENT)).isEqualTo("00-stub-trace-01");
    }

    @Test
    void GIVEN_a_correlation_id_header_WHEN_inserted_THEN_it_is_also_stored_in_its_own_searchable_column() {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-corr-1").aggregateType("Order").seq(1).payload("{}".getBytes())
                .header(TandemHeaders.CORRELATION_ID, "corr-abc").build());

        // The column exists to be searched/indexed; the header stays the source of truth for Kafka.
        assertThat(correlationIdColumnOf("order-corr-1")).isEqualTo("corr-abc");
        assertThat(headerOf("order-corr-1", TandemHeaders.CORRELATION_ID)).isEqualTo("corr-abc");
    }

    @Test
    void GIVEN_tracing_enabled_and_no_explicit_correlation_id_WHEN_inserted_THEN_the_captured_one_reaches_the_column() {
        tracingRepository.insert(OutboxMessage.builder()
                .aggregateId("order-corr-2").aggregateType("Order").seq(1).payload("{}".getBytes()).build());

        assertThat(correlationIdColumnOf("order-corr-2")).isEqualTo("captured-corr");
    }

    @Test
    void GIVEN_no_correlation_id_at_all_WHEN_inserted_THEN_the_column_is_null() {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-corr-3").aggregateType("Order").seq(1).payload("{}".getBytes()).build());

        assertThat(correlationIdColumnOf("order-corr-3")).isNull();
    }

    @Test
    void GIVEN_an_oversized_externally_supplied_correlation_id_WHEN_inserted_THEN_the_column_is_truncated_and_the_insert_still_succeeds() {
        // The correlation id typically arrives from outside the application, so it is untrusted input:
        // an unbounded value must not fail the caller's business transaction on a column-width error.
        String oversized = "x".repeat(JdbcOutboxRepository.MAX_CORRELATION_ID_LENGTH + 50);
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-corr-4").aggregateType("Order").seq(1).payload("{}".getBytes())
                .header(TandemHeaders.CORRELATION_ID, oversized).build());

        assertThat(correlationIdColumnOf("order-corr-4"))
                .hasSize(JdbcOutboxRepository.MAX_CORRELATION_ID_LENGTH)
                .isEqualTo(oversized.substring(0, JdbcOutboxRepository.MAX_CORRELATION_ID_LENGTH));
        // The header keeps the full value — it is not indexed, and it is what reaches Kafka.
        assertThat(headerOf("order-corr-4", TandemHeaders.CORRELATION_ID)).isEqualTo(oversized);
    }

    private static String correlationIdColumnOf(String aggregateId) {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT correlation_id FROM tandem_outbox WHERE aggregate_id = ?")) {
            ps.setString(1, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String contentTypeOf(String aggregateId) {
        return headerOf(aggregateId, TandemHeaders.CONTENT_TYPE);
    }

    private static String headerOf(String aggregateId, String headerName) {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT headers->>? FROM tandem_outbox WHERE aggregate_id = ?")) {
            ps.setString(1, headerName);
            ps.setString(2, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static long rowCount() {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM tandem_outbox");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
    @Test
    void GIVEN_an_aggregate_with_no_version_WHEN_its_events_are_inserted_THEN_tandem_numbers_them_in_write_order() {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-managed-1").aggregateType("Order").managedSeq().payload("{}".getBytes()).build());
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-managed-1").aggregateType("Order").managedSeq().payload("{}".getBytes()).build());

        assertThat(seqsOf("order-managed-1")).hasSize(2).isSorted().doesNotHaveDuplicates();
    }

    @Test
    void GIVEN_a_batch_mixing_numbered_and_tandem_numbered_events_WHEN_inserted_THEN_the_rows_keep_the_collection_order() {
        repository.insertAll(List.of(
                OutboxMessage.builder().aggregateId("order-mixed-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build(),
                OutboxMessage.builder().aggregateId("order-mixed-2").aggregateType("Order").managedSeq().payload("{}".getBytes()).build(),
                OutboxMessage.builder().aggregateId("order-mixed-3").aggregateType("Order").seq(1).payload("{}".getBytes()).build(),
                OutboxMessage.builder().aggregateId("order-mixed-4").aggregateType("Order").managedSeq().payload("{}".getBytes()).build(),
                OutboxMessage.builder().aggregateId("order-mixed-5").aggregateType("Order").managedSeq().payload("{}".getBytes()).build()));

        assertThat(aggregateIdsInRowOrder())
                .containsExactly("order-mixed-1", "order-mixed-2", "order-mixed-3", "order-mixed-4", "order-mixed-5");
    }

    @Test
    void GIVEN_an_event_tandem_would_number_WHEN_the_insert_fails_THEN_the_failure_reports_no_number_rather_than_a_made_up_one() {
        OutboxMessage notJson = OutboxMessage.builder()
                .aggregateId("order-managed-2").aggregateType("Order").managedSeq()
                .payload("not json".getBytes()).build();

        assertThatThrownBy(() -> repository.insert(notJson))
                .isInstanceOf(OutboxInsertException.class)
                .hasMessageContaining("order-managed-2")
                .hasMessageContaining("managed");
    }

    @Test
    void GIVEN_a_batch_whose_last_event_repeats_a_number_already_used_WHEN_inserted_THEN_the_failure_names_that_event() {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-batch-fail").aggregateType("Order").seq(1).payload("{}".getBytes()).build());

        assertThatThrownBy(() -> repository.insertAll(List.of(
                OutboxMessage.builder().aggregateId("order-batch-ok").aggregateType("Order").managedSeq().payload("{}".getBytes()).build(),
                OutboxMessage.builder().aggregateId("order-batch-fail").aggregateType("Order").seq(1).payload("{}".getBytes()).build())))
                .isInstanceOf(DuplicateSeqException.class)
                .hasMessageContaining("order-batch-fail");
    }

    private static List<Long> seqsOf(String aggregateId) {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT seq FROM tandem_outbox WHERE aggregate_id = ? ORDER BY id")) {
            ps.setString(1, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                List<Long> seqs = new ArrayList<>();
                while (rs.next()) {
                    seqs.add(rs.getLong(1));
                }
                return seqs;
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<String> aggregateIdsInRowOrder() {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT aggregate_id FROM tandem_outbox ORDER BY id");
             ResultSet rs = ps.executeQuery()) {
            List<String> ids = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
            return ids;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
