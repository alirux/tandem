package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.exception.DuplicateSeqException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;

class JdbcOutboxRepositoryIT extends AbstractPostgresIT {

    private static final int BUCKETS = 256;

    private final JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, BUCKETS);

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

    private static String contentTypeOf(String aggregateId) {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT headers->>'content-type' FROM tandem_outbox WHERE aggregate_id = ?")) {
            ps.setString(1, aggregateId);
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
}
