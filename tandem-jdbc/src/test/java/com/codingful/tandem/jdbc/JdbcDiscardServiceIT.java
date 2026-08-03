package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxStatus;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class JdbcDiscardServiceIT extends AbstractPostgresIT {

    private final JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, 256);
    private final JdbcDiscardService discardService = new JdbcDiscardService(DATA_SOURCE);

    private long insert(String aggregateId) {
        repository.insert(OutboxMessage.builder()
                .aggregateId(aggregateId).aggregateType("Order").seq(1).payload("{}".getBytes()).build());
        return maxId();
    }

    @Test
    void GIVEN_a_failed_row_WHEN_discarded_THEN_it_becomes_discarded_with_the_reason_recorded() {
        long id = insert("order-1");
        setStatus(id, OutboxStatus.FAILED);

        boolean discarded = discardService.discard(id, "duplicate event, safe to skip");

        assertThat(discarded).isTrue();
        assertThat(statusOf(id)).isEqualTo(OutboxStatus.DISCARDED.code());
        assertThat(discardReasonOf(id)).isEqualTo("duplicate event, safe to skip");
    }

    @Test
    void GIVEN_a_pending_row_WHEN_discarded_THEN_it_is_refused_and_left_unchanged() {
        long id = insert("order-1");   // still PENDING

        boolean discarded = discardService.discard(id, "irrelevant");

        assertThat(discarded).isFalse();
        assertThat(statusOf(id)).isEqualTo(OutboxStatus.PENDING.code());
    }

    @Test
    void GIVEN_a_missing_id_WHEN_discarded_THEN_it_is_refused() {
        assertThat(discardService.discard(999_999L, "irrelevant")).isFalse();
    }

    private static void setStatus(long id, OutboxStatus status) {
        execute("UPDATE tandem_outbox SET status = " + status.code() + " WHERE id = " + id);
    }

    private static int statusOf(long id) {
        return intColumn("SELECT status FROM tandem_outbox WHERE id = ?", id);
    }

    private static String discardReasonOf(long id) {
        try (Connection conn = DATA_SOURCE.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT discard_reason FROM tandem_outbox WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static int intColumn(String sql, long id) {
        try (Connection conn = DATA_SOURCE.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static long maxId() {
        try (Connection conn = DATA_SOURCE.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT max(id) FROM tandem_outbox");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
