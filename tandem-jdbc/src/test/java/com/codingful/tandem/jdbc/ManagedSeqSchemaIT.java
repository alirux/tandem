package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

/**
 * Pins the property the shipped schema has to hold for a Tandem-assigned {@code seq}
 * (HLD-managed-seq §4.1): the numbers it hands out never go backwards, whichever session asks for
 * them. A cached sequence would break that silently — each session pre-allocates its own range, so
 * the session that started first keeps handing out lower numbers after the other has already used
 * higher ones, and the relay reports the gap as a write-side ordering violation that never happened.
 */
class ManagedSeqSchemaIT extends AbstractPostgresIT {

    @Test
    void GIVEN_two_sessions_writing_at_the_same_time_WHEN_tandem_assigns_the_sequence_number_THEN_a_later_write_never_gets_a_lower_one()
            throws SQLException {
        try (Connection first = DATA_SOURCE.getConnection();
             Connection second = DATA_SOURCE.getConnection()) {
            long firstSessionEarly = insertLettingTandemAssignSeq(first, "order-A");
            long secondSession = insertLettingTandemAssignSeq(second, "order-B");
            long firstSessionLate = insertLettingTandemAssignSeq(first, "order-A");

            assertThat(secondSession).isGreaterThan(firstSessionEarly);
            assertThat(firstSessionLate).isGreaterThan(secondSession);
        }
    }

    /** Omits the {@code seq} column entirely — the column default is the whole mechanism. */
    private static long insertLettingTandemAssignSeq(Connection conn, String aggregateId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, payload)"
                        + " VALUES (?, 'Order', 0, '{}'::jsonb) RETURNING seq")) {
            ps.setString(1, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                return rs.getLong("seq");
            }
        }
    }
}
