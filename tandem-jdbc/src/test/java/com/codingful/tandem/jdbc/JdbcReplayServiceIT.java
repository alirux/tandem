package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.ReplayResult;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JdbcReplayServiceIT extends AbstractPostgresIT {

    private final JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, 256);
    private final JdbcReplayService replay = new JdbcReplayService(DATA_SOURCE);

    private void insert(String aggregateId, long seq) {
        repository.insert(OutboxMessage.builder()
                .aggregateId(aggregateId).aggregateType("Order").seq(seq).payload("{}".getBytes()).build());
    }

    private static void setStatus(long id, OutboxStatus status, int attempts) {
        execute("UPDATE tandem_outbox SET status = " + status.code() + ", attempts = " + attempts
                + " WHERE id = " + id);
    }

    @Test
    void GIVEN_done_and_failed_and_discarded_rows_WHEN_replayed_by_aggregate_THEN_only_the_replayable_ones_reset() {
        insert("order-1", 1);   // id 1 → DONE
        insert("order-1", 2);   // id 2 → FAILED
        insert("order-1", 3);   // id 3 → DISCARDED (never replayed)
        setStatus(1, OutboxStatus.DONE, 5);
        setStatus(2, OutboxStatus.FAILED, 5);
        setStatus(3, OutboxStatus.DISCARDED, 5);

        ReplayResult result = replay.replay(new ReplayCriteria(
                AggregateId.of("order-1"), null, null, null, Set.of(), false));

        assertThat(result.matched()).isEqualTo(2);
        assertThat(result.replayed()).isEqualTo(2);
        assertThat(statusOf(1)).isEqualTo(OutboxStatus.PENDING.code());
        assertThat(attemptsOf(1)).isZero();   // reset to a fresh attempt budget
        assertThat(statusOf(2)).isEqualTo(OutboxStatus.PENDING.code());
        assertThat(statusOf(3)).isEqualTo(OutboxStatus.DISCARDED.code());   // terminal, untouched
    }

    @Test
    void GIVEN_a_dry_run_WHEN_replayed_THEN_it_reports_the_matches_without_changing_any_row() {
        insert("order-1", 1);
        setStatus(1, OutboxStatus.FAILED, 3);

        ReplayResult result = replay.replay(new ReplayCriteria(
                AggregateId.of("order-1"), null, null, null, Set.of(), true));

        assertThat(result.matched()).isEqualTo(1);
        assertThat(result.replayed()).isZero();
        assertThat(result.dryRun()).isTrue();
        assertThat(statusOf(1)).isEqualTo(OutboxStatus.FAILED.code());   // unchanged
    }

    @Test
    void GIVEN_a_status_selector_WHEN_replaying_by_type_THEN_only_rows_in_that_status_reset() {
        insert("order-1", 1);   // id 1 → DONE (should stay)
        insert("order-2", 1);   // id 2 → FAILED (should reset)
        setStatus(1, OutboxStatus.DONE, 1);
        setStatus(2, OutboxStatus.FAILED, 1);

        ReplayResult result = replay.replay(new ReplayCriteria(
                null, "Order", null, null, Set.of(OutboxStatus.FAILED), false));

        assertThat(result.matched()).isEqualTo(1);
        assertThat(statusOf(1)).isEqualTo(OutboxStatus.DONE.code());
        assertThat(statusOf(2)).isEqualTo(OutboxStatus.PENDING.code());
    }

    private static int statusOf(long id) {
        return intColumn("SELECT status FROM tandem_outbox WHERE id = ?", id);
    }

    private static int attemptsOf(long id) {
        return intColumn("SELECT attempts FROM tandem_outbox WHERE id = ?", id);
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
}
