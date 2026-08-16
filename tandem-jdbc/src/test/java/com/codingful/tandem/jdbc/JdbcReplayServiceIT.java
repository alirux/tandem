package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.ReplayResult;
import com.codingful.tandem.core.exception.TandemException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JdbcReplayServiceIT extends AbstractPostgresIT {

    private static final String LAST_FAILURE_REASON = "broker unreachable";

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
    void GIVEN_a_failed_row_WHEN_replayed_THEN_it_gets_a_fresh_attempt_budget_but_keeps_the_failure_reason() {
        insert("order-1", 1);
        execute("UPDATE tandem_outbox SET status = " + OutboxStatus.FAILED.code() + ", attempts = 5,"
                + " last_error = '" + LAST_FAILURE_REASON + "' WHERE id = 1");

        replay.replay(new ReplayCriteria(AggregateId.of("order-1"), null, null, null, Set.of(), false));

        assertThat(statusOf(1)).isEqualTo(OutboxStatus.PENDING.code());
        assertThat(attemptsOf(1)).isZero();
        assertThat(lastErrorOf(1)).isEqualTo(LAST_FAILURE_REASON);   // the operator's only "why did it fail?"
        assertThat(replaysOf(1)).isEqualTo(1);   // the attempt budget resets, the replay history does not
    }

    @Test
    void GIVEN_a_row_replayed_repeatedly_WHEN_inspected_THEN_every_replay_is_counted() {
        // "Replayed twice" is a different story from "replayed once" when reconstructing an incident,
        // so the row carries a count rather than a flag.
        insert("order-1", 1);
        setStatus(1, OutboxStatus.FAILED, 5);
        ReplayCriteria criteria = new ReplayCriteria(AggregateId.of("order-1"), null, null, null, Set.of(), false);

        replay.replay(criteria);
        setStatus(1, OutboxStatus.FAILED, 5);
        replay.replay(criteria);
        setStatus(1, OutboxStatus.FAILED, 5);
        replay.replay(criteria);

        assertThat(replaysOf(1)).isEqualTo(3);
    }

    @Test
    void GIVEN_a_replay_that_only_counts_matches_WHEN_it_completes_THEN_no_replay_is_recorded_on_the_row() {
        insert("order-1", 1);
        setStatus(1, OutboxStatus.FAILED, 5);

        ReplayResult result = replay.replay(new ReplayCriteria(
                AggregateId.of("order-1"), null, null, null, Set.of(), true));

        assertThat(result.matched()).isEqualTo(1);
        assertThat(replaysOf(1)).isZero();   // a dry run must leave the row exactly as it found it
    }

    @Test
    void GIVEN_a_row_that_was_never_replayed_WHEN_inspected_THEN_its_replay_count_is_zero() {
        insert("order-1", 1);

        assertThat(replaysOf(1)).isZero();
    }

    @Test
    void GIVEN_several_failed_rows_WHEN_replaying_a_single_id_THEN_only_that_row_is_reset() {
        // The Admin API's single-message replay narrows to one row with fromId == toId, so this is the
        // most-used shape of the range selector, not an exotic one.
        insert("order-1", 1);
        insert("order-1", 2);
        insert("order-1", 3);
        setStatus(1, OutboxStatus.FAILED, 5);
        setStatus(2, OutboxStatus.FAILED, 5);
        setStatus(3, OutboxStatus.FAILED, 5);

        ReplayResult result = replay.replay(new ReplayCriteria(null, null, 2L, 2L, Set.of(), false));

        assertThat(result.replayed()).isEqualTo(1);
        assertThat(statusOf(1)).isEqualTo(OutboxStatus.FAILED.code());
        assertThat(statusOf(2)).isEqualTo(OutboxStatus.PENDING.code());
        assertThat(statusOf(3)).isEqualTo(OutboxStatus.FAILED.code());
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
    void GIVEN_a_status_selector_with_no_replayable_status_WHEN_replayed_THEN_nothing_matches() {
        insert("order-1", 1);
        setStatus(1, OutboxStatus.FAILED, 3);

        // PENDING is not a replayable status - intersecting it with {DONE, FAILED} leaves nothing eligible.
        ReplayResult result = replay.replay(new ReplayCriteria(null, null, null, null, Set.of(OutboxStatus.PENDING), false));

        assertThat(result.matched()).isZero();
        assertThat(result.replayed()).isZero();
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

    @Test
    void GIVEN_the_database_is_unreachable_WHEN_replayed_THEN_the_failure_surfaces_as_a_tandem_exception() {
        // Callers (the Admin API) depend on the adapter never leaking a raw SQLException.
        JdbcReplayService unreachable = new JdbcReplayService(
                new SimpleDataSource("jdbc:postgresql://127.0.0.1:1/none", "none", "none"));

        assertThatThrownBy(() -> unreachable.replay(
                new ReplayCriteria(AggregateId.of("order-1"), null, null, null, Set.of(), false)))
                .isInstanceOf(TandemException.class)
                .hasCauseInstanceOf(SQLException.class);
    }

    private static int statusOf(long id) {
        return intColumn("SELECT status FROM tandem_outbox WHERE id = ?", id);
    }

    private static int attemptsOf(long id) {
        return intColumn("SELECT attempts FROM tandem_outbox WHERE id = ?", id);
    }

    private static int replaysOf(long id) {
        return intColumn("SELECT replays FROM tandem_outbox WHERE id = ?", id);
    }

    private static String lastErrorOf(long id) {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT last_error FROM tandem_outbox WHERE id = ?")) {
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
}
