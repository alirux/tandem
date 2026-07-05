package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.ReplayResult;
import com.codingful.tandem.core.exception.TandemException;
import com.codingful.tandem.core.port.ReplayService;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.sql.DataSource;

/**
 * Replays matching rows back to {@code PENDING} so the relay re-publishes them (LLD-core §2.6, HLD §8).
 * Only {@code DONE} and {@code FAILED} rows are replayable — the only backward transitions;
 * {@code DISCARDED} is terminal and never replayed (HLD §6). Reset mirrors HLD §8:
 * {@code status=PENDING, attempts=0, last_error=NULL, next_attempt_at=NULL} (and clears any stale lease).
 * Honours {@code dryRun}.
 */
public final class JdbcReplayService implements ReplayService {

    private static final Set<OutboxStatus> REPLAYABLE = Set.of(OutboxStatus.DONE, OutboxStatus.FAILED);

    private final DataSource dataSource;

    /** @param dataSource used for both the dry-run count query and the actual reset UPDATE */
    public JdbcReplayService(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public ReplayResult replay(ReplayCriteria criteria) {
        Objects.requireNonNull(criteria, "criteria");
        Set<OutboxStatus> effectiveStatuses = effectiveStatuses(criteria);
        if (effectiveStatuses.isEmpty()) {
            return new ReplayResult(0, 0, criteria.dryRun());   // nothing replayable was requested
        }

        List<Object> params = new ArrayList<>();
        String where = buildWhere(criteria, effectiveStatuses, params);

        try (Connection conn = dataSource.getConnection()) {
            if (criteria.dryRun()) {
                long matched = count(conn, where, params);
                return new ReplayResult(matched, 0, true);
            }
            int replayed = resetToPending(conn, where, params);
            return new ReplayResult(replayed, replayed, false);
        } catch (SQLException e) {
            throw new TandemException("replay failed", e);
        }
    }

    /** Requested statuses intersected with the replayable set; defaults to all replayable when unset. */
    private static Set<OutboxStatus> effectiveStatuses(ReplayCriteria criteria) {
        if (criteria.statuses().isEmpty()) {
            return REPLAYABLE;
        }
        Set<OutboxStatus> effective = new LinkedHashSet<>(criteria.statuses());
        effective.retainAll(REPLAYABLE);
        return effective;
    }

    private static String buildWhere(ReplayCriteria criteria, Set<OutboxStatus> statuses, List<Object> params) {
        StringBuilder where = new StringBuilder();
        StringBuilder statusIn = new StringBuilder();
        for (OutboxStatus status : statuses) {
            statusIn.append(statusIn.isEmpty() ? "" : ", ").append('?');
            params.add(status.code());
        }
        where.append("status IN (").append(statusIn).append(')');
        if (criteria.aggregateId() != null) {
            where.append(" AND aggregate_id = ?");
            params.add(criteria.aggregateId().value());
        }
        if (criteria.aggregateType() != null) {
            where.append(" AND aggregate_type = ?");
            params.add(criteria.aggregateType());
        }
        if (criteria.fromId() != null) {
            where.append(" AND id >= ?");
            params.add(criteria.fromId());
        }
        if (criteria.toId() != null) {
            where.append(" AND id <= ?");
            params.add(criteria.toId());
        }
        return where.toString();
    }

    private static long count(Connection conn, String where, List<Object> params) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM tandem_outbox WHERE " + where)) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static int resetToPending(Connection conn, String where, List<Object> params) throws SQLException {
        String sql = "UPDATE tandem_outbox"
                + "   SET status = 0, attempts = 0, last_error = NULL, next_attempt_at = NULL,"
                + "       locked_by = NULL, locked_until = NULL"
                + " WHERE " + where;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bind(ps, params);
            return ps.executeUpdate();
        }
    }

    private static void bind(PreparedStatement ps, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
        }
    }
}
