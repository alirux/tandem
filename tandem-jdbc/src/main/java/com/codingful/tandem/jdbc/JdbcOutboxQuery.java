package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxRowDetail;
import com.codingful.tandem.core.OutboxRowView;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.port.OutboxQuery;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * Admin-API read side (HLD-admin-api §4, {@link OutboxQuery}): counts, search, and single-row lookup
 * over {@code tandem_outbox}. Named columns only, never {@code SELECT *} (AGENTS, HLD §1.4). Maps to
 * its own read types — {@link OutboxRowMapper} stays untouched, mapping to {@code OutboxRecord} for
 * the write/relay path.
 *
 * <p>The list query never selects {@code payload}/{@code headers}: the point of the separate
 * {@link OutboxRowView} type is that those JSONB columns are never read for a list page, not merely
 * dropped afterwards.
 */
public final class JdbcOutboxQuery implements OutboxQuery {

    private static final String VIEW_COLUMNS = "id, aggregate_id, aggregate_type, type, seq, status, "
            + "attempts, replays, last_error, discard_reason, next_attempt_at, locked_by, locked_until, "
            + "created_at, correlation_id";

    private final DataSource dataSource;

    /** @param dataSource used for every query; each call opens and closes its own connection */
    public JdbcOutboxQuery(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public Map<OutboxStatus, Long> statusCounts() {
        Map<OutboxStatus, Long> counts = new EnumMap<>(OutboxStatus.class);
        for (OutboxStatus status : OutboxStatus.values()) {
            counts.put(status, 0L);
        }
        String sql = "SELECT status, count(*) AS row_count FROM tandem_outbox GROUP BY status";
        return Jdbc.run(dataSource, "statusCounts query failed", conn ->
                Jdbc.withStatement(conn, sql, ps -> Jdbc.withResultSet(ps, rs -> {
                    while (rs.next()) {
                        counts.put(OutboxStatus.fromCode(rs.getInt("status")), rs.getLong("row_count"));
                    }
                    return counts;
                })));
    }

    @Override
    public List<OutboxRowView> search(OutboxSearchCriteria criteria) {
        Objects.requireNonNull(criteria, "criteria");
        List<Object> params = new ArrayList<>();
        List<String> predicates = new ArrayList<>();
        if (criteria.afterId() != null) {
            predicates.add("id > ?");
            params.add(criteria.afterId());
        }
        if (criteria.status() != null) {
            predicates.add("status = ?");
            params.add(criteria.status().code());
        }
        if (criteria.aggregateId() != null) {
            predicates.add("aggregate_id = ?");
            params.add(criteria.aggregateId().value());
        }
        if (criteria.aggregateType() != null) {
            predicates.add("aggregate_type = ?");
            params.add(criteria.aggregateType());
        }
        if (criteria.type() != null) {
            predicates.add("type = ?");
            params.add(criteria.type());
        }
        if (criteria.createdFrom() != null) {
            predicates.add("created_at >= ?");
            params.add(OffsetDateTime.ofInstant(criteria.createdFrom(), ZoneOffset.UTC));
        }
        if (criteria.createdTo() != null) {
            predicates.add("created_at <= ?");
            params.add(OffsetDateTime.ofInstant(criteria.createdTo(), ZoneOffset.UTC));
        }
        if (criteria.correlationId() != null) {
            // Exact match on the indexed column (idx_tandem_outbox_correlation) — never a LIKE/prefix
            // scan: this is the incident-time lookup and must stay index-backed on a large outbox.
            predicates.add("correlation_id = ?");
            params.add(criteria.correlationId());
        }

        StringBuilder sql = new StringBuilder("SELECT ").append(VIEW_COLUMNS).append(" FROM tandem_outbox");
        if (!predicates.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", predicates));
        }
        sql.append(" ORDER BY id LIMIT ?");
        params.add(criteria.limit());

        return Jdbc.run(dataSource, "search query failed", conn ->
                Jdbc.withStatement(conn, sql.toString(), ps -> {
                    bind(ps, params);
                    return Jdbc.withResultSet(ps, rs -> {
                        List<OutboxRowView> rows = new ArrayList<>();
                        while (rs.next()) {
                            rows.add(mapView(rs));
                        }
                        return rows;
                    });
                }));
    }

    @Override
    public Optional<OutboxRowDetail> findById(long id) {
        String sql = "SELECT " + VIEW_COLUMNS + ", payload::text AS payload, headers::text AS headers"
                + "  FROM tandem_outbox WHERE id = ?";
        return Jdbc.run(dataSource, "findById query failed", conn ->
                Jdbc.withStatement(conn, sql, ps -> {
                    ps.setLong(1, id);
                    return Jdbc.withResultSet(ps, rs -> {
                        if (!rs.next()) {
                            return Optional.empty();
                        }
                        OutboxRowView view = mapView(rs);
                        String payloadText = rs.getString("payload");
                        Map<String, String> headers = MiniJson.parseObject(rs.getString("headers"));
                        byte[] payload = payloadText == null
                                ? new byte[0]
                                : payloadText.getBytes(StandardCharsets.UTF_8);
                        return Optional.of(new OutboxRowDetail(view, payload, headers));
                    });
                }));
    }

    private static OutboxRowView mapView(ResultSet rs) throws SQLException {
        return new OutboxRowView(
                rs.getLong("id"),
                AggregateId.of(rs.getString("aggregate_id")),
                rs.getString("aggregate_type"),
                rs.getString("type"),
                rs.getLong("seq"),
                OutboxStatus.fromCode(rs.getInt("status")),
                rs.getInt("attempts"),
                rs.getInt("replays"),
                rs.getString("last_error"),
                rs.getString("discard_reason"),
                instantOrNull(rs, "next_attempt_at"),
                rs.getString("locked_by"),
                instantOrNull(rs, "locked_until"),
                instantOrNull(rs, "created_at"),
                rs.getString("correlation_id"));
    }

    private static Instant instantOrNull(ResultSet rs, String column) throws SQLException {
        OffsetDateTime odt = rs.getObject(column, OffsetDateTime.class);
        return odt == null ? null : odt.toInstant();
    }

    private static void bind(PreparedStatement ps, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
        }
    }
}
