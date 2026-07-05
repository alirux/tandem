package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.TandemHeaders;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Maps a {@code tandem_outbox} result row to an {@link OutboxRecord}. Reads <b>named columns</b> only
 * (never {@code SELECT *}) and tolerates extra columns, so the schema can grow additively (AGENTS,
 * HLD §1.4). The {@code payload}/{@code headers} JSONB columns are selected as text.
 */
final class OutboxRowMapper {

    private OutboxRowMapper() {
    }

    /**
     * The column list the claim query selects, qualified with the {@code o} alias (the claim's
     * {@code UPDATE … FROM claimed c} also exposes {@code id}, so an unqualified list is ambiguous).
     * Read back by the labels {@code id, aggregate_id, …} ({@code payload}/{@code headers} aliased).
     */
    static final String COLUMNS =
            "o.id, o.aggregate_id, o.aggregate_type, o.type, o.seq, o.payload::text AS payload, "
                    + "o.headers::text AS headers, o.status, o.locked_by, o.locked_until, o.attempts, "
                    + "o.last_error, o.next_attempt_at, o.created_at";

    static OutboxRecord map(ResultSet rs) throws SQLException {
        Map<String, String> headers = MiniJson.parseObject(rs.getString("headers"));
        String payload = rs.getString("payload");

        OutboxMessage.Builder message = OutboxMessage.builder()
                .aggregateId(rs.getString("aggregate_id"))
                .aggregateType(rs.getString("aggregate_type"))
                .type(rs.getString("type"))
                .seq(rs.getLong("seq"))
                .payload(payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8))
                .headers(headers);
        String contentType = headers.get(TandemHeaders.CONTENT_TYPE);
        if (contentType != null) {
            message.contentType(contentType);
        }

        return OutboxRecord.builder()
                .id(rs.getLong("id"))
                .message(message.build())
                .status(OutboxStatus.fromCode(rs.getInt("status")))
                .attempts(rs.getInt("attempts"))
                .lockedBy(rs.getString("locked_by"))
                .lockedUntil(instantOrNull(rs, "locked_until"))
                .lastError(rs.getString("last_error"))
                .nextAttemptAt(instantOrNull(rs, "next_attempt_at"))
                .createdAt(instantOrNull(rs, "created_at"))
                .build();
    }

    private static Instant instantOrNull(ResultSet rs, String column) throws SQLException {
        OffsetDateTime odt = rs.getObject(column, OffsetDateTime.class);
        return odt == null ? null : odt.toInstant();
    }
}
