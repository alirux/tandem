package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.exception.DuplicateSeqException;
import com.codingful.tandem.core.exception.OutboxInsertException;
import com.codingful.tandem.core.port.OutboxRepository;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * Write-side JDBC adapter (LLD-jdbc §2): inserts rows into {@code tandem_outbox} <b>using the
 * connection the {@link DataSource} hands back</b> and never opening or committing its own
 * transaction — so with a transaction-aware {@code DataSource} the insert joins the caller's
 * {@code @Transactional}, atomically with the business state change.
 *
 * <p>It depends only on {@code java.sql} (JDK): <b>no Kafka, no JSON library</b> (the {@code payload}
 * is the bytes the client already serialized; only {@code headers} is encoded, via {@link MiniJson}) —
 * the minimal client footprint (§1.3).
 *
 * <p><b>Payload constraint:</b> the {@code payload} column is PostgreSQL {@code jsonb}, so this
 * adapter requires {@link OutboxMessage#payload()} to be valid UTF-8 JSON. A non-JSON payload is
 * rejected by Postgres at insert time and surfaces as an {@link OutboxInsertException} (SQLSTATE
 * {@code 22P02}, {@code invalid_text_representation}). Callers that must carry opaque (non-JSON)
 * bytes should JSON-encode them (e.g. a base64 string) before insert, or use a schema whose
 * {@code payload} column is {@code bytea}/{@code text}. Note also that {@code jsonb} stores the
 * <b>parsed</b> value, not the original text: the payload read back (and published to Kafka) is
 * semantically identical but not byte-identical — key order, whitespace and number formatting are
 * normalized — so consumers must not verify signatures/hashes computed over the original bytes.
 */
public final class JdbcOutboxRepository implements OutboxRepository {

    private static final String INSERT_SQL =
            "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq, payload, headers) "
                    + "VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb))";

    /** PostgreSQL {@code unique_violation} SQLSTATE (LLD-jdbc §2 → DuplicateSeqException). */
    private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";

    private final DataSource dataSource;
    private final int bucketCount;

    /**
     * @param dataSource  the write-side connection source; the insert joins whatever transaction the
     *                     returned {@link Connection} is already part of
     * @param bucketCount must match the relay's {@link RelayConfig#bucketCount()} — baked into every
     *                     row inserted, never change after first deployment
     * @throws IllegalArgumentException if {@code bucketCount <= 0} or above
     *                                  {@link RelayConfig#MAX_BUCKET_COUNT} (the {@code SMALLINT} column bound)
     */
    public JdbcOutboxRepository(DataSource dataSource, int bucketCount) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.bucketCount = RelayConfig.boundedBucketCount(bucketCount);
    }

    @Override
    public void insert(OutboxMessage message) {
        Objects.requireNonNull(message, "message");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            bind(ps, message);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw translate(e, message);
        }
    }

    @Override
    public void insertAll(Collection<OutboxMessage> messages) {
        Objects.requireNonNull(messages, "messages");
        if (messages.isEmpty()) {
            return;
        }
        OutboxMessage current = null;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            for (OutboxMessage message : messages) {
                current = message;
                bind(ps, message);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw translate(e, current);
        }
    }

    private void bind(PreparedStatement ps, OutboxMessage message) throws SQLException {
        ps.setString(1, message.aggregateId().value());
        ps.setString(2, message.aggregateType());
        if (message.type() == null) {
            ps.setNull(3, Types.VARCHAR);
        } else {
            ps.setString(3, message.type());
        }
        ps.setInt(4, BucketHash.bucketFor(message.aggregateId().value(), bucketCount));
        ps.setLong(5, message.seq());
        ps.setString(6, new String(message.payload(), StandardCharsets.UTF_8));
        ps.setString(7, MiniJson.writeObject(effectiveHeaders(message)));
    }

    /**
     * The headers actually stored: the message headers with {@code contentType} folded into
     * {@code headers["content-type"]}. The typed field is the single source of truth — it overrides
     * any {@code content-type} already in the map (LLD-jdbc §2).
     */
    private static Map<String, String> effectiveHeaders(OutboxMessage message) {
        Map<String, String> headers = new LinkedHashMap<>(message.headers());
        if (message.contentType() != null) {
            headers.put(TandemHeaders.CONTENT_TYPE, message.contentType());
        }
        return headers;
    }

    private static OutboxInsertException translate(SQLException e, OutboxMessage message) {
        String aggregate = message == null ? "?" : message.aggregateId().toString();
        long seq = message == null ? -1 : message.seq();
        if (SQLSTATE_UNIQUE_VIOLATION.equals(e.getSQLState())) {
            return new DuplicateSeqException(
                    "duplicate (aggregate_id, seq) = (" + aggregate + ", " + seq + ')', e);
        }
        return new OutboxInsertException(
                "failed to insert outbox row for (aggregate_id, seq) = (" + aggregate + ", " + seq + ')', e);
    }
}
