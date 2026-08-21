package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.exception.DuplicateSeqException;
import com.codingful.tandem.core.exception.OutboxInsertException;
import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.core.port.TracePropagator;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
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
            "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq, payload, headers, correlation_id) "
                    + "VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), ?)";

    /**
     * The same insert with {@code seq} left out, for a message that leaves the number to Tandem
     * ({@link OutboxMessage.Builder#managedSeq()}): omitting the column is what makes the
     * {@code tandem_seq} default fire, and it is the entire mechanism — no extra statement, no round
     * trip and no lock inside the caller's transaction (HLD-managed-seq §4.1).
     */
    private static final String INSERT_MANAGED_SEQ_SQL =
            "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, payload, headers, correlation_id) "
                    + "VALUES (?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), ?)";

    /** PostgreSQL {@code unique_violation} SQLSTATE (LLD-jdbc §2 → DuplicateSeqException). */
    private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";

    /**
     * Width of the {@code correlation_id} column, which the value is truncated to at insert (LLD-jdbc §2).
     * The correlation id normally originates <b>outside</b> this application — an inbound HTTP header, a
     * consumed message — so it is untrusted input; an unbounded value would otherwise fail the insert (and
     * with it the caller's business transaction) or bloat the index it exists to serve. The header copy in
     * {@code headers} is left at full length: it is what reaches Kafka, and it is not indexed.
     */
    public static final int MAX_CORRELATION_ID_LENGTH = 255;

    private final DataSource dataSource;
    private final int bucketCount;
    private final TracePropagator tracePropagator;

    /**
     * <p>This constructor does <b>no</b> I/O: the {@code dataSource} may be a transaction-aware proxy
     * that only yields a connection inside a caller transaction (that is the point — {@link #insert}
     * joins the caller's {@code @Transactional}), so it cannot be queried at construction time. The
     * bucket-count guard (LLD-bucket-count-guard) is therefore an explicit assembly step run against a
     * plain {@code DataSource} — {@link BucketCountGuard} — not something this constructor performs.
     *
     * <p>Delegates to the 3-arg constructor with {@link TracePropagator#NOOP} — trace/correlation
     * capture disabled (HLD-tracing.md §7).
     *
     * @param dataSource  the write-side connection source; the insert joins whatever transaction the
     *                     returned {@link Connection} is already part of
     * @param bucketCount must match the relay's {@link RelayConfig#bucketCount()} — baked into every
     *                     row inserted, never change after first deployment
     * @throws IllegalArgumentException if {@code bucketCount <= 0} or above
     *                                  {@link RelayConfig#MAX_BUCKET_COUNT} (the {@code SMALLINT} column bound)
     */
    public JdbcOutboxRepository(DataSource dataSource, int bucketCount) {
        this(dataSource, bucketCount, TracePropagator.NOOP);
    }

    /**
     * @param dataSource      the write-side connection source; see {@link #JdbcOutboxRepository(DataSource, int)}
     * @param bucketCount     must match the relay's {@link RelayConfig#bucketCount()}; see
     *                        {@link #JdbcOutboxRepository(DataSource, int)}
     * @param tracePropagator captures trace/correlation headers at insert time when
     *                        {@link TracePropagator#isEnabled()} (HLD-tracing.md §5); real adapters ship
     *                        in {@code tandem-spring-producer} / {@code tandem-tracing-otel}
     * @throws IllegalArgumentException if {@code bucketCount <= 0} or above
     *                                  {@link RelayConfig#MAX_BUCKET_COUNT} (the {@code SMALLINT} column bound)
     * @throws NullPointerException     if {@code dataSource} or {@code tracePropagator} is {@code null}
     */
    public JdbcOutboxRepository(DataSource dataSource, int bucketCount, TracePropagator tracePropagator) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.bucketCount = RelayConfig.boundedBucketCount(bucketCount);
        this.tracePropagator = Objects.requireNonNull(tracePropagator, "tracePropagator");
    }

    @Override
    public void insert(OutboxMessage message) {
        Objects.requireNonNull(message, "message");
        Jdbc.exec(dataSource, conn -> Jdbc.exec(conn, sqlFor(message), ps -> {
            bind(ps, message);
            ps.executeUpdate();
        }), e -> translate(e, message));
    }

    @Override
    public void insertAll(Collection<OutboxMessage> messages) {
        Objects.requireNonNull(messages, "messages");
        if (messages.isEmpty()) {
            return;
        }
        // Mutated inside the lambda below and read from the exception mapper on failure — an array
        // (not a plain local) because the mapper closes over it after the lambda returns/throws.
        OutboxMessage[] current = new OutboxMessage[1];
        Jdbc.exec(dataSource, conn -> {
            // The two modes need different column lists, so they cannot share one batch. Split on
            // CONSECUTIVE runs rather than partitioning by mode: the tiers promise rows land in the
            // collection's order, and grouping all managed messages together would reorder them.
            List<OutboxMessage> run = new ArrayList<>();
            for (OutboxMessage message : messages) {
                if (!run.isEmpty() && message.managedSeq() != run.get(0).managedSeq()) {
                    insertBatch(conn, run, current);
                    run.clear();
                }
                run.add(message);
            }
            insertBatch(conn, run, current);
        }, e -> translate(e, current[0]));
    }

    private void insertBatch(Connection conn, List<OutboxMessage> batch, OutboxMessage[] current)
            throws SQLException {
        Jdbc.exec(conn, sqlFor(batch.get(0)), ps -> {
            for (OutboxMessage message : batch) {
                current[0] = message;
                bind(ps, message);
                ps.addBatch();
            }
            ps.executeBatch();
        });
    }

    private static String sqlFor(OutboxMessage message) {
        return message.managedSeq() ? INSERT_MANAGED_SEQ_SQL : INSERT_SQL;
    }

    /** Parameter positions are walked rather than fixed: a managed-{@code seq} insert has one fewer. */
    private void bind(PreparedStatement ps, OutboxMessage message) throws SQLException {
        int index = 1;
        ps.setString(index++, message.aggregateId().value());
        ps.setString(index++, message.aggregateType());
        if (message.type() == null) {
            ps.setNull(index++, Types.VARCHAR);
        } else {
            ps.setString(index++, message.type());
        }
        ps.setInt(index++, BucketHash.bucketFor(message.aggregateId().value(), bucketCount));
        if (!message.managedSeq()) {
            ps.setLong(index++, message.seq());
        }
        ps.setString(index++, new String(message.payload(), StandardCharsets.UTF_8));
        Map<String, String> headers = effectiveHeaders(message);
        ps.setString(index++, MiniJson.writeObject(headers));
        String correlationId = truncate(headers.get(TandemHeaders.CORRELATION_ID));
        if (correlationId == null) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, correlationId);
        }
    }

    /** Bounds an untrusted, externally-originated correlation id to the column width; {@code null} stays null. */
    private static String truncate(String correlationId) {
        return correlationId == null || correlationId.length() <= MAX_CORRELATION_ID_LENGTH
                ? correlationId
                : correlationId.substring(0, MAX_CORRELATION_ID_LENGTH);
    }

    /**
     * The headers actually stored: the message headers with {@code contentType} folded into
     * {@code headers["content-type"]} (the typed field is the single source of truth — it overrides
     * any {@code content-type} already in the map), plus any trace/correlation context captured from
     * {@link #tracePropagator} for a key not already present — an explicit header the caller set is
     * never overwritten by captured context (LLD-jdbc §2, HLD-tracing.md §5).
     *
     * <p>Also the source of the {@code correlation_id} column: whatever ends up under
     * {@code headers["correlation-id"]} here is copied into it, so the column can never disagree with
     * the header that actually reaches Kafka.
     */
    private Map<String, String> effectiveHeaders(OutboxMessage message) {
        Map<String, String> headers = new LinkedHashMap<>(message.headers());
        if (message.contentType() != null) {
            headers.put(TandemHeaders.CONTENT_TYPE, message.contentType());
        }
        if (tracePropagator.isEnabled()) {
            for (Map.Entry<String, String> captured : tracePropagator.capture().entrySet()) {
                headers.putIfAbsent(captured.getKey(), captured.getValue());
            }
        }
        return headers;
    }

    private static OutboxInsertException translate(SQLException e, OutboxMessage message) {
        String aggregate = message == null ? "?" : message.aggregateId().toString();
        // Never a number for a managed message: it has none until this insert succeeds, and printing
        // one would send whoever reads the failure looking for a row that does not exist.
        String seq = message == null ? "?" : message.managedSeq() ? "managed" : Long.toString(message.seq());
        if (SQLSTATE_UNIQUE_VIOLATION.equals(e.getSQLState())) {
            return new DuplicateSeqException(
                    "duplicate (aggregate_id, seq) = (" + aggregate + ", " + seq + ')', e);
        }
        return new OutboxInsertException(
                "failed to insert outbox row for (aggregate_id, seq) = (" + aggregate + ", " + seq + ')', e);
    }
}
