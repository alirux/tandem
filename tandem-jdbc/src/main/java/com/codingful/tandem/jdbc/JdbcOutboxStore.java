package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.exception.TandemException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.sql.DataSource;

/**
 * Relay-side JDBC persistence (LLD-jdbc §3.3–§3.7), PostgreSQL baseline. Each operation runs in its
 * own short transaction (the connection's autocommit) — exclusivity during publish is carried by
 * {@code status = IN_FLIGHT} + the {@code locked_until} lease, not an open transaction (Q9). Depends
 * only on {@code java.sql}.
 */
public final class JdbcOutboxStore implements OutboxStore {

    // Head-of-chain claim (§3.3): the earliest PENDING+due row of each aggregate with no earlier
    // unfinished (0/1/3) row, locked with SKIP LOCKED, marked IN_FLIGHT, returned via RETURNING.
    private static final String CLAIM_SQL =
            "WITH claimed AS ("
                    + "  SELECT o.id FROM tandem_outbox o"
                    + "   WHERE o.bucket = ANY(?)"
                    + "     AND o.status = 0"
                    + "     AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= now())"
                    + "     AND NOT EXISTS ("
                    + "         SELECT 1 FROM tandem_outbox e"
                    + "          WHERE e.aggregate_id = o.aggregate_id"
                    + "            AND e.id < o.id"
                    + "            AND e.status IN (0, 1, 3))"
                    + "   ORDER BY o.id"
                    + "   FOR UPDATE SKIP LOCKED"
                    + "   LIMIT ?)"
                    + " UPDATE tandem_outbox o"
                    + "    SET status = 1, locked_by = ?, locked_until = now() + (? * interval '1 millisecond')"
                    + "   FROM claimed c"
                    + "  WHERE o.id = c.id"
                    + " RETURNING " + OutboxRowMapper.COLUMNS;

    // Also clears the lease columns: a DONE row keeping a stale locked_by/locked_until would read as
    // still-owned in the table (and later in the Admin API), and clearing keeps parity with InMemoryOutbox.
    private static final String MARK_DONE_SQL =
            "UPDATE tandem_outbox SET status = 2, locked_by = NULL, locked_until = NULL WHERE id = ANY(?)";

    // next_attempt_at is anchored on the DB clock (like locked_until above), not on the relay's: the
    // claim compares it with the DB's now(), so anchoring it locally would shift a row's due time by
    // the relay-to-DB clock offset (§3.2/§3.6). The caller supplies only the relative backoff.
    private static final String MARK_FOR_RETRY_SQL =
            "UPDATE tandem_outbox"
                    + "   SET status = 0, attempts = attempts + 1, last_error = ?,"
                    + "       next_attempt_at = now() + (? * interval '1 millisecond'),"
                    + "       locked_by = NULL, locked_until = NULL"
                    + " WHERE id = ?";

    private static final String MARK_FAILED_SQL =
            "UPDATE tandem_outbox"
                    + "   SET status = 3, attempts = attempts + 1, last_error = ?,"
                    + "       locked_by = NULL, locked_until = NULL"
                    + " WHERE id = ?";

    // Failover (§3.5): reset expired IN_FLIGHT leases; each reclaim counts as an attempt and
    // quarantines to FAILED at maxAttempts so a crash-poison row cannot loop forever.
    private static final String RECLAIM_SQL =
            "UPDATE tandem_outbox"
                    + "   SET attempts = attempts + 1, last_error = ?,"
                    + "       status = CASE WHEN attempts + 1 >= ? THEN 3 ELSE 0 END,"
                    + "       locked_by = NULL, locked_until = NULL"
                    + " WHERE status = 1 AND locked_until < now()";

    private static final String CLEANUP_SQL =
            "DELETE FROM tandem_outbox"
                    + " WHERE id IN (SELECT id FROM tandem_outbox"
                    + "               WHERE status IN (2, 4) AND created_at < ?"
                    + "               ORDER BY id LIMIT ?)";

    static final String LEASE_EXPIRED_ERROR = "lease expired (worker crash or stall) before ack";

    private final DataSource dataSource;
    private final int maxAttempts;

    /**
     * @param dataSource  used for every operation; each call opens and closes its own short-lived connection
     * @param maxAttempts retriable failures (including lease-reclaims) allowed before a row is quarantined to {@code FAILED}
     * @throws IllegalArgumentException if {@code maxAttempts <= 0}
     */
    public JdbcOutboxStore(DataSource dataSource, int maxAttempts) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        this.maxAttempts = maxAttempts;
    }

    @Override
    public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
        Objects.requireNonNull(buckets, "buckets");
        Objects.requireNonNull(workerId, "workerId");
        if (buckets.isEmpty() || batchSize <= 0) {
            return List.of();
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CLAIM_SQL)) {
            Array bucketArray = conn.createArrayOf("integer", buckets.toArray());
            ps.setArray(1, bucketArray);
            ps.setInt(2, batchSize);
            ps.setString(3, workerId);
            ps.setLong(4, lease.toMillis());
            List<OutboxRecord> claimed = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    claimed.add(OutboxRowMapper.map(rs));
                }
            }
            return claimed;
        } catch (SQLException e) {
            throw new TandemException("claimBatch failed", e);
        }
    }

    @Override
    public void markDone(long id) {
        markDoneBatch(List.of(id));
    }

    /**
     * Mark several acked rows {@code DONE} in one statement (§3.4.1). The ids may span different
     * aggregates — mark-DONE is order-independent — so batching is safe and cuts DB round-trips.
     */
    @Override
    public void markDoneBatch(Collection<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(MARK_DONE_SQL)) {
            ps.setArray(1, conn.createArrayOf("bigint", ids.toArray()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new TandemException("markDoneBatch failed", e);
        }
    }

    @Override
    public void markForRetry(long id, String error, Duration retryDelay) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(MARK_FOR_RETRY_SQL)) {
            ps.setString(1, error);
            if (retryDelay == null) {
                ps.setNull(2, Types.BIGINT);          // now() + NULL = NULL → due immediately
            } else {
                ps.setLong(2, retryDelay.toMillis());
            }
            ps.setLong(3, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new TandemException("markForRetry failed for id " + id, e);
        }
    }

    @Override
    public void markFailed(long id, String error) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(MARK_FAILED_SQL)) {
            ps.setString(1, error);
            ps.setLong(2, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new TandemException("markFailed failed for id " + id, e);
        }
    }

    @Override
    public int reclaimExpiredLeases() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(RECLAIM_SQL)) {
            ps.setString(1, LEASE_EXPIRED_ERROR);
            ps.setInt(2, maxAttempts);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new TandemException("reclaimExpiredLeases failed", e);
        }
    }

    @Override
    public int cleanup(Instant doneBefore, int batchSize) {
        Objects.requireNonNull(doneBefore, "doneBefore");
        if (batchSize <= 0) {
            return 0;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CLEANUP_SQL)) {
            ps.setObject(1, OffsetDateTime.ofInstant(doneBefore, ZoneOffset.UTC));
            ps.setInt(2, batchSize);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new TandemException("cleanup failed", e);
        }
    }
}
