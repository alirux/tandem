package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.LagSnapshot;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.OutboxStore;
import java.sql.Array;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
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

    // Backlog = PENDING only: an IN_FLIGHT row is work in progress, and if its relay dies the lease
    // reclaim returns it to PENDING, so it re-enters this reading on its own (HLD §7, §3.5).
    private static final String LAG_SQL =
            "SELECT count(*) AS pending, min(created_at) AS oldest FROM tandem_outbox WHERE status = 0";

    // Live, not accumulated: a FAILED row can later move to DISCARDED (admin-only), and the next
    // reading must reflect that — a running total of failure events would never go back down.
    private static final String FAILED_COUNT_SQL = "SELECT count(*) FROM tandem_outbox WHERE status = 3";

    // PENDING rows a FAILED row of the same aggregate stands in front of — the complement of CLAIM_SQL's
    // head-of-chain NOT EXISTS, narrowed to the one status that never resolves on its own. Ordered by id
    // for the same reason the claim is (§3.3), not by seq.
    //
    // Driven from the FAILED rows rather than from the backlog: grouping them first (there are normally
    // none, and few when there are) leaves one index range scan per affected aggregate on
    // idx_tandem_outbox_aggregate, so the cost tracks the number of blocked rows instead of the size of
    // the outbox. Written the other way round — scanning every PENDING row and asking whether a failure
    // precedes it — it would be a probe per backlog row, on a query that runs on every metrics tick.
    //
    // The two predicates on the last lines overlap by design, and neither can be killed on its own: a row
    // after a FAILED head can never be claimed, so it is necessarily still PENDING, which makes each one
    // redundant while the other holds. `o.id > first_failed_id` is the one that earns its place on cost
    // (it is what the index range scan keys on); `status = 0` states the intent and guards the invariant
    // in case an admin-side transition ever breaks it. Together they pin the query against the wrong
    // implementation that actually threatens it — counting the whole aggregate rather than its tail.
    private static final String BLOCKED_COUNT_SQL =
            "SELECT count(*) FROM tandem_outbox o"
                    + " JOIN (SELECT aggregate_id, min(id) AS first_failed_id FROM tandem_outbox"
                    + "        WHERE status = 3 GROUP BY aggregate_id) f"
                    + "   ON f.aggregate_id = o.aggregate_id AND o.id > f.first_failed_id"
                    + " WHERE o.status = 0";

    // The seq-regression discriminator (HLD §8), read by primary key and only after the relay has
    // already seen a seq go backwards — never part of CLAIM_SQL's projection, which is what keeps this
    // column off the hot path and a pre-column relay working against a migrated database (§1.4).
    private static final String REPLAYS_SQL = "SELECT replays FROM tandem_outbox WHERE id = ?";

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
        return Jdbc.run(dataSource, "claimBatch failed", conn ->
                Jdbc.withStatement(conn, CLAIM_SQL, ps -> {
                    Array bucketArray = conn.createArrayOf("integer", buckets.toArray());
                    ps.setArray(1, bucketArray);
                    ps.setInt(2, batchSize);
                    ps.setString(3, workerId);
                    ps.setLong(4, lease.toMillis());
                    return Jdbc.withResultSet(ps, rs -> {
                        List<OutboxRecord> claimed = new ArrayList<>();
                        while (rs.next()) {
                            claimed.add(OutboxRowMapper.map(rs));
                        }
                        return claimed;
                    });
                }));
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
        Jdbc.exec(dataSource, "markDoneBatch failed", conn ->
                Jdbc.exec(conn, MARK_DONE_SQL, ps -> {
                    ps.setArray(1, conn.createArrayOf("bigint", ids.toArray()));
                    ps.executeUpdate();
                }));
    }

    @Override
    public void markForRetry(long id, String error, Duration retryDelay) {
        Jdbc.exec(dataSource, "markForRetry failed for id " + id, conn ->
                Jdbc.exec(conn, MARK_FOR_RETRY_SQL, ps -> {
                    ps.setString(1, error);
                    if (retryDelay == null) {
                        ps.setNull(2, Types.BIGINT);          // now() + NULL = NULL → due immediately
                    } else {
                        ps.setLong(2, retryDelay.toMillis());
                    }
                    ps.setLong(3, id);
                    ps.executeUpdate();
                }));
    }

    @Override
    public void markFailed(long id, String error) {
        Jdbc.exec(dataSource, "markFailed failed for id " + id, conn ->
                Jdbc.exec(conn, MARK_FAILED_SQL, ps -> {
                    ps.setString(1, error);
                    ps.setLong(2, id);
                    ps.executeUpdate();
                }));
    }

    @Override
    public int reclaimExpiredLeases() {
        return Jdbc.run(dataSource, "reclaimExpiredLeases failed", conn ->
                Jdbc.withStatement(conn, RECLAIM_SQL, ps -> {
                    ps.setString(1, LEASE_EXPIRED_ERROR);
                    ps.setInt(2, maxAttempts);
                    return ps.executeUpdate();
                }));
    }

    /**
     * Both gauges in one statement, so the count and the oldest row always describe the same instant
     * (§4). Named columns, never {@code SELECT *} (§1.4).
     */
    @Override
    public Optional<LagSnapshot> lag() {
        return Jdbc.run(dataSource, "lag failed", conn ->
                Jdbc.withStatement(conn, LAG_SQL, ps -> Jdbc.withResultSet(ps, rs -> {
                    rs.next();   // an aggregate without GROUP BY always returns exactly one row
                    long pending = rs.getLong(1);
                    OffsetDateTime oldest = rs.getObject(2, OffsetDateTime.class);   // NULL when nothing is pending
                    return Optional.of(new LagSnapshot(pending,
                            Optional.ofNullable(oldest).map(OffsetDateTime::toInstant)));
                })));
    }

    @Override
    public OptionalLong failedCount() {
        return Jdbc.run(dataSource, "failedCount failed", conn ->
                Jdbc.withStatement(conn, FAILED_COUNT_SQL, ps -> Jdbc.withResultSet(ps, rs -> {
                    rs.next();   // an aggregate without GROUP BY always returns exactly one row
                    return OptionalLong.of(rs.getLong(1));
                })));
    }

    @Override
    public OptionalLong blockedCount() {
        return Jdbc.run(dataSource, "blockedCount failed", conn ->
                Jdbc.withStatement(conn, BLOCKED_COUNT_SQL, ps -> Jdbc.withResultSet(ps, rs -> {
                    rs.next();   // an aggregate without GROUP BY always returns exactly one row
                    return OptionalLong.of(rs.getLong(1));
                })));
    }

    @Override
    public OptionalInt replaysOf(long id) {
        return Jdbc.run(dataSource, "replaysOf failed for id " + id, conn ->
                Jdbc.withStatement(conn, REPLAYS_SQL, ps -> {
                    ps.setLong(1, id);
                    // Empty when the row is gone — cleanup can delete a DONE row between the ack and this
                    // lookup, and an absent row cannot be shown not to have been replayed.
                    return Jdbc.withResultSet(ps, rs -> rs.next() ? OptionalInt.of(rs.getInt(1)) : OptionalInt.empty());
                }));
    }

    @Override
    public int cleanup(Instant doneBefore, int batchSize) {
        Objects.requireNonNull(doneBefore, "doneBefore");
        if (batchSize <= 0) {
            return 0;
        }
        return Jdbc.run(dataSource, "cleanup failed", conn ->
                Jdbc.withStatement(conn, CLEANUP_SQL, ps -> {
                    ps.setObject(1, OffsetDateTime.ofInstant(doneBefore, ZoneOffset.UTC));
                    ps.setInt(2, batchSize);
                    return ps.executeUpdate();
                }));
    }
}
