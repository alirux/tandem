package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.BucketStatusView;
import com.codingful.tandem.core.RelayCoordinationMode;
import com.codingful.tandem.core.RelayStatusView;
import com.codingful.tandem.core.WorkerView;
import com.codingful.tandem.core.exception.TandemException;
import com.codingful.tandem.core.port.RelayQuery;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * Admin-API read side over relay-control state (HLD-admin-api §4.1, {@link RelayQuery}): coordination
 * mode, whole-relay status, and per-bucket/per-worker observability — {@code tandem_meta},
 * {@code tandem_bucket_lease}, {@code tandem_relay_member}. Named columns only, never {@code SELECT *}
 * (AGENTS, HLD §1.4).
 */
public final class JdbcRelayQuery implements RelayQuery {

    private static final String META_SQL =
            "SELECT key, value FROM tandem_meta WHERE key IN ('bucket_count', 'relay_paused', 'coordination')";

    // Same predicate BucketLeaseManager's own uncoveredBuckets() uses: free/expired AND has pending work.
    private static final String UNCOVERED_COUNT_SQL =
            "SELECT count(*) FROM tandem_bucket_lease bl"
                    + " WHERE (bl.owner IS NULL OR bl.lease_until < now())"
                    + "   AND EXISTS (SELECT 1 FROM tandem_outbox o WHERE o.bucket = bl.bucket AND o.status = 0)";

    private static final String LIVE_WORKER_COUNT_SQL =
            "SELECT count(*) FROM tandem_relay_member WHERE lease_until > now()";

    private static final String BUCKET_COLUMNS =
            "bl.bucket, bl.owner, bl.lease_until, bl.paused,"
                    + " (bl.owner IS NOT NULL AND bl.lease_until > now()) AS covered,"
                    + " COALESCE(o.pending_count, 0) AS pending_count, o.oldest_pending_at";

    private static final String BUCKETS_SQL =
            "SELECT " + BUCKET_COLUMNS
                    + "  FROM tandem_bucket_lease bl"
                    + "  LEFT JOIN (SELECT bucket, count(*) AS pending_count, min(created_at) AS oldest_pending_at"
                    + "               FROM tandem_outbox WHERE status = 0 GROUP BY bucket) o ON o.bucket = bl.bucket"
                    + " ORDER BY bl.bucket";

    private static final String UNCOVERED_BUCKETS_SQL =
            "SELECT " + BUCKET_COLUMNS
                    + "  FROM tandem_bucket_lease bl"
                    + "  LEFT JOIN (SELECT bucket, count(*) AS pending_count, min(created_at) AS oldest_pending_at"
                    + "               FROM tandem_outbox WHERE status = 0 GROUP BY bucket) o ON o.bucket = bl.bucket"
                    + " WHERE (bl.owner IS NULL OR bl.lease_until < now()) AND COALESCE(o.pending_count, 0) > 0"
                    + " ORDER BY bl.bucket";

    private static final String BUCKET_BY_ID_SQL =
            "SELECT " + BUCKET_COLUMNS
                    + "  FROM tandem_bucket_lease bl"
                    + "  LEFT JOIN (SELECT bucket, count(*) AS pending_count, min(created_at) AS oldest_pending_at"
                    + "               FROM tandem_outbox WHERE status = 0 GROUP BY bucket) o ON o.bucket = bl.bucket"
                    + " WHERE bl.bucket = ?";

    private static final String WORKERS_SQL =
            "SELECT m.owner, m.updated_at,"
                    + "       (SELECT count(*) FROM tandem_bucket_lease bl"
                    + "         WHERE bl.owner = m.owner AND bl.lease_until > now()) AS bucket_count"
                    + "  FROM tandem_relay_member m"
                    + " WHERE m.lease_until > now()"
                    + " ORDER BY m.owner";

    private final DataSource dataSource;

    /** @param dataSource used for every query; each call opens and closes its own connection */
    public JdbcRelayQuery(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public RelayCoordinationMode coordinationMode() {
        String value = readMeta().get("coordination");
        return value == null ? RelayCoordinationMode.SINGLE : RelayCoordinationMode.valueOf(value);
    }

    @Override
    public RelayStatusView status() {
        Map<String, String> meta = readMeta();
        boolean paused = "true".equals(meta.get("relay_paused"));
        int bucketCount = meta.containsKey("bucket_count") ? Integer.parseInt(meta.get("bucket_count")) : 0;
        RelayCoordinationMode mode = meta.containsKey("coordination")
                ? RelayCoordinationMode.valueOf(meta.get("coordination"))
                : RelayCoordinationMode.SINGLE;
        if (mode == RelayCoordinationMode.SINGLE) {
            return new RelayStatusView(paused, bucketCount, 0, 0);
        }
        return new RelayStatusView(paused, bucketCount, count(UNCOVERED_COUNT_SQL), count(LIVE_WORKER_COUNT_SQL));
    }

    @Override
    public List<BucketStatusView> buckets(boolean uncoveredOnly) {
        String sql = uncoveredOnly ? UNCOVERED_BUCKETS_SQL : BUCKETS_SQL;
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            List<BucketStatusView> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(mapBucket(rs));
            }
            return rows;
        } catch (SQLException e) {
            throw new TandemException("buckets query failed", e);
        }
    }

    @Override
    public Optional<BucketStatusView> bucket(int bucket) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(BUCKET_BY_ID_SQL)) {
            ps.setInt(1, bucket);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapBucket(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new TandemException("bucket query failed", e);
        }
    }

    @Override
    public List<WorkerView> workers() {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(WORKERS_SQL);
                ResultSet rs = ps.executeQuery()) {
            List<WorkerView> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(new WorkerView(rs.getString("owner"), instantOrNull(rs, "updated_at"), rs.getInt("bucket_count")));
            }
            return rows;
        } catch (SQLException e) {
            throw new TandemException("workers query failed", e);
        }
    }

    private Map<String, String> readMeta() {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(META_SQL);
                ResultSet rs = ps.executeQuery()) {
            Map<String, String> meta = new HashMap<>();
            while (rs.next()) {
                meta.put(rs.getString("key"), rs.getString("value"));
            }
            return meta;
        } catch (SQLException e) {
            throw new TandemException("tandem_meta query failed", e);
        }
    }

    private int count(String sql) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new TandemException("count query failed", e);
        }
    }

    private static BucketStatusView mapBucket(ResultSet rs) throws SQLException {
        Instant oldestPending = instantOrNull(rs, "oldest_pending_at");
        Double lagAgeSeconds = oldestPending == null
                ? null
                : Math.max(0, (Instant.now().toEpochMilli() - oldestPending.toEpochMilli()) / 1000d);
        return new BucketStatusView(
                rs.getInt("bucket"),
                rs.getString("owner"),
                instantOrNull(rs, "lease_until"),
                rs.getBoolean("covered"),
                rs.getBoolean("paused"),
                rs.getLong("pending_count"),
                lagAgeSeconds);
    }

    private static Instant instantOrNull(ResultSet rs, String column) throws SQLException {
        OffsetDateTime odt = rs.getObject(column, OffsetDateTime.class);
        return odt == null ? null : odt.toInstant();
    }
}
