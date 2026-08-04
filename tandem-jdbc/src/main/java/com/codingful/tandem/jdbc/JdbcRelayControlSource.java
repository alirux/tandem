package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.exception.TandemException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;

/**
 * JDBC {@link RelayControlSource}: publishes this instance's {@link Coordination} to {@code tandem_meta}
 * once at startup, and caches the desired pause state on {@link #refresh()} so the claim hot path never
 * touches the database (HLD-admin-api §4.1).
 */
public final class JdbcRelayControlSource implements RelayControlSource {

    private static final String UPSERT_COORDINATION_SQL =
            "INSERT INTO tandem_meta (key, value, updated_at) VALUES ('coordination', ?, now())"
                    + " ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()";

    private static final String READ_RELAY_PAUSED_SQL =
            "SELECT value FROM tandem_meta WHERE key = 'relay_paused'";

    private static final String READ_PAUSED_BUCKETS_SQL =
            "SELECT bucket FROM tandem_bucket_lease WHERE paused = true";

    private final DataSource dataSource;
    private final Coordination coordination;

    private volatile boolean wholeRelayPaused;
    private final AtomicReference<Set<Integer>> pausedBuckets = new AtomicReference<>(Set.of());

    public JdbcRelayControlSource(DataSource dataSource, Coordination coordination) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.coordination = Objects.requireNonNull(coordination, "coordination");
    }

    @Override
    public void onStart() {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(UPSERT_COORDINATION_SQL)) {
            ps.setString(1, coordination.name());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new TandemException("recording relay coordination mode failed", e);
        }
    }

    @Override
    public void refresh() {
        try (Connection conn = dataSource.getConnection()) {
            wholeRelayPaused = readRelayPaused(conn);
            pausedBuckets.set(coordination == Coordination.LEASE ? readPausedBuckets(conn) : Set.of());
        } catch (SQLException e) {
            throw new TandemException("refreshing relay control state failed", e);
        }
    }

    @Override
    public boolean wholeRelayPaused() {
        return wholeRelayPaused;
    }

    @Override
    public boolean bucketPaused(int bucket) {
        return pausedBuckets.get().contains(bucket);
    }

    private static boolean readRelayPaused(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(READ_RELAY_PAUSED_SQL);
                ResultSet rs = ps.executeQuery()) {
            return rs.next() && "true".equals(rs.getString(1));
        }
    }

    private static Set<Integer> readPausedBuckets(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(READ_PAUSED_BUCKETS_SQL);
                ResultSet rs = ps.executeQuery()) {
            Set<Integer> buckets = new HashSet<>();
            while (rs.next()) {
                buckets.add(rs.getInt(1));
            }
            return buckets;
        }
    }
}
