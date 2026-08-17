package com.codingful.tandem.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
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

    private static final String UPSERT_HEARTBEAT_INTERVAL_SQL =
            "INSERT INTO tandem_meta (key, value, updated_at) VALUES ('relay_heartbeat_interval_seconds', ?, now())"
                    + " ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()";

    private static final String HEARTBEAT_SQL =
            "UPDATE tandem_meta SET updated_at = now() WHERE key = 'coordination'";

    private static final String READ_RELAY_PAUSED_SQL =
            "SELECT value FROM tandem_meta WHERE key = 'relay_paused'";

    private static final String READ_PAUSED_BUCKETS_SQL =
            "SELECT bucket FROM tandem_bucket_lease WHERE paused = true";

    private final DataSource dataSource;
    private final Coordination coordination;
    private final Duration heartbeatInterval;

    private volatile boolean wholeRelayPaused;
    private final AtomicReference<Set<Integer>> pausedBuckets = new AtomicReference<>(Set.of());

    /**
     * @param heartbeatInterval how often {@link #heartbeat()} is called (the relay's own
     *                          {@code reclaimInterval}) — published once at {@link #onStart()} so the
     *                          Admin API can compute a staleness threshold for {@code RelayStatus.state
     *                          == DOWN} without guessing it (HLD-admin-api §4.1)
     */
    public JdbcRelayControlSource(DataSource dataSource, Coordination coordination, Duration heartbeatInterval) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.coordination = Objects.requireNonNull(coordination, "coordination");
        this.heartbeatInterval = Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
    }

    @Override
    public void onStart() {
        Jdbc.run(dataSource, "recording relay coordination mode failed", conn -> {
            try (PreparedStatement coordinationPs = conn.prepareStatement(UPSERT_COORDINATION_SQL);
                    PreparedStatement intervalPs = conn.prepareStatement(UPSERT_HEARTBEAT_INTERVAL_SQL)) {
                coordinationPs.setString(1, coordination.name());
                coordinationPs.executeUpdate();
                intervalPs.setString(1, Long.toString(heartbeatInterval.toSeconds()));
                intervalPs.executeUpdate();
            }
        });
    }

    @Override
    public void heartbeat() {
        Jdbc.run(dataSource, "relay heartbeat failed", conn -> {
            try (PreparedStatement ps = conn.prepareStatement(HEARTBEAT_SQL)) {
                ps.executeUpdate();
            }
        });
    }

    @Override
    public void refresh() {
        Jdbc.run(dataSource, "refreshing relay control state failed", conn -> {
            wholeRelayPaused = readRelayPaused(conn);
            pausedBuckets.set(coordination == Coordination.LEASE ? readPausedBuckets(conn) : Set.of());
        });
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
