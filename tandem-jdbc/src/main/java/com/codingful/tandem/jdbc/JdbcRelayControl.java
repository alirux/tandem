package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.port.RelayControl;
import java.sql.PreparedStatement;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * Admin-API write side over relay-control state (HLD-admin-api §4.1, {@link RelayControl}): the
 * whole-relay pause flag in {@code tandem_meta}, and per-bucket pause/release in
 * {@code tandem_bucket_lease}. The relay itself only ever reads this state (via
 * {@link RelayControlSource}); this adapter is never on the relay's own claim/dispatch path.
 */
public final class JdbcRelayControl implements RelayControl {

    private static final String UPSERT_RELAY_PAUSED_SQL =
            "INSERT INTO tandem_meta (key, value, updated_at) VALUES ('relay_paused', ?, now())"
                    + " ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()";

    private static final String SET_BUCKET_PAUSED_SQL =
            "UPDATE tandem_bucket_lease SET paused = ?, updated_at = now() WHERE bucket = ?";

    private static final String RELEASE_BUCKET_SQL =
            "UPDATE tandem_bucket_lease SET owner = NULL, lease_until = NULL, updated_at = now() WHERE bucket = ?";

    private final DataSource dataSource;

    /** @param dataSource used for every write; each call opens and closes its own connection */
    public JdbcRelayControl(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public void pauseAll() {
        setRelayPaused(true);
    }

    @Override
    public void resumeAll() {
        setRelayPaused(false);
    }

    @Override
    public boolean pauseBucket(int bucket) {
        return setBucketPaused(bucket, true);
    }

    @Override
    public boolean resumeBucket(int bucket) {
        return setBucketPaused(bucket, false);
    }

    @Override
    public boolean releaseBucket(int bucket) {
        return Jdbc.run(dataSource, "releaseBucket failed for bucket " + bucket, conn -> {
            try (PreparedStatement ps = conn.prepareStatement(RELEASE_BUCKET_SQL)) {
                ps.setInt(1, bucket);
                return ps.executeUpdate() > 0;
            }
        });
    }

    private void setRelayPaused(boolean paused) {
        Jdbc.exec(dataSource, "setting relay_paused failed", conn -> {
            try (PreparedStatement ps = conn.prepareStatement(UPSERT_RELAY_PAUSED_SQL)) {
                ps.setString(1, Boolean.toString(paused));
                ps.executeUpdate();
            }
        });
    }

    private boolean setBucketPaused(int bucket, boolean paused) {
        return Jdbc.run(dataSource, "setting bucket paused failed for bucket " + bucket, conn -> {
            try (PreparedStatement ps = conn.prepareStatement(SET_BUCKET_PAUSED_SQL)) {
                ps.setBoolean(1, paused);
                ps.setInt(2, bucket);
                return ps.executeUpdate() > 0;
            }
        });
    }
}
