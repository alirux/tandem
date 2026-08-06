package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;

class JdbcRelayControlSourceIT extends AbstractPostgresIT {

    @Test
    void GIVEN_a_relay_starting_up_WHEN_onStart_runs_THEN_its_coordination_mode_is_recorded() {
        new JdbcRelayControlSource(DATA_SOURCE, Coordination.LEASE, Duration.ofSeconds(5)).onStart();

        assertThat(metaValue("coordination")).isEqualTo("LEASE");
    }

    @Test
    void GIVEN_a_relay_restarting_with_a_different_mode_WHEN_onStart_runs_THEN_the_recorded_mode_is_overwritten() {
        new JdbcRelayControlSource(DATA_SOURCE, Coordination.LEASE, Duration.ofSeconds(5)).onStart();

        new JdbcRelayControlSource(DATA_SOURCE, Coordination.SINGLE, Duration.ofSeconds(5)).onStart();

        assertThat(metaValue("coordination")).isEqualTo("SINGLE");
    }

    @Test
    void GIVEN_the_admin_API_paused_the_relay_WHEN_refreshed_THEN_wholeRelayPaused_reflects_it() {
        execute("INSERT INTO tandem_meta (key, value) VALUES ('relay_paused', 'true')");
        RelayControlSource source = new JdbcRelayControlSource(DATA_SOURCE, Coordination.SINGLE, Duration.ofSeconds(5));

        source.refresh();

        assertThat(source.wholeRelayPaused()).isTrue();
    }

    @Test
    void GIVEN_no_relay_paused_key_WHEN_refreshed_THEN_wholeRelayPaused_defaults_to_false() {
        RelayControlSource source = new JdbcRelayControlSource(DATA_SOURCE, Coordination.SINGLE, Duration.ofSeconds(5));

        source.refresh();

        assertThat(source.wholeRelayPaused()).isFalse();
    }

    @Test
    void GIVEN_LEASE_coordination_and_a_paused_bucket_WHEN_refreshed_THEN_bucketPaused_reflects_it() {
        execute("UPDATE tandem_bucket_lease SET paused = true WHERE bucket = 5");
        RelayControlSource source = new JdbcRelayControlSource(DATA_SOURCE, Coordination.LEASE, Duration.ofSeconds(5));

        source.refresh();

        assertThat(source.bucketPaused(5)).isTrue();
        assertThat(source.bucketPaused(6)).isFalse();
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_refreshed_THEN_bucketPaused_is_always_false_even_if_the_column_says_otherwise() {
        // A paused column value under SINGLE means nothing - the relay never reads tandem_bucket_lease
        // in that mode - so the source must not even look at it.
        execute("UPDATE tandem_bucket_lease SET paused = true WHERE bucket = 5");
        RelayControlSource source = new JdbcRelayControlSource(DATA_SOURCE, Coordination.SINGLE, Duration.ofSeconds(5));

        source.refresh();

        assertThat(source.bucketPaused(5)).isFalse();
    }

    @Test
    void GIVEN_a_relay_starting_up_WHEN_onStart_runs_THEN_its_heartbeat_interval_is_recorded() {
        new JdbcRelayControlSource(DATA_SOURCE, Coordination.SINGLE, Duration.ofSeconds(7)).onStart();

        assertThat(metaValue("relay_heartbeat_interval_seconds")).isEqualTo("7");
    }

    @Test
    void GIVEN_a_relay_WHEN_heartbeat_runs_THEN_the_coordination_rows_updated_at_advances() throws InterruptedException {
        RelayControlSource source = new JdbcRelayControlSource(DATA_SOURCE, Coordination.SINGLE, Duration.ofSeconds(5));
        source.onStart();
        Instant afterStart = metaUpdatedAt("coordination");
        Thread.sleep(5);   // real DB clock, not a fake one - needs actual time to pass to see now() advance

        source.heartbeat();

        assertThat(metaUpdatedAt("coordination")).isAfter(afterStart);
    }

    private static Instant metaUpdatedAt(String key) {
        try (Connection conn = DATA_SOURCE.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT updated_at FROM tandem_meta WHERE key = ?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getObject(1, OffsetDateTime.class).toInstant();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String metaValue(String key) {
        try (Connection conn = DATA_SOURCE.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT value FROM tandem_meta WHERE key = ?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
