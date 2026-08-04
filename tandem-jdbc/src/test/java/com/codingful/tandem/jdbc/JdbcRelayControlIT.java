package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class JdbcRelayControlIT extends AbstractPostgresIT {

    private final JdbcRelayControl control = new JdbcRelayControl(DATA_SOURCE);

    @Test
    void GIVEN_no_prior_state_WHEN_the_relay_is_paused_THEN_relay_paused_is_recorded_true() {
        control.pauseAll();

        assertThat(metaValue("relay_paused")).isEqualTo("true");
    }

    @Test
    void GIVEN_a_paused_relay_WHEN_resumed_THEN_relay_paused_is_recorded_false() {
        control.pauseAll();

        control.resumeAll();

        assertThat(metaValue("relay_paused")).isEqualTo("false");
    }

    @Test
    void GIVEN_an_existing_bucket_WHEN_paused_THEN_it_is_marked_paused() {
        boolean found = control.pauseBucket(5);

        assertThat(found).isTrue();
        assertThat(bucketPaused(5)).isTrue();
    }

    @Test
    void GIVEN_a_paused_bucket_WHEN_resumed_THEN_it_is_no_longer_paused() {
        control.pauseBucket(5);

        boolean found = control.resumeBucket(5);

        assertThat(found).isTrue();
        assertThat(bucketPaused(5)).isFalse();
    }

    @Test
    void GIVEN_a_bucket_outside_the_seeded_range_WHEN_paused_THEN_it_is_refused() {
        assertThat(control.pauseBucket(99_999)).isFalse();
    }

    @Test
    void GIVEN_an_owned_bucket_WHEN_released_THEN_its_lease_is_cleared() {
        execute("UPDATE tandem_bucket_lease SET owner = 'instance-1', lease_until = now() + interval '1 minute' WHERE bucket = 5");

        boolean found = control.releaseBucket(5);

        assertThat(found).isTrue();
        assertThat(ownerOf(5)).isNull();
    }

    @Test
    void GIVEN_a_bucket_outside_the_seeded_range_WHEN_released_THEN_it_is_refused() {
        assertThat(control.releaseBucket(99_999)).isFalse();
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

    private static boolean bucketPaused(int bucket) {
        try (Connection conn = DATA_SOURCE.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT paused FROM tandem_bucket_lease WHERE bucket = ?")) {
            ps.setInt(1, bucket);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String ownerOf(int bucket) {
        try (Connection conn = DATA_SOURCE.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT owner FROM tandem_bucket_lease WHERE bucket = ?")) {
            ps.setInt(1, bucket);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
