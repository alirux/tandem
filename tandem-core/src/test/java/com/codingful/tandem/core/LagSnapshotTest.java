package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LagSnapshotTest {

    private static final Instant OLDEST = Instant.parse("2024-01-01T00:00:00Z");

    @Test
    void GIVEN_nothing_waiting_WHEN_the_backlog_age_is_asked_for_THEN_it_is_zero() {
        LagSnapshot empty = new LagSnapshot(0, Optional.empty());

        assertThat(empty.pending()).isZero();
        assertThat(empty.ageSecondsAt(OLDEST)).isZero();
    }

    @Test
    void GIVEN_an_event_waiting_since_a_known_moment_WHEN_the_age_is_asked_for_THEN_it_counts_from_that_moment() {
        LagSnapshot lag = new LagSnapshot(3, Optional.of(OLDEST));

        assertThat(lag.ageSecondsAt(OLDEST.plusSeconds(90))).isEqualTo(90);
        assertThat(lag.ageSecondsAt(OLDEST.plusMillis(1_500))).isEqualTo(1.5);
    }

    @Test
    void GIVEN_a_reading_taken_before_the_oldest_event_WHEN_the_age_is_asked_for_THEN_it_never_goes_negative() {
        // The relay's clock and the database's need not agree; a backlog cannot be minus ten seconds old.
        LagSnapshot lag = new LagSnapshot(1, Optional.of(OLDEST));

        assertThat(lag.ageSecondsAt(OLDEST.minusSeconds(10))).isZero();
    }

    @Test
    void GIVEN_a_negative_count_of_waiting_events_WHEN_a_reading_is_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> new LagSnapshot(-1, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("-1");
    }

    @Test
    void GIVEN_a_missing_oldest_moment_WHEN_a_reading_is_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> new LagSnapshot(0, null)).isInstanceOf(NullPointerException.class);
    }
}
