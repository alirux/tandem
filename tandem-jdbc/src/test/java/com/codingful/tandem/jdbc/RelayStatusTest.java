package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RelayStatusTest {

    private static final Instant CYCLE_AT = Instant.parse("2024-01-01T00:00:00Z");

    @Test
    void GIVEN_no_worker_alive_WHEN_the_cycle_age_is_asked_for_THEN_it_is_zero() {
        RelayStatus status = new RelayStatus("relay-1", RelayStatus.State.STOPPED, Coordination.SINGLE,
                2, 0, Optional.empty());

        assertThat(status.oldestWorkerCycleAgeSecondsAt(CYCLE_AT)).isZero();
    }

    @Test
    void GIVEN_a_worker_that_last_progressed_at_a_known_moment_WHEN_the_cycle_age_is_asked_for_THEN_it_counts_from_that_moment() {
        RelayStatus status = new RelayStatus("relay-1", RelayStatus.State.RUNNING, Coordination.SINGLE,
                2, 2, Optional.of(CYCLE_AT));

        assertThat(status.oldestWorkerCycleAgeSecondsAt(CYCLE_AT.plusSeconds(45))).isEqualTo(45);
    }

    @Test
    void GIVEN_a_reading_taken_before_the_last_recorded_cycle_WHEN_the_cycle_age_is_asked_for_THEN_it_never_goes_negative() {
        // The relay's clock and the caller's need not agree; a cycle cannot be minus ten seconds old.
        RelayStatus status = new RelayStatus("relay-1", RelayStatus.State.RUNNING, Coordination.SINGLE,
                1, 1, Optional.of(CYCLE_AT));

        assertThat(status.oldestWorkerCycleAgeSecondsAt(CYCLE_AT.minusSeconds(10))).isZero();
    }

    @Test
    void GIVEN_a_negative_configured_worker_count_WHEN_a_reading_is_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> new RelayStatus("relay-1", RelayStatus.State.STOPPED, Coordination.SINGLE,
                -1, 0, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("-1");
    }

    @Test
    void GIVEN_a_negative_alive_worker_count_WHEN_a_reading_is_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> new RelayStatus("relay-1", RelayStatus.State.STOPPED, Coordination.SINGLE,
                1, -1, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("-1");
    }

    @Test
    void GIVEN_a_missing_required_component_WHEN_a_reading_is_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> new RelayStatus(null, RelayStatus.State.STOPPED, Coordination.SINGLE,
                1, 0, Optional.empty()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RelayStatus("relay-1", null, Coordination.SINGLE, 1, 0, Optional.empty()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RelayStatus("relay-1", RelayStatus.State.STOPPED, null, 1, 0, Optional.empty()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RelayStatus("relay-1", RelayStatus.State.STOPPED, Coordination.SINGLE,
                1, 0, null))
                .isInstanceOf(NullPointerException.class);
    }
}
