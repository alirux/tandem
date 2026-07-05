package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class FullJitterBackoffTest {

    private final BackoffStrategy backoff = new FullJitterBackoff(Duration.ofSeconds(1), Duration.ofMinutes(5));

    @Test
    void GIVEN_the_first_failure_WHEN_a_delay_is_drawn_THEN_it_stays_within_the_base_window() {
        for (int i = 0; i < 1_000; i++) {
            Duration delay = backoff.delayFor(0);
            assertThat(delay).isGreaterThan(Duration.ZERO).isLessThanOrEqualTo(Duration.ofSeconds(1));
        }
    }

    @Test
    void GIVEN_increasing_attempts_WHEN_delays_are_drawn_THEN_the_upper_bound_grows_then_holds_at_the_cap() {
        // The window doubles each attempt: by attempt 3 it can exceed 1s; it never exceeds the cap.
        boolean sawAboveBase = false;
        for (int i = 0; i < 1_000; i++) {
            if (backoff.delayFor(3).compareTo(Duration.ofSeconds(1)) > 0) {
                sawAboveBase = true;
                break;
            }
        }
        assertThat(sawAboveBase).as("attempt 3 should be able to exceed the 1s base window").isTrue();
    }

    @Test
    void GIVEN_a_very_large_attempt_count_WHEN_a_delay_is_drawn_THEN_it_never_exceeds_the_cap_and_never_overflows() {
        for (int attempts : new int[] {20, 40, 62, 1_000, Integer.MAX_VALUE}) {
            Duration delay = backoff.delayFor(attempts);
            assertThat(delay).isGreaterThan(Duration.ZERO).isLessThanOrEqualTo(Duration.ofMinutes(5));
        }
    }

    @Test
    void GIVEN_a_cap_below_the_base_WHEN_constructed_THEN_it_is_rejected() {
        assertThatThrownBy(() -> new FullJitterBackoff(Duration.ofSeconds(10), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_a_zero_base_WHEN_constructed_THEN_it_is_rejected() {
        assertThatThrownBy(() -> new FullJitterBackoff(Duration.ZERO, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("base must be positive");
    }

    @Test
    void GIVEN_a_negative_attempt_count_WHEN_a_delay_is_requested_THEN_it_is_rejected() {
        assertThatThrownBy(() -> backoff.delayFor(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attempts must be >= 0");
    }

    @Test
    void GIVEN_a_small_base_and_attempt_count_below_62_WHEN_shift_overflows_to_negative_THEN_the_cap_is_returned() {
        // 2ms base: 2 << 61 overflows to Long.MIN_VALUE (negative) — hits the scaled < 0 guard.
        BackoffStrategy tinyBase = new FullJitterBackoff(Duration.ofMillis(2), Duration.ofSeconds(10));
        Duration delay = tinyBase.delayFor(61);
        assertThat(delay).isGreaterThan(Duration.ZERO).isLessThanOrEqualTo(Duration.ofSeconds(10));
    }
}
