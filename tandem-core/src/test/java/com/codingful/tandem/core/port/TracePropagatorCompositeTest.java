package com.codingful.tandem.core.port;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TracePropagatorCompositeTest {

    /** A real propagator capturing exactly what it was built with — the collaborator, not a stand-in. */
    private static TracePropagator capturing(boolean enabled, Map<String, String> headers) {
        return new TracePropagator() {

            @Override
            public boolean isEnabled() {
                return enabled;
            }

            @Override
            public Map<String, String> capture() {
                return headers;
            }
        };
    }

    @Test
    void GIVEN_context_from_several_sources_WHEN_captured_THEN_the_row_carries_all_of_it() {
        TracePropagator composite = TracePropagator.composite(
                capturing(true, Map.of("traceparent", "tp")),
                capturing(true, Map.of("correlation-id", "corr")));

        assertThat(composite.isEnabled()).isTrue();
        assertThat(composite.capture()).containsOnly(
                Map.entry("traceparent", "tp"), Map.entry("correlation-id", "corr"));
    }

    @Test
    void GIVEN_two_sources_offering_the_same_header_WHEN_captured_THEN_the_higher_precedence_one_wins() {
        TracePropagator composite = TracePropagator.composite(
                capturing(true, Map.of("correlation-id", "first")),
                capturing(true, Map.of("correlation-id", "second")));

        assertThat(composite.capture()).containsExactly(Map.entry("correlation-id", "first"));
    }

    @Test
    void GIVEN_one_source_switched_off_WHEN_captured_THEN_only_the_live_one_contributes() {
        TracePropagator composite = TracePropagator.composite(
                capturing(false, Map.of("traceparent", "tp")),
                capturing(true, Map.of("correlation-id", "corr")));

        assertThat(composite.isEnabled()).isTrue();
        assertThat(composite.capture()).containsExactly(Map.entry("correlation-id", "corr"));
    }

    @Test
    void GIVEN_every_source_switched_off_WHEN_captured_THEN_capture_stays_off_and_costs_nothing() {
        TracePropagator composite = TracePropagator.composite(
                capturing(false, Map.of("traceparent", "tp")), TracePropagator.NOOP);

        assertThat(composite.isEnabled()).isFalse();
        assertThat(composite.capture()).isEmpty();
    }

    @Test
    void GIVEN_no_sources_at_all_WHEN_captured_THEN_capture_stays_off() {
        TracePropagator composite = TracePropagator.composite();

        assertThat(composite.isEnabled()).isFalse();
        assertThat(composite.capture()).isEmpty();
    }

    @Test
    void GIVEN_a_missing_source_WHEN_the_composite_is_built_THEN_it_fails_immediately() {
        assertThatNullPointerException()
                .isThrownBy(() -> TracePropagator.composite(TracePropagator.NOOP, null));
    }
}
