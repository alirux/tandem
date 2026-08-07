package com.codingful.tandem.core.port;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.TandemContext;
import com.codingful.tandem.core.TandemHeaders;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TracePropagatorFromTandemContextTest {

    private final TracePropagator propagator = TracePropagator.fromTandemContext();

    @AfterEach
    void clearContext() {
        TandemContext.clear();
    }

    @Test
    void GIVEN_a_correlation_id_set_for_the_unit_of_work_WHEN_a_row_is_written_THEN_it_carries_that_id() {
        TandemContext.setCorrelationId("corr-1");

        assertThat(propagator.isEnabled()).isTrue();
        assertThat(propagator.capture()).containsExactly(Map.entry(TandemHeaders.CORRELATION_ID, "corr-1"));
    }

    @Test
    void GIVEN_no_correlation_id_for_the_unit_of_work_WHEN_a_row_is_written_THEN_it_carries_no_empty_header() {
        // Still enabled: capture is wired, this unit of work simply has nothing to contribute — the
        // insert must not gain a header with a null or blank value.
        assertThat(propagator.isEnabled()).isTrue();
        assertThat(propagator.capture()).isEmpty();
    }

    @Test
    void GIVEN_a_correlation_id_and_a_trace_context_from_elsewhere_WHEN_merged_THEN_the_row_carries_both() {
        TandemContext.setCorrelationId("corr-1");
        TracePropagator traceContext = new TracePropagator() {

            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public Map<String, String> capture() {
                return Map.of(TandemHeaders.TRACEPARENT, "tp");
            }
        };

        Map<String, String> captured = TracePropagator.composite(traceContext, propagator).capture();

        assertThat(captured).containsOnly(
                Map.entry(TandemHeaders.TRACEPARENT, "tp"), Map.entry(TandemHeaders.CORRELATION_ID, "corr-1"));
    }
}
