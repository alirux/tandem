package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.TandemContext;
import com.codingful.tandem.core.TandemHeaders;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class MdcCorrelationTracePropagatorTest {

    private static final String MDC_KEY = "correlationId";

    private final MdcCorrelationTracePropagator propagator = new MdcCorrelationTracePropagator(MDC_KEY);

    @AfterEach
    void clearContext() {
        MDC.clear();
        TandemContext.clear();
    }

    @Test
    void GIVEN_an_mdc_value_WHEN_captured_THEN_it_is_the_correlation_id_header() {
        MDC.put(MDC_KEY, "corr-mdc");

        assertThat(propagator.isEnabled()).isTrue();
        assertThat(propagator.capture()).isEqualTo(Map.of(TandemHeaders.CORRELATION_ID, "corr-mdc"));
    }

    @Test
    void GIVEN_no_mdc_value_and_an_explicit_TandemContext_value_WHEN_captured_THEN_the_explicit_value_is_used() {
        TandemContext.setCorrelationId("corr-explicit");

        assertThat(propagator.capture()).isEqualTo(Map.of(TandemHeaders.CORRELATION_ID, "corr-explicit"));
    }

    @Test
    void GIVEN_both_an_mdc_value_and_an_explicit_TandemContext_value_WHEN_captured_THEN_the_mdc_value_wins() {
        MDC.put(MDC_KEY, "corr-mdc");
        TandemContext.setCorrelationId("corr-explicit");

        assertThat(propagator.capture()).isEqualTo(Map.of(TandemHeaders.CORRELATION_ID, "corr-mdc"));
    }

    @Test
    void GIVEN_neither_an_mdc_value_nor_an_explicit_TandemContext_value_WHEN_captured_THEN_nothing_is_captured() {
        assertThat(propagator.capture()).isEmpty();
    }
}
