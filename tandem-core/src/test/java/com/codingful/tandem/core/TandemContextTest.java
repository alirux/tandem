package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TandemContextTest {

    @AfterEach
    void clearContext() {
        TandemContext.clear();
    }

    @Test
    void GIVEN_no_correlation_id_set_WHEN_read_THEN_it_is_null() {
        assertThat(TandemContext.currentCorrelationId()).isNull();
    }

    @Test
    void GIVEN_a_correlation_id_set_WHEN_read_on_the_same_thread_THEN_it_is_returned() {
        TandemContext.setCorrelationId("corr-1");

        assertThat(TandemContext.currentCorrelationId()).isEqualTo("corr-1");
    }

    @Test
    void GIVEN_a_correlation_id_set_WHEN_cleared_THEN_a_later_read_is_null() {
        TandemContext.setCorrelationId("corr-1");

        TandemContext.clear();

        assertThat(TandemContext.currentCorrelationId()).isNull();
    }

    @Test
    void GIVEN_a_correlation_id_set_on_one_thread_WHEN_read_from_another_THEN_it_is_not_visible() throws InterruptedException {
        TandemContext.setCorrelationId("corr-main-thread");
        AtomicReference<String> readOnOtherThread = new AtomicReference<>("not-run");

        Thread other = new Thread(() -> readOnOtherThread.set(TandemContext.currentCorrelationId()));
        other.start();
        other.join();

        assertThat(readOnOtherThread.get()).isNull();
    }
}
