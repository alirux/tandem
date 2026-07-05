package com.codingful.tandem.core.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ExceptionHierarchyTest {

    @Test
    void GIVEN_a_message_WHEN_TandemException_constructed_THEN_it_is_an_unchecked_exception_with_that_message() {
        TandemException ex = new TandemException("boom");
        assertThat(ex).isInstanceOf(RuntimeException.class);
        assertThat(ex.getMessage()).isEqualTo("boom");
    }

    @Test
    void GIVEN_a_cause_WHEN_TandemException_constructed_THEN_the_cause_is_accessible() {
        Throwable cause = new RuntimeException("root");
        assertThat(new TandemException("wrapped", cause).getCause()).isSameAs(cause);
    }

    @Test
    void GIVEN_a_message_WHEN_OutboxInsertException_constructed_THEN_it_is_a_TandemException() {
        OutboxInsertException ex = new OutboxInsertException("insert failed");
        assertThat(ex).isInstanceOf(TandemException.class);
        assertThat(ex.getMessage()).isEqualTo("insert failed");
    }

    @Test
    void GIVEN_a_cause_WHEN_OutboxInsertException_constructed_THEN_the_cause_is_accessible() {
        Throwable cause = new RuntimeException("sql");
        assertThat(new OutboxInsertException("insert failed", cause).getCause()).isSameAs(cause);
    }

    @Test
    void GIVEN_a_message_WHEN_DuplicateSeqException_constructed_THEN_it_is_an_OutboxInsertException() {
        DuplicateSeqException ex = new DuplicateSeqException("dup seq");
        assertThat(ex).isInstanceOf(OutboxInsertException.class);
        assertThat(ex.getMessage()).isEqualTo("dup seq");
    }

    @Test
    void GIVEN_a_cause_WHEN_DuplicateSeqException_constructed_THEN_the_cause_is_accessible() {
        Throwable cause = new RuntimeException("constraint violation");
        assertThat(new DuplicateSeqException("dup seq", cause).getCause()).isSameAs(cause);
    }

    @Test
    void GIVEN_a_message_WHEN_PayloadSerializationException_constructed_THEN_it_is_a_TandemException() {
        PayloadSerializationException ex = new PayloadSerializationException("cannot serialize");
        assertThat(ex).isInstanceOf(TandemException.class);
        assertThat(ex.getMessage()).isEqualTo("cannot serialize");
    }

    @Test
    void GIVEN_a_cause_WHEN_PayloadSerializationException_constructed_THEN_the_cause_is_accessible() {
        Throwable cause = new RuntimeException("json error");
        assertThat(new PayloadSerializationException("cannot serialize", cause).getCause()).isSameAs(cause);
    }

    @Test
    void GIVEN_a_message_WHEN_TandemConfigurationException_constructed_THEN_it_is_a_TandemException() {
        TandemConfigurationException ex = new TandemConfigurationException("bad config");
        assertThat(ex).isInstanceOf(TandemException.class);
        assertThat(ex.getMessage()).isEqualTo("bad config");
    }

    @Test
    void GIVEN_a_cause_WHEN_TandemConfigurationException_constructed_THEN_the_cause_is_accessible() {
        Throwable cause = new RuntimeException("invalid param");
        assertThat(new TandemConfigurationException("bad config", cause).getCause()).isSameAs(cause);
    }

    @Test
    void GIVEN_a_retriable_dispatch_failure_WHEN_OutboxDispatchException_constructed_THEN_isRetriable_is_true() {
        OutboxDispatchException ex = new OutboxDispatchException("network error", true);
        assertThat(ex).isInstanceOf(TandemException.class);
        assertThat(ex.getMessage()).isEqualTo("network error");
        assertThat(ex.isRetriable()).isTrue();
    }

    @Test
    void GIVEN_a_permanent_dispatch_failure_WHEN_OutboxDispatchException_constructed_THEN_isRetriable_is_false() {
        assertThat(new OutboxDispatchException("auth error", false).isRetriable()).isFalse();
    }

    @Test
    void GIVEN_a_cause_WHEN_OutboxDispatchException_constructed_THEN_the_cause_and_verdict_are_both_accessible() {
        Throwable cause = new RuntimeException("kafka");
        OutboxDispatchException ex = new OutboxDispatchException("dispatch failed", true, cause);
        assertThat(ex.getCause()).isSameAs(cause);
        assertThat(ex.isRetriable()).isTrue();
    }
}
