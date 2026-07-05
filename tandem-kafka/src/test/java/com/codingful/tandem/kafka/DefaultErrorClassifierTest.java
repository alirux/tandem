package com.codingful.tandem.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.exception.OutboxDispatchException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Test;

class DefaultErrorClassifierTest {

    private final ErrorClassifier classifier = new DefaultErrorClassifier();

    @Test
    void GIVEN_a_transient_timeout_WHEN_classified_THEN_it_is_retriable() {
        assertThat(classifier.classify(new TimeoutException("broker slow")).isRetriable()).isTrue();
    }

    @Test
    void GIVEN_an_unknown_error_WHEN_classified_THEN_it_defaults_to_retriable() {
        assertThat(classifier.classify(new RuntimeException("who knows")).isRetriable()).isTrue();
    }

    @Test
    void GIVEN_a_record_too_large_WHEN_classified_THEN_it_is_permanent() {
        assertThat(classifier.classify(new RecordTooLargeException("too big")).isRetriable()).isFalse();
    }

    @Test
    void GIVEN_an_authorization_error_WHEN_classified_THEN_it_is_permanent() {
        assertThat(classifier.classify(new TopicAuthorizationException("denied")).isRetriable()).isFalse();
    }

    @Test
    void GIVEN_an_invalid_topic_WHEN_classified_THEN_it_is_permanent() {
        assertThat(classifier.classify(new InvalidTopicException("bad/topic")).isRetriable()).isFalse();
    }

    @Test
    void GIVEN_a_cause_WHEN_classified_THEN_the_verdict_carries_the_cause_and_a_message() {
        TimeoutException cause = new TimeoutException("broker slow");
        OutboxDispatchException result = classifier.classify(cause);

        assertThat(result).hasCause(cause);
        assertThat(result.getMessage()).contains("retriable").contains("TimeoutException");
    }
}
