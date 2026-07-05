package com.codingful.tandem.core.port;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class TopicRouterTest {

    private static OutboxRecord recordOfType(String aggregateType) {
        OutboxMessage message = OutboxMessage.builder()
                .aggregateId("id-1")
                .aggregateType(aggregateType)
                .seq(1)
                .payload(new byte[] {0})
                .build();
        return OutboxRecord.builder().id(1).message(message).createdAt(Instant.EPOCH).build();
    }

    @Test
    void GIVEN_a_single_word_aggregate_type_WHEN_routed_THEN_it_lower_cases_and_adds_the_suffix() {
        TopicRouter router = TopicRouter.kebabWithSuffix("-topic");

        assertThat(router.topicFor(recordOfType("Order"))).isEqualTo("order-topic");
    }

    @Test
    void GIVEN_a_camel_case_aggregate_type_WHEN_routed_THEN_word_boundaries_become_hyphens() {
        TopicRouter router = TopicRouter.kebabWithSuffix("-topic");

        assertThat(router.topicFor(recordOfType("OrderLine"))).isEqualTo("order-line-topic");
    }

    @Test
    void GIVEN_an_acronym_prefixed_type_WHEN_converted_to_a_topic_name_THEN_word_boundaries_are_detected() {
        assertThat(TopicRouter.kebabCase("HTTPServer")).isEqualTo("http-server");
    }
}
