package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.exception.PayloadSerializationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class CollectingOutboxCollectorTest {

    private record OrderPlaced(String orderId) {
    }

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JacksonPayloadSerializer serializer = new JacksonPayloadSerializer(objectMapper);

    @Test
    void GIVEN_an_object_payload_WHEN_recorded_THEN_it_becomes_a_serialized_message_with_the_given_identity()
            throws Exception {
        String aggregateType = "Order";
        String aggregateId = "order-1";
        long seq = 7L;
        OrderPlaced payload = new OrderPlaced(aggregateId);
        CollectingOutboxCollector collector = new CollectingOutboxCollector(serializer);

        collector.record(aggregateType, aggregateId, seq, payload);

        assertThat(collector.collected()).singleElement().satisfies(message -> {
            assertThat(message.aggregateType()).isEqualTo(aggregateType);
            assertThat(message.aggregateId().value()).isEqualTo(aggregateId);
            assertThat(message.seq()).isEqualTo(seq);
            assertThat(message.contentType()).isEqualTo(serializer.contentType());
            assertThat(objectMapper.readValue(message.payload(), OrderPlaced.class)).isEqualTo(payload);
        });
    }

    @Test
    void GIVEN_a_prebuilt_message_WHEN_added_THEN_it_is_collected_unchanged() {
        OutboxMessage message = OutboxMessage.builder()
                .aggregateType("Order").aggregateId("order-2").seq(1L).payload(new byte[] {1, 2, 3}).build();
        CollectingOutboxCollector collector = new CollectingOutboxCollector(serializer);

        collector.add(message);

        assertThat(collector.collected()).containsExactly(message);
    }

    @Test
    void GIVEN_no_serializer_WHEN_an_object_payload_is_recorded_THEN_it_fails_fast() {
        CollectingOutboxCollector collector = new CollectingOutboxCollector(null);

        assertThatThrownBy(() -> collector.record("Order", "order-3", 1L, new OrderPlaced("order-3")))
                .isInstanceOf(PayloadSerializationException.class);
    }

    @Test
    void GIVEN_several_records_WHEN_collected_THEN_their_order_is_preserved() {
        CollectingOutboxCollector collector = new CollectingOutboxCollector(serializer);

        collector.record("Order", "order-4", 1L, new OrderPlaced("first"));
        collector.record("Order", "order-4", 2L, new OrderPlaced("second"));

        assertThat(collector.collected()).extracting(OutboxMessage::seq).containsExactly(1L, 2L);
    }
}
