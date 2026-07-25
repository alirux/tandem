package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.PayloadSerializationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class JacksonPayloadSerializerTest {

    private record OrderPlaced(String orderId, int amount) {
    }

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JacksonPayloadSerializer serializer = new JacksonPayloadSerializer(objectMapper);

    @Test
    void GIVEN_an_object_payload_WHEN_it_is_serialized_THEN_the_bytes_round_trip_back_to_it() throws Exception {
        OrderPlaced event = new OrderPlaced("order-42", 1250);

        byte[] bytes = serializer.serialize(event);

        assertThat(objectMapper.readValue(bytes, OrderPlaced.class)).isEqualTo(event);
    }

    @Test
    void GIVEN_the_serializer_WHEN_asked_for_its_content_type_THEN_it_is_json() {
        assertThat(serializer.contentType()).isEqualTo("application/json");
    }

    @Test
    void GIVEN_a_payload_jackson_cannot_serialize_WHEN_it_is_serialized_THEN_it_fails_with_a_tandem_exception() {
        // A bare Object has no serializable properties, which Jackson rejects by default.
        assertThatThrownBy(() -> serializer.serialize(new Object()))
                .isInstanceOf(PayloadSerializationException.class);
    }
}
