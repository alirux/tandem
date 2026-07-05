package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OutboxMessageTest {

    private static OutboxMessage.Builder validBuilder() {
        return OutboxMessage.builder()
                .aggregateId("order-1")
                .aggregateType("Order")
                .seq(1)
                .payload(new byte[] {1, 2, 3});
    }

    @Test
    void GIVEN_all_required_fields_WHEN_built_THEN_the_values_round_trip() {
        OutboxMessage message = validBuilder()
                .type("com.acme.order.placed")
                .contentType("application/json")
                .header("correlation-id", "abc")
                .build();

        assertThat(message.aggregateId()).isEqualTo(AggregateId.of("order-1"));
        assertThat(message.aggregateType()).isEqualTo("Order");
        assertThat(message.type()).isEqualTo("com.acme.order.placed");
        assertThat(message.seq()).isEqualTo(1);
        assertThat(message.payload()).containsExactly(1, 2, 3);
        assertThat(message.contentType()).isEqualTo("application/json");
        assertThat(message.headers()).containsEntry("correlation-id", "abc");
    }

    @Test
    void GIVEN_no_type_or_content_type_WHEN_built_THEN_they_are_null_and_headers_empty() {
        OutboxMessage message = validBuilder().build();

        assertThat(message.type()).isNull();
        assertThat(message.contentType()).isNull();
        assertThat(message.headers()).isEmpty();
    }

    @Test
    void GIVEN_a_missing_aggregate_id_WHEN_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> OutboxMessage.builder()
                .aggregateType("Order")
                .payload(new byte[] {1})
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void GIVEN_a_blank_aggregate_type_WHEN_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> validBuilder().aggregateType("  ").build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_a_missing_payload_WHEN_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> OutboxMessage.builder()
                .aggregateId("order-1")
                .aggregateType("Order")
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void GIVEN_a_payload_array_WHEN_mutated_after_build_THEN_the_stored_payload_is_unaffected() {
        byte[] source = {1, 2, 3};
        OutboxMessage message = validBuilder().payload(source).build();

        source[0] = 99;                 // mutate the caller's array
        message.payload()[1] = 99;      // mutate the returned copy

        assertThat(message.payload()).containsExactly(1, 2, 3);
    }

    @Test
    void GIVEN_a_headers_map_WHEN_mutated_after_build_THEN_the_stored_headers_are_unaffected_and_immutable() {
        Map<String, String> source = new HashMap<>();
        source.put("k", "v");
        OutboxMessage message = validBuilder().headers(source).build();

        source.put("k2", "v2");         // mutate the caller's map

        assertThat(message.headers()).containsExactly(Map.entry("k", "v"));
        assertThatThrownBy(() -> message.headers().put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void GIVEN_two_messages_with_equal_payload_arrays_WHEN_compared_THEN_they_are_equal() {
        OutboxMessage a = validBuilder().payload(new byte[] {9, 8, 7}).build();
        OutboxMessage b = validBuilder().payload(new byte[] {9, 8, 7}).build();

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
    }

    @Test
    void GIVEN_two_messages_differing_only_in_payload_WHEN_compared_THEN_they_are_not_equal() {
        OutboxMessage a = validBuilder().payload(new byte[] {1}).build();
        OutboxMessage b = validBuilder().payload(new byte[] {2}).build();

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void GIVEN_a_sensitive_payload_and_header_value_WHEN_toString_THEN_neither_appears_but_structural_fields_do() {
        String piiInPayload = "email:jane@example.com";
        String secretHeaderValue = "Bearer super-secret-token";
        OutboxMessage message = validBuilder()
                .payload(piiInPayload.getBytes(StandardCharsets.UTF_8))
                .header("authorization", secretHeaderValue)
                .build();

        String rendered = message.toString();

        assertThat(rendered)
                .doesNotContain(piiInPayload)
                .doesNotContain(secretHeaderValue)
                .contains("payloadBytes=" + piiInPayload.getBytes(StandardCharsets.UTF_8).length)
                .contains("headerNames=")
                .contains("authorization");
    }
}
