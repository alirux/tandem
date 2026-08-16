package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OutboxRowDetailTest {

    private static OutboxRowView view() {
        return new OutboxRowView(
                1L, AggregateId.of("order-1"), "Order", "com.acme.order.placed", 1,
                OutboxStatus.DONE, 1, 0, null, null, null, null, null,
                Instant.parse("2026-08-01T00:00:00Z"), null);
    }

    @Test
    void GIVEN_a_payload_array_WHEN_mutated_after_construction_THEN_the_stored_payload_is_unaffected() {
        byte[] source = {1, 2, 3};
        OutboxRowDetail detail = new OutboxRowDetail(view(), source, Map.of());

        source[0] = 99;
        detail.payload()[1] = 99;

        assertThat(detail.payload()).containsExactly(1, 2, 3);
    }

    @Test
    void GIVEN_two_details_with_equal_payload_arrays_WHEN_compared_THEN_they_are_equal() {
        OutboxRowDetail a = new OutboxRowDetail(view(), new byte[] {9, 8, 7}, Map.of());
        OutboxRowDetail b = new OutboxRowDetail(view(), new byte[] {9, 8, 7}, Map.of());

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
    }

    @Test
    void GIVEN_two_details_differing_only_in_payload_WHEN_compared_THEN_they_are_not_equal() {
        OutboxRowDetail a = new OutboxRowDetail(view(), new byte[] {1}, Map.of());
        OutboxRowDetail b = new OutboxRowDetail(view(), new byte[] {2}, Map.of());

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void GIVEN_two_details_differing_only_in_headers_WHEN_compared_THEN_they_are_not_equal() {
        OutboxRowDetail a = new OutboxRowDetail(view(), new byte[] {1}, Map.of("k", "v1"));
        OutboxRowDetail b = new OutboxRowDetail(view(), new byte[] {1}, Map.of("k", "v2"));

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void GIVEN_the_same_instance_WHEN_compared_to_itself_THEN_it_is_equal() {
        OutboxRowDetail detail = new OutboxRowDetail(view(), new byte[] {1}, Map.of());

        assertThat(detail.equals(detail)).isTrue();
    }

    @Test
    void GIVEN_a_value_of_a_different_type_WHEN_compared_THEN_it_is_not_equal() {
        OutboxRowDetail detail = new OutboxRowDetail(view(), new byte[] {1}, Map.of());

        assertThat(detail.equals("not a detail")).isFalse();
    }

    @Test
    void GIVEN_a_sensitive_payload_and_header_value_WHEN_toString_THEN_neither_appears_but_structural_fields_do() {
        String piiInPayload = "email:jane@example.com";
        String secretHeaderValue = "Bearer super-secret-token";
        OutboxRowDetail detail = new OutboxRowDetail(
                view(), piiInPayload.getBytes(StandardCharsets.UTF_8), Map.of("authorization", secretHeaderValue));

        String rendered = detail.toString();

        assertThat(rendered)
                .doesNotContain(piiInPayload)
                .doesNotContain(secretHeaderValue)
                .contains("payloadBytes=" + piiInPayload.getBytes(StandardCharsets.UTF_8).length)
                .contains("headerNames=")
                .contains("authorization");
    }

    @Test
    void GIVEN_null_headers_WHEN_constructed_THEN_it_defaults_to_empty() {
        OutboxRowDetail detail = new OutboxRowDetail(view(), new byte[0], null);

        assertThat(detail.headers()).isEmpty();
    }
}
