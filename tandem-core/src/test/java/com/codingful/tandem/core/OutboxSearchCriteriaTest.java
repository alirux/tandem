package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class OutboxSearchCriteriaTest {

    @Test
    void GIVEN_no_selector_at_all_WHEN_built_THEN_it_is_accepted() {
        OutboxSearchCriteria criteria = OutboxSearchCriteria.builder().build();

        assertThat(criteria.status()).isNull();
        assertThat(criteria.aggregateId()).isNull();
        assertThat(criteria.limit()).isEqualTo(OutboxSearchCriteria.DEFAULT_LIMIT);
    }

    @Test
    void GIVEN_a_limit_below_the_minimum_WHEN_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> OutboxSearchCriteria.builder().limit(0).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_a_limit_above_the_maximum_WHEN_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> OutboxSearchCriteria.builder().limit(501).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_the_boundary_limits_WHEN_built_THEN_they_are_accepted() {
        assertThat(OutboxSearchCriteria.builder().limit(1).build().limit()).isEqualTo(1);
        assertThat(OutboxSearchCriteria.builder().limit(500).build().limit()).isEqualTo(500);
    }

    @Test
    void GIVEN_every_filter_set_WHEN_built_THEN_the_values_round_trip() {
        AggregateId aggregateId = AggregateId.of("order-1");
        java.time.Instant from = java.time.Instant.parse("2026-08-01T00:00:00Z");
        java.time.Instant to = java.time.Instant.parse("2026-08-02T00:00:00Z");

        OutboxSearchCriteria criteria = OutboxSearchCriteria.builder()
                .status(OutboxStatus.FAILED)
                .aggregateId(aggregateId)
                .aggregateType("Order")
                .type("com.acme.order.placed")
                .createdFrom(from)
                .createdTo(to)
                .afterId(42L)
                .limit(100)
                .build();

        assertThat(criteria.status()).isEqualTo(OutboxStatus.FAILED);
        assertThat(criteria.aggregateId()).isEqualTo(aggregateId);
        assertThat(criteria.aggregateType()).isEqualTo("Order");
        assertThat(criteria.type()).isEqualTo("com.acme.order.placed");
        assertThat(criteria.createdFrom()).isEqualTo(from);
        assertThat(criteria.createdTo()).isEqualTo(to);
        assertThat(criteria.afterId()).isEqualTo(42L);
        assertThat(criteria.limit()).isEqualTo(100);
    }
}
