package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.junit.jupiter.api.Test;

class ReplayCriteriaTest {

    @Test
    void GIVEN_no_selector_WHEN_constructed_THEN_it_is_rejected() {
        assertThatThrownBy(() -> new ReplayCriteria(null, null, null, null, Set.of(), false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_a_null_status_set_WHEN_constructed_with_another_selector_THEN_it_defaults_to_empty() {
        ReplayCriteria criteria = new ReplayCriteria(null, "Order", null, null, null, true);

        assertThat(criteria.statuses()).isEmpty();
        assertThat(criteria.dryRun()).isTrue();
    }

    @Test
    void GIVEN_only_a_status_selector_WHEN_constructed_THEN_it_is_accepted() {
        ReplayCriteria criteria =
                new ReplayCriteria(null, null, null, null, Set.of(OutboxStatus.FAILED), false);

        assertThat(criteria.statuses()).containsExactly(OutboxStatus.FAILED);
    }
}
