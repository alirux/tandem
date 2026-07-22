package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.BucketCountReconciliation.Decision;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class BucketCountReconciliationTest {

    private final BucketCountReconciliation policy = BucketCountReconciliation.seedOrValidate();

    @Test
    void GIVEN_no_stored_value_WHEN_reconciled_THEN_it_decides_to_seed() {
        Decision decision = policy.decide(OptionalInt.empty(), 256);

        assertThat(decision.kind()).isEqualTo(Decision.Kind.SEED);
        assertThat(decision.message()).isNull();
    }

    @Test
    void GIVEN_a_stored_value_equal_to_configured_WHEN_reconciled_THEN_it_decides_to_proceed() {
        Decision decision = policy.decide(OptionalInt.of(256), 256);

        assertThat(decision.kind()).isEqualTo(Decision.Kind.PROCEED);
        assertThat(decision.message()).isNull();
    }

    @Test
    void GIVEN_a_stored_value_different_from_configured_WHEN_reconciled_THEN_it_conflicts_naming_both_values() {
        Decision decision = policy.decide(OptionalInt.of(256), 512);

        assertThat(decision.kind()).isEqualTo(Decision.Kind.CONFLICT);
        assertThat(decision.message())
                .contains("512")   // the misconfigured (configured) value
                .contains("256");  // the established (stored) value
    }

    @Test
    void GIVEN_a_non_positive_configured_value_WHEN_reconciled_THEN_it_is_rejected() {
        assertThatThrownBy(() -> policy.decide(OptionalInt.empty(), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void GIVEN_the_decision_is_pure_WHEN_called_twice_with_the_same_inputs_THEN_the_kind_is_stable() {
        assertThat(policy.decide(OptionalInt.of(128), 64).kind())
                .isEqualTo(policy.decide(OptionalInt.of(128), 64).kind())
                .isEqualTo(Decision.Kind.CONFLICT);
    }
}
