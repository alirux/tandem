package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class OutboxStatusTest {

    @ParameterizedTest
    @EnumSource(OutboxStatus.class)
    void GIVEN_any_status_WHEN_its_stored_code_round_trips_THEN_the_same_status_returns(OutboxStatus status) {
        assertThat(OutboxStatus.fromCode(status.code())).isEqualTo(status);
    }

    @Test
    void GIVEN_the_persisted_codes_WHEN_read_THEN_they_match_the_stored_smallint_contract() {
        // The numeric codes are the on-disk contract (tandem_outbox.status) — pin them.
        assertThat(OutboxStatus.PENDING.code()).isZero();
        assertThat(OutboxStatus.IN_FLIGHT.code()).isEqualTo(1);
        assertThat(OutboxStatus.DONE.code()).isEqualTo(2);
        assertThat(OutboxStatus.FAILED.code()).isEqualTo(3);
        assertThat(OutboxStatus.DISCARDED.code()).isEqualTo(4);
    }

    @Test
    void GIVEN_an_unknown_stored_code_WHEN_decoded_THEN_it_is_rejected() {
        assertThatThrownBy(() -> OutboxStatus.fromCode(99))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
