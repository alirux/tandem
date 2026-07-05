package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LamportClockTest {

    @Test
    void GIVEN_a_local_clock_ahead_of_the_inbound_WHEN_merged_THEN_it_advances_past_the_local() {
        assertThat(LamportClock.merge(10, 4)).isEqualTo(11);
    }

    @Test
    void GIVEN_an_inbound_clock_ahead_of_the_local_WHEN_merged_THEN_it_advances_past_the_inbound() {
        assertThat(LamportClock.merge(4, 10)).isEqualTo(11);
    }

    @Test
    void GIVEN_equal_clocks_WHEN_merged_THEN_it_advances_by_one() {
        assertThat(LamportClock.merge(7, 7)).isEqualTo(8);
    }
}
