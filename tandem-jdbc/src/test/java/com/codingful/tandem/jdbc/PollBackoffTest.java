package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PollBackoffTest {

    private static final Duration POLL = Duration.ofMillis(100);
    private static final Duration CAP = Duration.ofSeconds(5);
    private static final int SAMPLES = 200;

    @Test
    void GIVEN_an_idle_outbox_WHEN_a_worker_waits_between_empty_claims_THEN_it_waits_about_the_configured_interval() {
        PollBackoff backoff = new PollBackoff(POLL, CAP);

        for (int i = 0; i < SAMPLES; i++) {
            assertThat(backoff.idleSleepMillis()).isBetween(80L, 120L);
        }
    }

    @Test
    void GIVEN_several_idle_workers_WHEN_they_wait_between_empty_claims_THEN_they_do_not_all_wait_the_same_time() {
        // The point of the jitter: workers started in the same instant must not keep polling in
        // lockstep for the lifetime of the relay. A fixed sleep would make this set a singleton.
        PollBackoff backoff = new PollBackoff(POLL, CAP);

        Set<Long> distinctWaits = new HashSet<>();
        for (int i = 0; i < SAMPLES; i++) {
            distinctWaits.add(backoff.idleSleepMillis());
        }

        assertThat(distinctWaits).hasSizeGreaterThan(1);
    }

    @Test
    void GIVEN_a_worker_failing_every_cycle_WHEN_it_keeps_failing_THEN_each_wait_is_longer_than_the_last() {
        PollBackoff backoff = new PollBackoff(POLL, CAP);

        List<Long> waits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            waits.add(backoff.failedCycleSleepMillis());
        }

        assertThat(waits.get(0)).isBetween(80L, 120L);   // the first failure waits the poll interval
        assertThat(waits).isSorted();                    // ±20% jitter stays under the doubling factor
        assertThat(waits).doesNotHaveDuplicates();
    }

    @Test
    void GIVEN_a_database_that_stays_down_WHEN_a_worker_has_failed_many_times_THEN_the_wait_stops_at_the_cap() {
        PollBackoff backoff = new PollBackoff(POLL, CAP);
        for (int i = 0; i < 20; i++) {
            backoff.failedCycleSleepMillis();   // saturate
        }

        List<Long> saturated = new ArrayList<>();
        for (int i = 0; i < SAMPLES; i++) {
            saturated.add(backoff.failedCycleSleepMillis());
        }

        // The cap is a hard ceiling — jitter is applied before the clamp, so a saturated worker waits
        // within 20% *below* it and never past it.
        assertThat(saturated).allSatisfy(wait -> assertThat(wait).isBetween(4_000L, CAP.toMillis()));
        assertThat(Collections.max(saturated)).isEqualTo(CAP.toMillis());
    }

    @Test
    void GIVEN_a_worker_that_had_been_failing_WHEN_one_cycle_completes_THEN_the_next_failure_waits_from_the_floor_again() {
        // Recovery must be as fast as the first failure was: a transient blip that resolves must not
        // leave the worker sleeping at the cap the next time it stumbles.
        PollBackoff backoff = new PollBackoff(POLL, CAP);
        for (int i = 0; i < 10; i++) {
            backoff.failedCycleSleepMillis();
        }

        backoff.onCycleCompleted();

        assertThat(backoff.failedCycleSleepMillis()).isBetween(80L, 120L);
    }

    @Test
    void GIVEN_a_cap_below_the_poll_interval_WHEN_a_worker_fails_THEN_it_still_waits_the_poll_interval() {
        // reclaimInterval can legitimately be configured shorter than pollInterval; a failure backoff
        // shorter than the idle one would retry a broken database faster than a healthy idle poll.
        PollBackoff backoff = new PollBackoff(POLL, Duration.ofMillis(10));

        for (int i = 0; i < 5; i++) {
            assertThat(backoff.failedCycleSleepMillis()).isBetween(80L, 120L);
        }
    }

    @Test
    void GIVEN_a_sub_millisecond_poll_interval_WHEN_a_worker_waits_THEN_it_still_yields_rather_than_spinning() {
        PollBackoff backoff = new PollBackoff(Duration.ZERO, Duration.ZERO);

        assertThat(backoff.idleSleepMillis()).isEqualTo(1);
        assertThat(backoff.failedCycleSleepMillis()).isEqualTo(1);
    }
}
