package com.codingful.tandem.micrometer;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

class MicrometerTandemMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MicrometerTandemMetrics metrics = new MicrometerTandemMetrics(registry);

    @Test
    void GIVEN_a_backlog_reading_reported_twice_WHEN_read_THEN_the_same_gauge_reflects_the_latest_value() {
        metrics.recordLag(5);
        metrics.recordLagAgeSeconds(1.5);

        metrics.recordLag(2);
        metrics.recordLagAgeSeconds(0.25);

        // Exactly one series per name: a Gauge is registered once, at construction, over a mutable
        // holder — a second recordLag call must move that same holder, never register a duplicate.
        assertThat(registry.find("tandem.outbox.lag.count").gauges()).hasSize(1);
        assertThat(registry.get("tandem.outbox.lag.count").gauge().value()).isEqualTo(2);
        assertThat(registry.get("tandem.outbox.lag.age_seconds").gauge().value()).isEqualTo(0.25);
    }

    @Test
    void GIVEN_the_failed_count_drops_WHEN_read_again_THEN_the_gauge_follows_it_down() {
        // The exact property a tally-of-events implementation cannot have (the bug this adapter must
        // not repeat): once a stuck row is resolved, the count an operator sees must fall, not just
        // ever climb.
        metrics.recordFailed(3);
        assertThat(registry.get("tandem.outbox.failed.count").gauge().value()).isEqualTo(3);

        metrics.recordFailed(0);
        assertThat(registry.get("tandem.outbox.failed.count").gauge().value()).isEqualTo(0);
    }

    @Test
    void GIVEN_events_published_across_several_calls_WHEN_read_THEN_the_counter_accumulates() {
        // Distinguishes a Counter from every gauge above: publishing is an event tally, not a live
        // reading, so successive calls must sum rather than overwrite.
        metrics.incrementPublished(4);
        metrics.incrementPublished(3);

        assertThat(registry.get("tandem.outbox.published").counter().count()).isEqualTo(7);
    }

    @Test
    void GIVEN_retriable_failures_and_lease_expiries_WHEN_read_THEN_both_counters_are_wired_independently() {
        metrics.incrementRetry();
        metrics.incrementRetry();
        metrics.incrementLeaseExpired(5);

        assertThat(registry.get("tandem.outbox.retry.count").counter().count()).isEqualTo(2);
        assertThat(registry.get("tandem.outbox.lease_expired.count").counter().count()).isEqualTo(5);
    }

    @Test
    void GIVEN_active_workers_and_uncovered_buckets_reported_WHEN_read_THEN_each_gauge_reflects_its_own_value() {
        metrics.recordActiveWorkers(8);
        metrics.recordUncoveredBuckets(3);

        assertThat(registry.get("tandem.outbox.workers.active").gauge().value()).isEqualTo(8);
        assertThat(registry.get("tandem.outbox.bucket.uncovered").gauge().value()).isEqualTo(3);
    }

    @Test
    void GIVEN_the_same_check_reported_twice_WHEN_read_THEN_only_one_series_is_registered() {
        metrics.recordConfigInvalid("row_lease_unsafe");
        metrics.recordConfigInvalid("row_lease_unsafe");

        assertThat(registry.find("tandem.relay.config.invalid").gauges()).hasSize(1);
        assertThat(registry.get("tandem.relay.config.invalid").tag("check", "row_lease_unsafe").gauge().value())
                .isEqualTo(1);
    }

    @Test
    void GIVEN_two_distinct_checks_fail_WHEN_read_THEN_each_gets_its_own_tagged_series() {
        metrics.recordConfigInvalid("row_lease_unsafe");
        metrics.recordConfigInvalid("bucket_lease_not_seeded");

        assertThat(registry.find("tandem.relay.config.invalid").gauges()).hasSize(2);
        assertThat(registry.get("tandem.relay.config.invalid").tag("check", "row_lease_unsafe").gauge().value())
                .isEqualTo(1);
        assertThat(registry.get("tandem.relay.config.invalid").tag("check", "bucket_lease_not_seeded").gauge().value())
                .isEqualTo(1);
    }

    @Test
    void GIVEN_the_adapter_WHEN_asked_if_enabled_THEN_it_is() {
        assertThat(metrics.isEnabled()).isTrue();
    }
}
