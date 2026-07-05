package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import java.lang.System.Logger;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RelayConfigTest {

    private static final Logger LOG = System.getLogger(RelayConfigTest.class.getName());

    @Test
    void GIVEN_the_basic_round_defaults_WHEN_read_THEN_they_match_the_specified_values() {
        RelayConfig cfg = RelayConfig.defaults();

        assertThat(cfg.bucketCount()).isEqualTo(256);
        assertThat(cfg.batchSize()).isEqualTo(100);
        assertThat(cfg.pollInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(cfg.rowLease()).isEqualTo(Duration.ofSeconds(60));
        assertThat(cfg.maxAttempts()).isEqualTo(10);
        assertThat(cfg.retention()).isEqualTo(Duration.ofDays(14));
    }

    @Test
    void GIVEN_the_defaults_WHEN_read_THEN_coordination_is_single_with_a_derived_instance_id() {
        RelayConfig cfg = RelayConfig.defaults();

        assertThat(cfg.coordination()).isEqualTo(Coordination.SINGLE);
        assertThat(cfg.bucketLease()).isEqualTo(Duration.ofSeconds(30));
        // Derived when unset — never null, and within the tandem_bucket_lease.owner length.
        assertThat(cfg.instanceId()).isNotBlank().hasSizeLessThanOrEqualTo(64);
    }

    @Test
    void GIVEN_an_explicit_instance_id_WHEN_built_THEN_it_is_used_verbatim() {
        RelayConfig cfg = RelayConfig.builder().instanceId("relay-a").build();

        assertThat(cfg.instanceId()).isEqualTo("relay-a");
    }

    @Test
    void GIVEN_an_instance_id_over_the_owner_length_WHEN_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> RelayConfig.builder().instanceId("x".repeat(65)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("64");
    }

    @Test
    void GIVEN_a_bucket_count_over_the_smallint_bound_WHEN_built_THEN_it_is_rejected() {
        // The bucket column is SMALLINT: a larger count would only fail later, at insert, with a
        // cryptic Postgres out-of-range error.
        assertThatCode(() -> RelayConfig.builder().bucketCount(RelayConfig.MAX_BUCKET_COUNT))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> RelayConfig.builder().bucketCount(RelayConfig.MAX_BUCKET_COUNT + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SMALLINT");
    }

    @Test
    void GIVEN_the_derivation_WHEN_invoked_THEN_it_stays_within_the_owner_length() {
        assertThat(RelayConfig.deriveInstanceId()).isNotBlank().hasSizeLessThanOrEqualTo(64);
    }

    @Test
    void GIVEN_a_row_lease_above_the_delivery_timeout_WHEN_validated_THEN_it_passes_silently() {
        RelayConfig cfg = RelayConfig.builder()
                .rowLease(Duration.ofSeconds(60)).deliveryTimeoutMs(30_000).build();
        RecordingMetrics metrics = new RecordingMetrics();

        assertThatCode(() -> cfg.checkRowLeaseSafe(metrics, LOG)).doesNotThrowAnyException();
        assertThat(metrics.configInvalidChecks()).isEmpty();
    }

    @Test
    void GIVEN_a_row_lease_not_above_the_delivery_timeout_WHEN_validated_THEN_it_fails_fast_and_records_the_invalid_config() {
        RelayConfig cfg = RelayConfig.builder()
                .rowLease(Duration.ofSeconds(20)).deliveryTimeoutMs(30_000).build();
        RecordingMetrics metrics = new RecordingMetrics();

        assertThatThrownBy(() -> cfg.checkRowLeaseSafe(metrics, LOG))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining("rowLease")
                .hasMessageContaining("delivery.timeout.ms")
                .hasMessageContaining("20000")
                .hasMessageContaining("30000");
        assertThat(metrics.configInvalidChecks()).containsExactly(RelayConfig.CHECK_ROW_LEASE);
    }

    @Test
    void GIVEN_a_row_lease_equal_to_the_delivery_timeout_WHEN_validated_THEN_it_still_fails_fast() {
        RelayConfig cfg = RelayConfig.builder()
                .rowLease(Duration.ofMillis(30_000)).deliveryTimeoutMs(30_000).build();

        assertThatThrownBy(() -> cfg.checkRowLeaseSafe(new RecordingMetrics(), LOG))
                .isInstanceOf(TandemConfigurationException.class);
    }

    @Test
    void GIVEN_all_builder_fields_set_WHEN_read_THEN_every_accessor_returns_the_configured_value() {
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(128)
                .coordination(Coordination.LEASE)
                .instanceId("relay-1")
                .bucketLease(Duration.ofSeconds(45))
                .workersPerInstance(2)
                .batchSize(50)
                .pollInterval(Duration.ofMillis(200))
                .rowLease(Duration.ofSeconds(90))
                .maxAttempts(5)
                .retention(Duration.ofDays(7))
                .cleanupBatchSize(500)
                .reclaimInterval(Duration.ofSeconds(10))
                .cleanupInterval(Duration.ofMinutes(30))
                .deliveryTimeoutMs(15_000)
                .build();

        assertThat(cfg.bucketCount()).isEqualTo(128);
        assertThat(cfg.coordination()).isEqualTo(Coordination.LEASE);
        assertThat(cfg.instanceId()).isEqualTo("relay-1");
        assertThat(cfg.bucketLease()).isEqualTo(Duration.ofSeconds(45));
        assertThat(cfg.workersPerInstance()).isEqualTo(2);
        assertThat(cfg.batchSize()).isEqualTo(50);
        assertThat(cfg.pollInterval()).isEqualTo(Duration.ofMillis(200));
        assertThat(cfg.rowLease()).isEqualTo(Duration.ofSeconds(90));
        assertThat(cfg.maxAttempts()).isEqualTo(5);
        assertThat(cfg.retention()).isEqualTo(Duration.ofDays(7));
        assertThat(cfg.cleanupBatchSize()).isEqualTo(500);
        assertThat(cfg.reclaimInterval()).isEqualTo(Duration.ofSeconds(10));
        assertThat(cfg.cleanupInterval()).isEqualTo(Duration.ofMinutes(30));
        assertThat(cfg.deliveryTimeoutMs()).isEqualTo(15_000);
    }
}
