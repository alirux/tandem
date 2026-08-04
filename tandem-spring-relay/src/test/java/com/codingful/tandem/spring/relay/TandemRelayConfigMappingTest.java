package com.codingful.tandem.spring.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.jdbc.Coordination;
import com.codingful.tandem.jdbc.RelayConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class TandemRelayConfigMappingTest {

    private static final TandemOutboxProperties OUTBOX = new TandemOutboxProperties(256);

    private static TandemRelayProperties allUnset() {
        return new TandemRelayProperties(
                null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    @Test
    void GIVEN_no_relay_properties_are_set_WHEN_building_the_config_THEN_relayconfig_defaults_are_preserved() {
        RelayConfig config = TandemRelayAutoConfiguration.buildRelayConfig(OUTBOX, allUnset());
        RelayConfig defaults = RelayConfig.defaults();

        assertThat(config.coordination()).isEqualTo(defaults.coordination());
        assertThat(config.batchSize()).isEqualTo(defaults.batchSize());
        assertThat(config.pollInterval()).isEqualTo(defaults.pollInterval());
        assertThat(config.rowLease()).isEqualTo(defaults.rowLease());
        assertThat(config.maxAttempts()).isEqualTo(defaults.maxAttempts());
        assertThat(config.cleanupInterval()).isEqualTo(defaults.cleanupInterval());
        assertThat(config.metricsInterval()).isEqualTo(defaults.metricsInterval());
        assertThat(config.logEveryRows()).isEqualTo(defaults.logEveryRows());
        assertThat(config.bucketCount()).isEqualTo(256);
    }

    @Test
    void GIVEN_relay_properties_are_set_WHEN_building_the_config_THEN_they_override_the_defaults() {
        TandemRelayProperties relay = new TandemRelayProperties(
                Coordination.LEASE, "relay-1", Duration.ofSeconds(45), 8, Duration.ofMillis(250),
                50, Duration.ofSeconds(90), 5, Duration.ofDays(7), 500, Duration.ofSeconds(10),
                Duration.ofMinutes(30), Duration.ofSeconds(30), 5_000L);

        RelayConfig config = TandemRelayAutoConfiguration.buildRelayConfig(OUTBOX, relay);

        assertThat(config.coordination()).isEqualTo(Coordination.LEASE);
        assertThat(config.instanceId()).isEqualTo("relay-1");
        assertThat(config.bucketLease()).isEqualTo(Duration.ofSeconds(45));
        assertThat(config.workersPerInstance()).isEqualTo(8);
        assertThat(config.pollInterval()).isEqualTo(Duration.ofMillis(250));
        assertThat(config.batchSize()).isEqualTo(50);
        assertThat(config.rowLease()).isEqualTo(Duration.ofSeconds(90));
        assertThat(config.maxAttempts()).isEqualTo(5);
        assertThat(config.retention()).isEqualTo(Duration.ofDays(7));
        assertThat(config.cleanupBatchSize()).isEqualTo(500);
        assertThat(config.reclaimInterval()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config.cleanupInterval()).isEqualTo(Duration.ofMinutes(30));
        assertThat(config.metricsInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.logEveryRows()).isEqualTo(5_000L);
    }
}
