package com.codingful.tandem.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class KafkaProducerConfigTest {

    @Test
    void GIVEN_an_empty_config_WHEN_hardened_THEN_the_safe_defaults_and_binding_serializers_are_applied() {
        Map<String, Object> hardened = KafkaProducerConfig.harden(Map.of());

        assertThat(hardened.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo(true);
        assertThat(hardened.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
        assertThat(hardened.get(ProducerConfig.RETRIES_CONFIG)).isEqualTo(Integer.MAX_VALUE);
        assertThat(hardened.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class.getName());
        assertThat(hardened.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(ByteArraySerializer.class.getName());
    }

    @Test
    void GIVEN_idempotence_disabled_WHEN_hardened_THEN_it_fails_fast() {
        assertThatThrownBy(() -> KafkaProducerConfig.harden(Map.of(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
    }

    @Test
    void GIVEN_acks_weaker_than_all_WHEN_hardened_THEN_it_fails_fast() {
        assertThatThrownBy(() -> KafkaProducerConfig.harden(Map.of(ProducerConfig.ACKS_CONFIG, "1")))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining(ProducerConfig.ACKS_CONFIG);
        assertThatThrownBy(() -> KafkaProducerConfig.harden(Map.of(ProducerConfig.ACKS_CONFIG, "0")))
                .isInstanceOf(TandemConfigurationException.class);
    }

    @Test
    void GIVEN_acks_minus_one_WHEN_hardened_THEN_it_is_accepted_as_equivalent_to_all() {
        Map<String, Object> hardened = KafkaProducerConfig.harden(Map.of(ProducerConfig.ACKS_CONFIG, "-1"));

        assertThat(hardened.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("-1");
    }

    @Test
    void GIVEN_too_many_in_flight_requests_WHEN_hardened_THEN_it_fails_fast() {
        assertThatThrownBy(() -> KafkaProducerConfig.harden(
                Map.of(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 6)))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
    }

    @Test
    void GIVEN_no_delivery_timeout_WHEN_read_THEN_it_reports_the_default_otherwise_the_override() {
        assertThat(KafkaProducerConfig.deliveryTimeoutMs(Map.of()))
                .isEqualTo(KafkaProducerConfig.DEFAULT_DELIVERY_TIMEOUT_MS);
        assertThat(KafkaProducerConfig.deliveryTimeoutMs(Map.of(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 45_000)))
                .isEqualTo(45_000);
    }
}
