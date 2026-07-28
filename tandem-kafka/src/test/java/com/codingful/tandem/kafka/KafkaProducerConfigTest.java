package com.codingful.tandem.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
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

    /**
     * The config Tandem hardens with no user tuning at all must build a producer on whatever client
     * version is on the classpath — the case that broke when Kafka 4 moved the default {@code linger.ms}
     * off zero and pushed the {@code delivery.timeout.ms} floor above Tandem's 30 s default.
     */
    @Test
    void GIVEN_no_producer_tuning_WHEN_hardened_THEN_the_defaults_still_build_a_producer() {
        Map<String, Object> hardened = KafkaProducerConfig.harden(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(hardened)) {
            assertThat(producer).isNotNull();
        }
    }

    /**
     * Kafka rejects a producer whose {@code delivery.timeout.ms} is below {@code linger.ms +
     * request.timeout.ms}, so filling in Tandem's 30 s default blindly made an ordinary batching setting
     * fail producer construction. Building the real producer is the assertion that matters — the config
     * numbers alone would not catch the rule changing.
     */
    @Test
    void GIVEN_a_producer_tuned_to_batch_WHEN_it_is_hardened_THEN_the_delivery_timeout_makes_room_for_the_delay() {
        Map<String, Object> hardened = KafkaProducerConfig.harden(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.LINGER_MS_CONFIG, 17));

        assertThat(KafkaProducerConfig.deliveryTimeoutMs(hardened))
                .isEqualTo(KafkaProducerConfig.DEFAULT_DELIVERY_TIMEOUT_MS + 17);
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(hardened)) {
            assertThat(producer).isNotNull();
        }
    }

    /** Configuration bound from properties arrives as text, so the numeric settings must parse either way. */
    @Test
    void GIVEN_the_batching_delay_as_text_WHEN_hardened_THEN_it_sizes_the_delivery_timeout_the_same() {
        Map<String, Object> fromProperties = KafkaProducerConfig.harden(Map.of(ProducerConfig.LINGER_MS_CONFIG, "17"));
        Map<String, Object> fromCode = KafkaProducerConfig.harden(Map.of(ProducerConfig.LINGER_MS_CONFIG, 17));

        assertThat(KafkaProducerConfig.deliveryTimeoutMs(fromProperties))
                .isEqualTo(KafkaProducerConfig.deliveryTimeoutMs(fromCode));
    }

    @Test
    void GIVEN_a_raised_request_timeout_WHEN_hardened_THEN_the_delivery_timeout_grows_with_it() {
        Map<String, Object> hardened = KafkaProducerConfig.harden(
                Map.of(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 45_000));

        assertThat(KafkaProducerConfig.deliveryTimeoutMs(hardened)).isEqualTo(45_000);
    }

    @Test
    void GIVEN_a_batching_delay_that_is_not_a_number_WHEN_hardened_THEN_it_fails_fast() {
        assertThatThrownBy(() -> KafkaProducerConfig.harden(Map.of(ProducerConfig.LINGER_MS_CONFIG, "soon")))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining("soon");
    }

    @Test
    void GIVEN_an_explicit_delivery_timeout_WHEN_hardened_THEN_the_users_value_stands() {
        Map<String, Object> hardened = KafkaProducerConfig.harden(Map.of(
                ProducerConfig.LINGER_MS_CONFIG, 100,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000));

        assertThat(KafkaProducerConfig.deliveryTimeoutMs(hardened)).isEqualTo(120_000);
    }
}
