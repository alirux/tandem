package com.codingful.tandem.kafka;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Hardens the user's Kafka producer config to the mandated safe values (LLD-kafka §1). These settings
 * protect the no-loss + ordering guarantees, so Tandem <b>fails fast</b> ({@link
 * TandemConfigurationException}) if the user overrides them to unsafe values, and fills safe defaults
 * otherwise. The {@code byte[]} value / {@code String} key serializers are fixed by the CloudEvents
 * binary binding and always forced.
 */
final class KafkaProducerConfig {

    /** Max in-flight requests allowed with idempotence (LLD-kafka §1). */
    static final int MAX_IN_FLIGHT_LIMIT = 5;

    /** Kafka producer default for {@code delivery.timeout.ms} (also Tandem's default). */
    static final long DEFAULT_DELIVERY_TIMEOUT_MS = 30_000;

    private static final Set<String> SAFE_ACKS = Set.of("all", "-1");

    private KafkaProducerConfig() {
    }

    /** Validate the unsafe-override invariants, fill safe defaults, and force the binding serializers. */
    static Map<String, Object> harden(Map<String, ?> userConfig) {
        Map<String, Object> config = new HashMap<>(userConfig);

        if (isFalse(config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))) {
            throw unsafe(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG),
                    "must stay true — idempotence prevents duplicate/reordered batches from the producer's own retries");
        }
        Object acks = config.get(ProducerConfig.ACKS_CONFIG);
        if (acks != null && !SAFE_ACKS.contains(acks.toString().trim().toLowerCase())) {
            throw unsafe(ProducerConfig.ACKS_CONFIG, acks,
                    "must be 'all' (or -1) — acks=0/1 risk acknowledging before a durable replica write, losing events");
        }
        Object maxInFlight = config.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        if (maxInFlight != null && parseInt(maxInFlight) > MAX_IN_FLIGHT_LIMIT) {
            throw unsafe(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlight,
                    "must be <= " + MAX_IN_FLIGHT_LIMIT + " — required for idempotent ordering");
        }

        config.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        config.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        config.putIfAbsent(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) DEFAULT_DELIVERY_TIMEOUT_MS);

        // The CloudEvents binary binding produces String keys and byte[] values — fixed, not user-tunable.
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return config;
    }

    /** The effective {@code delivery.timeout.ms} after hardening — the relay reads it for the rowLease invariant (LLD-jdbc §3.5). */
    static long deliveryTimeoutMs(Map<String, ?> config) {
        Object value = config.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        return value == null ? DEFAULT_DELIVERY_TIMEOUT_MS : parseInt(value);
    }

    private static boolean isFalse(Object value) {
        return value != null && "false".equalsIgnoreCase(value.toString().trim());
    }

    private static int parseInt(Object value) {
        if (value instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(value.toString().trim());
        } catch (NumberFormatException e) {
            throw new TandemConfigurationException("Unsafe Kafka producer config: expected an integer but got '" + value + "'");
        }
    }

    private static TandemConfigurationException unsafe(String property, Object value, String why) {
        return new TandemConfigurationException(
                "Unsafe Kafka producer config: `" + property + "` = " + value + " — " + why
                        + " (LLD-kafka §1). Remove the override or set a safe value.");
    }
}
