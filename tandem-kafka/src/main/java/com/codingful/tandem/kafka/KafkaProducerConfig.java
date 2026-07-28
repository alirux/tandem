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

    /**
     * Tandem's default {@code delivery.timeout.ms} — deliberately below Kafka's own 2-minute default, so
     * that it stays under the 60 s default {@code rowLease} the invariant of LLD-jdbc §3.5 requires.
     */
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
        config.putIfAbsent(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) fillInDeliveryTimeoutMs(config));

        // The CloudEvents binary binding produces String keys and byte[] values — fixed, not user-tunable.
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return config;
    }

    /**
     * The {@code delivery.timeout.ms} to fill in when the user set none. Kafka rejects a producer whose
     * {@code delivery.timeout.ms < linger.ms + request.timeout.ms}, and Tandem's 30 s default is only just
     * above that floor: filling it in blindly breaks producer construction with a raw Kafka error as soon
     * as anything pushes the floor up — an ordinary {@code linger.ms}, or simply a newer client (Kafka 4
     * moved the default {@code linger.ms} from 0 to 5 ms, which is enough on its own). So the fill-in
     * grows to whatever the floor requires, and the relay's {@code rowLease > delivery.timeout.ms} check
     * then runs against that real value — a producer that outgrows the row lease fails loudly, with the
     * diagnostic that names the fix.
     *
     * <p>The floor is computed from <b>Kafka's own declared defaults</b> rather than copies of them:
     * hardcoding "linger defaults to 0" is exactly what made this break on a client upgrade.
     */
    private static long fillInDeliveryTimeoutMs(Map<String, ?> config) {
        Map<String, Object> kafkaDefaults = ProducerConfig.configDef().defaultValues();
        long linger = effective(config, kafkaDefaults, ProducerConfig.LINGER_MS_CONFIG);
        long requestTimeout = effective(config, kafkaDefaults, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        return Math.max(DEFAULT_DELIVERY_TIMEOUT_MS, linger + requestTimeout);
    }

    /** The user's value for {@code key} if set, else the one Kafka's own config definition declares. */
    private static long effective(Map<String, ?> config, Map<String, Object> kafkaDefaults, String key) {
        Object value = config.get(key);
        return parseInt(value != null ? value : kafkaDefaults.get(key));
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
