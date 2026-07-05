package com.codingful.tandem.kafka;

import java.net.URI;
import java.util.Objects;

/**
 * Tandem-specific settings for the CloudEvents binding (LLD-kafka §3/§6), separate from the raw Kafka
 * producer properties.
 *
 * @param source           the single configured CloudEvents {@code source} URI ({@code tandem.kafka.source})
 * @param defaultContentType {@code datacontenttype} when a row stored none ({@code tandem.kafka.default-content-type}, default {@code application/json})
 * @param defaultDataSchema  optional default {@code dataschema} URI; {@code null} to omit
 */
public record KafkaRelayConfig(URI source, String defaultContentType, URI defaultDataSchema) {

    public KafkaRelayConfig {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(defaultContentType, "defaultContentType");
    }

    /** With the default content type {@code application/json} and no default data schema. */
    public static KafkaRelayConfig of(String source) {
        return new KafkaRelayConfig(URI.create(source), "application/json", null);
    }
}
