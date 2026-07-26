package com.codingful.tandem.spring.relay;

import java.net.URI;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Binds {@code tandem.kafka.*} — the CloudEvents binding and the Kafka producer settings
 * (LLD-spring-config §2.3). The {@code producer} map is a raw passthrough to the Kafka producer; the
 * relay hardens the unsafe overrides (idempotence, {@code acks}, in-flight limit) and forces the
 * serializers the CloudEvents binding needs, so {@code bootstrap.servers} and any tuning go here.
 *
 * @param source             CloudEvents {@code source} URI — <b>required</b>; startup fails if absent
 * @param defaultContentType {@code datacontenttype} when a row stored none; default {@code application/json}
 * @param defaultDataSchema  optional default {@code dataschema} URI; unset → omitted from the envelope
 * @param topicSuffix        appended to the kebab-cased aggregate type for the topic; default {@code -topic}
 * @param producer           raw Kafka producer properties (e.g. {@code bootstrap.servers}); default empty
 */
@ConfigurationProperties("tandem.kafka")
public record TandemKafkaProperties(
        URI source,
        @DefaultValue("application/json") String defaultContentType,
        URI defaultDataSchema,
        @DefaultValue("-topic") String topicSuffix,
        Map<String, String> producer) {

    /** Guarantee a non-null producer map, so an application that sets no {@code tandem.kafka.producer.*}
     *  keys yields an empty map rather than {@code null}. */
    public TandemKafkaProperties {
        producer = producer == null ? Map.of() : producer;
    }
}
