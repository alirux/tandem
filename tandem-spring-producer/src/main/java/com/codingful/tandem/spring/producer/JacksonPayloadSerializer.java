package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.exception.PayloadSerializationException;
import com.codingful.tandem.core.port.PayloadSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;

/**
 * Jackson-backed {@link PayloadSerializer} — the optional object-payload path for the write-side tiers
 * (LLD-spring-producer §2). It is auto-configured only when Jackson is on the consumer's classpath and
 * is never forced, so the {@code byte[]} path stays dependency-free; it reuses the application's own
 * {@code ObjectMapper} when one exists.
 */
public final class JacksonPayloadSerializer implements PayloadSerializer {

    private static final String CONTENT_TYPE = "application/json";

    private final ObjectMapper objectMapper;

    /**
     * @param objectMapper the mapper to serialize with — typically the application's shared bean
     * @throws NullPointerException if {@code objectMapper} is null
     */
    public JacksonPayloadSerializer(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    @Override
    public byte[] serialize(Object payload) {
        try {
            return objectMapper.writeValueAsBytes(payload);
        } catch (JsonProcessingException e) {
            throw new PayloadSerializationException("Failed to serialize payload to JSON", e);
        }
    }

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }
}
