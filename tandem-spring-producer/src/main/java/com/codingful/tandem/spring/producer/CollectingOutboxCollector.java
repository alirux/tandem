package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.exception.PayloadSerializationException;
import com.codingful.tandem.core.port.PayloadSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The default {@link OutboxCollector}: accumulates messages in call order for the template to insert.
 * Object payloads are serialized with the (optional) {@link PayloadSerializer}; when none is configured
 * an object payload fails fast (LLD-spring-producer §2), while the {@link #add(OutboxMessage)} path stays
 * available with no serializer at all.
 */
final class CollectingOutboxCollector implements OutboxCollector {

    private final PayloadSerializer payloadSerializer; // may be null — object payloads then fail fast
    private final List<OutboxMessage> messages = new ArrayList<>();

    CollectingOutboxCollector(PayloadSerializer payloadSerializer) {
        this.payloadSerializer = payloadSerializer;
    }

    @Override
    public void add(OutboxMessage message) {
        messages.add(Objects.requireNonNull(message, "message"));
    }

    @Override
    public void record(String aggregateType, AggregateId aggregateId, long seq, Object payload) {
        if (payloadSerializer == null) {
            throw new PayloadSerializationException(
                    "No PayloadSerializer configured — add a JSON library, supply a PayloadSerializer bean,"
                            + " or record a pre-built OutboxMessage via add(...)");
        }
        messages.add(OutboxMessage.builder()
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .seq(seq)
                .payload(payloadSerializer.serialize(payload))
                .contentType(payloadSerializer.contentType())
                .build());
    }

    @Override
    public void record(String aggregateType, String aggregateId, long seq, Object payload) {
        record(aggregateType, AggregateId.of(aggregateId), seq, payload);
    }

    List<OutboxMessage> collected() {
        return messages;
    }
}
