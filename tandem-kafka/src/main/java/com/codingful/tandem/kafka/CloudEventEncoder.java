package com.codingful.tandem.kafka;

import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.port.TopicRouter;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.kafka.KafkaMessageFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Builds the CloudEvent for a record and writes it as a Kafka {@link ProducerRecord} in <b>binary</b>
 * mode (LLD-kafka §3): attributes become {@code ce_*} headers, the payload becomes the body, and the
 * record key is the {@code aggregate_id} (so per-aggregate events share a partition).
 */
final class CloudEventEncoder {

    private final TopicRouter router;
    private final KafkaRelayConfig cfg;

    CloudEventEncoder(TopicRouter router, KafkaRelayConfig cfg) {
        this.router = Objects.requireNonNull(router, "router");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    ProducerRecord<String, byte[]> encode(OutboxRecord record) {
        String topic = router.topicFor(record);
        String key = record.aggregateId().value();

        CloudEventBuilder builder = CloudEventBuilder.v1()
                .withId(String.valueOf(record.id()))                 // id = outbox id (§6)
                .withSource(cfg.source())                            // single configured URI (§6)
                .withType(typeOf(record))                            // null `type` falls back to aggregate_type (Q20)
                .withSubject(key)                                    // = aggregate_id
                .withTime(record.createdAt().atOffset(ZoneOffset.UTC));

        String contentType = contentTypeOf(record);
        URI dataSchema = dataSchemaOf(record);
        if (dataSchema != null) {
            builder.withData(contentType, dataSchema, record.payload());
        } else {
            builder.withData(contentType, record.payload());
        }

        builder.withExtension(CloudEventsHeaders.EXT_SEQ, record.seq());            // always present → ce_seq
        builder.withExtension(CloudEventsHeaders.EXT_PARTITION_KEY, key);           // always = key → ce_partitionkey
        if (record.lamport() != null) {                                            // only when causal ordering on
            builder.withExtension(CloudEventsHeaders.EXT_LOGICAL_CLOCK, record.lamport());
        }

        CloudEvent event = builder.build();
        ProducerRecord<String, byte[]> producerRecord =
                KafkaMessageFactory.<String>createWriter(topic, key).writeBinary(event);

        // Pass the stored headers through as Kafka headers (e.g. correlation-id), except the ones the
        // CloudEvents envelope already carries (content-type → datacontenttype, dataschema → ce_dataschema).
        record.headers().forEach((name, value) -> {
            if (!name.equals(TandemHeaders.CONTENT_TYPE) && !name.equals(TandemHeaders.DATA_SCHEMA)) {
                producerRecord.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
            }
        });
        return producerRecord;
    }

    /** CloudEvents {@code type} is required; fall back to {@code aggregate_type} when the column is null (Q20). */
    private static String typeOf(OutboxRecord record) {
        return record.type() != null ? record.type() : record.aggregateType();
    }

    private String contentTypeOf(OutboxRecord record) {
        String stored = record.headers().get(TandemHeaders.CONTENT_TYPE);
        if (stored != null) {
            return stored;
        }
        return record.contentType() != null ? record.contentType() : cfg.defaultContentType();
    }

    private URI dataSchemaOf(OutboxRecord record) {
        String stored = record.headers().get(TandemHeaders.DATA_SCHEMA);
        if (stored != null) {
            return URI.create(stored);
        }
        return cfg.defaultDataSchema();
    }
}
