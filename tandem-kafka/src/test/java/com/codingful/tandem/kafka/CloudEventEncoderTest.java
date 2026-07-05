package com.codingful.tandem.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.port.TopicRouter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

class CloudEventEncoderTest {

    private static final String SOURCE = "/tandem/orders";
    private final CloudEventEncoder encoder =
            new CloudEventEncoder(TopicRouter.kebabWithSuffix("-topic"), KafkaRelayConfig.of(SOURCE));

    private static OutboxRecord.Builder recordOf(OutboxMessage message) {
        return OutboxRecord.builder().id(42).message(message).createdAt(Instant.parse("2024-01-01T00:00:00Z"));
    }

    private static String header(ProducerRecord<String, byte[]> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    @Test
    void GIVEN_a_record_WHEN_encoded_THEN_the_binary_cloudevent_carries_the_mapped_attributes_and_body() {
        OutboxRecord record = recordOf(OutboxMessage.builder()
                .aggregateId("order-7").aggregateType("Order").type("com.acme.order.placed").seq(5)
                .payload("{\"amount\":1}".getBytes(StandardCharsets.UTF_8))
                .contentType("application/json").header("correlation-id", "corr-9").build()).build();

        ProducerRecord<String, byte[]> encoded = encoder.encode(record);

        assertThat(encoded.topic()).isEqualTo("order-topic");
        assertThat(encoded.key()).isEqualTo("order-7");
        assertThat(new String(encoded.value(), StandardCharsets.UTF_8)).isEqualTo("{\"amount\":1}");
        assertThat(header(encoded, "ce_id")).isEqualTo("42");
        assertThat(header(encoded, "ce_source")).isEqualTo(SOURCE);
        assertThat(header(encoded, "ce_type")).isEqualTo("com.acme.order.placed");
        assertThat(header(encoded, "ce_subject")).isEqualTo("order-7");
        assertThat(header(encoded, CloudEventsHeaders.CE_SEQ)).isEqualTo("5");
        assertThat(header(encoded, CloudEventsHeaders.CE_PARTITION_KEY)).isEqualTo("order-7");
        assertThat(header(encoded, TandemHeaders.CONTENT_TYPE)).isEqualTo("application/json");
        assertThat(header(encoded, "correlation-id")).isEqualTo("corr-9");
    }

    @Test
    void GIVEN_a_record_without_a_type_WHEN_encoded_THEN_the_cloudevent_type_falls_back_to_the_aggregate_type() {
        OutboxRecord record = recordOf(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build()).build();

        assertThat(header(encoder.encode(record), "ce_type")).isEqualTo("Order");
    }

    @Test
    void GIVEN_no_stored_content_type_WHEN_encoded_THEN_the_configured_default_is_used() {
        OutboxRecord record = recordOf(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build()).build();

        assertThat(header(encoder.encode(record), TandemHeaders.CONTENT_TYPE)).isEqualTo("application/json");
    }

    @Test
    void GIVEN_a_stored_data_schema_header_WHEN_encoded_THEN_it_becomes_the_cloudevent_data_schema_not_a_passthrough() {
        OutboxRecord record = recordOf(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload("{}".getBytes())
                .header(TandemHeaders.DATA_SCHEMA, "https://schemas.example/order").build()).build();

        ProducerRecord<String, byte[]> encoded = encoder.encode(record);
        assertThat(header(encoded, "ce_dataschema")).isEqualTo("https://schemas.example/order");
        assertThat(header(encoded, "dataschema")).isNull();   // not duplicated as a raw header
    }

    @Test
    void GIVEN_a_camel_case_aggregate_type_WHEN_encoded_THEN_the_topic_is_kebab_cased() {
        OutboxRecord record = recordOf(OutboxMessage.builder()
                .aggregateId("ol-1").aggregateType("OrderLine").seq(1).payload("{}".getBytes()).build()).build();

        assertThat(encoder.encode(record).topic()).isEqualTo("order-line-topic");
    }
}
