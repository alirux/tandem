package com.codingful.tandem.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.port.TopicRouter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Tag("integration")
@SuppressWarnings({"deprecation", "resource"})  // deprecation: legacy KafkaContainer+withKraft; resource: JUnit @BeforeAll/@AfterAll lifecycle
class KafkaRelayIT {

    // Confluent cp-kafka in KRaft mode (no ZooKeeper) — a reliable single-node broker for the binding test.
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1")).withKraft();

    @BeforeAll
    static void startKafka() {
        KAFKA.start();
    }

    @AfterAll
    static void stopKafka() {
        KAFKA.stop();
    }

    @Test
    void GIVEN_a_record_WHEN_dispatched_to_a_real_broker_THEN_the_cloudevent_lands_on_the_topic_in_binary_form()
            throws Exception {
        String topic = "order-topic";
        OutboxRecord record = OutboxRecord.builder()
                .id(99)
                .message(OutboxMessage.builder()
                        .aggregateId("order-7").aggregateType("Order").type("com.acme.order.placed").seq(3)
                        .payload("{\"amount\":42}".getBytes(StandardCharsets.UTF_8))
                        .contentType("application/json").header("correlation-id", "corr-1").build())
                .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
                .build();

        try (KafkaConsumer<String, byte[]> consumer = consumer();
             KafkaRelay relay = new KafkaRelay(producerConfig(), TopicRouter.kebabWithSuffix("-topic"),
                     KafkaRelayConfig.of("/tandem/orders"))) {
            consumer.subscribe(List.of(topic));
            consumer.poll(Duration.ofMillis(200));   // join the group before producing

            relay.dispatch(record).get(Duration.ofSeconds(10).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);

            ConsumerRecord<String, byte[]> consumed = pollOne(consumer);
            assertThat(consumed.key()).isEqualTo("order-7");
            assertThat(new String(consumed.value(), StandardCharsets.UTF_8)).isEqualTo("{\"amount\":42}");
            assertThat(header(consumed, "ce_id")).isEqualTo("99");
            assertThat(header(consumed, "ce_source")).isEqualTo("/tandem/orders");
            assertThat(header(consumed, "ce_type")).isEqualTo("com.acme.order.placed");
            assertThat(header(consumed, "ce_subject")).isEqualTo("order-7");
            assertThat(header(consumed, CloudEventsHeaders.CE_SEQ)).isEqualTo("3");
            assertThat(header(consumed, CloudEventsHeaders.CE_PARTITION_KEY)).isEqualTo("order-7");
            assertThat(header(consumed, TandemHeaders.CONTENT_TYPE)).isEqualTo("application/json");
            assertThat(header(consumed, "correlation-id")).isEqualTo("corr-1");
        }
    }

    private static Map<String, Object> producerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        return config;
    }

    private static KafkaConsumer<String, byte[]> consumer() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "tandem-it-" + System.nanoTime());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer());
    }

    private static ConsumerRecord<String, byte[]> pollOne(KafkaConsumer<String, byte[]> consumer) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        throw new AssertionError("no record arrived on the topic within the timeout");
    }

    private static String header(ConsumerRecord<String, byte[]> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
