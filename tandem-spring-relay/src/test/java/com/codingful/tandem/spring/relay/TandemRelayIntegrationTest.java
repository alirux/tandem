package com.codingful.tandem.spring.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.test.TandemTestContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.SmartLifecycle;

/**
 * Proves the autoconfigured relay actually delivers (LLD-spring-config §4.4): with only a DataSource and
 * the Kafka properties, the module wires the engine and its {@link org.springframework.context.SmartLifecycle}
 * starts it, so pending outbox rows reach Kafka in per-aggregate order. Runs against a real PostgreSQL +
 * Kafka via {@link TandemTestContainer}. The default router maps {@code Order} to the {@code order-topic}.
 */
@Tag("integration")
class TandemRelayIntegrationTest {

    private static final String TOPIC = "order-topic";
    private static final int BUCKET_COUNT = 256;

    private static TandemTestContainer infrastructure;

    @BeforeAll
    static void startContainer() {
        infrastructure = new TandemTestContainer().start();
    }

    @AfterAll
    static void stopContainer() {
        if (infrastructure != null) {
            infrastructure.close();
        }
    }

    private static OutboxMessage message(String aggregateId, long seq) {
        return OutboxMessage.builder()
                .aggregateType("Order").aggregateId(aggregateId).seq(seq)
                .payload(("{\"id\":\"" + aggregateId + "\"}").getBytes(StandardCharsets.UTF_8))
                .contentType("application/json")
                .build();
    }

    @Test
    void GIVEN_pending_outbox_rows_WHEN_the_autoconfigured_relay_runs_THEN_they_reach_kafka_in_order() {
        OutboxRepository repository = infrastructure.newRepository(BUCKET_COUNT);
        repository.insert(message("order-A", 1L));
        repository.insert(message("order-A", 2L));
        repository.insert(message("order-B", 1L));
        infrastructure.createTopic(TOPIC, 4);

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(TandemRelayAutoConfiguration.class))
                .withBean(DataSource.class, () -> infrastructure.dataSource())
                // A second SmartLifecycle bean must not make the relay's own lifecycle back off (that
                // would leave the relay unstarted) — a real Boot app always has other lifecycle beans.
                .withBean("unrelatedLifecycle", SmartLifecycle.class, NoopLifecycle::new)
                .withPropertyValues(
                        "tandem.kafka.source=/tandem/relay-it",
                        "tandem.kafka.producer[bootstrap.servers]=" + infrastructure.bootstrapServers())
                .run(context -> {
                    try (KafkaConsumer<String, byte[]> consumer =
                            infrastructure.newConsumer("relay-it-group", TOPIC)) {
                        Map<String, List<Long>> delivered = consume(consumer, 3, Duration.ofSeconds(30));
                        assertThat(delivered.get("order-A")).containsExactly(1L, 2L);
                        assertThat(delivered.get("order-B")).containsExactly(1L);
                    }
                });
    }

    /** A stand-in for the other lifecycle beans a real application has, to guard the relay's own. */
    static final class NoopLifecycle implements SmartLifecycle {
        private boolean running;

        @Override
        public void start() {
            running = true;
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }
    }

    private static Map<String, List<Long>> consume(KafkaConsumer<String, byte[]> consumer, int expected,
            Duration timeout) {
        Map<String, List<Long>> seqsByAggregate = new LinkedHashMap<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        int total = 0;
        while (total < expected && System.nanoTime() < deadline) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, byte[]> record : records) {
                long seq = Long.parseLong(
                        new String(record.headers().lastHeader("ce_seq").value(), StandardCharsets.UTF_8));
                seqsByAggregate.computeIfAbsent(record.key(), key -> new ArrayList<>()).add(seq);
                total++;
            }
        }
        return seqsByAggregate;
    }
}
