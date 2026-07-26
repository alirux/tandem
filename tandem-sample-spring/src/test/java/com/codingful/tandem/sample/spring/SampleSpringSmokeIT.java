package com.codingful.tandem.sample.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.spring.producer.TransactionalOutboxTemplate;
import com.codingful.tandem.test.TandemTestContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * The end-to-end smoke test for the Spring integration: it boots the <b>real sample application</b> — the
 * same {@code @SpringBootApplication} a reader runs — so both autoconfigurations are active in one context,
 * writes events through the write-side tiers, and asserts the autoconfigured relay published them to Kafka
 * in per-aggregate order. It is the only automated test that exercises {@code tandem-spring-producer} and
 * {@code tandem-spring-relay} <em>together</em>: their own integration tests cover each side alone.
 *
 * <p>Living here has a second payoff: the published tutorial is verified by CI, so it cannot rot unnoticed.
 *
 * <p>The Testcontainers PostgreSQL and Kafka come from the sample's own
 * {@link DemoContainerInitializer} (registered in {@code application.yml}), which is exactly the wiring path
 * under test. The {@code test} profile keeps {@link SampleRunner} out of the context — the demo's own
 * writes and console narration would be an uncontrolled precondition — so this test drives the tiers
 * itself; the aggregate ids are still distinct, so the assertions never depend on that.
 */
@Tag("integration")
@SpringBootTest
@ActiveProfiles("test")
class SampleSpringSmokeIT {

    private static final String AGGREGATE_TYPE = "Order";
    private static final String ANNOTATED_AGGREGATE = "smoke-annotated";
    private static final String TEMPLATE_AGGREGATE = "smoke-template";

    @Autowired
    private OrderService orders;

    @Autowired
    private TransactionalOutboxTemplate outboxTemplate;

    @Autowired
    private TandemTestContainer infrastructure;

    @Test
    void GIVEN_the_spring_application_WHEN_events_are_written_through_the_tiers_THEN_the_relay_delivers_them_in_order() {
        orders.place(ANNOTATED_AGGREGATE);
        orders.confirm(ANNOTATED_AGGREGATE);
        outboxTemplate.executeWithoutResult(outbox ->
                outbox.record(AGGREGATE_TYPE, TEMPLATE_AGGREGATE, 1L, Map.of("order", TEMPLATE_AGGREGATE)));

        Map<String, List<Long>> delivered = consumeSmokeEvents(3, Duration.ofSeconds(60));

        assertThat(delivered.get(ANNOTATED_AGGREGATE)).containsExactly(1L, 2L);
        assertThat(delivered.get(TEMPLATE_AGGREGATE)).containsExactly(1L);
    }

    /**
     * Poll the topic until this test's own aggregates have produced {@code expected} records, ignoring any
     * the demo runner may have written. A unique consumer group reads from the beginning, so records the
     * relay published before the first poll are still seen.
     */
    private Map<String, List<Long>> consumeSmokeEvents(int expected, Duration timeout) {
        Map<String, List<Long>> seqsByAggregate = new LinkedHashMap<>();
        String groupId = "smoke-it-" + System.nanoTime();
        try (KafkaConsumer<String, byte[]> consumer =
                infrastructure.newConsumer(groupId, DemoContainerInitializer.TOPIC)) {
            long deadline = System.nanoTime() + timeout.toNanos();
            int mine = 0;
            while (mine < expected && System.nanoTime() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (!record.key().startsWith("smoke-")) {
                        continue;
                    }
                    long seq = Long.parseLong(
                            new String(record.headers().lastHeader("ce_seq").value(), StandardCharsets.UTF_8));
                    seqsByAggregate.computeIfAbsent(record.key(), key -> new ArrayList<>()).add(seq);
                    mine++;
                }
            }
        }
        return seqsByAggregate;
    }
}
