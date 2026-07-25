package com.codingful.tandem.sample.spring;

import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.WorkerPool;
import com.codingful.tandem.kafka.KafkaRelayConfig;
import com.codingful.tandem.spring.producer.TransactionalOutboxTemplate;
import com.codingful.tandem.test.TandemTestContainer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * [DEMO-ONLY] Drives the sample once the context is up: it writes events through the three write-side
 * tiers, prints the resulting {@code tandem_outbox} rows, then runs the relay to confirm delivery to
 * Kafka in per-aggregate order. A real application has no such runner — its write side is just the
 * {@code @TransactionalOutbox} methods on {@link OrderService}, called by its own request handlers.
 */
@Component
class SampleRunner implements CommandLineRunner {

    private static final String TOPIC = "orders";
    private static final int BUCKET_COUNT = 256;
    private static final int EXPECTED_EVENTS = 7;

    private final OrderService orders;
    private final TransactionalOutboxTemplate outboxTemplate;
    private final TandemTestContainer infrastructure;

    SampleRunner(OrderService orders, TransactionalOutboxTemplate outboxTemplate,
            TandemTestContainer infrastructure) {
        this.orders = orders;
        this.outboxTemplate = outboxTemplate;
        this.infrastructure = infrastructure;
    }

    @Override
    public void run(String... args) throws Exception {
        banner(); // [DEMO-ONLY] console narration
        writeThroughTheTiers(); // [CALLER] the orders.place/confirm/ship/... calls are real app code; the println's around them are demo narration
        printOutboxRows(); // [DEMO-ONLY] direct SQL introspection of tandem_outbox, just to show the reader what landed
        deliverAndVerify(); // [DEMO-ONLY] starts the relay + a Kafka consumer in-process to verify delivery; in production the relay is its own process, not part of the app's request flow
        System.out.println("\nDone."); // [DEMO-ONLY]
    }

    /** Step 1 [CALLER] — write events through three tiers. This is the developer experience on show. */
    private void writeThroughTheTiers() {
        System.out.println("► Writing events (the producer developer experience)\n");

        // Tier 1 — @TransactionalOutbox: no explicit transaction, no explicit outbox call. Two orders
        // are interleaved on purpose, to show ordering is per-aggregate, not per-insert.
        System.out.println("  @TransactionalOutbox — interleaving order-A and order-B:");
        orders.place("order-A");
        orders.place("order-B");
        orders.confirm("order-A");
        orders.confirm("order-B");
        orders.ship("order-A");
        System.out.println("    order-A: place, confirm, ship   order-B: place, confirm");

        // Tier 2 — Template: an object payload, serialized by the optional Jackson serializer.
        System.out.println("  TransactionalOutboxTemplate — object payload for order-T:");
        outboxTemplate.executeWithoutResult(outbox ->
                outbox.record("Order", "order-T", 1L, new OrderPayload("order-T", "OrderPlaced")));
        System.out.println("    order-T: placed via the template");

        // Tier 3 — Spring events: publish a domain event, a mapper turns it into an outbox row.
        System.out.println("  Spring application events — order-E:");
        orders.noteViaEvent("order-E", 1L);
        System.out.println("    order-E: noted via a published event\n");
    }

    /** Step 2 — prove the rows landed atomically in tandem_outbox. */
    private void printOutboxRows() throws Exception {
        System.out.println("► tandem_outbox now holds:");
        try (Connection connection = infrastructure.dataSource().getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT aggregate_id, seq, type FROM tandem_outbox ORDER BY aggregate_id, seq");
                ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                System.out.printf("    %-10s seq=%d  %s%n",
                        resultSet.getString("aggregate_id"), resultSet.getLong("seq"), resultSet.getString("type"));
            }
        }
        System.out.println();
    }

    /** Step 3 [TANDEM/DEMO] — start the relay, consume what it publishes, and check per-aggregate order. */
    private void deliverAndVerify() throws Exception {
        System.out.println("► Starting the relay and consuming from Kafka...");
        infrastructure.createTopic(TOPIC, 4);
        try (KafkaConsumer<String, byte[]> consumer = infrastructure.newConsumer("sample-spring-group", TOPIC)) {
            consumer.poll(Duration.ofMillis(200)); // join the group before the relay starts

            RelayConfig relayConfig = RelayConfig.builder()
                    .bucketCount(BUCKET_COUNT)
                    .workersPerInstance(2)
                    .pollInterval(Duration.ofMillis(50))
                    .build();
            WorkerPool relay = infrastructure.newRelay(
                    relayConfig, record -> TOPIC, KafkaRelayConfig.of("/tandem/sample-spring"));
            relay.start();

            Map<String, List<Long>> seqsByAggregate = consume(consumer, EXPECTED_EVENTS, Duration.ofSeconds(30));
            relay.stop();

            System.out.println();
            seqsByAggregate.forEach((aggregateId, seqs) ->
                    System.out.printf("    %-10s → ce_seq %s%n", aggregateId, seqs));

            boolean orderedA = List.of(1L, 2L, 3L).equals(seqsByAggregate.get("order-A"));
            boolean orderedB = List.of(1L, 2L).equals(seqsByAggregate.get("order-B"));
            System.out.println();
            if (orderedA && orderedB) {
                System.out.println("  ✓ Each aggregate's events arrived in seq order, from all three tiers.");
            } else {
                System.out.println("  ✗ Ordering check failed — see output above.");
            }
        }
    }

    /** Poll until {@code expected} records are collected or the timeout elapses; group ce_seq by key. */
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

    /** [DEMO-ONLY] The object payload for the template tier; Jackson serializes it to JSON. */
    private record OrderPayload(String order, String event) {
    }

    private static void banner() {
        System.out.println("=".repeat(60));
        System.out.println(" Tandem — the write side with Spring Boot");
        System.out.println("=".repeat(60));
        System.out.println();
        System.out.println(" Writes events through three producer tiers and shows them");
        System.out.println(" reach Kafka in per-aggregate seq order.");
        System.out.println();
    }
}
