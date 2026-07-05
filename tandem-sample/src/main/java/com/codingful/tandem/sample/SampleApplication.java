package com.codingful.tandem.sample;

import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.WorkerPool;
import com.codingful.tandem.kafka.KafkaRelayConfig;
import com.codingful.tandem.test.TandemTestContainer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * <h2>Tandem — getting started</h2>
 *
 * <p>Run this class directly. Docker must be running; containers start automatically.
 *
 * <h3>The problem: dual write</h3>
 *
 * <p>An order service must save an {@code Order} row to the database AND publish an
 * {@code OrderPlaced} event to Kafka. Doing both in sequence is unsafe:
 *
 * <ul>
 *   <li>The database commit can succeed while the Kafka publish fails — the event is lost.</li>
 *   <li>The Kafka publish can succeed while the database rolls back — a ghost event is emitted.</li>
 * </ul>
 *
 * <h3>The solution: Transactional Outbox</h3>
 *
 * <p>Instead of writing to Kafka directly, the service writes an event row to a
 * {@code tandem_outbox} table <em>in the same transaction</em> as the domain change. The two
 * writes are atomic. A background relay process reads the outbox table and publishes to Kafka,
 * acknowledging each row only after the broker confirms receipt.
 *
 * <pre>
 *   Order Service               Tandem Relay
 *   ┌──────────────────────┐   ┌──────────────────────────────┐
 *   │ BEGIN TX             │   │                              │
 *   │ INSERT orders        │   │  claim head-of-chain rows    │
 *   │ INSERT tandem_outbox │   │  publish → Kafka (CloudEvent)│
 *   │ COMMIT               │──►│  mark DONE after ack         │
 *   └──────────────────────┘   └──────────────────────────────┘
 * </pre>
 *
 * <h3>Per-aggregate ordering</h3>
 *
 * <p>Tandem guarantees that events for the same aggregate (same {@code aggregateId}) are published
 * to Kafka in {@code seq} order. It never publishes seq N+1 until seq N is acknowledged. Events
 * for different aggregates are dispatched concurrently.
 *
 * <h3>What this sample demonstrates</h3>
 *
 * <ol>
 *   <li>Inserting 5 outbox events for two interleaved orders (A: created/confirmed/shipped,
 *       B: created/confirmed).</li>
 *   <li>Starting the relay with 2 workers.</li>
 *   <li>Consuming the resulting CloudEvents from Kafka.</li>
 *   <li>Verifying that each order's events arrive in seq order even though the two orders were
 *       interleaved in the outbox.</li>
 * </ol>
 *
 * <h3>Who does what</h3>
 *
 * <p>Every step below is tagged with who owns it in a real application:
 *
 * <ul>
 *   <li><b>[CALLER]</b> — your application code. The only Tandem class on this path is
 *       {@link com.codingful.tandem.jdbc.JdbcOutboxRepository}.</li>
 *   <li><b>[TANDEM]</b> — the library: the relay engine that claims, publishes, and acknowledges
 *       outbox rows. You configure and start it; you never call its internals directly.</li>
 *   <li><b>[DEMO-ONLY]</b> — exists to make this sample self-contained and observable
 *       (Testcontainers, topic creation, the Kafka consumer that prints results). None of it
 *       ships with Tandem or appears in a real application.</li>
 * </ul>
 */
public final class SampleApplication {

    private static final String TOPIC = "orders";

    public static void main(String[] args) throws Exception {
        banner();

        // -----------------------------------------------------------------------
        // [DEMO-ONLY] Step 1 — start PostgreSQL + Kafka
        //
        // TandemTestContainer uses Testcontainers to start real Docker containers.
        // In production you supply your own DataSource and Kafka bootstrap servers;
        // TandemTestContainer is only needed here to make the sample self-contained.
        // Tandem itself has no Docker/Testcontainers dependency.
        // -----------------------------------------------------------------------
        System.out.println("► Starting PostgreSQL + Kafka (Docker)...");
        try (TandemTestContainer tandem = new TandemTestContainer()) {
            tandem.start();
            System.out.println("  ✓ ready\n");

            // -------------------------------------------------------------------
            // [DEMO-ONLY] Connect with an external tool
            //
            // The containers stay up for the whole run (and pause for input at
            // the end), so you can point a SQL client at Postgres or a Kafka UI
            // at the broker while the demo is in progress or after it completes.
            // -------------------------------------------------------------------
            printConnectionDetails(tandem);

            // -------------------------------------------------------------------
            // [CALLER] Step 2 — wire up the write side
            //
            // JdbcOutboxRepository is the only Tandem class your application code
            // imports on the write path. It has a single method: insert(OutboxMessage).
            // tandem.newRepository(bucketCount) is a TandemTestContainer convenience
            // for `new JdbcOutboxRepository(dataSource, bucketCount)` — the real
            // production constructor, shown in the README.
            //
            // bucketCount is a shard count baked into every outbox row. It controls
            // which relay worker is responsible for each aggregate. The value must
            // (a) never change after the first deployment, and (b) be the IDENTICAL
            // value passed to RelayConfig.builder().bucketCount(...) below (Step 5) —
            // a mismatch means rows hash into buckets the relay never polls.
            // -------------------------------------------------------------------
            int bucketCount = 256;
            var repository = tandem.newRepository(bucketCount);

            // -------------------------------------------------------------------
            // [CALLER] Step 3 — insert events (simulating the write side)
            //
            // In a real application each call below lives inside a @Transactional
            // method so the domain INSERT and the outbox INSERT commit atomically.
            // See OrderService for the insert(OutboxMessage) call this wraps.
            //
            // We interleave two orders (A and B) on purpose to show that the relay
            // (a [TANDEM] responsibility, Step 5) maintains per-aggregate ordering
            // independently of insertion order.
            // -------------------------------------------------------------------
            System.out.println("► Inserting events into tandem_outbox...");
            var orders = new OrderService(repository);
            orders.place("order-A");       // outbox: A seq=1
            orders.place("order-B");       // outbox: B seq=1
            orders.confirm("order-A");     // outbox: A seq=2
            orders.confirm("order-B");     // outbox: B seq=2
            orders.ship("order-A");        // outbox: A seq=3
            System.out.println("  ✓ 5 events in tandem_outbox\n");

            // -------------------------------------------------------------------
            // [DEMO-ONLY] Step 4 — create the topic and subscribe the consumer
            //
            // Neither topic creation nor the consumer are part of Tandem — they
            // exist purely so this sample can observe what the relay publishes.
            // The consumer must join the group BEFORE the relay starts so no event
            // is lost. TandemTestContainer sets AUTO_OFFSET_RESET=earliest, so even
            // if an event lands before the first poll it will be read from the start.
            // -------------------------------------------------------------------
            tandem.createTopic(TOPIC, 4);   // 4 partitions — one key per aggregate partition

            try (var consumer = tandem.newConsumer("sample-group", TOPIC)) {
                consumer.poll(Duration.ofMillis(200));  // triggers group coordinator join

                // ---------------------------------------------------------------
                // [TANDEM] Step 5 — start the relay
                //
                // WorkerPool spawns relay workers. Each worker:
                //   1. Claims the head-of-chain outbox rows for its assigned buckets
                //      using SELECT … FOR UPDATE SKIP LOCKED — no polling conflicts.
                //   2. Publishes each event to Kafka as a CloudEvent (binary mode).
                //   3. Marks the row DONE only after the broker acknowledges.
                //   4. Moves to the next seq for the same aggregate.
                // All of the above is Tandem's job — your application only builds
                // the config and calls start()/stop().
                //
                // tandem.newRelay(...) is a TandemTestContainer convenience for:
                //   new WorkerPool(new JdbcOutboxStore(dataSource, cfg.maxAttempts()),
                //                   new KafkaRelay(producerConfig, router, kafkaCfg),
                //                   cfg)
                // — see the README for the real production wiring.
                //
                // The two arguments your application DOES configure ([CALLER]):
                //   - TopicRouter maps each OutboxRecord to a Kafka topic. Here every
                //     event goes to the same "orders" topic. In production you would
                //     typically route by aggregateType, e.g.:
                //       TopicRouter.kebabWithSuffix("-events") → "Order" → "order-events"
                //   - KafkaRelayConfig sets the CloudEvents `source` attribute.
                // ---------------------------------------------------------------
                System.out.println("► Starting relay (2 workers)...");
                RelayConfig relayCfg = RelayConfig.builder()
                        .bucketCount(bucketCount)   // must match Step 2's repository bucketCount
                        .workersPerInstance(2)
                        .pollInterval(Duration.ofMillis(50))
                        .build();

                WorkerPool relay = tandem.newRelay(
                        relayCfg,
                        record -> TOPIC,
                        KafkaRelayConfig.of("/tandem/sample"));
                relay.start();
                System.out.println("  ✓ relay running\n");

                // ---------------------------------------------------------------
                // [DEMO-ONLY] Step 6 — consume and print the CloudEvents
                //
                // Reading back from Kafka to display results is not part of Tandem
                // either — it's how this sample proves the relay did its job.
                // Each Kafka message uses CloudEvents binary encoding:
                //   key     = aggregateId   (Kafka partition key → same partition per aggregate)
                //   value   = raw payload bytes
                //   headers = ce_id, ce_type, ce_source, ce_time,
                //             ce_seq (Tandem extension: the per-aggregate sequence number),
                //             ce_partitionkey (Tandem extension: equals aggregateId),
                //             content-type, …
                // ---------------------------------------------------------------
                System.out.println("► Consuming events from Kafka topic '" + TOPIC + "'...");
                var sampler = new SampleConsumer(consumer);
                Map<String, List<Long>> received = sampler.consumeAll(5, Duration.ofSeconds(30));

                relay.stop();

                // ---------------------------------------------------------------
                // [DEMO-ONLY] Step 7 — verify per-aggregate ordering
                //
                // Even though the two orders were interleaved in the outbox, each
                // order's events must arrive in strict seq order on Kafka — this
                // assertion is the sample's self-check, not a Tandem API.
                // -------------------------------------------------------------------
                System.out.println("\n" + "─".repeat(56));
                System.out.println(" Results");
                System.out.println("─".repeat(56));
                received.forEach((id, seqs) ->
                        System.out.printf("  %-12s → ce_seq list: %s%n", id, seqs));
                System.out.println();

                boolean orderedA = List.of(1L, 2L, 3L).equals(received.get("order-A"));
                boolean orderedB = List.of(1L, 2L).equals(received.get("order-B"));
                if (orderedA && orderedB) {
                    System.out.println("  ✓ All events delivered in per-aggregate seq order.");
                } else {
                    System.out.println("  ✗ Ordering check failed — see output above.");
                }
            }

            // -------------------------------------------------------------------
            // Keep the containers alive until the user is done inspecting them.
            // Closing this try-with-resources block (next line) stops Postgres
            // and Kafka, so we block here rather than exiting immediately.
            // -------------------------------------------------------------------
            waitForUserToInspect();
        }

        System.out.println("\nDone.");
    }

    /** Prints JDBC and Kafka connection details so an external client can attach to the running containers. */
    private static void printConnectionDetails(TandemTestContainer tandem) {
        System.out.println("─".repeat(56));
        System.out.println(" Connect with an external tool");
        System.out.println("─".repeat(56));
        System.out.println(" PostgreSQL:");
        System.out.println("   JDBC URL : " + tandem.postgresJdbcUrl());
        System.out.println("   Username : " + tandem.postgresUsername());
        System.out.println("   Password : " + tandem.postgresPassword());
        System.out.println("   Table    : tandem_outbox");
        System.out.println();
        System.out.println(" Kafka:");
        System.out.println("   Bootstrap servers : " + tandem.bootstrapServers());
        System.out.println("   Topic             : " + TOPIC);
        System.out.println("─".repeat(56));
        System.out.println();
    }

    /** Blocks until the user presses ENTER, keeping the containers alive in the meantime. */
    private static void waitForUserToInspect() throws IOException {
        System.out.println();
        System.out.println("Containers are still running — connect now with an external tool if you like.");
        System.out.print("Press ENTER to stop the containers and exit... ");
        System.out.flush();
        new BufferedReader(new InputStreamReader(System.in)).readLine();
    }

    private static void banner() {
        System.out.println("=".repeat(56));
        System.out.println(" Tandem — Transactional Outbox sample");
        System.out.println("=".repeat(56));
        System.out.println();
        System.out.println(" Inserts 5 outbox events for two interleaved orders and");
        System.out.println(" verifies that the relay delivers them to Kafka in");
        System.out.println(" per-aggregate seq order.");
        System.out.println();
    }
}
