package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.jdbc.BackoffStrategy;
import com.codingful.tandem.jdbc.BucketCountGuard;
import com.codingful.tandem.jdbc.BucketSource;
import com.codingful.tandem.jdbc.JdbcOutboxStore;
import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.WorkerPool;
import com.codingful.tandem.kafka.KafkaRelay;
import com.codingful.tandem.kafka.KafkaRelayConfig;
import com.codingful.tandem.test.TandemTestContainer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Dedicated container + wiring environment for the benchmark (LLD-benchmark §3) — <b>not</b>
 * {@code TandemTestContainer.newRelay}: that helper's {@code DataSource} opens a fresh, unpooled
 * connection per call, which cannot bound the driver's real concurrency (LLD-benchmark §4.2). This
 * class wires a Hikari-pooled {@link DataSource} and assembles the relay directly, with a
 * {@link FaultInjector} seam for S6.
 *
 * <p>The Kafka producer already defaults to the mandated {@code acks=all} +
 * {@code enable.idempotence=true} — {@link KafkaRelay} hardens any producer config to those values
 * (LLD-kafka §1), so no override is needed here to get the production config.
 */
public final class BenchmarkEnvironment implements AutoCloseable {

    public static final String TOPIC = "tandem-benchmark-events";
    private static final int TOPIC_PARTITIONS = 16;

    private final BenchmarkConfig config;
    private final TandemTestContainer containers = new TandemTestContainer();
    private final FaultInjector faultInjector = new FaultInjector();
    private final List<KafkaRelay> extraProducers = new ArrayList<>();

    private HikariDataSource dataSource;
    private JdbcOutboxStore store;
    private KafkaRelay relay;
    private WorkerPool relayPool;
    private BenchmarkMetrics metrics;
    private LagProbe lagProbe;

    public BenchmarkEnvironment(BenchmarkConfig config) {
        this.config = config;
    }

    public BenchmarkEnvironment start() {
        containers.start();
        dataSource = pooledDataSource();
        applyBenchSchema();
        containers.createTopic(TOPIC, TOPIC_PARTITIONS);

        // Bucket-count guard (LLD-bucket-count-guard §7): the explicit assembly-level startup check,
        // run once against the plain pooled DataSource. The write-side (LoadGenerator's repository) and
        // every relay in this environment share config.bucketCount(), so seeding it here establishes the
        // value the whole run agrees on — mirroring how a real assembly runs the guard at startup.
        BucketCountGuard.check(dataSource, config.bucketCount());

        store = new JdbcOutboxStore(dataSource, config.maxAttempts());
        metrics = new BenchmarkMetrics();
        lagProbe = new LagProbe(dataSource);

        relay = new KafkaRelay(producerConfig(), record -> TOPIC, KafkaRelayConfig.of("/tandem/benchmark"));
        OutboxDispatcher dispatcher = new FaultInjectingDispatcher(relay, faultInjector);

        // relayConfigBuilder() defaults coordination to RelayConfig's own default (SINGLE), matching
        // every scenario except S8 (which builds its own LEASE-coordinated instances via
        // newRelayInstance instead of this primary pool).
        RelayConfig relayCfg = relayConfigBuilder().build();
        relayPool = new WorkerPool(store, dispatcher, relayCfg, metrics, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), BucketSource.forCoordination(relayCfg, dataSource));
        return this;
    }

    /**
     * A {@link RelayConfig.Builder} pre-populated from this environment's {@link BenchmarkConfig}
     * (bucket count, workers, batch size, row lease, max attempts, the real delivery timeout) — the
     * shared sizing every relay instance in this environment uses. S8 layers
     * {@code .coordination(LEASE).instanceId(...)} on top to build its own additional instances.
     */
    public RelayConfig.Builder relayConfigBuilder() {
        return RelayConfig.builder()
                .bucketCount(config.bucketCount())
                .workersPerInstance(config.workers())
                .batchSize(config.batchSize())
                .rowLease(config.rowLease())
                .maxAttempts(config.maxAttempts())
                .deliveryTimeoutMs(relay.deliveryTimeoutMs());
    }

    /**
     * Builds an additional, independent relay instance — its own Kafka producer, its own
     * {@link BucketSource} per {@code relayCfg.coordination()} — sharing this environment's
     * {@code DataSource}/{@code JdbcOutboxStore} (as real instances share one DB). For scenarios
     * simulating more than one relay instance against one outbox (S8: {@code LEASE} coordination,
     * HLD §3.2 axis 2 — the "N embedded replicas" case a naive {@code SINGLE} deployment gets wrong).
     * The caller {@code start()}s/{@code stop()}s the returned pool; this environment closes the
     * producer on {@link #close()}.
     */
    public RelayInstance newRelayInstance(RelayConfig relayCfg) {
        KafkaRelay producer = new KafkaRelay(producerConfig(), record -> TOPIC, KafkaRelayConfig.of("/tandem/benchmark"));
        extraProducers.add(producer);
        OutboxDispatcher dispatcher = new FaultInjectingDispatcher(producer, faultInjector);
        BucketSource bucketSource = BucketSource.forCoordination(relayCfg, dataSource);
        WorkerPool pool = new WorkerPool(store, dispatcher, relayCfg, metrics, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), bucketSource);
        return new RelayInstance(pool, bucketSource, producer);
    }

    private Map<String, Object> producerConfig() {
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, containers.bootstrapServers());
        producerConfig.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) config.deliveryTimeoutMs());
        // Kafka requires delivery.timeout.ms >= linger.ms + request.timeout.ms; request.timeout.ms
        // defaults to 30s, so a smaller deliveryTimeoutMs (the smoke config) must shrink it too.
        producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) Math.min(30_000, config.deliveryTimeoutMs()));
        return producerConfig;
    }

    public BenchmarkConfig config() {
        return config;
    }

    /** The pooled write/relay {@link DataSource}. */
    public DataSource dataSource() {
        return dataSource;
    }

    public JdbcOutboxStore store() {
        return store;
    }

    /** Caller {@code start()}s/{@code stop()}s it per scenario. */
    public WorkerPool relayPool() {
        return relayPool;
    }

    public BenchmarkMetrics metrics() {
        return metrics;
    }

    public LagProbe lagProbe() {
        return lagProbe;
    }

    public FaultInjector faultInjector() {
        return faultInjector;
    }

    /** A consumer subscribed to the benchmark topic, reading from the beginning. Caller closes it. */
    public KafkaConsumer<String, byte[]> newConsumer(String groupId) {
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, containers.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, byte[]> consumer =
                new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(List.of(TOPIC));
        return consumer;
    }

    private HikariDataSource pooledDataSource() {
        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(containers.postgresJdbcUrl());
        hikari.setUsername(containers.postgresUsername());
        hikari.setPassword(containers.postgresPassword());
        hikari.setMaximumPoolSize(config.maxConnections());
        hikari.setPoolName("tandem-benchmark");
        return new HikariDataSource(hikari);
    }

    private void applyBenchSchema() {
        try (InputStream ddlStream = getClass().getResourceAsStream("/bench-schema.sql")) {
            if (ddlStream == null) {
                throw new IllegalStateException("bench-schema.sql not found on classpath");
            }
            String ddl = new String(ddlStream.readAllBytes(), StandardCharsets.UTF_8);
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("applying bench schema failed", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        if (relayPool != null) {
            relayPool.stop();
        }
        if (relay != null) {
            relay.close();
        }
        for (KafkaRelay producer : extraProducers) {
            producer.close();
        }
        if (dataSource != null) {
            dataSource.close();
        }
        containers.close();
    }
}
