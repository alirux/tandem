package com.codingful.tandem.spring.relay;

import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.core.port.TopicRouter;
import com.codingful.tandem.jdbc.BackoffStrategy;
import com.codingful.tandem.jdbc.BucketSource;
import com.codingful.tandem.jdbc.JdbcOutboxStore;
import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.WorkerPool;
import com.codingful.tandem.kafka.KafkaRelay;
import com.codingful.tandem.kafka.KafkaRelayConfig;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Relay autoconfiguration (LLD-spring-config §4.4): contributes the relay engine — topic router, Kafka
 * dispatcher, outbox store, bucket source and {@link WorkerPool} — and a {@link RelayLifecycle} that
 * starts and stops it with the application. Ordered after Spring Boot's own
 * {@code DataSourceAutoConfiguration} and gated on a single {@code DataSource} candidate; the whole
 * configuration is conditional on {@code tandem.relay.enabled} (default true), the supported way to load
 * the module without running a relay. Every bean is {@link ConditionalOnMissingBean}, so an application
 * can replace any piece — most usefully a custom {@link TopicRouter}.
 *
 * <p>The ordering is declared by <b>name</b> for both generations: Boot 4 moved
 * {@code DataSourceAutoConfiguration} into {@code spring-boot-jdbc}, so a class literal would name a type
 * absent there and the ordering would be silently lost (LLD-spring-config §1.1).
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",   // Spring Boot 3.x
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration"})  // Spring Boot 4.x
@ConditionalOnSingleCandidate(DataSource.class)
@ConditionalOnProperty(prefix = "tandem.relay", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties({
        TandemOutboxProperties.class, TandemRelayProperties.class, TandemKafkaProperties.class})
public class TandemRelayAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    RelayConfig tandemRelayConfig(TandemOutboxProperties outbox, TandemRelayProperties relay) {
        return buildRelayConfig(outbox, relay);
    }

    /**
     * Map the bound properties onto a {@link RelayConfig}. Only properties the user set override
     * {@code RelayConfig}'s own defaults, so the config object stays the single source of truth
     * (LLD-spring-config §2.2/§4.2). Package-private so it is unit-testable without a context.
     */
    static RelayConfig buildRelayConfig(TandemOutboxProperties outbox, TandemRelayProperties relay) {
        RelayConfig.Builder builder = RelayConfig.builder().bucketCount(outbox.bucketCount());
        // Only override RelayConfig's own defaults for properties the user actually set, so the config
        // object stays the single source of truth for defaults (LLD-spring-config §2.2/§4.2).
        if (relay.coordination() != null) {
            builder.coordination(relay.coordination());
        }
        if (relay.instanceId() != null) {
            builder.instanceId(relay.instanceId());
        }
        if (relay.bucketLease() != null) {
            builder.bucketLease(relay.bucketLease());
        }
        if (relay.workersPerInstance() != null) {
            builder.workersPerInstance(relay.workersPerInstance());
        }
        if (relay.pollInterval() != null) {
            builder.pollInterval(relay.pollInterval());
        }
        if (relay.batchSize() != null) {
            builder.batchSize(relay.batchSize());
        }
        if (relay.rowLease() != null) {
            builder.rowLease(relay.rowLease());
        }
        if (relay.maxAttempts() != null) {
            builder.maxAttempts(relay.maxAttempts());
        }
        if (relay.retention() != null) {
            builder.retention(relay.retention());
        }
        if (relay.cleanupBatchSize() != null) {
            builder.cleanupBatchSize(relay.cleanupBatchSize());
        }
        if (relay.reclaimInterval() != null) {
            builder.reclaimInterval(relay.reclaimInterval());
        }
        if (relay.cleanupInterval() != null) {
            builder.cleanupInterval(relay.cleanupInterval());
        }
        if (relay.metricsInterval() != null) {
            builder.metricsInterval(relay.metricsInterval());
        }
        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean
    TopicRouter tandemTopicRouter(TandemKafkaProperties kafka) {
        return TopicRouter.kebabWithSuffix(kafka.topicSuffix());
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxDispatcher tandemOutboxDispatcher(TandemKafkaProperties kafka, TopicRouter topicRouter) {
        Map<String, Object> producerConfig = new HashMap<>(kafka.producer());
        KafkaRelayConfig kafkaConfig =
                new KafkaRelayConfig(kafka.source(), kafka.defaultContentType(), kafka.defaultDataSchema());
        return new KafkaRelay(producerConfig, topicRouter, kafkaConfig);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxStore tandemOutboxStore(DataSource dataSource, RelayConfig relayConfig) {
        return new JdbcOutboxStore(dataSource, relayConfig.maxAttempts());
    }

    @Bean
    @ConditionalOnMissingBean
    TandemMetrics tandemMetrics() {
        return TandemMetrics.NOOP;
    }

    @Bean
    @ConditionalOnMissingBean
    BucketSource tandemBucketSource(RelayConfig relayConfig, DataSource dataSource) {
        return BucketSource.forCoordination(relayConfig, dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    WorkerPool tandemWorkerPool(OutboxStore outboxStore, OutboxDispatcher outboxDispatcher, RelayConfig relayConfig,
            TandemMetrics tandemMetrics, BucketSource bucketSource) {
        return new WorkerPool(outboxStore, outboxDispatcher, relayConfig, tandemMetrics, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), bucketSource);
    }

    @Bean
    @ConditionalOnMissingBean
    RelayLifecycle tandemRelayLifecycle(WorkerPool workerPool, DataSource dataSource, TandemOutboxProperties outbox) {
        // Return the concrete type, not SmartLifecycle: @ConditionalOnMissingBean on the SmartLifecycle
        // interface would back off whenever the application has any other lifecycle bean (a real Boot app
        // has several), silently leaving the relay unstarted.
        return new RelayLifecycle(workerPool, dataSource, outbox.bucketCount());
    }
}
