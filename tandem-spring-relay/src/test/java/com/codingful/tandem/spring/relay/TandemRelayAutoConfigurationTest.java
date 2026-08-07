package com.codingful.tandem.spring.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.core.port.TopicRouter;
import com.codingful.tandem.jdbc.BucketSource;
import com.codingful.tandem.jdbc.JdbcRelayControlSource;
import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.RelayControlSource;
import com.codingful.tandem.jdbc.WorkerPool;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wiring tests for the relay autoconfiguration (LLD-spring-config §4.4) that need no Docker, so they run
 * on both Spring generations (§1.2). They cover the engine the module assembles and the {@code tandem.*}
 * keys it binds onto it, plus the cases where it must back off entirely. Only the relay's actual delivery
 * — and the lifecycle really starting the pool — stays in the integration test, which needs a database
 * and a broker.
 */
class TandemRelayAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TandemRelayAutoConfiguration.class));

    /**
     * The wired engine, with the pool left unstarted. The autoconfigured {@link RelayLifecycle} would
     * open the database at context refresh (the bucket-count guard, then the pool), which is the
     * integration test's job — so this replaces just that one bean, and everything else is the real
     * autoconfigured graph, Kafka producer included.
     */
    private ApplicationContextRunner wiredRelay() {
        return runner.withBean(DataSource.class, NoopDataSource::new)
                .withUserConfiguration(UnstartedLifecycle.class)
                .withPropertyValues(
                        "tandem.kafka.source=/tandem/test",
                        "tandem.kafka.producer[bootstrap.servers]=localhost:9092");
    }

    @Test
    void GIVEN_a_datasource_and_the_kafka_settings_WHEN_the_context_starts_THEN_the_whole_engine_is_wired() {
        wiredRelay().run(context -> {
            assertThat(context).hasSingleBean(TopicRouter.class);
            assertThat(context).hasSingleBean(OutboxDispatcher.class);
            assertThat(context).hasSingleBean(OutboxStore.class);
            assertThat(context).hasSingleBean(BucketSource.class);
            assertThat(context).hasSingleBean(WorkerPool.class);
            assertThat(context).hasSingleBean(RelayConfig.class);
            assertThat(context).hasSingleBean(RelayControlSource.class);
            assertThat(context.getBean(RelayControlSource.class)).isInstanceOf(JdbcRelayControlSource.class);
            assertThat(context.getBean(TandemMetrics.class)).isSameAs(TandemMetrics.NOOP);
        });
    }

    @Test
    void GIVEN_relay_settings_in_the_configuration_WHEN_the_context_starts_THEN_they_reach_the_engine() {
        wiredRelay().withPropertyValues(
                        "tandem.outbox.bucket-count=512",
                        "tandem.relay.batch-size=55",
                        "tandem.relay.row-lease=90s",
                        "tandem.relay.metrics-interval=33s")
                .run(context -> {
                    RelayConfig config = context.getBean(RelayConfig.class);
                    assertThat(config.bucketCount()).isEqualTo(512);
                    assertThat(config.batchSize()).isEqualTo(55);
                    assertThat(config.rowLease()).isEqualTo(Duration.ofSeconds(90));
                    assertThat(config.metricsInterval()).isEqualTo(Duration.ofSeconds(33));
                    // Unset keys keep RelayConfig's own defaults — the properties never shadow them.
                    assertThat(config.maxAttempts()).isEqualTo(RelayConfig.defaults().maxAttempts());
                });
    }

    @Test
    void GIVEN_an_application_running_a_tracer_WHEN_the_publish_span_is_asked_for_THEN_the_relay_emits_it() {
        wiredRelay().withBean(Propagator.class, TandemRelayAutoConfigurationTest::w3cPropagator)
                .withPropertyValues("tandem.tracing.publish-span=true")
                .run(context -> assertThat(context.getBean(TandemSpanRecorder.class))
                        .isInstanceOf(MicrometerTandemSpanRecorder.class));
    }

    @Test
    void GIVEN_an_application_running_a_tracer_WHEN_the_publish_span_is_not_asked_for_THEN_no_span_is_emitted() {
        wiredRelay().withBean(Propagator.class, TandemRelayAutoConfigurationTest::w3cPropagator)
                .run(context -> assertThat(context).doesNotHaveBean(TandemSpanRecorder.class));
    }

    @Test
    void GIVEN_no_tracing_at_all_WHEN_the_publish_span_is_asked_for_THEN_the_relay_still_starts() {
        wiredRelay().withPropertyValues("tandem.tracing.publish-span=true")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(TandemSpanRecorder.class);
                    assertThat(context).hasSingleBean(OutboxDispatcher.class);
                });
    }

    /**
     * With no tracing library available the relay must still start, simply without span emission. Same
     * limit as the producer module's equivalent test: {@link FilteredClassLoader} only makes the classpath
     * <i>checks</i> fail, so this pins the conditional back-off and not the erased-signature trap that
     * breaks context creation outright — {@code tandem-sample-spring}'s smoke integration test, on a
     * classpath genuinely without the library, is what catches that.
     */
    @Test
    void GIVEN_an_application_without_any_tracing_library_WHEN_the_context_starts_THEN_the_relay_still_starts() {
        wiredRelay().withClassLoader(new FilteredClassLoader(Propagator.class))
                .withPropertyValues("tandem.tracing.publish-span=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(WorkerPool.class);
                });
    }

    private static Propagator w3cPropagator() {
        return new OtelPropagator(ContextPropagators.create(W3CTraceContextPropagator.getInstance()),
                SdkTracerProvider.builder().build().get("tandem-relay"));
    }

    @Test
    void GIVEN_a_custom_topic_router_WHEN_the_context_starts_THEN_it_replaces_the_autoconfigured_one() {
        TopicRouter custom = record -> "fixed-topic";

        wiredRelay().withBean(TopicRouter.class, () -> custom)
                .run(context -> assertThat(context.getBean(TopicRouter.class)).isSameAs(custom));
    }

    @Test
    void GIVEN_no_datasource_WHEN_the_context_starts_THEN_no_relay_is_contributed() {
        runner.withPropertyValues("tandem.kafka.source=/tandem/test")
                .run(context -> assertThat(context).doesNotHaveBean(WorkerPool.class));
    }

    @Test
    void GIVEN_the_relay_is_disabled_WHEN_the_context_starts_THEN_no_relay_is_contributed() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withPropertyValues("tandem.relay.enabled=false", "tandem.kafka.source=/tandem/test")
                .run(context -> assertThat(context).doesNotHaveBean(WorkerPool.class));
    }

    /**
     * The CloudEvents source is the one setting with no default (LLD-spring-config §2.3), so its absence
     * must name the key the operator has to set — not surface as a NullPointerException from deep inside
     * the CloudEvents configuration.
     */
    @Test
    void GIVEN_no_cloudevents_source_WHEN_the_context_starts_THEN_it_fails_naming_the_missing_setting() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .run(context -> assertThat(context).getFailure()
                        .rootCause()
                        .isInstanceOf(TandemConfigurationException.class)
                        .hasMessageContaining("tandem.kafka.source"));
    }

    /**
     * The autoconfigured lifecycle, with {@code start}/{@code stop} suppressed: a real one would connect
     * to the database at context refresh. Everything the wiring tests assert is built by the
     * autoconfiguration itself — only the starting of the pool is out of scope here.
     */
    @Configuration(proxyBeanMethods = false)
    static class UnstartedLifecycle {

        @Bean
        RelayLifecycle tandemRelayLifecycle(WorkerPool workerPool, DataSource dataSource,
                TandemOutboxProperties outbox) {
            return new RelayLifecycle(workerPool, dataSource, outbox.bucketCount()) {
                @Override
                public void start() {
                }

                @Override
                public void stop() {
                }
            };
        }
    }
}
