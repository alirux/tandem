package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.core.port.PayloadSerializer;
import com.codingful.tandem.core.port.TracePropagator;
import com.codingful.tandem.test.InMemoryOutbox;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Wiring tests for the write-side autoconfiguration (LLD-spring-config §4.3): no database is touched —
 * a {@link NoopDataSource} satisfies the presence condition and an {@link InMemoryOutbox} stands in as
 * the real write-side, so the JDBC bean (and its bucket-count guard) never runs. The real JDBC path is
 * covered by the integration tests.
 */
class TandemProducerAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TandemProducerAutoConfiguration.class));

    @Test
    void GIVEN_no_datasource_WHEN_the_context_starts_THEN_no_write_side_is_contributed() {
        runner.run(context -> assertThat(context).doesNotHaveBean(OutboxRepository.class));
    }

    @Test
    void GIVEN_a_user_supplied_repository_WHEN_the_context_starts_THEN_it_replaces_the_autoconfigured_one() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .run(context -> assertThat(context.getBean(OutboxRepository.class)).isInstanceOf(InMemoryOutbox.class));
    }

    @Test
    void GIVEN_a_configured_bucket_count_WHEN_the_context_starts_THEN_it_is_bound() {
        int buckets = 512;
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .withPropertyValues("tandem.outbox.bucket-count=" + buckets)
                .run(context -> assertThat(context.getBean(TandemOutboxProperties.class).bucketCount())
                        .isEqualTo(buckets));
    }

    @Test
    void GIVEN_no_bucket_count_property_WHEN_the_context_starts_THEN_the_contract_default_applies() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .run(context -> assertThat(context.getBean(TandemOutboxProperties.class).bucketCount())
                        .isEqualTo(256));
    }

    @Test
    void GIVEN_jackson_on_the_classpath_WHEN_the_context_starts_THEN_a_jackson_serializer_is_contributed() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .run(context -> assertThat(context.getBean(PayloadSerializer.class))
                        .isInstanceOf(JacksonPayloadSerializer.class));
    }

    @Test
    void GIVEN_a_user_supplied_serializer_WHEN_the_context_starts_THEN_it_replaces_the_autoconfigured_one() {
        PayloadSerializer custom = new PayloadSerializer() {
            @Override
            public byte[] serialize(Object payload) {
                return new byte[0];
            }

            @Override
            public String contentType() {
                return "application/x-custom";
            }
        };
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .withBean(PayloadSerializer.class, () -> custom)
                .run(context -> assertThat(context.getBean(PayloadSerializer.class)).isSameAs(custom));
    }

    @Test
    void GIVEN_a_transaction_manager_WHEN_the_context_starts_THEN_the_template_is_contributed() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .withBean(PlatformTransactionManager.class, () -> new DataSourceTransactionManager(new NoopDataSource()))
                .run(context -> assertThat(context).hasSingleBean(TransactionalOutboxTemplate.class));
    }

    @Test
    void GIVEN_no_transaction_manager_WHEN_the_context_starts_THEN_no_template_is_contributed() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .run(context -> assertThat(context).doesNotHaveBean(TransactionalOutboxTemplate.class));
    }

    @Test
    void GIVEN_aspectj_on_the_classpath_WHEN_the_context_starts_THEN_the_annotation_aspect_is_contributed() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .run(context -> assertThat(context).hasSingleBean(TransactionalOutboxAspect.class));
    }

    @Test
    void GIVEN_the_write_side_WHEN_the_context_starts_THEN_the_events_tier_is_contributed() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .run(context -> {
                    assertThat(context).hasSingleBean(OutboxEventListener.class);
                    assertThat(context).hasSingleBean(OutboxEventMapperRegistry.class);
                });
    }

    @Test
    void GIVEN_tracing_not_enabled_WHEN_the_context_starts_THEN_no_trace_propagator_is_contributed() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .run(context -> assertThat(context).doesNotHaveBean(TracePropagator.class));
    }

    @Test
    void GIVEN_tracing_enabled_WHEN_the_context_starts_THEN_an_mdc_correlation_propagator_is_contributed() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .withPropertyValues("tandem.tracing.enabled=true")
                .run(context -> assertThat(context.getBean(TracePropagator.class))
                        .isInstanceOf(MdcCorrelationTracePropagator.class));
    }

    @Test
    void GIVEN_a_configured_mdc_key_WHEN_the_context_starts_THEN_it_is_bound() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .withPropertyValues("tandem.tracing.enabled=true", "tandem.tracing.correlation-id-mdc-key=reqId")
                .run(context -> assertThat(context.getBean(TandemTracingProperties.class).correlationIdMdcKey())
                        .isEqualTo("reqId"));
    }

    @Test
    void GIVEN_a_user_supplied_trace_propagator_WHEN_tracing_is_enabled_THEN_it_replaces_the_autoconfigured_one() {
        TracePropagator custom = new TracePropagator() {
        };
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .withBean(TracePropagator.class, () -> custom)
                .withPropertyValues("tandem.tracing.enabled=true")
                .run(context -> assertThat(context.getBean(TracePropagator.class)).isSameAs(custom));
    }
}
