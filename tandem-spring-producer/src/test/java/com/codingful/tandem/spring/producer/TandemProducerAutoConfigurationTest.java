package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.test.InMemoryOutbox;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

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
}
