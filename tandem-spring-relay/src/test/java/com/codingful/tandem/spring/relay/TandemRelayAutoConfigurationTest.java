package com.codingful.tandem.spring.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.jdbc.WorkerPool;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Conditional wiring tests for the relay autoconfiguration (LLD-spring-config §4.4) that need no Docker:
 * they assert the relay backs off entirely when it should, which is exactly when nothing starts and no
 * database is touched. The positive case — the autoconfigured relay actually delivers — is the
 * integration test, since a started relay opens the database and Kafka.
 */
class TandemRelayAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TandemRelayAutoConfiguration.class));

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
}
