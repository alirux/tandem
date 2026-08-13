package com.codingful.tandem.admin;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.port.OutboxQuery;
import com.codingful.tandem.core.port.OutboxStore;
import java.sql.Connection;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.AbstractDataSource;

/**
 * The direct regression test for the defect this source set exists to catch: on a stock Boot 4
 * classpath the admin context did not start at all. Spring introspects a whole {@code @Configuration}
 * class to build its bean definitions, so a single {@code ObjectMapper} in one bean signature made
 * every bean in that file fail with {@code NoClassDefFoundError} — including beans that have nothing
 * to do with JSON.
 */
class AdminWiringOnJacksonThreeTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations
                    .of(TandemAdminAutoConfiguration.class));

    @Test
    void GIVEN_a_stock_boot_four_application_WHEN_the_admin_api_is_enabled_THEN_the_context_starts_and_is_wired() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withPropertyValues("tandem.admin.enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(OutboxQuery.class);
                    assertThat(context).hasSingleBean(OutboxStore.class);
                    assertThat(context).hasSingleBean(TandemAdminExceptionHandler.class);
                });
    }

    @Test
    void GIVEN_a_stock_boot_four_application_WHEN_the_admin_api_is_not_enabled_THEN_nothing_is_contributed() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(OutboxQuery.class);
                });
    }

    /** A real {@link DataSource} that is never connected to — only its presence matters here. */
    private static final class NoopDataSource extends AbstractDataSource {
        @Override
        public Connection getConnection() {
            throw new UnsupportedOperationException("NoopDataSource must not be connected to in a wiring test");
        }

        @Override
        public Connection getConnection(String username, String password) {
            return getConnection();
        }
    }
}
