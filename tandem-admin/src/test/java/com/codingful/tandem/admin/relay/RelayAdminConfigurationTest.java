package com.codingful.tandem.admin.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.BucketStatusView;
import com.codingful.tandem.core.RelayCoordinationMode;
import com.codingful.tandem.core.RelayStatusView;
import com.codingful.tandem.core.WorkerView;
import com.codingful.tandem.core.port.RelayControl;
import com.codingful.tandem.core.port.RelayQuery;
import com.codingful.tandem.jdbc.JdbcRelayControl;
import com.codingful.tandem.jdbc.JdbcRelayQuery;
import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.AbstractDataSource;

/**
 * Wiring test for the relay feature's own configuration, in isolation from the root autoconfiguration's
 * gating (that belongs to {@code TandemAdminAutoConfigurationTest}). Mirrors
 * {@code OutboxAdminConfigurationTest} — single-purpose stub beans, never one object registered under
 * two port types (the ambiguity that pattern found once already).
 */
class RelayAdminConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(RelayAdminConfiguration.class);

    private ApplicationContextRunner withInfraBeans() {
        return runner
                .withBean(RelayQuery.class, PlainRelayQuery::new)
                .withBean(RelayControl.class, PlainRelayControl::new);
    }

    @Test
    void GIVEN_the_infra_beans_are_present_WHEN_the_context_starts_THEN_the_relay_feature_is_fully_wired() {
        withInfraBeans().run(context -> {
            assertThat(context).hasSingleBean(RelayAdminService.class);
            assertThat(context).hasSingleBean(RelayAdminController.class);
            assertThat(context).hasSingleBean(RelayExceptionHandler.class);
        });
    }

    @Test
    void GIVEN_no_relay_query_bean_and_no_data_source_WHEN_the_context_starts_THEN_it_fails_to_start() {
        runner.withBean(RelayControl.class, PlainRelayControl::new)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void GIVEN_a_data_source_but_no_explicit_relay_beans_WHEN_the_context_starts_THEN_jdbc_backed_ones_are_wired() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .run(context -> {
                    assertThat(context).hasSingleBean(RelayQuery.class);
                    assertThat(context).hasSingleBean(RelayControl.class);
                    assertThat(context.getBean(RelayQuery.class)).isInstanceOf(JdbcRelayQuery.class);
                    assertThat(context.getBean(RelayControl.class)).isInstanceOf(JdbcRelayControl.class);
                });
    }

    @Test
    void GIVEN_an_existing_relay_admin_service_bean_WHEN_the_context_starts_THEN_the_configuration_reuses_it() {
        RelayAdminService custom = new RelayAdminService(new PlainRelayQuery(), new PlainRelayControl());
        withInfraBeans().withBean(RelayAdminService.class, () -> custom)
                .run(context -> assertThat(context.getBean(RelayAdminService.class)).isSameAs(custom));
    }

    /** Never called — this test only proves bean wiring, not behaviour. */
    private static final class PlainRelayQuery implements RelayQuery {
        @Override
        public RelayCoordinationMode coordinationMode() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RelayStatusView status() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<BucketStatusView> buckets(boolean uncoveredOnly) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<BucketStatusView> bucket(int bucket) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<WorkerView> workers() {
            throw new UnsupportedOperationException();
        }
    }

    /** Never called — this test only proves bean wiring, not behaviour. */
    private static final class PlainRelayControl implements RelayControl {
        @Override
        public void pauseAll() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resumeAll() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean pauseBucket(int bucket) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean resumeBucket(int bucket) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean releaseBucket(int bucket) {
            throw new UnsupportedOperationException();
        }
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
