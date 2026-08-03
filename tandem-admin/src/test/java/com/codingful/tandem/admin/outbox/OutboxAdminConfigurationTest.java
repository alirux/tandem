package com.codingful.tandem.admin.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.admin.TandemAdminObjectMappers;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxRowDetail;
import com.codingful.tandem.core.OutboxRowView;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.ReplayResult;
import com.codingful.tandem.core.port.DiscardService;
import com.codingful.tandem.core.port.OutboxQuery;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.ReplayService;
import com.codingful.tandem.jdbc.JdbcDiscardService;
import com.codingful.tandem.jdbc.JdbcReplayService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.AbstractDataSource;

/**
 * Wiring test for the outbox feature's own configuration, in isolation from the root
 * autoconfiguration's gating (that belongs to {@code TandemAdminAutoConfigurationTest}). Supplies the
 * infra beans {@link OutboxAdminConfiguration} depends on directly, via two single-purpose stubs
 * ({@link PlainOutboxQuery}/{@link PlainOutboxStore}) rather than one object implementing both ports —
 * registering the same {@code InMemoryOutbox} instance under both bean types made it ambiguous which
 * one satisfied an {@code OutboxQuery} dependency, since Spring's by-type autowiring inspects the
 * bean's actual class, not just the type it was registered under.
 */
class OutboxAdminConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(OutboxAdminConfiguration.class);

    private ApplicationContextRunner withInfraBeans() {
        return runner
                .withBean(OutboxQuery.class, PlainOutboxQuery::new)
                .withBean(OutboxStore.class, PlainOutboxStore::new)
                .withBean(ReplayService.class, PlainReplayService::new)
                .withBean(DiscardService.class, PlainDiscardService::new)
                .withBean(ObjectMapper.class, TandemAdminObjectMappers::newDefault);
    }

    @Test
    void GIVEN_the_infra_beans_are_present_WHEN_the_context_starts_THEN_the_outbox_feature_is_fully_wired() {
        withInfraBeans().run(context -> {
            assertThat(context).hasSingleBean(OutboxAdminService.class);
            assertThat(context).hasSingleBean(OutboxAdminController.class);
            assertThat(context).hasSingleBean(OutboxExceptionHandler.class);
        });
    }

    @Test
    void GIVEN_no_outbox_query_bean_WHEN_the_context_starts_THEN_it_fails_to_start() {
        runner.withBean(OutboxStore.class, PlainOutboxStore::new)
                .withBean(ReplayService.class, PlainReplayService::new)
                .withBean(DiscardService.class, PlainDiscardService::new)
                .withBean(ObjectMapper.class, TandemAdminObjectMappers::newDefault)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void GIVEN_a_data_source_but_no_explicit_replay_or_discard_beans_WHEN_the_context_starts_THEN_jdbc_backed_ones_are_wired() {
        runner.withBean(OutboxQuery.class, PlainOutboxQuery::new)
                .withBean(OutboxStore.class, PlainOutboxStore::new)
                .withBean(ObjectMapper.class, TandemAdminObjectMappers::newDefault)
                .withBean(DataSource.class, NoopDataSource::new)
                .run(context -> {
                    assertThat(context).hasSingleBean(ReplayService.class);
                    assertThat(context).hasSingleBean(DiscardService.class);
                    assertThat(context.getBean(ReplayService.class)).isInstanceOf(JdbcReplayService.class);
                    assertThat(context.getBean(DiscardService.class)).isInstanceOf(JdbcDiscardService.class);
                });
    }

    @Test
    void GIVEN_an_existing_outbox_admin_service_bean_WHEN_the_context_starts_THEN_the_configuration_reuses_it() {
        OutboxAdminService custom = new OutboxAdminService(
                new PlainOutboxQuery(), new PlainOutboxStore(), new PlainReplayService(), new PlainDiscardService(),
                TandemAdminObjectMappers.newDefault(), Clock.systemUTC());
        withInfraBeans().withBean(OutboxAdminService.class, () -> custom)
                .run(context -> assertThat(context.getBean(OutboxAdminService.class)).isSameAs(custom));
    }

    /** Never called — this test only proves bean wiring, not behaviour. */
    private static final class PlainOutboxQuery implements OutboxQuery {
        @Override
        public Map<OutboxStatus, Long> statusCounts() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<OutboxRowView> search(OutboxSearchCriteria criteria) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<OutboxRowDetail> findById(long id) {
            throw new UnsupportedOperationException();
        }
    }

    /** Never called — this test only proves bean wiring, not behaviour. */
    private static final class PlainOutboxStore implements OutboxStore {
        @Override
        public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markDone(long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markForRetry(long id, String error, Duration retryDelay) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markFailed(long id, String error) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int reclaimExpiredLeases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int cleanup(Instant doneBefore, int batchSize) {
            throw new UnsupportedOperationException();
        }
    }

    /** Never called — this test only proves bean wiring, not behaviour. */
    private static final class PlainReplayService implements ReplayService {
        @Override
        public ReplayResult replay(ReplayCriteria criteria) {
            throw new UnsupportedOperationException();
        }
    }

    /** Never called — this test only proves bean wiring, not behaviour. */
    private static final class PlainDiscardService implements DiscardService {
        @Override
        public boolean discard(long id, String reason) {
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
