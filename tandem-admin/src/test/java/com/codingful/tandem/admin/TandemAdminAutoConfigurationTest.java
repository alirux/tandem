package com.codingful.tandem.admin;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.port.OutboxQuery;
import com.codingful.tandem.core.port.OutboxStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.web.bind.annotation.RestController;

/**
 * Wiring tests for the Admin API's root autoconfiguration (HLD-admin-api §3/§4): off by default,
 * gated on a single {@code DataSource} candidate, and every infra bean replaceable — no Docker, so
 * these run on both Spring generations (bootFourTest). Asserts only on this class's own
 * responsibility — the cross-cutting infra beans and that gating cascades to whatever feature
 * packages are {@code @Import}ed — never on a specific feature's own types (those are package-private
 * in their own package on purpose; see e.g. {@code outbox.OutboxAdminConfigurationTest}).
 */
class TandemAdminAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TandemAdminAutoConfiguration.class));

    @Test
    void GIVEN_the_property_unset_WHEN_the_context_starts_THEN_nothing_is_contributed() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .run(context -> assertThat(context).doesNotHaveBean(OutboxQuery.class));
    }

    @Test
    void GIVEN_the_admin_api_explicitly_disabled_WHEN_the_context_starts_THEN_nothing_is_contributed() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withPropertyValues("tandem.admin.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(OutboxQuery.class));
    }

    @Test
    void GIVEN_no_datasource_WHEN_the_context_starts_THEN_nothing_is_contributed_even_if_enabled() {
        runner.withPropertyValues("tandem.admin.enabled=true")
                .run(context -> assertThat(context).doesNotHaveBean(OutboxQuery.class));
    }

    @Test
    void GIVEN_a_datasource_and_enabled_true_WHEN_the_context_starts_THEN_the_infra_beans_are_wired() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withPropertyValues("tandem.admin.enabled=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(OutboxQuery.class);
                    assertThat(context).hasSingleBean(OutboxStore.class);
                    assertThat(context).hasSingleBean(ObjectMapper.class);
                    assertThat(context).hasSingleBean(TandemAdminExceptionHandler.class);
                });
    }

    /**
     * Proves the gating actually cascades into an {@code @Import}ed feature package — without naming
     * that feature's own (package-private) controller type, which this test has no business knowing.
     */
    @Test
    void GIVEN_a_datasource_and_enabled_true_WHEN_the_context_starts_THEN_a_feature_controller_is_wired() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withPropertyValues("tandem.admin.enabled=true")
                .run(context -> assertThat(context.getBeanNamesForAnnotation(RestController.class)).isNotEmpty());
    }

    @Test
    void GIVEN_an_existing_outbox_query_bean_WHEN_the_context_starts_THEN_the_autoconfiguration_reuses_it() {
        OutboxQuery custom = new NoopOutboxQuery();
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withBean(OutboxQuery.class, () -> custom)
                .withPropertyValues("tandem.admin.enabled=true")
                .run(context -> assertThat(context.getBean(OutboxQuery.class)).isSameAs(custom));
    }
}
