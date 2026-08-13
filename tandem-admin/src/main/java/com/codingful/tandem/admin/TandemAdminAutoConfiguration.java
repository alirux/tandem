package com.codingful.tandem.admin;

import com.codingful.tandem.admin.outbox.OutboxAdminConfiguration;
import com.codingful.tandem.admin.relay.RelayAdminConfiguration;
import com.codingful.tandem.core.port.OutboxQuery;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.jdbc.JdbcOutboxQuery;
import com.codingful.tandem.jdbc.JdbcOutboxStore;
import com.codingful.tandem.jdbc.RelayConfig;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * Admin API autoconfiguration (HLD-admin-api §3/§4): off by default — {@code tandem.admin.enabled}
 * must be set {@code true} — and gated on a single {@code DataSource} candidate, since every slice-1
 * operation is DB-only (HLD-admin-api §4.1). Works whether embedded in the client's own Spring
 * application (reusing its {@code OutboxQuery}/{@code OutboxStore} beans if {@code tandem-spring-relay}
 * already supplies them) or run as a fully standalone service pointed at the outbox datasource — every
 * bean is {@link ConditionalOnMissingBean}, so an embedded deployment's own beans win.
 *
 * <p>Owns only cross-cutting infrastructure — the DB-derived adapters more than one feature package
 * needs — plus generic error handling. Each REST feature's own use cases/controller/
 * problem-slug mapping live in their own package ({@code outbox}, {@code relay}), {@code @Import}ed
 * here so they inherit this class's gating: a {@code @Configuration} imported from a class whose own
 * conditions fail is never processed, so an imported feature's beans never register either.
 *
 * <p>The ordering is declared by <b>name</b> for both Spring generations: Boot 4 moved
 * {@code DataSourceAutoConfiguration} into {@code spring-boot-jdbc}, so a class literal would name a
 * type absent there and the ordering would be silently lost (LLD-spring-config §1.1).
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",   // Spring Boot 3.x
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration"})  // Spring Boot 4.x
@ConditionalOnSingleCandidate(DataSource.class)
@ConditionalOnProperty(prefix = "tandem.admin", name = "enabled", matchIfMissing = false)
@Import({TandemAdminExceptionHandler.class, OutboxAdminConfiguration.class, RelayAdminConfiguration.class})
public class TandemAdminAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    OutboxQuery tandemOutboxQuery(DataSource dataSource) {
        return new JdbcOutboxQuery(dataSource);
    }

    /**
     * Only {@link OutboxStore#lag()} is exercised by the outbox feature today (for its summary
     * endpoint) — the {@code maxAttempts} value is otherwise irrelevant to this module, so it is
     * sourced from {@link RelayConfig}'s own default rather than a magic number.
     */
    @Bean
    @ConditionalOnMissingBean
    OutboxStore tandemOutboxStoreForAdmin(DataSource dataSource) {
        return new JdbcOutboxStore(dataSource, RelayConfig.builder().build().maxAttempts());
    }

}
