package com.codingful.tandem.admin.relay;

import com.codingful.tandem.core.port.RelayControl;
import com.codingful.tandem.core.port.RelayQuery;
import com.codingful.tandem.jdbc.JdbcRelayControl;
import com.codingful.tandem.jdbc.JdbcRelayQuery;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Wires the relay-control feature (slice 3, HLD-admin-api §2). {@code @Import}ed by
 * {@code TandemAdminAutoConfiguration} rather than auto-discovered on its own, so it inherits that
 * class's gating ({@code tandem.admin.enabled} + a single {@code DataSource} candidate). {@link RelayQuery}/
 * {@link RelayControl} are wired here, feature-local, since no other feature package needs them —
 * unlike {@code OutboxQuery}/{@code OutboxStore}, which the root autoconfiguration owns
 * (IMPLEMENTATION-PLAN-admin-api.md §8.3's placement rule, applied to this feature too).
 */
@Configuration
@Import(RelayExceptionHandler.class)
public class RelayAdminConfiguration {

    @Bean
    @ConditionalOnMissingBean
    RelayQuery tandemRelayQueryForAdmin(DataSource dataSource) {
        return new JdbcRelayQuery(dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    RelayControl tandemRelayControlForAdmin(DataSource dataSource) {
        return new JdbcRelayControl(dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    RelayAdminService relayAdminService(RelayQuery relayQuery, RelayControl relayControl) {
        return new RelayAdminService(relayQuery, relayControl);
    }

    @Bean
    @ConditionalOnMissingBean
    RelayAdminController relayAdminController(RelayAdminService relayAdminService) {
        return new RelayAdminController(relayAdminService);
    }
}
