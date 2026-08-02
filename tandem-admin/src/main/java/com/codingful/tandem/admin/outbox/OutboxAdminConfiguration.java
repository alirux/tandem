package com.codingful.tandem.admin.outbox;

import com.codingful.tandem.core.port.OutboxQuery;
import com.codingful.tandem.core.port.OutboxStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Wires the outbox feature (slice 1: reads, HLD-admin-api §2) — the use case, its REST driving
 * adapter, and its own RFC 9457 mapping. {@code @Import}ed by {@code TandemAdminAutoConfiguration}
 * rather than auto-discovered on its own, so it inherits that class's gating
 * ({@code tandem.admin.enabled} + a single {@code DataSource} candidate): a {@code @Configuration}
 * class {@code @Import}ed from a class whose own conditions fail is never processed, so none of
 * these beans are registered either. Takes {@link OutboxQuery}/{@link OutboxStore}/
 * {@link ObjectMapper} as inputs — the root autoconfiguration owns those, since a future feature
 * package (e.g. relay control) may need to share them too.
 */
@Configuration
@Import(OutboxExceptionHandler.class)
public class OutboxAdminConfiguration {

    @Bean
    @ConditionalOnMissingBean
    OutboxAdminService outboxAdminService(OutboxQuery outboxQuery, OutboxStore outboxStore, ObjectMapper objectMapper) {
        return new OutboxAdminService(outboxQuery, outboxStore, objectMapper, Clock.systemUTC());
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxAdminController outboxAdminController(OutboxAdminService outboxAdminService) {
        return new OutboxAdminController(outboxAdminService);
    }
}
