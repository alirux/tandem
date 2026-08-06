package com.codingful.tandem.spring.producer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Binds {@code tandem.tracing.*} (HLD-tracing.md §9). Trace/correlation capture is off unless
 * {@code enabled} is set explicitly — never auto-enabled by a tracing adapter's mere classpath
 * presence, so an unrelated dependency cannot silently start adding headers to outbox rows.
 *
 * @param enabled           trace/correlation capture at insert time; off by default
 * @param correlationIdMdcKey the MDC key read for the correlation id when {@code enabled}
 */
@ConfigurationProperties("tandem.tracing")
public record TandemTracingProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("correlationId") String correlationIdMdcKey) {
}
