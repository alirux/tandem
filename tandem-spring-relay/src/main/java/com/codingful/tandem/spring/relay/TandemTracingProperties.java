package com.codingful.tandem.spring.relay;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Binds the relay's share of {@code tandem.tracing.*} (HLD-tracing.md §6/§9). Separate from the write
 * side's own {@code tandem.tracing.enabled}: under the split topology the relay is usually a different
 * process configured on its own, and the two decisions are genuinely independent — propagation costs a
 * header on the row, an emitted span costs export volume in the tracing backend.
 *
 * @param publishSpan emit the {@code tandem.relay.publish} span per delivered record; off by default,
 *                    and explicit only — never enabled by a tracing library's mere presence (§9)
 */
@ConfigurationProperties("tandem.tracing")
public record TandemTracingProperties(@DefaultValue("false") boolean publishSpan) {
}
