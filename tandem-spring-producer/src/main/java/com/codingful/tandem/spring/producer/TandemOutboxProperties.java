package com.codingful.tandem.spring.producer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Binds {@code tandem.outbox.*} — the values the write-side and the relay must agree on
 * (LLD-spring-config §2.1). The bucket count is the one value both sides must share and it must never
 * change after first deployment, because it is baked into every stored row's {@code bucket}; the
 * cross-module guard (LLD-bucket-count-guard) makes a mismatch fail fast rather than stall delivery
 * silently.
 *
 * @param bucketCount number of virtual buckets; the single value the write-side and relay must share,
 *                    bound identically by both modules, and it must never change after first deploy
 */
@ConfigurationProperties("tandem.outbox")
public record TandemOutboxProperties(@DefaultValue("256") int bucketCount) {
}
