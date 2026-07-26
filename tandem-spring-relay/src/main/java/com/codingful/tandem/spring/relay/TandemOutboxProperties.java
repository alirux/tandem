package com.codingful.tandem.spring.relay;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Binds {@code tandem.outbox.*} — the values the write-side and the relay must agree on
 * (LLD-spring-config §2.1). The relay binds it independently of {@code tandem-spring-producer} (the two
 * modules never share code); the bucket-count guard fails fast if the two sides diverge.
 *
 * @param bucketCount number of virtual buckets; the single value the write side and relay must share,
 *                    bound identically by both modules, and it must never change after first deploy
 */
@ConfigurationProperties("tandem.outbox")
public record TandemOutboxProperties(@DefaultValue("256") int bucketCount) {
}
