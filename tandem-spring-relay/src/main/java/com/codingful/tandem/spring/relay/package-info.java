/**
 * Spring Boot autoconfiguration for the Tandem relay (LLD-spring-config §4.4): the engine that polls the
 * outbox and publishes CloudEvents to Kafka. Split from the write-side module so the client can avoid
 * Kafka (LLD-spring-config §1); this module depends on both {@code tandem-jdbc} and {@code tandem-kafka}.
 */
package com.codingful.tandem.spring.relay;
