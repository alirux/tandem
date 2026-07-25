/**
 * Spring Boot autoconfiguration and convenience tiers for Tandem's write-side (the outbox INSERT),
 * split by role so the client never pulls Kafka (LLD-spring-config §1, LLD-spring-producer). Provides
 * the {@code OutboxRepository} bean plus the optional Template, {@code @TransactionalOutbox} annotation,
 * and Spring application-events tiers on top of it.
 */
package com.codingful.tandem.spring.producer;
