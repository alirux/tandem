package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.core.port.PayloadSerializer;
import com.codingful.tandem.jdbc.BucketCountGuard;
import com.codingful.tandem.jdbc.JdbcOutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.sql.DataSource;
import org.aspectj.lang.ProceedingJoinPoint;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Write-side autoconfiguration (LLD-spring-config §4.3): contributes the {@link OutboxRepository} the
 * application uses to insert outbox rows inside its own transaction. Declared {@code after} Spring
 * Boot's {@link DataSourceAutoConfiguration} so the application's {@link DataSource} is already defined,
 * and gated on a single {@code DataSource} candidate ({@link ConditionalOnSingleCandidate}) — it resolves
 * the {@code @Primary} one when several exist and backs off, rather than guessing, when the choice is
 * ambiguous. This module never pulls Kafka.
 */
@AutoConfiguration(after = {DataSourceAutoConfiguration.class, TransactionAutoConfiguration.class})
@ConditionalOnSingleCandidate(DataSource.class)
@EnableConfigurationProperties(TandemOutboxProperties.class)
public class TandemProducerAutoConfiguration {

    /**
     * The write-side repository, backed by JDBC. The bucket-count guard runs first, so a bucket count
     * that diverges from what the database already holds fails context refresh (LLD-spring-config §3)
     * instead of silently inserting into buckets the relay never polls.
     */
    @Bean
    @ConditionalOnMissingBean
    OutboxRepository tandemOutboxRepository(DataSource dataSource, TandemOutboxProperties properties) {
        BucketCountGuard.check(dataSource, properties.bucketCount());
        // Wrap in a transaction-aware proxy so the insert joins the application's Spring transaction —
        // the JdbcOutboxRepository takes whatever connection the DataSource hands it, and this proxy hands
        // it the one bound to the active transaction. Without it, the insert would run on a separate
        // autocommitted connection and lose atomicity with the business state change.
        return new JdbcOutboxRepository(new TransactionAwareDataSourceProxy(dataSource), properties.bucketCount());
    }

    /**
     * The Spring application-events tier (LLD-spring-producer §5). The registry is built from the
     * registered {@link OutboxEventMapper} beans (empty is fine — the listener still handles a directly
     * published {@code OutboxMessage}); the listener is scoped to those handleable types.
     */
    @Bean
    @ConditionalOnMissingBean
    OutboxEventMapperRegistry tandemOutboxEventMapperRegistry(ObjectProvider<OutboxEventMapper<?>> mappers) {
        return OutboxEventMapperRegistry.of(mappers.stream().toList());
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxEventListener tandemOutboxEventListener(OutboxRepository outboxRepository,
            OutboxEventMapperRegistry mapperRegistry) {
        return new OutboxEventListener(outboxRepository, mapperRegistry);
    }

    /**
     * The Template tier (LLD-spring-producer §3). Contributed only when a {@link PlatformTransactionManager}
     * exists — the template owns its transaction — so the plain repository tier still works without one.
     * The optional {@link PayloadSerializer} enables object payloads; absent, only {@code add(...)} works.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(PlatformTransactionManager.class)
    TransactionalOutboxTemplate tandemTransactionalOutboxTemplate(OutboxRepository outboxRepository,
            PlatformTransactionManager transactionManager, ObjectProvider<PayloadSerializer> payloadSerializer) {
        return new DefaultTransactionalOutboxTemplate(
                outboxRepository, new TransactionTemplate(transactionManager), payloadSerializer.getIfAvailable());
    }

    /**
     * The optional Jackson {@link PayloadSerializer} (LLD-spring-producer §2), contributed only when
     * Jackson is on the classpath and never forced. Reuses the application's {@code ObjectMapper} bean
     * when one exists, otherwise a plain default.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(ObjectMapper.class)
    static class JacksonPayloadSerializerConfiguration {

        @Bean
        @ConditionalOnMissingBean(PayloadSerializer.class)
        PayloadSerializer tandemPayloadSerializer(ObjectProvider<ObjectMapper> objectMapper) {
            return new JacksonPayloadSerializer(objectMapper.getIfAvailable(ObjectMapper::new));
        }
    }

    /**
     * The {@link TransactionalOutbox} annotation tier (LLD-spring-producer §4), contributed only when
     * AspectJ is on the classpath. Spring Boot's AOP autoconfiguration enables the aspect proxying that
     * makes the advice apply.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(ProceedingJoinPoint.class)
    static class TransactionalOutboxAspectConfiguration {

        @Bean
        @ConditionalOnMissingBean
        TransactionalOutboxAspect tandemTransactionalOutboxAspect(OutboxRepository outboxRepository) {
            return new TransactionalOutboxAspect(outboxRepository);
        }
    }
}
