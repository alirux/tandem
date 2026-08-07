package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.core.port.PayloadSerializer;
import com.codingful.tandem.core.port.TracePropagator;
import com.codingful.tandem.jdbc.BucketCountGuard;
import com.codingful.tandem.jdbc.JdbcOutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import javax.sql.DataSource;
import org.aspectj.lang.ProceedingJoinPoint;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Write-side autoconfiguration (LLD-spring-config §4.3): contributes the {@link OutboxRepository} the
 * application uses to insert outbox rows inside its own transaction. Ordered after Spring Boot's own
 * {@code DataSourceAutoConfiguration} and {@code TransactionAutoConfiguration} so the application's
 * {@link DataSource} and transaction manager already exist, and gated on a single {@code DataSource}
 * candidate ({@link ConditionalOnSingleCandidate}) — it resolves the {@code @Primary} one when several
 * exist and backs off, rather than guessing, when the choice is ambiguous. This module never pulls Kafka.
 *
 * <p>The ordering is declared by <b>name</b>, listing both generations' coordinates: Boot 4 moved every
 * one of these autoconfigurations out of {@code spring-boot-autoconfigure} (and out of
 * {@code spring-boot-actuator-autoconfigure}) into its own module, so a class literal would name a type
 * that does not exist there and the ordering would be silently lost — one jar has to satisfy both lines
 * (LLD-spring-config §1.1). Names that match nothing are ignored. The tracing entries matter because
 * {@link #tandemMicrometerTracePropagator} is conditional on the {@code Tracer}/{@code Propagator} beans
 * those autoconfigurations contribute: evaluated too early, the condition sees no tracer and the write
 * side silently falls back to correlation-id-only capture.
 */
@AutoConfiguration(afterName = {
        // Spring Boot 3.x
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
        "org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.tracing.MicrometerTracingAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.tracing.BraveAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.tracing.OpenTelemetryAutoConfiguration",
        // Spring Boot 4.x — relocated into spring-boot-jdbc / spring-boot-transaction and, for tracing,
        // into spring-boot-micrometer-tracing{,-brave,-opentelemetry}
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
        "org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration",
        "org.springframework.boot.micrometer.tracing.autoconfigure.MicrometerTracingAutoConfiguration",
        "org.springframework.boot.micrometer.tracing.brave.autoconfigure.BraveAutoConfiguration",
        "org.springframework.boot.micrometer.tracing.opentelemetry.autoconfigure.OpenTelemetryTracingAutoConfiguration"})
@ConditionalOnSingleCandidate(DataSource.class)
@EnableConfigurationProperties({TandemOutboxProperties.class, TandemTracingProperties.class})
public class TandemProducerAutoConfiguration {

    /**
     * The write-side repository, backed by JDBC. The bucket-count guard runs first, so a bucket count
     * that diverges from what the database already holds fails context refresh (LLD-spring-config §3)
     * instead of silently inserting into buckets the relay never polls. {@code tracePropagator} is
     * empty unless {@link #tandemTracePropagator} was contributed, in which case {@link JdbcOutboxRepository}
     * falls back to {@link TracePropagator#NOOP} — trace/correlation capture stays off (HLD-tracing.md §7).
     */
    @Bean
    @ConditionalOnMissingBean
    OutboxRepository tandemOutboxRepository(DataSource dataSource, TandemOutboxProperties properties,
            ObjectProvider<TracePropagator> tracePropagator) {
        BucketCountGuard.check(dataSource, properties.bucketCount());
        // Wrap in a transaction-aware proxy so the insert joins the application's Spring transaction —
        // the JdbcOutboxRepository takes whatever connection the DataSource hands it, and this proxy hands
        // it the one bound to the active transaction. Without it, the insert would run on a separate
        // autocommitted connection and lose atomicity with the business state change.
        return new JdbcOutboxRepository(new TransactionAwareDataSourceProxy(dataSource), properties.bucketCount(),
                tracePropagator.getIfAvailable(() -> TracePropagator.NOOP));
    }

    /**
     * Full trace capture — the distributed trace context <i>and</i> the correlation id (HLD-tracing.md
     * §3/§5) — contributed when the application runs Micrometer Tracing. The two are merged rather than
     * folded into one adapter because they are independent identifiers from independent sources (§2):
     * an application may well have a correlation id and no active trace, or the reverse.
     *
     * <p>Like every capture bean it is gated on the explicit {@code tandem.tracing.enabled} flag, never on
     * the tracing library's mere presence — a transitively-pulled dependency must not silently start
     * writing headers onto outbox rows (§9). Declared before {@link #tandemTracePropagator} so that, when
     * both apply, this is the one that wins and the correlation-id-only bean backs off.
     *
     * <p>The class and bean conditions sit on the bean method, not on a nested {@code @Configuration},
     * which Spring Boot 4 would not process (LLD-spring-config §1.1). Spring reads those annotations from
     * ASM metadata, so naming the Micrometer types <i>there</i> is safe when the library is absent — but
     * it still reflects over every method of this class to build its bean definitions, so an optional type
     * must not appear in a method's <b>erased signature</b>. Hence {@link ObjectProvider}: the type
     * survives only as a generic argument, which erasure removes. Declaring the parameters as
     * {@code Tracer}/{@code Propagator} directly compiles and passes the isolated wiring tests, then fails
     * every application that does not have Micrometer Tracing with a {@code NoClassDefFoundError} — the
     * conditions never get the chance to back the bean off.
     */
    @Bean
    @ConditionalOnClass(Tracer.class)
    @ConditionalOnBean({Tracer.class, Propagator.class})
    @ConditionalOnMissingBean(TracePropagator.class)
    @ConditionalOnProperty(prefix = "tandem.tracing", name = "enabled", havingValue = "true")
    TracePropagator tandemMicrometerTracePropagator(ObjectProvider<Tracer> tracer,
            ObjectProvider<Propagator> propagator, TandemTracingProperties properties) {
        return TracePropagator.composite(
                new MicrometerTracePropagator(tracer.getObject(), propagator.getObject()),
                new MdcCorrelationTracePropagator(properties.correlationIdMdcKey()));
    }

    /**
     * Correlation-id-only trace capture (HLD-tracing.md §9), the fallback when the application has no
     * tracing library — contributed only when {@code tandem.tracing.enabled=true}, never from a tracing
     * adapter's mere classpath presence, so an unrelated dependency cannot silently turn this on. MDC is
     * assumed present (Spring Boot always ships SLF4J).
     */
    @Bean
    @ConditionalOnMissingBean(TracePropagator.class)
    @ConditionalOnProperty(prefix = "tandem.tracing", name = "enabled", havingValue = "true")
    TracePropagator tandemTracePropagator(TandemTracingProperties properties) {
        return new MdcCorrelationTracePropagator(properties.correlationIdMdcKey());
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
     *
     * <p>The class condition sits on the bean method rather than on a nested {@code @Configuration}: a
     * nested member configuration is <b>not</b> processed inside an {@code @AutoConfiguration} under Spring
     * Boot 4, so grouping optional beans that way would silently drop them there (LLD-spring-config §1.1).
     * Spring evaluates the condition from ASM metadata before it resolves this method's signature, so the
     * {@code ObjectMapper} reference is safe even when Jackson is absent.
     */
    @Bean
    @ConditionalOnClass(ObjectMapper.class)
    @ConditionalOnMissingBean(PayloadSerializer.class)
    PayloadSerializer tandemPayloadSerializer(ObjectProvider<ObjectMapper> objectMapper) {
        return new JacksonPayloadSerializer(objectMapper.getIfAvailable(ObjectMapper::new));
    }

    /**
     * The {@link TransactionalOutbox} annotation tier (LLD-spring-producer §4), contributed only when
     * AspectJ is on the classpath. Spring Boot's AOP autoconfiguration enables the aspect proxying that
     * makes the advice apply.
     */
    @Bean
    @ConditionalOnClass(ProceedingJoinPoint.class)
    @ConditionalOnMissingBean
    TransactionalOutboxAspect tandemTransactionalOutboxAspect(OutboxRepository outboxRepository) {
        return new TransactionalOutboxAspect(outboxRepository);
    }
}
