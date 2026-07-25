package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.jdbc.BucketCountGuard;
import com.codingful.tandem.jdbc.JdbcOutboxRepository;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Write-side autoconfiguration (LLD-spring-config §4.3): contributes the {@link OutboxRepository} the
 * application uses to insert outbox rows inside its own transaction. Declared {@code after} Spring
 * Boot's {@link DataSourceAutoConfiguration} so the application's {@link DataSource} is already defined,
 * and gated on a single {@code DataSource} candidate ({@link ConditionalOnSingleCandidate}) — it resolves
 * the {@code @Primary} one when several exist and backs off, rather than guessing, when the choice is
 * ambiguous. This module never pulls Kafka.
 */
@AutoConfiguration(after = DataSourceAutoConfiguration.class)
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
        return new JdbcOutboxRepository(dataSource, properties.bucketCount());
    }
}
