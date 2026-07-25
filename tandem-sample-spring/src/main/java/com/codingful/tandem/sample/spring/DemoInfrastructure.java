package com.codingful.tandem.sample.spring;

import com.codingful.tandem.test.TandemTestContainer;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * [DEMO-ONLY] Makes the sample self-contained. It starts a real PostgreSQL + Kafka via
 * {@link TandemTestContainer} (which applies Tandem's baseline schema) and exposes the container's
 * PostgreSQL as the application's {@link DataSource}. Spring Boot then autoconfigures the transaction
 * manager, and Tandem's write side wires on top of it, exactly as it would over a production DataSource.
 *
 * <p>A real application deletes this class and configures its own DataSource in {@code application.yml};
 * nothing here is part of Tandem.
 */
@Configuration(proxyBeanMethods = false)
class DemoInfrastructure {

    @Bean(destroyMethod = "close")
    TandemTestContainer tandemInfrastructure() {
        return new TandemTestContainer().start();
    }

    @Bean
    DataSource dataSource(TandemTestContainer infrastructure) {
        return infrastructure.dataSource();
    }
}
