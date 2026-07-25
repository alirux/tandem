package com.codingful.tandem.sample.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * A minimal Spring Boot application demonstrating Tandem's write side.
 *
 * <p>This class is deliberately just the Boot entry point: the Tandem autoconfiguration wires the
 * {@code OutboxRepository}, the {@code @TransactionalOutbox} aspect, the Template and the Spring-events
 * tiers, and {@link OrderService} writes only business logic. Everything that makes this sample
 * <em>self-contained and observable</em> — a Testcontainers PostgreSQL/Kafka ({@link DemoInfrastructure})
 * and a runner that exercises the tiers ({@link SampleRunner}) — is marked demo-only and lives in its
 * own class. A real application keeps a class like this one plus its own beans, and nothing else.
 */
@SpringBootApplication
public class SampleSpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(SampleSpringApplication.class, args);
    }
}
