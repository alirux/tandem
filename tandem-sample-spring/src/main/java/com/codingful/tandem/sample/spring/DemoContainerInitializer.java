package com.codingful.tandem.sample.spring;

import com.codingful.tandem.test.TandemTestContainer;
import java.util.Map;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;

/**
 * [DEMO-ONLY] Makes the sample self-contained. Before the context refreshes it starts a real PostgreSQL
 * and Kafka via {@link TandemTestContainer} (which applies Tandem's baseline schema) and publishes their
 * connection details as ordinary Spring properties — so Boot autoconfigures the {@code DataSource} and
 * Tandem's <em>producer</em> and <em>relay</em> autoconfigurations wire on top of them exactly as they
 * would over a production environment, with no hand-assembly. The container is registered as a bean only
 * so the runner can attach a Kafka consumer to it.
 *
 * <p>Registered via {@code context.initializer.classes} in application.yml. A real application deletes
 * this class and sets {@code spring.datasource.*} / {@code tandem.kafka.*} in its own configuration.
 */
public class DemoContainerInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    /** Where the relay publishes: the default router maps aggregate type {@code Order} to {@code order-topic}. */
    static final String TOPIC = "order-topic";

    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        TandemTestContainer infrastructure = new TandemTestContainer().start();
        infrastructure.createTopic(TOPIC, 4);
        Runtime.getRuntime().addShutdownHook(new Thread(infrastructure::close, "tandem-demo-infra-close"));

        Map<String, Object> connectionDetails = Map.of(
                "spring.datasource.url", infrastructure.postgresJdbcUrl(),
                "spring.datasource.username", infrastructure.postgresUsername(),
                "spring.datasource.password", infrastructure.postgresPassword(),
                "tandem.kafka.source", "/tandem/sample-spring",
                // Bracket the dotted map key so it binds as the single producer key "bootstrap.servers".
                "tandem.kafka.producer[bootstrap.servers]", infrastructure.bootstrapServers());
        applicationContext.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("tandemDemoContainers", connectionDetails));

        applicationContext.getBeanFactory().registerSingleton("tandemInfrastructure", infrastructure);
    }
}
