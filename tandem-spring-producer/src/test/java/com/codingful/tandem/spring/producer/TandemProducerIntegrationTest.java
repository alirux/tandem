package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.port.TandemAggregate;
import com.codingful.tandem.test.TandemTestContainer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.Transactional;

/**
 * Integration tests (LLD-spring-producer §3–§5) proving that every write-side tier inserts <b>within</b>
 * the caller's Spring transaction: each row is present after a commit and absent after a rollback. The
 * annotation tier's rollback case is what pins the AOP advice ordering — the aspect must run inside the
 * transaction. Runs against a real PostgreSQL via {@link TandemTestContainer}.
 */
@Tag("integration")
class TandemProducerIntegrationTest {

    private static TandemTestContainer container;

    @BeforeAll
    static void startContainer() {
        container = new TandemTestContainer().start();
    }

    @AfterAll
    static void stopContainer() {
        if (container != null) {
            container.close();
        }
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    TandemProducerAutoConfiguration.class,
                    DataSourceTransactionManagerAutoConfiguration.class,
                    TransactionAutoConfiguration.class,
                    AopAutoConfiguration.class))
            .withBean(DataSource.class, () -> container.dataSource())
            .withUserConfiguration(TiersConfig.class);

    private static long outboxRowCount(String aggregateId) {
        try (Connection connection = container.dataSource().getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT count(*) FROM tandem_outbox WHERE aggregate_id = ?")) {
            statement.setString(1, aggregateId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("counting outbox rows failed", e);
        }
    }

    @Test
    void GIVEN_the_template_WHEN_the_work_commits_THEN_the_row_is_inserted() {
        runner.run(context -> {
            context.getBean(TransactionalOutboxTemplate.class)
                    .executeWithoutResult(outbox -> outbox.record("Order", "tpl-commit", 1L, new OrderEvent("tpl-commit")));
            assertThat(outboxRowCount("tpl-commit")).isEqualTo(1);
        });
    }

    @Test
    void GIVEN_the_template_WHEN_the_work_throws_THEN_the_row_is_rolled_back() {
        runner.run(context -> {
            TransactionalOutboxTemplate template = context.getBean(TransactionalOutboxTemplate.class);
            assertThatThrownBy(() -> template.executeWithoutResult(outbox -> {
                outbox.record("Order", "tpl-rollback", 1L, new OrderEvent("tpl-rollback"));
                throw new IllegalStateException("boom");
            })).isInstanceOf(IllegalStateException.class);
            assertThat(outboxRowCount("tpl-rollback")).isZero();
        });
    }

    @Test
    void GIVEN_the_annotation_tier_WHEN_the_method_commits_THEN_the_row_is_inserted() {
        runner.run(context -> {
            context.getBean(OrderService.class).placeOrder("ann-commit");
            assertThat(outboxRowCount("ann-commit")).isEqualTo(1);
        });
    }

    @Test
    void GIVEN_the_annotation_tier_WHEN_the_surrounding_transaction_rolls_back_THEN_the_row_is_rolled_back() {
        runner.run(context -> {
            OuterService outerService = context.getBean(OuterService.class);
            assertThatThrownBy(() -> outerService.placeThenFail("ann-rollback")).isInstanceOf(IllegalStateException.class);
            assertThat(outboxRowCount("ann-rollback")).isZero();
        });
    }

    @Test
    void GIVEN_the_events_tier_WHEN_the_method_commits_THEN_the_row_is_inserted() {
        runner.run(context -> {
            context.getBean(EventService.class).publish("evt-commit");
            assertThat(outboxRowCount("evt-commit")).isEqualTo(1);
        });
    }

    @Test
    void GIVEN_the_events_tier_WHEN_the_transaction_rolls_back_THEN_the_row_is_rolled_back() {
        runner.run(context -> {
            EventService eventService = context.getBean(EventService.class);
            assertThatThrownBy(() -> eventService.publishThenFail("evt-rollback")).isInstanceOf(IllegalStateException.class);
            assertThat(outboxRowCount("evt-rollback")).isZero();
        });
    }

    private static OutboxMessage message(String aggregateId) {
        return OutboxMessage.builder()
                .aggregateType("Order").aggregateId(aggregateId).seq(1L)
                .payload(("{\"id\":\"" + aggregateId + "\"}").getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .build();
    }

    private record OrderEvent(String id) {
    }

    private static final class Order implements TandemAggregate {
        private final String id;

        private Order(String id) {
            this.id = id;
        }

        @Override
        public Collection<OutboxMessage> pendingOutboxMessages() {
            return List.of(message(id));
        }
    }

    static class OrderService {
        @TransactionalOutbox
        public Order placeOrder(String id) {
            return new Order(id);
        }
    }

    static class OuterService {
        private final OrderService orderService;

        OuterService(OrderService orderService) {
            this.orderService = orderService;
        }

        @Transactional
        public void placeThenFail(String id) {
            orderService.placeOrder(id);
            throw new IllegalStateException("boom");
        }
    }

    static class EventService {
        private final ApplicationEventPublisher publisher;

        EventService(ApplicationEventPublisher publisher) {
            this.publisher = publisher;
        }

        @Transactional
        public void publish(String id) {
            publisher.publishEvent(new OrderEvent(id));
        }

        @Transactional
        public void publishThenFail(String id) {
            publisher.publishEvent(new OrderEvent(id));
            throw new IllegalStateException("boom");
        }
    }

    static final class OrderEventMapper implements OutboxEventMapper<OrderEvent> {
        @Override
        public Collection<OutboxMessage> map(OrderEvent event) {
            return List.of(message(event.id()));
        }
    }

    @Configuration
    static class TiersConfig {
        @Bean
        OrderService orderService() {
            return new OrderService();
        }

        @Bean
        OuterService outerService(OrderService orderService) {
            return new OuterService(orderService);
        }

        @Bean
        EventService eventService(ApplicationEventPublisher publisher) {
            return new EventService(publisher);
        }

        @Bean
        OrderEventMapper orderEventMapper() {
            return new OrderEventMapper();
        }
    }
}
