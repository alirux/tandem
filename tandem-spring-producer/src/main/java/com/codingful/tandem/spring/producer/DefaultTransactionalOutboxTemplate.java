package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.core.port.PayloadSerializer;
import java.util.Objects;
import java.util.function.Function;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Default {@link TransactionalOutboxTemplate} (LLD-spring-producer §3): owns the transaction through a
 * Spring {@link TransactionTemplate}, runs the unit of work, then inserts everything the work collected
 * within that same transaction. Because the insert happens before the transaction commits, the business
 * state and the outbox rows are atomic.
 */
final class DefaultTransactionalOutboxTemplate implements TransactionalOutboxTemplate {

    private final OutboxRepository outboxRepository;
    private final TransactionTemplate transactionTemplate;
    private final PayloadSerializer payloadSerializer; // may be null — only object payloads need it

    DefaultTransactionalOutboxTemplate(OutboxRepository outboxRepository, TransactionTemplate transactionTemplate,
            PayloadSerializer payloadSerializer) {
        this.outboxRepository = Objects.requireNonNull(outboxRepository, "outboxRepository");
        this.transactionTemplate = Objects.requireNonNull(transactionTemplate, "transactionTemplate");
        this.payloadSerializer = payloadSerializer;
    }

    @Override
    public <T> T execute(Function<OutboxCollector, T> work) {
        Objects.requireNonNull(work, "work");
        return transactionTemplate.execute(status -> {
            CollectingOutboxCollector collector = new CollectingOutboxCollector(payloadSerializer);
            T result = work.apply(collector);
            outboxRepository.insertAll(collector.collected());
            return result;
        });
    }
}
