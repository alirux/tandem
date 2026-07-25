package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.exception.OutboxInsertException;
import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.core.port.TandemAggregate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Applies the {@link TransactionalOutbox} annotation tier (LLD-spring-producer §4): after the annotated
 * method runs, it extracts the pending messages from the returned {@code TandemAggregate}(s) and inserts
 * them within the same transaction.
 *
 * <p>The advice must run <em>inside</em> the transaction the composed {@code @Transactional} opened. If a
 * misordering ever placed it outside, {@link #insertPendingMessages} would see no active transaction and
 * fail fast rather than insert non-atomically — the loud backstop the design relies on; the atomicity
 * itself is pinned by a rollback integration test.
 */
@Aspect
class TransactionalOutboxAspect {

    private final OutboxRepository outboxRepository;

    TransactionalOutboxAspect(OutboxRepository outboxRepository) {
        this.outboxRepository = Objects.requireNonNull(outboxRepository, "outboxRepository");
    }

    @Around("@annotation(transactionalOutbox)")
    Object insertPendingMessages(ProceedingJoinPoint joinPoint, TransactionalOutbox transactionalOutbox)
            throws Throwable {
        Object result = joinPoint.proceed();
        insertPending(extract(result), transactionalOutbox.aggregateType());
        return result;
    }

    /**
     * Guard the extracted messages, then insert them within the active transaction — failing fast when
     * there is none, so a misordering can never insert non-atomically. Separated from the advice so it is
     * testable without AOP.
     */
    void insertPending(List<OutboxMessage> messages, String declaredAggregateType) {
        guardAggregateType(messages, declaredAggregateType);
        if (messages.isEmpty()) {
            return;
        }
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new OutboxInsertException("@TransactionalOutbox produced outbox messages outside an active"
                    + " transaction — the insert would not be atomic");
        }
        outboxRepository.insertAll(messages);
    }

    /**
     * Collect the pending messages a method's return value exposes: a single {@code TandemAggregate}, or
     * each {@code TandemAggregate} in an {@link Iterable} (non-aggregate elements are ignored). Any other
     * return value — including {@code void}/{@code null} — yields nothing, which is allowed.
     */
    static List<OutboxMessage> extract(Object returnValue) {
        if (returnValue instanceof TandemAggregate aggregate) {
            return List.copyOf(aggregate.pendingOutboxMessages());
        }
        if (returnValue instanceof Iterable<?> iterable) {
            List<OutboxMessage> all = new ArrayList<>();
            for (Object element : iterable) {
                if (element instanceof TandemAggregate aggregate) {
                    all.addAll(aggregate.pendingOutboxMessages());
                }
            }
            return all;
        }
        return List.of();
    }

    /** When a {@code aggregateType} is declared on the annotation, assert every message carries it. */
    static void guardAggregateType(List<OutboxMessage> messages, String declaredAggregateType) {
        if (declaredAggregateType == null || declaredAggregateType.isEmpty()) {
            return;
        }
        for (OutboxMessage message : messages) {
            if (!declaredAggregateType.equals(message.aggregateType())) {
                throw new OutboxInsertException("Extracted outbox message does not match the @TransactionalOutbox"
                        + " declaration declaredAggregateType:" + declaredAggregateType
                        + ", messageAggregateType:" + message.aggregateType());
            }
        }
    }
}
