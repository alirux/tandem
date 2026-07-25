package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.exception.OutboxInsertException;
import com.codingful.tandem.core.port.OutboxRepository;
import java.util.List;
import java.util.Objects;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.PayloadApplicationEvent;
import org.springframework.context.event.GenericApplicationListener;
import org.springframework.core.ResolvableType;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * The Spring application-events tier (LLD-spring-producer §5): a synchronous listener that maps a
 * published event to outbox rows and inserts them in the publisher's transaction. It is scoped — via
 * {@link #supportsEventType} — to exactly the events it can handle ({@link OutboxMessage} directly, or a
 * type with a registered {@link OutboxEventMapper}), so framework and unrelated events are never
 * intercepted. Because it runs inline, it asserts an active transaction and fails fast rather than
 * inserting under autocommit.
 */
class OutboxEventListener implements GenericApplicationListener {

    private final OutboxRepository outboxRepository;
    private final OutboxEventMapperRegistry mapperRegistry;

    OutboxEventListener(OutboxRepository outboxRepository, OutboxEventMapperRegistry mapperRegistry) {
        this.outboxRepository = Objects.requireNonNull(outboxRepository, "outboxRepository");
        this.mapperRegistry = Objects.requireNonNull(mapperRegistry, "mapperRegistry");
    }

    @Override
    public boolean supportsEventType(ResolvableType eventType) {
        Class<?> rawType = eventType.getRawClass();
        if (rawType == null || !PayloadApplicationEvent.class.isAssignableFrom(rawType)) {
            return false;
        }
        Class<?> payloadType = eventType.getGeneric(0).resolve();
        return payloadType != null
                && (OutboxMessage.class.isAssignableFrom(payloadType) || mapperRegistry.handles(payloadType));
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (!(event instanceof PayloadApplicationEvent<?> payloadEvent)) {
            return;
        }
        Object payload = payloadEvent.getPayload();
        List<OutboxMessage> messages = toMessages(payload);
        if (messages.isEmpty()) {
            return;
        }
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new OutboxInsertException("A Tandem outbox event was published outside an active transaction —"
                    + " the insert would not be atomic eventType:" + payload.getClass().getName());
        }
        outboxRepository.insertAll(messages);
    }

    private List<OutboxMessage> toMessages(Object payload) {
        if (payload instanceof OutboxMessage message) {
            return List.of(message);
        }
        return List.copyOf(mapperRegistry.map(payload));
    }
}
