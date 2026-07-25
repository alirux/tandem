package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.exception.OutboxInsertException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.core.ResolvableType;

/**
 * Resolves a published event to its {@link OutboxEventMapper} (LLD-spring-producer §5). Built once at
 * startup from the registered mapper beans, keyed by each mapper's resolved event type; two mappers for
 * the same event type fail fast here rather than per event. Lookup is by the event's runtime type, with
 * a fall-back to the most specific registered supertype.
 */
final class OutboxEventMapperRegistry {

    private final Map<Class<?>, OutboxEventMapper<?>> byEventType;

    private OutboxEventMapperRegistry(Map<Class<?>, OutboxEventMapper<?>> byEventType) {
        this.byEventType = byEventType;
    }

    static OutboxEventMapperRegistry of(Collection<OutboxEventMapper<?>> mappers) {
        Map<Class<?>, OutboxEventMapper<?>> map = new HashMap<>();
        for (OutboxEventMapper<?> mapper : mappers) {
            Class<?> eventType = resolveEventType(mapper);
            OutboxEventMapper<?> existing = map.putIfAbsent(eventType, mapper);
            if (existing != null) {
                throw new IllegalStateException("Two OutboxEventMappers are registered for the same event type"
                        + " eventType:" + eventType.getName());
            }
        }
        return new OutboxEventMapperRegistry(map);
    }

    private static Class<?> resolveEventType(OutboxEventMapper<?> mapper) {
        Class<?> eventType = ResolvableType.forInstance(mapper).as(OutboxEventMapper.class).getGeneric(0).resolve();
        if (eventType == null) {
            throw new IllegalStateException("Could not resolve the event type of OutboxEventMapper implementation:"
                    + mapper.getClass().getName());
        }
        return eventType;
    }

    /** Whether some registered mapper handles this event type (exact or by an assignable supertype). */
    boolean handles(Class<?> eventType) {
        return byEventType.containsKey(eventType)
                || byEventType.keySet().stream().anyMatch(registered -> registered.isAssignableFrom(eventType));
    }

    /**
     * Map the event to its outbox rows.
     *
     * @throws OutboxInsertException if no mapper handles it, the match is ambiguous, or the mapper
     *         returned {@code null}
     */
    Collection<OutboxMessage> map(Object event) {
        OutboxEventMapper<?> mapper = findMapper(event.getClass())
                .orElseThrow(() -> new OutboxInsertException("No OutboxEventMapper registered for event type:"
                        + event.getClass().getName()));
        Collection<OutboxMessage> messages = invoke(mapper, event);
        if (messages == null) {
            throw new OutboxInsertException("OutboxEventMapper returned null for event type:"
                    + event.getClass().getName());
        }
        return messages;
    }

    @SuppressWarnings("unchecked")
    private static Collection<OutboxMessage> invoke(OutboxEventMapper<?> mapper, Object event) {
        return ((OutboxEventMapper<Object>) mapper).map(event);
    }

    private Optional<OutboxEventMapper<?>> findMapper(Class<?> eventType) {
        OutboxEventMapper<?> exact = byEventType.get(eventType);
        if (exact != null) {
            return Optional.of(exact);
        }
        List<Class<?>> assignable = byEventType.keySet().stream()
                .filter(registered -> registered.isAssignableFrom(eventType))
                .toList();
        List<Class<?>> mostSpecific = assignable.stream()
                .filter(candidate -> assignable.stream()
                        .noneMatch(other -> other != candidate && candidate.isAssignableFrom(other)))
                .toList();
        if (mostSpecific.size() > 1) {
            throw new OutboxInsertException("Ambiguous OutboxEventMapper match for event type:" + eventType.getName()
                    + ", candidates:" + mostSpecific);
        }
        return mostSpecific.stream().findFirst().map(byEventType::get);
    }
}
