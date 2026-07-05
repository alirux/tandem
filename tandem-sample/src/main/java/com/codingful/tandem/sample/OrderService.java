package com.codingful.tandem.sample;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.jdbc.JdbcOutboxRepository;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * [CALLER] Simulates an order domain service that uses the Transactional Outbox pattern.
 *
 * <p>This entire class is application code — Tandem ships none of it. The only Tandem
 * touchpoint is the {@link #repository}'s {@code insert} call at the bottom of {@link #insert}:
 * everything above it (assigning {@code seq}, building the JSON payload, deciding the event
 * {@code type}) is the caller's responsibility, not the library's.
 *
 * <h3>How this fits into a real application</h3>
 *
 * <p>Each method in this class would normally run inside a transaction that also modifies the
 * {@code orders} domain table:
 *
 * <pre>{@code
 * // Spring example:
 * @Transactional
 * public Order placeOrder(PlaceOrderCommand cmd) {
 *     Order saved = orderRepo.save(new Order(cmd));     // your domain row
 *     outboxRepo.insert(orderPlacedEvent(saved));       // outbox row — same TX
 *     return saved;
 * }
 * }</pre>
 *
 * <p>Because both rows commit or roll back together there is no window where the domain state
 * changes without a corresponding outbox row — that is the Transactional Outbox guarantee.
 *
 * <h3>Sequence numbers</h3>
 *
 * <p>{@code seq} is the per-aggregate event sequence number. It must be monotonically increasing
 * for each {@code aggregateId}. Typical sources: an optimistic-lock version counter, a dedicated
 * event counter column, or a simple in-memory counter as used here. The relay uses {@code seq} to
 * enforce in-order delivery: it will not publish seq N+1 until seq N is acknowledged by Kafka.
 */
public final class OrderService {

    private final JdbcOutboxRepository repository;

    // Next seq per aggregate. In a real application this comes from the aggregate's
    // version counter or an event-sourcing sequence.
    private final Map<String, Long> seqs = new ConcurrentHashMap<>();

    public OrderService(JdbcOutboxRepository repository) {
        this.repository = repository;
    }

    /** Inserts an {@code order.placed} event into the outbox. */
    public void place(String orderId) {
        insert(orderId, "order.placed", "PLACED");
    }

    /** Inserts an {@code order.confirmed} event into the outbox. */
    public void confirm(String orderId) {
        insert(orderId, "order.confirmed", "CONFIRMED");
    }

    /** Inserts an {@code order.shipped} event into the outbox. */
    public void ship(String orderId) {
        insert(orderId, "order.shipped", "SHIPPED");
    }

    private void insert(String orderId, String eventType, String status) {
        // Assigns the next seq: first call → 1, second → 2, …
        long seq = seqs.merge(orderId, 1L, Long::sum);

        // In production replace the hand-rolled JSON with a proper serializer.
        String json = String.format("{\"orderId\":\"%s\",\"status\":\"%s\"}", orderId, status);

        OutboxMessage event = OutboxMessage.builder()
                .aggregateId(orderId)
                .aggregateType("Order")
                .type(eventType)
                .seq(seq)
                .payload(json.getBytes(StandardCharsets.UTF_8))
                .contentType("application/json")
                .build();

        repository.insert(event);
        System.out.printf("  [outbox] %-10s  seq=%d  type=%s%n", orderId, seq, eventType);
    }
}
