package com.codingful.tandem.core;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * The full detail behind one {@link com.codingful.tandem.core.port.OutboxQuery#findById} result: the
 * {@link OutboxRowView} plus the {@code payload} and {@code headers} the list view omits. Composition,
 * not inheritance — records cannot extend one another, and the two genuinely differ: a list page
 * never reads these columns at all (HLD-admin-api §4).
 */
public record OutboxRowDetail(OutboxRowView view, byte[] payload, Map<String, String> headers) {

    public OutboxRowDetail {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(payload, "payload");
        payload = payload.clone();
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    /** Defensive copy — the value is immutable. */
    @Override
    public byte[] payload() {
        return payload.clone();
    }

    // --- convenience delegates to the view ---

    public long id() {
        return view.id();
    }

    public AggregateId aggregateId() {
        return view.aggregateId();
    }

    public String aggregateType() {
        return view.aggregateType();
    }

    public String type() {
        return view.type();
    }

    public long seq() {
        return view.seq();
    }

    public OutboxStatus status() {
        return view.status();
    }

    public int attempts() {
        return view.attempts();
    }

    public int replays() {
        return view.replays();
    }

    public String lastError() {
        return view.lastError();
    }

    public String discardReason() {
        return view.discardReason();
    }

    public Instant nextAttemptAt() {
        return view.nextAttemptAt();
    }

    public String lockedBy() {
        return view.lockedBy();
    }

    public Instant lockedUntil() {
        return view.lockedUntil();
    }

    public Instant createdAt() {
        return view.createdAt();
    }

    public String correlationId() {
        return view.correlationId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OutboxRowDetail other)) {
            return false;
        }
        return view.equals(other.view) && Arrays.equals(payload, other.payload) && headers.equals(other.headers);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(view, headers);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "OutboxRowDetail{id=" + view.id()
                + ", aggregateType=" + view.aggregateType()
                + ", aggregateId=" + view.aggregateId()
                + ", seq=" + view.seq()
                + ", status=" + view.status()
                + ", attempts=" + view.attempts()
                + ", payloadBytes=" + payload.length
                + ", headerNames=" + headers.keySet() + '}';
    }
}
