package com.codingful.tandem.core;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * A persisted outbox row with its delivery state (LLD-core §1.4), returned by the relay-side
 * {@link com.codingful.tandem.core.port.OutboxStore} and the Admin API. Immutable; built via
 * {@link #builder()}.
 *
 * <p>Convenience accessors ({@link #aggregateId()}, {@link #aggregateType()}, {@link #type()},
 * {@link #seq()}, {@link #payload()}, …) delegate to the wrapped {@link OutboxMessage} so the
 * dispatcher and router read the record directly (LLD-kafka §3).
 */
public final class OutboxRecord {

    private final long id;
    private final OutboxMessage message;
    private final OutboxStatus status;
    private final int attempts;
    private final String lockedBy;        // nullable
    private final Instant lockedUntil;    // nullable
    private final String lastError;       // nullable
    private final Instant nextAttemptAt;  // nullable
    private final Instant createdAt;
    private final Long lamport;           // nullable; only when causal ordering enabled (§9)

    private OutboxRecord(Builder b) {
        this.id = b.id;
        this.message = Objects.requireNonNull(b.message, "message");
        this.status = Objects.requireNonNull(b.status, "status");
        this.attempts = b.attempts;
        this.lockedBy = b.lockedBy;
        this.lockedUntil = b.lockedUntil;
        this.lastError = b.lastError;
        this.nextAttemptAt = b.nextAttemptAt;
        this.createdAt = Objects.requireNonNull(b.createdAt, "createdAt");
        this.lamport = b.lamport;
    }

    public long id() {
        return id;
    }

    public OutboxMessage message() {
        return message;
    }

    public OutboxStatus status() {
        return status;
    }

    public int attempts() {
        return attempts;
    }

    public String lockedBy() {
        return lockedBy;
    }

    public Instant lockedUntil() {
        return lockedUntil;
    }

    public String lastError() {
        return lastError;
    }

    public Instant nextAttemptAt() {
        return nextAttemptAt;
    }

    public Instant createdAt() {
        return createdAt;
    }

    /** Lamport timestamp; {@code null} unless causal ordering is enabled (§9). */
    public Long lamport() {
        return lamport;
    }

    // --- convenience delegates to the wrapped message ---

    public AggregateId aggregateId() {
        return message.aggregateId();
    }

    public String aggregateType() {
        return message.aggregateType();
    }

    public String type() {
        return message.type();
    }

    public long seq() {
        return message.seq();
    }

    public byte[] payload() {
        return message.payload();
    }

    public String contentType() {
        return message.contentType();
    }

    public Map<String, String> headers() {
        return message.headers();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "OutboxRecord{id=" + id
                + ", aggregateType=" + aggregateType()
                + ", aggregateId=" + aggregateId()
                + ", seq=" + seq()
                + ", status=" + status
                + ", attempts=" + attempts + '}';
    }

    /** A copy of this record with the given status (and nothing else changed). */
    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .message(message)
                .status(status)
                .attempts(attempts)
                .lockedBy(lockedBy)
                .lockedUntil(lockedUntil)
                .lastError(lastError)
                .nextAttemptAt(nextAttemptAt)
                .createdAt(createdAt)
                .lamport(lamport);
    }

    public static final class Builder {
        private long id;
        private OutboxMessage message;
        private OutboxStatus status = OutboxStatus.PENDING;
        private int attempts;
        private String lockedBy;
        private Instant lockedUntil;
        private String lastError;
        private Instant nextAttemptAt;
        private Instant createdAt;
        private Long lamport;

        private Builder() {
        }

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public Builder message(OutboxMessage message) {
            this.message = message;
            return this;
        }

        public Builder status(OutboxStatus status) {
            this.status = status;
            return this;
        }

        public Builder attempts(int attempts) {
            this.attempts = attempts;
            return this;
        }

        public Builder lockedBy(String lockedBy) {
            this.lockedBy = lockedBy;
            return this;
        }

        public Builder lockedUntil(Instant lockedUntil) {
            this.lockedUntil = lockedUntil;
            return this;
        }

        public Builder lastError(String lastError) {
            this.lastError = lastError;
            return this;
        }

        public Builder nextAttemptAt(Instant nextAttemptAt) {
            this.nextAttemptAt = nextAttemptAt;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder lamport(Long lamport) {
            this.lamport = lamport;
            return this;
        }

        public OutboxRecord build() {
            return new OutboxRecord(this);
        }
    }
}
