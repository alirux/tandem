package com.codingful.tandem.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The write-side value the client builds and inserts (LLD-core §1.3) — immutable, built via
 * {@link #builder()}.
 *
 * <ul>
 *   <li><b>Payload is {@code byte[]}</b>: the core never serializes, so it forces no JSON library on
 *       the client (§1.3). Higher tiers may offer an {@code Object}-accepting overload backed by a
 *       {@link com.codingful.tandem.core.port.PayloadSerializer}.</li>
 *   <li>{@code seq} is app-assigned, from the aggregate's version (HLD §4.2) — the core never
 *       generates it.</li>
 *   <li>{@code contentType} is the only typed convenience field that maps onto a header: the
 *       write-side serializes it into {@code headers["content-type"]} at insert (LLD-jdbc §2), the
 *       key the relay reads for the CloudEvents {@code datacontenttype} (LLD-kafka §3.2). There is no
 *       dedicated column — it reuses {@code headers} (Pareto).</li>
 * </ul>
 *
 * <p>There is deliberately <b>no {@code causationId}</b> here: causation is part of the opt-in
 * causal-ordering feature (off by default; HLD §9), so it is not carried on the basic-round write
 * value.
 */
public final class OutboxMessage {

    private final AggregateId aggregateId;
    private final String aggregateType;
    private final String type;            // CloudEvents `type`; nullable (Q20)
    private final long seq;               // app-assigned (HLD §4.2)
    private final byte[] payload;         // already serialized (Q3)
    private final String contentType;     // nullable; persisted into headers["content-type"]
    private final Map<String, String> headers;  // immutable copy; may be empty

    private OutboxMessage(Builder b) {
        this.aggregateId = Objects.requireNonNull(b.aggregateId, "aggregateId");
        this.aggregateType = requireNonBlank(b.aggregateType, "aggregateType");
        this.type = b.type;
        this.seq = b.seq;
        this.payload = Objects.requireNonNull(b.payload, "payload").clone();
        this.contentType = b.contentType;
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(b.headers));
    }

    public AggregateId aggregateId() {
        return aggregateId;
    }

    public String aggregateType() {
        return aggregateType;
    }

    /** CloudEvents {@code type}; may be {@code null} (Q20 — the relay falls back to {@code aggregateType}). */
    public String type() {
        return type;
    }

    public long seq() {
        return seq;
    }

    /** The serialized payload bytes. Returns a defensive copy — the value is immutable. */
    public byte[] payload() {
        return payload.clone();
    }

    /** e.g. {@code "application/json"}; may be {@code null}. */
    public String contentType() {
        return contentType;
    }

    /** Immutable; may be empty. */
    public Map<String, String> headers() {
        return headers;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OutboxMessage other)) {
            return false;
        }
        return seq == other.seq
                && aggregateId.equals(other.aggregateId)
                && aggregateType.equals(other.aggregateType)
                && Objects.equals(type, other.type)
                && Arrays.equals(payload, other.payload)
                && Objects.equals(contentType, other.contentType)
                && headers.equals(other.headers);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(aggregateId, aggregateType, type, seq, contentType, headers);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "OutboxMessage{aggregateId=" + aggregateId
                + ", aggregateType=" + aggregateType
                + ", type=" + type
                + ", seq=" + seq
                + ", contentType=" + contentType
                + ", payloadBytes=" + payload.length
                + ", headerNames=" + headers.keySet() + '}';
    }

    private static String requireNonBlank(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    public static final class Builder {
        private AggregateId aggregateId;
        private String aggregateType;
        private String type;
        private long seq;
        private byte[] payload;
        private String contentType;
        private final Map<String, String> headers = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder aggregateId(AggregateId aggregateId) {
            this.aggregateId = aggregateId;
            return this;
        }

        public Builder aggregateId(String aggregateId) {
            this.aggregateId = AggregateId.of(aggregateId);
            return this;
        }

        public Builder aggregateType(String aggregateType) {
            this.aggregateType = aggregateType;
            return this;
        }

        /** CloudEvents {@code type}; optional — the relay falls back to {@code aggregateType} if unset (Q20). */
        public Builder type(String type) {
            this.type = type;
            return this;
        }

        /** The app-assigned, per-aggregate monotonically increasing sequence number (HLD §4.2). */
        public Builder seq(long seq) {
            this.seq = seq;
            return this;
        }

        /**
         * The already-serialized event bytes; the core never serializes (§1.3). A storage adapter may
         * constrain the encoding: the bundled PostgreSQL adapter stores this in a {@code jsonb} column,
         * so it must be valid UTF-8 JSON there (see {@code JdbcOutboxRepository}).
         */
        public Builder payload(byte[] payload) {
            this.payload = payload;
            return this;
        }

        /** e.g. {@code "application/json"}; persisted into {@code headers["content-type"]} (overrides any existing entry). */
        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        /** Adds one header; call repeatedly for multiple entries. */
        public Builder header(String name, String value) {
            this.headers.put(
                    Objects.requireNonNull(name, "header name"),
                    Objects.requireNonNull(value, "header value"));
            return this;
        }

        /** Merges all entries of {@code headers} (same semantics as repeated {@link #header}). */
        public Builder headers(Map<String, String> headers) {
            headers.forEach(this::header);
            return this;
        }

        /** @throws NullPointerException if a required field ({@code aggregateId}, {@code payload}) is unset */
        public OutboxMessage build() {
            return new OutboxMessage(this);
        }
    }
}
