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
 *   <li><b>{@code seq} is app-assigned by default</b>, from the aggregate's version (HLD §4.2). A
 *       message built with {@link Builder#managedSeq()} instead leaves the number to Tandem, for an
 *       aggregate that has no version to take it from (HLD-managed-seq §4.1); the write-side adapter
 *       then omits the column and the database supplies it.</li>
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
    private final long seq;               // app-assigned (HLD §4.2); unset when managedSeq
    private final boolean managedSeq;     // true = Tandem assigns the number at insert (HLD-managed-seq §4.1)
    private final byte[] payload;         // already serialized (Q3)
    private final String contentType;     // nullable; persisted into headers["content-type"]
    private final Map<String, String> headers;  // immutable copy; may be empty

    private OutboxMessage(Builder b) {
        this.aggregateId = Objects.requireNonNull(b.aggregateId, "aggregateId");
        this.aggregateType = requireNonBlank(b.aggregateType, "aggregateType");
        this.type = b.type;
        if (b.seqAssigned && b.managedSeq) {
            throw new IllegalStateException(
                    "seq(long) and managedSeq() are mutually exclusive — either the application "
                            + "assigns the sequence number or Tandem does");
        }
        this.seq = b.seq;
        this.managedSeq = b.managedSeq;
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

    /**
     * The app-assigned sequence number.
     *
     * @throws IllegalStateException if this message leaves the number to Tandem
     *                               ({@link Builder#managedSeq()}) — there is none until the row is
     *                               inserted, so any value returned here would be a fiction. Guard
     *                               with {@link #managedSeq()}.
     */
    public long seq() {
        if (managedSeq) {
            throw new IllegalStateException(
                    "seq is assigned by Tandem at insert and is not available on the message");
        }
        return seq;
    }

    /** Whether Tandem assigns {@code seq} at insert instead of the application (HLD-managed-seq §4.1). */
    public boolean managedSeq() {
        return managedSeq;
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
                && managedSeq == other.managedSeq
                && aggregateId.equals(other.aggregateId)
                && aggregateType.equals(other.aggregateType)
                && Objects.equals(type, other.type)
                && Arrays.equals(payload, other.payload)
                && Objects.equals(contentType, other.contentType)
                && headers.equals(other.headers);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(aggregateId, aggregateType, type, seq, managedSeq, contentType, headers);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "OutboxMessage{aggregateId=" + aggregateId
                + ", aggregateType=" + aggregateType
                + ", type=" + type
                + ", seq=" + (managedSeq ? "managed" : seq)
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
        private boolean seqAssigned;
        private boolean managedSeq;
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

        /**
         * The app-assigned, per-aggregate monotonically increasing sequence number (HLD §4.2).
         * Mutually exclusive with {@link #managedSeq()}.
         */
        public Builder seq(long seq) {
            this.seq = seq;
            this.seqAssigned = true;
            return this;
        }

        /**
         * Leaves the sequence number to Tandem, for an aggregate with no version to take it from
         * (HLD-managed-seq §4.1): the write-side adapter omits the column and the database assigns
         * the value, at no cost on the caller's transaction.
         *
         * <p>Opting in is per message, so one aggregate type can be managed while another stays
         * app-assigned. It changes what a consumer reads in the {@code seq} CloudEvents extension —
         * Tandem's own counter, large and not dense per aggregate, rather than the aggregate's
         * version — so it is a contract change for an existing stream (HLD-managed-seq §5).
         *
         * <p>Mutually exclusive with {@link #seq(long)}: setting both fails in {@link #build()}.
         */
        public Builder managedSeq() {
            this.managedSeq = true;
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

        /**
         * @throws NullPointerException  if a required field ({@code aggregateId}, {@code payload}) is unset
         * @throws IllegalStateException if both {@link #seq(long)} and {@link #managedSeq()} were set
         */
        public OutboxMessage build() {
            return new OutboxMessage(this);
        }
    }
}
