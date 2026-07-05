package com.codingful.tandem.core;

/**
 * Canonical names of the entries Tandem stores in {@code tandem_outbox.headers} (LLD-core §4). They
 * are part of the cross-version wire contract (HLD §1.4), so they are declared once here — in the
 * dependency-free core — and referenced by name everywhere (never re-typed as literals).
 */
public final class TandemHeaders {

    private TandemHeaders() {
    }

    /**
     * The payload media type. Set from {@link OutboxMessage#contentType()} at insert (LLD-jdbc §2);
     * read by the relay as the CloudEvents {@code datacontenttype} (LLD-kafka §3.2).
     */
    public static final String CONTENT_TYPE = "content-type";

    /** Optional schema reference for the payload; read as the CloudEvents {@code dataschema} (LLD-kafka §3.2). */
    public static final String DATA_SCHEMA = "dataschema";

    /** Correlation id, propagated as a passthrough header (LLD-kafka §3.3). */
    public static final String CORRELATION_ID = "correlation-id";

    /** Causation id — carried only when the opt-in causal-ordering feature is on (HLD §9; off by default). */
    public static final String CAUSATION_ID = "causation_id";

    /** W3C Trace Context parent — carried only when tracing is enabled (HLD §7.2; off by default). */
    public static final String TRACEPARENT = "traceparent";

    /** W3C Trace Context state — carried only when tracing is enabled (HLD §7.2; off by default). */
    public static final String TRACESTATE = "tracestate";
}
