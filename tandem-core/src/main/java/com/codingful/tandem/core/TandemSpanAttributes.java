package com.codingful.tandem.core;

/**
 * Canonical names of the attributes a relay publish span carries (HLD-tracing.md §6.4). Like
 * {@link TandemHeaders}, these are a cross-version contract — an operator's saved query or dashboard
 * panel in the tracing backend is written against them — so they are declared once here, in the
 * dependency-free core, and referenced by name from every
 * {@link com.codingful.tandem.core.port.TandemSpanRecorder} adapter rather than re-typed as literals.
 * Uniform {@code tandem.*} prefix, so one search finds every span this library emits regardless of
 * which tracing library exported it.
 *
 * <p>Structural identifiers only: there is deliberately no attribute for the payload, a header value
 * or an error body (§6.4).
 */
public final class TandemSpanAttributes {

    private TandemSpanAttributes() {
    }

    /** The outbox row id — the join key to the Admin API's message detail. */
    public static final String OUTBOX_ROW_ID = "tandem.outbox.row_id";

    /** The record's aggregate type. */
    public static final String AGGREGATE_TYPE = "tandem.aggregate.type";

    /** The record's aggregate id. */
    public static final String AGGREGATE_ID = "tandem.aggregate.id";

    /** Delivery attempts before this one — non-zero means the span covers a retry. */
    public static final String ATTEMPTS = "tandem.attempts";

    /** The resolved destination topic. */
    public static final String TOPIC = "tandem.topic";

    /**
     * The row's correlation id — the join key between the tracing backend and the Admin API's search
     * (HLD-tracing.md §4.1), so an investigation that starts in either can cross to the other.
     */
    public static final String CORRELATION_ID = "tandem.correlation_id";
}
