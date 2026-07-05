package com.codingful.tandem.core;

/**
 * Names of the CloudEvents extension attributes Tandem sets and their Kafka <b>binary-mode</b> header
 * forms (LLD-kafka §3.1/§3.3). In binary mode an extension {@code foo} becomes the header
 * {@code ce_foo}. Declared in the dependency-free core because the consumer-side adapters read these
 * exact names off the wire (HLD §1.4) — they must never drift.
 *
 * <p>The standard CloudEvents attributes (id/source/type/subject/time) are written by the CloudEvents
 * SDK; only Tandem's own extensions are named here.
 */
public final class CloudEventsHeaders {

    private CloudEventsHeaders() {
    }

    /** Binary-mode header prefix: an extension {@code foo} is written as {@code ce_foo}. */
    public static final String BINARY_PREFIX = "ce_";

    /** Per-aggregate sequence (always present). Extension {@code seq} → header {@code ce_seq}. */
    public static final String EXT_SEQ = "seq";
    public static final String CE_SEQ = BINARY_PREFIX + EXT_SEQ;

    /** Partition key (always equals the aggregate id). Extension {@code partitionkey} → header {@code ce_partitionkey}. */
    public static final String EXT_PARTITION_KEY = "partitionkey";
    public static final String CE_PARTITION_KEY = BINARY_PREFIX + EXT_PARTITION_KEY;

    /**
     * Lamport / logical clock — present only when causal ordering is enabled (HLD §9; off by default).
     * Extension {@code logicalclock} → header {@code ce_logicalclock}. ("lamport" is the design term;
     * "logicalclock" is the on-the-wire name — LLD-kafka §3.3.)
     */
    public static final String EXT_LOGICAL_CLOCK = "logicalclock";
    public static final String CE_LOGICAL_CLOCK = BINARY_PREFIX + EXT_LOGICAL_CLOCK;
}
