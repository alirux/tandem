package com.codingful.tandem.core.port;

import java.util.OptionalLong;

/**
 * Optional inbound-causality port (LLD-core §2.5, §9): the consumer declares the timestamp of the event
 * that caused the mutation it is about to write, so the write-side can merge it into the aggregate's
 * Lamport clock ({@code max(local, inbound) + 1}).
 *
 * <p><b>Reserved API — designed but not implemented.</b> This is one member of the cross-aggregate
 * causal-ordering surface (HLD §9, {@code docs/HLD-causal-ordering.md}); today <b>nothing in Tandem reads
 * or wires it</b>, and there is no switch that turns the feature on. It ships so that building the
 * feature stays an additive change. The full inventory of what is published versus what is missing is
 * in {@code docs/HLD-causal-ordering.md} §0 — read that before changing anything here.
 */
public interface CausalContext {

    /** A no-op context — every mutation is a causal root. */
    CausalContext NONE = OptionalLong::empty;

    /** The inbound Lamport timestamp, or empty when this mutation is a causal root. */
    OptionalLong inboundTimestamp();
}
