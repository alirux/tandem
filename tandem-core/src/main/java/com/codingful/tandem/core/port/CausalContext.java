package com.codingful.tandem.core.port;

import java.util.OptionalLong;

/**
 * Optional inbound-causality port (LLD-core §2.5, §9). The default reports no inbound timestamp, so a
 * mutation is treated as a causal root. A real adapter ships on the consumer side ({@code tandem-spring}).
 */
public interface CausalContext {

    /** A no-op context — every mutation is a causal root. */
    CausalContext NONE = OptionalLong::empty;

    /** The inbound Lamport timestamp, or empty when this mutation is a causal root. */
    OptionalLong inboundTimestamp();
}
