package com.codingful.tandem.core;

/**
 * Lamport-clock merge (HLD-causal-ordering.md §3) — pure logic in the core.
 *
 * <p><b>Reserved API — designed but not implemented.</b> One member of the cross-aggregate
 * causal-ordering surface (HLD §9, {@code docs/HLD-causal-ordering.md}); <b>no product code calls
 * this</b> — the write-side advance that would use it does not exist. Inventory and status:
 * {@code docs/HLD-causal-ordering.md} §0.
 */
public final class LamportClock {

    private LamportClock() {
    }

    /** The merged timestamp on receiving {@code inbound} given a {@code local} clock: {@code max(local, inbound) + 1}. */
    public static long merge(long local, long inbound) {
        return Math.max(local, inbound) + 1;
    }
}
