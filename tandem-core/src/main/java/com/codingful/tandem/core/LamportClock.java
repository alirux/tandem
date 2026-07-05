package com.codingful.tandem.core;

/**
 * Lamport-clock merge (HLD §9.2). Pure logic in the core; the causal-ordering feature that uses it is
 * opt-in and off by default (§9).
 */
public final class LamportClock {

    private LamportClock() {
    }

    /** The merged timestamp on receiving {@code inbound} given a {@code local} clock: {@code max(local, inbound) + 1}. */
    public static long merge(long local, long inbound) {
        return Math.max(local, inbound) + 1;
    }
}
