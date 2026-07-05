package com.codingful.tandem.benchmark;

/** Names of the harness-owned headers carried on the outbox row (LLD-benchmark §5.1) — benchmark-only, never part of Tandem's own wire contract. */
final class BenchmarkHeaders {

    private BenchmarkHeaders() {
    }

    /** Insert-time {@code System.nanoTime()}, written by {@link LoadGenerator} — the always-on latency proxy. */
    static final String T0_NANOS = "bench-t0-nanos";
}
