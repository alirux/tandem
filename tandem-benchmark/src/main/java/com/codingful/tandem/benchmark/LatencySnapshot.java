package com.codingful.tandem.benchmark;

import java.time.Duration;

/** Percentile snapshot over one measurement window (HLD-load-testing.md §2.2). */
public record LatencySnapshot(long count, Duration p50, Duration p95, Duration p99, Duration p999) {
}
