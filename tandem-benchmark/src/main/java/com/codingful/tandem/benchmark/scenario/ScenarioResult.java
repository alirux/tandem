package com.codingful.tandem.benchmark.scenario;

import java.util.Map;

/**
 * {@code passed} reflects <b>correctness only</b> — zero ordering violations and zero lost events
 * (HLD-load-testing.md §6: correctness is non-negotiable regardless of performance). Throughput and
 * latency numbers are informational: on a non-reference host they are not KPI-gated
 * (HLD-load-testing.md §5.1), so they live in {@code metrics}/{@code summary}, not in {@code passed}.
 */
public record ScenarioResult(String scenarioId, boolean passed, String summary, Map<String, Object> metrics) {
}
