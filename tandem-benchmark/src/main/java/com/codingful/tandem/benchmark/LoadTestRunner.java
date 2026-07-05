package com.codingful.tandem.benchmark;

import com.codingful.tandem.benchmark.scenario.S1SustainedThroughput;
import com.codingful.tandem.benchmark.scenario.S2LatencyAtNormalLoad;
import com.codingful.tandem.benchmark.scenario.S3HotPartition;
import com.codingful.tandem.benchmark.scenario.S4Saturation;
import com.codingful.tandem.benchmark.scenario.S5WorkerFailover;
import com.codingful.tandem.benchmark.scenario.S6PoisonMessage;
import com.codingful.tandem.benchmark.scenario.S8MultiInstanceLease;
import com.codingful.tandem.benchmark.scenario.Scenario;
import com.codingful.tandem.benchmark.scenario.ScenarioContext;
import com.codingful.tandem.benchmark.scenario.ScenarioResult;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@code ./gradlew :tandem-benchmark:loadTest} entrypoint (LLD-benchmark §9): runs the selected
 * scenarios against one {@link BenchmarkEnvironment}, then prints each result. S7 (causal-ordering
 * overhead) is deferred to the 2nd round (HLD-load-testing.md §4) and is not registered here. S8
 * (multi-instance {@code LEASE} coordination) builds its own additional relay instances on top of the
 * shared environment (LLD-benchmark §8) rather than using the environment's primary {@code SINGLE} pool.
 *
 * <p>Usage: {@code LoadTestRunner [--smoke|--demo] [--duration=<seconds>] [S1,S2,...]}:
 * <ul>
 *   <li>{@code --smoke} — tiny rate/duration, correctness only, no KPI numbers (HLD-load-testing.md §5.1).</li>
 *   <li>{@code --demo} — real relay concurrency (default workers/batchSize/bucketCount) but a short
 *       {@code duration}, so the harness runs against real Docker containers in under a couple of
 *       minutes instead of the multi-hour full-run default. Still not a KPI number on a developer
 *       machine (HLD-load-testing.md §5.1) — just faster to look at.</li>
 *   <li>{@code --duration=<seconds>} — overrides whichever base config's {@code duration} (applied
 *       after {@code --smoke}/{@code --demo}). Useful for a run longer than {@code --demo}'s 20s but
 *       shorter than the 10-minute full-run default — note S3 caps its own active drive phase
 *       independent of this (LLD-benchmark §8): its backlog is structurally serialized per aggregate,
 *       so it does not scale the same way as the other scenarios.</li>
 *   <li>neither flag — the full-run default ({@code BenchmarkConfig.defaults()}, 10 min/scenario).</li>
 * </ul>
 * The scenario list defaults to all six.
 */
public final class LoadTestRunner {

    private static final Map<String, Scenario> ALL_SCENARIOS = registerScenarios();
    private static final Set<String> FLAGS = Set.of("--smoke", "--demo");
    private static final String DURATION_PREFIX = "--duration=";

    public static void main(String[] args) throws Exception {
        boolean smoke = List.of(args).contains("--smoke");
        boolean demo = List.of(args).contains("--demo");
        List<String> scenarioIds = List.of(args).stream()
                .filter(a -> !FLAGS.contains(a) && !a.startsWith(DURATION_PREFIX))
                .findFirst()
                .map(csv -> List.of(csv.split(",")))
                .orElse(List.copyOf(ALL_SCENARIOS.keySet()));

        BenchmarkConfig baseConfig = smoke ? BenchmarkConfig.defaults().toSmoke()
                : demo ? BenchmarkConfig.defaults().toDemo()
                : BenchmarkConfig.defaults();
        BenchmarkConfig config = List.of(args).stream().filter(a -> a.startsWith(DURATION_PREFIX)).findFirst()
                .map(a -> baseConfig.withDuration(Duration.ofSeconds(Long.parseLong(a.substring(DURATION_PREFIX.length())))))
                .orElse(baseConfig);
        System.out.println("Tandem load test — scenarios=" + scenarioIds + ", smoke=" + smoke + ", demo=" + demo
                + ", duration=" + config.duration() + ", workers=" + config.workers());

        try (BenchmarkEnvironment env = new BenchmarkEnvironment(config).start()) {
            ScenarioContext ctx = new ScenarioContext(env, config);
            boolean allPassed = true;
            for (String id : scenarioIds) {
                Scenario scenario = ALL_SCENARIOS.get(id);
                if (scenario == null) {
                    System.out.println("Unknown scenario: " + id + " (known: " + ALL_SCENARIOS.keySet() + ")");
                    continue;
                }
                System.out.println("--- Running " + id + " ---");
                ScenarioResult result = scenario.run(ctx);
                System.out.println((result.passed() ? "PASS " : "FAIL ") + result.scenarioId() + ": " + result.summary());
                allPassed &= result.passed();
            }
            System.out.println(allPassed ? "All scenarios passed correctness." : "One or more scenarios FAILED correctness.");
            if (!allPassed) {
                System.exit(1);
            }
        }
    }

    private static Map<String, Scenario> registerScenarios() {
        Map<String, Scenario> byId = new LinkedHashMap<>();
        for (Scenario s : List.<Scenario>of(
                new S1SustainedThroughput(),
                new S2LatencyAtNormalLoad(),
                new S3HotPartition(),
                new S4Saturation(),
                new S5WorkerFailover(),
                new S6PoisonMessage(),
                new S8MultiInstanceLease())) {
            byId.put(s.id(), s);
        }
        return Collections.unmodifiableMap(byId);
    }

    /** Every scenario id this runner knows about, in HLD table order (S7 excluded — deferred). */
    public static Set<String> knownScenarioIds() {
        return ALL_SCENARIOS.keySet();
    }

    private LoadTestRunner() {
    }
}
