package com.codingful.tandem.benchmark.scenario;

/** Common contract every load-test scenario implements (HLD-load-testing.md §4, LLD-benchmark §8). */
public interface Scenario {

    /** Short id matching the HLD scenario table, e.g. {@code "S1"}. */
    String id();

    ScenarioResult run(ScenarioContext context) throws Exception;
}
