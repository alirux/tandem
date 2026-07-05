package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.BenchmarkEnvironment;

/** The shared environment + sizing every scenario runs against. */
public record ScenarioContext(BenchmarkEnvironment environment, BenchmarkConfig config) {
}
