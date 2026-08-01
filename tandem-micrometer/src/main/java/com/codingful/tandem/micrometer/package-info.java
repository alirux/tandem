/**
 * The Micrometer metrics adapter (LLD-micrometer): {@link com.codingful.tandem.micrometer.MicrometerTandemMetrics}
 * implements the {@link com.codingful.tandem.core.port.TandemMetrics} port over a Micrometer
 * {@code MeterRegistry}, so the relay's backlog, failure and worker signals become real gauges and
 * counters (HLD §7).
 *
 * <p>Framework-agnostic — usable by a manually assembled relay exactly as by a Spring one, which is
 * autoconfigured by {@code tandem-spring-relay} rather than by anything in this module (Q31). This
 * module is relay-side only — never on the client write-side (§1.3).
 */
package com.codingful.tandem.micrometer;
