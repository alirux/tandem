/**
 * The OpenTelemetry tracing adapters (HLD-tracing.md §5): {@link
 * com.codingful.tandem.tracing.otel.OtelTracePropagator} implements the write-side {@link
 * com.codingful.tandem.core.port.TracePropagator} port and {@link
 * com.codingful.tandem.tracing.otel.OtelTandemSpanRecorder} the relay-side {@link
 * com.codingful.tandem.core.port.TandemSpanRecorder} port, so an application that instruments itself
 * with the OpenTelemetry SDK directly gets both tracing modes across the outbox boundary —
 * propagation and the {@code tandem.relay.publish} span (§6).
 *
 * <p>For applications outside Spring: a Spring one is served by {@code tandem-spring-producer} and
 * {@code tandem-spring-relay}, whose adapters bridge to Micrometer Tracing and are wired from
 * {@code tandem.tracing.*} properties. Here there are no properties and no autoconfiguration —
 * constructing an adapter and passing it in <i>is</i> the opt-in (§9: enablement is always explicit,
 * never triggered by a library's mere presence on the classpath).
 *
 * <p>Both sides may be used independently: the propagator alone gives propagation mode, and adding
 * the recorder where the relay runs gives instrumented mode. Only the OpenTelemetry <i>API</i> is
 * redistributed — the SDK stays the application's own choice.
 */
package com.codingful.tandem.tracing.otel;
