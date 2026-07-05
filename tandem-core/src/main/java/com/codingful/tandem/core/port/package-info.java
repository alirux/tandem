/**
 * The hexagonal <b>ports</b> (LLD-core §2): interfaces the core defines and the adapters implement,
 * so the core never depends on any adapter. Persistence ports
 * ({@link com.codingful.tandem.core.port.OutboxRepository} write-side,
 * {@link com.codingful.tandem.core.port.OutboxStore} relay-side) are implemented by {@code tandem-jdbc};
 * the publish port ({@link com.codingful.tandem.core.port.OutboxDispatcher}) by {@code tandem-kafka}.
 *
 * <p>The optional ports ({@link com.codingful.tandem.core.port.TandemMetrics},
 * {@link com.codingful.tandem.core.port.AttemptRecorder},
 * {@link com.codingful.tandem.core.port.TracePropagator},
 * {@link com.codingful.tandem.core.port.CausalContext}) ship a no-op default and an {@code isEnabled()}
 * guard, so the off-path costs nothing until a real adapter is wired.
 */
package com.codingful.tandem.core.port;
