/**
 * The hexagonal <b>ports</b> (LLD-core §2): interfaces the core defines and the adapters implement,
 * so the core never depends on any adapter. Persistence ports
 * ({@link com.codingful.tandem.core.port.OutboxRepository} write-side,
 * {@link com.codingful.tandem.core.port.OutboxStore} relay-side,
 * {@link com.codingful.tandem.core.port.OutboxQuery} the Admin API's read side,
 * {@link com.codingful.tandem.core.port.DiscardService} the Admin API's discard action,
 * {@link com.codingful.tandem.core.port.RelayQuery}/{@link com.codingful.tandem.core.port.RelayControl}
 * the Admin API's relay observability and control) are implemented by {@code tandem-jdbc}; the
 * publish port ({@link com.codingful.tandem.core.port.OutboxDispatcher}) by {@code tandem-kafka}.
 *
 * <p>The optional ports ({@link com.codingful.tandem.core.port.TandemMetrics},
 * {@link com.codingful.tandem.core.port.TracePropagator},
 * {@link com.codingful.tandem.core.port.TandemSpanRecorder}) ship a no-op default and an
 * {@code isEnabled()} guard, so the off-path costs nothing until a real adapter is wired.
 *
 * <p>{@link com.codingful.tandem.core.port.CausalContext} is <b>reserved API</b>, not an optional port
 * in that sense: it has no {@code isEnabled()} (an empty {@code OptionalLong} <i>is</i> the guard) and,
 * more importantly, nothing in Tandem wires it — the causal-ordering feature is designed but not built
 * (HLD §9; inventory in {@code docs/HLD-causal-ordering.md} §0).
 */
package com.codingful.tandem.core.port;
