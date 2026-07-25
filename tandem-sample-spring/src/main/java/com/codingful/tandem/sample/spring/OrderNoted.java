package com.codingful.tandem.sample.spring;

/**
 * A plain domain event for the Spring application-events tier. The application publishes it inside a
 * transaction; {@link OrderNotedMapper} converts it to an outbox row. It carries the {@code seq} taken
 * from the aggregate's version (HLD §4.2).
 */
record OrderNoted(String orderId, long seq) {
}
