package com.codingful.tandem.kafka;

import com.codingful.tandem.core.exception.OutboxDispatchException;

/**
 * Classifies a Kafka send failure into an {@link OutboxDispatchException} carrying the
 * retriable/permanent verdict (Q17, LLD-kafka §4), so {@code tandem-jdbc} routes it to
 * {@code markForRetry} or {@code markFailed} without knowing any Kafka type. Pluggable SPI; the
 * default is {@link DefaultErrorClassifier}.
 */
@FunctionalInterface
public interface ErrorClassifier {

    /**
     * @param cause the failure the Kafka producer reported
     * @return the verdict to hand back to {@code tandem-jdbc}
     */
    OutboxDispatchException classify(Throwable cause);
}
