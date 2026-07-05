package com.codingful.tandem.kafka;

import com.codingful.tandem.core.exception.OutboxDispatchException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

/**
 * Default Kafka error mapping (LLD-kafka §4):
 *
 * <ul>
 *   <li><b>Permanent</b> (fail-fast, no wasted retries) — data/config errors that will never succeed:
 *       {@link RecordTooLargeException}, {@link SerializationException}, authorization/authentication
 *       errors, {@link InvalidTopicException}, {@link UnsupportedVersionException}.</li>
 *   <li><b>Retriable</b> — everything else: transient {@code RetriableException}s surfacing after the
 *       producer's internal retries exhausted, <b>and unknown errors by default</b>.</li>
 * </ul>
 */
public final class DefaultErrorClassifier implements ErrorClassifier {

    @Override
    public OutboxDispatchException classify(Throwable cause) {
        boolean permanent = cause instanceof RecordTooLargeException
                || cause instanceof SerializationException
                || cause instanceof AuthorizationException
                || cause instanceof AuthenticationException
                || cause instanceof InvalidTopicException
                || cause instanceof UnsupportedVersionException;
        boolean retriable = !permanent;   // RetriableException and unknown errors default to retriable
        String message = "Kafka publish failed (" + (retriable ? "retriable" : "permanent") + "): "
                + cause.getClass().getSimpleName()
                + (cause.getMessage() == null ? "" : " - " + cause.getMessage());
        return new OutboxDispatchException(message, retriable, cause);
    }
}
