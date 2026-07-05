package com.codingful.tandem.core;

import java.time.Instant;

/**
 * One delivery attempt's outcome, handed to the (opt-in) {@link
 * com.codingful.tandem.core.port.AttemptRecorder} (LLD-core §2.5, HLD-attempt-archive §2). Mirrors
 * the {@code tandem_outbox_attempt} columns. Success-only fields ({@code topic}/{@code partition}/
 * {@code kafkaOffset}) and failure-only fields ({@code errorClass}/{@code errorMessage}/
 * {@code errorDetail}) are {@code null} on the other path.
 *
 * <p>In the basic round the recorder is the no-op default, so this is built only when the archive is
 * enabled (the construction itself is guarded — HLD-attempt-archive §5).
 */
public record AttemptOutcome(long outboxId,
                             AggregateId aggregateId,
                             String aggregateType,
                             int attemptNumber,
                             AttemptStatus status,
                             Instant startedAt,
                             Instant finishedAt,
                             Integer latencyMs,
                             String workerId,
                             String topic,
                             Integer partition,
                             Long kafkaOffset,
                             String errorClass,
                             String errorMessage,
                             String errorDetail,
                             String traceId,
                             String correlationId) {
}
