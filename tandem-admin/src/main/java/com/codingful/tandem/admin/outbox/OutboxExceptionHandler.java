package com.codingful.tandem.admin.outbox;

import com.codingful.tandem.admin.ProblemDetails;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * The outbox feature's own RFC 9457 mapping — exceptions with an outbox-specific problem slug. The
 * module-wide {@code TandemAdminExceptionHandler} (generic 400/500) coexists with this one; Spring
 * dispatches to whichever advice's {@code @ExceptionHandler} matches.
 *
 * <p><b>{@code @Order(0)} is load-bearing.</b> An advice bean with no {@code @Order} defaults to
 * {@code Ordered.LOWEST_PRECEDENCE} — the same value {@code TandemAdminExceptionHandler} sets
 * explicitly — and {@code ExceptionHandlerExceptionResolver} breaks a tie by bean *registration*
 * order, which favours the generic advice (imported before this feature's own configuration). Without
 * an explicit, higher-precedence order here, {@code OutboxMessageNotFoundException} would still be
 * caught by the generic advice's {@code Exception.class} handler and rendered as a 500 instead of a
 * 404 — verified against a real running Spring context, not just a unit test, precisely because this
 * failure mode does not reproduce in every kind of test (HLD-admin-api, IMPLEMENTATION-PLAN-admin-api.md).
 */
@RestControllerAdvice
@Order(0)
class OutboxExceptionHandler {

    @ExceptionHandler(OutboxMessageNotFoundException.class)
    ResponseEntity<ProblemDetail> handleNotFound(OutboxMessageNotFoundException e) {
        return ProblemDetails.of(HttpStatus.NOT_FOUND, "not-found", "Resource not found", e.getMessage());
    }

    @ExceptionHandler(MessageNotReplayableException.class)
    ResponseEntity<ProblemDetail> handleNotReplayable(MessageNotReplayableException e) {
        return ProblemDetails.of(HttpStatus.CONFLICT, "message-not-replayable",
                "Message is not in a replayable state", e.getMessage());
    }

    @ExceptionHandler(MessageNotDiscardableException.class)
    ResponseEntity<ProblemDetail> handleNotDiscardable(MessageNotDiscardableException e) {
        return ProblemDetails.of(HttpStatus.CONFLICT, "message-not-discardable",
                "Message is not in a discardable state", e.getMessage());
    }

    @ExceptionHandler(OrderingBreakNotAcknowledgedException.class)
    ResponseEntity<ProblemDetail> handleOrderingBreakNotAcknowledged(OrderingBreakNotAcknowledgedException e) {
        return ProblemDetails.of(HttpStatus.BAD_REQUEST, "ordering-break-not-acknowledged",
                "Ordering-break acknowledgement is required to discard", e.getMessage());
    }

    @ExceptionHandler(ReplayNoSelectorException.class)
    ResponseEntity<ProblemDetail> handleReplayNoSelector(ReplayNoSelectorException e) {
        return ProblemDetails.of(HttpStatus.BAD_REQUEST, "replay-no-selector",
                "At least one replay selector is required", e.getMessage());
    }
}
