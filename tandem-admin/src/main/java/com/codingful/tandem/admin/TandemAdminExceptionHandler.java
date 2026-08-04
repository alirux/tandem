package com.codingful.tandem.admin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * Generic error handling shared by every feature package's controllers — the cases with no
 * feature-specific meaning: an unparseable parameter, or a genuinely unexpected failure. A feature
 * package (e.g. {@code outbox}) adds its own {@code @RestControllerAdvice} for exceptions that carry
 * a feature-specific problem slug (e.g. {@code not-found}) — Spring dispatches to whichever advice's
 * {@code @ExceptionHandler} matches, so several can coexist. Authentication (401) is deliberately not
 * handled anywhere here — Tandem ships the endpoints, not the authentication, so the host's own
 * security layer owns that response.
 *
 * <p><b>{@code @Order(LOWEST_PRECEDENCE)} is load-bearing, not decorative.</b>
 * {@code ExceptionHandlerExceptionResolver} does not rank {@code @ExceptionHandler} methods by
 * exception-type specificity <em>across</em> different advice beans — only within one bean's own
 * methods. Across beans it tries each advice bean in {@code @Order} sequence and uses the first one
 * with any applicable method at all. Since {@link #handleUnexpected} matches every {@code Exception},
 * an unordered (or earlier-ordered) instance of this class would shadow every feature-specific advice
 * added after it — e.g. {@code outbox}'s {@code OutboxMessageNotFoundException} handler — turning a
 * 404 into a 500. Keeping this advice last is what lets any feature-specific advice's narrower match
 * win.
 *
 * <p>Public: every feature package composes this alongside its own advice, both in production (via
 * the root autoconfiguration's {@code @Import}) and in a feature's own standalone MockMvc tests that
 * want the full, realistic error-rendering behaviour rather than just their own specific mapping.
 *
 * <p><b>{@code basePackages} is load-bearing, not decorative.</b> An unscoped {@code @RestControllerAdvice}
 * applies to every {@code @Controller} in the whole {@code ApplicationContext} — in embedded mode, that
 * includes the host application's own controllers, which have nothing to do with Tandem. Without this,
 * an unrelated {@code IllegalArgumentException} thrown by the host's own endpoint would be rendered as
 * one of *this* module's RFC 9457 problems. Scoped to the whole {@code admin} package tree (a prefix
 * match, so it also covers {@code outbox}, {@code relay}, and any future feature package without needing
 * to list them here) rather than to specific controller classes, which this generic advice — by design —
 * does not know about.
 */
@RestControllerAdvice(basePackages = "com.codingful.tandem.admin")
@Order(Ordered.LOWEST_PRECEDENCE)
public class TandemAdminExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TandemAdminExceptionHandler.class);

    @ExceptionHandler({IllegalArgumentException.class, NumberFormatException.class, MethodArgumentTypeMismatchException.class})
    ResponseEntity<ProblemDetail> handleBadRequest(RuntimeException e) {
        return ProblemDetails.of(HttpStatus.BAD_REQUEST, "invalid-parameter", "Invalid request parameter", e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ProblemDetail> handleUnexpected(Exception e) {
        LOG.error("Admin API request failed", e);
        // Never the exception's own message here: it could carry a DB/query detail an operator
        // should see in the log (already written above, with the Throwable) but a REST client should
        // not (AGENTS logging §5 — the same non-disclosure discipline applies to responses, not only
        // to logs).
        return ProblemDetails.of(HttpStatus.INTERNAL_SERVER_ERROR, "internal-error", "Unexpected internal error",
                "An unexpected error occurred; see server logs for detail");
    }
}
