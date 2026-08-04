package com.codingful.tandem.admin.relay;

import com.codingful.tandem.admin.ProblemDetails;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * The relay feature's own RFC 9457 mapping. See {@code OutboxExceptionHandler}'s javadoc for why
 * {@code @Order(0)} is load-bearing here too: an advice with no explicit order ties with the module-wide
 * {@code TandemAdminExceptionHandler} and loses on registration order, turning these into 500s. Also see
 * that same javadoc for why {@code basePackages} is load-bearing, not decorative: it keeps this advice
 * from ever applying to a host application's own controllers in embedded mode.
 */
@RestControllerAdvice(basePackages = "com.codingful.tandem.admin.relay")
@Order(0)
class RelayExceptionHandler {

    @ExceptionHandler(RelayBucketNotFoundException.class)
    ResponseEntity<ProblemDetail> handleNotFound(RelayBucketNotFoundException e) {
        return ProblemDetails.of(HttpStatus.NOT_FOUND, "not-found", "Resource not found", e.getMessage());
    }

    @ExceptionHandler(RelayCoordinationUnsupportedException.class)
    ResponseEntity<ProblemDetail> handleCoordinationUnsupported(RelayCoordinationUnsupportedException e) {
        return ProblemDetails.of(HttpStatus.CONFLICT, "relay-coordination-unsupported",
                "Not available under this relay coordination mode", e.getMessage());
    }
}
