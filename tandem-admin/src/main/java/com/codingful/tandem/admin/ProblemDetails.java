package com.codingful.tandem.admin;

import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;

/**
 * Builds RFC 9457 Problem Details responses (HLD-admin-api §3): {@code type} is always a canonical
 * {@code https://tandem.codingful.com/problems/{slug}} URL, never {@code about:blank}. Shared by
 * every feature package's exception handling, so every error — regardless of which endpoint raised
 * it — takes the same shape.
 */
public final class ProblemDetails {

    private static final URI PROBLEM_BASE = URI.create("https://tandem.codingful.com/problems/");

    private ProblemDetails() {
    }

    /**
     * @param status the HTTP status to respond with
     * @param slug   the stable, kebab-case problem-type identifier (never changes once published)
     * @param title  a short, human-readable summary of the problem type
     * @param detail human-readable detail for this occurrence — never {@code null}: the contract's
     *               {@code ProblemDetail.detail} is a plain, non-nullable string, so an empty or
     *               absent detail would fail conformance the same way Jackson's default null
     *               rendering already did once (HLD-admin-api, IMPLEMENTATION-PLAN-admin-api.md §6)
     */
    public static ResponseEntity<ProblemDetail> of(HttpStatus status, String slug, String title, String detail) {
        ProblemDetail body = ProblemDetail.forStatusAndDetail(status, detail);
        body.setType(PROBLEM_BASE.resolve(slug));
        body.setTitle(title);
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_PROBLEM_JSON).body(body);
    }
}
