/**
 * Tandem's Admin API (HLD-admin-api.md, admin-api.openapi.yaml): an optional, API-first REST
 * operations layer over the outbox and the relay, off by default (gated on
 * {@code tandem.admin.enabled}). Ships in slices (IMPLEMENTATION-PLAN-admin-api.md); slice 1
 * (read-only outbox endpoints) is implemented in {@link com.codingful.tandem.admin.outbox}.
 *
 * <p>This root package holds only cross-cutting infrastructure shared across every feature: the
 * {@link com.codingful.tandem.admin.TandemAdminAutoConfiguration} entry point, the DB-derived
 * adapter beans a future feature package may also need, generic error handling
 * ({@link com.codingful.tandem.admin.TandemAdminExceptionHandler}), and the shared RFC 9457 builder
 * ({@link com.codingful.tandem.admin.ProblemDetails}). Each REST feature gets its own sub-package —
 * its use cases, controller, DTOs, and feature-specific problem-slug mapping — imported by the root
 * autoconfiguration so it inherits that class's gating. Later slices (replay/discard join
 * {@code outbox}; relay control gets its own {@code relay} sub-package) follow the same pattern.
 *
 * <p>Depends on {@code tandem-jdbc} for outbox access and, for the REST layer, on Spring — kept
 * {@code compileOnly} so no Spring version is redistributed to a consumer, the same footprint
 * discipline as {@code tandem-spring-relay}. Never a dependency of the client write-side.
 */
package com.codingful.tandem.admin;
