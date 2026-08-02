/**
 * The outbox feature of the Admin API (HLD-admin-api §2, the OpenAPI {@code Outbox} tag): slice 1
 * (health summary, search, message detail) is implemented here; replay and discard (slice 2) join
 * this package when built, since they act on the same resource. {@code OutboxAdminConfiguration} is
 * the package's one public type — the wiring entry point {@code TandemAdminAutoConfiguration}
 * imports; everything else (the use case, the controller, the DTOs, the exception mapping) is an
 * internal implementation detail with no reason to be visible outside this package.
 */
package com.codingful.tandem.admin.outbox;
