/**
 * The relay-control feature of the Admin API (slice 3, HLD-admin-api §2/§4.1, the OpenAPI {@code Relay}
 * tag): status, pause/resume (whole relay or one bucket), per-bucket/per-worker observability, and
 * force-release. {@code RelayAdminConfiguration} is the package's one public type — the wiring entry
 * point {@code TandemAdminAutoConfiguration} imports; the use case, the controller, and the
 * exception mapping are internal implementation details with no reason to be visible outside this
 * package. The request/response wire models are in the {@link com.codingful.tandem.admin.relay.dto}
 * sub-package, public out of Java necessity (a sub-package cannot see package-private members
 * here), not because they are meant for use outside the REST layer.
 */
package com.codingful.tandem.admin.relay;
