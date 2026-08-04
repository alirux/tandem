/**
 * The outbox feature of the Admin API (HLD-admin-api §2, the OpenAPI {@code Outbox} tag): reads
 * (health summary, search, message detail — slice 1) and the transitions that act on a row (replay,
 * bulk replay, discard — slice 2), since both act on the same resource. {@code OutboxAdminConfiguration}
 * is the package's one public type — the wiring entry point {@code TandemAdminAutoConfiguration}
 * imports; the use case, the controller, and the exception mapping are internal implementation
 * details with no reason to be visible outside this package. The request/response wire models are
 * in the {@link com.codingful.tandem.admin.outbox.dto} sub-package, public out of Java necessity
 * (a sub-package cannot see package-private members here), not because they are meant for use
 * outside the REST layer.
 */
package com.codingful.tandem.admin.outbox;
