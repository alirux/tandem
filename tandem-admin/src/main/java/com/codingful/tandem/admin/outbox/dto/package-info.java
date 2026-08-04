/**
 * Wire representations of the OpenAPI {@code Outbox} tag's schemas — deliberately independent of
 * {@code tandem-core}'s types (HLD-admin-api §4). Public because a Java package cannot see across a
 * sub-package boundary at package-private visibility; these are still purely 1:1 renderings of the
 * published, public OpenAPI schema, so nothing is exposed here that a REST client cannot already
 * see on the wire.
 */
package com.codingful.tandem.admin.outbox.dto;
