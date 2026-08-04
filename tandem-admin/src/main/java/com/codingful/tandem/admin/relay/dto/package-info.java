/**
 * Wire representations of the OpenAPI {@code Relay} tag's schemas — deliberately independent of
 * {@code tandem-core}'s/{@code tandem-jdbc}'s own relay types (HLD-admin-api §4.1). Public because a
 * Java package cannot see across a sub-package boundary at package-private visibility; these are
 * still purely 1:1 renderings of the published, public OpenAPI schema, so nothing is exposed here
 * that a REST client cannot already see on the wire.
 */
package com.codingful.tandem.admin.relay.dto;
