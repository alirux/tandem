package com.codingful.tandem.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * The one place the module's default {@link ObjectMapper} configuration is defined, so production
 * (the autoconfiguration's fallback bean) and tests build an identically-behaving mapper. Existed
 * because Jackson's own default — {@code WRITE_DATES_AS_TIMESTAMPS} enabled — renders
 * {@code createdAt} etc. as epoch numbers, violating the OpenAPI's {@code string}/{@code date-time}
 * schema; caught by the conformance tests (HLD-admin-api §5), not by inspection.
 *
 * <p>Public: every feature package's tests build their {@code ObjectMapper} through this, not
 * {@code new ObjectMapper()}, so a feature's standalone MockMvc test renders JSON exactly like
 * production does.
 */
public final class TandemAdminObjectMappers {

    private TandemAdminObjectMappers() {
    }

    public static ObjectMapper newDefault() {
        return new ObjectMapper()
                .findAndRegisterModules()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
