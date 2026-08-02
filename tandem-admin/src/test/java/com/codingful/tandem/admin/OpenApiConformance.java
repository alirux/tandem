package com.codingful.tandem.admin;

import static org.assertj.core.api.Assertions.assertThat;

import com.atlassian.oai.validator.OpenApiInteractionValidator;
import com.atlassian.oai.validator.model.Request;
import com.atlassian.oai.validator.model.SimpleRequest;
import com.atlassian.oai.validator.model.SimpleResponse;
import com.atlassian.oai.validator.report.ValidationReport;
import jakarta.servlet.http.Cookie;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.ResultMatcher;

/**
 * Conformance against {@code admin-api.openapi.yaml} (HLD-admin-api §5) via
 * {@code openapi-request-validator-core} — hand-adapted rather than that library's own
 * {@code -mockmvc} module, which is still built against {@code javax.servlet}/Spring 5 (checked
 * against its published POM, 2026-08-02) and cannot run on this project's Jakarta/Spring 6 baseline.
 * The core module has no servlet dependency at all, so this adapter is the only Jakarta-specific part.
 *
 * <p>Public: shared across every feature package's own controller tests (e.g. {@code outbox}'s),
 * since the committed OpenAPI contract is a module-wide concern, not one feature's.
 */
public final class OpenApiConformance {

    private static final OpenApiInteractionValidator VALIDATOR =
            OpenApiInteractionValidator.createFor(specPath()).build();

    private OpenApiConformance() {
    }

    /** A {@code ResultMatcher} usable in a normal {@code mockMvc.perform(...).andExpect(...)} chain. */
    public static ResultMatcher conformsToOpenApi() {
        return result -> assertConforms(result.getRequest(), result.getResponse());
    }

    /**
     * Asserts the response half of the given real MockMvc interaction conforms to the committed
     * OpenAPI contract for that operation. Deliberately validates the <b>response only</b>, not the
     * request: several tests exercise the 400 path by sending an intentionally invalid parameter, and
     * what those must prove is that Tandem's own *error response* still matches the contract — not
     * that the deliberately-bad request does.
     */
    private static void assertConforms(MockHttpServletRequest request, MockHttpServletResponse response) {
        Request validatorRequest = toValidatorRequest(request);
        com.atlassian.oai.validator.model.Response validatorResponse = toValidatorResponse(response);
        ValidationReport report = VALIDATOR.validateResponse(
                validatorRequest.getPath(), validatorRequest.getMethod(), validatorResponse);
        assertThat(report.hasErrors())
                .as(report.getMessages().toString())
                .isFalse();
    }

    private static Request toValidatorRequest(MockHttpServletRequest request) {
        SimpleRequest.Builder builder =
                new SimpleRequest.Builder(request.getMethod(), request.getRequestURI());
        for (String name : Collections.list(request.getParameterNames())) {
            builder.withQueryParam(name, request.getParameterValues(name));
        }
        for (Cookie cookie : orEmpty(request.getCookies())) {
            builder.withHeader("Cookie", cookie.getName() + '=' + cookie.getValue());
        }
        return builder.build();
    }

    private static com.atlassian.oai.validator.model.Response toValidatorResponse(MockHttpServletResponse response) {
        SimpleResponse.Builder builder = SimpleResponse.Builder.status(response.getStatus());
        for (String name : response.getHeaderNames()) {
            builder.withHeader(name, List.copyOf(response.getHeaders(name)));
        }
        byte[] content = response.getContentAsByteArray();
        if (content.length > 0) {
            builder.withBody(new String(content, StandardCharsets.UTF_8));
        }
        return builder.build();
    }

    private static Cookie[] orEmpty(Cookie[] cookies) {
        return cookies == null ? new Cookie[0] : cookies;
    }

    /** Locate {@code docs/admin-api.openapi.yaml} by walking up from the working directory. */
    private static String specPath() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("docs/admin-api.openapi.yaml");
            if (Files.exists(candidate)) {
                return candidate.toString();
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("could not locate docs/admin-api.openapi.yaml");
    }
}
