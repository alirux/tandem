package com.codingful.tandem.admin;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxRowDetail;
import com.codingful.tandem.core.OutboxRowView;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.port.OutboxQuery;
import com.codingful.tandem.core.port.OutboxStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.web.servlet.MockMvc;

/**
 * The one test in this module that boots a <b>real</b> {@code WebApplicationContext} — every other
 * Admin API test uses {@code MockMvcBuilders.standaloneSetup(...)}, which resolves
 * {@code @ExceptionHandler} advice precedence <em>differently</em> from a real Spring context: it
 * does not consult {@code @Order} at all for manually-supplied advice instances, where a real context
 * does (via {@code ControllerAdviceBean} discovery). A real bug — the module-wide generic advice
 * shadowing the outbox feature's specific 404 mapping — passed all 31 other tests in this module and
 * was only caught by manually curling the running sample app (see
 * {@code IMPLEMENTATION-PLAN-admin-api.md} §6). This test exists so that regression is automated
 * instead of depending on someone remembering to curl a real deployment again.
 *
 * <p>Tagged {@code boot3-only} and excluded from {@code bootFourTest} (see the module's
 * {@code build.gradle.kts}): on the Boot 4.x line, {@code @AutoConfigureMockMvc} — from
 * {@code spring-boot-test-autoconfigure} — does not contribute a {@code MockMvc} bean under this
 * module's dual-generation test setup, a test-support-only discrepancy, not a compatibility gap in
 * {@code tandem-admin}'s own production code (which {@code TandemAdminAutoConfigurationTest} and
 * {@code OutboxAdminConfigurationTest} already verify on both generations).
 */
@SpringBootTest(classes = TandemAdminEndToEndTest.TestApplication.class, properties = "tandem.admin.enabled=true")
@AutoConfigureMockMvc
@Tag("boot3-only")
class TandemAdminEndToEndTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void GIVEN_a_missing_id_WHEN_fetched_over_a_real_web_context_THEN_the_feature_specific_404_wins_over_the_generic_advice()
            throws Exception {
        mockMvc.perform(get("/tandem/admin/v1/outbox/messages/{id}", 999_999))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/not-found"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    /**
     * A minimal {@code @SpringBootApplication} for this one test — the module itself is a library
     * with no application entry point of its own. {@code @EnableAutoConfiguration} (implied) still
     * discovers {@code TandemAdminAutoConfiguration} via the real
     * {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports} file
     * this module ships, so the wiring under test is the real thing, not a re-creation of it.
     */
    @SpringBootApplication
    static class TestApplication {

        /** Never connected to — {@link #outboxQuery()}/{@link #outboxStore()} below replace the JDBC ones. */
        @Bean
        DataSource dataSource() {
            return new NoopDataSource();
        }

        @Bean
        OutboxQuery outboxQuery() {
            return new OutboxQuery() {
                @Override
                public Map<OutboxStatus, Long> statusCounts() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<OutboxRowView> search(OutboxSearchCriteria criteria) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Optional<OutboxRowDetail> findById(long id) {
                    return Optional.empty();
                }
            };
        }

        @Bean
        OutboxStore outboxStore() {
            return new OutboxStore() {
                @Override
                public List<OutboxRecord> claimBatch(
                        Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void markDone(long id) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void markForRetry(long id, String error, Duration retryDelay) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void markFailed(long id, String error) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int reclaimExpiredLeases() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int cleanup(Instant doneBefore, int batchSize) {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
