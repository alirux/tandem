package com.codingful.tandem.admin.outbox;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.codingful.tandem.admin.OpenApiConformance;
import com.codingful.tandem.admin.TandemAdminExceptionHandler;
import com.codingful.tandem.admin.TandemAdminObjectMappers;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRowDetail;
import com.codingful.tandem.core.OutboxRowView;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.port.OutboxQuery;
import com.codingful.tandem.test.InMemoryOutbox;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * Real Spring MVC dispatch (MockMvc, standalone — no full ApplicationContext needed) over
 * {@link OutboxAdminController} + the module-wide {@link TandemAdminExceptionHandler} and this
 * feature's own {@link OutboxExceptionHandler}: HTTP status, JSON shape, and the RFC 9457
 * problem+json error path (HLD-admin-api §3).
 */
class OutboxAdminControllerTest {

    private InMemoryOutbox outbox;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        outbox = new InMemoryOutbox();
        ObjectMapper objectMapper = TandemAdminObjectMappers.newDefault();
        OutboxAdminService service = new OutboxAdminService(outbox, outbox, objectMapper, Clock.systemUTC());
        mockMvc = MockMvcBuilders.standaloneSetup(new OutboxAdminController(service))
                // Order matters here, unlike in a real ApplicationContext: standalone setup does not
                // honour @Order across manually-supplied advice instances, so the generic catch-all
                // (TandemAdminExceptionHandler#handleUnexpected matches every Exception) must come
                // last, or it shadows OutboxExceptionHandler's more specific 404 mapping. In production
                // (a real Spring context, @Order(LOWEST_PRECEDENCE) on the class) this is unambiguous.
                .setControllerAdvice(new OutboxExceptionHandler(), new TandemAdminExceptionHandler())
                // Same ObjectMapper the service renders payloads with — otherwise MockMvc's own default
                // converter builds an unconfigured mapper and createdAt etc. serialize as epoch numbers.
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    private void insert(String aggregateId, String payload) {
        outbox.insert(OutboxMessage.builder()
                .aggregateId(aggregateId).aggregateType("Order").seq(1).payload(payload.getBytes()).build());
    }

    @Test
    void GIVEN_an_empty_outbox_WHEN_summary_is_requested_THEN_it_reports_zero_counts_and_no_lag() throws Exception {
        mockMvc.perform(get("/tandem/admin/v1/outbox/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.counts.PENDING").value(0))
                .andExpect(jsonPath("$.lagCount").value(0))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_pending_rows_WHEN_messages_are_searched_THEN_the_list_view_carries_no_payload() throws Exception {
        insert("order-1", "{\"secret\":true}");

        mockMvc.perform(get("/tandem/admin/v1/outbox/messages"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].aggregateId").value("order-1"))
                .andExpect(jsonPath("$.items[0].payload").doesNotExist())
                .andExpect(jsonPath("$.nextCursor").doesNotExist())
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_status_query_param_WHEN_messages_are_searched_THEN_only_matching_rows_are_returned() throws Exception {
        insert("order-1", "{}");
        insert("order-2", "{}");

        mockMvc.perform(get("/tandem/admin/v1/outbox/messages?status=PENDING"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(2))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_an_existing_id_WHEN_the_message_is_fetched_THEN_the_full_detail_including_payload_is_returned() throws Exception {
        insert("order-1", "{\"amount\":42}");
        long id = outbox.all().get(0).id();

        mockMvc.perform(get("/tandem/admin/v1/outbox/messages/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.aggregateId").value("order-1"))
                .andExpect(jsonPath("$.payload.amount").value(42))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_missing_id_WHEN_the_message_is_fetched_THEN_a_problem_json_404_is_returned() throws Exception {
        mockMvc.perform(get("/tandem/admin/v1/outbox/messages/{id}", 999_999))
                .andExpect(status().isNotFound())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/not-found"))
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_an_invalid_status_filter_WHEN_messages_are_searched_THEN_a_problem_json_400_is_returned() throws Exception {
        mockMvc.perform(get("/tandem/admin/v1/outbox/messages?status=NOT_A_STATUS"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/invalid-parameter"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_an_invalid_cursor_WHEN_messages_are_searched_THEN_a_problem_json_400_is_returned() throws Exception {
        mockMvc.perform(get("/tandem/admin/v1/outbox/messages?cursor=not-a-number"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_non_numeric_path_id_WHEN_the_message_is_fetched_THEN_a_problem_json_400_is_returned() throws Exception {
        mockMvc.perform(get("/tandem/admin/v1/outbox/messages/{id}", "not-a-number"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/invalid-parameter"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_valid_cursor_WHEN_messages_are_searched_THEN_only_rows_after_it_are_returned() throws Exception {
        insert("order-1", "{}");
        insert("order-2", "{}");
        long first = outbox.all().get(0).id();

        mockMvc.perform(get("/tandem/admin/v1/outbox/messages?cursor=" + first))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.items[0].aggregateId").value("order-2"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_an_unexpected_failure_WHEN_summary_is_requested_THEN_a_problem_json_500_is_returned() throws Exception {
        ObjectMapper objectMapper = TandemAdminObjectMappers.newDefault();
        OutboxAdminService brokenService =
                new OutboxAdminService(new BrokenOutboxQuery(), outbox, objectMapper, Clock.systemUTC());
        MockMvc brokenMockMvc = MockMvcBuilders.standaloneSetup(new OutboxAdminController(brokenService))
                .setControllerAdvice(new OutboxExceptionHandler(), new TandemAdminExceptionHandler())
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();

        brokenMockMvc.perform(get("/tandem/admin/v1/outbox/summary"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/internal-error"))
                .andExpect(jsonPath("$.status").value(500))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    /** Always fails — exercises the generic 500 handler, a path no valid input reaches. */
    private static final class BrokenOutboxQuery implements OutboxQuery {
        @Override
        public Map<OutboxStatus, Long> statusCounts() {
            throw new IllegalStateException("simulated database failure");
        }

        @Override
        public List<OutboxRowView> search(OutboxSearchCriteria criteria) {
            throw new IllegalStateException("simulated database failure");
        }

        @Override
        public Optional<OutboxRowDetail> findById(long id) {
            throw new IllegalStateException("simulated database failure");
        }
    }
}
