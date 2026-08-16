package com.codingful.tandem.admin.outbox;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.codingful.tandem.admin.OpenApiConformance;
import com.codingful.tandem.admin.TandemAdminExceptionHandler;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.jdbc.JdbcDiscardService;
import com.codingful.tandem.jdbc.JdbcOutboxQuery;
import com.codingful.tandem.jdbc.JdbcOutboxRepository;
import com.codingful.tandem.jdbc.JdbcOutboxStore;
import com.codingful.tandem.jdbc.JdbcReplayService;
import com.codingful.tandem.test.TandemTestContainer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * End-to-end slice-1 (reads) and slice-2 (replay/discard) test over a real PostgreSQL
 * (Testcontainers): write via {@link JdbcOutboxRepository}, act through the real
 * {@link JdbcOutboxQuery}/{@link JdbcOutboxStore}/{@link JdbcReplayService}/{@link JdbcDiscardService}
 * and the actual REST layer — no in-memory shortcuts. Confirms the whole chain the unit tests each
 * exercise in isolation actually fits together.
 */
@Tag("integration")
class OutboxAdminIT {

    private static final int BUCKET_COUNT = 256;

    private static TandemTestContainer container;
    private static JdbcOutboxRepository repository;
    private MockMvc mockMvc;

    @BeforeAll
    static void startContainer() {
        container = new TandemTestContainer().start();
        repository = container.newRepository(BUCKET_COUNT);
    }

    @AfterAll
    static void stopContainer() {
        container.close();
    }

    @BeforeEach
    void setUp() throws SQLException {
        try (Connection conn = container.dataSource().getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE tandem_outbox RESTART IDENTITY");
        }
        JdbcOutboxQuery query = new JdbcOutboxQuery(container.dataSource());
        JdbcOutboxStore store = container.newStore(10);
        JdbcReplayService replayService = new JdbcReplayService(container.dataSource());
        JdbcDiscardService discardService = new JdbcDiscardService(container.dataSource());
        OutboxAdminService service =
                new OutboxAdminService(query, store, replayService, discardService, Clock.systemUTC());
        mockMvc = MockMvcBuilders.standaloneSetup(new OutboxAdminController(service))
                .setControllerAdvice(new OutboxExceptionHandler(), new TandemAdminExceptionHandler())
                .build();
    }

    @Test
    void GIVEN_a_row_written_through_the_real_repository_WHEN_summarized_over_http_THEN_it_is_counted() throws Exception {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-it-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build());

        mockMvc.perform(get("/tandem/admin/v1/outbox/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.lagCount").value(1))
                .andExpect(jsonPath("$.counts.PENDING").value(1))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_row_written_through_the_real_repository_WHEN_fetched_by_id_over_http_THEN_the_payload_round_trips() throws Exception {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-it-2").aggregateType("Order").seq(1)
                .payload("{\"amount\":7}".getBytes()).header("correlation-id", "xyz").build());

        long id = idOf("order-it-2");

        mockMvc.perform(get("/tandem/admin/v1/outbox/messages/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.aggregateId").value("order-it-2"))
                .andExpect(jsonPath("$.payload.amount").value(7))
                .andExpect(jsonPath("$.headers['correlation-id']").value("xyz"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_row_written_through_the_real_repository_WHEN_searched_over_http_THEN_the_list_view_omits_the_payload() throws Exception {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-it-3").aggregateType("Order").seq(1).payload("{\"x\":1}".getBytes()).build());

        mockMvc.perform(get("/tandem/admin/v1/outbox/messages?aggregateId=order-it-3"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].aggregateId").value("order-it-3"))
                .andExpect(jsonPath("$.items[0].payload").doesNotExist())
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_failed_row_WHEN_replayed_over_http_THEN_it_is_reset_to_pending() throws Exception {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-it-4").aggregateType("Order").seq(1).payload("{}".getBytes()).build());
        long id = idOf("order-it-4");
        setStatus(id, "boom");

        mockMvc.perform(post("/tandem/admin/v1/outbox/messages/{id}/replay", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                // The replay is visible on the row the operator gets back, not only in the admin log.
                .andExpect(jsonPath("$.replays").value(1))
                .andExpect(jsonPath("$.attempts").value(0))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_pending_row_WHEN_replayed_over_http_THEN_a_problem_json_409_is_returned() throws Exception {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-it-5").aggregateType("Order").seq(1).payload("{}".getBytes()).build());
        long id = idOf("order-it-5");   // still PENDING, not replayable

        mockMvc.perform(post("/tandem/admin/v1/outbox/messages/{id}/replay", id))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/message-not-replayable"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_failed_row_WHEN_discarded_over_http_THEN_it_is_discarded_with_the_reason_recorded_and_last_error_kept() throws Exception {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-it-6").aggregateType("Order").seq(1).payload("{}".getBytes()).build());
        long id = idOf("order-it-6");
        setStatus(id, "boom");

        mockMvc.perform(post("/tandem/admin/v1/outbox/messages/{id}/discard", id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"acknowledgeOrderingBreak\":true,\"reason\":\"no longer needed\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DISCARDED"))
                .andExpect(jsonPath("$.discardReason").value("no longer needed"))
                .andExpect(jsonPath("$.lastError").value("boom"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_the_ordering_break_is_not_acknowledged_WHEN_discarded_over_http_THEN_a_problem_json_400_is_returned() throws Exception {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-it-7").aggregateType("Order").seq(1).payload("{}".getBytes()).build());
        long id = idOf("order-it-7");
        setStatus(id, "boom");

        mockMvc.perform(post("/tandem/admin/v1/outbox/messages/{id}/discard", id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"acknowledgeOrderingBreak\":false}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/ordering-break-not-acknowledged"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_selector_less_bulk_replay_request_WHEN_posted_over_http_THEN_a_problem_json_400_is_returned() throws Exception {
        mockMvc.perform(post("/tandem/admin/v1/outbox/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/replay-no-selector"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_failed_rows_for_an_aggregate_WHEN_bulk_replayed_over_http_THEN_they_are_reset_and_the_count_reported() throws Exception {
        repository.insert(OutboxMessage.builder()
                .aggregateId("order-it-8").aggregateType("Order").seq(1).payload("{}".getBytes()).build());
        setStatus(idOf("order-it-8"), "boom");

        mockMvc.perform(post("/tandem/admin/v1/outbox/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"aggregateId\":\"order-it-8\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(1))
                .andExpect(jsonPath("$.replayed").value(1))
                .andExpect(jsonPath("$.dryRun").value(false))
                .andExpect(OpenApiConformance.conformsToOpenApi());
        mockMvc.perform(get("/tandem/admin/v1/outbox/messages/{id}", idOf("order-it-8")))
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    private static void setStatus(long id, String error) throws SQLException {
        try (Connection conn = container.dataSource().getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "UPDATE tandem_outbox SET status = 3, last_error = ? WHERE id = ?")) {
            ps.setString(1, error);
            ps.setLong(2, id);
            ps.executeUpdate();
        }
    }

    private static long idOf(String aggregateId) throws SQLException {
        try (Connection conn = container.dataSource().getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT id FROM tandem_outbox WHERE aggregate_id = ?")) {
            ps.setString(1, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }
}
