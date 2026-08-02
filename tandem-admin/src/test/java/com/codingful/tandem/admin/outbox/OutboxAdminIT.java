package com.codingful.tandem.admin.outbox;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.codingful.tandem.admin.OpenApiConformance;
import com.codingful.tandem.admin.TandemAdminExceptionHandler;
import com.codingful.tandem.admin.TandemAdminObjectMappers;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.jdbc.JdbcOutboxQuery;
import com.codingful.tandem.jdbc.JdbcOutboxRepository;
import com.codingful.tandem.jdbc.JdbcOutboxStore;
import com.codingful.tandem.test.TandemTestContainer;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * End-to-end slice-1 test over a real PostgreSQL (Testcontainers): write via
 * {@link JdbcOutboxRepository}, read back through the real {@link JdbcOutboxQuery}/
 * {@link JdbcOutboxStore} and the actual REST layer — no in-memory shortcuts. Confirms the whole
 * chain the unit tests each exercise in isolation actually fits together.
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
        ObjectMapper objectMapper = TandemAdminObjectMappers.newDefault();
        OutboxAdminService service = new OutboxAdminService(query, store, objectMapper, Clock.systemUTC());
        mockMvc = MockMvcBuilders.standaloneSetup(new OutboxAdminController(service))
                .setControllerAdvice(new OutboxExceptionHandler(), new TandemAdminExceptionHandler())
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
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
