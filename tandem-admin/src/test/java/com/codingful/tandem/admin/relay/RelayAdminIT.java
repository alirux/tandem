package com.codingful.tandem.admin.relay;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.codingful.tandem.admin.OpenApiConformance;
import com.codingful.tandem.admin.TandemAdminExceptionHandler;
import com.codingful.tandem.admin.TandemAdminObjectMappers;
import com.codingful.tandem.jdbc.JdbcRelayControl;
import com.codingful.tandem.jdbc.JdbcRelayQuery;
import com.codingful.tandem.test.TandemTestContainer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * End-to-end relay-control test over a real PostgreSQL (Testcontainers): the real
 * {@link JdbcRelayQuery}/{@link JdbcRelayControl} against the real {@code tandem_meta}/
 * {@code tandem_bucket_lease}/{@code tandem_relay_member} tables and the actual REST layer.
 */
@Tag("integration")
class RelayAdminIT {

    private static TandemTestContainer container;
    private MockMvc mockMvc;

    @BeforeAll
    static void startContainer() {
        container = new TandemTestContainer().start();
    }

    @AfterAll
    static void stopContainer() {
        container.close();
    }

    @BeforeEach
    void setUp() throws SQLException {
        try (Connection conn = container.dataSource().getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tandem_bucket_lease SET owner = NULL, lease_until = NULL, paused = false");
            stmt.execute("TRUNCATE tandem_relay_member");
            stmt.execute("TRUNCATE tandem_meta");
        }
        JdbcRelayQuery query = new JdbcRelayQuery(container.dataSource());
        JdbcRelayControl control = new JdbcRelayControl(container.dataSource());
        RelayAdminService service = new RelayAdminService(query, control);
        ObjectMapper objectMapper = TandemAdminObjectMappers.newDefault();
        mockMvc = MockMvcBuilders.standaloneSetup(new RelayAdminController(service))
                .setControllerAdvice(new RelayExceptionHandler(), new TandemAdminExceptionHandler())
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    @Test
    void GIVEN_no_relay_has_ever_started_WHEN_status_is_requested_THEN_it_reports_RUNNING_with_zero_SINGLE_fields() throws Exception {
        mockMvc.perform(get("/tandem/admin/v1/relay/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("RUNNING"))
                .andExpect(jsonPath("$.uncoveredBuckets").value(0))
                .andExpect(jsonPath("$.workers").value(0))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_the_whole_relay_is_paused_over_http_THEN_it_is_recorded_in_tandem_meta() throws Exception {
        mockMvc.perform(post("/tandem/admin/v1/relay/pause"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("PAUSED"))
                .andExpect(OpenApiConformance.conformsToOpenApi());

        mockMvc.perform(get("/tandem/admin/v1/relay/status"))
                .andExpect(jsonPath("$.state").value("PAUSED"));
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_buckets_are_requested_over_http_THEN_a_problem_json_409_is_returned() throws Exception {
        mockMvc.perform(get("/tandem/admin/v1/relay/buckets"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/relay-coordination-unsupported"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_relay_recorded_LEASE_WHEN_buckets_are_requested_over_http_THEN_every_seeded_bucket_is_returned() throws Exception {
        recordCoordination("LEASE");

        mockMvc.perform(get("/tandem/admin/v1/relay/buckets"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(256))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_LEASE_coordination_and_an_owned_bucket_WHEN_read_over_http_THEN_its_status_is_returned() throws Exception {
        recordCoordination("LEASE");
        try (Connection conn = container.dataSource().getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tandem_bucket_lease SET owner = 'instance-1', lease_until = now() + interval '1 minute' WHERE bucket = 5");
        }

        mockMvc.perform(get("/tandem/admin/v1/relay/buckets/{bucket}", 5))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bucket").value(5))
                .andExpect(jsonPath("$.owner").value("instance-1"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_LEASE_coordination_and_an_owned_bucket_WHEN_released_over_http_THEN_its_lease_is_cleared() throws Exception {
        recordCoordination("LEASE");
        try (Connection conn = container.dataSource().getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tandem_bucket_lease SET owner = 'instance-1', lease_until = now() + interval '1 minute' WHERE bucket = 5");
        }

        mockMvc.perform(post("/tandem/admin/v1/relay/buckets/{bucket}/release", 5))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.owner").doesNotExist())
                .andExpect(jsonPath("$.covered").value(false))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_LEASE_coordination_and_a_live_member_WHEN_workers_are_requested_over_http_THEN_it_is_returned() throws Exception {
        recordCoordination("LEASE");
        try (Connection conn = container.dataSource().getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO tandem_relay_member (owner, lease_until) VALUES ('instance-1', now() + interval '1 minute')");
        }

        mockMvc.perform(get("/tandem/admin/v1/relay/workers"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].workerId").value("instance-1"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_LEASE_coordination_WHEN_a_bucket_is_paused_over_http_THEN_it_is_recorded_in_tandem_bucket_lease() throws Exception {
        recordCoordination("LEASE");

        mockMvc.perform(post("/tandem/admin/v1/relay/pause")
                        .content("{\"bucket\":5}")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(OpenApiConformance.conformsToOpenApi());

        mockMvc.perform(get("/tandem/admin/v1/relay/buckets"))
                .andExpect(jsonPath("$[5].paused").value(true));
    }

    private static void recordCoordination(String mode) throws SQLException {
        try (Connection conn = container.dataSource().getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO tandem_meta (key, value) VALUES ('coordination', '" + mode + "')");
        }
    }
}
