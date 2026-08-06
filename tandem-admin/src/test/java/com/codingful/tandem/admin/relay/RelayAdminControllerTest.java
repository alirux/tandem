package com.codingful.tandem.admin.relay;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.codingful.tandem.admin.OpenApiConformance;
import com.codingful.tandem.admin.TandemAdminExceptionHandler;
import com.codingful.tandem.admin.TandemAdminObjectMappers;
import com.codingful.tandem.core.RelayCoordinationMode;
import com.codingful.tandem.test.InMemoryOutbox;
import com.codingful.tandem.test.InMemoryRelayControl;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * Real Spring MVC dispatch (MockMvc, standalone) over {@link RelayAdminController} + the module-wide
 * {@link TandemAdminExceptionHandler} and this feature's own {@link RelayExceptionHandler}.
 *
 * <p>Tests that POST a JSON body are tagged {@code boot3-only} — see
 * {@code OutboxAdminControllerTest}'s class javadoc for the Boot4/Spring7 {@code MockMvc} builder
 * incompatibility this works around; {@code GET}s and body-less {@code POST}s are unaffected.
 */
class RelayAdminControllerTest {

    private static final int BUCKETS = 8;

    private InMemoryRelayControl control;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        control = new InMemoryRelayControl(new InMemoryOutbox(BUCKETS, 10, Clock.systemUTC()), BUCKETS);
        ObjectMapper objectMapper = TandemAdminObjectMappers.newDefault();
        RelayAdminService service = new RelayAdminService(control, control);
        mockMvc = MockMvcBuilders.standaloneSetup(new RelayAdminController(service))
                .setControllerAdvice(new RelayExceptionHandler(), new TandemAdminExceptionHandler())
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    @Test
    void GIVEN_a_running_relay_WHEN_status_is_requested_THEN_it_reports_RUNNING() throws Exception {
        mockMvc.perform(get("/tandem/admin/v1/relay/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("RUNNING"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_no_relay_has_heartbeated_WHEN_status_is_requested_THEN_it_reports_DOWN() throws Exception {
        control.setAlive(false);

        mockMvc.perform(get("/tandem/admin/v1/relay/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("DOWN"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_no_request_body_WHEN_the_relay_is_paused_THEN_the_whole_relay_is_paused() throws Exception {
        mockMvc.perform(post("/tandem/admin/v1/relay/pause"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("PAUSED"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    @Tag("boot3-only")   // chains .principal(...) after the bare static factory — see OutboxAdminControllerTest's class javadoc
    void GIVEN_an_authenticated_caller_WHEN_the_relay_is_paused_THEN_the_request_still_succeeds() throws Exception {
        mockMvc.perform(post("/tandem/admin/v1/relay/pause").principal(() -> "alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("PAUSED"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_paused_relay_WHEN_resumed_with_no_body_THEN_it_is_running_again() throws Exception {
        control.pauseAll();

        mockMvc.perform(post("/tandem/admin/v1/relay/resume"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("RUNNING"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    @Tag("boot3-only")
    void GIVEN_SINGLE_coordination_WHEN_a_bucket_selector_is_used_to_pause_THEN_a_problem_json_409_is_returned() throws Exception {
        mockMvc.perform(post("/tandem/admin/v1/relay/pause")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"bucket\":3}"))
                .andExpect(status().isConflict())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/relay-coordination-unsupported"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    @Tag("boot3-only")
    void GIVEN_LEASE_coordination_and_a_valid_bucket_WHEN_paused_THEN_it_is_paused() throws Exception {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);

        mockMvc.perform(post("/tandem/admin/v1/relay/pause")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"bucket\":3}"))
                .andExpect(status().isOk())
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    @Tag("boot3-only")
    void GIVEN_LEASE_coordination_and_a_bucket_outside_the_range_WHEN_paused_THEN_a_problem_json_404_is_returned() throws Exception {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);

        mockMvc.perform(post("/tandem/admin/v1/relay/pause")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"bucket\":99}"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/not-found"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_buckets_are_listed_THEN_a_problem_json_409_is_returned() throws Exception {
        mockMvc.perform(get("/tandem/admin/v1/relay/buckets"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/relay-coordination-unsupported"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_LEASE_coordination_WHEN_buckets_are_listed_THEN_every_bucket_is_returned() throws Exception {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);

        mockMvc.perform(get("/tandem/admin/v1/relay/buckets"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(BUCKETS))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_a_single_bucket_is_read_THEN_a_problem_json_409_is_returned() throws Exception {
        mockMvc.perform(get("/tandem/admin/v1/relay/buckets/{bucket}", 3))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.type").value("https://tandem.codingful.com/problems/relay-coordination-unsupported"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_LEASE_coordination_and_an_owned_bucket_WHEN_read_THEN_its_status_is_returned() throws Exception {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);
        control.own(3, "instance-1", Instant.now().plusSeconds(60));

        mockMvc.perform(get("/tandem/admin/v1/relay/buckets/{bucket}", 3))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.owner").value("instance-1"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_LEASE_coordination_and_a_bucket_outside_the_range_WHEN_read_THEN_a_problem_json_404_is_returned() throws Exception {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);

        mockMvc.perform(get("/tandem/admin/v1/relay/buckets/{bucket}", 99))
                .andExpect(status().isNotFound())
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_an_owned_bucket_WHEN_released_THEN_its_lease_is_cleared() throws Exception {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);
        control.own(3, "instance-1", Instant.now().plusSeconds(60));

        mockMvc.perform(post("/tandem/admin/v1/relay/buckets/{bucket}/release", 3))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.covered").value(false))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_a_bucket_is_released_THEN_a_problem_json_409_is_returned() throws Exception {
        mockMvc.perform(post("/tandem/admin/v1/relay/buckets/{bucket}/release", 3))
                .andExpect(status().isConflict())
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_LEASE_coordination_and_a_bucket_outside_the_range_WHEN_released_THEN_a_problem_json_404_is_returned() throws Exception {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);

        mockMvc.perform(post("/tandem/admin/v1/relay/buckets/{bucket}/release", 99))
                .andExpect(status().isNotFound())
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_workers_are_listed_THEN_a_problem_json_409_is_returned() throws Exception {
        mockMvc.perform(get("/tandem/admin/v1/relay/workers"))
                .andExpect(status().isConflict())
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }

    @Test
    void GIVEN_a_live_member_WHEN_workers_are_listed_THEN_it_is_returned() throws Exception {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);
        control.addMember("instance-1", Instant.now());

        mockMvc.perform(get("/tandem/admin/v1/relay/workers"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].workerId").value("instance-1"))
                .andExpect(OpenApiConformance.conformsToOpenApi());
    }
}
