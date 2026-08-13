package com.codingful.tandem.admin.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import com.codingful.tandem.admin.TandemAdminExceptionHandler;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.test.InMemoryOutbox;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * The rendering half of the stock-Boot-4 gate. Starting the context again was only the first stage of
 * the defect: with Jackson 2 forced back onto a Boot 4 application the context did start, but the only
 * JSON converter registered was Jackson 3's, which serialized the Jackson 2 tree embedded in the DTO
 * by bean introspection — {@code {"array":false,"bigDecimal":false,…}} in place of the stored payload,
 * silently violating the contract's {@code oneOf: [object, string]}.
 *
 * <p>So these assertions read the raw response body produced by Spring's <b>own</b> Jackson 3
 * converter (standalone MockMvc registers exactly what the application would), rather than going
 * through a JSON-path matcher that could hide the difference. This source set's own minimal, isolated
 * classpath (see the build file) is what makes standalone MockMvc safe to use here — the module's
 * shared test classpath carries a Jackson-2-internal library that trips over a Jackson-3-only setup
 * for reasons unrelated to this module's own code.
 */
class OutboxRenderingOnJacksonThreeTest {

    private static final Instant CREATED_AT = Instant.parse("2026-08-13T10:15:30Z");

    private InMemoryOutbox outbox;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        outbox = new InMemoryOutbox(256, 10, Clock.fixed(CREATED_AT, ZoneOffset.UTC));
        OutboxAdminService service = new OutboxAdminService(outbox, outbox, outbox, outbox, Clock.systemUTC());
        mockMvc = MockMvcBuilders.standaloneSetup(new OutboxAdminController(service))
                .setControllerAdvice(new OutboxExceptionHandler(), new TandemAdminExceptionHandler())
                .build();
    }

    @Test
    void GIVEN_a_json_payload_WHEN_the_message_detail_is_read_THEN_the_payload_is_embedded_as_json() throws Exception {
        long id = insert("{\"amount\":42,\"currency\":\"EUR\"}");

        String body = detailBody(id);

        assertThat(body).contains("\"payload\":{\"amount\":42,\"currency\":\"EUR\"}");
    }

    @Test
    void GIVEN_a_payload_that_is_not_json_WHEN_the_message_detail_is_read_THEN_it_is_rendered_as_a_string()
            throws Exception {
        long id = insert("not-json-at-all");

        assertThat(detailBody(id)).contains("\"payload\":\"not-json-at-all\"");
    }

    @Test
    void GIVEN_a_stored_message_WHEN_it_is_read_THEN_its_timestamps_are_rendered_as_date_times_not_epochs()
            throws Exception {
        long id = insert("{}");

        assertThat(detailBody(id)).contains("\"createdAt\":\"" + CREATED_AT + "\"");
    }

    @Test
    void GIVEN_a_stored_message_WHEN_the_list_view_is_read_THEN_the_payload_is_omitted_entirely() throws Exception {
        insert("{\"secret\":1}");

        String body = mockMvc.perform(get("/tandem/admin/v1/outbox/messages"))
                .andReturn().getResponse().getContentAsString();

        assertThat(body).doesNotContain("payload").doesNotContain("secret");
    }

    private long insert(String payload) {
        outbox.insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload(payload.getBytes()).build());
        return outbox.all().get(0).id();
    }

    private String detailBody(long id) throws Exception {
        return mockMvc.perform(get("/tandem/admin/v1/outbox/messages/{id}", id))
                .andReturn().getResponse().getContentAsString();
    }
}
