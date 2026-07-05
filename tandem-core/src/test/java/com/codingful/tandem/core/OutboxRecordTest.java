package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class OutboxRecordTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private static final OutboxMessage MESSAGE = OutboxMessage.builder()
            .aggregateId("order-1")
            .aggregateType("Order")
            .type("com.acme.order.placed")
            .seq(3)
            .payload(new byte[]{1, 2})
            .contentType("application/json")
            .header("x-source", "checkout")
            .build();

    @Test
    void GIVEN_required_fields_only_WHEN_built_THEN_defaults_are_applied() {
        OutboxRecord r = OutboxRecord.builder().id(42).message(MESSAGE).createdAt(NOW).build();

        assertThat(r.id()).isEqualTo(42);
        assertThat(r.message()).isSameAs(MESSAGE);
        assertThat(r.status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(r.attempts()).isZero();
        assertThat(r.lockedBy()).isNull();
        assertThat(r.lockedUntil()).isNull();
        assertThat(r.lastError()).isNull();
        assertThat(r.nextAttemptAt()).isNull();
        assertThat(r.lamport()).isNull();
        assertThat(r.createdAt()).isEqualTo(NOW);
    }

    @Test
    void GIVEN_all_optional_fields_set_WHEN_built_THEN_every_accessor_returns_the_configured_value() {
        Instant lockedUntil = NOW.plusSeconds(60);
        Instant nextAttempt = NOW.plusSeconds(30);

        OutboxRecord r = OutboxRecord.builder()
                .id(7)
                .message(MESSAGE)
                .status(OutboxStatus.IN_FLIGHT)
                .attempts(2)
                .lockedBy("worker-1")
                .lockedUntil(lockedUntil)
                .lastError("transient")
                .nextAttemptAt(nextAttempt)
                .createdAt(NOW)
                .lamport(99L)
                .build();

        assertThat(r.status()).isEqualTo(OutboxStatus.IN_FLIGHT);
        assertThat(r.attempts()).isEqualTo(2);
        assertThat(r.lockedBy()).isEqualTo("worker-1");
        assertThat(r.lockedUntil()).isEqualTo(lockedUntil);
        assertThat(r.lastError()).isEqualTo("transient");
        assertThat(r.nextAttemptAt()).isEqualTo(nextAttempt);
        assertThat(r.lamport()).isEqualTo(99L);
    }

    @Test
    void GIVEN_a_record_WHEN_convenience_delegates_called_THEN_they_mirror_the_wrapped_message() {
        OutboxRecord r = OutboxRecord.builder().id(1).message(MESSAGE).createdAt(NOW).build();

        assertThat(r.aggregateId()).isEqualTo(MESSAGE.aggregateId());
        assertThat(r.aggregateType()).isEqualTo("Order");
        assertThat(r.type()).isEqualTo("com.acme.order.placed");
        assertThat(r.seq()).isEqualTo(3);
        assertThat(r.payload()).isEqualTo(MESSAGE.payload());
        // contentType is a typed field on OutboxMessage — it is folded into headers["content-type"]
        // only by the outbox store at insert time, not by the message builder itself.
        assertThat(r.contentType()).isEqualTo("application/json");
        assertThat(r.headers()).containsEntry("x-source", "checkout")
                               .doesNotContainKey("content-type");
    }

    @Test
    void GIVEN_a_record_WHEN_toBuilder_and_rebuilt_THEN_all_fields_are_preserved() {
        Instant lockedUntil = NOW.plusSeconds(60);
        OutboxRecord original = OutboxRecord.builder()
                .id(5).message(MESSAGE).status(OutboxStatus.IN_FLIGHT)
                .attempts(1).lockedBy("worker-2").lockedUntil(lockedUntil)
                .lastError("err").nextAttemptAt(NOW.plusSeconds(10))
                .createdAt(NOW).lamport(42L).build();

        OutboxRecord copy = original.toBuilder().build();

        assertThat(copy.id()).isEqualTo(original.id());
        assertThat(copy.message()).isSameAs(original.message());
        assertThat(copy.status()).isEqualTo(original.status());
        assertThat(copy.attempts()).isEqualTo(original.attempts());
        assertThat(copy.lockedBy()).isEqualTo(original.lockedBy());
        assertThat(copy.lockedUntil()).isEqualTo(original.lockedUntil());
        assertThat(copy.lastError()).isEqualTo(original.lastError());
        assertThat(copy.nextAttemptAt()).isEqualTo(original.nextAttemptAt());
        assertThat(copy.createdAt()).isEqualTo(original.createdAt());
        assertThat(copy.lamport()).isEqualTo(original.lamport());
    }

    @Test
    void GIVEN_a_record_WHEN_toBuilder_overrides_one_field_THEN_only_that_field_changes() {
        OutboxRecord pending = OutboxRecord.builder().id(3).message(MESSAGE).createdAt(NOW).build();

        OutboxRecord failed = pending.toBuilder().status(OutboxStatus.FAILED).build();

        assertThat(failed.status()).isEqualTo(OutboxStatus.FAILED);
        assertThat(failed.id()).isEqualTo(pending.id());
        assertThat(failed.message()).isSameAs(pending.message());
        assertThat(failed.createdAt()).isEqualTo(pending.createdAt());
    }

    @Test
    void GIVEN_no_message_WHEN_built_THEN_NullPointerException_names_the_field() {
        assertThatNullPointerException()
                .isThrownBy(() -> OutboxRecord.builder().id(1).createdAt(NOW).build())
                .withMessageContaining("message");
    }

    @Test
    void GIVEN_no_createdAt_WHEN_built_THEN_NullPointerException_names_the_field() {
        assertThatNullPointerException()
                .isThrownBy(() -> OutboxRecord.builder().id(1).message(MESSAGE).build())
                .withMessageContaining("createdAt");
    }

    @Test
    void GIVEN_a_record_with_sensitive_payload_header_lastError_and_lockedBy_WHEN_toString_THEN_only_structural_fields_appear() {
        String piiInPayload = "email:jane@example.com";
        String secretHeaderValue = "Bearer super-secret-token";
        String sensitiveLastError = "auth failed for connection string postgres://user:hunter2@host/db";
        OutboxMessage sensitiveMessage = OutboxMessage.builder()
                .aggregateId("order-1")
                .aggregateType("Order")
                .seq(3)
                .payload(piiInPayload.getBytes(StandardCharsets.UTF_8))
                .header("authorization", secretHeaderValue)
                .build();
        OutboxRecord r = OutboxRecord.builder()
                .id(7)
                .message(sensitiveMessage)
                .status(OutboxStatus.FAILED)
                .attempts(3)
                .lastError(sensitiveLastError)
                .lockedBy("worker-1")
                .createdAt(NOW)
                .build();

        String rendered = r.toString();

        assertThat(rendered)
                .contains("id=7")
                .contains("aggregateType=Order")
                .contains("aggregateId=order-1")
                .contains("seq=3")
                .contains("status=FAILED")
                .contains("attempts=3")
                .doesNotContain(piiInPayload)
                .doesNotContain(secretHeaderValue)
                .doesNotContain("hunter2")
                .doesNotContain(sensitiveLastError)
                .doesNotContain("worker-1");
    }
}
