package com.codingful.tandem.admin.outbox.dto;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The stored payload reaches the response body verbatim, so the "is this JSON?" verdict is what
 * keeps the body well-formed: a wrongly-accepted fragment corrupts the whole response, while a
 * wrongly-rejected one merely renders as a JSON string. These tests pin both directions, and
 * especially the rejections — that is the side with no second chance.
 */
class PayloadJsonTest {

    @Test
    void GIVEN_a_json_payload_WHEN_rendered_THEN_it_is_embedded_verbatim() {
        String stored = "{\"amount\":42,\"currency\":\"EUR\"}";

        assertThat(render(stored)).isEqualTo(stored);
    }

    @Test
    void GIVEN_a_payload_that_is_not_json_WHEN_rendered_THEN_it_becomes_a_quoted_json_string() {
        assertThat(render("not-json-at-all")).isEqualTo("\"not-json-at-all\"");
    }

    @Test
    void GIVEN_an_empty_payload_WHEN_rendered_THEN_it_becomes_an_empty_json_string() {
        // The previous parser-based rendering emitted nothing at all here, producing a malformed body.
        assertThat(render("")).isEqualTo("\"\"");
    }

    @Test
    void GIVEN_binary_bytes_that_begin_like_an_object_WHEN_rendered_THEN_they_are_quoted_not_embedded() {
        // Why the check is a real scan and not a "starts with {" heuristic: a BYTEA payload written by
        // a binary serializer (the baseline schema documents that option) can start with 0x7B.
        byte[] avroish = {'{', 0x00, 0x12, (byte) 0xFF, 0x7F};

        String rendered = PayloadJson.render(avroish);

        assertThat(rendered).startsWith("\"").endsWith("\"");
        assertThat(PayloadJson.isJsonValue(rendered)).isTrue();
    }

    @Test
    void GIVEN_control_characters_in_a_rejected_payload_WHEN_rendered_THEN_they_are_escaped() {
        String rendered = render("a\"b\\c\nd\te\r f\bg\fh\u0001i");

        assertThat(rendered).isEqualTo("\"a\\\"b\\\\c\\nd\\te\\r f\\bg\\fh\\u0001i\"");
        assertThat(PayloadJson.isJsonValue(rendered)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{}",
            "[]",
            "  {\"a\":[1,2,{\"b\":null}]}  ",
            "[{\"a\":true},{\"a\":false}]",
            "\"a string payload\"",
            "\"escaped \\\" quote and \\u00e9\"",
            // Whitespace between tokens can be tab/newline/CR, not just space.
            "{\n\t\"a\":\r[1]}",
            // Every named escape a JSON string can carry, including the forward-slash one.
            "\"\\\\ \\/ \\b \\f \\n \\r \\t\"",
            "42",
            "-0.5e-3",
            "1E10",
            "0",
            "true",
            "false",
            "null",
    })
    void GIVEN_a_complete_json_value_WHEN_examined_THEN_it_is_accepted(String json) {
        assertThat(PayloadJson.isJsonValue(json)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "   ",
            "{",
            "}",
            "{\"a\":1",
            "{\"a\":1,}",
            "{a:1}",
            "{\"a\" 1}",
            "[1,]",
            "[1 2]",
            "{}{}",
            "{} trailing",
            "\"unterminated",
            "\"raw \u0007 control\"",
            "\"bad \\x escape\"",
            "\"short \\u12 escape\"",
            // An object whose key itself is an unterminated string, not just a missing/wrong key.
            "{\"a",
            // An object whose value is missing entirely after the colon.
            "{\"a\":}",
            // A backslash as the very last character — no room left to read an escape at all.
            "\"\\",
            // A truncated unicode escape: the string closes after only two hex digits.
            // Built via concatenation, not a single literal: javac's own unicode-escape
            // preprocessing runs on raw source before string literals even exist, and a
            // resolved backslash sitting right in front of a literal "u12" in one literal
            // is later rejected by the lexer as an illegal escape character.
            "\"\\" + "u12\"",
            "01",
            "+1",
            "1.",
            "1e",
            "1e+",
            ".5",
            "-",
            "tru",
            "nul",
            "NaN",
    })
    void GIVEN_something_that_is_not_one_complete_json_value_WHEN_examined_THEN_it_is_rejected(String text) {
        assertThat(PayloadJson.isJsonValue(text)).isFalse();
    }

    @Test
    void GIVEN_nesting_beyond_the_scan_limit_WHEN_examined_THEN_it_is_rejected_rather_than_overflowing_the_stack() {
        String deep = "[".repeat(5_000) + "]".repeat(5_000);

        assertThat(PayloadJson.isJsonValue(deep)).isFalse();
    }

    @Test
    void GIVEN_nesting_within_the_scan_limit_WHEN_examined_THEN_it_is_accepted() {
        String nested = "[".repeat(100) + "]".repeat(100);

        assertThat(PayloadJson.isJsonValue(nested)).isTrue();
    }

    private static String render(String stored) {
        return PayloadJson.render(stored.getBytes(StandardCharsets.UTF_8));
    }
}
