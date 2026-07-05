package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MiniJsonTest {

    // -------------------------------------------------------------------------
    // writeObject
    // -------------------------------------------------------------------------

    @Test
    void GIVEN_an_empty_map_WHEN_written_THEN_the_output_is_an_empty_json_object() {
        assertThat(MiniJson.writeObject(Map.of())).isEqualTo("{}");
    }

    @Test
    void GIVEN_a_single_string_entry_WHEN_written_THEN_key_and_value_are_quoted() {
        assertThat(MiniJson.writeObject(Map.of("k", "v"))).isEqualTo("{\"k\":\"v\"}");
    }

    @Test
    void GIVEN_a_null_value_WHEN_written_THEN_it_is_serialized_as_json_null() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("k", null);
        assertThat(MiniJson.writeObject(map)).isEqualTo("{\"k\":null}");
    }

    @Test
    void GIVEN_multiple_entries_WHEN_written_THEN_entries_are_comma_separated_in_insertion_order() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("a", "1");
        map.put("b", "2");
        assertThat(MiniJson.writeObject(map)).isEqualTo("{\"a\":\"1\",\"b\":\"2\"}");
    }

    @Test
    void GIVEN_a_value_with_special_characters_WHEN_written_THEN_every_char_is_properly_escaped() {
        Map<String, String> map = Map.of("k", "\"quote\" \\back\n\r\t\b\f");
        String json = MiniJson.writeObject(map);
        assertThat(json).isEqualTo("{\"k\":\"\\\"quote\\\" \\\\back\\n\\r\\t\\b\\f\"}");
    }

    @Test
    void GIVEN_a_value_with_a_low_control_character_WHEN_written_THEN_it_is_unicode_escaped() {
        Map<String, String> map = Map.of("k", "");
        assertThat(MiniJson.writeObject(map)).isEqualTo("{\"k\":\"\\u0001\"}");
    }

    // -------------------------------------------------------------------------
    // parseObject — happy paths
    // -------------------------------------------------------------------------

    @Test
    void GIVEN_a_null_input_WHEN_parsed_THEN_an_empty_map_is_returned() {
        assertThat(MiniJson.parseObject(null)).isEmpty();
    }

    @Test
    void GIVEN_a_non_object_input_WHEN_parsed_THEN_an_empty_map_is_returned() {
        assertThat(MiniJson.parseObject("\"not an object\"")).isEmpty();
        assertThat(MiniJson.parseObject("")).isEmpty();
    }

    @Test
    void GIVEN_an_empty_json_object_WHEN_parsed_THEN_an_empty_map_is_returned() {
        assertThat(MiniJson.parseObject("{}")).isEmpty();
    }

    @Test
    void GIVEN_a_single_string_entry_WHEN_parsed_THEN_the_map_contains_it() {
        assertThat(MiniJson.parseObject("{\"content-type\":\"application/json\"}"))
                .containsExactly(Map.entry("content-type", "application/json"));
    }

    @Test
    void GIVEN_multiple_entries_WHEN_parsed_THEN_all_are_in_the_map() {
        Map<String, String> result = MiniJson.parseObject("{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}");
        assertThat(result).containsExactly(
                Map.entry("a", "1"),
                Map.entry("b", "2"),
                Map.entry("c", "3"));
    }

    @Test
    void GIVEN_a_json_null_value_WHEN_parsed_THEN_the_map_entry_has_a_null_value() {
        assertThat(MiniJson.parseObject("{\"k\":null}")).containsEntry("k", null);
    }

    @Test
    void GIVEN_a_non_string_scalar_value_WHEN_parsed_THEN_it_is_kept_as_raw_literal() {
        assertThat(MiniJson.parseObject("{\"flag\":true,\"count\":42}"))
                .containsEntry("flag", "true")
                .containsEntry("count", "42");
    }

    @Test
    void GIVEN_whitespace_around_tokens_WHEN_parsed_THEN_it_is_ignored() {
        assertThat(MiniJson.parseObject("  {  \"k\"  :  \"v\"  }  "))
                .containsExactly(Map.entry("k", "v"));
    }

    // -------------------------------------------------------------------------
    // parseObject — string escape sequences
    // -------------------------------------------------------------------------

    @Test
    void GIVEN_all_standard_escape_sequences_WHEN_parsed_THEN_they_are_decoded_correctly() {
        String json = "{\"k\":\"\\\"\\\\\\/ \\n\\r\\t\\b\\f\"}";
        assertThat(MiniJson.parseObject(json))
                .containsExactly(Map.entry("k", "\"\\/ \n\r\t\b\f"));
    }

    @Test
    void GIVEN_a_unicode_escape_in_a_string_WHEN_parsed_THEN_the_character_is_decoded() {
        assertThat(MiniJson.parseObject("{\"k\":\"\\u0041\"}"))
                .containsExactly(Map.entry("k", "A"));
    }

    // -------------------------------------------------------------------------
    // parseObject — round-trip
    // -------------------------------------------------------------------------

    @Test
    void GIVEN_a_map_with_special_characters_WHEN_written_and_parsed_THEN_the_map_is_identical() {
        Map<String, String> original = new LinkedHashMap<>();
        original.put("content-type", "application/json");
        original.put("with-quotes", "say \"hello\"");
        original.put("with-newline", "line1\nline2");
        original.put("nullable", null);

        Map<String, String> roundTripped = MiniJson.parseObject(MiniJson.writeObject(original));

        assertThat(roundTripped).isEqualTo(original);
    }

    // -------------------------------------------------------------------------
    // parseObject — error cases
    // -------------------------------------------------------------------------

    @Test
    void GIVEN_a_non_string_key_WHEN_parsed_THEN_an_exception_is_thrown() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MiniJson.parseObject("{123:\"v\"}"))
                .withMessageContaining("expected a string key");
    }

    @Test
    void GIVEN_a_missing_colon_after_key_WHEN_parsed_THEN_an_exception_is_thrown() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MiniJson.parseObject("{\"k\" \"v\"}"))
                .withMessageContaining("expected ':'");
    }

    @Test
    void GIVEN_an_unexpected_character_after_value_WHEN_parsed_THEN_an_exception_is_thrown() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MiniJson.parseObject("{\"k\":\"v\" X}"))
                .withMessageContaining("expected ',' or '}'");
    }

    @Test
    void GIVEN_a_bad_escape_sequence_in_a_string_WHEN_parsed_THEN_an_exception_is_thrown() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MiniJson.parseObject("{\"k\":\"\\z\"}"))
                .withMessageContaining("bad escape");
    }

    @Test
    void GIVEN_an_unterminated_string_WHEN_parsed_THEN_an_exception_is_thrown() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MiniJson.parseObject("{\"k\":\"unterminated"))
                .withMessageContaining("unterminated string");
    }

    @Test
    void GIVEN_a_truncated_unicode_escape_at_end_of_input_WHEN_parsed_THEN_a_clear_exception_is_thrown() {
        // Must not leak a raw StringIndexOutOfBoundsException from the substring(i, i+4).
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MiniJson.parseObject("{\"k\":\"\\u12"))
                .withMessageContaining("truncated \\u escape");
    }

    @Test
    void GIVEN_a_dangling_backslash_at_end_of_input_WHEN_parsed_THEN_a_clear_exception_is_thrown() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MiniJson.parseObject("{\"k\":\"\\"))
                .withMessageContaining("dangling escape");
    }

    @Test
    void GIVEN_input_truncated_after_a_complete_value_WHEN_parsed_THEN_a_clear_exception_is_thrown() {
        // No raw StringIndexOutOfBoundsException at any truncation point — the parser's own message.
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MiniJson.parseObject("{\"k\":\"v\""))
                .withMessageContaining("unexpected end of input");
    }

    @Test
    void GIVEN_input_truncated_before_the_value_WHEN_parsed_THEN_a_clear_exception_is_thrown() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MiniJson.parseObject("{\"k\":"))
                .withMessageContaining("unexpected end of input");
    }

    @Test
    void GIVEN_input_truncated_before_any_key_WHEN_parsed_THEN_it_is_tolerated_as_an_empty_object() {
        // Nothing after the brace: no entry was started, so the tolerant reader treats it like `{}`
        // (and, regardless, never leaks a raw StringIndexOutOfBoundsException).
        assertThat(MiniJson.parseObject("{  ")).isEmpty();
    }
}
