package com.codingful.tandem.admin.outbox.dto;

import java.nio.charset.StandardCharsets;

/**
 * Renders a stored payload as a <b>JSON text fragment</b> — the value {@link OutboxEntryResponse}
 * hands to Jackson through {@code @JsonRawValue}, which copies it into the response body verbatim.
 *
 * <p>Exists because this module must not name a JSON <i>databind</i> type anywhere: Spring Boot 3
 * carries Jackson 2 ({@code com.fasterxml.jackson.databind}) and Spring Boot 4 carries Jackson 3
 * ({@code tools.jackson}), and only the annotations are shared between them. Deciding whether the
 * stored bytes are JSON used to be an {@code ObjectMapper.readTree} call, which is exactly what
 * dragged a version-specific type into a {@code @Bean} signature and stopped the module from
 * starting on Boot 4 at all.
 *
 * <p>The verdict cannot be assumed: {@code payload} is {@code JSONB} by default — validated by the
 * engine, so always JSON — but the baseline schema documents switching it to {@code BYTEA} for a
 * binary serializer (Avro/Protobuf), where the bytes are neither JSON nor necessarily UTF-8. A
 * cheaper heuristic ("starts with <code>{</code>") would emit a malformed response body for a binary
 * payload whose first byte happens to be {@code 0x7B}, so the check is a real, strict scan and its
 * failure direction is the safe one: anything not provably a JSON value is rendered as a JSON
 * string, matching the contract's {@code oneOf: [object, string]}.
 */
final class PayloadJson {

    /**
     * Nesting deeper than this is treated as "not JSON" (rendered as a string) rather than parsed.
     * The scan is recursive, and the payload is data from the database — an unbounded depth would
     * turn a pathological row into a {@link StackOverflowError} on an operator's read.
     */
    private static final int MAX_DEPTH = 500;

    private PayloadJson() {
    }

    /**
     * The stored payload as a JSON text fragment: the bytes verbatim when they are a complete JSON
     * value, otherwise their UTF-8 decoding wrapped and escaped as a JSON string.
     */
    static String render(byte[] payload) {
        String text = new String(payload, StandardCharsets.UTF_8);
        return isJsonValue(text) ? text : quote(text);
    }

    /** Whether {@code text} is exactly one complete JSON value, with nothing but whitespace around it. */
    static boolean isJsonValue(String text) {
        Scan scan = new Scan(text);
        scan.skipWhitespace();
        if (!scan.value(0)) {
            return false;
        }
        scan.skipWhitespace();
        return scan.atEnd();
    }

    /**
     * {@code text} as a JSON string literal, quotes included.
     *
     * <p>Byte-for-byte identical to {@code MiniJson.writeString} in {@code tandem-jdbc} (different
     * module, so it can't share this method) — keep them in sync if you touch either. Not worth a
     * shared published type for 18 lines of a closed, unchanging algorithm (RFC 8259 string escaping).
     */
    static String quote(String text) {
        StringBuilder sb = new StringBuilder(text.length() + 16);
        sb.append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    /**
     * A strict recursive-descent scan over RFC 8259's grammar. Strict on purpose: every case it
     * rejects is rendered as a JSON string instead, which is always valid on the wire, whereas a
     * wrongly-accepted fragment would corrupt the whole response.
     */
    private static final class Scan {

        private final String s;
        private int i;

        Scan(String s) {
            this.s = s;
        }

        boolean atEnd() {
            return i == s.length();
        }

        void skipWhitespace() {
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    i++;
                } else {
                    return;
                }
            }
        }

        boolean value(int depth) {
            if (depth > MAX_DEPTH || i >= s.length()) {
                return false;
            }
            return switch (s.charAt(i)) {
                case '{' -> object(depth);
                case '[' -> array(depth);
                case '"' -> string();
                case 't' -> literal("true");
                case 'f' -> literal("false");
                case 'n' -> literal("null");
                default -> number();
            };
        }

        private boolean object(int depth) {
            i++;   // '{'
            skipWhitespace();
            if (consumeIfPresent('}')) {
                return true;
            }
            while (true) {
                skipWhitespace();
                if (i >= s.length() || s.charAt(i) != '"' || !string()) {
                    return false;
                }
                skipWhitespace();
                if (!consumeIfPresent(':')) {
                    return false;
                }
                skipWhitespace();
                if (!value(depth + 1)) {
                    return false;
                }
                skipWhitespace();
                if (consumeIfPresent(',')) {
                    continue;
                }
                return consumeIfPresent('}');
            }
        }

        private boolean array(int depth) {
            i++;   // '['
            skipWhitespace();
            if (consumeIfPresent(']')) {
                return true;
            }
            while (true) {
                skipWhitespace();
                if (!value(depth + 1)) {
                    return false;
                }
                skipWhitespace();
                if (consumeIfPresent(',')) {
                    continue;
                }
                return consumeIfPresent(']');
            }
        }

        private boolean string() {
            i++;   // opening quote
            while (i < s.length()) {
                char c = s.charAt(i++);
                if (c == '"') {
                    return true;
                }
                if (c < 0x20) {
                    return false;   // a raw control character must be escaped
                }
                if (c == '\\' && !escape()) {
                    return false;
                }
            }
            return false;   // unterminated
        }

        private boolean escape() {
            if (i >= s.length()) {
                return false;
            }
            char c = s.charAt(i++);
            if (c == 'u') {
                if (i + 4 > s.length()) {
                    return false;
                }
                for (int end = i + 4; i < end; i++) {
                    if (Character.digit(s.charAt(i), 16) < 0) {
                        return false;
                    }
                }
                return true;
            }
            return c == '"' || c == '\\' || c == '/' || c == 'b' || c == 'f' || c == 'n' || c == 'r' || c == 't';
        }

        // Every path that returns without an earlier "return false" has advanced i past its entry
        // point — either consumeIfPresent('0') or digits() must have consumed at least one character,
        // or the method already returned at the first guard — so there is no case left to check here.
        private boolean number() {
            consumeIfPresent('-');
            if (!consumeIfPresent('0') && digits() == 0) {
                return false;
            }
            if (consumeIfPresent('.') && digits() == 0) {
                return false;
            }
            if (consumeIfPresent('e') || consumeIfPresent('E')) {
                if (!consumeIfPresent('+')) {
                    consumeIfPresent('-');
                }
                if (digits() == 0) {
                    return false;
                }
            }
            return true;
        }

        private int digits() {
            int start = i;
            while (i < s.length() && s.charAt(i) >= '0' && s.charAt(i) <= '9') {
                i++;
            }
            return i - start;
        }

        private boolean literal(String expected) {
            if (!s.startsWith(expected, i)) {
                return false;
            }
            i += expected.length();
            return true;
        }

        private boolean consumeIfPresent(char c) {
            if (i < s.length() && s.charAt(i) == c) {
                i++;
                return true;
            }
            return false;
        }
    }
}
