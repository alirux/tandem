package com.codingful.tandem.jdbc;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A tiny, dependency-free JSON codec for the {@code headers} column only — a flat object of
 * string→string (LLD-jdbc §2). The write-side must not force a JSON library on the client (§1.3), and
 * {@code headers} is the only structured value the adapter itself serializes (the {@code payload} is
 * passed through as raw JSON text the client already produced).
 *
 * <p>The reader is deliberately <b>tolerant</b> (compatibility, AGENTS): it keeps string values,
 * reads {@code null} as {@code null}, and preserves any other JSON scalar as its raw literal rather
 * than failing — so a header written by a future/other writer never breaks this reader.
 */
final class MiniJson {

    private MiniJson() {
    }

    /** Serialize a flat string→string map to a JSON object. */
    static String writeObject(Map<String, String> map) {
        StringBuilder sb = new StringBuilder(16 + map.size() * 16);
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            writeString(sb, e.getKey());
            sb.append(':');
            if (e.getValue() == null) {
                sb.append("null");
            } else {
                writeString(sb, e.getValue());
            }
        }
        sb.append('}');
        return sb.toString();
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
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
        sb.append('"');
    }

    /** Parse a flat JSON object into a string→string map. Returns an empty map for {@code null}/blank. */
    static Map<String, String> parseObject(String json) {
        Map<String, String> out = new LinkedHashMap<>();
        if (json == null) {
            return out;
        }
        int i = skipWs(json, 0);
        if (i >= json.length() || json.charAt(i) != '{') {
            return out;   // tolerate non-objects / empty
        }
        i++;
        i = skipWs(json, i);
        if (i < json.length() && json.charAt(i) == '}') {
            return out;
        }
        while (i < json.length()) {
            i = skipWs(json, i);
            if (charAt(json, i) != '"') {
                throw new IllegalArgumentException("expected a string key at index " + i + " in: " + json);
            }
            StringBuilder key = new StringBuilder();
            i = readString(json, i, key);
            i = skipWs(json, i);
            expect(json, i, ':');
            i = skipWs(json, i + 1);
            String value;
            if (charAt(json, i) == '"') {
                StringBuilder val = new StringBuilder();
                i = readString(json, i, val);
                value = val.toString();
            } else if (json.startsWith("null", i)) {
                value = null;
                i += 4;
            } else {
                int start = i;
                while (i < json.length() && json.charAt(i) != ',' && json.charAt(i) != '}') {
                    i++;
                }
                value = json.substring(start, i).trim();   // tolerant: keep the raw scalar
            }
            out.put(key.toString(), value);
            i = skipWs(json, i);
            char c = charAt(json, i);
            if (c == ',') {
                i++;
                continue;
            }
            if (c == '}') {
                break;
            }
            throw new IllegalArgumentException("expected ',' or '}' at index " + i + " in: " + json);
        }
        return out;
    }

    private static int readString(String json, int i, StringBuilder out) {
        // json.charAt(i) == '"'
        i++;
        while (i < json.length()) {
            char c = json.charAt(i++);
            if (c == '"') {
                return i;
            }
            if (c == '\\') {
                if (i >= json.length()) {
                    throw new IllegalArgumentException("dangling escape at end of: " + json);
                }
                char esc = json.charAt(i++);
                switch (esc) {
                    case '"' -> out.append('"');
                    case '\\' -> out.append('\\');
                    case '/' -> out.append('/');
                    case 'n' -> out.append('\n');
                    case 'r' -> out.append('\r');
                    case 't' -> out.append('\t');
                    case 'b' -> out.append('\b');
                    case 'f' -> out.append('\f');
                    case 'u' -> {
                        if (i + 4 > json.length()) {
                            throw new IllegalArgumentException("truncated \\u escape in: " + json);
                        }
                        out.append((char) Integer.parseInt(json.substring(i, i + 4), 16));
                        i += 4;
                    }
                    default -> throw new IllegalArgumentException("bad escape \\" + esc + " in: " + json);
                }
            } else {
                out.append(c);
            }
        }
        throw new IllegalArgumentException("unterminated string in: " + json);
    }

    /** {@code charAt} with a bounds check, so truncated input fails like any other malformed JSON. */
    private static char charAt(String json, int i) {
        if (i >= json.length()) {
            throw new IllegalArgumentException("unexpected end of input in: " + json);
        }
        return json.charAt(i);
    }

    private static int skipWs(String json, int i) {
        while (i < json.length() && Character.isWhitespace(json.charAt(i))) {
            i++;
        }
        return i;
    }

    private static void expect(String json, int i, char c) {
        if (i >= json.length() || json.charAt(i) != c) {
            throw new IllegalArgumentException("expected '" + c + "' at index " + i + " in: " + json);
        }
    }
}
