package com.codingful.tandem.test.spring;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Shared by the {@code tandem-spring-*} modules' own reference-configuration tests (LLD-spring-config
 * §2.4): each module's shipped reference YAML must document exactly the {@code tandem.*} keys its
 * generated {@code spring-configuration-metadata.json} declares — no more, no less. Internal build
 * tooling, not a Tandem testing collaborator, so it lives in {@code tandem-test}'s test fixtures rather
 * than its published main sources.
 */
public final class ConfigurationMetadataReference {

    private static final Pattern PROPERTY_NAME = Pattern.compile("\"name\"\\s*:\\s*\"(tandem\\.[^\"]+)\"");
    private static final Pattern DOCUMENTED_KEY = Pattern.compile("tandem\\.[a-z0-9.\\-]*[a-z0-9*]");

    private ConfigurationMetadataReference() {
    }

    /**
     * The {@code tandem.*} names a module actually binds, read from its generated configuration
     * metadata. Hint names (e.g. value hints for a raw map key) are excluded — they are not property
     * keys — by cutting the text at the {@code "hints"} array.
     */
    public static List<String> boundKeys(String metadataJson) {
        int hints = metadataJson.indexOf("\"hints\"");
        return matches(PROPERTY_NAME, hints < 0 ? metadataJson : metadataJson.substring(0, hints), 1);
    }

    /**
     * The {@code tandem.*} keys a reference YAML documents, with a map key's {@code .*} wildcard
     * (e.g. {@code tandem.kafka.producer.*}) reduced to its prefix so it compares against the bound
     * property name it stands for.
     */
    public static List<String> referencedKeys(String referenceYaml) {
        return matches(DOCUMENTED_KEY, referenceYaml, 0).stream()
                .map(key -> key.endsWith(".*") ? key.substring(0, key.length() - 2) : key)
                .distinct()
                .toList();
    }

    /** Reads a classpath resource (relative to {@code anchor}'s classloader) as a UTF-8 string. */
    public static String resource(Class<?> anchor, String path) {
        try (InputStream in = anchor.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource:" + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<String> matches(Pattern pattern, String text, int group) {
        Matcher matcher = pattern.matcher(text);
        List<String> found = new ArrayList<>();
        while (matcher.find()) {
            found.add(matcher.group(group));
        }
        return found;
    }
}
