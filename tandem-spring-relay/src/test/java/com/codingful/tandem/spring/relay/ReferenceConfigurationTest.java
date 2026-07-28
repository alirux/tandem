package com.codingful.tandem.spring.relay;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Keeps the shipped reference configuration honest (LLD-spring-config §2.4): it is a <em>rendering</em>
 * of the property contract, so it may never list a key the module does not bind, nor omit one it does.
 * Both directions are checked against the generated configuration metadata — the same artifact the IDE
 * reads — so adding, renaming or removing a property without touching the reference file fails here
 * rather than silently shipping a reference that lies.
 */
class ReferenceConfigurationTest {

    private static final Pattern PROPERTY_NAME = Pattern.compile("\"name\"\\s*:\\s*\"(tandem\\.[^\"]+)\"");
    private static final Pattern DOCUMENTED_KEY = Pattern.compile("tandem\\.[a-z0-9.\\-]*[a-z0-9*]");

    private final String metadata = resource("/META-INF/spring-configuration-metadata.json");
    private final String reference = resource("/tandem-relay-reference.yml");

    @Test
    void GIVEN_the_bound_property_contract_WHEN_compared_with_the_reference_THEN_every_key_is_documented() {
        assertThat(boundKeys()).isNotEmpty().allSatisfy(key -> assertThat(reference).contains(key));
    }

    @Test
    void GIVEN_the_reference_WHEN_compared_with_the_bound_property_contract_THEN_it_invents_no_key() {
        List<String> bound = boundKeys();

        assertThat(referencedKeys()).isNotEmpty().allSatisfy(key ->
                assertThat(bound).as("key %s is documented but bound by nothing", key)
                        .anySatisfy(boundKey -> assertThat(boundKey).startsWith(key)));
    }

    /** The {@code tandem.*} names the module actually binds, read from the generated metadata. */
    private List<String> boundKeys() {
        // Everything before "hints" is groups + properties; the hint names are not property keys.
        int hints = metadata.indexOf("\"hints\"");
        return matches(PROPERTY_NAME, hints < 0 ? metadata : metadata.substring(0, hints), 1);
    }

    /** The {@code tandem.*} keys the reference file names, with the map wildcard reduced to its prefix. */
    private List<String> referencedKeys() {
        return matches(DOCUMENTED_KEY, reference, 0).stream()
                .map(key -> key.endsWith(".*") ? key.substring(0, key.length() - 2) : key)
                .distinct()
                .toList();
    }

    private static List<String> matches(Pattern pattern, String text, int group) {
        Matcher matcher = pattern.matcher(text);
        List<String> found = new ArrayList<>();
        while (matcher.find()) {
            found.add(matcher.group(group));
        }
        return found;
    }

    private static String resource(String path) {
        try (InputStream in = ReferenceConfigurationTest.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource:" + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
