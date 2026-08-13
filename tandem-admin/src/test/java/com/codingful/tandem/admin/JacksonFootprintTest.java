package com.codingful.tandem.admin;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.admin.outbox.dto.OutboxEntryResponse;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Guards the rule that lets one artifact serve both Spring generations: <b>this module may name
 * Jackson's annotations, and neither Jackson's databind</b>. Spring Boot 3 carries Jackson 2
 * ({@code com.fasterxml.jackson.databind}) and Spring Boot 4, from 4.0.0 on, carries Jackson 3
 * ({@code tools.jackson}) and no Jackson 2 databind at all — so a databind type in any bean
 * signature or DTO makes the module unusable on one line or the other.
 *
 * <p>Reading the compiled classes rather than the sources is the point: the reference that broke
 * Boot 4 was in a {@code @Bean} method's signature, where it is invisible to a passing read but
 * present in the constant pool, and Spring resolves it while introspecting the whole configuration
 * class. This runs on every {@code test} invocation, so a reintroduction fails immediately — while
 * the {@code jacksonThreeTest} source set proves the other half, that the rendering is right on a
 * stock Boot 4 classpath.
 */
class JacksonFootprintTest {

    private static final List<String> FORBIDDEN = List.of(
            "com/fasterxml/jackson/databind", "com.fasterxml.jackson.databind",
            "tools/jackson", "tools.jackson");

    @Test
    void GIVEN_the_compiled_module_WHEN_its_classes_are_read_THEN_no_json_databind_type_is_referenced() {
        List<String> offenders;
        try (Stream<Path> classes = Files.walk(mainClassesDirectory())) {
            offenders = classes
                    .filter(path -> path.toString().endsWith(".class"))
                    .filter(JacksonFootprintTest::referencesForbiddenType)
                    .map(Path::toString)
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        assertThat(offenders).isEmpty();
    }

    /** Proves the scan can actually see a reference — otherwise an empty result means nothing. */
    @Test
    void GIVEN_a_class_that_does_reference_databind_WHEN_scanned_THEN_the_scan_detects_it() {
        assertThat(contains(TestFixtureNamingDatabind.class.getName().replace('.', '/') + ".class"))
                .as("the compiled test class itself must be findable")
                .isTrue();
    }

    private static boolean referencesForbiddenType(Path classFile) {
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(classFile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        String content = new String(bytes, StandardCharsets.ISO_8859_1);
        return FORBIDDEN.stream().anyMatch(content::contains);
    }

    private static boolean contains(String testClassResource) {
        Path compiled = testClassesDirectory().resolve(testClassResource);
        return Files.exists(compiled) && referencesForbiddenType(compiled);
    }

    private static Path mainClassesDirectory() {
        return codeSourceOf(OutboxEntryResponse.class);
    }

    private static Path testClassesDirectory() {
        return codeSourceOf(JacksonFootprintTest.class);
    }

    private static Path codeSourceOf(Class<?> type) {
        try {
            return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Cannot locate the compiled classes of " + type, e);
        }
    }

    /** Exists only to give the previous test something it must find. Never instantiated. */
    @SuppressWarnings("unused")
    private static final class TestFixtureNamingDatabind {
        private com.fasterxml.jackson.databind.ObjectMapper deliberatelyNamedHere;
    }
}
