description = "Tandem Admin API — optional REST operations layer over the outbox and the relay (API-first, off by default)"

dependencies {
    // Real, redistributed dependency: the OutboxQuery adapter (JdbcOutboxQuery) this module's use
    // cases run against.
    api(project(":tandem-jdbc"))

    // Spring is compile-only so no Spring version is propagated to the consumer — the application
    // brings its own Boot 3.x or 4.x and the JVM binds at runtime (LLD-spring-config §1.1), same
    // discipline as tandem-spring-relay. spring-web (not spring-webmvc) is enough to compile
    // @RestController/@RestControllerAdvice classes; the Servlet dispatch machinery comes from the
    // consuming application's own spring-boot-starter-web.
    compileOnly(platform(libs.spring.boot.dependencies))
    compileOnly(libs.spring.boot.autoconfigure)
    compileOnly(libs.spring.web)
    compileOnly(libs.slf4j.api)
    // Never bundled: a Spring Boot web application always carries Jackson (spring-boot-starter-json),
    // exactly like Spring itself. Needed here to render the payload column as JSON, not as an escaped
    // string, in the wire DTOs (never a core type — HLD-admin-api §4).
    compileOnly(libs.jackson.databind)
    compileOnly(libs.jackson.datatype.jsr310)

    // Generates META-INF/spring-configuration-metadata.json for IDE completion (LLD-spring-config §2.4).
    annotationProcessor(platform(libs.spring.boot.dependencies))
    annotationProcessor(libs.spring.boot.configuration.processor)

    // Unit tests: the real OutboxQuery collaborator, no database (AGENTS: no mocks).
    testImplementation(project(":tandem-test"))

    // Tests run against a real Spring MVC dispatch (MockMvc) on the baseline (3.x) line.
    testImplementation(platform(libs.spring.boot.dependencies))
    testImplementation(libs.spring.boot.autoconfigure)
    testImplementation(libs.spring.boot.test)
    testImplementation(libs.spring.boot.test.autoconfigure)
    testImplementation(libs.spring.web)
    testImplementation(libs.spring.webmvc)
    testImplementation(libs.spring.test)
    // AbstractDataSource is the real base the wiring tests' stub DataSource extends (test-only).
    testImplementation(libs.spring.jdbc)
    testImplementation(libs.jakarta.servlet.api)
    testImplementation(libs.jackson.databind)
    testImplementation(libs.jackson.datatype.jsr310)
    testImplementation(libs.json.path)
    testImplementation(libs.openapi.request.validator.core)
    testImplementation(libs.slf4j.api)

    // Integration tests: a real PostgreSQL via Testcontainers + the JDBC driver at runtime.
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgresql)
    testRuntimeOnly(libs.postgresql)
}

// The configuration processor reads META-INF/additional-spring-configuration-metadata.json (the hand-written
// entries of LLD-spring-config §2.4) off the *processed* resources, so the resources must be there before
// compileJava runs — without this the file is silently ignored and the keys it documents vanish from the
// metadata the IDE reads.
tasks.named("compileJava") {
    inputs.files(tasks.named("processResources"))
}

// ---------------------------------------------------------------------------------------------------
// Dual-generation matrix (LLD-spring-config §1.2) — this is a Spring module, so it needs the same gate
// as tandem-spring-relay/tandem-spring-producer: the module's main sources compile ONCE against the
// Boot 3.x baseline (compileOnly, above); this task re-runs the very same compiled test AND main
// classes with Spring swapped to the 4.x line. Only the Docker-free tests run here.
// ---------------------------------------------------------------------------------------------------
val bootFourTestRuntimeClasspath: Configuration by configurations.creating

dependencies {
    bootFourTestRuntimeClasspath(platform(libs.spring.boot.dependencies.v4))
    bootFourTestRuntimeClasspath(libs.spring.boot.autoconfigure)
    bootFourTestRuntimeClasspath(libs.spring.boot.test)
    bootFourTestRuntimeClasspath(libs.spring.boot.test.autoconfigure)
    bootFourTestRuntimeClasspath(libs.spring.web)
    bootFourTestRuntimeClasspath(libs.spring.webmvc)
    bootFourTestRuntimeClasspath(libs.spring.test)
    bootFourTestRuntimeClasspath(libs.spring.jdbc)
    bootFourTestRuntimeClasspath(libs.jakarta.servlet.api)
    bootFourTestRuntimeClasspath(libs.jackson.databind)
    bootFourTestRuntimeClasspath(libs.jackson.datatype.jsr310)
    bootFourTestRuntimeClasspath(libs.json.path)
    bootFourTestRuntimeClasspath(libs.openapi.request.validator.core)
    bootFourTestRuntimeClasspath(libs.slf4j.api)
    bootFourTestRuntimeClasspath(project(":tandem-test"))
    bootFourTestRuntimeClasspath(platform(libs.junit.bom))
    bootFourTestRuntimeClasspath(libs.junit.jupiter)
    bootFourTestRuntimeClasspath(libs.junit.platform.launcher)
    bootFourTestRuntimeClasspath(libs.assertj.core)
}

val sourceSets = the<SourceSetContainer>()
val mainOutput = sourceSets["main"].output
val testOutput = sourceSets["test"].output

val bootFourTest = tasks.register<Test>("bootFourTest") {
    description = "Re-runs the unit tests against the Spring Boot 4.x line (dual-generation matrix)."
    group = "verification"
    testClassesDirs = testOutput.classesDirs
    classpath = files(testOutput, mainOutput, bootFourTestRuntimeClasspath)
    // boot3-only: TandemAdminEndToEndTest's @AutoConfigureMockMvc does not contribute a MockMvc bean
    // on the 4.x line under this setup — a spring-boot-test-autoconfigure discrepancy, not a
    // tandem-admin compatibility gap (see the test's own javadoc).
    useJUnitPlatform { excludeTags("integration", "boot3-only") }
    shouldRunAfter(tasks.named("test"))
}

tasks.named("check") {
    dependsOn(bootFourTest)
}
