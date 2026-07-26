description = "Tandem Spring Boot autoconfiguration — write-side (outbox INSERT + the convenience tiers, NO Kafka)"

dependencies {
    // Real, redistributed dependency: the write-side JDBC adapter (which re-exports tandem-core).
    api(project(":tandem-jdbc"))

    // Spring is compile-only so no Spring version is propagated to the consumer — the application
    // brings its own Boot 3.x or 4.x and the JVM binds at runtime (LLD-spring-config §1.1). The BOM
    // only aligns the compile-time versions; it is not published.
    compileOnly(platform(libs.spring.boot.dependencies))
    compileOnly(libs.spring.boot.autoconfigure)
    compileOnly(libs.spring.tx)
    compileOnly(libs.spring.aspects)
    // TransactionAwareDataSourceProxy — makes the write-side insert join the Spring transaction. Present
    // in any Spring app with a DataSource (DataSourceAutoConfiguration pulls spring-jdbc), so compileOnly.
    compileOnly(libs.spring.jdbc)
    // Optional payload serializer — auto-configured only when Jackson is on the consumer's classpath
    // (LLD-spring-producer §2); never forced, hence compile-only.
    compileOnly(libs.jackson.databind)
    // Spring Boot always ships SLF4J; declaring it compile-only adds nothing to the footprint.
    compileOnly(libs.slf4j.api)

    // Generates META-INF/spring-configuration-metadata.json for IDE completion (LLD-spring-config §2.4).
    annotationProcessor(platform(libs.spring.boot.dependencies))
    annotationProcessor(libs.spring.boot.configuration.processor)

    // Tests run against a real Spring context on the baseline (3.x) line, with InMemoryOutbox as the
    // real outbox collaborator (no mocks, no database) — the dual-generation 4.x suite is added on top.
    testImplementation(platform(libs.spring.boot.dependencies))
    testImplementation(libs.spring.boot.autoconfigure)
    testImplementation(libs.spring.tx)
    testImplementation(libs.spring.aspects)
    testImplementation(libs.spring.boot.test)
    testImplementation(libs.jackson.databind)
    testImplementation(libs.slf4j.api)
    // AbstractDataSource is the real base the wiring tests' stub DataSource extends (test-only).
    testImplementation(libs.spring.jdbc)
    testImplementation(project(":tandem-test"))
}

// ---------------------------------------------------------------------------------------------------
// Dual-generation matrix (LLD-spring-config §1.2)
//
// The module's main sources are compiled ONCE against the Boot 3.x baseline (compileOnly, above). This
// task re-runs the very same compiled test AND main classes with Spring swapped to the 4.x line on the
// test runtime classpath — which is exactly the binary compatibility the single-artifact strategy bets
// on (§1.1). This module carries the matrix's real signal: its context-runner tests assert the
// autoconfiguration actually applies and contributes each tier's beans, so a moved or re-signed Spring
// symbol fails here loudly. The Docker-bound integration tests stay on the baseline.
// ---------------------------------------------------------------------------------------------------
val bootFourTestRuntimeClasspath: Configuration by configurations.creating

dependencies {
    bootFourTestRuntimeClasspath(platform(libs.spring.boot.dependencies.v4))
    bootFourTestRuntimeClasspath(libs.spring.boot.autoconfigure)
    bootFourTestRuntimeClasspath(libs.spring.boot.test)
    bootFourTestRuntimeClasspath(libs.spring.tx)
    bootFourTestRuntimeClasspath(libs.spring.aspects)
    bootFourTestRuntimeClasspath(libs.spring.jdbc)
    bootFourTestRuntimeClasspath(libs.jackson.databind)
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
    useJUnitPlatform { excludeTags("integration") }
    shouldRunAfter(tasks.named("test"))
}

tasks.named("check") {
    dependsOn(bootFourTest)
}
