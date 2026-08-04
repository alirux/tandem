description = "Tandem Spring Boot autoconfiguration — relay engine + CloudEvents publishing (JDBC + Kafka)"

dependencies {
    // Real, redistributed dependencies: the JDBC relay engine and the Kafka publish adapter.
    api(project(":tandem-jdbc"))
    api(project(":tandem-kafka"))

    // Spring is compile-only so no Spring version is propagated to the consumer — the application
    // brings its own Boot 3.x or 4.x and the JVM binds at runtime (LLD-spring-config §1.1).
    compileOnly(platform(libs.spring.boot.dependencies))
    compileOnly(libs.spring.boot.autoconfigure)
    compileOnly(libs.slf4j.api)

    // Optional, like Spring itself: an application that never adds tandem-micrometer must not
    // inherit it. The first optional dependency between two Tandem modules (LLD-micrometer §5, Q31).
    compileOnly(project(":tandem-micrometer"))
    compileOnly(libs.micrometer.core)

    // Generates META-INF/spring-configuration-metadata.json for IDE completion (LLD-spring-config §2.4).
    annotationProcessor(platform(libs.spring.boot.dependencies))
    annotationProcessor(libs.spring.boot.configuration.processor)

    // Tests run against a real Spring context on the baseline (3.x) line; the end-to-end relay test
    // uses TandemTestContainer's real PostgreSQL + Kafka.
    testImplementation(platform(libs.spring.boot.dependencies))
    testImplementation(libs.spring.boot.autoconfigure)
    testImplementation(libs.spring.boot.test)
    testImplementation(libs.slf4j.api)
    // AbstractDataSource is the real base the wiring tests' stub DataSource extends (test-only).
    testImplementation(libs.spring.jdbc)
    testImplementation(project(":tandem-test"))
    // Real Micrometer classes on the test classpath, so the wiring tests can prove the conditional
    // fires with a real MeterRegistry bean present and backs off to NOOP without one.
    testImplementation(project(":tandem-micrometer"))
    testImplementation(libs.micrometer.core)
    // SimpleMeterRegistry (used everywhere else in this module's tests) never materializes real
    // histogram buckets regardless of Timer config, so verifying tandem.metrics.max-publish-latency
    // actually reaches MicrometerTandemMetrics needs a registry that renders one (mirrors the same
    // trade-off in tandem-micrometer's own test suite).
    testImplementation(libs.micrometer.registry.prometheus)
    // The reference-configuration check shared with tandem-spring-producer (LLD-spring-config §2.4) —
    // internal-only, not published (see tandem-test/build.gradle.kts).
    testImplementation(testFixtures(project(":tandem-test")))
}

// The configuration processor reads META-INF/additional-spring-configuration-metadata.json (the hand-written
// entries of LLD-spring-config §2.4) off the *processed* resources, so the resources must be there before
// compileJava runs — without this the file is silently ignored and the keys it documents vanish from the
// metadata the IDE reads.
tasks.named("compileJava") {
    inputs.files(tasks.named("processResources"))
}

// ---------------------------------------------------------------------------------------------------
// Dual-generation matrix (LLD-spring-config §1.2)
//
// The module's main sources are compiled ONCE against the Boot 3.x baseline (compileOnly, above). This
// task re-runs the very same compiled test AND main classes with Spring swapped to the 4.x line on the
// test runtime classpath — which is exactly the binary compatibility the single-artifact strategy bets
// on (§1.1). Only the lightweight context-runner tests run here; the Docker-bound integration test stays
// on the baseline, where its far slower containers buy no extra compatibility signal.
// ---------------------------------------------------------------------------------------------------
val bootFourTestRuntimeClasspath: Configuration by configurations.creating

dependencies {
    bootFourTestRuntimeClasspath(platform(libs.spring.boot.dependencies.v4))
    bootFourTestRuntimeClasspath(libs.spring.boot.autoconfigure)
    bootFourTestRuntimeClasspath(libs.spring.boot.test)
    bootFourTestRuntimeClasspath(libs.spring.jdbc)
    bootFourTestRuntimeClasspath(libs.slf4j.api)
    bootFourTestRuntimeClasspath(project(":tandem-test"))
    bootFourTestRuntimeClasspath(testFixtures(project(":tandem-test")))
    bootFourTestRuntimeClasspath(project(":tandem-micrometer"))
    bootFourTestRuntimeClasspath(libs.micrometer.core)
    bootFourTestRuntimeClasspath(libs.micrometer.registry.prometheus)
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
