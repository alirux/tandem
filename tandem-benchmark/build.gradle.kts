plugins {
    java
    application
}

application {
    mainClass.set("com.codingful.tandem.benchmark.LoadTestRunner")
}

description = "Tandem load/performance benchmark harness (not published) — see docs/HLD-load-testing.md and docs/LLD-benchmark.md"

// JDK 25, not the project-wide 17 (LLD-benchmark §2): the load driver uses virtual threads, which
// need Java 21+, and the Java 24+ no-pinning fix (JEP 491) for blocking JDBC calls to actually
// scale on them. Safe because this module is never published — no consumer sees its bytecode — and
// it depends on the Java 17 tandem artifacts unchanged (a newer JVM runs older bytecode).
configure<JavaPluginExtension> {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:all")
}

dependencies {
    // Pulls in tandem-core, tandem-jdbc, tandem-kafka, kafka-clients, and the Testcontainers
    // (Postgres + Kafka) runtime transitively — reused for container lifecycle + baseline DDL.
    implementation(project(":tandem-test"))

    implementation(libs.hdrhistogram)
    implementation(libs.hikaricp)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)

    // A real SLF4J binding (not the NOP no-op) so tandem-kafka's own logging (HLD-logging.md §2.3)
    // and Testcontainers/Kafka/HikariCP's are visible on a benchmark run — this is a leaf app, not
    // a library, so it may take a concrete logging backend. Must track tandem-kafka's slf4j-api
    // version: an older 1.x binding (e.g. slf4j-nop 1.7.x) is not loadable against a 2.x provider
    // API and SLF4J silently falls back to its own internal NOP with a startup warning.
    runtimeOnly(libs.slf4j.simple)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

// Named task (rather than relying on the application plugin's generic `run`) so the docs'
// `./gradlew :tandem-benchmark:loadTest` (LLD-benchmark §9) actually resolves. Pass LoadTestRunner
// args via --args, e.g. `./gradlew :tandem-benchmark:loadTest --args="--demo S1,S2"`.
tasks.register<JavaExec>("loadTest") {
    description = "Runs the load-test harness (LoadTestRunner). Requires Docker."
    group = "verification"
    mainClass.set("com.codingful.tandem.benchmark.LoadTestRunner")
    classpath = sourceSets["main"].runtimeClasspath
    standardOutput = System.out
    errorOutput = System.err
    // Raises com.codingful.tandem's System.Logger output to DEBUG (default JUL root level is
    // INFO) so the relay's per-cycle claim/reclaim logging is visible on a benchmark run
    // (HLD-logging.md §8) — see src/main/resources/logging.properties.
    systemProperty("java.util.logging.config.file",
            sourceSets["main"].resources.srcDirs.first().resolve("logging.properties").absolutePath)
}

// Mirrors the shared convention's integrationTest task (root build.gradle.kts), hand-rolled here
// because this module opts out of that convention block (different toolchain, not published).
val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs @Tag(\"integration\") tests (require Docker) — the load-test smoke check (LLD-benchmark §9)."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform { includeTags("integration") }
    shouldRunAfter(tasks.named("test"))
}

tasks.named<Test>("test") {
    useJUnitPlatform { excludeTags("integration") }
}

tasks.named("check") {
    dependsOn(integrationTest)
}
