plugins {
    java
    application
}

application {
    mainClass.set("com.codingful.tandem.sample.SampleApplication")
}

description = "Tandem sample application — runnable end-to-end tutorial (not published)"

configure<JavaPluginExtension> {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:all")
}

dependencies {
    // TandemTestContainer pulls in tandem-core, tandem-jdbc, tandem-kafka, kafka-clients,
    // and the Testcontainers (Postgres + Kafka) runtime — all transitively.
    implementation(project(":tandem-test"))

    // A real SLF4J binding (not the NOP no-op) so tandem-kafka's own logging (HLD-logging.md §2.3)
    // and Testcontainers/Kafka's are visible when the sample runs — this is a leaf app, not a
    // library. Must track tandem-kafka's slf4j-api version: an older 1.x binding is not loadable
    // against a 2.x provider API and SLF4J silently falls back to its own internal NOP with a
    // startup warning.
    runtimeOnly(libs.slf4j.simple)
}
