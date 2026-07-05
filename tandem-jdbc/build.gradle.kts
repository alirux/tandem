description = "Tandem JDBC adapter — write-side insert and the relay engine (PostgreSQL baseline)"

dependencies {
    api(project(":tandem-core"))
    // Adapter uses only java.sql (JDK). No Kafka, no metrics library, no JSON binding (minimal footprint).

    // Unit tests: the in-memory collaborators (no DB).
    testImplementation(project(":tandem-test"))

    // Integration tests: a real PostgreSQL via Testcontainers + the JDBC driver at runtime.
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgresql)
    testRuntimeOnly(libs.postgresql)
}
