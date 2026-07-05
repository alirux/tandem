description = "Tandem test support — in-memory collaborators and the Testcontainers helper"

dependencies {
    api(project(":tandem-core"))

    // The in-memory helpers (InMemoryOutbox, RecordingDispatcher) depend only on tandem-core; the
    // TandemTestContainer helper wires the real adapters + brokers, so tandem-test (main) depends on
    // tandem-jdbc / tandem-kafka. This is acyclic: jdbc/kafka use tandem-test only in their *test*
    // configurations, and neither jdbc-main nor kafka-main depends on tandem-test.
    api(project(":tandem-jdbc"))
    api(project(":tandem-kafka"))

    // Brokers + driver for the container helper (test-support library, so these are part of its API).
    api(platform(libs.testcontainers.bom))
    api(libs.testcontainers.postgresql)
    api(libs.testcontainers.kafka)
    api(libs.kafka.clients)
    runtimeOnly(libs.postgresql)
}
