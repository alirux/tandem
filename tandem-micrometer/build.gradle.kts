description = "Tandem Micrometer adapter — TandemMetrics backed by a Micrometer MeterRegistry"

dependencies {
    api(project(":tandem-core"))
    // Relay-side only — never on the client write-side (§1.3). Not optional here: Micrometer is the
    // whole reason this module exists, unlike Spring's compileOnly treatment one layer up.
    api(libs.micrometer.core)
}
