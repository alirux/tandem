description = "Tandem Micrometer adapter — TandemMetrics backed by a Micrometer MeterRegistry"

dependencies {
    api(project(":tandem-core"))
    // Relay-side only — never on the client write-side (§1.3). Not optional here: Micrometer is the
    // whole reason this module exists, unlike Spring's compileOnly treatment one layer up.
    api(libs.micrometer.core)
    // Test-only: SimpleMeterRegistry (used by the rest of this module's tests) never materializes real
    // histogram buckets, so verifying the publish.latency ceiling needs a registry that actually renders
    // one. No redistributed footprint — excluded from THIRD-PARTY-NOTICES per AGENTS' module checklist.
    testImplementation(libs.micrometer.registry.prometheus)
}
