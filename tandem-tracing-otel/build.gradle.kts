description = "Tandem OpenTelemetry adapter — trace capture and relay publish spans without Spring"

dependencies {
    api(project(":tandem-core"))
    // Not optional here: OpenTelemetry is the entire reason this module exists, the same reasoning
    // tandem-micrometer applies to micrometer-core (LLD-micrometer §1). Only the API — the SDK is the
    // application's own choice, exactly as with any other OTel instrumentation library.
    api(libs.opentelemetry.api)

    // A real SDK exporting into memory, so the tests assert genuinely exported spans with genuinely
    // resolved parents rather than a hand-written stand-in. The BOM is needed because this module,
    // unlike the Spring ones, has no Spring Boot BOM to version the SDK coordinates.
    testImplementation(platform(libs.opentelemetry.bom))
    testImplementation(libs.opentelemetry.sdk)
    testImplementation(libs.opentelemetry.sdk.testing)
}
