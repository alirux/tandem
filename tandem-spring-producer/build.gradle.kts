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
