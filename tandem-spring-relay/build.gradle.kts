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
}
