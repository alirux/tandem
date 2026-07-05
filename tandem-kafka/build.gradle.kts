description = "Tandem Kafka adapter — OutboxDispatcher over the Kafka producer (CloudEvents binary binding)"

dependencies {
    api(project(":tandem-core"))
    // Relay-side only — never on the client write-side (§1.3).
    api(libs.kafka.clients)
    api(libs.cloudevents.kafka)   // brings cloudevents-core transitively
    // slf4j-api is already a runtime-scope transitive of kafka-clients; declaring it explicitly
    // makes it usable at compile time and pins it to a current version (HLD-logging.md §2.2).
    api(libs.slf4j.api)

    // Unit tests use Kafka's own in-memory MockProducer (a real test double, not a mock framework).
    testImplementation(project(":tandem-test"))
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.kafka)
}
