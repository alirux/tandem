description = "Tandem Kafka adapter — OutboxDispatcher over the Kafka producer (CloudEvents binary binding)"

dependencies {
    api(project(":tandem-core"))
    // Relay-side only — never on the client write-side (§1.3).
    api(libs.kafka.clients)
    api(libs.cloudevents.kafka)   // brings cloudevents-core transitively
    // slf4j-api is already a runtime-scope transitive of kafka-clients; declaring it explicitly
    // makes it usable at compile time and pins it to a current version (HLD-logging.md §2.2).
    api(libs.slf4j.api)

    // kafka-clients 3.9.2 resolves lz4-java 1.10.1, which carries a moderate advisory
    // (GHSA-xx22-p4ch-683r: the native XXHash bindings can crash the JVM when passed invalid
    // byte-array ranges), fixed in 1.11.1. Not reachable from Kafka's compression usage, but
    // tandem-kafka is published, so consumers inherit the flagged coordinate on their runtime
    // classpath and see it in their own scans. Raise the floor rather than make every consumer
    // do it. This is a constraint, not a dependency: it sets a minimum if lz4-java is present,
    // and pulls in nothing on its own. Drop it once kafka-clients ships >= 1.11.1.
    constraints {
        api(libs.lz4.java) {
            because("CVE remediation: lz4-java < 1.11.1 can crash the JVM via invalid XXHash byte ranges")
        }
    }

    // Unit tests use Kafka's own in-memory MockProducer (a real test double, not a mock framework).
    testImplementation(project(":tandem-test"))
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.kafka)
}
