description = "Tandem test support — in-memory collaborators and the Testcontainers helper"

// Internal-only helpers shared across modules' own test suites (currently: the Spring modules'
// configuration-reference check), never published — a Tandem consumer has no use for it. Kept out of
// `main` so it never enters the published jar's API surface or javadoc, unlike the real collaborators
// above; consuming modules add `testImplementation(testFixtures(project(":tandem-test")))`.
apply(plugin = "java-test-fixtures")

// java-test-fixtures otherwise publishes its jar (and sources jar) as extra artifacts of the "java"
// component by default — the opposite of "internal-only" above. Skip every testFixtures variant
// explicitly; the module still builds and is consumable in-repo via testFixtures(project(...)).
// Deferred to afterEvaluate: the maven-publish plugin (applied by the root convention, before this
// script runs) registers these configurations lazily, so looking them up eagerly here is too early.
afterEvaluate {
    components.withType<AdhocComponentWithVariants>().configureEach {
        listOf("testFixturesApiElements", "testFixturesRuntimeElements", "testFixturesSourcesElements")
                .mapNotNull { configurations.findByName(it) }
                .forEach { withVariantsFromConfiguration(it) { skip() } }
    }
}

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

    // Testcontainers 1.21.4 resolves commons-compress 1.24.0, which carries two moderate DoS
    // advisories (GHSA DUMP infinite loop / Pack200 OOM), both fixed in 1.26.0. Neither is
    // reachable from Testcontainers' usage — it only writes tar for the image build context —
    // but tandem-test is published, so consumers inherit the flagged coordinate on their runtime
    // classpath and see it in their own scans. Raise the floor rather than make every consumer
    // do it. This is a constraint, not a dependency: it sets a minimum if commons-compress is
    // present, and pulls in nothing on its own. Drop it once Testcontainers ships >= 1.26.0.
    constraints {
        api(libs.commons.compress) {
            because("CVE remediation: commons-compress < 1.26.0 has two moderate DoS advisories")
        }
    }
}
