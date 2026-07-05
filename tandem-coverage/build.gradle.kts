// Build-only module: it produces a single, project-wide JaCoCo coverage report and publishes nothing.
// The `jacoco` plugin is applied on its own (no java-library) purely to make the JacocoReport task type
// and the JaCoCo ant tooling available here.
plugins {
    jacoco
}

// Every module whose main sources should appear in the aggregated report.
val coveredProjects = listOf(
    ":tandem-core",
    ":tandem-jdbc",
    ":tandem-kafka",
    ":tandem-test",
).map { project(it) }

// Force each covered module to be configured before we read its source sets and test tasks below.
// Safe here because none of them depends back on tandem-coverage.
coveredProjects.forEach { evaluationDependsOn(it.path) }

// One JaCoCo report that re-maps ALL execution data — every module, every test phase (unit, integration,
// e2e) — against ALL modules' main sources in a single pass. Per-module reports scope themselves to their
// own sourceSet (`sourceSets(mainSourceSet)`), so coverage that a test in one module produces for a class
// owned by another module (e.g. tandem-jdbc's integration tests exercising tandem-core's ReplayResult) is
// dropped: it never lands in any per-module XML. Codecov's file-level merge cannot recover it because no
// uploaded report ever attributes that hit to the owning class. This task does, by design.
val aggregatedCoverageReport = tasks.register<JacocoReport>("aggregatedCoverageReport") {
    description = "Aggregated JaCoCo coverage across all modules and all test phases (unit + integration + e2e)."
    group = "verification"

    coveredProjects.forEach { covered ->
        val mainSourceSet = covered.the<SourceSetContainer>()["main"]
        sourceDirectories.from(mainSourceSet.allSource.sourceDirectories)
        classDirectories.from(mainSourceSet.output.classesDirs)

        // Run every test phase so its .exec exists before we read it; mirrors the per-module report wiring.
        dependsOn(covered.tasks.named("test"))
        dependsOn(covered.tasks.named("integrationTest"))

        // Collect whatever .exec files each module produced — test.exec, integrationTest.exec, and any
        // future phase (e.g. e2e.exec) — so a new test phase is picked up here with no change. fileTree
        // tolerates a skipped phase (e.g. `-x integrationTest`) instead of failing the report.
        executionData.from(
            covered.fileTree(covered.layout.buildDirectory.dir("jacoco")) { include("*.exec") }
        )
    }

    // Fixed output paths so CI can reference the XML deterministically.
    reports {
        xml.required.set(true)
        html.required.set(true)
        xml.outputLocation.set(layout.buildDirectory.file("reports/jacoco/aggregated/aggregated.xml"))
        html.outputLocation.set(layout.buildDirectory.dir("reports/jacoco/aggregated/html"))
    }
}
