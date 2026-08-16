description = "Tandem JDBC adapter — write-side insert and the relay engine (PostgreSQL baseline)"

val liquibase by configurations.creating

dependencies {
    api(project(":tandem-core"))
    // Adapter uses only java.sql (JDK). No Kafka, no metrics library, no JSON binding (minimal footprint).

    // Unit tests: the in-memory collaborators (no DB).
    testImplementation(project(":tandem-test"))

    // Integration tests: a real PostgreSQL via Testcontainers + the JDBC driver at runtime.
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgresql)
    testRuntimeOnly(libs.postgresql)

    liquibase(libs.liquibase.core)
    liquibase(libs.picocli) { version { require("4.7.7") } }
}

// The schema's source of truth is the Liquibase changelog under schema/postgres/changelog; the flat
// schema/postgres/tandem-baseline.sql an operator applies to an empty database is generated from it
// (LLD-jdbc §6). Liquibase lives on its own resolvable configuration so it stays a build-time tool:
// it is not on any compile or runtime classpath and never reaches the published POM.
val schemaDir = rootProject.layout.projectDirectory.dir("schema/postgres")

// Adopters running Liquibase themselves point at the changelog on the classpath rather than copying
// files out of the repository, so it ships inside the jar. Resources carry no dependency weight, so
// this does not widen the client footprint. The path is a published contract — renaming it breaks
// every `spring.liquibase.change-log` that references it.
tasks.named<ProcessResources>("processResources") {
    from(schemaDir.dir("changelog")) {
        into("tandem/schema/postgres/changelog")
        exclude("baseline-header.sql")
    }
}

val changelogDir = schemaDir.dir("changelog")
val baselineSql = schemaDir.file("tandem-baseline.sql")
// Liquibase renders the statements only; the contract preamble an operator reads is prepended from a
// file rather than inlined here, so the build script holds no prose.
val baselineHeader = changelogDir.file("baseline-header.sql")
val renderedStatements = layout.buildDirectory.file("schema/statements.sql")

val renderSchemaSql by tasks.registering(JavaExec::class) {
    group = "schema"
    description = "Renders the changelog to SQL under the build directory."

    // Offline mode keeps its applied-changeset history in a CSV next to the changelog by default.
    // Point it at the build directory and delete it every run: this task must render the schema from
    // nothing each time, or a second run would emit only the changesets added since the first.
    val history = layout.buildDirectory.file("schema/databasechangelog.csv")

    inputs.dir(changelogDir)
    outputs.file(renderedStatements)

    classpath = liquibase
    mainClass.set("liquibase.integration.commandline.LiquibaseCommandLine")
    // Liquibase resolves an unqualified changelog name against its search path, which defaults to
    // the working directory.
    workingDir = changelogDir.asFile
    // An offline URL renders the DDL without a live database, so generating the schema needs no
    // container and no credentials.
    argumentProviders.add(CommandLineArgumentProvider {
        listOf(
            "update-sql",
            "--changelog-file=db.changelog-master.xml",
            "--url=offline:postgresql?changeLogFile=${history.get().asFile.absolutePath}",
            "--output-file=${renderedStatements.get().asFile.absolutePath}",
        )
    })

    doFirst {
        renderedStatements.get().asFile.parentFile.mkdirs()
        history.get().asFile.delete()
    }
}

// Liquibase opens its output with a banner carrying a wall-clock "Ran at" stamp, which would change
// the file on every run and leave the drift gate permanently red. Keeping only from the first
// changeset marker on makes the baseline a pure function of the changelog.
fun renderedBaseline(): String {
    val lines = renderedStatements.get().asFile.readLines()
    val firstChangeset = lines.indexOfFirst { it.startsWith("-- Changeset ") }
    check(firstChangeset >= 0) { "Liquibase produced no changesets; the changelog may be empty." }
    return baselineHeader.asFile.readText() + "\n" + lines.drop(firstChangeset).joinToString("\n") + "\n"
}

val generateBaselineSql by tasks.registering {
    group = "schema"
    description = "Regenerates schema/postgres/tandem-baseline.sql from the Liquibase changelog."
    dependsOn(renderSchemaSql)
    inputs.dir(changelogDir)
    outputs.file(baselineSql)
    doLast { baselineSql.asFile.writeText(renderedBaseline()) }
}

// Fails the build when the committed baseline no longer matches what the changelog produces — the
// same regenerate-and-diff gate tandem-cli applies to its generated Admin API client. It compares
// against the file on disk rather than asking git, so an uncommitted edit is not mistaken for drift.
val checkBaselineSql by tasks.registering {
    group = "verification"
    description = "Fails if schema/postgres/tandem-baseline.sql has drifted from the changelog."
    dependsOn(renderSchemaSql)
    inputs.dir(changelogDir)
    inputs.file(baselineSql)
    doLast {
        if (baselineSql.asFile.readText() != renderedBaseline()) {
            throw GradleException(
                "schema/postgres/tandem-baseline.sql is out of date with the changelog under " +
                    "schema/postgres/changelog. Run ./gradlew generateBaselineSql and commit the result.",
            )
        }
    }
}

tasks.named("check") { dependsOn(checkBaselineSql) }
