plugins {
    // Applied per-subproject below; declared here so it is on the build classpath once.
    id("com.vanniktech.maven.publish") version "0.37.0" apply false
}

allprojects {
    group = "com.codingful"
    // The release workflow injects the tag version via the VERSION env var; default to a SNAPSHOT locally.
    version = System.getenv("VERSION") ?: "0.1.0-SNAPSHOT"
}

// The generated `libs` accessor only resolves in the root script scope, so capture the test
// dependencies here and reference them inside the subprojects block below.
val junitBom = libs.junit.bom
val junitJupiter = libs.junit.jupiter
val assertjCore = libs.assertj.core
val junitLauncher = libs.junit.platform.launcher

// Runnable-but-unpublished modules: each configures its own plugins/toolchain/tests directly
// (tandem-sample is a Java 17 tutorial app; tandem-benchmark needs a newer JDK for virtual threads —
// LLD-benchmark §2) rather than the shared java-library/publishing convention below.
val unpublishedModules = setOf("tandem-sample", "tandem-benchmark")

subprojects {
    if (name !in unpublishedModules) {
        apply(plugin = "com.vanniktech.maven.publish")
    }

    // Every module except the BOM is a Java library on a Java 17 toolchain with JUnit 6 + AssertJ.
    if (name != "tandem-bom" && name !in unpublishedModules) {
        apply(plugin = "java-library")
        apply(plugin = "jacoco")

        configure<JavaPluginExtension> {
            toolchain {
                languageVersion.set(JavaLanguageVersion.of(17))
            }
        }

        tasks.withType<JavaCompile>().configureEach {
            options.compilerArgs.add("-Xlint:all")
        }

        dependencies {
            "testImplementation"(platform(junitBom))
            "testImplementation"(junitJupiter)
            "testImplementation"(assertjCore)
            "testRuntimeOnly"(junitLauncher)
        }

        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
        }

        // `test` runs the Docker-free unit tests; `integrationTest` runs the @Tag("integration")
        // Testcontainers tests. `check` runs both; skip integration locally with
        // `./gradlew check -x integrationTest` (or just run `./gradlew test`).
        val mainSourceSet = the<SourceSetContainer>()["main"]
        val testSourceSet = the<SourceSetContainer>()["test"]
        val integrationTest = tasks.register<Test>("integrationTest") {
            description = "Runs @Tag(\"integration\") tests (require Docker)."
            group = "verification"
            testClassesDirs = testSourceSet.output.classesDirs
            classpath = testSourceSet.runtimeClasspath
            useJUnitPlatform { includeTags("integration") }
            shouldRunAfter(tasks.named("test"))
        }

        // Three JaCoCo reports per module:
        //   jacocoTestReport            — unit tests only       (default, always runs after `test`)
        //   jacocoIntegrationTestReport — integration tests only (runs after `integrationTest`)
        //   jacocoMergedReport          — unit + integration     (runs after `check`; upload this to Codecov)
        //
        // fileTree collects whichever .exec files exist, so that a skipped phase (e.g. -x integrationTest)
        // does not cause the report to fail. dependsOn declares the task-output relationship required by
        // Gradle's incremental-build validation; it also means running a report task directly will
        // run the corresponding test phase first.
        val jacocoExecDir = layout.buildDirectory.dir("jacoco")

        val jacocoIntegrationTestReport = tasks.register<JacocoReport>("jacocoIntegrationTestReport") {
            description = "JaCoCo report for @Tag(\"integration\") tests."
            group = "verification"
            dependsOn(integrationTest)
            executionData(fileTree(jacocoExecDir) { include("integrationTest.exec") })
            sourceSets(mainSourceSet)
        }

        val jacocoMergedReport = tasks.register<JacocoReport>("jacocoMergedReport") {
            description = "Merged JaCoCo report (unit + integration) — upload this to Codecov."
            group = "verification"
            dependsOn(tasks.named("test"), integrationTest)
            executionData(fileTree(jacocoExecDir) { include("*.exec") })
            sourceSets(mainSourceSet)
        }

        tasks.named<Test>("test") {
            useJUnitPlatform { excludeTags("integration") }
            finalizedBy(tasks.named("jacocoTestReport"))
        }

        tasks.named("integrationTest") {
            finalizedBy(jacocoIntegrationTestReport)
        }

        tasks.named("check") {
            dependsOn(integrationTest)
            finalizedBy(jacocoMergedReport)
        }

        // All JacocoReport tasks emit XML + HTML; Codecov consumes jacocoMergedReport.xml.
        tasks.withType<JacocoReport>().configureEach {
            reports {
                xml.required.set(true)
                html.required.set(true)
            }
        }
    }

    // Common Maven Central / POM metadata. Per-module name+description come from the subproject.
    if (name !in unpublishedModules) configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
        publishToMavenCentral()
        signAllPublications()
        pom {
            name.set(project.name)
            description.set(project.description ?: project.name)
            url.set("https://github.com/alirux/tandem")
            licenses {
                license {
                    name.set("The Apache License, Version 2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                }
            }
            developers {
                developer {
                    id.set("codingful")
                    name.set("Codingful")
                }
            }
            scm {
                url.set("https://github.com/alirux/tandem")
                connection.set("scm:git:https://github.com/alirux/tandem.git")
                developerConnection.set("scm:git:ssh://git@github.com/alirux/tandem.git")
            }
        }
    }
}
