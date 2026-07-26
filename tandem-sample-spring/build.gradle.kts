plugins {
    java
    application
}

application {
    mainClass.set("com.codingful.tandem.sample.spring.SampleSpringApplication")
}

description = "Tandem Spring sample — the write-side developer experience with Spring Boot (not published)"

configure<JavaPluginExtension> {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:all")
}

dependencies {
    // The Spring Boot runtime this leaf app actually runs on (real dependencies, not compileOnly like
    // the library modules). The BOM aligns versions; Boot 3.x is the baseline the modules compile against.
    implementation(platform(libs.spring.boot.dependencies))
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")  // DataSource + transaction manager
    implementation("org.springframework.boot:spring-boot-starter-json")  // Jackson — enables the object-payload tier
    implementation("org.springframework.boot:spring-boot-starter-aop")   // enables the @TransactionalOutbox aspect

    // The Tandem Spring modules under demonstration: the write-side tiers and the relay autoconfig
    // (so the relay runs itself, wired by Spring, rather than being assembled by hand).
    implementation(project(":tandem-spring-producer"))
    implementation(project(":tandem-spring-relay"))

    // [DEMO-ONLY] TandemTestContainer spins up a real PostgreSQL (and Kafka) so the sample is
    // self-contained; a real app supplies its own DataSource. Also provides the relay + consumer helpers
    // used by the end-to-end tail. Pulls in tandem-jdbc/kafka/core and the JDBC driver transitively.
    implementation(project(":tandem-test"))

    // This module is unpublished, so the root convention block configures neither JUnit/AssertJ nor an
    // integrationTest phase for it (AGENTS, "Adding a module") — the smoke test declares both itself.
    // spring-boot-starter-test is deliberately avoided: it would put its own JUnit 5 line on the
    // classpath against the project's JUnit 6.
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.spring.boot.test)
    testImplementation(libs.spring.test)
}

// The smoke test boots the real sample application against real containers, so it is Docker-bound and
// tagged `integration` like every other container-backed test in the project: `./gradlew test` stays
// Docker-free, `check` runs it.
val sourceSets = the<SourceSetContainer>()
val testSourceSet = sourceSets["test"]

val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs the @Tag(\"integration\") smoke test (requires Docker)."
    group = "verification"
    testClassesDirs = testSourceSet.output.classesDirs
    classpath = testSourceSet.runtimeClasspath
    useJUnitPlatform { includeTags("integration") }
    shouldRunAfter(tasks.named("test"))
}

tasks.named<Test>("test") {
    useJUnitPlatform { excludeTags("integration") }
}

tasks.named("check") {
    dependsOn(integrationTest)
}
