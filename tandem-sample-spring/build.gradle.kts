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
}
