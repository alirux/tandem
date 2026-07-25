plugins {
    // Lets the Java toolchain auto-provision JDK 17 when it is not already installed.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

rootProject.name = "tandem"

include(
    "tandem-bom",
    "tandem-core",
    "tandem-jdbc",
    "tandem-kafka",
    "tandem-test",
    "tandem-spring-producer",
    "tandem-sample",
    "tandem-sample-spring",
    "tandem-benchmark",
    "tandem-coverage",
)
