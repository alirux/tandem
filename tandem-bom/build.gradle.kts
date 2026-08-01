plugins {
    `java-platform`
}

description = "Tandem BOM — aligns versions of all Tandem modules"

dependencies {
    // Every published module, so a consumer that imports this BOM can declare any of them without a
    // version. A module missing here is unusable that way — add new published modules in the same change.
    constraints {
        api(project(":tandem-core"))
        api(project(":tandem-jdbc"))
        api(project(":tandem-kafka"))
        api(project(":tandem-test"))
        api(project(":tandem-spring-producer"))
        api(project(":tandem-spring-relay"))
        api(project(":tandem-micrometer"))
    }
}
