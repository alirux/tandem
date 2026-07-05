plugins {
    `java-platform`
}

description = "Tandem BOM — aligns versions of all Tandem modules"

dependencies {
    constraints {
        api(project(":tandem-core"))
        api(project(":tandem-jdbc"))
        api(project(":tandem-kafka"))
        api(project(":tandem-test"))
    }
}
