description = "Tandem core — models, ports, exceptions and pure logic (zero runtime dependencies)"

// Invariant (LLD-core §1.3): tandem-core has NO external runtime dependencies — JDK only.
// Test-scope dependencies (JUnit, AssertJ) are configured by the root build and never leak
// into the published runtime classpath.
