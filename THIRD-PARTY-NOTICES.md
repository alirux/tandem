# Third-Party Notices

Tandem is licensed under the Apache License, Version 2.0 (see [LICENSE](LICENSE)).

Tandem publishes **standard, non-shaded JARs**: third-party libraries are *not*
bundled into Tandem's artifacts. Consumers resolve them separately from Maven
Central, each under its own license. This file lists the third-party libraries
that reach a consumer's **compile / runtime classpath** when depending on a
Tandem module — it is provided as a convenience, for informational purposes only.

Dependencies used only to build and test Tandem itself (e.g. JUnit, AssertJ,
HikariCP, HdrHistogram, slf4j-simple) are **not** listed: they never reach a
consumer. Note that `tandem-test` is the one published module whose *purpose* is
testing, so the libraries it needs — Testcontainers and the PostgreSQL JDBC
driver among them — do reach whoever declares it, and are listed below.

## Runtime footprint by module

| Module         | Redistributed runtime dependencies                  |
|----------------|-----------------------------------------------------|
| `tandem-core`  | none — JDK only                                     |
| `tandem-jdbc`  | none beyond `tandem-core` (JDBC driver is test-only)|
| `tandem-kafka` | `kafka-clients`, `cloudevents-kafka`, `cloudevents-core`, `slf4j-api` |
| `tandem-spring-producer` | none beyond `tandem-jdbc` — **Spring and Jackson are `compileOnly`**, so the application's own versions are used and none is dragged in |
| `tandem-spring-relay` | none beyond `tandem-jdbc` and `tandem-kafka` — Spring is `compileOnly`, as above. `tandem-micrometer` (below) is also `compileOnly` here, so it adds nothing to this module's own footprint |
| `tandem-micrometer` | `micrometer-core` |
| `tandem-admin` | none beyond `tandem-jdbc` — Spring and Jackson are `compileOnly`, as above |
| `tandem-test`  | `testcontainers-postgresql`, `testcontainers-kafka`, `kafka-clients`, and the `postgresql` JDBC driver (runtime). Meant for a consumer's **test** scope, but its POM declares them at compile/runtime scope, so they are redistributed like any other dependency. Testcontainers itself brings a substantial transitive tree (docker-java, Jackson, commons-compress, …) not enumerated here |
| `tandem-bom`   | none — a POM with no code, publishing only version constraints |

## Dependencies

The complete set of third-party libraries a consumer may pull onto the
compile / runtime classpath is:

| Library                                 | Version | License      |
|-----------------------------------------|---------|--------------|
| org.apache.kafka:kafka-clients          | 3.9.2   | Apache-2.0   |
| io.cloudevents:cloudevents-kafka        | 4.1.1   | Apache-2.0   |
| io.cloudevents:cloudevents-core         | 4.1.1   | Apache-2.0   |
| org.slf4j:slf4j-api                     | 2.0.16  | MIT          |
| org.testcontainers:postgresql           | 1.21.4  | MIT          |
| org.testcontainers:kafka                | 1.21.4  | MIT          |
| org.postgresql:postgresql               | 42.7.12 | BSD-2-Clause |
| io.micrometer:micrometer-core            | 1.13.6  | Apache-2.0   |

`cloudevents-core` is pulled in transitively by `cloudevents-kafka`. The three
entries below `slf4j-api` reach only consumers of `tandem-test`; each Testcontainers
module additionally pulls `org.testcontainers:testcontainers` and its own transitive
dependencies, which are not enumerated here.

## `tandem-cli` (Go binary)

`tandem-cli` is not a JVM module — it is a separately versioned Go binary
(`tandem-cli/`, its own `go.mod`, LLD-cli.md §2/§9.1), so the "reaches a consumer's
classpath" framing above does not apply to it. The equivalent inheritance event is
**static linking**: every dependency below is compiled directly into the distributed
`tandem-cli` binary (Go has no dynamic linking for these), so all of them, not just
direct requires, reach whoever downloads a release. The list is the actual build-info
dependency set of a compiled binary (`go version -m`), not `go.mod`'s full module graph,
which also lists modules only `go generate`'s code-generator tool needs and that never
compile into `tandem-cli` itself.

| Library                                   | Version | License      |
|--------------------------------------------|---------|--------------|
| github.com/spf13/cobra                     | 1.10.2  | Apache-2.0   |
| github.com/spf13/pflag                     | 1.0.9   | BSD-3-Clause |
| github.com/inconshreveable/mousetrap       | 1.1.0   | Apache-2.0   |
| github.com/oapi-codegen/runtime            | 1.6.0   | Apache-2.0   |
| github.com/google/uuid                     | 1.6.0   | BSD-3-Clause |
| github.com/apapsch/go-jsonmerge/v2         | 2.0.0   | MIT          |

`mousetrap` links in only on Windows builds (it is cobra's own conditional dependency
for detecting a console-less launch there); the other five link in on every platform
`goreleaser` cross-compiles for (darwin/linux/windows × amd64/arm64, LLD-cli.md §9).
`oapi-codegen` itself (the code generator) does **not** appear here: it runs at
`go generate` time only, from a separate tools module (`tandem-cli/tools/`) kept apart
specifically so its own, larger dependency tree — and its higher minimum Go version —
never reaches the shipped binary or this table.

## License texts

### Apache License 2.0

Applies to: `kafka-clients`, `cloudevents-kafka`, `cloudevents-core` — and, in
`tandem-cli`'s statically-linked binary, `cobra`, `mousetrap`, `oapi-codegen/runtime`.

The full text of the Apache License, Version 2.0 is available in [LICENSE](LICENSE)
and at https://www.apache.org/licenses/LICENSE-2.0.

### BSD 3-Clause License

Applies to, in `tandem-cli`'s statically-linked binary: `pflag`, `google/uuid`.

```
Copyright (c) 2012 Alex Ogier. All rights reserved.
Copyright (c) 2012 The Go Authors. All rights reserved.
Copyright (c) 2009,2014 Google Inc. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

Each project's copyright line above is its own; the terms are identical BSD-3-Clause
boilerplate in both.

### MIT License

Applies to: `slf4j-api` and, in `tandem-cli`'s statically-linked binary,
`go-jsonmerge/v2` (© 2016-2019 Artur Kraev — the same MIT terms below, different
copyright holder).

```
Copyright (c) 2004-2023 QOS.ch Sarl (Switzerland)
All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

### BSD 2-Clause License

Applies to `org.postgresql:postgresql`.

```
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
```
