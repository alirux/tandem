# Third-Party Notices

Tandem is licensed under the Apache License, Version 2.0 (see [LICENSE](LICENSE)).

Tandem publishes **standard, non-shaded JARs**: third-party libraries are *not*
bundled into Tandem's artifacts. Consumers resolve them separately from Maven
Central, each under its own license. This file lists the third-party libraries
that reach a consumer's **compile / runtime classpath** when depending on a
Tandem module — it is provided as a convenience, for informational purposes only.

Test-only and benchmark-only dependencies (e.g. JUnit, AssertJ, Testcontainers,
PostgreSQL JDBC driver, HikariCP, HdrHistogram, slf4j-simple) are **not** listed:
they never enter the published runtime classpath.

## Runtime footprint by module

| Module         | Redistributed runtime dependencies                  |
|----------------|-----------------------------------------------------|
| `tandem-core`  | none — JDK only                                     |
| `tandem-jdbc`  | none beyond `tandem-core` (JDBC driver is test-only)|
| `tandem-kafka` | `kafka-clients`, `cloudevents-kafka`, `cloudevents-core`, `slf4j-api` |

## Dependencies

The complete set of third-party libraries a consumer may pull onto the
compile / runtime classpath is:

| Library                                 | Version | License      |
|-----------------------------------------|---------|--------------|
| org.apache.kafka:kafka-clients          | 3.9.2   | Apache-2.0   |
| io.cloudevents:cloudevents-kafka        | 4.1.1   | Apache-2.0   |
| io.cloudevents:cloudevents-core         | 4.1.1   | Apache-2.0   |
| org.slf4j:slf4j-api                     | 2.0.16  | MIT          |

`cloudevents-core` is pulled in transitively by `cloudevents-kafka`.

## License texts

### Apache License 2.0

Applies to: `kafka-clients`, `cloudevents-kafka`, `cloudevents-core`.

The full text of the Apache License, Version 2.0 is available in [LICENSE](LICENSE)
and at https://www.apache.org/licenses/LICENSE-2.0.

### MIT License

Applies to: `slf4j-api`.

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
