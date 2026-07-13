# Security Policy

## Supported versions

Tandem is pre-1.0. Security fixes target the latest published release on Maven Central
(`com.codingful:tandem-*`). Older versions are not backported.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Use GitHub's private vulnerability reporting for this repository —
[Report a vulnerability](https://github.com/alirux/tandem/security/advisories/new) (Security tab
→ "Report a vulnerability"). This opens a private advisory visible only to the maintainer until
a fix is ready, and is the only channel monitored for security reports. Please include:

- A description of the vulnerability and its potential impact.
- Steps to reproduce, or a minimal proof-of-concept.
- The Tandem module(s) and version(s) affected.

You should get an acknowledgment within a few days. Since Tandem is a solo-maintained
open-source project, please allow reasonable time for a fix before any public disclosure —
coordinated disclosure is appreciated.

## Scope

Tandem is a library: the write-side outbox insert and the relay run inside applications you
control, against a database and Kafka cluster you operate. Securing that database, broker, and
network is the deploying application's responsibility. In scope for reports here: bugs in
Tandem's own code that could cause data corruption, event loss/duplication beyond the documented
at-least-once/ordering guarantees, SQL injection, or unsafe deserialization within the library
itself.
