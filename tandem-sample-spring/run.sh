#!/usr/bin/env bash
# Runs the Tandem Spring sample application (requires Docker).
#
# Usage (from any directory):
#   tandem-sample-spring/run.sh
#   ./run.sh            (when inside tandem-sample-spring/)
#
# The script navigates to the project root automatically so that Gradle and
# Testcontainers can locate the schema files regardless of where you call it from.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT"
./gradlew :tandem-sample-spring:run --console=plain
