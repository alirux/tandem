#!/usr/bin/env bash
# Runs the Tandem Spring sample under LEASE coordination (requires Docker), demonstrating the
# Admin API's relay-control endpoints that only mean anything under LEASE: GET /relay/buckets,
# GET /relay/workers, POST /relay/buckets/{bucket}/release. See run.sh for the default (SINGLE)
# demo, which covers the write-side tiers and the coordination-agnostic Admin API endpoints.
#
# Usage (from any directory):
#   tandem-sample-spring/run-lease.sh
#   ./run-lease.sh            (when inside tandem-sample-spring/)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT"
./gradlew :tandem-sample-spring:run --console=plain --args="--spring.profiles.active=lease"
