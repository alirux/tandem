#!/usr/bin/env bash
#
# Assembles the publishable site into site/_build.
#
# The HTML lives here; the images do not. Every picture the site shows is already in docs/ and is
# referenced from the README as well, so it is copied in at build time rather than duplicated into
# the repository twice — a second copy would drift the first time a diagram is redrawn.
#
# Usage:
#   ./site/build.sh                     # assemble into site/_build
#   python3 -m http.server -d site/_build 8000   # then open http://localhost:8000
#
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
src="$repo_root/site"
out="$src/_build"

rm -rf "$out"
mkdir -p "$out/assets"

# HTML, CSS and the domain marker — everything except this script and a previous build.
(cd "$src" && find . -type f \
    ! -name build.sh \
    ! -path './_build/*' \
    -exec rsync -R {} "$out/" \;)

# Images, single-sourced from docs/ and renamed to site-local names.
cp "$repo_root/docs/tandem-logo-blackbg-shade.png" "$out/assets/logo.png"
cp "$repo_root/docs/tandem-architecture.svg"       "$out/assets/architecture.svg"
cp "$repo_root/docs/tandem-cli-status-watch.png"   "$out/assets/cli-status-watch.png"
cp "$repo_root/docs/tandem-metrics-dashboard.png"  "$out/assets/metrics-dashboard.png"
cp "$repo_root/docs/tandem-social-preview.png"     "$out/assets/social-preview.png"

echo "Built $(find "$out" -type f | wc -l | tr -d ' ') files into $out"
