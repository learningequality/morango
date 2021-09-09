#!/usr/bin/env bash

set -euo pipefail

mkdir -p dist
make dist
buildkite-agent artifact upload 'dist/*.whl'
buildkite-agent artifact upload 'dist/*.tar.gz'
