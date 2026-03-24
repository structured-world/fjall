#!/bin/bash
# Run all db_bench workloads and produce github-action-benchmark JSON.
# Usage: .github/scripts/run-benchmarks.sh [NUM_OPS]

set -euo pipefail

NUM=${1:-200000}

cargo run --release --manifest-path tools/db_bench/Cargo.toml -- \
  --benchmarks all --num "$NUM" --github_json \
  > benchmark-results.json

echo "Results written to benchmark-results.json" >&2
