#!/usr/bin/env bash
set -euo pipefail

# Collect test coverage for the orca project.
# Usage: scripts/coverage.sh [--ci]

CI_MODE=false
if [[ "${1:-}" == "--ci" ]]; then
  CI_MODE=true
fi

echo "Running tests with coverage..."
go test -race -coverprofile=coverage.txt -covermode=atomic -timeout 120s -json ./... | tee results.json

if command -v go-junit-report &>/dev/null && [[ "$CI_MODE" == "true" ]]; then
  go-junit-report -parser gojson -in results.json -out results.junit.xml
fi

if [[ -f coverage.txt ]]; then
  go tool cover -func=coverage.txt | tee coverage-summary.txt
  total=$(tail -1 coverage-summary.txt | awk '{print $NF}')
  echo ""
  echo "Total coverage: $total"
fi
