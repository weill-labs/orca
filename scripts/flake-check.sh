#!/usr/bin/env bash
set -euo pipefail

registry_path="${CI_FLAKE_REGISTRY:-.github/flake-registry.toml}"
results_path="${CI_FLAKE_RESULTS:-flake-results.json}"
report_path="${CI_FLAKE_REPORT:-flake-report.json}"
summary_path="${CI_FLAKE_SUMMARY:-flake-summary.md}"

status=0
go test ./... -count=3 -timeout 120s -json >"${results_path}" || status=$?

go run ./cmd/ci-flakes analyze \
  --registry "${registry_path}" \
  --input "${results_path}" \
  --output "${report_path}" \
  --summary "${summary_path}"

cat "${summary_path}"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  cat "${summary_path}" >>"${GITHUB_STEP_SUMMARY}"
fi

exit "${status}"
