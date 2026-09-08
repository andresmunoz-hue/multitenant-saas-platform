#!/usr/bin/env bash
# Smoke e2e: one tenant, short window, then re-run to assert idempotent gold row count.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TENANT="${SMOKE_TENANT:-sv}"
START="${SMOKE_START:-2025-03-01}"
END="${SMOKE_END:-2025-03-07}"
GOLD_DIR="data/gold/${TENANT}/daily_metrics_by_delivery_type"

echo "==> smoke build"
docker compose build pipeline

run_once() {
  local label="$1"
  echo "==> smoke run (${label}) tenant=${TENANT} ${START}..${END}"
  docker compose --profile smoke run --rm pipeline-smoke \
    python3 -m saas_pipeline \
      --env dev \
      --tenant "${TENANT}" \
      --start-date "${START}" \
      --end-date "${END}" \
      --layer all
}

count_gold_files() {
  if [[ ! -d "${GOLD_DIR}" ]]; then
    echo 0
    return
  fi
  find "${GOLD_DIR}" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' '
}

run_once "first"
first_count="$(count_gold_files)"
if [[ "${first_count}" -lt 1 ]]; then
  echo "FAIL: expected Gold parquet under ${GOLD_DIR}"
  exit 1
fi

run_once "second-idempotent"
second_count="$(count_gold_files)"

echo "gold parquet files first=${first_count} second=${second_count}"
if [[ "${second_count}" -lt 1 ]]; then
  echo "FAIL: Gold missing after second run"
  exit 1
fi

echo "OK smoke e2e + idempotent re-run for tenant=${TENANT}"
