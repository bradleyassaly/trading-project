#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

DATE_STAMP="$(date +%F)"
LOG_DIR="${REPO_ROOT}/artifacts/operating_baseline_daily/logs"
SUMMARY_DIR="${REPO_ROOT}/artifacts/operating_baseline_daily"
LOG_PATH="${LOG_DIR}/${DATE_STAMP}.log"

mkdir -p "${LOG_DIR}" "${SUMMARY_DIR}"

if [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
  PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
elif [[ -x "${REPO_ROOT}/.venv/Scripts/python.exe" ]]; then
  PYTHON_BIN="${REPO_ROOT}/.venv/Scripts/python.exe"
else
  PYTHON_BIN="python"
fi

if [[ -f "${REPO_ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.venv/bin/activate"
elif [[ -f "${REPO_ROOT}/.venv/Scripts/activate" ]]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.venv/Scripts/activate"
fi

"${PYTHON_BIN}" -m trading_platform.system.operating_baseline_daily \
  --config configs/orchestration_operating_baseline.yaml \
  --summary-dir artifacts/operating_baseline_daily \
  --alerts-config configs/alerts.yaml \
  --log-path "${LOG_PATH}" \
  "$@" 2>&1 | tee -a "${LOG_PATH}"
