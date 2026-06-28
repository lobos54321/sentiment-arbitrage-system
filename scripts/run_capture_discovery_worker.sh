#!/usr/bin/env bash
set -euo pipefail

# Dedicated discovery worker only. Do not run this in the dashboard/API
# container unless it has separate CPU/memory budget.

DATA_DIR="${ZEABUR_DATA_DIR:-${DATA_DIR:-/app/data}}"
mkdir -p "$DATA_DIR"

INTERVAL_SEC="${AGENT_CAPTURE_DISCOVERY_INTERVAL_SEC:-900}"
HOURS="${AGENT_CAPTURE_DISCOVERY_HOURS:-24}"
EXPECTED_CANDIDATES="${AGENT_CAPTURE_EXPECTED_CANDIDATES:-84}"
REPORT_TIMEOUT_SEC="${AGENT_CAPTURE_REPORT_TIMEOUT_SEC:-300}"
TEST_TIMEOUT_SEC="${AGENT_CAPTURE_TEST_TIMEOUT_SEC:-180}"
MAX_SCAN_ROWS="${AGENT_CAPTURE_MAX_SCAN_ROWS:-250000}"
LOG_PATH="${AGENT_CAPTURE_DISCOVERY_LOG:-$DATA_DIR/agent-capture-discovery.log}"

echo "[agent-capture-worker] $(date -u '+%Y-%m-%dT%H:%M:%SZ') starting dedicated worker" | tee -a "$LOG_PATH"

while true; do
  echo "[agent-capture-worker] $(date -u '+%Y-%m-%dT%H:%M:%SZ') starting bounded run" | tee -a "$LOG_PATH"
  set +e
  PAPER_DB="${PAPER_DB:-$DATA_DIR/paper_trades.db}" \
  RAW_SIGNAL_OUTCOMES_DB="${RAW_SIGNAL_OUTCOMES_DB:-$DATA_DIR/raw_signal_outcomes.db}" \
  SENTIMENT_DB="${SENTIMENT_DB:-$DATA_DIR/sentiment_arb.db}" \
  KLINE_DB="${KLINE_DB:-$DATA_DIR/kline_cache.db}" \
  PYTHONUNBUFFERED=1 \
  python3 scripts/agent_capture_discovery_loop.py \
    --paper-db "${PAPER_DB:-$DATA_DIR/paper_trades.db}" \
    --raw-db "${RAW_SIGNAL_OUTCOMES_DB:-$DATA_DIR/raw_signal_outcomes.db}" \
    --hours "$HOURS" \
    --expected-candidates "$EXPECTED_CANDIDATES" \
    --out-root "${AGENT_CAPTURE_RUNS_DIR:-$DATA_DIR/agent_runs}" \
    --handoff-dir "${AGENT_CAPTURE_HANDOFFS_DIR:-$DATA_DIR/agent_handoffs}" \
    --registry "${AGENT_CAPTURE_HYPOTHESIS_REGISTRY:-$DATA_DIR/hypothesis_registry.json}" \
    --markov-profiles "${AGENT_CAPTURE_MARKOV_PROFILES:-runtime,kline}" \
    --report-timeout-sec "$REPORT_TIMEOUT_SEC" \
    --test-timeout-sec "$TEST_TIMEOUT_SEC" \
    --max-scan-rows "$MAX_SCAN_ROWS" \
    --max-runs 1 \
    --interval-sec 1 2>&1 | tee -a "$LOG_PATH"
  EXIT_CODE=${PIPESTATUS[0]}
  set -e
  echo "[agent-capture-worker] $(date -u '+%Y-%m-%dT%H:%M:%SZ') run exited code=$EXIT_CODE; sleeping ${INTERVAL_SEC}s" | tee -a "$LOG_PATH"
  sleep "$INTERVAL_SEC"
done
