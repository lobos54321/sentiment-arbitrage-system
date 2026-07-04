#!/usr/bin/env bash
set -euo pipefail

# Dedicated P8 shadow worker only. It ingests pump.fun launches into an isolated
# shadow DB and refreshes read-only comparison artifacts. It must not be used to
# write production signal, decision, paper, executor, gate, canary, or risk
# tables.

DATA_DIR="${ZEABUR_DATA_DIR:-${DATA_DIR:-/app/data}}"
RUNS_DIR="${AGENT_CAPTURE_RUNS_DIR:-$DATA_DIR/agent_runs}"
LATEST_DIR="$RUNS_DIR/latest"
mkdir -p "$DATA_DIR" "$LATEST_DIR"

LOG_PATH="${PUMP_FUN_SHADOW_WORKER_LOG:-$DATA_DIR/pump-fun-shadow-worker.log}"
STATUS_PATH="${PUMP_FUN_SHADOW_WORKER_STATUS:-$LATEST_DIR/pump_fun_shadow_worker_status.json}"
PUMP_DB="${PUMP_FUN_SHADOW_DB:-$DATA_DIR/pump_fun_shadow_signals.db}"
WEBSOCKET_URL="${PUMP_FUN_SHADOW_WEBSOCKET_URL:-wss://pumpportal.fun/api/data}"
SOURCE_URL="${PUMP_FUN_SHADOW_SOURCE_URL:-}"
DURATION_SEC="${PUMP_FUN_SHADOW_DURATION_SEC:-300}"
LIMIT="${PUMP_FUN_SHADOW_LIMIT:-2000}"
INTERVAL_SEC="${PUMP_FUN_SHADOW_INTERVAL_SEC:-5}"
SIGNAL_DB="${SENTIMENT_DB:-$DATA_DIR/sentiment_arb.db}"
RAW_DB="${RAW_SIGNAL_OUTCOMES_DB:-$DATA_DIR/raw_signal_outcomes.db}"

write_status() {
  local state="$1"
  local note="${2:-}"
  local next_run_at="${3:-}"
  python3 - "$STATUS_PATH" "$state" "$note" "$next_run_at" "$PUMP_DB" "$LOG_PATH" <<'PY'
import json
import os
import sys
import time

path, state, note, next_run_at, pump_db, log_path = sys.argv[1:7]
payload = {
    "schema_version": "pump_fun_shadow_worker_status.v1",
    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "pid": os.getpid(),
    "state": state,
    "note": note or None,
    "next_run_at": next_run_at or None,
    "pump_db": pump_db,
    "log_path": log_path,
    "promotion_allowed": False,
    "production_impact": "zero_shadow_only",
    "guardrails": {
        "writes_premium_signals": False,
        "writes_candidate_observations": False,
        "writes_paper_trades": False,
        "changes_entry_policy": False,
        "changes_gates": False,
        "changes_executor": False,
        "changes_risk": False,
    },
}
os.makedirs(os.path.dirname(path), exist_ok=True)
tmp = f"{path}.{int(time.time() * 1000)}.tmp"
with open(tmp, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
    fh.write("\n")
os.replace(tmp, path)
PY
}

stop_worker() {
  echo "[pump-fun-shadow-worker] $(date -u '+%Y-%m-%dT%H:%M:%SZ') stopping" | tee -a "$LOG_PATH"
  write_status "stopped" "received_stop_signal" ""
  exit 0
}

trap stop_worker TERM INT

echo "[pump-fun-shadow-worker] $(date -u '+%Y-%m-%dT%H:%M:%SZ') starting shadow worker" | tee -a "$LOG_PATH"
write_status "starting" "worker_boot" ""

while true; do
  STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "[pump-fun-shadow-worker] $STARTED_AT collection start" | tee -a "$LOG_PATH"
  write_status "collecting" "bounded_shadow_collection" ""

  set +e
  if [[ -n "$SOURCE_URL" ]]; then
    node scripts/pump_fun_shadow_observer.js \
      --source-url "$SOURCE_URL" \
      --duration-sec "$DURATION_SEC" \
      --limit "$LIMIT" \
      --out "$LATEST_DIR/pump_fun_shadow_observer_summary.json" \
      --db "$PUMP_DB" 2>&1 | tee -a "$LOG_PATH"
    OBS_EXIT=${PIPESTATUS[0]}
  else
    node scripts/pump_fun_shadow_observer.js \
      --websocket-url "$WEBSOCKET_URL" \
      --duration-sec "$DURATION_SEC" \
      --limit "$LIMIT" \
      --out "$LATEST_DIR/pump_fun_shadow_observer_summary.json" \
      --db "$PUMP_DB" 2>&1 | tee -a "$LOG_PATH"
    OBS_EXIT=${PIPESTATUS[0]}
  fi
  set -e

  if [[ "$OBS_EXIT" -ne 0 ]]; then
    echo "[pump-fun-shadow-worker] $(date -u '+%Y-%m-%dT%H:%M:%SZ') observer exit=$OBS_EXIT" | tee -a "$LOG_PATH"
    write_status "observer_error" "observer_exit_$OBS_EXIT" ""
  else
    write_status "comparing" "refreshing_source_comparison" ""
  fi

  set +e
  python3 scripts/pump_fun_shadow_source_comparison.py \
    --pump-db "$PUMP_DB" \
    --signal-db "$SIGNAL_DB" \
    --raw-db "$RAW_DB" \
    --hours 24 \
    --out "$LATEST_DIR/pump_fun_shadow_source_comparison_24h.json" 2>&1 | tee -a "$LOG_PATH"
  CMP24_EXIT=${PIPESTATUS[0]}
  python3 scripts/pump_fun_shadow_source_comparison.py \
    --pump-db "$PUMP_DB" \
    --signal-db "$SIGNAL_DB" \
    --raw-db "$RAW_DB" \
    --hours 720 \
    --out "$LATEST_DIR/pump_fun_shadow_source_comparison_30d.json" 2>&1 | tee -a "$LOG_PATH"
  CMP30_EXIT=${PIPESTATUS[0]}
  set -e

  NEXT_RUN_AT="$(python3 - "$INTERVAL_SEC" <<'PY'
import sys, time
print(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + int(sys.argv[1]))))
PY
)"
  if [[ "$OBS_EXIT" -eq 0 && "$CMP24_EXIT" -eq 0 && "$CMP30_EXIT" -eq 0 ]]; then
    write_status "sleeping" "last_run_ok" "$NEXT_RUN_AT"
  else
    write_status "sleeping_after_error" "observer=$OBS_EXIT comparison24=$CMP24_EXIT comparison30=$CMP30_EXIT" "$NEXT_RUN_AT"
  fi
  echo "[pump-fun-shadow-worker] $(date -u '+%Y-%m-%dT%H:%M:%SZ') sleeping ${INTERVAL_SEC}s" | tee -a "$LOG_PATH"
  sleep "$INTERVAL_SEC"
done

