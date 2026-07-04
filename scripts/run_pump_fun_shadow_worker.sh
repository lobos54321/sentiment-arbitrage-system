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
COMPARE_30D_EVERY_N="${PUMP_FUN_SHADOW_COMPARE_30D_EVERY_N:-12}"
SIGNAL_DB="${SENTIMENT_DB:-$DATA_DIR/sentiment_arb.db}"
RAW_DB="${RAW_SIGNAL_OUTCOMES_DB:-$DATA_DIR/raw_signal_outcomes.db}"
WORKER_STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
WORKER_PARENT_PID="${PUMP_FUN_SHADOW_SUPERVISOR_PID:-${PPID:-}}"
SUPERVISOR_KIND="${PUMP_FUN_SHADOW_SUPERVISOR_KIND:-unknown}"
DEPLOYMENT_COMMIT="${PUMP_FUN_SHADOW_DEPLOYMENT_COMMIT:-${ZEABUR_GIT_COMMIT_SHA:-${ZEABUR_GIT_COMMIT:-${GIT_COMMIT:-${SOURCE_VERSION:-}}}}}"
LOOP_COUNT=0

write_status() {
  local state="$1"
  local note="${2:-}"
  local next_run_at="${3:-}"
  python3 - "$STATUS_PATH" "$state" "$note" "$next_run_at" "$PUMP_DB" "$LOG_PATH" "$$" "$WORKER_PARENT_PID" "$SUPERVISOR_KIND" "$DEPLOYMENT_COMMIT" "$WORKER_STARTED_AT" "$DURATION_SEC" "$INTERVAL_SEC" "$WEBSOCKET_URL" "$SOURCE_URL" "$COMPARE_30D_EVERY_N" "$LOOP_COUNT" <<'PY'
import json
import os
import sys
import time

(
    path,
    state,
    note,
    next_run_at,
    pump_db,
    log_path,
    worker_pid,
    worker_parent_pid,
    supervisor_kind,
    deployment_commit,
    worker_started_at,
    duration_sec,
    interval_sec,
    websocket_url,
    source_url,
    compare_30d_every_n,
    loop_count,
) = sys.argv[1:18]
payload = {
    "schema_version": "pump_fun_shadow_worker_status.v1",
    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "pid": int(worker_pid),
    "parent_pid": int(worker_parent_pid) if str(worker_parent_pid or "").isdigit() else None,
    "supervisor_pid": int(worker_parent_pid) if str(worker_parent_pid or "").isdigit() else None,
    "supervisor_kind": supervisor_kind or None,
    "deployment_commit": deployment_commit or None,
    "worker_status_source": "supervised_bash_sidecar",
    "started_at": worker_started_at,
    "state": state,
    "note": note or None,
    "next_run_at": next_run_at or None,
    "pump_db": pump_db,
    "log_path": log_path,
    "duration_sec": int(duration_sec),
    "interval_sec": int(interval_sec),
    "compare_30d_every_n": int(compare_30d_every_n),
    "loop_count": int(loop_count),
    "stream_config": {
        "mode": "http_poll" if source_url else "websocket",
        "websocket_url_configured": bool(websocket_url),
        "source_url_configured": bool(source_url),
    },
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
  LOOP_COUNT=$((LOOP_COUNT + 1))
  STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "[pump-fun-shadow-worker] $STARTED_AT collection start loop=$LOOP_COUNT" | tee -a "$LOG_PATH"
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
    --out "$LATEST_DIR/pump_fun_shadow_source_comparison_24h.json" \
    --quiet 2>&1 | tee -a "$LOG_PATH"
  CMP24_EXIT=${PIPESTATUS[0]}
  CMP30_EXIT=0
  if [[ "$LOOP_COUNT" -eq 1 || $((LOOP_COUNT % COMPARE_30D_EVERY_N)) -eq 0 ]]; then
    python3 scripts/pump_fun_shadow_source_comparison.py \
      --pump-db "$PUMP_DB" \
      --signal-db "$SIGNAL_DB" \
      --raw-db "$RAW_DB" \
      --hours 720 \
      --out "$LATEST_DIR/pump_fun_shadow_source_comparison_30d.json" \
      --quiet 2>&1 | tee -a "$LOG_PATH"
    CMP30_EXIT=${PIPESTATUS[0]}
  else
    echo "[pump-fun-shadow-worker] $(date -u '+%Y-%m-%dT%H:%M:%SZ') skipping 30d comparison loop=$LOOP_COUNT cadence=$COMPARE_30D_EVERY_N" | tee -a "$LOG_PATH"
  fi
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
