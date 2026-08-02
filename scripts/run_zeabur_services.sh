#!/usr/bin/env bash
set -e

mkdir -p /app/data /app/logs
: "${RUNTIME_FINAL_EVIDENCE_LOG:=/app/data/runtime_final_evidence.jsonl}"
export RUNTIME_FINAL_EVIDENCE_LOG
echo "[STARTUP] Runtime final evidence log: $RUNTIME_FINAL_EVIDENCE_LOG"

export PORT="${PORT:-8080}"
export ZEABUR_LOG_TRIM_MAX_MB="${ZEABUR_LOG_TRIM_MAX_MB:-64}"
export ZEABUR_LOG_TRIM_KEEP_MB="${ZEABUR_LOG_TRIM_KEEP_MB:-16}"
export ZEABUR_MAINTENANCE_INTERVAL_SEC="${ZEABUR_MAINTENANCE_INTERVAL_SEC:-300}"
export PAPER_DB_RETENTION_INTERVAL_SEC="${PAPER_DB_RETENTION_INTERVAL_SEC:-3600}"
export RAW_PATH_OBSERVER_ENABLED="${RAW_PATH_OBSERVER_ENABLED:-true}"
export RAW_PATH_OBSERVER_INTERVAL_SEC="${RAW_PATH_OBSERVER_INTERVAL_SEC:-120}"
export RAW_PATH_OBSERVER_RUN_TIMEOUT_SEC="${RAW_PATH_OBSERVER_RUN_TIMEOUT_SEC:-900}"
export RAW_PATH_OBSERVER_MAX_SIGNALS_PER_RUN="${RAW_PATH_OBSERVER_MAX_SIGNALS_PER_RUN:-10}"
export RAW_PATH_OBSERVER_LOOKBACK_HOURS="${RAW_PATH_OBSERVER_LOOKBACK_HOURS:-72}"
export RAW_DOG_DISCOVERY_OBSERVER_ENABLED="${RAW_DOG_DISCOVERY_OBSERVER_ENABLED:-true}"
export RAW_DOG_DISCOVERY_OBSERVER_INTERVAL_SEC="${RAW_DOG_DISCOVERY_OBSERVER_INTERVAL_SEC:-300}"
export AGENT_CAPTURE_DISCOVERY_SCHEDULER_ENABLED="${AGENT_CAPTURE_DISCOVERY_SCHEDULER_ENABLED:-true}"
export AGENT_CAPTURE_DISCOVERY_SCHEDULER_INITIAL_DELAY_SEC="${AGENT_CAPTURE_DISCOVERY_SCHEDULER_INITIAL_DELAY_SEC:-300}"
export AGENT_CAPTURE_DISCOVERY_SCHEDULER_INTERVAL_SEC="${AGENT_CAPTURE_DISCOVERY_SCHEDULER_INTERVAL_SEC:-21600}"
export AGENT_CAPTURE_DISCOVERY_SCHEDULER_TIMEOUT_SEC="${AGENT_CAPTURE_DISCOVERY_SCHEDULER_TIMEOUT_SEC:-3600}"
export AGENT_CAPTURE_DISCOVERY_SCHEDULER_CAPTURE_HOURS="${AGENT_CAPTURE_DISCOVERY_SCHEDULER_CAPTURE_HOURS:-24,48,72}"
export AGENT_CAPTURE_MAX_SCAN_ROWS="${AGENT_CAPTURE_MAX_SCAN_ROWS:-250000}"
export AGENT_CAPTURE_RUN_HISTORY_LIMIT="${AGENT_CAPTURE_RUN_HISTORY_LIMIT:-8}"
export STRATEGY_MEMORY_ARTIFACT_DIR="${STRATEGY_MEMORY_ARTIFACT_DIR:-/app/docs/agents/strategy-memory-seed}"
export PUMP_FUN_SHADOW_WORKER_ENABLED="${PUMP_FUN_SHADOW_WORKER_ENABLED:-true}"
export PUMP_FUN_SHADOW_RETENTION_DAYS="${PUMP_FUN_SHADOW_RETENTION_DAYS:-30}"
case "$PAPER_DB_RETENTION_INTERVAL_SEC" in
  ''|*[!0-9]*) export PAPER_DB_RETENTION_INTERVAL_SEC=3600 ;;
esac

# A_CLASS is paper-only tiny canary by construction.  The Zeabur service had
# A_CLASS_ENABLED=false from the shadow phase, so use a separate force switch to
# move the production paper runtime into the safe 0.001-SOL canary phase.
# Set A_CLASS_SAFE_CANARY_FORCE=false to keep it shadow-only.
export A_CLASS_SAFE_CANARY_FORCE="${A_CLASS_SAFE_CANARY_FORCE:-true}"
if [ "${A_CLASS_SAFE_CANARY_FORCE}" != "false" ]; then
  export A_CLASS_ENABLED=true
else
  export A_CLASS_ENABLED="${A_CLASS_ENABLED:-false}"
fi
export A_CLASS_LIVE_MAX_SIZE_SOL="${A_CLASS_LIVE_MAX_SIZE_SOL:-0.001}"
export A_CLASS_LIVE_MAX_CONCURRENT="${A_CLASS_LIVE_MAX_CONCURRENT:-1}"
export A_CLASS_LIVE_DAILY_LOSS_BUDGET_SOL="${A_CLASS_LIVE_DAILY_LOSS_BUDGET_SOL:-0.005}"
export A_CLASS_LIVE_MAX_ENQUEUES_PER_SCAN="${A_CLASS_LIVE_MAX_ENQUEUES_PER_SCAN:-1}"
export FINAL_ENTRY_CONTRACT_ENFORCE="${FINAL_ENTRY_CONTRACT_ENFORCE:-true}"
# This script is the Zeabur process supervisor.  Do not let the Node runtime
# spawn the same paper DB sidecars again; duplicate supervisors can leave orphan
# workers after SIGBUS and keep touching a marked/corrupt paper DB.
export SOURCE_SHADOW_WORKERS_ENABLED="${SOURCE_SHADOW_WORKERS_ENABLED:-false}"
export PAPER_DB_WRITE_SIDECARS_ENABLED="${PAPER_DB_WRITE_SIDECARS_ENABLED:-false}"
export PAPER_FAST_LANE_ENABLED="${PAPER_FAST_LANE_ENABLED:-false}"

PAPER_DB_PATH="${PAPER_DB_PATH:-/app/data/paper_trades.db}"
PAPER_DB_INTEGRITY_MARKER="${PAPER_DB_PATH}.integrity_error"

run_marker_aware_preflight() {
  local reason="${1:-runtime}"
  if [ -f "$PAPER_DB_INTEGRITY_MARKER" ]; then
    echo "[preflight] $(date -u '+%Y-%m-%dT%H:%M:%SZ') paper DB integrity marker present after ${reason}; running quarantine preflight" | tee -a /app/data/preflight.log
    ZEABUR_PREFLIGHT_DB_CHECK_ENABLED=true \
    ZEABUR_PREFLIGHT_PAPER_DB_BACKUP_ENABLED=false \
      python3 scripts/zeabur_preflight_cleanup.py 2>&1 | tee -a /app/data/preflight.log || true
  else
    ZEABUR_PREFLIGHT_DB_CHECK_ENABLED=false \
    ZEABUR_PREFLIGHT_PAPER_DB_BACKUP_ENABLED=false \
      python3 scripts/zeabur_preflight_cleanup.py 2>&1 | tee -a /app/data/preflight.log || true
  fi
}

paper_db_marked() {
  [ -f "$PAPER_DB_INTEGRITY_MARKER" ]
}

shutdown() {
  echo "[SHUTDOWN] Forwarding termination signal..."
  kill -TERM \
    "${REDIS_PID:-}" \
    "${DASHBOARD_PID:-}" \
    "${NODE_PID:-}" \
    "${MAINTENANCE_PID:-}" \
    "${LIFECYCLE_PID:-}" \
    "${PAPER_PID:-}" \
    "${CANDIDATE_SHADOW_PID:-}" \
    "${EVALUATOR_SNAPSHOT_PID:-}" \
    "${AGENT_CAPTURE_PID:-}" \
    "${SCOUT_PID:-}" \
    "${RESONANCE_PID:-}" \
    "${PUMP_FUN_SHADOW_PID:-}" \
    "${SOCIAL_PID:-}" 2>/dev/null || true
  wait || true
  exit 0
}

trap shutdown TERM INT

# Optional sidecars may be disabled by environment. Keep their PID variables
# defined because this script runs with `set -u`.
CANDIDATE_SHADOW_PID=""
EVALUATOR_SNAPSHOT_PID=""
AGENT_CAPTURE_PID=""
SCOUT_PID=""
RESONANCE_PID=""
PUMP_FUN_SHADOW_PID=""

echo "[STARTUP] Checking gmgn-cli..."
if command -v gmgn-cli >/dev/null 2>&1; then
  echo "[STARTUP] gmgn-cli found: $(command -v gmgn-cli)"
else
  echo "[STARTUP] WARN: gmgn-cli missing; GMGN enrichment will degrade"
fi

echo "[STARTUP] Running volume preflight cleanup..."
python3 scripts/zeabur_preflight_cleanup.py 2>&1 | tee -a /app/data/preflight.log || true

if [ "${PAPER_DB_RETENTION_ENABLED:-true}" != "false" ]; then
  echo "[STARTUP] Running paper DB retention..."
  PAPER_DB=/app/data/paper_trades.db \
  PAPER_DB_RETENTION_MODE="${PAPER_DB_RETENTION_MODE:-apply}" \
  PAPER_DB_RETENTION_ARCHIVE_DIR="${PAPER_DB_RETENTION_ARCHIVE_DIR:-/app/data/archive/paper-db-retention}" \
  PAPER_DB_RETENTION_STATUS_PATH="${PAPER_DB_RETENTION_STATUS_PATH:-/app/data/paper-db-retention-status.json}" \
  PAPER_DB_RETENTION_HISTORY_PATH="${PAPER_DB_RETENTION_HISTORY_PATH:-/app/data/paper-db-retention-history.jsonl}" \
  PAPER_DB_RETENTION_MAX_SECONDS="${PAPER_DB_RETENTION_STARTUP_MAX_SECONDS:-20}" \
  PAPER_DB_RETENTION_MAX_ROWS_TOTAL="${PAPER_DB_RETENTION_STARTUP_MAX_ROWS_TOTAL:-50000}" \
    python3 scripts/run_with_timeout.py \
      --timeout-sec "${PAPER_DB_RETENTION_STARTUP_TIMEOUT_SEC:-30}" \
      --log /app/data/paper-db-retention.log \
      -- python3 scripts/paper_db_retention.py || true
else
  echo "[STARTUP] Paper DB retention disabled."
fi

echo "[STARTUP] Starting standalone dashboard/health on PORT=$PORT..."
(
  while true; do
    echo "[dashboard] $(date -u '+%Y-%m-%dT%H:%M:%SZ') starting PORT=$PORT" | tee -a /app/data/dashboard.log
    set +e
    PORT="$PORT" \
    DASHBOARD_RUNTIME_ROLE=standalone_dashboard \
    DASHBOARD_RUNTIME_LOG_DIR=/app/data \
    DB_PATH=/app/data/sentiment_arb.db \
    SENTIMENT_DB=/app/data/sentiment_arb.db \
    PAPER_TRADES_DB=/app/data/paper_trades.db \
    LIFECYCLE_DB=/app/data/lifecycle_tracks.db \
    KLINE_DB=/app/data/kline_cache.db \
    PAPER_EVIDENCE_LOG_DIR=/app/data/paper_evidence_log \
    V27_EVENT_LOG_DIR=/app/data/v27_event_log \
    V27_READ_MODEL_DIR=/app/data/v27_read_models \
    V27_MODE_READINESS_PATH=/app/data/v27_read_models/mode_readiness.json \
    PYTHONUNBUFFERED=1 \
    node src/web/dashboard-server.js 2>&1 | tee -a /app/data/dashboard.log
    EXIT_CODE=${PIPESTATUS[0]}
    set -e
    echo "[dashboard] $(date -u '+%Y-%m-%dT%H:%M:%SZ') exited (code $EXIT_CODE), restarting in 5s" | tee -a /app/data/dashboard.log
    sleep 5
  done
) &
DASHBOARD_PID=$!

echo "[STARTUP] Starting runtime volume/log maintenance..."
(
  LAST_RETENTION_TS="$(date +%s)"
  while true; do
    if [ "${PAPER_DB_SNAPSHOT_REQUEST_WORKER_ENABLED:-true}" != "false" ]; then
      python3 scripts/paper_db_snapshot_request_worker.py \
        --source /app/data/paper_trades.db \
        --request /app/data/recovery/paper_db_snapshot_request.json \
        --status /app/data/recovery/paper_db_snapshot_status.json \
        --recovery-dir /app/data/recovery \
        --archive-dir /app/data/recovery/paper_db_snapshot_requests \
        --local-verify-dir "${PAPER_DB_SNAPSHOT_LOCAL_VERIFY_DIR:-/tmp/paper-db-snapshot-verify}" \
        >> /app/data/paper-db-snapshot-worker.log 2>&1 || true
    fi
    sleep "$ZEABUR_MAINTENANCE_INTERVAL_SEC"
    echo "[maintenance] $(date -u '+%Y-%m-%dT%H:%M:%SZ') running log trim" | tee -a /app/data/maintenance.log
    if [ -f "$PAPER_DB_INTEGRITY_MARKER" ]; then
      echo "[maintenance] paper DB integrity marker present; running quarantine preflight" | tee -a /app/data/maintenance.log
      ZEABUR_PREFLIGHT_DB_CHECK_ENABLED=true \
      ZEABUR_PREFLIGHT_PAPER_DB_BACKUP_ENABLED=false \
        python3 scripts/zeabur_preflight_cleanup.py 2>&1 | tee -a /app/data/maintenance.log || true
    else
      ZEABUR_PREFLIGHT_DB_CHECK_ENABLED="${ZEABUR_RUNTIME_DB_CHECK_ENABLED:-false}" \
      ZEABUR_PREFLIGHT_PAPER_DB_BACKUP_ENABLED=false \
        python3 scripts/zeabur_preflight_cleanup.py 2>&1 | tee -a /app/data/maintenance.log || true
    fi
    NOW_TS="$(date +%s)"
    if [ "${PAPER_DB_RETENTION_ENABLED:-true}" != "false" ] \
      && [ ! -f "$PAPER_DB_INTEGRITY_MARKER" ] \
      && [ $((NOW_TS - LAST_RETENTION_TS)) -ge "$PAPER_DB_RETENTION_INTERVAL_SEC" ]; then
      echo "[maintenance] $(date -u '+%Y-%m-%dT%H:%M:%SZ') running bounded paper DB retention" | tee -a /app/data/maintenance.log
      PAPER_DB=/app/data/paper_trades.db \
      PAPER_DB_RETENTION_MODE="${PAPER_DB_RETENTION_MODE:-apply}" \
      PAPER_DB_RETENTION_ARCHIVE_DIR="${PAPER_DB_RETENTION_ARCHIVE_DIR:-/app/data/archive/paper-db-retention}" \
      PAPER_DB_RETENTION_STATUS_PATH="${PAPER_DB_RETENTION_STATUS_PATH:-/app/data/paper-db-retention-status.json}" \
      PAPER_DB_RETENTION_HISTORY_PATH="${PAPER_DB_RETENTION_HISTORY_PATH:-/app/data/paper-db-retention-history.jsonl}" \
      PYTHONUNBUFFERED=1 \
        python3 scripts/run_with_timeout.py \
          --timeout-sec "${PAPER_DB_RETENTION_TIMEOUT_SEC:-120}" \
          --log /app/data/paper-db-retention.log \
          -- python3 scripts/paper_db_retention.py || true
      LAST_RETENTION_TS="$NOW_TS"
    fi
  done
) &
MAINTENANCE_PID=$!

echo "[STARTUP] Starting redis-server..."
redis-server --bind 127.0.0.1 --port 6379 --save '' --appendonly no \
  --dir /app/data --logfile /app/logs/redis.log --daemonize no &
REDIS_PID=$!

echo "[STARTUP] Waiting for Redis..."
REDIS_READY=0
for _ in $(seq 1 30); do
  if redis-cli -h 127.0.0.1 -p 6379 ping 2>/dev/null | grep -q PONG; then
    REDIS_READY=1
    echo "[STARTUP] Redis ready."
    break
  fi
  sleep 0.5
done
if [ "$REDIS_READY" -ne 1 ]; then
  echo "[STARTUP] Redis failed to become ready in time."
  exit 1
fi

echo "[STARTUP] Starting Node.js..."
(
  while true; do
    echo "[node] $(date -u '+%Y-%m-%dT%H:%M:%SZ') starting premium worker (embedded dashboard disabled)" | tee -a /app/data/node.log
    set +e
    SENTIMENT_DB=/app/data/sentiment_arb.db \
    LIFECYCLE_DB=/app/data/lifecycle_tracks.db \
    KLINE_DB=/app/data/kline_cache.db \
    PAPER_EVIDENCE_LOG_DIR=/app/data/paper_evidence_log \
    V27_EVENT_LOG_DIR=/app/data/v27_event_log \
    V27_READ_MODEL_DIR=/app/data/v27_read_models \
    V27_MODE_READINESS_PATH=/app/data/v27_read_models/mode_readiness.json \
    V27_RUNTIME_MODE_GATE_ENABLED="${V27_RUNTIME_MODE_GATE_ENABLED:-true}" \
    V27_READ_MODEL_REFRESH_WORKER_ENABLED="${V27_READ_MODEL_REFRESH_WORKER_ENABLED:-true}" \
    NODE_STARTUP_PREFLIGHT_ENABLED=false \
    DASHBOARD_RUNTIME_LOG_DIR=/app/data \
    EMBEDDED_DASHBOARD_ENABLED=false \
    PAPER_DB_RETENTION_ENABLED=false \
    SHADOW_MODE=false \
    AUTO_BUY_ENABLED=true \
    PYTHONUNBUFFERED=1 \
    node --import ./src/runtime/v27-paper-mode-preload.js src/index.js --premium 2>&1 | tee -a /app/data/node.log
    EXIT_CODE=${PIPESTATUS[0]}
    set -e
    echo "[node] $(date -u '+%Y-%m-%dT%H:%M:%SZ') exited (code $EXIT_CODE), running preflight then restarting in 15s" | tee -a /app/data/node.log
    run_marker_aware_preflight "node_exit"
    sleep 15
  done
) &
NODE_PID=$!

echo "[STARTUP] Starting lifecycle-tracker..."
(
  while true; do
    SENTIMENT_DB=/app/data/sentiment_arb.db \
    LIFECYCLE_DB=/app/data/lifecycle_tracks.db \
    KLINE_DB=/app/data/kline_cache.db \
    PYTHONUNBUFFERED=1 \
    python3 scripts/lifecycle_24h_tracker.py --track 2>&1 | tee -a /app/data/lifecycle.log
    echo "[lifecycle-tracker] restarting in 15s"
    sleep 15
  done
) &
LIFECYCLE_PID=$!

echo "[STARTUP] Starting paper-trader (with auto-restart)..."
(
  while true; do
    echo "[paper-trader] $(date -u '+%Y-%m-%dT%H:%M:%SZ') starting" | tee -a /app/data/paper-trader.log
    set +e
    if paper_db_marked; then
      echo "[paper-trader] $(date -u '+%Y-%m-%dT%H:%M:%SZ') paper DB integrity marker present before start; running quarantine preflight" | tee -a /app/data/paper-trader.log
      run_marker_aware_preflight "paper_start_guard"
    fi
    PAPER_DB=/app/data/paper_trades.db \
    KLINE_DB=/app/data/kline_cache.db \
    SENTIMENT_DB=/app/data/sentiment_arb.db \
    PAPER_EVIDENCE_LOG_DIR=/app/data/paper_evidence_log \
    V27_READ_MODEL_DIR=/app/data/v27_read_models \
    V27_MODE_READINESS_PATH=/app/data/v27_read_models/mode_readiness.json \
    V27_RUNTIME_MODE_GATE_ENABLED="${V27_RUNTIME_MODE_GATE_ENABLED:-true}" \
    V27_PAPER_MONITOR_RUNTIME_MODE_GATE_MIN_MODE="${V27_PAPER_MONITOR_RUNTIME_MODE_GATE_MIN_MODE:-ultra_tiny}" \
    RUNTIME_FINAL_EVIDENCE_LOG="${RUNTIME_FINAL_EVIDENCE_LOG:-/app/data/runtime_final_evidence.jsonl}" \
    PYTHONUNBUFFERED=1 \
    python3 scripts/paper_trade_monitor.py >> /app/data/paper-trader.log 2>&1
    EXIT_CODE=$?
    set -e
    echo "[paper-trader] $(date -u '+%Y-%m-%dT%H:%M:%SZ') exited (code $EXIT_CODE), running preflight then restarting in 15s" | tee -a /app/data/paper-trader.log
    run_marker_aware_preflight "paper_trader_exit"
    sleep 15
  done
) &
PAPER_PID=$!

if [ "${CANDIDATE_SHADOW_OBSERVER_ENABLED:-false}" = "true" ]; then
  echo "[STARTUP] Starting candidate-shadow-observer..."
  (
    while true; do
      echo "[candidate-shadow-observer] $(date -u '+%Y-%m-%dT%H:%M:%SZ') starting" | tee -a /app/data/candidate-shadow-observer.log
      if paper_db_marked; then
        echo "[candidate-shadow-observer] paper DB integrity marker present; idling until quarantine preflight clears it" | tee -a /app/data/candidate-shadow-observer.log
        run_marker_aware_preflight "candidate_shadow_start_guard"
        sleep 15
        continue
      fi
      PAPER_DB=/app/data/paper_trades.db \
      SENTIMENT_DB=/app/data/sentiment_arb.db \
      KLINE_DB=/app/data/kline_cache.db \
      PYTHONUNBUFFERED=1 \
      python3 scripts/candidate_shadow_observer.py \
        --loop \
        --interval "${CANDIDATE_SHADOW_OBSERVER_INTERVAL_SEC:-60}" \
        --limit "${CANDIDATE_SHADOW_OBSERVER_LIMIT:-10}" \
        --kline-limit "${CANDIDATE_SHADOW_KLINE_LIMIT:-125}" \
        --kline-fallback-max-fetches "${CANDIDATE_SHADOW_KLINE_FALLBACK_MAX_FETCHES:-2}" \
        --kline-fallback-cooldown-sec "${CANDIDATE_SHADOW_KLINE_FALLBACK_COOLDOWN_SEC:-900}" 2>&1 | tee -a /app/data/candidate-shadow-observer.log
      echo "[candidate-shadow-observer] $(date -u '+%Y-%m-%dT%H:%M:%SZ') exited, restarting in 15s" | tee -a /app/data/candidate-shadow-observer.log
      sleep 15
    done
  ) &
  CANDIDATE_SHADOW_PID=$!
else
  echo "[STARTUP] Candidate shadow observer disabled."
fi

if [ "${EVALUATOR_SNAPSHOT_WORKER_ENABLED:-true}" = "true" ]; then
  echo "[STARTUP] Starting bounded cross-DB evaluator snapshot worker..."
  PYTHONUNBUFFERED=1 python3 scripts/cross_db_evaluator_snapshot.py \
    --signal-db /app/data/sentiment_arb.db \
    --paper-db /app/data/paper_trades.db \
    --raw-db /app/data/raw_signal_outcomes.db \
    --kline-db /app/data/kline_cache.db \
    --out-root "${EVALUATOR_SNAPSHOT_OUT_ROOT:-/app/data/agent_evidence}" \
    --max-skew-sec "${EVALUATOR_SNAPSHOT_MAX_SKEW_SEC:-30}" \
    --min-free-after-gib "${EVALUATOR_SNAPSHOT_MIN_FREE_AFTER_GIB:-5}" \
    --keep-previous 0 \
    --max-runs 0 \
    --interval-sec "${EVALUATOR_SNAPSHOT_INTERVAL_SEC:-21600}" \
    --initial-delay-sec "${EVALUATOR_SNAPSHOT_INITIAL_DELAY_SEC:-30}" \
    --status-out "${EVALUATOR_SNAPSHOT_STATUS:-/app/data/agent_evidence/snapshot_status.json}" \
    --lock-file "${EVALUATOR_SNAPSHOT_LOCK_FILE:-/tmp/cross-db-evaluator-snapshot.lock}" \
    >> /app/data/cross-db-evaluator-snapshot.log 2>&1 &
  EVALUATOR_SNAPSHOT_PID=$!
else
  echo "[STARTUP] Cross-DB evaluator snapshot worker disabled."
fi

if [ "${AGENT_CAPTURE_DISCOVERY_FORCE_ENABLE:-false}" = "true" ]; then
  AGENT_CAPTURE_EVIDENCE_ROOT="${AGENT_CAPTURE_EVIDENCE_ROOT:-/app/data/agent_evidence/current}"
  AGENT_CAPTURE_EVIDENCE_DB="${AGENT_CAPTURE_EVIDENCE_DB:-$AGENT_CAPTURE_EVIDENCE_ROOT/paper_evidence.db}"
  AGENT_CAPTURE_EVIDENCE_SIGNAL_DB="${AGENT_CAPTURE_EVIDENCE_SIGNAL_DB:-$AGENT_CAPTURE_EVIDENCE_ROOT/signal.db}"
  AGENT_CAPTURE_EVIDENCE_RAW_DB="${AGENT_CAPTURE_EVIDENCE_RAW_DB:-$AGENT_CAPTURE_EVIDENCE_ROOT/raw.db}"
  AGENT_CAPTURE_EVIDENCE_KLINE_DB="${AGENT_CAPTURE_EVIDENCE_KLINE_DB:-$AGENT_CAPTURE_EVIDENCE_ROOT/kline.db}"
  AGENT_CAPTURE_EVIDENCE_MANIFEST="${AGENT_CAPTURE_EVIDENCE_MANIFEST:-$AGENT_CAPTURE_EVIDENCE_ROOT/manifest.json}"
  echo "[STARTUP] Starting gold/silver capture discovery agent loop..."
  (
    while true; do
      echo "[agent-capture-discovery] $(date -u '+%Y-%m-%dT%H:%M:%SZ') starting discovery run" | tee -a /app/data/agent-capture-discovery.log
      set +e
      if paper_db_marked; then
        echo "[agent-capture-discovery] paper DB integrity marker present; running quarantine preflight before discovery report" | tee -a /app/data/agent-capture-discovery.log
        run_marker_aware_preflight "agent_capture_discovery_start_guard"
      fi
      PAPER_DB="$AGENT_CAPTURE_EVIDENCE_DB" \
      AGENT_CAPTURE_EVIDENCE_DB="$AGENT_CAPTURE_EVIDENCE_DB" \
      RAW_SIGNAL_OUTCOMES_DB="$AGENT_CAPTURE_EVIDENCE_RAW_DB" \
      SENTIMENT_DB="$AGENT_CAPTURE_EVIDENCE_SIGNAL_DB" \
      KLINE_DB="$AGENT_CAPTURE_EVIDENCE_KLINE_DB" \
      PYTHONUNBUFFERED=1 \
      python3 scripts/agent_capture_discovery_loop.py \
        --signal-db "$AGENT_CAPTURE_EVIDENCE_SIGNAL_DB" \
        --paper-db "$AGENT_CAPTURE_EVIDENCE_DB" \
        --raw-db "$AGENT_CAPTURE_EVIDENCE_RAW_DB" \
        --kline-db "$AGENT_CAPTURE_EVIDENCE_KLINE_DB" \
        --evidence-manifest "$AGENT_CAPTURE_EVIDENCE_MANIFEST" \
        --evidence-max-age-sec "${EVALUATOR_SNAPSHOT_MAX_AGE_SEC:-28800}" \
        --evidence-lock-file "${EVALUATOR_SNAPSHOT_LOCK_FILE:-/tmp/cross-db-evaluator-snapshot.lock}" \
        --evidence-lock-timeout-sec "${EVALUATOR_SNAPSHOT_LOCK_TIMEOUT_SEC:-300}" \
        --hours "${AGENT_CAPTURE_DISCOVERY_HOURS:-24}" \
        --expected-candidates "${AGENT_CAPTURE_EXPECTED_CANDIDATES:-84}" \
        --out-root "${AGENT_CAPTURE_RUNS_DIR:-/app/data/agent_runs}" \
        --handoff-dir "${AGENT_CAPTURE_HANDOFFS_DIR:-/app/data/agent_handoffs}" \
        --registry "${AGENT_CAPTURE_HYPOTHESIS_REGISTRY:-/app/data/hypothesis_registry.json}" \
        --markov-profiles "${AGENT_CAPTURE_MARKOV_PROFILES:-runtime,kline}" \
        --report-timeout-sec "${AGENT_CAPTURE_REPORT_TIMEOUT_SEC:-30}" \
        --test-timeout-sec "${AGENT_CAPTURE_TEST_TIMEOUT_SEC:-180}" \
        --max-scan-rows "${AGENT_CAPTURE_MAX_SCAN_ROWS:-250000}" \
        --max-runs 1 \
        --interval-sec 1 2>&1 | tee -a /app/data/agent-capture-discovery.log
      EXIT_CODE=${PIPESTATUS[0]}
      set -e
      echo "[agent-capture-discovery] $(date -u '+%Y-%m-%dT%H:%M:%SZ') run exited (code $EXIT_CODE); next run in ${AGENT_CAPTURE_DISCOVERY_INTERVAL_SEC:-900}s" | tee -a /app/data/agent-capture-discovery.log
      sleep "${AGENT_CAPTURE_DISCOVERY_INTERVAL_SEC:-900}"
    done
  ) &
  AGENT_CAPTURE_PID=$!
else
  echo "[STARTUP] Gold/silver capture discovery agent loop disabled; set AGENT_CAPTURE_DISCOVERY_FORCE_ENABLE=true only for a dedicated worker/container."
fi

if [ "$PUMP_FUN_SHADOW_WORKER_ENABLED" = "true" ]; then
  echo "[STARTUP] Starting isolated pump.fun shadow worker..."
  PUMP_FUN_SHADOW_SUPERVISOR_PID="$$" \
  PUMP_FUN_SHADOW_SUPERVISOR_KIND="zeabur_run_script" \
    bash scripts/run_pump_fun_shadow_worker.sh &
  PUMP_FUN_SHADOW_PID=$!
else
  echo "[STARTUP] Pump.fun shadow worker disabled."
fi

if [ "$PAPER_DB_WRITE_SIDECARS_ENABLED" = "true" ] && [ "$SOURCE_SHADOW_WORKERS_ENABLED" = "true" ]; then
  echo "[STARTUP] Starting GMGN external-alpha scout..."
  (
    while true; do
      echo "[gmgn-scout] $(date -u '+%Y-%m-%dT%H:%M:%SZ') starting" | tee -a /app/data/gmgn-scout.log
      if paper_db_marked; then
        echo "[gmgn-scout] paper DB integrity marker present; idling until quarantine preflight clears it" | tee -a /app/data/gmgn-scout.log
        run_marker_aware_preflight "gmgn_scout_start_guard"
        sleep 15
        continue
      fi
      PAPER_DB=/app/data/paper_trades.db \
      EXTERNAL_ALPHA_DB=/app/data/paper_trades.db \
      PYTHONUNBUFFERED=1 \
      python3 scripts/gmgn_candidate_scout.py \
        --loop \
        --interval "${GMGN_SCOUT_INTERVAL_SEC:-60}" \
        --limit "${GMGN_SCOUT_LIMIT:-50}" \
        --state-db /app/data/paper_trades.db \
        --out /app/data/gmgn_candidates.jsonl 2>&1 | tee -a /app/data/gmgn-scout.log
      echo "[gmgn-scout] $(date -u '+%Y-%m-%dT%H:%M:%SZ') exited, restarting in 15s" | tee -a /app/data/gmgn-scout.log
      sleep 15
    done
  ) &
  SCOUT_PID=$!

  echo "[STARTUP] Starting source-resonance shadow..."
  (
    while true; do
      echo "[source-resonance] $(date -u '+%Y-%m-%dT%H:%M:%SZ') starting" | tee -a /app/data/source-resonance.log
      if paper_db_marked; then
        echo "[source-resonance] paper DB integrity marker present; idling until quarantine preflight clears it" | tee -a /app/data/source-resonance.log
        run_marker_aware_preflight "source_resonance_start_guard"
        sleep 15
        continue
      fi
      PAPER_DB=/app/data/paper_trades.db \
      SENTIMENT_DB=/app/data/sentiment_arb.db \
      PYTHONUNBUFFERED=1 \
      python3 scripts/source_resonance_shadow.py \
        --loop \
        --interval "${SOURCE_RESONANCE_INTERVAL_SEC:-60}" \
        --lookback-hours "${SOURCE_RESONANCE_LOOKBACK_HOURS:-24}" \
        --limit "${SOURCE_RESONANCE_LIMIT:-500}" \
        --paper-db /app/data/paper_trades.db \
        --signal-db /app/data/sentiment_arb.db 2>&1 | tee -a /app/data/source-resonance.log
      echo "[source-resonance] $(date -u '+%Y-%m-%dT%H:%M:%SZ') exited, restarting in 15s" | tee -a /app/data/source-resonance.log
      sleep 15
    done
  ) &
  RESONANCE_PID=$!
else
  echo "[STARTUP] Paper DB write sidecars disabled (PAPER_DB_WRITE_SIDECARS_ENABLED=$PAPER_DB_WRITE_SIDECARS_ENABLED SOURCE_SHADOW_WORKERS_ENABLED=$SOURCE_SHADOW_WORKERS_ENABLED); skipping GMGN scout and source-resonance shadow."
fi

echo "[STARTUP] Starting social-signal-service..."
(
  while true; do
    SOCIAL_SERVICE_PORT=8765 \
    PYTHONUNBUFFERED=1 \
    python3 scripts/social_signal_service.py 2>&1 | tee -a /app/data/social-service.log
    echo "[social-service] $(date -u '+%Y-%m-%dT%H:%M:%SZ') restarting in 10s" | tee -a /app/data/social-service.log
    sleep 10
  done
) &
SOCIAL_PID=$!

echo "[STARTUP] PIDs redis=$REDIS_PID dashboard=$DASHBOARD_PID node=$NODE_PID maintenance=$MAINTENANCE_PID lifecycle=$LIFECYCLE_PID paper=$PAPER_PID candidate_shadow=${CANDIDATE_SHADOW_PID:-disabled} evaluator_snapshot=${EVALUATOR_SNAPSHOT_PID:-disabled} agent_capture=${AGENT_CAPTURE_PID:-disabled} scout=${SCOUT_PID:-disabled} resonance=${RESONANCE_PID:-disabled} social=$SOCIAL_PID"
sleep 3
kill -0 "$REDIS_PID" 2>/dev/null || echo "WARN: REDIS dead"
kill -0 "$DASHBOARD_PID" 2>/dev/null || echo "WARN: DASHBOARD dead"
kill -0 "$NODE_PID" 2>/dev/null || echo "WARN: NODE dead"
kill -0 "$MAINTENANCE_PID" 2>/dev/null || echo "WARN: MAINTENANCE dead"
kill -0 "$LIFECYCLE_PID" 2>/dev/null || echo "WARN: LIFECYCLE dead"
kill -0 "$PAPER_PID" 2>/dev/null || echo "WARN: PAPER dead"
if [ -n "${CANDIDATE_SHADOW_PID:-}" ]; then
  kill -0 "$CANDIDATE_SHADOW_PID" 2>/dev/null || echo "WARN: CANDIDATE_SHADOW dead"
fi
if [ -n "${AGENT_CAPTURE_PID:-}" ]; then
  kill -0 "$AGENT_CAPTURE_PID" 2>/dev/null || echo "WARN: AGENT_CAPTURE dead"
fi
if [ -n "${EVALUATOR_SNAPSHOT_PID:-}" ]; then
  kill -0 "$EVALUATOR_SNAPSHOT_PID" 2>/dev/null || echo "WARN: EVALUATOR_SNAPSHOT dead"
fi
if [ -n "${SCOUT_PID:-}" ]; then
  kill -0 "$SCOUT_PID" 2>/dev/null || echo "WARN: GMGN_SCOUT dead"
fi
if [ -n "${RESONANCE_PID:-}" ]; then
  kill -0 "$RESONANCE_PID" 2>/dev/null || echo "WARN: SOURCE_RESONANCE dead"
fi
kill -0 "$SOCIAL_PID" 2>/dev/null || echo "WARN: SOCIAL dead"
wait
