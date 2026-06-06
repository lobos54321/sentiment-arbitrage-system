#!/usr/bin/env bash
set -e

mkdir -p /app/data /app/logs
export PORT="${PORT:-8080}"
export ZEABUR_LOG_TRIM_MAX_MB="${ZEABUR_LOG_TRIM_MAX_MB:-64}"
export ZEABUR_LOG_TRIM_KEEP_MB="${ZEABUR_LOG_TRIM_KEEP_MB:-16}"
export ZEABUR_MAINTENANCE_INTERVAL_SEC="${ZEABUR_MAINTENANCE_INTERVAL_SEC:-300}"

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
    "${SCOUT_PID:-}" \
    "${RESONANCE_PID:-}" \
    "${SOCIAL_PID:-}" 2>/dev/null || true
  wait || true
  exit 0
}

trap shutdown TERM INT

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
  python3 scripts/paper_db_retention.py 2>&1 | tee -a /app/data/paper-db-retention.log || true
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
  while true; do
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
    PYTHONUNBUFFERED=1 \
    python3 scripts/paper_trade_monitor.py 2>&1 | tee -a /app/data/paper-trader.log
    EXIT_CODE=${PIPESTATUS[0]}
    set -e
    echo "[paper-trader] $(date -u '+%Y-%m-%dT%H:%M:%SZ') exited (code $EXIT_CODE), running preflight then restarting in 15s" | tee -a /app/data/paper-trader.log
    run_marker_aware_preflight "paper_trader_exit"
    sleep 15
  done
) &
PAPER_PID=$!

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

echo "[STARTUP] PIDs redis=$REDIS_PID dashboard=$DASHBOARD_PID node=$NODE_PID maintenance=$MAINTENANCE_PID lifecycle=$LIFECYCLE_PID paper=$PAPER_PID scout=$SCOUT_PID resonance=$RESONANCE_PID social=$SOCIAL_PID"
sleep 3
kill -0 "$REDIS_PID" 2>/dev/null || echo "WARN: REDIS dead"
kill -0 "$DASHBOARD_PID" 2>/dev/null || echo "WARN: DASHBOARD dead"
kill -0 "$NODE_PID" 2>/dev/null || echo "WARN: NODE dead"
kill -0 "$MAINTENANCE_PID" 2>/dev/null || echo "WARN: MAINTENANCE dead"
kill -0 "$LIFECYCLE_PID" 2>/dev/null || echo "WARN: LIFECYCLE dead"
kill -0 "$PAPER_PID" 2>/dev/null || echo "WARN: PAPER dead"
kill -0 "$SCOUT_PID" 2>/dev/null || echo "WARN: GMGN_SCOUT dead"
kill -0 "$RESONANCE_PID" 2>/dev/null || echo "WARN: SOURCE_RESONANCE dead"
kill -0 "$SOCIAL_PID" 2>/dev/null || echo "WARN: SOCIAL dead"
wait
