#!/usr/bin/env bash
set -u

DATA_DIR="${ZEABUR_DATA_DIR:-./data}"
mkdir -p "$DATA_DIR"

echo "[prestart] Running volume preflight cleanup..."
ZEABUR_DATA_DIR="$DATA_DIR" \
python3 scripts/run_with_timeout.py \
  --timeout-sec "${ZEABUR_PREFLIGHT_TIMEOUT_SEC:-45}" \
  --log "$DATA_DIR/preflight.log" \
  -- python3 scripts/zeabur_preflight_cleanup.py || true

if [ "${PAPER_DB_RETENTION_ENABLED:-true}" != "false" ]; then
  echo "[prestart] Running paper DB retention..."
  PAPER_DB="${PAPER_DB:-$DATA_DIR/paper_trades.db}" \
  PAPER_DB_RETENTION_MODE="${PAPER_DB_RETENTION_MODE:-apply}" \
  PAPER_DB_RETENTION_ARCHIVE_DIR="${PAPER_DB_RETENTION_ARCHIVE_DIR:-$DATA_DIR/archive/paper-db-retention}" \
  python3 scripts/run_with_timeout.py \
    --timeout-sec "${PAPER_DB_RETENTION_TIMEOUT_SEC:-90}" \
    --log "$DATA_DIR/paper-db-retention.log" \
    -- python3 scripts/paper_db_retention.py || true
else
  echo "[prestart] Paper DB retention disabled."
fi
