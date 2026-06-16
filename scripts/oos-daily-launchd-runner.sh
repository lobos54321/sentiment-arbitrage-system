#!/bin/bash
#
# oos-daily-launchd-runner.sh — launchd wrapper for the signed-off OOS daily runner
# (scripts/run-oos-daily-cycle.js). Runs ONE real cycle/day headless and self-halts via
# STICKY markers at n=50 (runner exit 10) or fail-closed (runner exit 2/other).
#
# GUARANTEES (never relaxed here):
#   - never reads/computes AUC; never passes --look-point / --reveal-sealed-auc
#   - touches no strategy/main/production write path
#   - secrets (dashboard token, Dune key) are read from FILES only — never printed
#
# Modes:
#   (no arg)   run one real cycle; map exit code -> markers
#   --check    side-effect-free smoke (runs the runner's --check); writes NO markers
#
# Test seam: OOS_RUNNER_CMD overrides the runner invocation (tests/oos-daily-launchd-runner.test.sh).

set -u

REPO="/Users/boliu/sas-research"
DATAROOM="${OOS_DATAROOM:-/Users/boliu/sas-data-room}"

# Runner config + a launchd-safe PATH (launchd starts with a minimal PATH).
export OOS_DATAROOM="$DATAROOM"
export OOS_CUMULATIVE_DIR="${OOS_CUMULATIVE_DIR:-$DATAROOM/oos-cumulative-sol-curve-unique-buyers}"
export OOS_DASHBOARD_TOKEN_FILE="${OOS_DASHBOARD_TOKEN_FILE:-$HOME/.sas-dashboard-token}"
export DUNE_KEY_FILE="${DUNE_KEY_FILE:-$HOME/.dune_api_key}"
export PYTHON="${PYTHON:-/Library/Frameworks/Python.framework/Versions/3.10/bin/python3}"
export PATH="/usr/local/bin:/Library/Frameworks/Python.framework/Versions/3.10/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

LOG_DIR="$DATAROOM/oos-runner-logs"
STATE_DIR="$DATAROOM/oos-runner-state"
mkdir -p "$LOG_DIR" "$STATE_DIR"

HALT_MARKER="$STATE_DIR/LOOKPOINT_READY_N50_AUDIT_REQUIRED"
BLOCK_MARKER="$STATE_DIR/FAIL_CLOSED_BLOCKER"
LOCKDIR="$STATE_DIR/runner.lock.d"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_LOG="$LOG_DIR/cycle-$TS.log"
ulog() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG_DIR/runner.log"; }

MODE="cycle"; [ "${1:-}" = "--check" ] && MODE="check"

# Atomic lock (macOS has no flock). Reclaim a >6h-old lock from a crashed prior run.
if [ -d "$LOCKDIR" ] && [ -n "$(find "$LOCKDIR" -maxdepth 0 -mmin +360 2>/dev/null)" ]; then
  ulog "reclaiming stale lock (>6h): $LOCKDIR"; rmdir "$LOCKDIR" 2>/dev/null || true
fi
if ! mkdir "$LOCKDIR" 2>/dev/null; then ulog "another runner holds the lock ($LOCKDIR); exiting 0."; exit 0; fi
trap 'rmdir "$LOCKDIR" 2>/dev/null || true' EXIT

cd "$REPO" || { ulog "cannot cd $REPO; exiting 1."; exit 1; }

run_runner() {
  if [ -n "${OOS_RUNNER_CMD:-}" ]; then eval "$OOS_RUNNER_CMD" > "$RUN_LOG" 2>&1; return $?; fi
  node scripts/run-oos-daily-cycle.js "$@" > "$RUN_LOG" 2>&1; return $?
}

# Smoke: side-effect-free --check, writes no markers, ignores the halt gate (--check changes nothing).
if [ "$MODE" = "check" ]; then
  ulog "SMOKE --check (side-effect-free; writes no markers)"
  run_runner --check; rc=$?
  ulog "smoke --check exit=$rc (log: $RUN_LOG)"
  exit "$rc"
fi

# Sticky halt markers: once tripped, never run another real cycle until the operator clears it.
if [ -f "$HALT_MARKER" ]; then ulog "HALT marker present (LOOKPOINT_READY_N50_AUDIT_REQUIRED); skipping cycle. Awaiting Codex audit."; exit 0; fi
if [ -f "$BLOCK_MARKER" ]; then ulog "BLOCK marker present (FAIL_CLOSED_BLOCKER); skipping cycle. Fix, then remove $BLOCK_MARKER."; exit 0; fi

# Setup preflight (non-sticky: a setup glitch just retries next day; no permanent halt).
command -v node >/dev/null 2>&1 || { ulog "node not on PATH; exiting 1 (no marker)."; exit 1; }
[ -f "$OOS_DASHBOARD_TOKEN_FILE" ] || { ulog "dashboard token file missing: $OOS_DASHBOARD_TOKEN_FILE; exiting 1 (no marker)."; exit 1; }

ulog "starting OOS daily cycle; per-run log: $RUN_LOG"
run_runner; rc=$?
ulog "runner exit code = $rc"

case "$rc" in
  0)
    ulog "exit 0 -> DAILY_ACCUMULATION_CONTINUE / DATA_INSUFFICIENT_WAIT / ALREADY_RAN_TODAY"
    exit 0 ;;
  10)
    { echo "$TS LOOKPOINT_READY_N50_AUDIT_REQUIRED"; echo "run_log=$RUN_LOG"; echo "note=AUC NOT read; Codex audit required before any look point."; } > "$HALT_MARKER"
    ulog "exit 10 -> n=50 reached. Wrote $HALT_MARKER; future cycles skip. AUC NOT read."
    exit 0 ;;
  *)
    { echo "$TS FAIL_CLOSED_BLOCKER exit=$rc"; echo "run_log=$RUN_LOG"; echo "note=runner fail-closed; inspect log, fix, then remove this marker to resume."; } > "$BLOCK_MARKER"
    ulog "exit $rc -> FAIL_CLOSED_BLOCKER. Wrote $BLOCK_MARKER; future cycles skip until cleared."
    exit 0 ;;
esac
