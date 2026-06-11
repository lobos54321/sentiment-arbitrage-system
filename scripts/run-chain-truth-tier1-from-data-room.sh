#!/usr/bin/env bash
set -euo pipefail

# Run Tier 1 pump.fun chain-truth passes from a frozen data room.
# This script is offline/read-only with respect to production. It only reads
# local data-room worklists and writes local JSON/JSONL outputs.
#
# Usage:
#   DATA_ROOM_DIR=~/sas-data-room/chain-truth-... MODE=smoke DRY_RUN=1 \
#     bash scripts/run-chain-truth-tier1-from-data-room.sh
#
#   DATA_ROOM_DIR=~/sas-data-room/chain-truth-... MODE=all \
#     ALCHEMY_RPC_FILE=~/.alchemy_rpc bash scripts/run-chain-truth-tier1-from-data-room.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOM_DIR="${DATA_ROOM_DIR:-}"
MODE="${MODE:-smoke}" # smoke | anchor | peak | all
DRY_RUN="${DRY_RUN:-0}"
ALCHEMY_RPC_URL="${ALCHEMY_RPC_URL:-}"
ALCHEMY_RPC_FILE="${ALCHEMY_RPC_FILE:-$HOME/.alchemy_rpc}"
ANCHOR_WORKLIST="${ANCHOR_WORKLIST:-}"
PEAK_WORKLIST="${PEAK_WORKLIST:-}"
OUT_DIR="${OUT_DIR:-}"
LIMIT="${LIMIT:-}"
PAGE_SIZE="${PAGE_SIZE:-100}"
ANCHOR_MAX_PAGES="${ANCHOR_MAX_PAGES:-3}"
PEAK_MAX_PAGES="${PEAK_MAX_PAGES:-5}"
RPC_TX_DELAY_MS="${RPC_TX_DELAY_MS:-100}"
MAX_FEASIBLE_PRICE_SOL="${MAX_FEASIBLE_PRICE_SOL:-0}"
SAS_NODE_MODULES_SOURCE="${SAS_NODE_MODULES_SOURCE:-/Users/boliu/sentiment-arbitrage-system/node_modules}"

cleanup_node_modules=0
cleanup() {
  if [[ "$cleanup_node_modules" == "1" && -L "$ROOT_DIR/node_modules" ]]; then
    rm -f "$ROOT_DIR/node_modules"
  fi
}
trap cleanup EXIT

usage() {
  cat <<'EOF'
Usage:
  DATA_ROOM_DIR=~/sas-data-room/chain-truth-... MODE=smoke DRY_RUN=1 \
    bash scripts/run-chain-truth-tier1-from-data-room.sh

Env:
  MODE=smoke|anchor|peak|all
  DRY_RUN=1 to validate worklists without RPC
  ALCHEMY_RPC_URL or ALCHEMY_RPC_FILE=~/.alchemy_rpc for real runs
  LIMIT=<n> optional cap
  OUT_DIR defaults to $DATA_ROOM_DIR/chain-truth
EOF
}

ensure_node_modules() {
  if [[ -e "$ROOT_DIR/node_modules" ]]; then
    return
  fi
  if [[ -d "$SAS_NODE_MODULES_SOURCE" ]]; then
    ln -s "$SAS_NODE_MODULES_SOURCE" "$ROOT_DIR/node_modules"
    cleanup_node_modules=1
    return
  fi
  echo "node_modules missing in $ROOT_DIR and SAS_NODE_MODULES_SOURCE not found: $SAS_NODE_MODULES_SOURCE" >&2
  exit 2
}

read_rpc_url() {
  if [[ -n "$ALCHEMY_RPC_URL" ]]; then
    return
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    return
  fi
  if [[ ! -f "$ALCHEMY_RPC_FILE" ]]; then
    echo "Missing RPC URL. Set ALCHEMY_RPC_URL or ALCHEMY_RPC_FILE. Use DRY_RUN=1 for no-RPC validation." >&2
    exit 2
  fi
  local raw
  raw="$(tr -d '\r\n' < "$ALCHEMY_RPC_FILE")"
  if [[ "$raw" == *=* ]]; then
    ALCHEMY_RPC_URL="${raw#*=}"
  else
    ALCHEMY_RPC_URL="$raw"
  fi
  if [[ -z "$ALCHEMY_RPC_URL" ]]; then
    echo "Alchemy RPC URL is empty" >&2
    exit 2
  fi
}

run_pass() {
  local name="$1"
  local worklist="$2"
  local pre_sec="$3"
  local post_sec="$4"
  local max_pages="$5"
  local default_limit="$6"
  local out_json="$OUT_DIR/chain-truth-tier1-${name}.json"
  local out_jsonl="$OUT_DIR/chain-truth-tier1-${name}.jsonl"

  if [[ ! -f "$worklist" ]]; then
    echo "Missing worklist for $name: $worklist" >&2
    exit 2
  fi

  local -a cmd=(
    node "$ROOT_DIR/scripts/run-helius-pumpfun-curve-decode-audit.js"
    --tokens-file "$worklist"
    --out "$out_json"
    --checkpoint-out "$out_jsonl"
    --pre-sec "$pre_sec"
    --post-sec "$post_sec"
    --page-size "$PAGE_SIZE"
    --max-pages "$max_pages"
    --rpc-tx-delay-ms "$RPC_TX_DELAY_MS"
    --progress-every 1
    --resume
  )

  if [[ "$MAX_FEASIBLE_PRICE_SOL" != "0" ]]; then
    cmd+=(--max-feasible-price-sol "$MAX_FEASIBLE_PRICE_SOL")
  fi
  if [[ -n "$LIMIT" ]]; then
    cmd+=(--limit "$LIMIT")
  elif [[ -n "$default_limit" ]]; then
    cmd+=(--limit "$default_limit")
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    cmd+=(--dry-run)
  else
    cmd+=(--rpc-url "$ALCHEMY_RPC_URL" --rpc-mode raw)
  fi

  echo "== Tier1 $name =="
  echo "worklist=$worklist"
  echo "out=$out_json"
  "${cmd[@]}"
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ -z "$DATA_ROOM_DIR" ]]; then
  echo "DATA_ROOM_DIR is required" >&2
  usage >&2
  exit 2
fi

ANCHOR_WORKLIST="${ANCHOR_WORKLIST:-$DATA_ROOM_DIR/worklists/tiers/tier1-anchor-worklist-v2.txt}"
PEAK_WORKLIST="${PEAK_WORKLIST:-$DATA_ROOM_DIR/worklists/tiers/tier1-peak-worklist-v2.txt}"
OUT_DIR="${OUT_DIR:-$DATA_ROOM_DIR/chain-truth}"
mkdir -p "$OUT_DIR"
ensure_node_modules
read_rpc_url

case "$MODE" in
  smoke)
    LIMIT="${LIMIT:-2}"
    run_pass anchor-smoke "$ANCHOR_WORKLIST" 90 90 "$ANCHOR_MAX_PAGES" "$LIMIT"
    ;;
  anchor)
    run_pass anchor "$ANCHOR_WORKLIST" 90 90 "$ANCHOR_MAX_PAGES" ""
    ;;
  peak)
    run_pass peak "$PEAK_WORKLIST" 180 180 "$PEAK_MAX_PAGES" ""
    ;;
  all)
    run_pass anchor "$ANCHOR_WORKLIST" 90 90 "$ANCHOR_MAX_PAGES" ""
    run_pass peak "$PEAK_WORKLIST" 180 180 "$PEAK_MAX_PAGES" ""
    ;;
  *)
    echo "Unknown MODE=$MODE" >&2
    usage >&2
    exit 2
    ;;
esac

echo "Tier1 outputs: $OUT_DIR"
