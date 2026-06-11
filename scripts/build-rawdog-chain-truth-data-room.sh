#!/usr/bin/env bash
set -euo pipefail

# Build a durable, reproducible raw-dog chain-truth data room from a downloaded
# snapshot bundle. This script is offline/read-only: it never talks to
# production and never writes production DBs.
#
# Typical local usage after downloading Zeabur's rawdog-audit-dbs.tgz:
#   SNAPSHOT_TGZ=~/Downloads/rawdog-audit-dbs.tgz \
#   OUT_DIR=~/sas-data-room/chain-truth-$(date -u +%Y%m%dT%H%M%SZ) \
#   bash scripts/build-rawdog-chain-truth-data-room.sh
#
# Optional, when GMGN touch JSON files are available:
#   DOG_TOUCH=~/sas-data-room/gmgn-dog-touch-results.json \
#   DUD_TOUCH=~/sas-data-room/gmgn-dud-touch-results.json \
#   bash scripts/build-rawdog-chain-truth-data-room.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$HOME/sas-data-room/chain-truth-$(date -u +%Y%m%dT%H%M%SZ)}"
SNAPSHOT_TGZ="${SNAPSHOT_TGZ:-}"
SNAPSHOT_DIR="${SNAPSHOT_DIR:-}"
RAW_DB="${RAW_DB:-}"
DOG_TOUCH="${DOG_TOUCH:-}"
DUD_TOUCH="${DUD_TOUCH:-}"
LABEL_THRESHOLD="${LABEL_THRESHOLD:-2}"
WINDOW_SEC="${WINDOW_SEC:-7200}"
TOUCH_MAX_DELTA_SEC="${TOUCH_MAX_DELTA_SEC:-600}"
SAS_NODE_MODULES_SOURCE="${SAS_NODE_MODULES_SOURCE:-/Users/boliu/sentiment-arbitrage-system/node_modules}"

cleanup_node_modules=0
cleanup() {
  if [[ "$cleanup_node_modules" == "1" && -L "$ROOT_DIR/node_modules" ]]; then
    rm -f "$ROOT_DIR/node_modules"
  fi
}
trap cleanup EXIT

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
  echo "Install dependencies or set SAS_NODE_MODULES_SOURCE before running." >&2
  exit 2
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

write_manifest() {
  local manifest="$OUT_DIR/manifest.json"
  node --input-type=module - "$OUT_DIR" "$manifest" <<'NODE'
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const outDir = process.argv[2];
const manifestPath = process.argv[3];

function walk(dir) {
  const out = [];
  for (const item of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, item.name);
    if (full === manifestPath) continue;
    if (item.isDirectory()) out.push(...walk(full));
    else if (item.isFile()) out.push(full);
  }
  return out;
}

function sha256(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

const files = walk(outDir).sort().map((file) => ({
  path: path.relative(outDir, file),
  bytes: fs.statSync(file).size,
  sha256: sha256(file),
}));

const manifest = {
  schema_version: 'rawdog_chain_truth_data_room_manifest.v1',
  generated_at: new Date().toISOString(),
  out_dir: outDir,
  production_snapshot_source: process.env.SNAPSHOT_TGZ || process.env.SNAPSHOT_DIR || process.env.RAW_DB || null,
  git_commit: process.env.GIT_COMMIT || null,
  label_threshold: Number(process.env.LABEL_THRESHOLD || 2),
  window_sec: Number(process.env.WINDOW_SEC || 7200),
  touch_max_delta_sec: Number(process.env.TOUCH_MAX_DELTA_SEC || 600),
  files,
};
fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
NODE
}

mkdir -p "$OUT_DIR/input" "$OUT_DIR/audits" "$OUT_DIR/worklists"
ensure_node_modules

GIT_COMMIT="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || true)"
export GIT_COMMIT LABEL_THRESHOLD WINDOW_SEC TOUCH_MAX_DELTA_SEC SNAPSHOT_TGZ SNAPSHOT_DIR RAW_DB

echo "== rawdog chain-truth data room =="
echo "ROOT_DIR=$ROOT_DIR"
echo "OUT_DIR=$OUT_DIR"
echo "GIT_COMMIT=$GIT_COMMIT"

if [[ -n "$SNAPSHOT_TGZ" ]]; then
  if [[ ! -f "$SNAPSHOT_TGZ" ]]; then
    echo "SNAPSHOT_TGZ not found: $SNAPSHOT_TGZ" >&2
    exit 2
  fi
  echo "== extracting snapshot tgz =="
  tar -xzf "$SNAPSHOT_TGZ" -C "$OUT_DIR/input"
  SNAPSHOT_DIR="$OUT_DIR/input"
elif [[ -n "$SNAPSHOT_DIR" ]]; then
  if [[ ! -d "$SNAPSHOT_DIR" ]]; then
    echo "SNAPSHOT_DIR not found: $SNAPSHOT_DIR" >&2
    exit 2
  fi
else
  SNAPSHOT_DIR="$OUT_DIR/input"
fi

if [[ -z "$RAW_DB" ]]; then
  for candidate in \
    "$SNAPSHOT_DIR/raw_signal_outcomes.snapshot.db" \
    "$SNAPSHOT_DIR/raw_signal_outcomes.db" \
    "$OUT_DIR/input/raw_signal_outcomes.snapshot.db" \
    "$OUT_DIR/input/raw_signal_outcomes.db"; do
    if [[ -f "$candidate" ]]; then
      RAW_DB="$candidate"
      break
    fi
  done
fi

if [[ -z "$RAW_DB" || ! -f "$RAW_DB" ]]; then
  echo "raw DB not found. Provide RAW_DB=... or SNAPSHOT_TGZ/SNAPSHOT_DIR containing raw_signal_outcomes.snapshot.db" >&2
  exit 2
fi

echo "RAW_DB=$RAW_DB"
echo "$(sha256_file "$RAW_DB")  $RAW_DB" > "$OUT_DIR/raw-db.sha256"

LABEL_AUDIT="$OUT_DIR/audits/raw-dog-label-cleaning.json"
CLEAN_PACK_DIR="$OUT_DIR/clean-pack"

echo "== label cleaning audit =="
node "$ROOT_DIR/scripts/run-raw-dog-label-cleaning-audit.js" \
  --raw-db "$RAW_DB" \
  --out "$LABEL_AUDIT" \
  --threshold "$LABEL_THRESHOLD" \
  --window-sec "$WINDOW_SEC"

echo "== build clean rawdog pack =="
node "$ROOT_DIR/scripts/build-clean-rawdog-pack.js" \
  --label-audit "$LABEL_AUDIT" \
  --out-dir "$CLEAN_PACK_DIR"

if [[ -n "$DOG_TOUCH" || -n "$DUD_TOUCH" ]]; then
  if [[ -z "$DOG_TOUCH" || -z "$DUD_TOUCH" ]]; then
    echo "DOG_TOUCH and DUD_TOUCH must be provided together; skipping free-source audit" >&2
  elif [[ ! -f "$DOG_TOUCH" || ! -f "$DUD_TOUCH" ]]; then
    echo "DOG_TOUCH or DUD_TOUCH file missing; skipping free-source audit" >&2
  else
    echo "== filter GMGN touch to clean cohort =="
    DOG_TOUCH_FILTERED="$OUT_DIR/audits/clean-dog-touch.json"
    DUD_TOUCH_FILTERED="$OUT_DIR/audits/clean-dud-touch.json"
    node "$ROOT_DIR/scripts/filter-gmgn-touch-by-clean-pack.js" \
      --touch "$DOG_TOUCH" \
      --anchors "$CLEAN_PACK_DIR/clean-dogs.json" \
      --out "$DOG_TOUCH_FILTERED" \
      --max-delta-sec "$TOUCH_MAX_DELTA_SEC"
    node "$ROOT_DIR/scripts/filter-gmgn-touch-by-clean-pack.js" \
      --touch "$DUD_TOUCH" \
      --anchors "$CLEAN_PACK_DIR/clean-duds.json" \
      --out "$DUD_TOUCH_FILTERED" \
      --max-delta-sec "$TOUCH_MAX_DELTA_SEC"

    echo "== free-source coverage audit =="
    FREE_SOURCE_AUDIT="$OUT_DIR/audits/free-source-coverage.json"
    TARGETED_OUT="$OUT_DIR/worklists/targeted-chain-truth.txt"
    node "$ROOT_DIR/scripts/run-free-source-coverage-audit.js" \
      --dogs "$DOG_TOUCH_FILTERED" \
      --duds "$DUD_TOUCH_FILTERED" \
      --out "$FREE_SOURCE_AUDIT" \
      --targeted-out "$TARGETED_OUT"

    echo "== chain truth worklist v2 =="
    WORKLIST_V2="$OUT_DIR/worklists/chain-truth-worklist-v2.txt"
    node "$ROOT_DIR/scripts/build-chain-truth-worklist.js" \
      --targeted "$TARGETED_OUT" \
      --quarantine "$CLEAN_PACK_DIR/quarantine-tokens.txt" \
      --out "$WORKLIST_V2"

    echo "== tier worklists =="
    node "$ROOT_DIR/scripts/build-chain-truth-tier-worklists.js" \
      --worklist "$WORKLIST_V2" \
      --raw-db "$RAW_DB" \
      --out-dir "$OUT_DIR/worklists/tiers"
  fi
else
  echo "DOG_TOUCH/DUD_TOUCH not provided; building quarantine-only chain-truth worklists."
  WORKLIST_V2="$OUT_DIR/worklists/chain-truth-worklist-v2.txt"
  node "$ROOT_DIR/scripts/build-chain-truth-worklist.js" \
    --quarantine "$CLEAN_PACK_DIR/quarantine-tokens.txt" \
    --out "$WORKLIST_V2"
  node "$ROOT_DIR/scripts/build-chain-truth-tier-worklists.js" \
    --worklist "$WORKLIST_V2" \
    --raw-db "$RAW_DB" \
    --out-dir "$OUT_DIR/worklists/tiers"
fi

echo "== write manifest =="
write_manifest

echo "Data room ready: $OUT_DIR"
find "$OUT_DIR" -maxdepth 3 -type f | sort
