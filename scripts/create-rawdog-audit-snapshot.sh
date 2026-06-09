#!/usr/bin/env bash
set -euo pipefail

# Create zero-restart DB snapshots for the isolated raw-dog decision audit.
#
# Default mode is deliberately conservative:
#   - Online Backup for raw_signal_outcomes.db.
#   - 24h(+slack) decision-table subset from paper_trades.db.
#   - No full 1.4GB paper DB copy unless FULL_PAPER=1 and disk has headroom.
#
# Intended Zeabur shell usage:
#   bash scripts/create-rawdog-audit-snapshot.sh
#
# Optional:
#   DATA_DIR=/app/data OUT_DIR=/app/data/audit-snapshots HOURS=24 bash scripts/create-rawdog-audit-snapshot.sh
#   FULL_PAPER=1 MIN_FREE_MB=2500 bash scripts/create-rawdog-audit-snapshot.sh

DATA_DIR="${DATA_DIR:-/app/data}"
OUT_DIR="${OUT_DIR:-$DATA_DIR/audit-snapshots}"
HOURS="${HOURS:-24}"
MIN_FREE_MB="${MIN_FREE_MB:-2500}"
FULL_PAPER="${FULL_PAPER:-0}"
RAW_DB="${RAW_SIGNAL_OUTCOMES_DB:-${RAW_DB:-$DATA_DIR/raw_signal_outcomes.db}}"
PAPER_DB="${PAPER_DB:-$DATA_DIR/paper_trades.db}"

free_mb() {
  df -Pm "$DATA_DIR" | awk 'NR==2 {print $4}'
}

require_file() {
  local label="$1"
  local file="$2"
  if [[ ! -f "$file" ]]; then
    echo "missing ${label}: ${file}" >&2
    return 1
  fi
}

echo "== rawdog audit snapshot preflight =="
echo "DATA_DIR=$DATA_DIR"
echo "OUT_DIR=$OUT_DIR"
echo "HOURS=$HOURS"
echo "MIN_FREE_MB=$MIN_FREE_MB"
echo "FULL_PAPER=$FULL_PAPER"
echo "RAW_DB=$RAW_DB"
echo "PAPER_DB=$PAPER_DB"
df -h "$DATA_DIR"

if [[ ! -f "$RAW_DB" ]]; then
  echo "RAW_DB not found at $RAW_DB; searching $DATA_DIR ..." >&2
  find "$DATA_DIR" -name 'raw_signal*' -print 2>/dev/null || true
  exit 2
fi
require_file "paper db" "$PAPER_DB"

mkdir -p "$OUT_DIR"

echo "== snapshot raw_signal_outcomes.db with SQLite Online Backup =="
RAW_DB="$RAW_DB" OUT_DIR="$OUT_DIR" node --input-type=module <<'NODE'
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const src = process.env.RAW_DB;
const dst = path.join(process.env.OUT_DIR, 'raw_signal_outcomes.snapshot.db');

if (!fs.existsSync(src)) {
  console.error(`missing raw db: ${src}`);
  process.exit(2);
}

const db = new Database(src, { readonly: true, fileMustExist: true, timeout: 30000 });
db.pragma('mmap_size = 0');
await db.backup(dst);
db.close();
console.log(`raw snapshot ok: ${dst}`);
NODE

echo "== disk after raw snapshot =="
df -h "$DATA_DIR"
ls -lh "$OUT_DIR"

if [[ "$FULL_PAPER" == "1" ]]; then
  current_free_mb="$(free_mb)"
  if (( current_free_mb < MIN_FREE_MB )); then
    echo "free space ${current_free_mb}MB < MIN_FREE_MB ${MIN_FREE_MB}MB; refusing full paper DB backup" >&2
    exit 3
  fi
  echo "== FULL_PAPER=1: snapshot full paper_trades.db with SQLite Online Backup =="
  PAPER_DB="$PAPER_DB" OUT_DIR="$OUT_DIR" node --input-type=module <<'NODE'
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const src = process.env.PAPER_DB;
const dst = path.join(process.env.OUT_DIR, 'paper_trades.snapshot.db');

if (!fs.existsSync(src)) {
  console.error(`missing paper db: ${src}`);
  process.exit(2);
}

const db = new Database(src, { readonly: true, fileMustExist: true, timeout: 30000 });
db.pragma('mmap_size = 0');
await db.backup(dst);
db.close();
console.log(`paper snapshot ok: ${dst}`);
NODE
  PAPER_OUT="paper_trades.snapshot.db"
else
  echo "== create small paper_decision_subset.db for audit =="
  PAPER_DB="$PAPER_DB" OUT_DIR="$OUT_DIR" HOURS="$HOURS" node --input-type=module <<'NODE'
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const srcPath = process.env.PAPER_DB;
const outDir = process.env.OUT_DIR;
const dstPath = path.join(outDir, 'paper_decision_subset.db');
const hours = Math.max(1, Number(process.env.HOURS || 24) || 24);
const sinceTs = Math.floor(Date.now() / 1000) - (hours + 1) * 3600;
const tables = ['a_class_decision_events', 'opportunity_events'];

if (!fs.existsSync(srcPath)) {
  console.error(`missing paper db: ${srcPath}`);
  process.exit(2);
}
try { fs.unlinkSync(dstPath); } catch {}

const src = new Database(srcPath, { readonly: true, fileMustExist: true, timeout: 30000 });
src.pragma('mmap_size = 0');
src.pragma('query_only = ON');
const dst = new Database(dstPath);

const tableExists = src.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?");
for (const table of tables) {
  const row = tableExists.get(table);
  if (!row) {
    console.log(`subset skip missing table: ${table}`);
    continue;
  }
  const schema = src.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name=?").get(table)?.sql;
  if (!schema) {
    console.log(`subset skip schema missing: ${table}`);
    continue;
  }
  dst.exec(schema);
  const cols = src.prepare(`PRAGMA table_info(${table})`).all().map((col) => col.name);
  const quotedCols = cols.map((name) => `"${String(name).replaceAll('"', '""')}"`);
  const insert = dst.prepare(`INSERT INTO ${table} (${quotedCols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`);
  const selectSql = cols.includes('event_ts')
    ? `SELECT * FROM ${table} WHERE event_ts >= ? ORDER BY event_ts ASC, id ASC`
    : `SELECT * FROM ${table}`;
  const rows = src.prepare(selectSql).all(...(cols.includes('event_ts') ? [sinceTs] : []));
  const tx = dst.transaction((items) => {
    for (const item of items) insert.run(cols.map((name) => item[name]));
  });
  tx(rows);
  console.log(`subset copied ${table}: ${rows.length} rows`);
}

dst.pragma('wal_checkpoint(TRUNCATE)');
dst.close();
src.close();
console.log(`paper decision subset ok: ${dstPath}`);
NODE
  PAPER_OUT="paper_decision_subset.db"
fi

echo "== final snapshot files =="
ls -lh "$OUT_DIR"
df -h "$DATA_DIR"

if [[ "${NO_TAR:-0}" == "1" ]]; then
  echo "NO_TAR=1; download files directly from $OUT_DIR"
  exit 0
fi

echo "== create archive =="
tar -czf "$OUT_DIR/rawdog-audit-dbs.tgz" \
  -C "$OUT_DIR" \
  raw_signal_outcomes.snapshot.db \
  "$PAPER_OUT"

ls -lh "$OUT_DIR/rawdog-audit-dbs.tgz"
echo "Download: $OUT_DIR/rawdog-audit-dbs.tgz"
echo "After download, run: rm -rf '$OUT_DIR'"
