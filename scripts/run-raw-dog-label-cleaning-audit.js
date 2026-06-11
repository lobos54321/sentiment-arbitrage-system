#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    rawDb: process.env.RAW_SIGNAL_OUTCOMES_DB || '',
    out: './data/audits/raw-dog-label-cleaning/latest.json',
    threshold: 2,
    windowSec: 7200,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--raw-db') { args.rawDb = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--threshold') { args.threshold = Number(next); i += 1; continue; }
    if (key === '--window-sec') { args.windowSec = Number(next); i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/run-raw-dog-label-cleaning-audit.js --raw-db raw_signal_outcomes.db --out out.json',
    '',
    'Flags raw dog labels whose recorded peak is incompatible with the stored native price path.',
  ].join('\n');
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeTs(value) {
  const n = numeric(value);
  if (n == null) return null;
  return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

async function loadDatabase() {
  const mod = await import('better-sqlite3');
  return mod.default || mod;
}

function openReadonlySqlite(Database, filePath) {
  const db = new Database(filePath, { readonly: true, fileMustExist: true });
  try { db.pragma('mmap_size = 0'); } catch {}
  try { db.pragma('query_only = ON'); } catch {}
  return db;
}

function tableExists(db, tableName) {
  return db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(tableName) != null;
}

function tableColumns(db, tableName) {
  return new Set(db.prepare(`PRAGMA table_info(${tableName})`).all().map((row) => row.name));
}

function firstExisting(cols, names = []) {
  return names.find((name) => cols.has(name)) || null;
}

function sqlExprForFirst(cols, names = [], fallback = 'NULL') {
  const present = names.filter((name) => cols.has(name));
  if (!present.length) return fallback;
  if (present.length === 1) return present[0];
  return `COALESCE(${present.join(', ')})`;
}

function tierFromRow(row = {}) {
  return String(row.raw_sustained_tier || row.raw_primary_tier || row.tier || '').toLowerCase();
}

function tierFromObservedMultiple(value) {
  const multiple = numeric(value);
  if (multiple == null) return 'unknown';
  if (multiple >= 2) return 'gold';
  if (multiple >= 1.5) return 'silver';
  return 'dud';
}

function recordedPeakMultiple(row = {}) {
  const explicit = numeric(row.recorded_peak_multiple ?? row.raw_sustained_peak_multiple ?? row.peak_multiple);
  if (explicit != null && explicit > 0) return explicit;

  const pct = numeric(
    row.max_sustained_peak_pct
    ?? row.raw_sustained_peak_pct
    ?? row.raw_peak_pct
    ?? row.peak_pct
  );
  if (pct == null) return null;
  // Existing raw dog reports have used both decimal PnL (1.0 = +100%)
  // and percentage points (100 = +100%). Keep this explicit and auditable.
  if (Math.abs(pct) > 20) return 1 + (pct / 100);
  return 1 + pct;
}

function observedPeakMultiple({ baselinePrice, observedMaxPrice }) {
  const base = numeric(baselinePrice);
  const max = numeric(observedMaxPrice);
  if (base == null || base <= 0 || max == null || max <= 0) return null;
  return max / base;
}

export function classifyLabelRow(row = {}, { threshold = 2 } = {}) {
  const tier = tierFromRow(row);
  const recordedMultiple = recordedPeakMultiple(row);
  const observedMultiple = observedPeakMultiple({
    baselinePrice: row.baseline_price,
    observedMaxPrice: row.observed_max_price,
  });
  const ratio = recordedMultiple != null && observedMultiple != null && observedMultiple > 0
    ? recordedMultiple / observedMultiple
    : null;

  let label_status = 'clean';
  let reason = 'within_tolerance';
  let chainTruthNeed = 'none';
  let effectiveTier = tier;
  if (numeric(row.baseline_price) == null || numeric(row.baseline_price) <= 0) {
    label_status = 'quarantine';
    reason = 'missing_baseline_price';
    chainTruthNeed = 'baseline_reconstruction';
  } else if (observedMultiple == null) {
    label_status = 'quarantine';
    reason = 'no_native_bars';
    chainTruthNeed = recordedMultiple == null
      ? 'native_path_reconstruction_for_missing_peak'
      : 'native_path_reconstruction';
  } else if (recordedMultiple == null) {
    label_status = 'clean';
    reason = 'missing_recorded_peak_repaired_from_native_bars';
    effectiveTier = tierFromObservedMultiple(observedMultiple);
  } else if (ratio != null && ratio > threshold) {
    label_status = 'quarantine';
    reason = 'label_unit_corrupt';
    chainTruthNeed = 'polluted_peak_window_adjudication';
  }

  return {
    ...row,
    tier,
    effective_tier: effectiveTier,
    recorded_peak_multiple: recordedMultiple,
    observed_peak_multiple: observedMultiple,
    label_to_path_ratio: ratio,
    label_status,
    label_cleaning_reason: reason,
    chain_truth_need: chainTruthNeed,
  };
}

function loadOutcomeRows(db, { windowSec = 7200 } = {}) {
  if (!tableExists(db, 'raw_signal_outcomes')) {
    throw new Error('raw_signal_outcomes table missing');
  }
  const outCols = tableColumns(db, 'raw_signal_outcomes');
  const tierExpr = sqlExprForFirst(outCols, ['raw_sustained_tier', 'raw_primary_tier', 'tier'], "''");
  const baselineExpr = sqlExprForFirst(outCols, ['baseline_price', 'anchor_price', 'entry_price'], 'NULL');
  const peakPctExpr = sqlExprForFirst(outCols, ['max_sustained_peak_pct', 'raw_sustained_peak_pct', 'raw_peak_pct', 'peak_pct'], 'NULL');
  const peakMultExpr = sqlExprForFirst(outCols, ['recorded_peak_multiple', 'raw_sustained_peak_multiple', 'peak_multiple'], 'NULL');
  const timeToPeakExpr = sqlExprForFirst(outCols, ['time_to_sustained_peak_sec', 'time_to_peak_sec'], 'NULL');
  const symbolExpr = sqlExprForFirst(outCols, ['symbol', 'ticker'], 'NULL');
  const rows = db.prepare(`
    SELECT
      token_ca,
      signal_ts,
      ${symbolExpr} AS symbol,
      ${tierExpr} AS tier,
      ${baselineExpr} AS baseline_price,
      ${peakPctExpr} AS max_sustained_peak_pct,
      ${peakMultExpr} AS recorded_peak_multiple,
      ${timeToPeakExpr} AS time_to_sustained_peak_sec
    FROM raw_signal_outcomes
    WHERE token_ca IS NOT NULL
      AND signal_ts IS NOT NULL
  `).all();

  if (!tableExists(db, 'raw_price_bars_1m')) {
    return rows.map((row) => ({ ...row, observed_max_price: null, observed_bar_count: 0 }));
  }
  const barCols = tableColumns(db, 'raw_price_bars_1m');
  const tsCol = firstExisting(barCols, ['timestamp', 'ts', 'time']);
  if (!tsCol || !barCols.has('token_ca')) {
    return rows.map((row) => ({ ...row, observed_max_price: null, observed_bar_count: 0 }));
  }
  const priceExpr = sqlExprForFirst(barCols, ['high', 'close', 'price_sol', 'price', 'open'], 'NULL');
  const stmt = db.prepare(`
    SELECT
      MAX(${priceExpr}) AS observed_max_price,
      COUNT(*) AS observed_bar_count
    FROM raw_price_bars_1m
    WHERE token_ca = ?
      AND ${tsCol} >= ?
      AND ${tsCol} <= ?
  `);
  return rows.map((row) => {
    const ts = normalizeTs(row.signal_ts);
    if (ts == null) return { ...row, observed_max_price: null, observed_bar_count: 0 };
    const peakDelay = numeric(row.time_to_sustained_peak_sec);
    const endTs = ts + Math.max(windowSec, peakDelay != null ? Math.ceil(peakDelay) : 0);
    const bar = stmt.get(row.token_ca, ts - 60, endTs);
    return {
      ...row,
      observed_max_price: numeric(bar?.observed_max_price),
      observed_bar_count: Number(bar?.observed_bar_count || 0),
    };
  });
}

function summarize(rows = []) {
  const byReason = {};
  const uniqueByReason = {};
  const uniqueByStatus = {};
  const seenByReason = new Map();
  for (const row of rows) {
    byReason[row.label_cleaning_reason] = (byReason[row.label_cleaning_reason] || 0) + 1;
    uniqueByStatus[row.label_status] = uniqueByStatus[row.label_status] || new Set();
    uniqueByStatus[row.label_status].add(row.token_ca);
    if (!seenByReason.has(row.label_cleaning_reason)) seenByReason.set(row.label_cleaning_reason, new Set());
    seenByReason.get(row.label_cleaning_reason).add(row.token_ca);
  }
  for (const [reason, set] of seenByReason.entries()) uniqueByReason[reason] = set.size;
  return {
    rows_n: rows.length,
    unique_tokens_n: new Set(rows.map((row) => row.token_ca)).size,
    clean_rows_n: rows.filter((row) => row.label_status === 'clean').length,
    quarantine_rows_n: rows.filter((row) => row.label_status !== 'clean').length,
    clean_unique_tokens_n: uniqueByStatus.clean?.size || 0,
    quarantine_unique_tokens_n: uniqueByStatus.quarantine?.size || 0,
    by_reason: byReason,
    unique_by_reason: uniqueByReason,
  };
}

export function buildReport(rows = [], { threshold = 2, windowSec = 7200 } = {}) {
  const classified = rows.map((row) => classifyLabelRow(row, { threshold }));
  return {
    schema_version: 'raw_dog_label_cleaning.v1',
    generated_at: new Date().toISOString(),
    threshold,
    window_sec: windowSec,
    summary: summarize(classified),
    rows: classified,
  };
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.rawDb) throw new Error('Provide --raw-db');
  const Database = await loadDatabase();
  const db = openReadonlySqlite(Database, args.rawDb);
  let rows;
  try {
    rows = loadOutcomeRows(db, { windowSec: args.windowSec });
  } finally {
    try { db.close(); } catch {}
  }
  const report = buildReport(rows, { threshold: args.threshold, windowSec: args.windowSec });
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({ out: args.out, summary: report.summary }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}
