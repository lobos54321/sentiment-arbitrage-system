#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
let Database = null;

function getDatabase() {
  if (Database) return Database;
  try {
    Database = require('better-sqlite3');
    return Database;
  } catch (error) {
    throw new Error(`better-sqlite3 is required only when --raw-db or --paper-db is provided: ${error.message}`);
  }
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    dogs: '',
    duds: '',
    rawDb: process.env.RAW_SIGNAL_OUTCOMES_DB || '',
    paperDb: process.env.PAPER_DB || '',
    out: './data/audits/gmgn-stage-stratified/latest.json',
    decisionTokenOutDir: '',
    decisionWindowSec: 900,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--dogs') { args.dogs = next; i += 1; continue; }
    if (key === '--duds') { args.duds = next; i += 1; continue; }
    if (key === '--raw-db') { args.rawDb = next; i += 1; continue; }
    if (key === '--paper-db') { args.paperDb = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--decision-token-out-dir') { args.decisionTokenOutDir = next; i += 1; continue; }
    if (key === '--decision-window-sec') { args.decisionWindowSec = Number(next); i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/run-gmgn-stage-stratified-audit.js --dogs dog-touch.json --duds dud-touch.json [options]',
    '',
    'Options:',
    '  --raw-db <path>                 Optional raw_signal_outcomes.db for peak confirmation labels',
    '  --paper-db <path>               Optional paper_trades.db/subset for decision_ts matching',
    '  --out <path>                    Output JSON path',
    '  --decision-token-out-dir <dir>  Also write dog/dud token|decision_ts files for GMGN re-touch',
    '  --decision-window-sec <n>       Max signal->decision match window, default 900',
  ].join('\n');
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function loadResults(filePath) {
  const parsed = readJson(filePath);
  return Array.isArray(parsed?.results) ? parsed.results : [];
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeTs(value) {
  const n = numeric(value);
  if (n != null) return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return Math.floor(parsed / 1000);
  }
  return null;
}

function openReadonlySqlite(filePath) {
  const Sqlite = getDatabase();
  const db = new Sqlite(filePath, { readonly: true, fileMustExist: true });
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

function percentile(sortedValues, pct) {
  if (!sortedValues.length) return null;
  const idx = (sortedValues.length - 1) * pct;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sortedValues[lo];
  return sortedValues[lo] + (sortedValues[hi] - sortedValues[lo]) * (idx - lo);
}

function summarizeNumbers(values = []) {
  const clean = values.map(Number).filter(Number.isFinite).sort((a, b) => a - b);
  if (!clean.length) return { n: 0, min: null, median: null, p75: null, p90: null, max: null };
  return {
    n: clean.length,
    min: clean[0],
    median: percentile(clean, 0.5),
    p75: percentile(clean, 0.75),
    p90: percentile(clean, 0.9),
    max: clean[clean.length - 1],
  };
}

function auc(dogValues = [], dudValues = []) {
  const dogs = dogValues.map(Number).filter(Number.isFinite);
  const duds = dudValues.map(Number).filter(Number.isFinite);
  if (!dogs.length || !duds.length) return null;
  let wins = 0;
  let ties = 0;
  for (const d of dogs) {
    for (const u of duds) {
      if (d > u) wins += 1;
      else if (d === u) ties += 1;
    }
  }
  return Number(((wins + ties * 0.5) / (dogs.length * duds.length)).toFixed(4));
}

function volumeVisibilityStage(row = {}) {
  const lag = numeric(row.first_nonzero_volume_lag_sec);
  if (lag == null) return 'volume_not_visible_in_window';
  if (lag <= 0) return 'already_volume_visible_at_anchor';
  if (lag <= 300) return 'volume_visible_within_5m';
  if (lag <= 900) return 'volume_visible_5m_to_15m';
  return 'dark_after_15m';
}

function tokenKey(row = {}) {
  return `${String(row.token_ca || '').trim()}:${Math.floor(Number(row.signal_ts || 0))}`;
}

function loadPeakLabels(rawDbPath) {
  const labels = new Map();
  if (!rawDbPath || !fs.existsSync(rawDbPath)) return labels;
  const db = openReadonlySqlite(rawDbPath);
  try {
    if (!tableExists(db, 'raw_signal_outcomes')) return labels;
    const cols = tableColumns(db, 'raw_signal_outcomes');
    const tierExpr = cols.has('raw_sustained_tier') ? 'raw_sustained_tier' : 'raw_primary_tier';
    const rows = db.prepare(`
      SELECT
        token_ca,
        signal_ts,
        ${tierExpr} AS tier,
        time_to_sustained_peak_sec,
        max_sustained_peak_pct
      FROM raw_signal_outcomes
      WHERE token_ca IS NOT NULL
        AND signal_ts IS NOT NULL
        AND COALESCE(${tierExpr}, '') IN ('gold', 'silver')
    `).all();
    for (const row of rows) {
      labels.set(tokenKey(row), {
        tier: row.tier || null,
        time_to_sustained_peak_sec: numeric(row.time_to_sustained_peak_sec),
        max_sustained_peak_pct: numeric(row.max_sustained_peak_pct),
      });
    }
  } finally {
    try { db.close(); } catch {}
  }
  return labels;
}

function eventTime(row = {}) {
  return normalizeTs(row.event_ts ?? row.created_at ?? row.updated_at ?? row.timestamp ?? row.ts);
}

function readDecisionRows(db, tableName, tokenCa) {
  if (!tableExists(db, tableName)) return [];
  const cols = tableColumns(db, tableName);
  if (!cols.has('token_ca')) return [];
  const selectCols = ['event_ts', 'created_at', 'updated_at', 'timestamp', 'ts']
    .filter((col) => cols.has(col));
  if (!selectCols.length) return [];
  return db.prepare(`
    SELECT ${selectCols.join(', ')}
    FROM ${tableName}
    WHERE token_ca = ?
  `).all(tokenCa).map((row) => ({ source: tableName, event_ts: eventTime(row) })).filter((row) => row.event_ts != null);
}

function loadDecisionMatches(paperDbPath, anchors = [], { decisionWindowSec = 900 } = {}) {
  const matches = new Map();
  if (!paperDbPath || !fs.existsSync(paperDbPath)) return matches;
  const db = openReadonlySqlite(paperDbPath);
  const cache = new Map();
  try {
    for (const anchor of anchors) {
      const tokenCa = String(anchor.token_ca || '').trim();
      const signalTs = numeric(anchor.signal_ts);
      if (!tokenCa || signalTs == null) continue;
      if (!cache.has(tokenCa)) {
        cache.set(tokenCa, [
          ...readDecisionRows(db, 'a_class_decision_events', tokenCa),
          ...readDecisionRows(db, 'opportunity_events', tokenCa),
        ].sort((a, b) => a.event_ts - b.event_ts));
      }
      const rows = cache.get(tokenCa) || [];
      const match = rows.find((row) => row.event_ts >= signalTs - 60 && row.event_ts <= signalTs + decisionWindowSec);
      if (!match) continue;
      matches.set(tokenKey(anchor), {
        decision_ts: match.event_ts,
        decision_source: match.source,
        decision_lag_sec: Number((match.event_ts - signalTs).toFixed(3)),
      });
    }
  } finally {
    try { db.close(); } catch {}
  }
  return matches;
}

function enrichRows(rows = [], { peakLabels = new Map(), decisionMatches = new Map() } = {}) {
  return rows.map((row) => {
    const key = tokenKey(row);
    const peak = peakLabels.get(key) || {};
    const decision = decisionMatches.get(key) || {};
    const visibilityLag = numeric(row.first_nonzero_volume_lag_sec);
    const timeToPeak = numeric(peak.time_to_sustained_peak_sec);
    let peakConfirmationTier = null;
    if (timeToPeak != null && visibilityLag != null) {
      peakConfirmationTier = timeToPeak < visibilityLag ? 'curve_phase_unconfirmed' : 'volume_confirmed';
    } else if (timeToPeak != null) {
      peakConfirmationTier = 'volume_unavailable_peak_unconfirmed';
    }
    return {
      ...row,
      volume_visibility_lag_sec: visibilityLag,
      volume_visibility_stage: volumeVisibilityStage(row),
      peak_confirmation_tier: peakConfirmationTier,
      raw_time_to_sustained_peak_sec: timeToPeak,
      raw_sustained_tier_from_db: peak.tier || null,
      decision_ts: decision.decision_ts ?? null,
      decision_source: decision.decision_source || null,
      decision_lag_sec: decision.decision_lag_sec ?? null,
    };
  });
}

function countBy(rows = [], keyFn) {
  const out = {};
  for (const row of rows) {
    const key = String(keyFn(row) || 'unknown');
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function fieldValues(rows = [], field, { positiveOnly = false } = {}) {
  return rows.map((row) => numeric(row[field])).filter((value) => (
    value != null && (!positiveOnly || value > 0)
  ));
}

function stratifiedAuc(dogs = [], duds = [], field) {
  const stages = [...new Set([
    ...dogs.map((row) => row.volume_visibility_stage),
    ...duds.map((row) => row.volume_visibility_stage),
  ])].sort();
  const out = {};
  for (const stage of stages) {
    const dogRows = dogs.filter((row) => row.volume_visibility_stage === stage);
    const dudRows = duds.filter((row) => row.volume_visibility_stage === stage);
    out[stage] = {
      dogs_n: dogRows.length,
      duds_n: dudRows.length,
      auc_all: auc(fieldValues(dogRows, field), fieldValues(dudRows, field)),
      auc_positive_only: auc(
        fieldValues(dogRows, field, { positiveOnly: true }),
        fieldValues(dudRows, field, { positiveOnly: true }),
      ),
      dog_summary: summarizeNumbers(fieldValues(dogRows, field)),
      dud_summary: summarizeNumbers(fieldValues(dudRows, field)),
    };
  }
  return out;
}

function writeDecisionTokenFiles(outDir, dogs = [], duds = []) {
  if (!outDir) return null;
  fs.mkdirSync(outDir, { recursive: true });
  const write = (name, rows) => {
    const file = path.join(outDir, name);
    const lines = rows
      .filter((row) => row.decision_ts != null)
      .map((row) => `${row.token_ca}|${Math.floor(row.decision_ts)}|${row.symbol || ''}`);
    fs.writeFileSync(file, `${lines.join('\n')}${lines.length ? '\n' : ''}`);
    return { file, rows: lines.length };
  };
  return {
    dogs: write('dog-decision-tokens.txt', dogs),
    duds: write('dud-decision-tokens.txt', duds),
  };
}

function buildReport({ dogs, duds, args }) {
  const all = [...dogs, ...duds];
  return {
    schema_version: 'gmgn_stage_stratified_audit.v1',
    generated_at: new Date().toISOString(),
    inputs: {
      dogs_file: args.dogs,
      duds_file: args.duds,
      raw_db: args.rawDb || null,
      paper_db: args.paperDb || null,
      dogs_n: dogs.length,
      duds_n: duds.length,
    },
    visibility: {
      dogs_by_stage: countBy(dogs, (row) => row.volume_visibility_stage),
      duds_by_stage: countBy(duds, (row) => row.volume_visibility_stage),
      lag_auc_dog_greater_than_dud: auc(
        fieldValues(dogs, 'volume_visibility_lag_sec'),
        fieldValues(duds, 'volume_visibility_lag_sec'),
      ),
      dog_lag_summary: summarizeNumbers(fieldValues(dogs, 'volume_visibility_lag_sec')),
      dud_lag_summary: summarizeNumbers(fieldValues(duds, 'volume_visibility_lag_sec')),
    },
    peak_labels: {
      dogs_by_peak_confirmation_tier: countBy(dogs, (row) => row.peak_confirmation_tier || 'unknown'),
      dark_peak_dogs: dogs
        .filter((row) => row.peak_confirmation_tier === 'curve_phase_unconfirmed')
        .map((row) => ({
          token_tail: String(row.token_ca || '').slice(-8),
          token_ca: row.token_ca,
          tier: row.raw_sustained_tier_from_db || row.tier || null,
          time_to_sustained_peak_sec: row.raw_time_to_sustained_peak_sec,
          first_nonzero_volume_lag_sec: row.volume_visibility_lag_sec,
        })),
    },
    decision_timing: {
      matched_dogs_n: dogs.filter((row) => row.decision_ts != null).length,
      matched_duds_n: duds.filter((row) => row.decision_ts != null).length,
      dog_decision_lag_summary: summarizeNumbers(fieldValues(dogs, 'decision_lag_sec')),
      dud_decision_lag_summary: summarizeNumbers(fieldValues(duds, 'decision_lag_sec')),
    },
    auc: {
      early_5m_volume_usd_sum: {
        feature_timing: 'post_anchor_0_to_5m_future_for_immediate_gate_valid_only_as_t_plus_5m_confirmation',
        full: auc(fieldValues(dogs, 'early_5m_volume_usd_sum'), fieldValues(duds, 'early_5m_volume_usd_sum')),
        positive_only: auc(
          fieldValues(dogs, 'early_5m_volume_usd_sum', { positiveOnly: true }),
          fieldValues(duds, 'early_5m_volume_usd_sum', { positiveOnly: true }),
        ),
        by_stage: stratifiedAuc(dogs, duds, 'early_5m_volume_usd_sum'),
      },
      early_15m_volume_usd_sum: {
        feature_timing: 'post_anchor_0_to_15m_future_for_immediate_gate_valid_only_as_t_plus_15m_confirmation',
        full: auc(fieldValues(dogs, 'early_15m_volume_usd_sum'), fieldValues(duds, 'early_15m_volume_usd_sum')),
        positive_only: auc(
          fieldValues(dogs, 'early_15m_volume_usd_sum', { positiveOnly: true }),
          fieldValues(duds, 'early_15m_volume_usd_sum', { positiveOnly: true }),
        ),
        by_stage: stratifiedAuc(dogs, duds, 'early_15m_volume_usd_sum'),
      },
      volume_usd_sum_120m_not_ex_ante: {
        feature_timing: 'post_anchor_120m_future_info_forbidden_for_entry_gate',
        full: auc(fieldValues(dogs, 'volume_usd_sum'), fieldValues(duds, 'volume_usd_sum')),
      },
    },
    interpretation: {
      primary_observation: 'volume visibility stage is a confounder; compare dog/dud inside the same stage and re-anchor features to decision_ts before gate experiments',
      next_actions: [
        'Generate decision_ts token files and re-run GMGN touch with decision_ts anchors',
        'Treat curve_phase_unconfirmed dogs as label-quality debt until Helius/pump.fun decoding confirms traded volume before peak',
        'Use stage-stratified dog-vs-dud, not full-sample early-volume AUC, for feature claims',
        'Do not use early_5m/early_15m volume as immediate-entry features; they are post-anchor windows and only valid for delayed confirmation or residual-upside replay',
      ],
    },
    samples: {
      dogs: dogs.slice(0, 10).map((row) => ({
        token_tail: String(row.token_ca || '').slice(-8),
        signal_ts: row.signal_ts,
        decision_ts: row.decision_ts,
        decision_lag_sec: row.decision_lag_sec,
        volume_visibility_stage: row.volume_visibility_stage,
        volume_visibility_lag_sec: row.volume_visibility_lag_sec,
        peak_confirmation_tier: row.peak_confirmation_tier,
        early_15m_volume_usd_sum: row.early_15m_volume_usd_sum,
      })),
      duds: duds.slice(0, 10).map((row) => ({
        token_tail: String(row.token_ca || '').slice(-8),
        signal_ts: row.signal_ts,
        decision_ts: row.decision_ts,
        decision_lag_sec: row.decision_lag_sec,
        volume_visibility_stage: row.volume_visibility_stage,
        volume_visibility_lag_sec: row.volume_visibility_lag_sec,
        early_15m_volume_usd_sum: row.early_15m_volume_usd_sum,
      })),
    },
  };
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.dogs || !args.duds) throw new Error('Provide --dogs and --duds');
  const baseDogs = loadResults(args.dogs);
  const baseDuds = loadResults(args.duds);
  const anchors = [...baseDogs, ...baseDuds];
  const peakLabels = loadPeakLabels(args.rawDb);
  const decisionMatches = loadDecisionMatches(args.paperDb, anchors, {
    decisionWindowSec: Number(args.decisionWindowSec) || 900,
  });
  const dogs = enrichRows(baseDogs, { peakLabels, decisionMatches });
  const duds = enrichRows(baseDuds, { peakLabels, decisionMatches });
  const report = buildReport({ dogs, duds, args });
  report.decision_token_files = writeDecisionTokenFiles(args.decisionTokenOutDir, dogs, duds);
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({
    out: args.out,
    dogs_n: dogs.length,
    duds_n: duds.length,
    visibility: report.visibility,
    peak_labels: report.peak_labels.dogs_by_peak_confirmation_tier,
    decision_timing: report.decision_timing,
    auc: {
      early_15m_full: report.auc.early_15m_volume_usd_sum.full,
      early_15m_positive_only: report.auc.early_15m_volume_usd_sum.positive_only,
      lag: report.visibility.lag_auc_dog_greater_than_dud,
    },
    decision_token_files: report.decision_token_files,
  }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}

export {
  auc,
  buildReport,
  enrichRows,
  summarizeNumbers,
  volumeVisibilityStage,
};
