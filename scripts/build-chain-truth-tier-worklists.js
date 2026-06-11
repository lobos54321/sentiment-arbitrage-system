#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    worklist: '',
    rawDb: process.env.RAW_SIGNAL_OUTCOMES_DB || '',
    outDir: './data/audits/chain-truth-tiers',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--worklist') { args.worklist = next; i += 1; continue; }
    if (key === '--raw-db') { args.rawDb = next; i += 1; continue; }
    if (key === '--out-dir') { args.outDir = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/build-chain-truth-tier-worklists.js --worklist chain-truth-worklist-v2.txt --raw-db raw_signal_outcomes.db --out-dir out',
  ].join('\n');
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function readWorklist(filePath) {
  return fs.readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'))
    .map((line) => {
      const [token, ts, cohort = 'unknown', need = '', stage = ''] = line.split('|').map((part) => part.trim());
      return {
        token_ca: token,
        anchor_ts: Math.floor(Number(ts)),
        cohort,
        chain_truth_need: need,
        visibility_stage: stage,
      };
    })
    .filter((row) => row.token_ca && Number.isFinite(row.anchor_ts));
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

function loadPeakDelayMap(Database, rawDbPath) {
  const out = new Map();
  if (!rawDbPath || !fs.existsSync(rawDbPath)) return out;
  const db = openReadonlySqlite(Database, rawDbPath);
  try {
    if (!tableExists(db, 'raw_signal_outcomes')) return out;
    const cols = tableColumns(db, 'raw_signal_outcomes');
    if (!cols.has('token_ca') || !cols.has('signal_ts')) return out;
    const delayCol = firstExisting(cols, ['time_to_sustained_peak_sec', 'time_to_peak_sec']);
    if (!delayCol) return out;
    const rows = db.prepare(`
      SELECT token_ca, signal_ts, ${delayCol} AS peak_delay_sec
      FROM raw_signal_outcomes
      WHERE token_ca IS NOT NULL
        AND signal_ts IS NOT NULL
        AND ${delayCol} IS NOT NULL
    `).all();
    for (const row of rows) {
      const delay = numeric(row.peak_delay_sec);
      if (delay == null) continue;
      out.set(`${row.token_ca}:${Math.floor(Number(row.signal_ts))}`, Math.max(0, Math.floor(delay)));
    }
  } finally {
    try { db.close(); } catch {}
  }
  return out;
}

function worklistLine(row) {
  return [row.token_ca, row.anchor_ts, row.cohort, row.chain_truth_need, row.visibility_stage].join('|');
}

function isBaselineRow(row = {}) {
  return row.chain_truth_need === 'baseline_reconstruction';
}

function isNativePathRow(row = {}) {
  return ['native_path_reconstruction', 'native_path_reconstruction_for_missing_peak'].includes(row.chain_truth_need);
}

function isPollutedPeakRow(row = {}) {
  return row.chain_truth_need === 'polluted_peak_window_adjudication';
}

export function buildPeakRows(worklistRows = [], peakDelayByTokenSignal = new Map()) {
  const out = [];
  for (const row of worklistRows) {
    if (row.cohort !== 'quarantine') continue;
    if (!isPollutedPeakRow(row)) continue;
    const delay = peakDelayByTokenSignal.get(`${row.token_ca}:${row.anchor_ts}`);
    if (delay == null) continue;
    out.push({
      ...row,
      anchor_ts: row.anchor_ts + delay,
      chain_truth_need: 'polluted_peak_window_adjudication',
      visibility_stage: 'peak_window',
    });
  }
  return out.sort((a, b) => a.anchor_ts - b.anchor_ts || a.token_ca.localeCompare(b.token_ca));
}

export function splitTierRows(worklistRows = [], peakDelayByTokenSignal = new Map()) {
  const baselineRows = worklistRows
    .filter(isBaselineRow)
    .sort((a, b) => a.anchor_ts - b.anchor_ts || a.token_ca.localeCompare(b.token_ca));
  const nativePathRows = worklistRows
    .filter(isNativePathRow)
    .sort((a, b) => a.anchor_ts - b.anchor_ts || a.token_ca.localeCompare(b.token_ca));
  const anchorRows = [...baselineRows, ...nativePathRows]
    .sort((a, b) => a.anchor_ts - b.anchor_ts || a.token_ca.localeCompare(b.token_ca));
  const peakRows = buildPeakRows(worklistRows, peakDelayByTokenSignal);
  return {
    baselineRows,
    nativePathRows,
    anchorRows,
    peakRows,
  };
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.worklist) throw new Error('Provide --worklist');
  const worklist = readWorklist(args.worklist);
  const Database = args.rawDb ? await loadDatabase() : null;
  const tiers = splitTierRows(worklist, Database ? loadPeakDelayMap(Database, args.rawDb) : new Map());
  fs.mkdirSync(args.outDir, { recursive: true });
  const anchorOut = path.join(args.outDir, 'tier1-anchor-worklist-v2.txt');
  const baselineOut = path.join(args.outDir, 'tier1-baseline-worklist-v2.txt');
  const peakOut = path.join(args.outDir, 'tier1-peak-worklist-v2.txt');
  const nativePathOut = path.join(args.outDir, 'tier1-native-path-worklist-v2.txt');
  fs.writeFileSync(anchorOut, `${tiers.anchorRows.map(worklistLine).join('\n')}${tiers.anchorRows.length ? '\n' : ''}`);
  fs.writeFileSync(baselineOut, `${tiers.baselineRows.map(worklistLine).join('\n')}${tiers.baselineRows.length ? '\n' : ''}`);
  fs.writeFileSync(peakOut, `${tiers.peakRows.map(worklistLine).join('\n')}${tiers.peakRows.length ? '\n' : ''}`);
  fs.writeFileSync(nativePathOut, `${tiers.nativePathRows.map(worklistLine).join('\n')}${tiers.nativePathRows.length ? '\n' : ''}`);
  console.log(JSON.stringify({
    anchor_out: anchorOut,
    baseline_out: baselineOut,
    peak_out: peakOut,
    native_path_out: nativePathOut,
    anchor_rows_n: tiers.anchorRows.length,
    baseline_rows_n: tiers.baselineRows.length,
    peak_rows_n: tiers.peakRows.length,
    native_path_rows_n: tiers.nativePathRows.length,
  }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}
