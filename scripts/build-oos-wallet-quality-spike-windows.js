#!/usr/bin/env node
'use strict';

/**
 * Builds a LABEL-STRIPPED window list for the smart_money_buy_share feasibility
 * spike. The output intentionally contains no dog/dud label and no outcome/tier
 * fields, so downstream extraction cannot compute separation by construction.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const DEFAULT_TEMPLATE = path.join(path.dirname(new URL(import.meta.url).pathname), 'oos-wallet-quality-spike.template.sql');

function readRows(p) {
  const raw = fs.readFileSync(p, 'utf8').trim();
  if (!raw) return [];
  if (p.endsWith('.jsonl')) return raw.split('\n').filter(Boolean).map((line) => JSON.parse(line));
  const parsed = JSON.parse(raw);
  return Array.isArray(parsed) ? parsed : (parsed.rows || parsed.feature_rows || []);
}

function sha256File(p) {
  return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
}

function csvEscape(v) {
  const s = String(v ?? '');
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function sqlString(s) {
  return `'${String(s).replace(/'/g, "''")}'`;
}

function signalKey(row) {
  return `${row.token_ca}|${Math.floor(Number(row.signal_ts))}`;
}

function parseArgs(argv) {
  const a = { limit: 20, order: 'newest', preSec: 900, historyDays: 365, template: DEFAULT_TEMPLATE };
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i]; const v = argv[i + 1];
    if (k === '--features') { a.features = v; i += 1; }
    else if (k === '--out-csv') { a.outCsv = v; i += 1; }
    else if (k === '--out-values-sql') { a.outValuesSql = v; i += 1; }
    else if (k === '--out-sql') { a.outSql = v; i += 1; }
    else if (k === '--out-manifest') { a.outManifest = v; i += 1; }
    else if (k === '--limit') { a.limit = Number(v); i += 1; }
    else if (k === '--order') { a.order = v; i += 1; }
    else if (k === '--pre-sec') { a.preSec = Number(v); i += 1; }
    else if (k === '--history-days') { a.historyDays = Number(v); i += 1; }
    else if (k === '--template') { a.template = v; i += 1; }
    else if (k === '--help' || k === '-h') { a.help = true; }
    else throw new Error(`Unknown argument: ${k}`);
  }
  return a;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/build-oos-wallet-quality-spike-windows.js \\',
    '    --features cumulative_oos_features.jsonl --limit 20 \\',
    '    --out-csv signal_windows.csv --out-values-sql signal_windows_values.sql \\',
    '    --out-sql wallet-quality-spike.sql --out-manifest manifest.json',
    '',
    'Outputs are label-stripped. Do not add label/tier/outcome fields.',
  ].join('\n');
}

function buildWindows(rows, { limit, order, preSec, historyDays }) {
  const byKey = new Map();
  for (const row of rows) {
    if (!row.token_ca || row.signal_ts == null) continue;
    const minimal = {
      token_ca: String(row.token_ca),
      signal_ts: Math.floor(Number(row.signal_ts)),
    };
    if (!Number.isFinite(minimal.signal_ts)) continue;
    byKey.set(signalKey(minimal), minimal);
  }
  let out = [...byKey.values()];
  out.sort((a, b) => {
    if (a.signal_ts !== b.signal_ts) return order === 'oldest' ? a.signal_ts - b.signal_ts : b.signal_ts - a.signal_ts;
    return a.token_ca < b.token_ca ? -1 : a.token_ca > b.token_ca ? 1 : 0;
  });
  out = out.slice(0, limit).map((row, i) => ({
    window_id: `w${String(i + 1).padStart(4, '0')}`,
    token_ca: row.token_ca,
    signal_ts: row.signal_ts,
    window_start_ts: row.signal_ts - preSec,
    window_end_ts: row.signal_ts,
    history_start_ts: row.signal_ts - (historyDays * 86400),
  }));
  return out;
}

function writeCsv(p, rows) {
  const cols = ['window_id', 'token_ca', 'signal_ts', 'window_start_ts', 'window_end_ts', 'history_start_ts'];
  fs.mkdirSync(path.dirname(path.resolve(p)), { recursive: true });
  fs.writeFileSync(p, `${cols.join(',')}\n${rows.map((r) => cols.map((c) => csvEscape(r[c])).join(',')).join('\n')}${rows.length ? '\n' : ''}`);
}

function valuesSql(rows) {
  return rows.map((r) => [
    sqlString(r.window_id),
    sqlString(r.token_ca),
    r.signal_ts,
    r.window_start_ts,
    r.window_end_ts,
    r.history_start_ts,
  ].join(', ')).map((line) => `  (${line})`).join(',\n');
}

function main() {
  const a = parseArgs(process.argv);
  if (a.help || !a.features || !a.outCsv || !a.outValuesSql || !a.outManifest) {
    console.log(usage());
    process.exit(a.help ? 0 : 2);
  }
  if (!['newest', 'oldest'].includes(a.order)) throw new Error('--order must be newest|oldest');
  if (!Number.isFinite(a.limit) || a.limit <= 0 || a.limit > 500) throw new Error('--limit must be 1..500');
  if (!Number.isFinite(a.preSec) || a.preSec <= 0) throw new Error('--pre-sec must be positive');
  if (!Number.isFinite(a.historyDays) || a.historyDays <= 0) throw new Error('--history-days must be positive');

  const inputRows = readRows(a.features);
  const windows = buildWindows(inputRows, a);
  writeCsv(a.outCsv, windows);
  fs.mkdirSync(path.dirname(path.resolve(a.outValuesSql)), { recursive: true });
  fs.writeFileSync(a.outValuesSql, `${valuesSql(windows)}\n`);
  if (a.outSql) {
    const tpl = fs.readFileSync(a.template, 'utf8');
    fs.mkdirSync(path.dirname(path.resolve(a.outSql)), { recursive: true });
    fs.writeFileSync(a.outSql, tpl.replace('{{SIGNAL_WINDOWS_VALUES}}', valuesSql(windows)));
  }
  const manifest = {
    schema_version: 'oos_wallet_quality_spike_windows.v0',
    generated_at: new Date().toISOString(),
    input_features: a.features,
    input_features_sha256: sha256File(a.features),
    windows_csv: a.outCsv,
    values_sql: a.outValuesSql,
    rendered_sql: a.outSql || null,
    label_stripped: true,
    forbidden_fields: ['label', 'tier', 'effective_tier', 'auc'],
    selection: { limit: a.limit, order: a.order, pre_sec: a.preSec, history_days: a.historyDays },
    windows_n: windows.length,
    min_signal_ts: windows.length ? Math.min(...windows.map((r) => r.signal_ts)) : null,
    max_signal_ts: windows.length ? Math.max(...windows.map((r) => r.signal_ts)) : null,
  };
  fs.mkdirSync(path.dirname(path.resolve(a.outManifest)), { recursive: true });
  fs.writeFileSync(a.outManifest, `${JSON.stringify(manifest, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, windows_n: windows.length, out_csv: a.outCsv, out_sql: a.outSql || null }, null, 2));
}

if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  try { main(); } catch (err) { console.error(`FAIL_CLOSED: ${err.message}`); process.exit(1); }
}

export { buildWindows, valuesSql };
