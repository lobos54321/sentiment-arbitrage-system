#!/usr/bin/env node
'use strict';

/**
 * Validates a label-free smart-money wallet-quality feasibility spike result.
 * This tool intentionally reports coverage/availability/cost only. It refuses
 * label/AUC/separation fields.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { parse as parseCsv } from 'csv-parse/sync';

const LOOKBACKS = ['7d', '14d', '30d', 'all'];
const QUALIFY_KS = [1, 3, 5];
const MAX_LOOKBACK_DAYS = 365;

const REQUIRED_FIELDS = [
  'window_id', 'token_ca', 'signal_ts', 'window_complete',
  'n_buyers', 'buy_sol_total', 'n_buyers_with_prior_history',
  ...LOOKBACKS.flatMap((lb) => QUALIFY_KS.map((k) => `n_buyers_qualify_k${k}_${lb}`)),
  ...LOOKBACKS.map((lb) => `buy_sol_from_qualify_k3_${lb}`),
  ...LOOKBACKS.flatMap((lb) => QUALIFY_KS.map((k) => `n_buyers_qualify_k${k}_${lb}_nonsniper`)),
  ...LOOKBACKS.map((lb) => `buy_sol_from_qualify_k3_${lb}_nonsniper`),
  'n_buyers_creator_excluded', 'n_buyers_first_block_sniper', 'n_buyers_prior_sniper', 'n_buyers_any_sniper_proxy',
  'prior_trades_per_buyer_median', 'prior_trades_per_buyer_p90',
  'prior_snipe_rate_buyer_median', 'prior_snipe_rate_buyer_p90',
  'asof_ok',
];
const FORBIDDEN_KEYS = ['label', 'tier', 'effective_tier', 'auc', 'dog', 'dud'];

function sha256File(p) {
  return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
}

function readRows(p) {
  const raw = fs.readFileSync(p, 'utf8').trim();
  if (!raw) return [];
  if (p.endsWith('.csv')) return parseCsv(raw, { columns: true, skip_empty_lines: true, trim: true, bom: true });
  if (p.endsWith('.jsonl')) return raw.split('\n').filter(Boolean).map((line) => JSON.parse(line));
  const parsed = JSON.parse(raw);
  return Array.isArray(parsed) ? parsed : (parsed.rows || parsed.results || []);
}

function readJsonMaybe(p) {
  if (!p || !fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function hasForbiddenKey(obj, forbidden = FORBIDDEN_KEYS) {
  if (!obj || typeof obj !== 'object') return null;
  for (const k of Object.keys(obj)) {
    if (forbidden.includes(k)) return k;
    const nested = hasForbiddenKey(obj[k], forbidden);
    if (nested) return nested;
  }
  return null;
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function bool(v) {
  if (v === true || v === 'true' || v === 'TRUE' || v === 1 || v === '1') return true;
  if (v === false || v === 'false' || v === 'FALSE' || v === 0 || v === '0') return false;
  return null;
}

function frac(n, d) {
  return d > 0 ? Number((n / d).toFixed(6)) : null;
}

function historyDaysFromManifest(windowsManifest) {
  const n = Number(windowsManifest?.selection?.history_days);
  return Number.isFinite(n) ? n : null;
}

function summarize(rows, manifest = null, windowsManifest = null) {
  const windows = rows.length;
  const missingRequired = {};
  for (const f of REQUIRED_FIELDS) missingRequired[f] = rows.filter((r) => r[f] === undefined || r[f] === null || r[f] === '').length;
  const forbidden = hasForbiddenKey(rows);
  const asofViolations = rows.filter((r) => {
    const ok = bool(r.asof_ok);
    const sig = num(r.signal_ts);
    const maxPrior = num(r.max_prior_bt);
    return ok === false || (maxPrior !== null && sig !== null && maxPrior >= sig);
  });
  const windowsWithBuyers = rows.filter((r) => (num(r.n_buyers) || 0) > 0).length;
  const complete = rows.filter((r) => bool(r.window_complete) === true).length;
  const buyersTotal = rows.reduce((s, r) => s + (num(r.n_buyers) || 0), 0);
  const noHistoryBuyers = rows.reduce((s, r) => s + Math.max(0, (num(r.n_buyers) || 0) - (num(r.n_buyers_with_prior_history) || 0)), 0);
  const availability = {};
  for (const lb of LOOKBACKS) {
    availability[lb] = {};
    for (const k of QUALIFY_KS) {
      const field = `n_buyers_qualify_k${k}_${lb}`;
      const nonsniperField = `n_buyers_qualify_k${k}_${lb}_nonsniper`;
      availability[lb][`frac_window_ge1_k${k}`] = frac(rows.filter((r) => (num(r[field]) || 0) > 0).length, windows);
      availability[lb][`frac_window_ge1_k${k}_nonsniper`] = frac(rows.filter((r) => (num(r[nonsniperField]) || 0) > 0).length, windows);
    }
    const solField = `buy_sol_from_qualify_k3_${lb}`;
    const solNonsniperField = `buy_sol_from_qualify_k3_${lb}_nonsniper`;
    availability[lb].buy_sol_from_qualify_k3_total = Number(rows.reduce((s, r) => s + (num(r[solField]) || 0), 0).toFixed(9));
    availability[lb].buy_sol_from_qualify_k3_nonsniper_total = Number(rows.reduce((s, r) => s + (num(r[solNonsniperField]) || 0), 0).toFixed(9));
  }
  const historyDays = historyDaysFromManifest(windowsManifest);
  const lookbackOk = historyDays !== null && historyDays >= MAX_LOOKBACK_DAYS;
  return {
    schema_version: 'oos_wallet_quality_spike_validation.v0',
    generated_at: new Date().toISOString(),
    verdict: forbidden || Object.values(missingRequired).some((n) => n > 0) || asofViolations.length || !lookbackOk
      ? 'SPIKE_QA_FAIL_FIX_PIPELINE'
      : 'SPIKE_QA_PASS_NO_EDGE_CLAIM',
    forbidden_key_found: forbidden,
    windows_total: windows,
    windows_complete: complete,
    windows_with_ge1_buyer: windowsWithBuyers,
    availability_frac_window_ge1_qualify: availability,
    missingness: {
      frac_windows_0_buyers: frac(windows - windowsWithBuyers, windows),
      frac_buyers_no_prior_history: frac(noHistoryBuyers, buyersTotal),
      buyers_total: buyersTotal,
      buyers_no_prior_history: noHistoryBuyers,
      missing_required_fields: missingRequired,
    },
    asof_integrity_violations: asofViolations.length,
    asof_integrity_examples: asofViolations.slice(0, 5).map((r) => ({ window_id: r.window_id, token_ca: r.token_ca, signal_ts: r.signal_ts, max_prior_bt: r.max_prior_bt, asof_ok: r.asof_ok })),
    lookback_guard: {
      ok: lookbackOk,
      history_days: historyDays,
      required_history_days: MAX_LOOKBACK_DAYS,
      reason: lookbackOk ? null : 'history_days_missing_or_less_than_max_lookback',
    },
    runtime: {
      dune_execution_id: manifest?.execution_id || null,
      dune_row_count: manifest?.row_count ?? rows.length,
      performance_tier: manifest?.performance || null,
      final_state: manifest?.final_status?.state || manifest?.final_status?.status || null,
      query_runtime_millis: manifest?.final_status?.query_execution_ms
        || manifest?.final_status?.execution_time_millis
        || manifest?.final_status?.result_metadata?.execution_time_millis
        || null,
      execution_cost_credits: manifest?.final_status?.execution_cost_credits ?? null,
    },
    forbidden_outputs: {
      no_auc: forbidden !== 'auc',
      no_label: forbidden !== 'label',
      no_dog_dud: forbidden !== 'dog' && forbidden !== 'dud',
      note: 'This report is coverage/availability only. It must not be used as an edge/separation result.',
    },
  };
}

function parseArgs(argv) {
  const a = {};
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i]; const v = argv[i + 1];
    if (k === '--rows') { a.rows = v; i += 1; }
    else if (k === '--dune-manifest') { a.duneManifest = v; i += 1; }
    else if (k === '--windows-manifest') { a.windowsManifest = v; i += 1; }
    else if (k === '--out') { a.out = v; i += 1; }
    else if (k === '--help' || k === '-h') { a.help = true; }
    else throw new Error(`Unknown argument: ${k}`);
  }
  return a;
}

function main() {
  const a = parseArgs(process.argv);
  if (a.help || !a.rows || !a.out) {
    console.log('usage: validate-oos-wallet-quality-spike.js --rows spike-results.jsonl --windows-manifest windows-manifest.json --out validation.json [--dune-manifest manifest.json]');
    process.exit(a.help ? 0 : 2);
  }
  if (!a.windowsManifest) throw new Error('--windows-manifest is required so history_days cannot be silently under-scoped');
  const rows = readRows(a.rows);
  const manifest = readJsonMaybe(a.duneManifest);
  const windowsManifest = readJsonMaybe(a.windowsManifest);
  const report = summarize(rows, manifest, windowsManifest);
  report.inputs = {
    rows: a.rows,
    rows_sha256: sha256File(a.rows),
    windows_manifest: a.windowsManifest,
    windows_manifest_sha256: sha256File(a.windowsManifest),
    dune_manifest: a.duneManifest || null,
    dune_manifest_sha256: a.duneManifest ? sha256File(a.duneManifest) : null,
  };
  fs.mkdirSync(path.dirname(path.resolve(a.out)), { recursive: true });
  fs.writeFileSync(a.out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({
    ok: report.verdict === 'SPIKE_QA_PASS_NO_EDGE_CLAIM',
    verdict: report.verdict,
    windows_total: report.windows_total,
    asof_integrity_violations: report.asof_integrity_violations,
    forbidden_key_found: report.forbidden_key_found,
    out: a.out,
  }, null, 2));
  if (report.verdict !== 'SPIKE_QA_PASS_NO_EDGE_CLAIM') process.exit(1);
}

if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  try { main(); } catch (err) { console.error(`FAIL_CLOSED: ${err.message}`); process.exit(1); }
}

export { summarize, hasForbiddenKey };
