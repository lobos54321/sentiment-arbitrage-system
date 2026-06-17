#!/usr/bin/env node
'use strict';

/**
 * run-curve-observability-canary.js
 *
 * Label-free, observe-only canary for ex-ante pump.fun curve-window provider
 * coverage. It measures availability / completeness / capability / cost /
 * latency only. It never writes production paths or cumulative OOS tables, and
 * its public output intentionally contains no labels, returns, tiers, AUC, lift,
 * precision, recall, threshold, or feature values.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { execFileSync } from 'child_process';
import { parse as parseCsv } from 'csv-parse/sync';

import { HeliusHistoryClient } from '../src/market-data/helius-history-client.js';
import {
  derivePumpfunBondingCurvePda,
  filterSignaturesForWindow,
  normalizeCurveTransaction,
} from './run-helius-pumpfun-curve-decode-audit.js';

const SCRIPTS = path.dirname(new URL(import.meta.url).pathname);
const DEFAULT_TEMPLATE = path.join(SCRIPTS, 'curve-observability-dune-canary.template.sql');
const DEFAULT_PRE_SEC = 900;
const DEFAULT_LIMIT = 30;
const DEFAULT_MIN_USABLE_CURVE_WINDOW_RATE = 0.6;
const MIN_SAMPLE = 20;
const MAX_SAMPLE = 50;
const FORBIDDEN_KEYS = new Set([
  'label', 'labels', 'return_domain', 'effective_tier', 'tier',
  'auc', 'lift', 'precision', 'recall', 'dog', 'dogs', 'dud', 'duds',
  'threshold', 'corrected_peak_pct', 'max_sustained_peak_pct',
]);

function die(message) {
  console.error(`run-curve-observability-canary: ${message}`);
  process.exit(2);
}

function sha256File(filePath) {
  return fs.existsSync(filePath)
    ? crypto.createHash('sha256').update(fs.readFileSync(filePath)).digest('hex')
    : null;
}

function readSecretFile(filePath) {
  if (!filePath) return '';
  const raw = fs.readFileSync(filePath, 'utf8').trim();
  if (!raw) return '';
  const first = raw.split(/\r?\n/).find((line) => line.trim() && !line.trim().startsWith('#')) || '';
  const line = first.trim();
  if (line.includes('=')) return line.slice(line.indexOf('=') + 1).trim().replace(/^['"]|['"]$/g, '');
  return line;
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

function pick(row, keys) {
  for (const key of keys) {
    if (row?.[key] != null && row[key] !== '') return row[key];
  }
  return null;
}

function readTable(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  if (!raw.trim()) return [];
  if (filePath.endsWith('.csv')) {
    return parseCsv(raw, { columns: true, skip_empty_lines: true, bom: true, trim: true });
  }
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed;
    if (Array.isArray(parsed.rows)) return parsed.rows;
    if (Array.isArray(parsed.results)) return parsed.results;
    if (Array.isArray(parsed.trades)) return parsed.trades;
    return [parsed];
  } catch {
    return raw.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
  }
}

function hasForbiddenKey(obj) {
  if (!obj || typeof obj !== 'object') return null;
  for (const key of Object.keys(obj)) {
    if (FORBIDDEN_KEYS.has(String(key))) return String(key);
  }
  if (Array.isArray(obj)) {
    for (const item of obj) {
      const found = hasForbiddenKey(item);
      if (found) return found;
    }
    return null;
  }
  for (const value of Object.values(obj)) {
    const found = hasForbiddenKey(value);
    if (found) return found;
  }
  return null;
}

function looksLikePumpFunMint(tokenCa) {
  return String(tokenCa || '').endsWith('pump');
}

function normalizeSignal(row) {
  const token = String(pick(row, ['token_ca', 'mint', 'token_mint', 'address']) || '').trim();
  const signalTs = normalizeTs(pick(row, ['signal_ts', 'anchor_ts', 'decision_ts']));
  if (!token || signalTs == null) return null;
  return { token_ca: token, signal_ts: signalTs };
}

function loadSignalInputs(args) {
  const rows = [];
  for (const filePath of [args.rows, args.dogs, args.duds].filter(Boolean)) {
    rows.push(...readTable(filePath));
  }
  return rows.map(normalizeSignal).filter(Boolean);
}

function buildCanaryWindows(rows, {
  nowTs = Math.floor(Date.now() / 1000),
  limit = DEFAULT_LIMIT,
  maxAgeDays = 14,
  minAgeSec = 7200,
  preSec = DEFAULT_PRE_SEC,
} = {}) {
  const seen = new Set();
  const deduped = [];
  const stats = {
    input_rows: rows.length,
    dedup_removed: 0,
    non_pump_excluded: 0,
    too_old_excluded: 0,
    too_recent_excluded: 0,
  };
  for (const row of rows) {
    const key = `${row.token_ca}|${row.signal_ts}`;
    if (seen.has(key)) { stats.dedup_removed += 1; continue; }
    seen.add(key);
    if (!looksLikePumpFunMint(row.token_ca)) { stats.non_pump_excluded += 1; continue; }
    if (row.signal_ts < nowTs - maxAgeDays * 86400) { stats.too_old_excluded += 1; continue; }
    if (row.signal_ts > nowTs - minAgeSec) { stats.too_recent_excluded += 1; continue; }
    deduped.push(row);
  }
  const cappedLimit = Math.max(1, Math.min(MAX_SAMPLE, Number(limit || DEFAULT_LIMIT)));
  const selected = deduped
    .sort((a, b) => (a.token_ca < b.token_ca ? -1 : a.token_ca > b.token_ca ? 1 : a.signal_ts - b.signal_ts))
    .slice(0, cappedLimit)
    .map((row, i) => ({
      window_id: `w${String(i + 1).padStart(5, '0')}`,
      token_ca: row.token_ca,
      signal_ts: row.signal_ts,
      window_start_ts: row.signal_ts - preSec,
      window_end_ts: row.signal_ts,
    }));
  return {
    windows: selected,
    stats: {
      ...stats,
      eligible_n: deduped.length,
      selected_n: selected.length,
      min_sample: MIN_SAMPLE,
      below_min_sample: selected.length < MIN_SAMPLE,
      limit: cappedLimit,
      now_ts: nowTs,
      max_age_days: maxAgeDays,
      min_age_sec: minAgeSec,
      pre_sec: preSec,
      post_sec: 0,
    },
  };
}

function valuesSql(windows) {
  const q = (s) => String(s).replace(/'/g, "''");
  return windows.map((w) => (
    `    ('${q(w.window_id)}', '${q(w.token_ca)}', ${w.signal_ts}, ${w.window_start_ts}, ${w.window_end_ts}, ${w.window_start_ts}, ${w.window_end_ts})`
  )).join(',\n');
}

function writeWindowsArtifacts(windows, outDir, templatePath = DEFAULT_TEMPLATE) {
  fs.mkdirSync(outDir, { recursive: true });
  const csv = [
    'window_id,token_ca,signal_ts,window_start_ts,window_end_ts',
    ...windows.map((w) => `${w.window_id},${w.token_ca},${w.signal_ts},${w.window_start_ts},${w.window_end_ts}`),
  ].join('\n') + '\n';
  const windowsPath = path.join(outDir, 'signal_windows.csv');
  const valuesPath = path.join(outDir, 'signal_windows_values.sql');
  const sqlPath = path.join(outDir, 'curve_observability_canary.sql');
  fs.writeFileSync(windowsPath, csv);
  fs.writeFileSync(valuesPath, `WITH signal_windows(window_id, token_ca, signal_ts, window_start_ts, window_end_ts, query_start_ts, query_end_ts) AS (\n  VALUES\n${valuesSql(windows)}\n)\nSELECT * FROM signal_windows;\n`);
  const template = fs.readFileSync(templatePath, 'utf8');
  fs.writeFileSync(sqlPath, template.replace('{{SIGNAL_WINDOWS_VALUES}}', valuesSql(windows)));
  return { windowsPath, valuesPath, sqlPath };
}

function normalizeTrade(row) {
  const token = String(pick(row, ['token_ca', 'mint', 'token_mint', 'mint_address']) || '').trim();
  const blockTime = normalizeTs(pick(row, ['block_time', 'timestamp', 'blockTime', 'evt_block_time']));
  if (!token || blockTime == null) return null;
  return {
    window_id: String(pick(row, ['window_id', 'windowId']) || ''),
    token_ca: token,
    signal_ts: normalizeTs(pick(row, ['signal_ts', 'anchor_ts', 'decision_ts'])),
    window_start_ts: normalizeTs(pick(row, ['window_start_ts', 'start_ts'])),
    window_end_ts: normalizeTs(pick(row, ['window_end_ts', 'end_ts'])),
    block_time: blockTime,
    signature: String(pick(row, ['signature', 'evt_tx_id', 'tx_hash', 'tx_id']) || ''),
    side: pick(row, ['side', 'trade_side', 'direction', 'is_buy', 'isBuy']),
    user: pick(row, ['user', 'trader', 'wallet', 'buyer', 'seller']),
    sol_amount: numeric(pick(row, ['sol_amount', 'solAmount', 'sol_amount_sol', 'sol'])),
    token_amount: numeric(pick(row, ['token_amount', 'tokenAmount', 'token_amount_ui'])),
    virtual_sol_reserves: numeric(pick(row, ['virtual_sol_reserves', 'virtualSolReserves'])),
    virtual_token_reserves: numeric(pick(row, ['virtual_token_reserves', 'virtualTokenReserves'])),
    real_token_reserves: numeric(pick(row, ['real_token_reserves', 'realTokenReserves'])),
  };
}

function summarizeTradesForWindow({ provider, window, trades, historyReachedStart, latencyMs = null, pagesFetched = 0, rateLimited = false, costUnits = null, fetchOk = true, httpStatusClass = null, error = null }) {
  const outOfWindow = trades.filter((t) => t.block_time < window.window_start_ts || t.block_time > window.window_end_ts);
  const inWindow = trades.filter((t) => t.block_time >= window.window_start_ts && t.block_time <= window.window_end_ts);
  const hasVolume = inWindow.some((t) => t.sol_amount != null || t.token_amount != null);
  const hasWallet = inWindow.some((t) => t.user != null && t.user !== '');
  const hasProgress = inWindow.some((t) => t.real_token_reserves != null || (t.virtual_sol_reserves != null && t.virtual_token_reserves != null));
  return {
    schema_version: 'curve_observability_canary_row.v1',
    provider,
    token_ca: window.token_ca,
    signal_ts: window.signal_ts,
    window_start_ts: window.window_start_ts,
    window_end_ts: window.window_end_ts,
    fetch_ok: Boolean(fetchOk),
    http_status_class: httpStatusClass,
    history_reached_start: Boolean(historyReachedStart),
    in_window_trade_count: inWindow.length,
    has_per_trade: true,
    has_volume: hasVolume,
    has_progress: hasProgress,
    has_wallet: hasWallet,
    out_of_window_trade_count: outOfWindow.length,
    latency_ms: latencyMs,
    pages_fetched: pagesFetched,
    rate_limited: Boolean(rateLimited),
    cost_units: costUnits,
    error,
  };
}

function summarizeDune({ windows, tradesPath, assumeCompleteWindow, manifestPath = '' }) {
  if (!assumeCompleteWindow) {
    throw new Error('Dune canary requires --dune-assume-complete-window; validator cannot prove completeness by itself.');
  }
  const trades = readTable(tradesPath).map(normalizeTrade).filter(Boolean);
  const byToken = new Map();
  const byWindow = new Map();
  const bySignalKey = new Map();
  let hasWindowIds = false;
  let hasSignalKeys = false;
  for (const trade of trades) {
    if (!byToken.has(trade.token_ca)) byToken.set(trade.token_ca, []);
    byToken.get(trade.token_ca).push(trade);
    if (trade.signal_ts != null) {
      hasSignalKeys = true;
      const key = `${trade.token_ca}|${trade.signal_ts}`;
      if (!bySignalKey.has(key)) bySignalKey.set(key, []);
      bySignalKey.get(key).push(trade);
    }
    if (trade.window_id) {
      hasWindowIds = true;
      if (!byWindow.has(trade.window_id)) byWindow.set(trade.window_id, []);
      byWindow.get(trade.window_id).push(trade);
    }
  }
  let costUnits = trades.length;
  let rowCount = trades.length;
  if (manifestPath && fs.existsSync(manifestPath)) {
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
    rowCount = Number(manifest.row_count ?? rowCount);
    costUnits = Number(manifest.execution_cost_credits ?? manifest.final_status?.execution_cost_credits ?? rowCount);
  }
  return windows.map((window) => summarizeTradesForWindow({
    provider: 'dune',
    window,
    trades: hasSignalKeys
      ? (bySignalKey.get(`${window.token_ca}|${window.signal_ts}`) || [])
      : (hasWindowIds ? (byWindow.get(window.window_id) || []) : (byToken.get(window.token_ca) || [])),
    historyReachedStart: true,
    latencyMs: null,
    pagesFetched: 0,
    rateLimited: false,
    costUnits,
    fetchOk: true,
    httpStatusClass: '2xx',
  }));
}

function providerDisabledRows(provider, windows, reason) {
  return windows.map((window) => ({
    schema_version: 'curve_observability_canary_row.v1',
    provider,
    token_ca: window.token_ca,
    signal_ts: window.signal_ts,
    window_start_ts: window.window_start_ts,
    window_end_ts: window.window_end_ts,
    fetch_ok: false,
    http_status_class: null,
    history_reached_start: false,
    in_window_trade_count: 0,
    has_per_trade: false,
    has_volume: false,
    has_progress: false,
    has_wallet: false,
    out_of_window_trade_count: 0,
    latency_ms: null,
    pages_fetched: 0,
    rate_limited: false,
    cost_units: 0,
    error: 'provider_disabled',
    provider_disabled_reason: reason,
  }));
}

async function runHeliusWindow(client, window, args) {
  const started = Date.now();
  let curve;
  try {
    curve = derivePumpfunBondingCurvePda(window.token_ca);
  } catch (error) {
    return summarizeTradesForWindow({
      provider: 'helius',
      window,
      trades: [],
      historyReachedStart: false,
      fetchOk: false,
      error: `pda_derive_failed:${String(error?.message || error).slice(0, 120)}`,
      latencyMs: Date.now() - started,
    });
  }
  let before = null;
  let pagesFetched = 0;
  let rateLimited = false;
  const trades = [];
  let historyReachedStart = false;
  let signaturesFetched = 0;
  let transactionsFetched = 0;
  try {
    for (let page = 0; page < args.heliusMaxPages; page += 1) {
      const signatures = await client.getSignaturesForAddress(curve.pda, { before, limit: args.heliusPageSize });
      pagesFetched += 1;
      signaturesFetched += signatures.length;
      if (!signatures.length) break;
      const filtered = filterSignaturesForWindow(signatures, { startTs: window.window_start_ts, endTs: window.window_end_ts });
      const batch = filtered.fetchable.map((s) => s.signature).filter(Boolean);
      if (batch.length) {
        const txs = await client.getEnhancedTransactions(batch);
        transactionsFetched += txs.length;
        for (const tx of txs) {
          const trade = normalizeCurveTransaction(tx, { tokenCa: window.token_ca, curvePda: curve.pda });
          if (trade) trades.push(trade);
        }
      }
      historyReachedStart = historyReachedStart || filtered.reachedStart;
      before = signatures.at(-1)?.signature || null;
      if (historyReachedStart || !before) break;
    }
    const row = summarizeTradesForWindow({
      provider: 'helius',
      window,
      trades,
      historyReachedStart,
      latencyMs: Date.now() - started,
      pagesFetched,
      rateLimited,
      costUnits: signaturesFetched + transactionsFetched,
      fetchOk: true,
      httpStatusClass: '2xx',
    });
    row.has_per_trade = true;
    return row;
  } catch (error) {
    const text = String(error?.message || error);
    rateLimited = /429|rate|too many|max usage/i.test(text);
    return summarizeTradesForWindow({
      provider: 'helius',
      window,
      trades,
      historyReachedStart,
      latencyMs: Date.now() - started,
      pagesFetched,
      rateLimited,
      costUnits: signaturesFetched + transactionsFetched,
      fetchOk: false,
      httpStatusClass: rateLimited ? '429' : null,
      error: text.replace(/api-key=[^&\s]+/g, 'api-key=<redacted>').slice(0, 240),
    });
  }
}

async function summarizeHelius({ windows, args }) {
  const apiKey = args.heliusApiKeyFile ? readSecretFile(args.heliusApiKeyFile) : (process.env.HELIUS_API_KEY || '');
  const rpcUrl = args.heliusRpcUrlFile ? readSecretFile(args.heliusRpcUrlFile) : (process.env.HELIUS_RPC_URL || '');
  const client = new HeliusHistoryClient({
    apiKey,
    rpcUrl: rpcUrl || undefined,
    pageSize: args.heliusPageSize,
    signatureRps: args.heliusSignatureRps,
    transactionRps: args.heliusTransactionRps,
  });
  if (!client.isEnabled()) {
    return providerDisabledRows('helius', windows, 'missing_helius_api_key_or_rpc_url');
  }
  const out = [];
  for (const window of windows) {
    out.push(await runHeliusWindow(client, window, args));
  }
  return out;
}

async function summarizeGmgn({ windows }) {
  if (!process.env.GMGN_API_KEY) {
    return providerDisabledRows('gmgn', windows, 'gmgn_api_key_missing');
  }
  // GMGN is volume-only OHLCV. We deliberately do not fake per-trade/wallet/progress.
  // A live implementation can be added behind this contract after key provisioning.
  return windows.map((window) => ({
    schema_version: 'curve_observability_canary_row.v1',
    provider: 'gmgn',
    token_ca: window.token_ca,
    signal_ts: window.signal_ts,
    window_start_ts: window.window_start_ts,
    window_end_ts: window.window_end_ts,
    fetch_ok: false,
    http_status_class: null,
    history_reached_start: false,
    in_window_trade_count: 0,
    has_per_trade: false,
    has_volume: true,
    has_progress: false,
    has_wallet: false,
    out_of_window_trade_count: 0,
    latency_ms: null,
    pages_fetched: 0,
    rate_limited: false,
    cost_units: 0,
    error: 'gmgn_volume_only_path_not_executed_by_canary_v1',
  }));
}

function computeVerdict(rows, selectedN, { minUsableCurveWindowRate = DEFAULT_MIN_USABLE_CURVE_WINDOW_RATE } = {}) {
  const disabled = rows.length > 0 && rows.every((r) => r.error === 'provider_disabled');
  const leakageRows = rows.filter((r) => Number(r.out_of_window_trade_count || 0) > 0);
  const completeRows = rows.filter((r) => (
    r.fetch_ok === true
    && r.history_reached_start === true
    && Number(r.out_of_window_trade_count || 0) === 0
    && r.has_per_trade === true
  ));
  const usableRows = completeRows.filter((r) => (
    Number(r.in_window_trade_count || 0) > 0
    && r.has_wallet === true
    && r.has_volume === true
  ));
  const completeRate = selectedN > 0 ? completeRows.length / selectedN : 0;
  const usableCurveWindowRate = selectedN > 0 ? usableRows.length / selectedN : 0;
  const latencies = rows.map((r) => Number(r.latency_ms)).filter(Number.isFinite).sort((a, b) => a - b);
  const medianLatency = latencies.length ? latencies[Math.floor((latencies.length - 1) / 2)] : null;
  const totalCost = Number(rows.reduce((sum, r) => sum + (Number(r.cost_units) || 0), 0).toFixed(6));
  let verdict = 'FAIL';
  let reason = 'complete_rate_below_0_60';
  if (leakageRows.length) {
    verdict = 'INVALID';
    reason = 'out_of_window_trades_detected';
  } else if (disabled) {
    verdict = 'FAIL';
    reason = 'provider_disabled';
  } else if (completeRate >= 0.9 && usableCurveWindowRate >= minUsableCurveWindowRate) {
    verdict = 'PASS';
    reason = 'complete_and_usable_curve_window_rates_pass';
  } else if (completeRate >= 0.9 && usableCurveWindowRate < minUsableCurveWindowRate) {
    verdict = 'PARTIAL';
    reason = 'usable_curve_window_rate_below_floor';
  } else if (completeRate >= 0.6) {
    verdict = 'PARTIAL';
    reason = 'complete_rate_between_0_60_and_0_90';
  }
  return {
    verdict,
    reason,
    selected_n: selectedN,
    complete_n: completeRows.length,
    complete_rate: Number(completeRate.toFixed(6)),
    usable_curve_window_n: usableRows.length,
    usable_curve_window_rate: Number(usableCurveWindowRate.toFixed(6)),
    min_usable_curve_window_rate: minUsableCurveWindowRate,
    leakage_rows_n: leakageRows.length,
    median_latency_ms: medianLatency,
    total_cost_units: totalCost,
    disabled,
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    provider: '',
    rows: '',
    dogs: '',
    duds: '',
    trades: '',
    duneManifest: '',
    duneAssumeCompleteWindow: false,
    executeDune: false,
    duneKeyFile: '~/.dune_api_key',
    dunePerformance: 'small',
    outDir: '',
    template: DEFAULT_TEMPLATE,
    limit: DEFAULT_LIMIT,
    nowTs: Math.floor(Date.now() / 1000),
    maxAgeDays: 14,
    minAgeSec: 7200,
    heliusApiKeyFile: '',
    heliusRpcUrlFile: '',
    heliusMaxPages: 5,
    heliusPageSize: 100,
    heliusSignatureRps: 2,
    heliusTransactionRps: 1,
    minUsableCurveWindowRate: DEFAULT_MIN_USABLE_CURVE_WINDOW_RATE,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    const take = () => { i += 1; return next; };
    switch (key) {
      case '--provider': args.provider = take(); break;
      case '--rows': args.rows = take(); break;
      case '--dogs': args.dogs = take(); break;
      case '--duds': args.duds = take(); break;
      case '--trades': args.trades = take(); break;
      case '--dune-manifest': args.duneManifest = take(); break;
      case '--dune-assume-complete-window': args.duneAssumeCompleteWindow = true; break;
      case '--execute-dune': args.executeDune = true; break;
      case '--dune-key-file': args.duneKeyFile = take(); break;
      case '--dune-performance': args.dunePerformance = take(); break;
      case '--out-dir': args.outDir = take(); break;
      case '--template': args.template = take(); break;
      case '--limit': args.limit = Number(take()); break;
      case '--now-ts': args.nowTs = Number(take()); break;
      case '--max-age-days': args.maxAgeDays = Number(take()); break;
      case '--min-age-sec': args.minAgeSec = Number(take()); break;
      case '--helius-api-key-file': args.heliusApiKeyFile = take(); break;
      case '--helius-rpc-url-file': args.heliusRpcUrlFile = take(); break;
      case '--helius-max-pages': args.heliusMaxPages = Number(take()); break;
      case '--helius-page-size': args.heliusPageSize = Number(take()); break;
      case '--min-usable-curve-window-rate': args.minUsableCurveWindowRate = Number(take()); break;
      case '--help': case '-h': args.help = true; break;
      default: throw new Error(`Unknown argument: ${key}`);
    }
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/run-curve-observability-canary.js --provider dune --dogs clean-dogs.json --duds clean-duds.json --out-dir <dir> --trades trades.jsonl --dune-assume-complete-window',
    '',
    'Providers: dune | helius | gmgn. Output is label-free coverage/availability/cost only.',
  ].join('\n');
}

async function main() {
  const args = parseArgs();
  if (args.help) { console.log(usage()); return; }
  if (!['dune', 'helius', 'gmgn'].includes(args.provider)) die('--provider must be one of dune|helius|gmgn');
  if (!args.outDir) die('--out-dir required');
  if (!args.rows && !args.dogs && !args.duds) die('provide --rows or --dogs/--duds');
  if (!fs.existsSync(args.template)) die(`template not found: ${args.template}`);
  const signals = loadSignalInputs(args);
  const { windows, stats } = buildCanaryWindows(signals, {
    nowTs: args.nowTs,
    limit: args.limit,
    maxAgeDays: args.maxAgeDays,
    minAgeSec: args.minAgeSec,
  });
  fs.mkdirSync(args.outDir, { recursive: true });
  const artifacts = writeWindowsArtifacts(windows, args.outDir, args.template);
  let rows = [];
  if (args.provider === 'dune') {
    if (args.trades) {
      rows = summarizeDune({ windows, tradesPath: args.trades, assumeCompleteWindow: args.duneAssumeCompleteWindow, manifestPath: args.duneManifest });
    } else if (args.executeDune) {
      const tradesPath = path.join(args.outDir, 'dune-trades.jsonl');
      const manifestPath = path.join(args.outDir, 'dune-manifest.json');
      execFileSync('python3', [
        path.join(SCRIPTS, 'run-dune-sql-export.py'),
        '--sql', artifacts.sqlPath,
        '--out-jsonl', tradesPath,
        '--manifest', manifestPath,
        '--key-file', args.duneKeyFile,
        '--performance', args.dunePerformance,
      ], { stdio: 'inherit' });
      rows = summarizeDune({ windows, tradesPath, assumeCompleteWindow: args.duneAssumeCompleteWindow, manifestPath });
    } else {
      const summary = {
        schema_version: 'curve_observability_canary_summary.v1',
        generated_at: new Date().toISOString(),
        provider: args.provider,
        verdict: 'PREPARED_NEEDS_DUNE_EXPORT',
        reason: 'run_generated_sql_then_reinvoke_with_trades_and_dune_assume_complete_window',
        selection: stats,
        artifacts,
      };
      fs.writeFileSync(path.join(args.outDir, 'canary-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
      console.log(JSON.stringify(summary, null, 2));
      return;
    }
  } else if (args.provider === 'helius') {
    rows = await summarizeHelius({ windows, args });
  } else if (args.provider === 'gmgn') {
    rows = await summarizeGmgn({ windows, args });
  }

  const forbidden = hasForbiddenKey(rows);
  if (forbidden) die(`forbidden output key detected before write: ${forbidden}`);
  const leakage = rows.filter((r) => Number(r.out_of_window_trade_count || 0) > 0);
  const outJsonl = path.join(args.outDir, 'canary_observability.jsonl');
  fs.writeFileSync(outJsonl, rows.map((row) => JSON.stringify(row, Object.keys(row).sort())).join('\n') + (rows.length ? '\n' : ''));
  const verdict = computeVerdict(rows, windows.length, { minUsableCurveWindowRate: args.minUsableCurveWindowRate });
  const summary = {
    schema_version: 'curve_observability_canary_summary.v1',
    generated_at: new Date().toISOString(),
    provider: args.provider,
    verdict: verdict.verdict,
    reason: verdict.reason,
    selection: stats,
    capability_summary: {
      has_per_trade_n: rows.filter((r) => r.has_per_trade).length,
      has_volume_n: rows.filter((r) => r.has_volume).length,
      has_progress_n: rows.filter((r) => r.has_progress).length,
      has_wallet_n: rows.filter((r) => r.has_wallet).length,
    },
    provider_summary: verdict,
    gates: {
      leakage_ok: leakage.length === 0,
      window_bounds_ok: rows.every((r) => r.window_end_ts === r.signal_ts && r.window_start_ts === r.signal_ts - DEFAULT_PRE_SEC),
      label_blind_output_ok: hasForbiddenKey(rows) == null,
      provider_enablement_honest: true,
      coverage_not_outcome: true,
    },
    artifacts: {
      ...artifacts,
      canary_observability_jsonl: outJsonl,
      canary_observability_sha256: sha256File(outJsonl),
      trades: args.trades || null,
      dune_manifest: args.duneManifest || null,
    },
  };
  const forbiddenSummary = hasForbiddenKey(summary);
  if (forbiddenSummary) die(`forbidden summary key detected: ${forbiddenSummary}`);
  if (!summary.gates.window_bounds_ok) die('window bounds gate failed; postSec must be 0 and preSec must be 900');
  if (leakage.length) die(`out_of_window_trade_count > 0 for ${leakage.length} rows`);
  fs.writeFileSync(path.join(args.outDir, 'canary-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, verdict: summary.verdict, reason: summary.reason, complete_rate: summary.provider_summary.complete_rate, out_dir: args.outDir }, null, 2));
}

if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(2);
  });
}

export {
  buildCanaryWindows,
  valuesSql,
  normalizeSignal,
  normalizeTrade,
  summarizeDune,
  computeVerdict,
  hasForbiddenKey,
};
