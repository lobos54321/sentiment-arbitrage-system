#!/usr/bin/env node
import fs from 'fs';
import path from 'path';

const FEATURE_KEYS = [
  'total_sol_volume',
  'buy_count',
  'sell_count',
  'net_buy_sol_volume',
  'buy_sell_count_imbalance',
  'buy_sell_sol_imbalance',
  'unique_buyers',
  'unique_sellers',
  'trade_density_per_min',
  'baseline_progress_pct',
  'last_progress_pct',
  'progress_delta_pct',
  'last_trade_lag_sec',
  'first_trade_lag_sec',
];

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    dogs: '',
    duds: '',
    decode: '',
    out: '',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--dogs') { args.dogs = next; i += 1; continue; }
    if (key === '--duds') { args.duds = next; i += 1; continue; }
    if (key === '--decode') { args.decode = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/build-v10-curve-feature-table.js --dogs rebuilt-clean-dogs.json --duds rebuilt-clean-duds.json --decode curve-decode.json --out feature-table.json',
    '',
    'Builds a signal-anchor, no-future-leak curve-stage feature table and stratified dog-vs-dud AUC report.',
  ].join('\n');
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
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

function signalKey(row = {}) {
  return `${String(row.token_ca || '').trim()}|${normalizeTs(row.signal_ts ?? row.anchor_ts)}`;
}

function anchorKey(row = {}) {
  return `${String(row.token_ca || '').trim()}|${normalizeTs(row.anchor_ts ?? row.signal_ts)}`;
}

function loadDecodeRows(filePath) {
  const parsed = readJson(filePath);
  const rows = Array.isArray(parsed) ? parsed : (parsed.results || parsed.rows || []);
  return rows.filter((row) => row && row.token_ca && normalizeTs(row.anchor_ts) != null);
}

function percentile(sortedValues, pct) {
  if (!sortedValues.length) return null;
  const idx = (sortedValues.length - 1) * pct;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sortedValues[lo];
  return sortedValues[lo] + (sortedValues[hi] - sortedValues[lo]) * (idx - lo);
}

function summarize(values = []) {
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

function progressStage(progress) {
  const p = numeric(progress);
  if (p == null) return 'unknown_progress';
  if (p >= 99.5) return 'graduated_or_at_graduation';
  if (p >= 85) return 'near_graduation_85_99_5';
  return 'curve_active_lt85';
}

function sumBars(row = {}, field) {
  const bars = Array.isArray(row.bars) ? row.bars : [];
  return bars.reduce((sum, bar) => sum + (numeric(bar[field]) || 0), 0);
}

function lastProgress(row = {}) {
  const bars = Array.isArray(row.bars) ? row.bars : [];
  for (let i = bars.length - 1; i >= 0; i -= 1) {
    const p = numeric(bars[i]?.progress_pct);
    if (p != null) return p;
  }
  return numeric(row.baseline_progress_pct);
}

function buildFeatureRow(baseRow, label, decodeRow) {
  const decodeStatus = decodeRow?.status || 'missing_decode';
  const coverageComplete = decodeStatus === 'ok' && decodeRow?.history_reached_start === true;
  const usable = coverageComplete;
  const buySol = usable ? sumBars(decodeRow, 'buy_sol_volume') : null;
  const sellSol = usable ? sumBars(decodeRow, 'sell_sol_volume') : null;
  const baselineProgress = usable ? numeric(decodeRow?.baseline_progress_pct) : null;
  const lastProg = usable ? lastProgress(decodeRow) : null;
  const totalSol = usable ? (numeric(decodeRow?.total_sol_volume) || 0) : null;
  const buyCount = usable ? (numeric(decodeRow?.buy_count) || 0) : null;
  const sellCount = usable ? (numeric(decodeRow?.sell_count) || 0) : null;
  const trades = usable ? (numeric(decodeRow?.trades_n) || 0) : null;
  const row = {
    signal_id: signalKey(baseRow),
    token_ca: baseRow.token_ca,
    signal_ts: normalizeTs(baseRow.signal_ts),
    label,
    effective_tier: baseRow.effective_tier || baseRow.tier || 'unknown',
    return_domain: baseRow.return_domain || 'unknown',
    decode_status: decodeStatus,
    feature_coverage_status: decodeStatus === 'ok'
      ? (coverageComplete ? 'complete_window' : 'incomplete_window')
      : 'decode_unavailable',
    history_reached_start: decodeRow?.history_reached_start ?? null,
    trades_n: trades,
    exact_trade_event_n: usable ? (numeric(decodeRow?.exact_trade_event_n) || 0) : null,
    bars_n: usable ? (numeric(decodeRow?.bars_n) || 0) : null,
    first_trade_lag_sec: usable ? numeric(decodeRow?.first_trade_lag_sec) : null,
    last_trade_lag_sec: usable ? numeric(decodeRow?.last_trade_lag_sec) : null,
    baseline_progress_pct: baselineProgress,
    last_progress_pct: lastProg,
    progress_delta_pct: baselineProgress != null && lastProg != null ? Number((lastProg - baselineProgress).toFixed(6)) : null,
    progress_stage: usable ? progressStage(baselineProgress) : 'decode_unavailable',
    total_sol_volume: totalSol,
    buy_count: buyCount,
    sell_count: sellCount,
    net_buy_sol_volume: usable ? Number((buySol - sellSol).toFixed(9)) : null,
    buy_sol_volume: usable ? Number(buySol.toFixed(9)) : null,
    sell_sol_volume: usable ? Number(sellSol.toFixed(9)) : null,
    buy_sell_count_imbalance: usable ? (buyCount + sellCount > 0 ? Number(((buyCount - sellCount) / (buyCount + sellCount)).toFixed(6)) : 0) : null,
    buy_sell_sol_imbalance: usable ? (buySol + sellSol > 0 ? Number(((buySol - sellSol) / (buySol + sellSol)).toFixed(6)) : 0) : null,
    unique_buyers: usable ? (numeric(decodeRow?.unique_buyers) || 0) : null,
    unique_sellers: usable ? (numeric(decodeRow?.unique_sellers) || 0) : null,
    trade_density_per_min: usable ? Number((trades / 15).toFixed(6)) : null,
    no_future_window: true,
    feature_window: '[signal_ts-900,signal_ts]',
  };
  return row;
}

function stratify(rows, keyFn) {
  const out = new Map();
  for (const row of rows) {
    const key = keyFn(row);
    if (!out.has(key)) out.set(key, []);
    out.get(key).push(row);
  }
  return out;
}

function featureReport(rows) {
  const usableRows = rows.filter((row) => row.feature_coverage_status === 'complete_window');
  const dogs = usableRows.filter((row) => row.label === 'dog');
  const duds = usableRows.filter((row) => row.label === 'dud');
  const features = {};
  for (const feature of FEATURE_KEYS) {
    features[feature] = {
      auc: auc(dogs.map((row) => row[feature]), duds.map((row) => row[feature])),
      dog_summary: summarize(dogs.map((row) => row[feature])),
      dud_summary: summarize(duds.map((row) => row[feature])),
    };
  }
  return {
    n: rows.length,
    usable_n: usableRows.length,
    dogs_n: dogs.length,
    duds_n: duds.length,
    insufficient: dogs.length < 30 || duds.length < 30,
    features,
  };
}

function coverageCounts(rows) {
  const out = {};
  for (const row of rows) {
    const key = row.feature_coverage_status || 'unknown';
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function main() {
  const args = parseArgs();
  if (args.help || !args.dogs || !args.duds || !args.decode || !args.out) {
    console.log(usage());
    process.exit(args.help ? 0 : 1);
  }
  const dogs = readJson(args.dogs).map((row) => ({ ...row, __label: 'dog' }));
  const duds = readJson(args.duds).map((row) => ({ ...row, __label: 'dud' }));
  const decodeRows = loadDecodeRows(args.decode);
  const decodeByKey = new Map(decodeRows.map((row) => [anchorKey(row), row]));
  const rows = [...dogs, ...duds].map((row) => buildFeatureRow(row, row.__label, decodeByKey.get(signalKey(row))));

  const strata = {};
  const maps = {
    all: new Map([['all', rows]]),
    return_domain: stratify(rows, (row) => row.return_domain),
    progress_stage: stratify(rows, (row) => row.progress_stage),
    return_domain_x_progress_stage: stratify(rows, (row) => `${row.return_domain}|${row.progress_stage}`),
  };
  for (const [name, groups] of Object.entries(maps)) {
    strata[name] = {};
    for (const [key, groupRows] of groups.entries()) {
      strata[name][key] = featureReport(groupRows);
    }
  }

  const report = {
    schema_version: 'v10_curve_stage_feature_table.v1',
    generated_at: new Date().toISOString(),
    inputs: {
      dogs: args.dogs,
      duds: args.duds,
      decode: args.decode,
    },
    rows_n: rows.length,
    dogs_n: rows.filter((row) => row.label === 'dog').length,
    duds_n: rows.filter((row) => row.label === 'dud').length,
    decode_rows_n: decodeRows.length,
    matched_decode_n: rows.filter((row) => row.decode_status !== 'missing_decode').length,
    exact_decode_rows_n: rows.filter((row) => row.exact_trade_event_n > 0).length,
    feature_coverage_counts: coverageCounts(rows),
    guardrail: 'Features are valid only for the pre-anchor window [signal_ts-900, signal_ts]. Do not use post-anchor GMGN early_5m/early_15m fields for immediate-entry gates.',
    strata,
    rows,
  };
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({
    out: args.out,
    rows_n: report.rows_n,
    matched_decode_n: report.matched_decode_n,
    exact_decode_rows_n: report.exact_decode_rows_n,
    feature_coverage_counts: report.feature_coverage_counts,
    all: report.strata.all.all,
  }, null, 2));
}

main();
