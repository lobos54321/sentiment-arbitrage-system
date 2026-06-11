#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    signal: '',
    decision: '',
    out: './data/audits/gmgn-matched-anchor-ceiling/latest.json',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--signal') { args.signal = next; i += 1; continue; }
    if (key === '--decision') { args.decision = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/run-gmgn-matched-anchor-ceiling.js --signal signal-touch.json --decision decision-touch.json [--out out.json]',
    '',
    'Compares residual capture on the same token cohort across two anchors. This measures anchor timing cost, not dog/dud precision.',
  ].join('\n');
}

function loadRows(filePath) {
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return Array.isArray(parsed?.results) ? parsed.results : [];
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function byToken(rows = []) {
  const out = new Map();
  for (const row of rows) {
    const token = String(row.token_ca || '').trim();
    if (!token || out.has(token)) continue;
    out.set(token, row);
  }
  return out;
}

function summarize(values = []) {
  const clean = values.map(Number).filter(Number.isFinite).sort((a, b) => a - b);
  if (!clean.length) return { n: 0, min: null, median: null, p75: null, p90: null, max: null };
  const pct = (p) => {
    const idx = (clean.length - 1) * p;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    if (lo === hi) return clean[lo];
    return clean[lo] + (clean[hi] - clean[lo]) * (idx - lo);
  };
  return {
    n: clean.length,
    min: clean[0],
    median: Number(pct(0.5).toFixed(6)),
    p75: Number(pct(0.75).toFixed(6)),
    p90: Number(pct(0.9).toFixed(6)),
    max: clean[clean.length - 1],
  };
}

function entryPriceSource(row, delayMin) {
  const price = numeric(row[`entry_${delayMin}m_price`]);
  if (price == null) return 'missing_entry_price';
  const nonzeroTs = numeric(row.first_nonzero_volume_ts);
  const entryTs = numeric(row[`entry_${delayMin}m_ts`]);
  if (nonzeroTs == null || entryTs == null) return 'gmgn_price_without_volume_timestamp';
  return entryTs < nonzeroTs ? 'gmgn_price_before_first_nonzero_volume' : 'gmgn_price_after_first_nonzero_volume';
}

function countBy(rows = [], fn) {
  const out = {};
  for (const row of rows) {
    const key = fn(row) || 'unknown';
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function buildDelayRow(pairs = [], delayMin = 0) {
  const residualField = `residual_peak_${delayMin}m_pct`;
  const silverField = `residual_to_silver_${delayMin}m`;
  const goldField = `residual_to_gold_${delayMin}m`;
  const usable = pairs.filter(({ signal, decision }) => (
    numeric(signal?.[residualField]) != null
    && numeric(decision?.[residualField]) != null
  ));
  const signalSilver = usable.filter(({ signal }) => signal[silverField] === true).length;
  const signalGold = usable.filter(({ signal }) => signal[goldField] === true).length;
  const decisionSilver = usable.filter(({ decision }) => decision[silverField] === true).length;
  const decisionGold = usable.filter(({ decision }) => decision[goldField] === true).length;
  const n = usable.length;
  return {
    delay_min: delayMin,
    matched_tokens_evaluable_n: n,
    excluded_missing_signal_or_decision_entry_price: pairs.length - n,
    signal_silver_capture_n: signalSilver,
    decision_silver_capture_n: decisionSilver,
    signal_silver_capture_rate: n ? Number((signalSilver / n).toFixed(4)) : null,
    decision_silver_capture_rate: n ? Number((decisionSilver / n).toFixed(4)) : null,
    silver_delta_decision_minus_signal: n ? Number(((decisionSilver - signalSilver) / n).toFixed(4)) : null,
    signal_gold_capture_n: signalGold,
    decision_gold_capture_n: decisionGold,
    signal_gold_capture_rate: n ? Number((signalGold / n).toFixed(4)) : null,
    decision_gold_capture_rate: n ? Number((decisionGold / n).toFixed(4)) : null,
    gold_delta_decision_minus_signal: n ? Number(((decisionGold - signalGold) / n).toFixed(4)) : null,
    signal_residual_summary: summarize(usable.map(({ signal }) => numeric(signal[residualField]))),
    decision_residual_summary: summarize(usable.map(({ decision }) => numeric(decision[residualField]))),
    signal_entry_price_source_counts: countBy(usable.map((pair) => pair.signal), (row) => entryPriceSource(row, delayMin)),
    decision_entry_price_source_counts: countBy(usable.map((pair) => pair.decision), (row) => entryPriceSource(row, delayMin)),
  };
}

function buildReport({ signalRows = [], decisionRows = [] }) {
  const signalByToken = byToken(signalRows);
  const decisionByToken = byToken(decisionRows);
  const pairs = [];
  for (const [token, decision] of decisionByToken.entries()) {
    const signal = signalByToken.get(token);
    if (!signal) continue;
    pairs.push({ token_ca: token, signal, decision });
  }
  const delays = [0, 5, 15];
  return {
    schema_version: 'gmgn_matched_anchor_ceiling.v1',
    generated_at: new Date().toISOString(),
    anchor_comparison_definition: 'Matched by token_ca between signal-anchor and decision-anchor GMGN touch results. This estimates anchor timing cost on the same GMGN-evaluable dog cohort; it does not measure dog/dud precision.',
    inputs: {
      signal_rows_n: signalRows.length,
      decision_rows_n: decisionRows.length,
      matched_tokens_n: pairs.length,
      unmatched_decision_tokens_n: decisionRows.length - pairs.length,
    },
    delays_min: delays,
    rows: delays.map((delayMin) => buildDelayRow(pairs, delayMin)),
  };
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.signal || !args.decision) throw new Error('Provide --signal and --decision');
  const report = buildReport({
    signalRows: loadRows(args.signal),
    decisionRows: loadRows(args.decision),
  });
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({
    out: args.out,
    matched_tokens_n: report.inputs.matched_tokens_n,
    rows: report.rows,
  }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}

export {
  buildReport,
  buildDelayRow,
};
