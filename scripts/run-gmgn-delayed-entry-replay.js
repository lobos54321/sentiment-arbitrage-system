#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    dogs: '',
    duds: '',
    out: './data/audits/gmgn-delayed-entry-replay/latest.json',
    thresholds: [0, 5_000, 10_000, 20_000, 40_000],
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--dogs') { args.dogs = next; i += 1; continue; }
    if (key === '--duds') { args.duds = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--thresholds') {
      args.thresholds = String(next || '').split(',').map((v) => Number(v.trim())).filter(Number.isFinite);
      i += 1;
      continue;
    }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/run-gmgn-delayed-entry-replay.js --dogs dog-touch.json --duds dud-touch.json [options]',
    '',
    'Options:',
    '  --thresholds <csv>  early_5m volume thresholds for delayed confirmation, default 0,5000,10000,20000,40000',
    '  --out <path>        Output JSON path',
  ].join('\n');
}

function loadResults(filePath) {
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return Array.isArray(parsed?.results) ? parsed.results : [];
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
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
  return { n: clean.length, min: clean[0], median: pct(0.5), p75: pct(0.75), p90: pct(0.9), max: clean[clean.length - 1] };
}

function selectedByThreshold(row, threshold) {
  return (numeric(row.early_5m_volume_usd_sum) ?? 0) >= threshold;
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

function replayForDelay({ dogs = [], duds = [], delayMin = 0, threshold = 0 }) {
  const residualField = `residual_peak_${delayMin}m_pct`;
  const silverField = `residual_to_silver_${delayMin}m`;
  const goldField = `residual_to_gold_${delayMin}m`;
  const selectedDogsAll = dogs.filter((row) => selectedByThreshold(row, threshold));
  const selectedDudsAll = duds.filter((row) => selectedByThreshold(row, threshold));
  const dogRows = selectedDogsAll.filter((row) => numeric(row[residualField]) != null);
  const dudRows = selectedDudsAll.filter((row) => numeric(row[residualField]) != null);
  const dogSilver = dogRows.filter((row) => row[silverField] === true).length;
  const dogGold = dogRows.filter((row) => row[goldField] === true).length;
  const dudSilver = dudRows.filter((row) => row[silverField] === true).length;
  const dudGold = dudRows.filter((row) => row[goldField] === true).length;
  return {
    threshold_early_5m_volume_usd: threshold,
    delay_min: delayMin,
    capture_denominator_definition: 'selected rows with residual_peak_<delay>m_pct available; this is an evaluable GMGN-price subset, not necessarily the full raw dog denominator',
    precision_definition: 'dog_silver_capture_n / (dog_silver_capture_n + dud_reaches_silver_n) among selected evaluable dog/dud rows',
    selected_dogs_all: selectedDogsAll.length,
    selected_duds_all: selectedDudsAll.length,
    selected_dogs: dogRows.length,
    selected_duds: dudRows.length,
    excluded_dogs_missing_entry_price: selectedDogsAll.length - dogRows.length,
    excluded_duds_missing_entry_price: selectedDudsAll.length - dudRows.length,
    dog_silver_capture_n: dogSilver,
    dog_gold_capture_n: dogGold,
    dog_silver_capture_rate: dogRows.length ? Number((dogSilver / dogRows.length).toFixed(4)) : null,
    dog_gold_capture_rate: dogRows.length ? Number((dogGold / dogRows.length).toFixed(4)) : null,
    dud_reaches_silver_n: dudSilver,
    dud_reaches_gold_n: dudGold,
    selected_precision_if_silver_target: (dogSilver + dudSilver) ? Number((dogSilver / (dogSilver + dudSilver)).toFixed(4)) : null,
    dog_entry_price_source_counts: countBy(dogRows, (row) => entryPriceSource(row, delayMin)),
    dud_entry_price_source_counts: countBy(dudRows, (row) => entryPriceSource(row, delayMin)),
    dog_residual_summary: summarize(dogRows.map((row) => numeric(row[residualField]))),
    dud_residual_summary: summarize(dudRows.map((row) => numeric(row[residualField]))),
  };
}

function buildReport({ dogs, duds, thresholds = [] }) {
  const delays = [0, 5, 15];
  const captureDenominatorDefinition = 'selected rows with residual_peak_<delay>m_pct available; this is an evaluable GMGN-price subset, not necessarily the full raw dog denominator';
  const precisionDefinition = 'dog_silver_capture_n / (dog_silver_capture_n + dud_reaches_silver_n) among selected evaluable dog/dud rows';
  return {
    schema_version: 'gmgn_delayed_entry_replay.v1',
    generated_at: new Date().toISOString(),
    capture_denominator_definition: captureDenominatorDefinition,
    precision_definition: precisionDefinition,
    inputs: {
      dogs_n: dogs.length,
      duds_n: duds.length,
      note: 'Requires GMGN touch results generated after residual_peak_* fields were added. early_5m threshold is a T+5m delayed confirmation feature, not an immediate-entry feature.',
    },
    thresholds,
    delays_min: delays,
    rows: thresholds.flatMap((threshold) => delays.map((delayMin) => replayForDelay({
      dogs,
      duds,
      delayMin,
      threshold,
    }))),
  };
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.dogs || !args.duds) throw new Error('Provide --dogs and --duds');
  const dogs = loadResults(args.dogs);
  const duds = loadResults(args.duds);
  const report = buildReport({ dogs, duds, thresholds: args.thresholds });
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({
    out: args.out,
    dogs_n: dogs.length,
    duds_n: duds.length,
    rows: report.rows.length,
    best_directional_rows: report.rows
      .filter((row) => row.delay_min === 5)
      .sort((a, b) => (b.selected_precision_if_silver_target ?? -1) - (a.selected_precision_if_silver_target ?? -1))
      .slice(0, 3),
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
  replayForDelay,
};
