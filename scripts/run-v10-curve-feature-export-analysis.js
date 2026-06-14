#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { spawnSync } from 'child_process';
import { parse as parseCsv } from 'csv-parse/sync';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    packDir: '',
    windows: '',
    trades: '',
    dogs: '',
    duds: '',
    worklist: '',
    outDir: '',
    assumeCompleteWindow: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--pack-dir') { args.packDir = next; i += 1; continue; }
    if (key === '--windows') { args.windows = next; i += 1; continue; }
    if (key === '--trades') { args.trades = next; i += 1; continue; }
    if (key === '--dogs') { args.dogs = next; i += 1; continue; }
    if (key === '--duds') { args.duds = next; i += 1; continue; }
    if (key === '--worklist') { args.worklist = next; i += 1; continue; }
    if (key === '--out-dir') { args.outDir = next; i += 1; continue; }
    if (key === '--assume-complete-window') { args.assumeCompleteWindow = true; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/run-v10-curve-feature-export-analysis.js \\',
    '    --pack-dir <export-pack-v2> \\',
    '    --trades <exported-pumpfun-trades.csv-or-jsonl> \\',
    '    --out-dir <analysis-out> \\',
    '    --assume-complete-window',
    '',
    'Optional overrides:',
    '  --windows <signal_windows.csv>',
    '  --worklist <token|signal_ts|label file>',
    '  --dogs <rebuilt-clean-dogs.json>',
    '  --duds <rebuilt-clean-duds.json>',
    '',
    'Runs validation -> decode-from-export -> feature table and writes a compact summary.',
    'Do not pass --assume-complete-window unless the indexed export guarantees full coverage.',
  ].join('\n');
}

function readJsonIfExists(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function resolveFromPack(args) {
  const packManifest = args.packDir ? readJsonIfExists(path.join(args.packDir, 'manifest.json')) : null;
  return {
    windows: args.windows || packManifest?.outputs?.signal_windows_csv || (args.packDir ? path.join(args.packDir, 'signal_windows.csv') : ''),
    dogs: args.dogs || packManifest?.inputs?.dogs_json || '',
    duds: args.duds || packManifest?.inputs?.duds_json || '',
  };
}

function readWindowsCsv(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  return parseCsv(raw, {
    columns: true,
    skip_empty_lines: true,
    bom: true,
    trim: true,
  });
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
  const token = String(row.token_ca || '').trim();
  const ts = normalizeTs(row.signal_ts);
  return token && ts != null ? `${token}|${ts}` : '';
}

function writeWorklistFromWindows(windowsCsv, outPath) {
  const rows = readWindowsCsv(windowsCsv);
  const lines = rows.map((row) => `${row.token_ca}|${row.signal_ts}|${row.label || ''}`);
  fs.writeFileSync(outPath, `${lines.join('\n')}\n`);
  return { outPath, rows: rows.length };
}

function writeCohortSubset(windowsCsv, dogsPath, dudsPath, outDir) {
  const windows = readWindowsCsv(windowsCsv);
  const wanted = new Map(windows.map((row) => [signalKey(row), row.label]));
  const dogs = JSON.parse(fs.readFileSync(dogsPath, 'utf8'));
  const duds = JSON.parse(fs.readFileSync(dudsPath, 'utf8'));
  const dogSubset = dogs.filter((row) => wanted.get(signalKey(row)) === 'dog');
  const dudSubset = duds.filter((row) => wanted.get(signalKey(row)) === 'dud');
  const found = new Set([...dogSubset, ...dudSubset].map(signalKey));
  const missing = [...wanted.keys()].filter((key) => key && !found.has(key));
  const dogSubsetPath = path.join(outDir, 'dogs-cohort-subset.json');
  const dudSubsetPath = path.join(outDir, 'duds-cohort-subset.json');
  fs.writeFileSync(dogSubsetPath, `${JSON.stringify(dogSubset, null, 2)}\n`);
  fs.writeFileSync(dudSubsetPath, `${JSON.stringify(dudSubset, null, 2)}\n`);
  return {
    dogs: dogSubsetPath,
    duds: dudSubsetPath,
    windows_n: windows.length,
    dogs_n: dogSubset.length,
    duds_n: dudSubset.length,
    missing_n: missing.length,
    missing_sample: missing.slice(0, 20),
  };
}

function runNode(script, args) {
  const result = spawnSync(process.execPath, [script, ...args], {
    cwd: process.cwd(),
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  if (result.status !== 0) {
    const err = [
      `Command failed: node ${script} ${args.join(' ')}`,
      result.stdout,
      result.stderr,
    ].filter(Boolean).join('\n');
    throw new Error(err);
  }
  return result.stdout.trim();
}

function maxRateDelta(group = {}) {
  const values = Object.values(group)
    .map((row) => row?.trade_hit_rate ?? row?.complete_rate)
    .filter((value) => value != null)
    .map(Number)
    .filter(Number.isFinite);
  if (values.length < 2) return null;
  return Number((Math.max(...values) - Math.min(...values)).toFixed(6));
}

function summarizeValidation(validation) {
  const guardrail = validation.trade_hit_guardrail || {};
  return {
    windows_n: validation.summary?.windows_n ?? null,
    trades_n: validation.summary?.trades_n ?? null,
    windows_with_trades_n: validation.summary?.windows_with_trades_n ?? null,
    label_trade_hit_delta: maxRateDelta(guardrail.by_label),
    return_domain_x_label_trade_hit_delta: maxRateDelta(guardrail.by_return_domain_x_label),
    warning: guardrail.warning || validation.summary?.warning || null,
  };
}

function summarizeFeatureTable(table) {
  const coverage = table.feature_coverage || {};
  return {
    rows_n: table.rows_n,
    dogs_n: table.dogs_n,
    duds_n: table.duds_n,
    matched_decode_n: table.matched_decode_n,
    exact_decode_rows_n: table.exact_decode_rows_n,
    feature_coverage_counts: table.feature_coverage_counts,
    label_complete_rate_delta: maxRateDelta(coverage.by_label),
    return_domain_x_label_complete_rate_delta: maxRateDelta(coverage.by_return_domain_x_label),
    all: {
      usable_n: table.strata?.all?.all?.usable_n ?? null,
      dogs_n: table.strata?.all?.all?.dogs_n ?? null,
      duds_n: table.strata?.all?.all?.duds_n ?? null,
      insufficient: table.strata?.all?.all?.insufficient ?? null,
    },
  };
}

function main() {
  const args = parseArgs();
  if (args.help || !args.trades || !args.outDir || (!args.packDir && !args.windows)) {
    console.log(usage());
    process.exit(args.help ? 0 : 1);
  }
  const resolved = resolveFromPack(args);
  const windows = resolved.windows;
  const dogs = resolved.dogs;
  const duds = resolved.duds;
  for (const [label, filePath] of Object.entries({ windows, trades: args.trades, dogs, duds })) {
    if (!filePath || !fs.existsSync(filePath)) {
      throw new Error(`Missing required ${label} file: ${filePath || '(empty)'}`);
    }
  }

  fs.mkdirSync(args.outDir, { recursive: true });
  const worklist = args.worklist || path.join(args.outDir, 'worklist-from-windows.txt');
  if (!args.worklist) writeWorklistFromWindows(windows, worklist);
  const cohortSubset = writeCohortSubset(windows, dogs, duds, args.outDir);
  const validationOut = path.join(args.outDir, 'trade-export-validation.json');
  const decodeOut = path.join(args.outDir, 'merged-decode-from-export.json');
  const featureOut = path.join(args.outDir, 'curve-feature-table.json');
  const summaryOut = path.join(args.outDir, 'analysis-summary.json');

  runNode('scripts/validate-v10-curve-feature-trade-export.js', [
    '--windows', windows,
    '--trades', args.trades,
    '--out', validationOut,
  ]);
  const decodeArgs = [
    '--worklist', worklist,
    '--trades', args.trades,
    '--out', decodeOut,
  ];
  if (args.assumeCompleteWindow) decodeArgs.push('--assume-complete-window');
  runNode('scripts/build-v10-curve-feature-decode-from-trades.js', decodeArgs);
  runNode('scripts/build-v10-curve-feature-table.js', [
    '--dogs', cohortSubset.dogs,
    '--duds', cohortSubset.duds,
    '--decode', decodeOut,
    '--out', featureOut,
  ]);

  const validation = JSON.parse(fs.readFileSync(validationOut, 'utf8'));
  const featureTable = JSON.parse(fs.readFileSync(featureOut, 'utf8'));
  const summary = {
    schema_version: 'v10_curve_feature_export_analysis_summary.v1',
    generated_at: new Date().toISOString(),
    inputs: {
      pack_dir: args.packDir || null,
      windows,
      trades: args.trades,
      worklist,
      dogs,
      duds,
      dogs_subset: cohortSubset.dogs,
      duds_subset: cohortSubset.duds,
      assume_complete_window: args.assumeCompleteWindow,
    },
    outputs: {
      validation: validationOut,
      decode: decodeOut,
      feature_table: featureOut,
      summary: summaryOut,
    },
    cohort_subset: {
      windows_n: cohortSubset.windows_n,
      dogs_n: cohortSubset.dogs_n,
      duds_n: cohortSubset.duds_n,
      missing_n: cohortSubset.missing_n,
      missing_sample: cohortSubset.missing_sample,
    },
    validation: summarizeValidation(validation),
    feature_table: summarizeFeatureTable(featureTable),
    guardrail: 'Read validation trade-hit deltas and feature complete-rate deltas before reading any AUC. If coverage is asymmetric, explain coverage first.',
  };
  fs.writeFileSync(summaryOut, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify(summary, null, 2));
}

main();
