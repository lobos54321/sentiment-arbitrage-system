#!/usr/bin/env node
import fs from 'fs';
import path from 'path';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    inDir: '',
    out: '',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--in-dir') { args.inDir = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/merge-v10-curve-feature-decode-batches.js --in-dir decode-batches --out merged-decode.json',
    '',
    'Merges decode-batch-*.json files from run-v10-curve-feature-decode-batches.js into one decode file consumable by build-v10-curve-feature-table.js.',
  ].join('\n');
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function listBatchFiles(inDir) {
  return fs.readdirSync(inDir)
    .filter((name) => /^decode-batch-\d+\.json$/.test(name))
    .sort()
    .map((name) => path.join(inDir, name));
}

function normalizeTs(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

function rowKey(row = {}) {
  return `${String(row.token_ca || '').trim()}|${normalizeTs(row.anchor_ts ?? row.signal_ts)}`;
}

function count(rows, predicate) {
  return rows.reduce((sum, row) => sum + (predicate(row) ? 1 : 0), 0);
}

function sum(rows, key) {
  return rows.reduce((acc, row) => acc + (Number(row[key]) || 0), 0);
}

function main() {
  const args = parseArgs();
  if (args.help || !args.inDir || !args.out) {
    console.log(usage());
    process.exit(args.help ? 0 : 1);
  }
  const files = listBatchFiles(args.inDir);
  const results = [];
  const seen = new Set();
  const duplicateKeys = [];
  const batchSummaries = [];
  for (const file of files) {
    const parsed = readJson(file);
    const rows = Array.isArray(parsed.results) ? parsed.results : [];
    batchSummaries.push({
      file,
      generated_at: parsed.generated_at || null,
      rows: rows.length,
      summary: parsed.summary || {},
    });
    for (const row of rows) {
      const key = rowKey(row);
      if (seen.has(key)) {
        duplicateKeys.push(key);
        continue;
      }
      seen.add(key);
      results.push(row);
    }
  }
  const okRows = results.filter((row) => row.status === 'ok');
  const report = {
    schema_version: 'v10_curve_feature_decode_merged.v1',
    generated_at: new Date().toISOString(),
    inputs: {
      in_dir: args.inDir,
      batch_files: files,
    },
    summary: {
      batch_files_n: files.length,
      results_n: results.length,
      duplicate_keys_n: duplicateKeys.length,
      ok_n: okRows.length,
      error_n: count(results, (row) => row.status === 'error'),
      planned_n: count(results, (row) => row.status === 'planned'),
      anchors_with_trades_n: count(okRows, (row) => Number(row.trades_n || 0) > 0),
      anchors_with_bars_n: count(okRows, (row) => Number(row.bars_n || 0) > 0),
      per_token_timeout_n: count(results, (row) => String(row.error || '').startsWith('per_token_timeout_ms:')),
      history_reached_start_n: count(okRows, (row) => row.history_reached_start === true),
      history_incomplete_n: count(okRows, (row) => row.history_reached_start === false),
      exact_trade_event_n: sum(okRows, 'exact_trade_event_n'),
      transfer_heuristic_trade_n: sum(okRows, 'transfer_heuristic_trade_n'),
      total_signatures_fetched: sum(okRows, 'signatures_fetched'),
      total_signatures_skipped_after_end: sum(okRows, 'signatures_skipped_after_end'),
      total_signatures_skipped_before_start: sum(okRows, 'signatures_skipped_before_start'),
      total_transactions_fetched: sum(okRows, 'transactions_fetched'),
      total_curve_trades: sum(okRows, 'trades_n'),
      total_sol_volume: Number(sum(okRows, 'total_sol_volume').toFixed(9)),
    },
    duplicate_keys: duplicateKeys.slice(0, 200),
    batch_summaries: batchSummaries,
    results,
  };
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({
    out: args.out,
    summary: report.summary,
  }, null, 2));
}

main();
