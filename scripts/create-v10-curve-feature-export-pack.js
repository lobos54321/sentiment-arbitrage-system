#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    worklist: '',
    outDir: '',
    preSec: 900,
    postSec: 0,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--worklist') { args.worklist = next; i += 1; continue; }
    if (key === '--out-dir') { args.outDir = next; i += 1; continue; }
    if (key === '--pre-sec') { args.preSec = Number(next); i += 1; continue; }
    if (key === '--post-sec') { args.postSec = Number(next); i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/create-v10-curve-feature-export-pack.js --worklist v10-curve-feature-stratified-300.txt --out-dir ./export-pack',
    '',
    'Creates a durable, self-contained input pack for an indexed pump.fun TradeEvent export.',
    'The pack includes signal windows CSV, token list, Dune-style VALUES snippet, and manifest hashes.',
  ].join('\n');
}

function sha256(filePath) {
  return createHash('sha256').update(fs.readFileSync(filePath)).digest('hex');
}

function csvEscape(value) {
  const text = String(value ?? '');
  if (!/[",\n\r]/.test(text)) return text;
  return `"${text.replace(/"/g, '""')}"`;
}

function normalizeTs(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

function readWorklist(filePath) {
  return fs.readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'))
    .map((line, index) => {
      const [token, signalTs, label = ''] = line.split(/[|,\t]/).map((part) => part.trim());
      const ts = normalizeTs(signalTs);
      if (!token || ts == null) return null;
      return {
        window_id: `w${String(index + 1).padStart(5, '0')}`,
        token_ca: token,
        signal_ts: ts,
        label,
      };
    })
    .filter(Boolean);
}

function writeCsv(filePath, rows, columns) {
  const lines = [
    columns.join(','),
    ...rows.map((row) => columns.map((column) => csvEscape(row[column])).join(',')),
  ];
  fs.writeFileSync(filePath, lines.join('\n') + '\n');
}

function makeSqlValues(rows) {
  return rows.map((row) => (
    `('${row.window_id}', '${row.token_ca}', ${row.signal_ts}, ${row.window_start_ts}, ${row.window_end_ts}, '${row.label}')`
  )).join(',\n    ');
}

function main() {
  const args = parseArgs();
  if (args.help || !args.worklist || !args.outDir) {
    console.log(usage());
    process.exit(args.help ? 0 : 1);
  }
  const rows = readWorklist(args.worklist).map((row) => ({
    ...row,
    window_start_ts: row.signal_ts - Number(args.preSec || 0),
    window_end_ts: row.signal_ts + Number(args.postSec || 0),
  }));
  const tokenRows = [...new Set(rows.map((row) => row.token_ca))]
    .sort()
    .map((token_ca) => ({ token_ca }));
  fs.mkdirSync(args.outDir, { recursive: true });
  const windowsCsv = path.join(args.outDir, 'signal_windows.csv');
  const tokensCsv = path.join(args.outDir, 'tokens.csv');
  const valuesSql = path.join(args.outDir, 'signal_windows_values.sql');
  const readme = path.join(args.outDir, 'README.md');
  writeCsv(windowsCsv, rows, ['window_id', 'token_ca', 'signal_ts', 'window_start_ts', 'window_end_ts', 'label']);
  writeCsv(tokensCsv, tokenRows, ['token_ca']);
  fs.writeFileSync(valuesSql, [
    '-- Paste this CTE into an indexed pump.fun TradeEvent query.',
    '-- Required output fields are documented in claudedocs/v10-curve-feature-export-spec.md.',
    'WITH signal_windows(window_id, token_ca, signal_ts, window_start_ts, window_end_ts, label) AS (',
    '  VALUES',
    `    ${makeSqlValues(rows)}`,
    ')',
    'SELECT * FROM signal_windows;',
    '',
  ].join('\n'));
  fs.writeFileSync(readme, [
    '# V10 Curve Feature Export Pack',
    '',
    'Use this pack to export complete pump.fun TradeEvent rows for the no-future feature window:',
    '',
    '`[signal_ts - 900 seconds, signal_ts]`',
    '',
    'Files:',
    '',
    '- `signal_windows.csv`: one row per signal-anchor window.',
    '- `tokens.csv`: unique token list for systems that query by token first.',
    '- `signal_windows_values.sql`: VALUES CTE for SQL engines such as Dune.',
    '- `manifest.json`: file hashes and row counts.',
    '',
    'After exporting trades, run:',
    '',
    '```bash',
    'node scripts/build-v10-curve-feature-decode-from-trades.js \\',
    '  --worklist <worklist.txt> \\',
    '  --trades <pumpfun-trades.csv-or-jsonl> \\',
    '  --out <merged-decode.json> \\',
    '  --assume-complete-window',
    '',
    'node scripts/build-v10-curve-feature-table.js \\',
    '  --dogs <rebuilt-clean-dogs.json> \\',
    '  --duds <rebuilt-clean-duds.json> \\',
    '  --decode <merged-decode.json> \\',
    '  --out <curve-feature-table.json>',
    '```',
    '',
    'Do not pass `--assume-complete-window` unless the export guarantees full window coverage for every row.',
    '',
  ].join('\n'));
  const manifest = {
    schema_version: 'v10_curve_feature_export_pack.v1',
    generated_at: new Date().toISOString(),
    inputs: {
      worklist: args.worklist,
      pre_sec: Number(args.preSec || 0),
      post_sec: Number(args.postSec || 0),
    },
    outputs: {
      out_dir: args.outDir,
      signal_windows_csv: windowsCsv,
      tokens_csv: tokensCsv,
      signal_windows_values_sql: valuesSql,
      readme,
    },
    rows: rows.length,
    dogs: rows.filter((row) => row.label === 'dog').length,
    duds: rows.filter((row) => row.label === 'dud').length,
    unique_tokens: tokenRows.length,
  };
  const manifestPath = path.join(args.outDir, 'manifest.json');
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  const hashes = {
    [path.basename(windowsCsv)]: sha256(windowsCsv),
    [path.basename(tokensCsv)]: sha256(tokensCsv),
    [path.basename(valuesSql)]: sha256(valuesSql),
    [path.basename(readme)]: sha256(readme),
    [path.basename(manifestPath)]: sha256(manifestPath),
  };
  fs.writeFileSync(path.join(args.outDir, 'SHA256SUMS.json'), `${JSON.stringify(hashes, null, 2)}\n`);
  console.log(JSON.stringify({
    out_dir: args.outDir,
    rows: rows.length,
    dogs: manifest.dogs,
    duds: manifest.duds,
    unique_tokens: tokenRows.length,
    files: manifest.outputs,
  }, null, 2));
}

main();
