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
    '',
    'Optional env:',
    '  V10_CURVE_FEATURE_DOGS_JSON=/path/rebuilt-clean-dogs.json',
    '  V10_CURVE_FEATURE_DUDS_JSON=/path/rebuilt-clean-duds.json',
    '',
    'When provided, return_domain/effective_tier are joined into the export windows so downstream',
    'coverage checks can detect dog/dud and domain-level export skew before reading AUC.',
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

function readJsonIfExists(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return [];
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function cohortMetaBySignal(rows) {
  const out = new Map();
  for (const row of rows) {
    const token = String(row.token_ca || '').trim();
    const ts = normalizeTs(row.signal_ts);
    if (!token || ts == null) continue;
    out.set(`${token}|${ts}`, {
      return_domain: row.return_domain || 'unknown',
      effective_tier: row.effective_tier || row.tier || 'unknown',
    });
  }
  return out;
}

function countBy(rows, keyFn) {
  const out = {};
  for (const row of rows) {
    const key = keyFn(row) || 'unknown';
    out[key] = (out[key] || 0) + 1;
  }
  return out;
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
    `('${row.window_id}', '${row.token_ca}', ${row.signal_ts}, ${row.window_start_ts}, ${row.window_end_ts}, '${row.label}', '${row.return_domain}', '${row.effective_tier}')`
  )).join(',\n    ');
}

function makeIndexedTradeExportTemplate(rows) {
  const values = makeSqlValues(rows);
  return [
    '-- V10 curve-stage feature export template.',
    '-- Replace YOUR_PUMPFUN_TRADE_EVENT_TABLE and source column names in source_trades.',
    '-- Required semantic contract:',
    '--   one row = one pump.fun TradeEvent for a token in signal_windows',
    '--   block_time between window_start_ts and window_end_ts, inclusive',
    '--   no post-signal trades; this export is for ex-ante features only',
    '--',
    '-- After export, validate before using it:',
    '--   node scripts/validate-v10-curve-feature-trade-export.js --windows signal_windows.csv --trades exported_trades.csv --out validation.json',
    '-- Read validation.trade_hit_guardrail before reading AUC.',
    '',
    'WITH signal_windows(window_id, token_ca, signal_ts, window_start_ts, window_end_ts, label, return_domain, effective_tier) AS (',
    '  VALUES',
    `    ${values}`,
    '),',
    'bounds AS (',
    '  SELECT',
    '    MIN(window_start_ts) AS min_ts,',
    '    MAX(window_end_ts) AS max_ts',
    '  FROM signal_windows',
    '),',
    'source_trades AS (',
    '  SELECT',
    '    -- TODO: map these aliases from the indexed pump.fun TradeEvent source.',
    '    CAST(mint AS VARCHAR) AS token_ca,',
    '    CAST(to_unixtime(block_time) AS BIGINT) AS block_time,',
    '    CAST(signature AS VARCHAR) AS signature,',
    "    CASE WHEN is_buy THEN 'buy' ELSE 'sell' END AS side,",
    '    CAST(user AS VARCHAR) AS user,',
    '    CAST(sol_amount AS DOUBLE) AS sol_amount,',
    '    CAST(token_amount AS DOUBLE) AS token_amount,',
    '    CAST(virtual_sol_reserves AS DOUBLE) AS virtual_sol_reserves,',
    '    CAST(virtual_token_reserves AS DOUBLE) AS virtual_token_reserves,',
    '    CAST(real_token_reserves AS DOUBLE) AS real_token_reserves',
    '  FROM YOUR_PUMPFUN_TRADE_EVENT_TABLE',
    '  WHERE CAST(to_unixtime(block_time) AS BIGINT) BETWEEN (SELECT min_ts FROM bounds) AND (SELECT max_ts FROM bounds)',
    '    AND CAST(mint AS VARCHAR) IN (SELECT DISTINCT token_ca FROM signal_windows)',
    '),',
    'joined AS (',
    '  SELECT',
    '    w.window_id,',
    '    w.token_ca,',
    '    w.signal_ts,',
    '    w.window_start_ts,',
    '    w.window_end_ts,',
    '    w.label,',
    '    w.return_domain,',
    '    w.effective_tier,',
    '    t.block_time,',
    '    t.signature,',
    '    t.side,',
    '    t.user,',
    '    t.sol_amount,',
    '    t.token_amount,',
    '    t.virtual_sol_reserves,',
    '    t.virtual_token_reserves,',
    '    t.real_token_reserves,',
    '    CASE',
    '      WHEN t.virtual_token_reserves IS NOT NULL AND t.virtual_token_reserves != 0',
    '      THEN t.virtual_sol_reserves / t.virtual_token_reserves',
    '      ELSE NULL',
    '    END AS reserve_price_sol',
    '  FROM signal_windows w',
    '  LEFT JOIN source_trades t',
    '    ON t.token_ca = w.token_ca',
    '   AND t.block_time BETWEEN w.window_start_ts AND w.window_end_ts',
    ')',
    'SELECT *',
    'FROM joined',
    'WHERE block_time IS NOT NULL',
    'ORDER BY window_id, block_time, signature;',
    '',
  ].join('\n');
}

function main() {
  const args = parseArgs();
  if (args.help || !args.worklist || !args.outDir) {
    console.log(usage());
    process.exit(args.help ? 0 : 1);
  }
  const meta = new Map([
    ...cohortMetaBySignal(readJsonIfExists(process.env.V10_CURVE_FEATURE_DOGS_JSON || '')),
    ...cohortMetaBySignal(readJsonIfExists(process.env.V10_CURVE_FEATURE_DUDS_JSON || '')),
  ]);
  const rows = readWorklist(args.worklist).map((row) => ({
    ...row,
    window_start_ts: row.signal_ts - Number(args.preSec || 0),
    window_end_ts: row.signal_ts + Number(args.postSec || 0),
    return_domain: meta.get(`${row.token_ca}|${row.signal_ts}`)?.return_domain || 'unknown',
    effective_tier: meta.get(`${row.token_ca}|${row.signal_ts}`)?.effective_tier || 'unknown',
  }));
  const tokenRows = [...new Set(rows.map((row) => row.token_ca))]
    .sort()
    .map((token_ca) => ({ token_ca }));
  fs.mkdirSync(args.outDir, { recursive: true });
  const windowsCsv = path.join(args.outDir, 'signal_windows.csv');
  const tokensCsv = path.join(args.outDir, 'tokens.csv');
  const valuesSql = path.join(args.outDir, 'signal_windows_values.sql');
  const exportTemplateSql = path.join(args.outDir, 'indexed_trade_export_template.sql');
  const readme = path.join(args.outDir, 'README.md');
  writeCsv(windowsCsv, rows, ['window_id', 'token_ca', 'signal_ts', 'window_start_ts', 'window_end_ts', 'label', 'return_domain', 'effective_tier']);
  writeCsv(tokensCsv, tokenRows, ['token_ca']);
  fs.writeFileSync(valuesSql, [
    '-- Paste this CTE into an indexed pump.fun TradeEvent query.',
    '-- Required output fields are documented in claudedocs/v10-curve-feature-export-spec.md.',
    'WITH signal_windows(window_id, token_ca, signal_ts, window_start_ts, window_end_ts, label, return_domain, effective_tier) AS (',
    '  VALUES',
    `    ${makeSqlValues(rows)}`,
    ')',
    'SELECT * FROM signal_windows;',
    '',
  ].join('\n'));
  fs.writeFileSync(exportTemplateSql, makeIndexedTradeExportTemplate(rows));
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
    '- `indexed_trade_export_template.sql`: copy/edit SQL template for indexed pump.fun TradeEvent export.',
    '- `manifest.json`: file hashes and row counts.',
    '',
    'Export contract:',
    '',
    '- Export all pump.fun TradeEvents in `[signal_ts - 900, signal_ts]` for every row.',
    '- Keep `window_id`, `label`, `return_domain`, and `effective_tier` in the output.',
    '- Do not use post-signal trades for this feature table.',
    '- Validate dog/dud and return-domain coverage symmetry before reading AUC.',
    '',
    'After exporting trades, prefer the one-command analysis runner:',
    '',
    '```bash',
    'node scripts/run-v10-curve-feature-export-analysis.js \\',
    '  --pack-dir <this-export-pack-dir> \\',
    '  --trades <pumpfun-trades.csv-or-jsonl> \\',
    '  --out-dir <analysis-out> \\',
    '  --assume-complete-window',
    '```',
    '',
    'Only pass `--assume-complete-window` when the indexed export guarantees full coverage for every row.',
    'The runner validates the export, converts trades into decode rows, subsets the cohort to this pack, builds the feature table, and writes `analysis-summary.json`.',
    '',
    'Manual equivalent:',
    '',
    '```bash',
    'node scripts/validate-v10-curve-feature-trade-export.js \\',
    '  --windows signal_windows.csv \\',
    '  --trades <pumpfun-trades.csv-or-jsonl> \\',
    '  --out validation.json',
    '',
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
    '  --out <curve-feature-table.json> \\',
    '  --include-discrimination-report',
    '```',
    '',
    'Read `validation.trade_hit_guardrail` and `feature_table.label_complete_rate_delta` / `return_domain_x_label_complete_rate_delta` before reading any AUC.',
    '',
  ].join('\n'));
  const manifest = {
    schema_version: 'v10_curve_feature_export_pack.v1',
    generated_at: new Date().toISOString(),
    inputs: {
      worklist: args.worklist,
      dogs_json: process.env.V10_CURVE_FEATURE_DOGS_JSON || null,
      duds_json: process.env.V10_CURVE_FEATURE_DUDS_JSON || null,
      pre_sec: Number(args.preSec || 0),
      post_sec: Number(args.postSec || 0),
    },
    outputs: {
      out_dir: args.outDir,
      signal_windows_csv: windowsCsv,
      tokens_csv: tokensCsv,
      signal_windows_values_sql: valuesSql,
      indexed_trade_export_template_sql: exportTemplateSql,
      readme,
    },
    rows: rows.length,
    dogs: rows.filter((row) => row.label === 'dog').length,
    duds: rows.filter((row) => row.label === 'dud').length,
    unique_tokens: tokenRows.length,
    coverage_guardrail: {
      return_domain_counts: countBy(rows, (row) => row.return_domain),
      return_domain_x_label_counts: countBy(rows, (row) => `${row.return_domain}|${row.label}`),
      missing_cohort_meta_rows: rows.filter((row) => row.return_domain === 'unknown' || row.effective_tier === 'unknown').length,
      warning: 'If missing_cohort_meta_rows is nonzero for a strategy audit pack, regenerate with V10_CURVE_FEATURE_DOGS_JSON and V10_CURVE_FEATURE_DUDS_JSON set.',
    },
  };
  const manifestPath = path.join(args.outDir, 'manifest.json');
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  const hashes = {
    [path.basename(windowsCsv)]: sha256(windowsCsv),
    [path.basename(tokensCsv)]: sha256(tokensCsv),
    [path.basename(valuesSql)]: sha256(valuesSql),
    [path.basename(exportTemplateSql)]: sha256(exportTemplateSql),
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
