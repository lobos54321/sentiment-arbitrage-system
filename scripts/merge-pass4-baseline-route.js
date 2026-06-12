#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    baselineRouted: '',
    pass4: '',
    outDir: './data/audits/baseline-route-v2',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--baseline-routed') { args.baselineRouted = next; i += 1; continue; }
    if (key === '--pass4') { args.pass4 = next; i += 1; continue; }
    if (key === '--out-dir') { args.outDir = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/merge-pass4-baseline-route.js \\',
    '    --baseline-routed baseline-route-v1/chain-truth-tier1-baseline-routed.jsonl \\',
    '    --pass4 baseline-pass4-quiet-deep-lookback-full/chain-truth-tier1-baseline.json \\',
    '    --out-dir baseline-route-v2',
    '',
    'Promotes quiet no-curve baseline rows to explicit routes using true Pass-4',
    'last-pre-anchor chain evidence. Rows that are already graduated at anchor',
    'must be re-priced through GMGN/AMM USD instead of using curve graduation SOL',
    'as a returns baseline.',
  ].join('\n');
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function readJsonl(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8').trim();
  if (!raw) return [];
  return raw.split('\n').filter(Boolean).map((line) => JSON.parse(line));
}

function readPass4Rows(filePath) {
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed.results)) return parsed.results;
  throw new Error(`Unsupported Pass-4 JSON shape: ${filePath}`);
}

function anchorKey(row = {}) {
  return `${row.token_ca}|${row.anchor_ts ?? row.signal_ts}`;
}

function countBy(rows = [], field) {
  const out = {};
  for (const row of rows) {
    const key = row[field] ?? 'unknown';
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function classifyPass4Route(row = {}) {
  const hasBars = Array.isArray(row.bars) && row.bars.length > 0;
  const progress = numeric(row.baseline_progress_pct);
  if (!hasBars) return null;
  if (progress != null && progress >= 99.5) return 'graduated_route_gmgn_amm';
  if (progress != null && progress >= 85) return 'near_graduation_curve_baseline';
  return 'curve_active_curve_baseline';
}

function mergeRoutes({ baselineRows = [], pass4Rows = [] } = {}) {
  const pass4ByKey = new Map(pass4Rows.map((row) => [anchorKey(row), row]));
  let pass4MatchedRows = 0;
  let pass4PromotedRows = 0;
  let pass4StillMissingRows = 0;
  const rows = baselineRows.map((row) => {
    const pass4 = pass4ByKey.get(anchorKey(row));
    if (!pass4) return row;
    pass4MatchedRows += 1;
    const pass4Route = classifyPass4Route(pass4);
    if (!pass4Route) {
      pass4StillMissingRows += 1;
      return {
        ...row,
        pass4_status: pass4.status || null,
        pass4_history_reached_start: pass4.history_reached_start ?? null,
        pass4_history_incomplete: pass4.history_reached_start === false,
        pass4_bars_n: Array.isArray(pass4.bars) ? pass4.bars.length : 0,
        pass4_exact_trade_event_n: pass4.exact_trade_event_n ?? null,
        pass4_note: 'true_pass4_found_no_baseline_trade',
      };
    }
    pass4PromotedRows += 1;
    return {
      ...row,
      baseline_route_v1: pass4Route,
      baseline_route_previous: row.baseline_route_v1 || null,
      baseline_route_source: 'true_pass4_last_pre_anchor',
      baseline_price_sol_chain: pass4.baseline_price_sol_chain ?? row.baseline_price_sol_chain ?? null,
      baseline_progress_pct: pass4.baseline_progress_pct ?? row.baseline_progress_pct ?? null,
      baseline_trade_lag_sec: pass4.baseline_trade_lag_sec ?? row.baseline_trade_lag_sec ?? null,
      baseline_signature: pass4.baseline_signature ?? row.baseline_signature ?? null,
      baseline_source: pass4.baseline_source ?? row.baseline_source ?? null,
      baseline_scan_mode: pass4.baseline_scan_mode ?? row.baseline_scan_mode ?? null,
      baseline_real_sol_reserves: pass4.baseline_real_sol_reserves ?? row.baseline_real_sol_reserves ?? null,
      baseline_real_token_reserves: pass4.baseline_real_token_reserves ?? row.baseline_real_token_reserves ?? null,
      pass4_status: pass4.status || null,
      pass4_history_reached_start: pass4.history_reached_start ?? null,
      pass4_history_incomplete: pass4.history_reached_start === false,
      pass4_bars_n: Array.isArray(pass4.bars) ? pass4.bars.length : 0,
      pass4_exact_trade_event_n: pass4.exact_trade_event_n ?? null,
      pass4_transactions_fetched: pass4.transactions_fetched ?? null,
      pass4_signatures_fetched: pass4.signatures_fetched ?? null,
    };
  });
  return {
    rows,
    summary: {
      schema_version: 'baseline_route_v2_pass4_merge.v1',
      generated_at: new Date().toISOString(),
      rows_n: rows.length,
      pass4_rows_n: pass4Rows.length,
      pass4_matched_rows_n: pass4MatchedRows,
      pass4_promoted_rows_n: pass4PromotedRows,
      pass4_still_missing_rows_n: pass4StillMissingRows,
      by_baseline_route_v1: countBy(rows, 'baseline_route_v1'),
      by_baseline_route_source: countBy(rows, 'baseline_route_source'),
    },
  };
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.baselineRouted || !args.pass4) {
    throw new Error('Provide --baseline-routed and --pass4');
  }
  const baselineRows = readJsonl(args.baselineRouted);
  const pass4Rows = readPass4Rows(args.pass4);
  const report = mergeRoutes({ baselineRows, pass4Rows });
  const outDir = path.resolve(args.outDir);
  fs.mkdirSync(outDir, { recursive: true });
  const outJsonl = path.join(outDir, 'chain-truth-tier1-baseline-routed-v2.jsonl');
  const outSummary = path.join(outDir, 'summary.json');
  fs.writeFileSync(outJsonl, `${report.rows.map((row) => JSON.stringify(row)).join('\n')}\n`);
  fs.writeFileSync(outSummary, `${JSON.stringify(report.summary, null, 2)}\n`);
  console.log(JSON.stringify({ outJsonl, outSummary, summary: report.summary }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}

export {
  classifyPass4Route,
  mergeRoutes,
};
