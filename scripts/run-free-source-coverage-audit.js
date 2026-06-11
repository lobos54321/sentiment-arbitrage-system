#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    dogs: '',
    duds: '',
    out: './data/audits/free-source-coverage/latest.json',
    targetedOut: '',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--dogs') { args.dogs = next; i += 1; continue; }
    if (key === '--duds') { args.duds = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--targeted-out') { args.targetedOut = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/run-free-source-coverage-audit.js --dogs clean-dog-touch.json --duds clean-dud-touch.json --out out.json',
  ].join('\n');
}

function loadRows(filePath) {
  if (!filePath) return [];
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed.results)) return parsed.results;
  if (Array.isArray(parsed.rows)) return parsed.rows;
  return [];
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function volumeVisibilityStage(row = {}) {
  const lag = numeric(row.first_nonzero_volume_lag_sec);
  if (lag == null) return 'volume_not_visible_in_window';
  if (lag <= 0) return 'already_volume_visible_at_anchor';
  if (lag <= 300) return 'volume_visible_within_5m';
  if (lag <= 900) return 'volume_visible_5m_to_15m';
  return 'dark_after_15m';
}

export function classifyFreeSource(row = {}) {
  const stage = volumeVisibilityStage(row);
  const hasBars = Number(row.bars || 0) > 0;
  const hasResidual = numeric(row.residual_peak_0m_pct) != null || numeric(row.price_max) != null;
  const freeSourceEnough = hasBars && hasResidual && !['volume_not_visible_in_window', 'dark_after_15m'].includes(stage);
  return {
    ...row,
    volume_visibility_stage: stage,
    free_source_verdict: freeSourceEnough ? 'free_source_enough' : 'chain_truth_required',
    chain_truth_need: freeSourceEnough ? null : (
      stage === 'volume_not_visible_in_window'
        ? 'missing_volume_path'
        : 'pre_grad_or_dark_volume_truth'
    ),
  };
}

function summarizeRows(rows = []) {
  const byVerdict = {};
  const byStage = {};
  for (const row of rows) {
    byVerdict[row.free_source_verdict] = (byVerdict[row.free_source_verdict] || 0) + 1;
    byStage[row.volume_visibility_stage] = (byStage[row.volume_visibility_stage] || 0) + 1;
  }
  return {
    rows_n: rows.length,
    free_source_enough_n: rows.filter((row) => row.free_source_verdict === 'free_source_enough').length,
    chain_truth_required_n: rows.filter((row) => row.free_source_verdict === 'chain_truth_required').length,
    by_verdict: byVerdict,
    by_volume_visibility_stage: byStage,
  };
}

function tokenLine(row = {}, cohort = 'unknown') {
  return [
    row.token_ca,
    Math.floor(Number(row.signal_ts || row.clean_anchor_signal_ts || row.anchor_ts || 0)),
    cohort,
    row.chain_truth_need || 'chain_truth_required',
    row.volume_visibility_stage || 'unknown',
  ].join('|');
}

export function buildReport({ dogRows = [], dudRows = [] } = {}) {
  const dogs = dogRows.map(classifyFreeSource);
  const duds = dudRows.map(classifyFreeSource);
  const targeted = [
    ...dogs.filter((row) => row.free_source_verdict === 'chain_truth_required').map((row) => ({ ...row, cohort: 'dog' })),
    ...duds.filter((row) => row.free_source_verdict === 'chain_truth_required').map((row) => ({ ...row, cohort: 'dud' })),
  ];
  return {
    schema_version: 'free_source_coverage_audit.v1',
    generated_at: new Date().toISOString(),
    summary: {
      dogs: summarizeRows(dogs),
      duds: summarizeRows(duds),
      targeted_chain_truth_rows_n: targeted.length,
    },
    dogs,
    duds,
    targeted_chain_truth: targeted,
  };
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  const report = buildReport({
    dogRows: loadRows(args.dogs),
    dudRows: loadRows(args.duds),
  });
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, `${JSON.stringify(report, null, 2)}\n`);
  if (args.targetedOut) {
    fs.mkdirSync(path.dirname(path.resolve(args.targetedOut)), { recursive: true });
    fs.writeFileSync(
      args.targetedOut,
      `${report.targeted_chain_truth.map((row) => tokenLine(row, row.cohort)).join('\n')}${report.targeted_chain_truth.length ? '\n' : ''}`,
    );
  }
  console.log(JSON.stringify({ out: args.out, summary: report.summary }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}

