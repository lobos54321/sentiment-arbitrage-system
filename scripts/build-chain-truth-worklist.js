#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    targeted: '',
    quarantine: '',
    out: './data/audits/chain-truth-worklist-v2.txt',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--targeted') { args.targeted = next; i += 1; continue; }
    if (key === '--quarantine') { args.quarantine = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/build-chain-truth-worklist.js --targeted targeted.txt --quarantine quarantine.txt --out worklist.txt',
    '',
    'Output format: token|anchor_ts|cohort|chain_truth_need|visibility_stage',
  ].join('\n');
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function parseLine(line, { source }) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith('#')) return null;
  const [token, ts, third = '', need = '', stage = ''] = trimmed.split(/[|,\t]/).map((part) => part.trim());
  const anchorTs = numeric(ts);
  if (!token || anchorTs == null) return null;
  let cohort = third;
  if (source === 'quarantine') {
    cohort = 'quarantine';
  } else if (!['dog', 'dud', 'quarantine'].includes(String(cohort).toLowerCase())) {
    cohort = 'unknown';
  }
  return {
    token_ca: token,
    anchor_ts: Math.floor(anchorTs),
    cohort: String(cohort).toLowerCase(),
    chain_truth_need: need || (source === 'quarantine' ? 'label_quarantine_adjudication' : 'chain_truth_required'),
    visibility_stage: stage || 'unknown',
  };
}

export function readRows(filePath, { source }) {
  if (!filePath || !fs.existsSync(filePath)) return [];
  return fs.readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .map((line) => parseLine(line, { source }))
    .filter(Boolean);
}

function rankNeed(row = {}) {
  if (row.cohort === 'quarantine') return 3;
  if (row.cohort === 'dog') return 2;
  if (row.cohort === 'dud') return 1;
  return 0;
}

export function mergeWorklist({ targetedRows = [], quarantineRows = [] } = {}) {
  const byTokenSignal = new Map();
  for (const row of [...targetedRows, ...quarantineRows]) {
    const key = `${row.token_ca}:${row.anchor_ts}`;
    const existing = byTokenSignal.get(key);
    if (!existing || rankNeed(row) > rankNeed(existing)) {
      byTokenSignal.set(key, row);
    }
  }
  return [...byTokenSignal.values()].sort((a, b) => a.anchor_ts - b.anchor_ts || a.token_ca.localeCompare(b.token_ca));
}

function lineFor(row) {
  return [
    row.token_ca,
    row.anchor_ts,
    row.cohort,
    row.chain_truth_need,
    row.visibility_stage,
  ].join('|');
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.targeted && !args.quarantine) throw new Error('Provide --targeted and/or --quarantine');
  const rows = mergeWorklist({
    targetedRows: readRows(args.targeted, { source: 'targeted' }),
    quarantineRows: readRows(args.quarantine, { source: 'quarantine' }),
  });
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, `${rows.map(lineFor).join('\n')}${rows.length ? '\n' : ''}`);
  console.log(JSON.stringify({ out: args.out, rows_n: rows.length }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}
