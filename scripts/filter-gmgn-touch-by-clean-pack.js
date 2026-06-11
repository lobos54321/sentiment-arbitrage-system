#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    touch: '',
    anchors: '',
    out: './data/audits/gmgn-touch-filtered/latest.json',
    maxDeltaSec: 600,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--touch') { args.touch = next; i += 1; continue; }
    if (key === '--anchors') { args.anchors = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--max-delta-sec') { args.maxDeltaSec = Number(next); i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/filter-gmgn-touch-by-clean-pack.js --touch gmgn.json --anchors clean-dogs.json --out out.json',
  ].join('\n');
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function loadJsonRows(filePath) {
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed.results)) return parsed.results;
  if (Array.isArray(parsed.rows)) return parsed.rows;
  return [];
}

function rowToken(row = {}) {
  return String(row.token_ca || row.token || row.address || '').trim();
}

function rowTs(row = {}) {
  return numeric(row.signal_ts ?? row.anchor_ts ?? row.decision_ts ?? row.ts);
}

export function chooseClosest(anchor = {}, candidates = [], { maxDeltaSec = 600 } = {}) {
  const token = rowToken(anchor);
  const ts = rowTs(anchor);
  if (!token || ts == null) return null;
  let best = null;
  for (const row of candidates) {
    if (rowToken(row) !== token) continue;
    const other = rowTs(row);
    if (other == null) continue;
    const delta = Math.abs(other - ts);
    if (delta > maxDeltaSec) continue;
    if (!best || delta < best.delta_sec) best = { row, delta_sec: delta };
  }
  return best;
}

export function buildTouchSubset({ touchRows = [], anchors = [], maxDeltaSec = 600 } = {}) {
  const matched = [];
  const missing = [];
  for (const anchor of anchors) {
    const match = chooseClosest(anchor, touchRows, { maxDeltaSec });
    if (!match) {
      missing.push(anchor);
      continue;
    }
    matched.push({
      ...match.row,
      clean_anchor_signal_ts: rowTs(anchor),
      clean_anchor_tier: anchor.tier || anchor.raw_sustained_tier || anchor.raw_primary_tier || null,
      clean_anchor_match_delta_sec: match.delta_sec,
    });
  }
  return {
    schema_version: 'gmgn_touch_filtered_by_clean_pack.v1',
    generated_at: new Date().toISOString(),
    max_delta_sec: maxDeltaSec,
    summary: {
      anchors_n: anchors.length,
      matched_n: matched.length,
      missing_n: missing.length,
    },
    results: matched,
    missing_anchors: missing,
  };
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.touch || !args.anchors) throw new Error('Provide --touch and --anchors');
  const report = buildTouchSubset({
    touchRows: loadJsonRows(args.touch),
    anchors: loadJsonRows(args.anchors),
    maxDeltaSec: args.maxDeltaSec,
  });
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({ out: args.out, summary: report.summary }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}

