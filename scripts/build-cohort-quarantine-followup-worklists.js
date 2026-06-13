#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    quarantine: '',
    outDir: './data/audits/cohort-quarantine-followup',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--quarantine') { args.quarantine = next; i += 1; continue; }
    if (key === '--out-dir') { args.outDir = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/build-cohort-quarantine-followup-worklists.js \\',
    '    --quarantine cohort-rebuild-v2/rebuilt-quarantine.json \\',
    '    --out-dir cohort-quarantine-followup-v1',
    '',
    'Builds action-oriented follow-up worklists from the rebuilt cohort quarantine layer.',
    'This is an offline research tool only; it never changes strategy gates or live state.',
  ].join('\n');
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function readRows(filePath) {
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed.rows)) return parsed.rows;
  if (Array.isArray(parsed.results)) return parsed.results;
  throw new Error(`Unsupported quarantine JSON shape for ${filePath}`);
}

function signalKey(row = {}) {
  return `${String(row.token_ca || '').trim()}|${Math.floor(Number(row.signal_ts ?? row.anchor_ts ?? 0))}`;
}

function isPumpfunMint(tokenCa) {
  return String(tokenCa || '').endsWith('pump');
}

function venueClass(row = {}) {
  if (isPumpfunMint(row.token_ca)) return 'pumpfun';
  const token = String(row.token_ca || '').toLowerCase();
  if (token.includes('bonk')) return 'bonkish';
  return 'other';
}

export function classifyQuarantineRow(row = {}) {
  const status = String(row.label_adjudication_status || '');
  const reason = String(row.label_adjudication_reason || '');
  const route = String(row.route_unit_domain || '');
  const cleaning = String(row.label_cleaning_reason || '');
  const hasExternalTruth = Boolean(row.chain_truth_route_present || row.gmgn_full_window_present || row.peak_window_present);
  const venue = venueClass(row);

  if (
    status === 'quarantine_refuted_by_physics_needs_corrected_peak'
    && reason === 'missing_compatible_peak_or_bridge'
    && !hasExternalTruth
  ) {
    return {
      bucket: 'needs_legal_peak_no_external_truth',
      action: venue === 'pumpfun' ? 'gmgn_full_window_or_curve_peak_adjudication' : 'venue_peak_decoder_required',
      priority: venue === 'pumpfun' ? 1 : 4,
      venue,
    };
  }

  if (reason === 'missing_baseline') {
    if (route === 'venue_other') {
      return {
        bucket: 'venue_other_missing_baseline',
        action: 'venue_baseline_decoder_required',
        priority: 4,
        venue,
      };
    }
    if (route === 'history_incomplete') {
      return {
        bucket: row.refuted_by_physics ? 'history_incomplete_baseline_and_peak' : 'history_incomplete_baseline',
        action: 'baseline_deep_backfill_or_external_route',
        priority: row.refuted_by_physics ? 2 : 3,
        venue,
      };
    }
    if (route === 'missing_curve_baseline') {
      return {
        bucket: row.refuted_by_physics ? 'missing_curve_baseline_and_peak' : 'missing_curve_baseline',
        action: 'curve_baseline_reconstruction',
        priority: row.refuted_by_physics ? 2 : 3,
        venue,
      };
    }
    return {
      bucket: row.refuted_by_physics ? 'unrouted_missing_baseline_and_peak' : 'unrouted_missing_baseline',
      action: 'baseline_route_required',
      priority: 3,
      venue,
    };
  }

  if (reason === 'missing_compatible_peak_or_bridge') {
    return {
      bucket: row.refuted_by_physics ? 'needs_compatible_peak_or_bridge' : 'incomplete_peak_or_bridge',
      action: venue === 'pumpfun' ? 'curve_peak_or_splice_bridge_adjudication' : 'venue_peak_decoder_required',
      priority: row.refuted_by_physics ? 2 : 4,
      venue,
    };
  }

  return {
    bucket: 'unclassified_quarantine',
    action: 'manual_review',
    priority: 5,
    venue,
  };
}

function worklistLine(row = {}) {
  return [
    row.token_ca,
    Math.floor(Number(row.signal_ts ?? row.anchor_ts ?? 0)),
    'quarantine',
    row.followup_action,
    row.followup_bucket,
  ].join('|');
}

function uniqueRows(rows = []) {
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const key = `${signalKey(row)}|${row.followup_action}|${row.followup_bucket}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out.sort((a, b) => {
    const pa = numeric(a.followup_priority) ?? 99;
    const pb = numeric(b.followup_priority) ?? 99;
    if (pa !== pb) return pa - pb;
    const ta = Math.floor(Number(a.signal_ts ?? a.anchor_ts ?? 0));
    const tb = Math.floor(Number(b.signal_ts ?? b.anchor_ts ?? 0));
    return ta - tb || String(a.token_ca).localeCompare(String(b.token_ca));
  });
}

function countBy(rows = [], field) {
  const out = {};
  for (const row of rows) {
    const key = row[field] ?? 'unknown';
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function uniqueSignalCount(rows = []) {
  return new Set(rows.map(signalKey).filter((key) => !key.endsWith('|0'))).size;
}

function uniqueTokenCount(rows = []) {
  return new Set(rows.map((row) => row.token_ca).filter(Boolean)).size;
}

export function buildFollowup(rows = []) {
  const classified = rows.map((row) => {
    const cls = classifyQuarantineRow(row);
    return {
      ...row,
      followup_bucket: cls.bucket,
      followup_action: cls.action,
      followup_priority: cls.priority,
      venue_class: cls.venue,
    };
  });
  const worklistRows = uniqueRows(classified);
  const actions = {};
  for (const row of worklistRows) {
    actions[row.followup_action] ||= [];
    actions[row.followup_action].push(row);
  }
  return {
    schema_version: 'cohort_quarantine_followup.v1',
    generated_at: new Date().toISOString(),
    rows: classified,
    worklistRows,
    actions,
    summary: {
      input_rows_n: rows.length,
      worklist_rows_n: worklistRows.length,
      unique_signals_n: uniqueSignalCount(classified),
      unique_tokens_n: uniqueTokenCount(classified),
      by_bucket: countBy(classified, 'followup_bucket'),
      by_action: countBy(classified, 'followup_action'),
      by_venue_class: countBy(classified, 'venue_class'),
      by_priority: countBy(classified, 'followup_priority'),
    },
  };
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function writeJsonl(filePath, rows = []) {
  fs.writeFileSync(filePath, `${rows.map((row) => JSON.stringify(row)).join('\n')}${rows.length ? '\n' : ''}`);
}

function writeText(filePath, text) {
  fs.writeFileSync(filePath, text);
}

function writeReport(report, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  writeJson(path.join(outDir, 'followup-summary.json'), {
    schema_version: report.schema_version,
    generated_at: report.generated_at,
    summary: report.summary,
  });
  writeJsonl(path.join(outDir, 'followup-rows.jsonl'), report.rows);
  writeJsonl(path.join(outDir, 'followup-worklist.jsonl'), report.worklistRows);
  writeText(path.join(outDir, 'followup-worklist.txt'), `${report.worklistRows.map(worklistLine).join('\n')}${report.worklistRows.length ? '\n' : ''}`);
  for (const [action, rows] of Object.entries(report.actions)) {
    const safe = action.replace(/[^a-zA-Z0-9_-]+/g, '_');
    writeText(path.join(outDir, `${safe}.txt`), `${rows.map(worklistLine).join('\n')}${rows.length ? '\n' : ''}`);
  }
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.quarantine) throw new Error('Provide --quarantine');
  const rows = readRows(args.quarantine);
  const report = buildFollowup(rows);
  const outDir = path.resolve(args.outDir);
  writeReport(report, outDir);
  console.log(JSON.stringify({ out_dir: outDir, summary: report.summary }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}
