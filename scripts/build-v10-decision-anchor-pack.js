#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { execFileSync } from 'child_process';
import { pathToFileURL } from 'url';

const DEFAULT_DATA_ROOM = '/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z';
const DEFAULT_COHORT_DIR = path.join(DEFAULT_DATA_ROOM, 'cohort-rebuild-v10-final-native-return-guard');

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    paperDb: '',
    dataRoom: DEFAULT_DATA_ROOM,
    dogs: path.join(DEFAULT_COHORT_DIR, 'rebuilt-clean-dogs.json'),
    duds: path.join(DEFAULT_COHORT_DIR, 'rebuilt-clean-duds.json'),
    outDir: '',
    marginSec: 900,
    decisionWindowSec: 900,
    preSignalGraceSec: 60,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--paper-db') { args.paperDb = next; i += 1; continue; }
    if (key === '--data-room') { args.dataRoom = next; i += 1; continue; }
    if (key === '--dogs') { args.dogs = next; i += 1; continue; }
    if (key === '--duds') { args.duds = next; i += 1; continue; }
    if (key === '--out-dir') { args.outDir = next; i += 1; continue; }
    if (key === '--margin-sec') { args.marginSec = Number(next); i += 1; continue; }
    if (key === '--decision-window-sec') { args.decisionWindowSec = Number(next); i += 1; continue; }
    if (key === '--pre-signal-grace-sec') { args.preSignalGraceSec = Number(next); i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  if (!args.outDir) args.outDir = path.join(args.dataRoom, 'v10-decision-anchor-pack');
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/build-v10-decision-anchor-pack.js --paper-db /path/to/full/paper_trades.db',
    '',
    'One-command v10 decision-anchor pack builder:',
    '  1. export paper_decision_subset.db',
    '  2. run v10 clean-cohort decision funnel',
    '  3. write decision-anchor-pack-summary.json',
    '',
    'This does not change strategy and does not infer edge by itself.',
  ].join('\n');
}

function runNode(script, args = []) {
  const out = execFileSync(process.execPath, [script, ...args], {
    encoding: 'utf8',
    maxBuffer: 200 * 1024 * 1024,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  return JSON.parse(out);
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function rate(num, den) {
  return den ? num / den : null;
}

function packStatus(report) {
  const dogs = report.dogs?.summary || {};
  const duds = report.duds?.summary || {};
  const dogDecisionRate = rate(Number(dogs.has_decision_record || 0), Number(dogs.raw_sustained_dogs || 0));
  const dudDecisionRate = rate(Number(duds.has_decision_record || 0), Number(duds.raw_sustained_dogs || 0));
  const warnings = [];
  if (dogDecisionRate != null && dogDecisionRate < 0.1) {
    warnings.push('dog_decision_match_rate_below_10pct_check_for_partial_paper_db_or_pipeline_gap');
  }
  if (dudDecisionRate != null && dudDecisionRate < 0.1) {
    warnings.push('dud_decision_match_rate_below_10pct_check_for_partial_paper_db_or_pipeline_gap');
  }
  if (!Number(report.inputs?.decision_records_n || 0)) {
    warnings.push('no_decision_records_exported');
  }
  return {
    status: warnings.length ? 'review_required' : 'ok',
    warnings,
    dog_decision_match_rate: dogDecisionRate,
    dud_decision_match_rate: dudDecisionRate,
  };
}

function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.paperDb || !fs.existsSync(args.paperDb)) throw new Error('Provide an existing --paper-db');
  if (!fs.existsSync(args.dogs)) throw new Error(`Missing dogs file: ${args.dogs}`);
  if (!fs.existsSync(args.duds)) throw new Error(`Missing duds file: ${args.duds}`);
  fs.mkdirSync(args.outDir, { recursive: true });

  const subsetDb = path.join(args.outDir, 'paper_decision_subset.db');
  const funnelDir = path.join(args.outDir, 'decision-funnel');
  const exporter = runNode(path.join('scripts', 'export-paper-decision-subset.js'), [
    '--paper-db', args.paperDb,
    '--out-db', subsetDb,
    '--cohort-dogs', args.dogs,
    '--cohort-duds', args.duds,
    '--margin-sec', String(args.marginSec),
  ]);
  const funnelRun = runNode(path.join('scripts', 'run-v10-decision-funnel-audit.js'), [
    '--paper-db', subsetDb,
    '--dogs', args.dogs,
    '--duds', args.duds,
    '--out-dir', funnelDir,
    '--decision-window-sec', String(args.decisionWindowSec),
    '--pre-signal-grace-sec', String(args.preSignalGraceSec),
  ]);
  const funnelReport = readJson(funnelRun.paths.jsonPath);
  const status = packStatus(funnelReport);
  const summary = {
    schema_version: 'v10_decision_anchor_pack.v1',
    generated_at: new Date().toISOString(),
    ...status,
    inputs: {
      source_paper_db: args.paperDb,
      dogs_json: args.dogs,
      duds_json: args.duds,
      margin_sec: args.marginSec,
      decision_window_sec: args.decisionWindowSec,
      pre_signal_grace_sec: args.preSignalGraceSec,
    },
    outputs: {
      out_dir: args.outDir,
      paper_decision_subset_db: subsetDb,
      decision_funnel_json: funnelRun.paths.jsonPath,
      decision_funnel_markdown: funnelRun.paths.mdPath,
    },
    export_counts: {
      a_class_decision_events: exporter.a_class_decision_events,
      opportunity_events: exporter.opportunity_events,
      canonical_trade_ledger: exporter.canonical_trade_ledger,
      start_ts: exporter.start_ts,
      end_ts: exporter.end_ts,
      margin_sec: exporter.margin_sec,
    },
    funnel: {
      dogs: funnelReport.dogs.summary,
      duds: funnelReport.duds.summary,
    },
    guardrail: 'Low decision match rates can mean a partial paper DB or a real pipeline coverage gap. Do not change gate/exit/live size from this pack alone; inspect source coverage first.',
  };
  const summaryPath = path.join(args.outDir, 'decision-anchor-pack-summary.json');
  fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, summary_path: summaryPath, ...summary }, null, 2));
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main();
  } catch (error) {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  }
}
