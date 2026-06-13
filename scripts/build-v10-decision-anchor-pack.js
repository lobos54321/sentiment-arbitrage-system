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
    paperSubsetDb: '',
    snapshotTgz: '',
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
    if (key === '--paper-subset-db') { args.paperSubsetDb = next; i += 1; continue; }
    if (key === '--snapshot-tgz') { args.snapshotTgz = next; i += 1; continue; }
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
    '  node scripts/build-v10-decision-anchor-pack.js --paper-subset-db /path/to/paper_decision_subset.db',
    '  node scripts/build-v10-decision-anchor-pack.js --snapshot-tgz /path/to/rawdog-audit-dbs.tgz',
    '',
    'One-command v10 decision-anchor pack builder:',
    '  1. export or import paper_decision_subset.db',
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

function runSqlJson(dbPath, sql) {
  const out = execFileSync('sqlite3', ['-json', dbPath, sql], {
    encoding: 'utf8',
    maxBuffer: 20 * 1024 * 1024,
  }).trim();
  return out ? JSON.parse(out) : [];
}

function runSqlText(dbPath, sql) {
  return execFileSync('sqlite3', [dbPath, sql], {
    encoding: 'utf8',
    maxBuffer: 20 * 1024 * 1024,
  }).trim();
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function rate(num, den) {
  return den ? num / den : null;
}

function numberOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function numericTs(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (Number.isFinite(n)) return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return Math.floor(parsed / 1000);
  }
  return null;
}

function loadJsonRows(filePath) {
  const parsed = readJson(filePath);
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed.results)) return parsed.results;
  if (Array.isArray(parsed.rows)) return parsed.rows;
  return [];
}

function deriveCohortWindow(dogsPath, dudsPath) {
  const ts = [
    ...loadJsonRows(dogsPath),
    ...loadJsonRows(dudsPath),
  ].map((row) => numericTs(row.signal_ts)).filter((value) => value != null).sort((a, b) => a - b);
  if (!ts.length) throw new Error('Could not derive cohort window from dogs/duds signal_ts');
  return { start: ts[0], end: ts[ts.length - 1] };
}

function tableExists(dbPath, tableName) {
  const escaped = String(tableName).replaceAll("'", "''");
  return runSqlText(dbPath, `SELECT 1 FROM sqlite_master WHERE type='table' AND name='${escaped}' LIMIT 1;`) === '1';
}

function tableRange(dbPath, tableName, tsExpr) {
  if (!tableExists(dbPath, tableName)) return { rows: 0, min_ts: null, max_ts: null };
  const [row] = runSqlJson(dbPath, `SELECT count(*) AS rows, min(${tsExpr}) AS min_ts, max(${tsExpr}) AS max_ts FROM ${tableName};`);
  return row || { rows: 0, min_ts: null, max_ts: null };
}

function subsetManifest(dbPath) {
  if (!tableExists(dbPath, 'export_manifest')) return null;
  const [row] = runSqlJson(dbPath, 'SELECT * FROM export_manifest LIMIT 1;');
  return row || null;
}

function inspectDecisionSubset(dbPath, dogsPath, dudsPath, sourceMode = 'paper_subset_db') {
  const manifest = subsetManifest(dbPath);
  const cohortWindow = deriveCohortWindow(dogsPath, dudsPath);
  const startTs = numberOrNull(manifest?.start_ts) ?? cohortWindow.start;
  const endTs = numberOrNull(manifest?.end_ts) ?? cohortWindow.end;
  const marginSec = numberOrNull(manifest?.margin_sec);
  const aClass = tableRange(dbPath, 'a_class_decision_events', 'event_ts');
  const opportunity = tableRange(dbPath, 'opportunity_events', 'event_ts');
  const ledger = tableRange(dbPath, 'canonical_trade_ledger', 'entry_ts');
  return {
    out_db: dbPath,
    source_mode: sourceMode,
    source_paper_db: manifest?.source_paper_db || null,
    source_window: manifest?.source_window || 'unknown',
    a_class_decision_events: aClass.rows || 0,
    opportunity_events: opportunity.rows || 0,
    canonical_trade_ledger: ledger.rows || 0,
    start_ts: startTs,
    end_ts: endTs,
    margin_sec: marginSec,
    exported_table_ranges: {
      a_class_decision_events: aClass,
      opportunity_events: opportunity,
      canonical_trade_ledger: ledger,
    },
  };
}

function copyIfDifferent(src, dst) {
  if (path.resolve(src) === path.resolve(dst)) return;
  fs.copyFileSync(src, dst);
}

function findFile(root, fileName) {
  const entries = fs.readdirSync(root, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(root, entry.name);
    if (entry.isFile() && entry.name === fileName) return fullPath;
    if (entry.isDirectory()) {
      const found = findFile(fullPath, fileName);
      if (found) return found;
    }
  }
  return '';
}

function extractSnapshotTgz(snapshotTgz, outDir) {
  const extractDir = path.join(outDir, 'snapshot-extract');
  fs.rmSync(extractDir, { recursive: true, force: true });
  fs.mkdirSync(extractDir, { recursive: true });
  execFileSync('tar', ['-xzf', snapshotTgz, '-C', extractDir], { stdio: ['ignore', 'pipe', 'pipe'] });
  const subset = findFile(extractDir, 'paper_decision_subset.db');
  if (!subset) {
    throw new Error(`Snapshot tgz did not contain paper_decision_subset.db: ${snapshotTgz}`);
  }
  return { extractDir, subset };
}

function tableRangeWarnings(exporter = {}) {
  const warnings = [];
  const startTs = numberOrNull(exporter.start_ts);
  const endTs = numberOrNull(exporter.end_ts);
  const ranges = exporter.exported_table_ranges || {};
  const decisionTables = ['a_class_decision_events', 'opportunity_events'];
  if (startTs == null || endTs == null) return warnings;

  for (const table of decisionTables) {
    const range = ranges[table] || {};
    const rows = Number(range.rows || 0);
    const minTs = numberOrNull(range.min_ts);
    const maxTs = numberOrNull(range.max_ts);
    if (!rows) {
      warnings.push(`${table}_empty_in_exported_window`);
      continue;
    }
    if (minTs != null && minTs > startTs + 3600) {
      warnings.push(`${table}_starts_more_than_1h_after_cohort_start`);
    }
    if (maxTs != null && maxTs < endTs - 3600) {
      warnings.push(`${table}_ends_more_than_1h_before_cohort_end`);
    }
  }
  return warnings;
}

function packStatus(report, exporter = {}) {
  const dogs = report.dogs?.summary || {};
  const duds = report.duds?.summary || {};
  const dogDecisionRate = rate(Number(dogs.has_decision_record || 0), Number(dogs.raw_sustained_dogs || 0));
  const dudDecisionRate = rate(Number(duds.has_decision_record || 0), Number(duds.raw_sustained_dogs || 0));
  const warnings = [...tableRangeWarnings(exporter)];
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
  const sourceCount = [args.paperDb, args.paperSubsetDb, args.snapshotTgz].filter(Boolean).length;
  if (sourceCount !== 1) {
    throw new Error('Provide exactly one of --paper-db, --paper-subset-db, or --snapshot-tgz');
  }
  if (args.paperDb && !fs.existsSync(args.paperDb)) throw new Error('Provide an existing --paper-db');
  if (args.paperSubsetDb && !fs.existsSync(args.paperSubsetDb)) throw new Error('Provide an existing --paper-subset-db');
  if (args.snapshotTgz && !fs.existsSync(args.snapshotTgz)) throw new Error('Provide an existing --snapshot-tgz');
  if (!fs.existsSync(args.dogs)) throw new Error(`Missing dogs file: ${args.dogs}`);
  if (!fs.existsSync(args.duds)) throw new Error(`Missing duds file: ${args.duds}`);
  fs.mkdirSync(args.outDir, { recursive: true });

  const subsetDb = path.join(args.outDir, 'paper_decision_subset.db');
  const funnelDir = path.join(args.outDir, 'decision-funnel');
  let exporter;
  let sourcePaperSubsetDb = '';
  let snapshotExtractDir = '';
  if (args.paperDb) {
    exporter = runNode(path.join('scripts', 'export-paper-decision-subset.js'), [
      '--paper-db', args.paperDb,
      '--out-db', subsetDb,
      '--cohort-dogs', args.dogs,
      '--cohort-duds', args.duds,
      '--margin-sec', String(args.marginSec),
    ]);
  } else {
    if (args.snapshotTgz) {
      const extracted = extractSnapshotTgz(args.snapshotTgz, args.outDir);
      sourcePaperSubsetDb = extracted.subset;
      snapshotExtractDir = extracted.extractDir;
    } else {
      sourcePaperSubsetDb = args.paperSubsetDb;
    }
    copyIfDifferent(sourcePaperSubsetDb, subsetDb);
    exporter = inspectDecisionSubset(subsetDb, args.dogs, args.duds, args.snapshotTgz ? 'snapshot_tgz' : 'paper_subset_db');
  }
  const funnelRun = runNode(path.join('scripts', 'run-v10-decision-funnel-audit.js'), [
    '--paper-db', subsetDb,
    '--dogs', args.dogs,
    '--duds', args.duds,
    '--out-dir', funnelDir,
    '--decision-window-sec', String(args.decisionWindowSec),
    '--pre-signal-grace-sec', String(args.preSignalGraceSec),
  ]);
  const funnelReport = readJson(funnelRun.paths.jsonPath);
  const status = packStatus(funnelReport, exporter);
  const summary = {
    schema_version: 'v10_decision_anchor_pack.v1',
    generated_at: new Date().toISOString(),
    ...status,
    inputs: {
      source_mode: args.paperDb ? 'paper_db_export' : exporter.source_mode,
      source_paper_db: args.paperDb || exporter.source_paper_db || null,
      source_paper_subset_db: sourcePaperSubsetDb || null,
      source_snapshot_tgz: args.snapshotTgz || null,
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
      snapshot_extract_dir: snapshotExtractDir || null,
    },
    export_counts: {
      a_class_decision_events: exporter.a_class_decision_events,
      opportunity_events: exporter.opportunity_events,
      canonical_trade_ledger: exporter.canonical_trade_ledger,
      start_ts: exporter.start_ts,
      end_ts: exporter.end_ts,
      margin_sec: exporter.margin_sec,
      exported_table_ranges: exporter.exported_table_ranges || null,
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
