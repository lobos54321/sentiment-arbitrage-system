#!/usr/bin/env node
'use strict';

/**
 * run-oos-daily-operation.js
 *
 * Thin, fail-closed orchestrator for one daily OOS cycle. It SEQUENCES the
 * signed-off components (it does NOT re-implement any science):
 *   producer (build-daily-oos-sol-curve-cohort.js)
 *   -> OOS selection (selectOosCohort: window + training + already-cumulative + unit-dedup)
 *   -> signal_windows.csv + Dune SQL splice (oos-dune-trade-export.template.sql)
 *   -> [operator runs Dune on oos.sql] -> validate the export
 *   -> wrapper (run-daily-oos-accumulation.js: worklist -> decode -> feature table -> accumulate)
 *   -> QA report (build-oos-daily-qa-report.js)
 *   -> operation manifest (sha256 of every stage)
 *
 * Without --dune-trades it runs PREPARE-ONLY (emits oos cohort + signal_windows.csv
 * + oos.sql, then stops for the operator to run Dune). With --dune-trades it runs
 * the full INGEST. AUC is never read (no --look-point, no --reveal-sealed-auc).
 *
 * Fail-closed on: output path exists (no overwrite unless --force-smoke), missing
 * production commit / training tokens / snapshot, snapshot integrity not ok, prereg
 * lock mismatch, Dune metadata mismatch, validation out_of_window_trades > 0, and any
 * AUC/lookpoint/sealed artifact appearing.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { execFileSync } from 'child_process';

const SCRIPTS = path.dirname(new URL(import.meta.url).pathname);
const REPO = path.resolve(SCRIPTS, '..');
const NODE = process.execPath;
const DEFAULT_OOS_START = 1781222400; // 2026-06-12 00:00 UTC (post 6/6-6/11 training window)
const DEFAULT_PREREG = path.join(REPO, 'claudedocs/oos-sol-curve-unique-buyers-preregister.md');
const DEFAULT_PREREG_LOCK = path.join(REPO, 'claudedocs/oos-sol-curve-unique-buyers-preregister.sha256');
const DEFAULT_SCHEMA = 'v10_curve_stage_feature_table.v1';
const DEFAULT_TEMPLATE = path.join(SCRIPTS, 'oos-dune-trade-export.template.sql');

function sha256File(p) { return fs.existsSync(p) ? crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex') : null; }
function readJson(p) { return JSON.parse(fs.readFileSync(p, 'utf8')); }
function jsonlCount(p) { return fs.existsSync(p) ? fs.readFileSync(p, 'utf8').trim().split('\n').filter(Boolean).length : 0; }
function hasExactKey(obj, key) {
  if (!obj || typeof obj !== 'object') return false;
  if (Object.prototype.hasOwnProperty.call(obj, key)) return true;
  if (Array.isArray(obj)) return obj.some((v) => hasExactKey(v, key));
  return Object.values(obj).some((v) => hasExactKey(v, key));
}
function die(msg) { console.error(`run-oos-daily-operation: ${msg}`); process.exit(2); }
function gitHead() { try { return execFileSync('git', ['-C', REPO, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(); } catch { return null; } }

/**
 * Pure OOS cohort selection (exported for tests). Window (>= oosWindowStart) +
 * training-token exclusion + already-cumulative exclusion + (token_ca,signal_ts) dedup.
 */
function selectOosCohort(dogs, duds, { oosWindowStart, trainingTokens, cumulativeKeys }) {
  const stat = { date_excluded: 0, training_excluded: 0, already_in_cumulative: 0, signal_dedup_removed: 0 };
  const seen = new Set();
  const sel = (arr) => {
    const out = [];
    for (const r of arr.slice().sort((a, b) => (a.token_ca < b.token_ca ? -1 : a.token_ca > b.token_ca ? 1 : a.signal_ts - b.signal_ts))) {
      if (Number(r.signal_ts) < oosWindowStart) { stat.date_excluded += 1; continue; }
      if (trainingTokens.has(r.token_ca)) { stat.training_excluded += 1; continue; }
      const k = `${r.token_ca}|${r.signal_ts}`;
      if (cumulativeKeys.has(k)) { stat.already_in_cumulative += 1; continue; }
      if (seen.has(k)) { stat.signal_dedup_removed += 1; continue; }
      seen.add(k); out.push(r);
    }
    return out;
  };
  return { oosDogs: sel(dogs), oosDuds: sel(duds), stat };
}

function verifyPreregLock(specPath, lockPath) {
  if (!fs.existsSync(specPath) || !fs.existsSync(lockPath)) die(`prereg lock missing (spec=${fs.existsSync(specPath)} lock=${fs.existsSync(lockPath)})`);
  const expected = (fs.readFileSync(lockPath, 'utf8').trim().split(/\s+/)[0] || '').toLowerCase();
  const actual = crypto.createHash('sha256').update(fs.readFileSync(specPath)).digest('hex').toLowerCase();
  if (expected !== actual) die(`prereg_lock_mismatch: ${actual.slice(0, 12)} != ${expected.slice(0, 12)} (fail-closed)`);
  return actual;
}

function spliceDuneSql(rows, templatePath) {
  const tmpl = fs.readFileSync(templatePath, 'utf8');
  const q = (s) => String(s).replace(/'/g, "''");
  const wid = (i) => `w${String(i + 1).padStart(5, '0')}`;
  const values = rows.map((r, i) => `    ('${wid(i)}', '${q(r.token_ca)}', ${r.signal_ts}, ${r.signal_ts - 900}, ${r.signal_ts}, '${r.label}', '${q(r.return_domain)}', '${q(r.effective_tier || r.tier || 'unknown')}')`).join(',\n');
  return tmpl.replace('{{SIGNAL_WINDOWS_VALUES}}', values);
}

function parseArgs(argv) {
  const a = { oosWindowStart: DEFAULT_OOS_START, prereg: DEFAULT_PREREG, preregLock: DEFAULT_PREREG_LOCK, schemaVersion: DEFAULT_SCHEMA, template: DEFAULT_TEMPLATE, forceSmoke: false };
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i]; const v = argv[i + 1];
    const take = () => { i += 1; return v; };
    switch (k) {
      case '--pack-id': a.packId = take(); break;
      case '--snapshot': a.snapshot = take(); break;
      case '--production-commit': a.productionCommit = take(); break;
      case '--dune-trades': a.duneTrades = take(); break;
      case '--dune-manifest': a.duneManifest = take(); break;
      case '--dune-validation': a.duneValidation = take(); break;
      case '--cumulative-dir': a.cumulativeDir = take(); break;
      case '--out-dir': a.outDir = take(); break;
      case '--oos-window-start': a.oosWindowStart = Number(take()); break;
      case '--training-tokens': a.trainingTokens = take(); break;
      case '--prereg': a.prereg = take(); break;
      case '--prereg-lock': a.preregLock = take(); break;
      case '--schema-version': a.schemaVersion = take(); break;
      case '--dune-sql-template': a.template = take(); break;
      case '--force-smoke': a.forceSmoke = true; break;
      case '--reveal-sealed-auc': die('refused: --reveal-sealed-auc is never permitted (prereg seal discipline).'); break;
      case '--look-point': die('refused: --look-point is not driven by this operator; AUC is withheld until cumulative reaches a milestone (the accumulator guards it).'); break;
      case '--help': case '-h': a.help = true; break;
      default: die(`unknown arg: ${k}`);
    }
  }
  return a;
}

function run(label, script, args) {
  console.error(`\n[stage] ${label}: ${path.basename(script)} ${args.join(' ')}`);
  execFileSync(NODE, [path.join(SCRIPTS, script), ...args], { stdio: 'inherit' });
}

function main() {
  const a = parseArgs(process.argv);
  if (a.help) { console.log('usage: run-oos-daily-operation.js --pack-id <id> --snapshot <db> --production-commit <hash> --cumulative-dir <d> --out-dir <d> --training-tokens <f> [--oos-window-start <unix>] [--dune-trades <f> --dune-manifest <f> --dune-validation <f>] [--force-smoke]'); return; }
  // ---- fail-closed preconditions ----
  if (!a.packId) die('--pack-id required (unique).');
  if (!a.snapshot) die('--snapshot required.');
  if (!a.cumulativeDir) die('--cumulative-dir required.');
  if (!a.outDir) die('--out-dir required.');
  if (!a.trainingTokens) die('--training-tokens required.');
  if (!a.productionCommit && !a.forceSmoke) die('--production-commit required (or --force-smoke for a throwaway run).');
  if (fs.existsSync(a.outDir) && !a.forceSmoke) die(`output path exists: ${a.outDir} (no overwrite; use a unique pack-id/out-dir or --force-smoke).`);
  if (!fs.existsSync(a.snapshot)) die(`snapshot not found: ${a.snapshot}`);
  if (!fs.existsSync(a.trainingTokens)) die(`training tokens not found: ${a.trainingTokens}`);
  const integ = execFileSync('sqlite3', [`file:${path.resolve(a.snapshot)}?mode=ro`, 'PRAGMA integrity_check;'], { encoding: 'utf8' }).trim();
  if (integ !== 'ok') die(`snapshot integrity_check != ok: ${integ}`);
  const preregSha = verifyPreregLock(a.prereg, a.preregLock);
  fs.mkdirSync(a.outDir, { recursive: true });
  const cohortDir = path.join(a.outDir, 'cohort');

  // ---- stage 1: producer ----
  run('producer', 'build-daily-oos-sol-curve-cohort.js', ['--snapshot', a.snapshot, '--out-dir', cohortDir]);

  // ---- stage 2: OOS selection (window + training + already-cumulative + unit-dedup) ----
  const dogs = readJson(path.join(cohortDir, 'clean-dogs.json'));
  const duds = readJson(path.join(cohortDir, 'clean-duds.json'));
  const trainingTokens = new Set(fs.readFileSync(a.trainingTokens, 'utf8').trim().split('\n').map((s) => s.trim().split(/[|,\s]/)[0]).filter(Boolean));
  const cumPath = path.join(a.cumulativeDir, 'cumulative_oos_features.jsonl');
  const cumulativeKeys = new Set(fs.existsSync(cumPath) ? fs.readFileSync(cumPath, 'utf8').trim().split('\n').filter(Boolean).map((l) => { const r = JSON.parse(l); return `${r.token_ca}|${r.signal_ts}`; }) : []);
  const { oosDogs, oosDuds, stat } = selectOosCohort(dogs, duds, { oosWindowStart: a.oosWindowStart, trainingTokens, cumulativeKeys });
  fs.writeFileSync(path.join(cohortDir, 'oos-dogs.json'), JSON.stringify(oosDogs, null, 2));
  fs.writeFileSync(path.join(cohortDir, 'oos-duds.json'), JSON.stringify(oosDuds, null, 2));
  fs.writeFileSync(path.join(cohortDir, 'oos-cohort-selection.json'), JSON.stringify({ oos_window_start_ts: a.oosWindowStart, unit: '(token_ca, signal_ts)', cohort_stage_exclusions: stat, dogs: oosDogs.length, duds: oosDuds.length }, null, 2));
  console.error(`[oos-selection] dogs=${oosDogs.length} duds=${oosDuds.length} exclusions=${JSON.stringify(stat)}`);

  // ---- stage 3: signal_windows.csv + Dune SQL splice ----
  const duneDir = path.join(a.outDir, 'dune'); fs.mkdirSync(duneDir, { recursive: true });
  const allRows = [...oosDogs, ...oosDuds].sort((x, y) => (x.token_ca < y.token_ca ? -1 : x.token_ca > y.token_ca ? 1 : x.signal_ts - y.signal_ts));
  const wid = (i) => `w${String(i + 1).padStart(5, '0')}`;
  const csv = ['window_id,token_ca,signal_ts,window_start_ts,window_end_ts,label,return_domain,effective_tier',
    ...allRows.map((r, i) => `${wid(i)},${r.token_ca},${r.signal_ts},${r.signal_ts - 900},${r.signal_ts},${r.label},${r.return_domain},${r.effective_tier || r.tier || 'unknown'}`)].join('\n') + '\n';
  fs.writeFileSync(path.join(duneDir, 'signal_windows.csv'), csv);
  fs.writeFileSync(path.join(duneDir, 'oos.sql'), spliceDuneSql(allRows, a.template));

  const opManifest = {
    schema_version: 'oos_daily_operation_manifest.v1', pack_id: a.packId,
    production_commit: a.productionCommit || null, research_commit: gitHead(),
    prereg_sha256: preregSha, oos_window_start_ts: a.oosWindowStart,
    snapshot: { path: path.resolve(a.snapshot), sha256: sha256File(a.snapshot), integrity: integ },
    oos_selection: stat, cohort: { oos_dogs: oosDogs.length, oos_duds: oosDuds.length },
    stage_sha256: {
      clean_dogs: sha256File(path.join(cohortDir, 'clean-dogs.json')),
      clean_duds: sha256File(path.join(cohortDir, 'clean-duds.json')),
      oos_dogs: sha256File(path.join(cohortDir, 'oos-dogs.json')),
      oos_duds: sha256File(path.join(cohortDir, 'oos-duds.json')),
      signal_windows: sha256File(path.join(duneDir, 'signal_windows.csv')),
      oos_sql: sha256File(path.join(duneDir, 'oos.sql')),
    },
  };

  // ---- PREPARE-ONLY: no Dune export yet ----
  if (!a.duneTrades) {
    opManifest.phase = 'prepare_only';
    opManifest.next = `Run Dune on ${path.join(duneDir, 'oos.sql')}, then re-run with --dune-trades/--dune-manifest/--dune-validation.`;
    fs.writeFileSync(path.join(a.outDir, 'operation-manifest.json'), JSON.stringify(opManifest, null, 2));
    console.log(JSON.stringify({ ok: true, phase: 'prepare_only', oos_dogs: oosDogs.length, oos_duds: oosDuds.length, signal_windows: path.join(duneDir, 'signal_windows.csv'), oos_sql: path.join(duneDir, 'oos.sql') }, null, 2));
    return;
  }

  // ---- stage 4: copy the provided Dune export into the operation dir (self-contained,
  //               auditable) + validate it (fail-closed) on the local copies. ----
  if (!a.duneManifest || !a.duneValidation) die('--dune-trades requires --dune-manifest and --dune-validation.');
  const duneTradesLocal = path.join(duneDir, 'trades.jsonl');
  const duneManifestLocal = path.join(duneDir, 'dune-manifest.json');
  const duneValidationLocal = path.join(duneDir, 'validation.json');
  fs.copyFileSync(a.duneTrades, duneTradesLocal);
  fs.copyFileSync(a.duneManifest, duneManifestLocal);
  fs.copyFileSync(a.duneValidation, duneValidationLocal);
  const dm = readJson(duneManifestLocal); const dv = readJson(duneValidationLocal);
  const tradeRows = jsonlCount(duneTradesLocal); const tradeSha = sha256File(duneTradesLocal);
  if (dm.row_count !== tradeRows) die(`dune metadata mismatch: manifest row_count ${dm.row_count} != trades.jsonl lines ${tradeRows}`);
  if (dv.summary && dv.summary.trades_n !== tradeRows) die(`dune metadata mismatch: validation trades_n ${dv.summary.trades_n} != trades.jsonl lines ${tradeRows}`);
  if (dm.out_jsonl_sha256 && dm.out_jsonl_sha256 !== tradeSha) die(`dune sha mismatch: manifest ${String(dm.out_jsonl_sha256).slice(0, 12)} != actual ${String(tradeSha).slice(0, 12)}`);
  if (dv.summary && Number(dv.summary.out_of_window_trades_n) > 0) die(`validation out_of_window_trades_n = ${dv.summary.out_of_window_trades_n} > 0 (feature_ts must be <= signal_ts; fail-closed)`);

  // ---- stage 5: wrapper (worklist -> decode -> feature table -> accumulate). No --look-point, no --reveal-sealed-auc. ----
  const workDir = path.join(a.outDir, 'work');
  run('wrapper', 'run-daily-oos-accumulation.js', [
    '--pack-id', a.packId, '--decode-mode', 'dune',
    '--dogs', path.join(cohortDir, 'oos-dogs.json'), '--duds', path.join(cohortDir, 'oos-duds.json'),
    '--trades', duneTradesLocal, '--dune-assume-complete-window', '--validated-trade-export', duneValidationLocal,
    '--production-commit', a.productionCommit, '--work-dir', workDir, '--cumulative-dir', a.cumulativeDir,
    '--training-tokens', a.trainingTokens, '--schema-version', a.schemaVersion,
    '--prereg', a.prereg, '--prereg-lock', a.preregLock,
  ]);

  // ---- stage 6: QA report (artifact-driven) ----
  run('qa-report', 'build-oos-daily-qa-report.js', ['--run-dir', a.outDir, '--cumulative-dir', a.cumulativeDir]);

  // ---- stage 7: assert no AUC leak (fail-closed) ----
  const accDir = path.join(workDir, 'accumulate-out');
  const leak = fs.readdirSync(accDir).filter((f) => /lookpoint|sealed|auc/i.test(f));
  if (leak.length) die(`AUC artifact leak: ${leak.join(', ')}`);
  const featureTable = readJson(path.join(workDir, 'curve-feature-table.json'));
  if (hasExactKey(featureTable, 'auc')) die('AUC field present in daily feature table before a look point (fail-closed).');
  const qa = readJson(path.join(accDir, 'daily_qa_report.json'));
  if ('auc' in qa) die('AUC field present in daily QA before a look point (fail-closed).');

  // ---- stage 8: operation manifest (full, sha256 of every stage) ----
  opManifest.phase = 'ingested';
  opManifest.dune = { execution_id: dm.execution_id, row_count: dm.row_count, out_jsonl_sha256: dm.out_jsonl_sha256, trades_sha256: tradeSha };
  opManifest.stage_sha256.feature_table = sha256File(path.join(workDir, 'curve-feature-table.json'));
  opManifest.stage_sha256.cumulative = sha256File(cumPath);
  opManifest.accumulator = { daily_cohort: qa.daily_cohort, cumulative_cohort: qa.cumulative_cohort, coverage_asymmetry_pp: qa.cumulative_coverage_asymmetry_pp, coverage_gate_ok: qa.coverage_gate.ok, auc_withheld: true };
  fs.writeFileSync(path.join(a.outDir, 'operation-manifest.json'), JSON.stringify(opManifest, null, 2));

  console.log(JSON.stringify({ ok: true, phase: 'ingested', pack_id: a.packId, daily_cohort: qa.daily_cohort, cumulative_cohort: qa.cumulative_cohort, coverage_gate_ok: qa.coverage_gate.ok, auc_withheld: true, out_dir: a.outDir }, null, 2));
}

if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) { main(); }

export { selectOosCohort, spliceDuneSql };
