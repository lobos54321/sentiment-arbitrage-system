#!/usr/bin/env node
'use strict';

/**
 * run-daily-oos-accumulation.js
 *
 * Daily orchestration wrapper for the locked OOS preregistration:
 *   claudedocs/oos-sol-curve-unique-buyers-preregister.md
 *
 * It chains the EXISTING stage scripts (it adds no new science):
 *   [optional] cohort rebuild -> curve-feature worklist -> decode (rpc | dune)
 *   -> curve-feature table -> accumulate-oos-features
 *
 * Cross-cutting guarantees this wrapper enforces (operator constraints):
 *   (#3) forces an explicit --schema-version into the accumulator AND verifies
 *        the feature-table's own manifest schema_version equals it before
 *        ingest (codex wrapper contract): a pack whose feature rows all lack
 *        schema_version is stamped from this value, so it MUST be the real
 *        feature-table schema or the pack is rejected (fail-closed);
 *   (#3) writes a pack manifest preserving production_commit (git HEAD) +
 *        schema_version + per-stage output sha256 (provenance/hash);
 *   (#4) DEFAULT runs QA/counts only: it never passes --look-point unless the
 *        cumulative has reached a preregistered look point (50/100/130) AND the
 *        operator explicitly requested it (guarded, fail-closed otherwise);
 *   (#5) never passes --reveal-sealed-auc (the flag is not even accepted).
 *
 * It changes nothing live and reads no AUC outside guarded, reached look points.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { execFileSync } from 'child_process';

const SCRIPTS = path.dirname(new URL(import.meta.url).pathname);
const REPO_ROOT = path.resolve(SCRIPTS, '..');
const NODE = process.execPath;
const ALLOWED_LOOK_POINTS = [50, 100, 130];
const DEFAULT_SCHEMA = 'v10_curve_stage_feature_table.v1';
const VALIDATOR_SCHEMA = 'v10_curve_feature_trade_export_validation.v1';
const DEFAULT_TRAINING_TOKENS = '/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/oos-training-token-exclusion/training-tokens.txt';
const DEFAULT_PREREG = path.join(REPO_ROOT, 'claudedocs/oos-sol-curve-unique-buyers-preregister.md');
const DEFAULT_PREREG_LOCK = path.join(REPO_ROOT, 'claudedocs/oos-sol-curve-unique-buyers-preregister.sha256');

function sha256File(p) { return fs.existsSync(p) ? crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex') : null; }
function readJson(p) { return JSON.parse(fs.readFileSync(p, 'utf8')); }
function gitHead() {
  try { return execFileSync('git', ['-C', REPO_ROOT, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(); }
  catch { return null; }
}
function nowIso() { return new Date().toISOString(); }

function die(msg) { console.error(`run-daily-oos-accumulation: ${msg}`); process.exit(2); }

function parseArgs(argv) {
  const a = {
    schemaVersion: DEFAULT_SCHEMA, trainingTokens: DEFAULT_TRAINING_TOKENS,
    prereg: DEFAULT_PREREG, preregLock: DEFAULT_PREREG_LOCK, dedupeWorklistForSmoke: false,
    dryRun: false,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i]; const v = argv[i + 1];
    const take = () => { i += 1; return v; };
    switch (k) {
      case '--pack-id': a.packId = take(); break;
      case '--decode-mode': a.decodeMode = take(); break;
      // input boundary A: start from already-rebuilt clean cohort
      case '--dogs': a.dogs = take(); break;
      case '--duds': a.duds = take(); break;
      // input boundary B: start from a label audit (+ supporting) -> cohort rebuild
      case '--rebuild-label-audit': a.rebuildLabelAudit = take(); break;
      case '--rebuild-baseline-routed': a.rebuildBaselineRouted = take(); break;
      case '--rebuild-gmgn-full-window': a.rebuildGmgnFullWindow = take(); break;
      case '--rebuild-peak-window': a.rebuildPeakWindow = take(); break;
      // decode source
      case '--rpc-url-file': a.rpcUrlFile = take(); break;
      case '--trades': a.trades = take(); break;
      case '--pre-sec': a.preSec = take(); break;
      case '--post-sec': a.postSec = take(); break;
      // dune completeness proof (required for dune; see validation below)
      case '--dune-assume-complete-window': a.duneAssumeCompleteWindow = true; break;
      case '--validated-trade-export': a.validatedTradeExport = take(); break;
      // provenance: production commit comes from the live pack, never research HEAD
      case '--production-commit': a.productionCommit = take(); break;
      case '--production-commit-from': a.productionCommitFrom = take(); break;
      case '--allow-unknown-production-commit-for-smoke': a.allowUnknownProductionCommitForSmoke = true; break;
      // config / outputs
      case '--work-dir': a.workDir = take(); break;
      case '--cumulative-dir': a.cumulativeDir = take(); break;
      case '--training-tokens': a.trainingTokens = take(); break;
      case '--schema-version': a.schemaVersion = take(); break;
      case '--prereg': a.prereg = take(); break;
      case '--prereg-lock': a.preregLock = take(); break;
      case '--look-point': a.lookPoint = Number(take()); break;
      case '--dedupe-worklist-for-smoke': a.dedupeWorklistForSmoke = true; break;
      case '--dry-run': a.dryRun = true; break;
      case '--reveal-sealed-auc':
        die('refused: --reveal-sealed-auc is never permitted from the daily wrapper (prereg seal discipline).');
        break;
      case '--help': case '-h': a.help = true; break;
      default: die(`unknown arg: ${k}`);
    }
  }
  return a;
}

function run(label, script, args, dryRun) {
  const full = [path.join(SCRIPTS, script), ...args];
  console.error(`\n[stage] ${label}: node ${script} ${args.join(' ')}`);
  if (dryRun) return;
  execFileSync(NODE, full, { stdio: 'inherit' });
}

function main() {
  const a = parseArgs(process.argv);
  if (a.help) {
    console.log('usage: run-daily-oos-accumulation.js --pack-id <id> --decode-mode <rpc|dune> '
      + '(--dogs <json> --duds <json> | --rebuild-label-audit <json> --rebuild-baseline-routed <jsonl> '
      + '--rebuild-gmgn-full-window <json> --rebuild-peak-window <json>) '
      + '(--rpc-url-file <f> | --trades <f> --dune-assume-complete-window [--validated-trade-export <f>]) '
      + '--work-dir <dir> --cumulative-dir <dir> '
      + '(--production-commit <hash> | --production-commit-from <manifest> | --allow-unknown-production-commit-for-smoke) '
      + '[--training-tokens <f>] [--schema-version <s>] [--look-point 50|100|130] '
      + '[--dedupe-worklist-for-smoke] [--dry-run]\n\n'
      + 'Notes: worklist is signal-level by default (OOS unit = token_ca,signal_ts); --dedupe-worklist-for-smoke '
      + 'is RPC-smoke ONLY. Dune REQUIRES --dune-assume-complete-window (operator attestation); --validated-trade-export '
      + 'is supporting QA only and cannot authorize complete-window. A formal pack must record production_commit; '
      + '--allow-unknown-production-commit-for-smoke permits null but forbids --look-point. --reveal-sealed-auc is never permitted.');
    return;
  }
  // ---- validate required inputs (fail-closed) ----
  if (!a.packId) die('--pack-id is required (daily idempotency key).');
  if (!a.workDir) die('--work-dir is required.');
  if (!a.cumulativeDir) die('--cumulative-dir is required.');
  if (a.decodeMode !== 'rpc' && a.decodeMode !== 'dune') die('--decode-mode must be rpc or dune (explicit; no default).');
  if (a.decodeMode === 'rpc' && !a.rpcUrlFile) die('--decode-mode rpc requires --rpc-url-file.');
  if (a.decodeMode === 'dune' && !a.trades) die('--decode-mode dune requires --trades <csv|jsonl>.');
  // Dune completeness gate (fail-closed): the from-trades decoder defaults to
  // history_reached_start=false -> feature table marks incomplete_window ->
  // accumulator excludes -> a Dune run would silently accumulate 0 usable rows.
  // The LOAD-BEARING gate is the OPERATOR ATTESTATION. The standalone validator,
  // by its own statement, "cannot prove completeness unless the export query/source
  // guarantees it" -> a validation file alone must NOT authorize complete-window.
  if (a.decodeMode === 'dune' && !a.duneAssumeCompleteWindow) {
    die('--decode-mode dune requires --dune-assume-complete-window: the operator must attest the Dune export query '
      + 'guarantees full [signal_ts-pre_sec, signal_ts] coverage. --validated-trade-export is supporting QA only and '
      + 'CANNOT authorize complete-window on its own (it cannot prove completeness). Without the attestation every row '
      + 'is incomplete_window and the pack accumulates 0 usable rows (fail-closed).');
  }
  // --validated-trade-export is optional supporting provenance; if present it must be
  // a genuine validator report (schema check), else fail-closed. It does NOT open the
  // complete-window path by itself — only the attestation above does.
  let validatedTradeExportReport = null;
  if (a.validatedTradeExport) {
    if (!fs.existsSync(a.validatedTradeExport)) die(`--validated-trade-export not found: ${a.validatedTradeExport}`);
    try { validatedTradeExportReport = readJson(a.validatedTradeExport); }
    catch { die(`--validated-trade-export is not valid JSON: ${a.validatedTradeExport}`); }
    if (validatedTradeExportReport.schema_version !== VALIDATOR_SCHEMA) {
      die(`--validated-trade-export schema "${validatedTradeExportReport.schema_version}" != "${VALIDATOR_SCHEMA}" `
        + '(not a genuine validate-v10-curve-feature-trade-export.js report; fail-closed).');
    }
  }
  const hasCohort = a.dogs && a.duds;
  const hasRebuild = a.rebuildLabelAudit && a.rebuildBaselineRouted && a.rebuildGmgnFullWindow && a.rebuildPeakWindow;
  if (!hasCohort && !hasRebuild) {
    die('provide EITHER --dogs/--duds (already-rebuilt cohort) OR the four --rebuild-* inputs (run cohort rebuild).');
  }
  if (a.lookPoint !== undefined && !ALLOWED_LOOK_POINTS.includes(a.lookPoint)) {
    die(`--look-point must be one of ${ALLOWED_LOOK_POINTS.join('/')} (preregistered).`);
  }
  if (!fs.existsSync(a.trainingTokens)) {
    die(`training tokens not found: ${a.trainingTokens} (run export-oos-training-tokens.js first).`);
  }
  // provenance fail-closed (resolved BEFORE stages run): a formal daily pack must record
  // the live pack-producing commit. research HEAD is never a substitute. A smoke run may
  // opt out, but then a formal look point is forbidden.
  let productionCommit = a.productionCommit || null;
  if (!productionCommit && a.productionCommitFrom) {
    try {
      const m = readJson(a.productionCommitFrom);
      productionCommit = m.production_commit || m.commit || m.git_commit || m.production_sha || null;
    } catch { productionCommit = null; }
  }
  if (!productionCommit) {
    if (!a.allowUnknownProductionCommitForSmoke) {
      die('production_commit unknown: pass --production-commit <hash> or --production-commit-from <live pack/health '
        + 'manifest>. A formal daily OOS pack must record the live pack-producing commit (research HEAD is not a '
        + 'substitute). For a throwaway smoke run pass --allow-unknown-production-commit-for-smoke (then --look-point '
        + 'is forbidden).');
    }
    if (a.lookPoint !== undefined) {
      die('--allow-unknown-production-commit-for-smoke cannot be combined with --look-point: a look point may only be '
        + 'read on a formal pack with a recorded production_commit (fail-closed).');
    }
    console.error('[provenance] WARNING: production_commit unknown; proceeding as SMOKE (look points forbidden).');
  }
  fs.mkdirSync(a.workDir, { recursive: true });

  const stageOut = {};
  const preSec = a.preSec ?? '900';
  const postSec = a.postSec ?? '0';

  // ---- stage 1 (optional): cohort rebuild from a daily label audit ----
  let dogs = a.dogs; let duds = a.duds;
  if (!hasCohort) {
    const rbDir = path.join(a.workDir, 'cohort-rebuild');
    run('cohort-rebuild', 'rebuild-chain-truth-rawdog-cohort.js', [
      '--label-audit', a.rebuildLabelAudit,
      '--baseline-routed', a.rebuildBaselineRouted,
      '--gmgn-full-window', a.rebuildGmgnFullWindow,
      '--peak-window', a.rebuildPeakWindow,
      '--out-dir', rbDir,
    ], a.dryRun);
    dogs = path.join(rbDir, 'rebuilt-clean-dogs.json');
    duds = path.join(rbDir, 'rebuilt-clean-duds.json');
  }

  // ---- stage 2: curve-feature worklist ----
  // OOS unit is (token_ca, signal_ts): NEVER dedupe to token level for formal daily
  // OOS — it would drop second signals of the same token and change the sample
  // denominator. --dedupe is permitted ONLY for explicit RPC smoke tests.
  const worklist = path.join(a.workDir, 'curve-feature-worklist.txt');
  run('worklist', 'build-v10-curve-feature-worklist.js', [
    '--dogs', dogs, '--duds', duds, '--out', worklist, ...(a.dedupeWorklistForSmoke ? ['--dedupe'] : []),
  ], a.dryRun);

  // ---- stage 3: decode (rpc batches+merge | dune from-trades) ----
  const mergedDecode = path.join(a.workDir, 'merged-decode.json');
  if (a.decodeMode === 'rpc') {
    const batchDir = path.join(a.workDir, 'decode-batches');
    run('decode:rpc', 'run-v10-curve-feature-decode-batches.js', [
      '--worklist', worklist, '--out-dir', batchDir, '--rpc-url-file', a.rpcUrlFile,
      '--pre-sec', String(preSec), '--post-sec', String(postSec),
    ], a.dryRun);
    run('decode:merge', 'merge-v10-curve-feature-decode-batches.js', [
      '--in-dir', batchDir, '--out', mergedDecode,
    ], a.dryRun);
  } else {
    // completeness proof was required in validation above; pass --assume-complete-window
    // so decoded windows are marked complete_window (else 0 usable rows).
    run('decode:dune', 'build-v10-curve-feature-decode-from-trades.js', [
      '--worklist', worklist, '--trades', a.trades, '--out', mergedDecode,
      '--pre-sec', String(preSec), '--post-sec', String(postSec),
      '--assume-complete-window',
    ], a.dryRun);
  }

  // ---- stage 4: curve-feature table ----
  const featureTable = path.join(a.workDir, 'curve-feature-table.json');
  run('feature-table', 'build-v10-curve-feature-table.js', [
    '--dogs', dogs, '--duds', duds, '--decode', mergedDecode, '--out', featureTable,
  ], a.dryRun);

  if (a.dryRun) { console.error('\n[dry-run] stages printed; not executed.'); return; }

  // ---- (#3) schema contract: feature-table manifest schema MUST equal the forced schema ----
  const ft = readJson(featureTable);
  if (ft.schema_version && ft.schema_version !== a.schemaVersion) {
    die(`schema_contract_violation: feature-table schema_version "${ft.schema_version}" != forced --schema-version `
      + `"${a.schemaVersion}". Refusing to stamp a mislabeled schema into the cumulative (fail-closed).`);
  }

  // ---- pack manifest: research/production commit + schema_version + completeness + per-stage sha256 ----
  // (productionCommit + validatedTradeExportReport were resolved/validated up-front.)
  stageOut.dogs = dogs; stageOut.duds = duds; stageOut.decode = mergedDecode; stageOut.feature_table = featureTable;
  const manifest = {
    schema_version: a.schemaVersion,
    research_commit: gitHead(),
    production_commit: productionCommit,
    production_commit_smoke_unknown: !productionCommit || undefined,
    pack_id: a.packId,
    generated_at: nowIso(),
    decode_mode: a.decodeMode,
    dune_completeness: a.decodeMode === 'dune'
      ? {
        attested: Boolean(a.duneAssumeCompleteWindow),
        validated_trade_export: a.validatedTradeExport || null,
        validated_trade_export_sha256: a.validatedTradeExport ? sha256File(a.validatedTradeExport) : null,
        validated_trade_export_summary: validatedTradeExportReport ? (validatedTradeExportReport.summary || null) : null,
      }
      : null,
    feature_window: { pre_sec: Number(preSec), post_sec: Number(postSec) },
    stage_sha256: Object.fromEntries(Object.entries(stageOut).map(([k, p]) => [k, sha256File(p)])),
  };
  const manifestPath = path.join(a.workDir, 'pack-manifest.json');
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));

  // ---- stage 5: accumulate (QA/counts only by default; NO look-point, NO seal) ----
  const accOut = path.join(a.workDir, 'accumulate-out');
  fs.mkdirSync(accOut, { recursive: true });
  run('accumulate:qa', 'accumulate-oos-features.js', [
    '--feature-rows', featureTable,
    '--pack-id', a.packId,
    '--pack-manifest', manifestPath,
    '--training-tokens', a.trainingTokens,
    '--cumulative-dir', a.cumulativeDir,
    '--out-dir', accOut,
    '--schema-version', a.schemaVersion,
    '--prereg', a.prereg,
    '--prereg-lock', a.preregLock,
  ], false);

  // ---- (#4) guarded look point: ONLY if requested AND the milestone is reached ----
  let lookSummary = { requested: a.lookPoint ?? null, ran: false, reason: 'default QA/counts only (no look point requested)' };
  if (a.lookPoint !== undefined) {
    const qa = readJson(path.join(accOut, 'daily_qa_report.json'));
    const cc = qa.cumulative_cohort || { dog: 0, dud: 0 };
    if (cc.dog >= a.lookPoint && cc.dud >= a.lookPoint) {
      run('accumulate:lookpoint', 'accumulate-oos-features.js', [
        '--feature-rows', featureTable,
        '--pack-id', a.packId,
        '--pack-manifest', manifestPath,
        '--training-tokens', a.trainingTokens,
        '--cumulative-dir', a.cumulativeDir,
        '--out-dir', accOut,
        '--schema-version', a.schemaVersion,
        '--prereg', a.prereg,
        '--prereg-lock', a.preregLock,
        '--look-point', String(a.lookPoint),
        // NOTE: --reveal-sealed-auc is intentionally NEVER forwarded.
      ], false);
      lookSummary = { requested: a.lookPoint, ran: true, cumulative: cc };
    } else {
      lookSummary = {
        requested: a.lookPoint, ran: false,
        reason: `milestone not reached: cumulative dog=${cc.dog} dud=${cc.dud} < ${a.lookPoint} per class. `
          + 'Look point withheld (fail-closed); QA/counts already written.',
        cumulative: cc,
      };
      console.error(`\n[look-point] ${lookSummary.reason}`);
    }
  }

  console.log(JSON.stringify({
    ok: true, pack_id: a.packId, decode_mode: a.decodeMode,
    schema_version: a.schemaVersion,
    research_commit: manifest.research_commit, production_commit: manifest.production_commit,
    manifest: manifestPath, accumulate_out: accOut, cumulative_dir: a.cumulativeDir,
    look_point: lookSummary,
  }, null, 2));
}

if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  main();
}

export { parseArgs };
