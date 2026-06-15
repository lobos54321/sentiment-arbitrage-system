#!/usr/bin/env node
'use strict';

/**
 * accumulate-oos-features.js
 *
 * Daily OOS feature accumulator for the locked preregistration:
 *   claudedocs/oos-sol-curve-unique-buyers-preregister.md
 *
 * This tool enforces the preregistration as STRUCTURE, not convention:
 *   - fail-closed if the prereg spec hash does not match its lock file;
 *   - hard gates: training-window token exclusion -> cohort filter -> dedup -> count;
 *   - AUC physically cannot be computed unless an allowed look point (50/100/130
 *     per class) is reached and explicitly requested;
 *   - n=50 is futility-only: the verdict is emitted, the AUC value is sealed in a
 *     report file and never printed to the daily/stdout surface;
 *   - idempotent: the same pack_id is never appended to the cumulative table twice.
 *
 * It is a measurement/research tool. It changes nothing live and reads no AUC
 * outside the preregistered look points.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { execFileSync } from 'child_process';
import { fileURLToPath } from 'url';

const ALLOWED_LOOK_POINTS = [50, 100, 130];
const COHORT_DOMAIN = 'sol_curve';
const REQUIRED_ROW_FIELDS = [
  'token_ca', 'signal_ts', 'label', 'return_domain', 'trades_n',
  'feature_coverage_status', 'unique_buyers',
];
const PRIMARY_FEATURE = 'unique_buyers';

// ---------- small utils ----------
function readJson(p) { return JSON.parse(fs.readFileSync(p, 'utf8')); }
function readRows(p) {
  const txt = fs.readFileSync(p, 'utf8').trim();
  if (!txt) return [];
  if (p.endsWith('.jsonl')) {
    return txt.split('\n').filter(Boolean).map((l) => JSON.parse(l));
  }
  const d = JSON.parse(txt);
  return d.rows || d.feature_rows || (Array.isArray(d) ? d : []);
}
function writeJsonl(p, rows) {
  fs.writeFileSync(p, rows.map((r) => JSON.stringify(r)).join('\n') + (rows.length ? '\n' : ''));
}
function appendJsonl(p, rows) {
  if (!rows.length) return;
  fs.appendFileSync(p, rows.map((r) => JSON.stringify(r)).join('\n') + '\n');
}
function numeric(v) { return (v === null || v === undefined || Number.isNaN(Number(v))) ? null : Number(v); }
function hasTrades(row) { return (numeric(row.trades_n) || 0) > 0; }
function sigKey(row) { return `${row.token_ca}|${row.signal_ts}`; }

// reproducible RNG so the bootstrap CI is identical on re-run of a locked test
function mulberry32(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0; a = (a + 0x6D2B79F5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ---------- preregistration lock (fail-closed) ----------
function verifyPreregLock(specPath, lockPath) {
  if (!fs.existsSync(specPath) || !fs.existsSync(lockPath)) {
    throw new Error(`prereg_lock_missing: spec=${fs.existsSync(specPath)} lock=${fs.existsSync(lockPath)}`);
  }
  // cwd-independent: parse the expected hash from the lock and compare to the
  // actual sha256 of the spec ourselves (no reliance on shasum's path resolution).
  const expected = (fs.readFileSync(lockPath, 'utf8').trim().split(/\s+/)[0] || '').toLowerCase();
  const actual = crypto.createHash('sha256').update(fs.readFileSync(specPath)).digest('hex').toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(expected)) {
    throw new Error(`prereg_lock_unreadable: no sha256 found in ${lockPath} (fail-closed)`);
  }
  if (expected !== actual) {
    throw new Error(`prereg_lock_mismatch: ${specPath} sha256 ${actual.slice(0, 12)} != locked ${expected.slice(0, 12)} (fail-closed)`);
  }
  return actual;
}

// ---------- hard gates (LOCKED ORDER: exclude training -> cohort filter -> dedup) ----------
function applyGates(rows, { trainingTokens, seenSigKeys }) {
  const stats = {
    input_rows: rows.length,
    excluded_training_token_count: 0,
    excluded_missing_fields: 0,
    excluded_cross_pack_duplicate: 0,
    deduped_within_pack: 0,
  };
  // required fields
  let r = rows.filter((row) => {
    const ok = REQUIRED_ROW_FIELDS.every((f) => row[f] !== undefined);
    if (!ok) stats.excluded_missing_fields += 1;
    return ok;
  });
  // 1. training-window token exclusion (Section 2, mandatory)
  r = r.filter((row) => {
    if (trainingTokens.has(row.token_ca)) { stats.excluded_training_token_count += 1; return false; }
    return true;
  });
  // pre-cohort coverage symmetry is measured on r (post training exclusion, pre cohort filter)
  const preCohort = r;
  // 2. cohort filter: sol_curve AND has_trades
  let cohort = r.filter((row) => row.return_domain === COHORT_DOMAIN && hasTrades(row));
  // 3a. dedup within pack by (token_ca, signal_ts)
  const withinSeen = new Set();
  cohort = cohort.filter((row) => {
    const k = sigKey(row);
    if (withinSeen.has(k)) { stats.deduped_within_pack += 1; return false; }
    withinSeen.add(k); return true;
  });
  // 3b. dedup vs cumulative (idempotency / cross-pack)
  cohort = cohort.filter((row) => {
    if (seenSigKeys.has(sigKey(row))) { stats.excluded_cross_pack_duplicate += 1; return false; }
    return true;
  });
  return { cohortRows: cohort, preCohortRows: preCohort, stats };
}

// ---------- QA counts ----------
function rate(num, den) { return den > 0 ? Number((num / den).toFixed(4)) : null; }
function classCounts(rows) {
  const dog = rows.filter((r) => r.label === 'dog');
  const dud = rows.filter((r) => r.label === 'dud');
  return { dog: dog.length, dud: dud.length, dogRows: dog, dudRows: dud };
}
function symmetryReport(preCohortRows) {
  // measured on post-training, pre-cohort-filter rows so coverage asymmetry is visible
  const out = {};
  for (const lab of ['dog', 'dud']) {
    const s = preCohortRows.filter((r) => r.label === lab);
    const solc = s.filter((r) => r.return_domain === COHORT_DOMAIN);
    out[lab] = {
      n: s.length,
      sol_curve_n: solc.length,
      sol_curve_has_trades_n: solc.filter(hasTrades).length,
      trade_hit_rate: rate(s.filter(hasTrades).length, s.length),
      complete_window_rate: rate(s.filter((r) => r.feature_coverage_status === 'complete_window').length, s.length),
    };
  }
  out.trade_hit_asymmetry_pp = (out.dog.trade_hit_rate !== null && out.dud.trade_hit_rate !== null)
    ? Number(((out.dog.trade_hit_rate - out.dud.trade_hit_rate) * 100).toFixed(1)) : null;
  out.complete_window_asymmetry_pp = (out.dog.complete_window_rate !== null && out.dud.complete_window_rate !== null)
    ? Number(((out.dog.complete_window_rate - out.dud.complete_window_rate) * 100).toFixed(1)) : null;
  return out;
}

// ---------- AUC / bootstrap (GUARDED: only callable at a reached look point) ----------
function aucRaw(dogValues, dudValues) {
  const d = dogValues.filter((v) => v !== null && v !== undefined);
  const u = dudValues.filter((v) => v !== null && v !== undefined);
  if (!d.length || !u.length) return null;
  let wins = 0;
  for (const a of d) for (const b of u) { if (a > b) wins += 1; else if (a === b) wins += 0.5; }
  return wins / (d.length * u.length);
}
function bootstrapCi(dogValues, dudValues, iters = 2000) {
  const rng = mulberry32(1234567);
  const d = dogValues.filter((v) => v !== null && v !== undefined);
  const u = dudValues.filter((v) => v !== null && v !== undefined);
  if (!d.length || !u.length) return null;
  const samp = (arr) => arr[Math.floor(rng() * arr.length)];
  const aucs = [];
  for (let i = 0; i < iters; i += 1) {
    const db = Array.from({ length: d.length }, () => samp(d));
    const ub = Array.from({ length: u.length }, () => samp(u));
    aucs.push(aucRaw(db, ub));
  }
  aucs.sort((a, b) => a - b);
  return { lo: aucs[Math.floor(iters * 0.025)], hi: aucs[Math.floor(iters * 0.975)] };
}
function topkPrecision(rows, k) {
  const ranked = [...rows].filter((r) => numeric(r[PRIMARY_FEATURE]) !== null)
    .sort((a, b) => numeric(b[PRIMARY_FEATURE]) - numeric(a[PRIMARY_FEATURE]));
  const top = ranked.slice(0, k);
  if (!top.length) return null;
  return Number((top.filter((r) => r.label === 'dog').length / top.length).toFixed(4));
}

/**
 * The ONLY entry point that computes AUC. It refuses unless a valid look point is
 * reached. n=50 returns a futility verdict WITHOUT exposing the AUC point estimate
 * on the public surface (the caller seals it into a report file).
 */
function lookpointAnalysis(cohortRows, lookPoint) {
  if (!ALLOWED_LOOK_POINTS.includes(lookPoint)) {
    throw new Error(`look_point_not_allowed: ${lookPoint} not in ${ALLOWED_LOOK_POINTS.join('/')}`);
  }
  const { dog, dud, dogRows, dudRows } = classCounts(cohortRows);
  if (dog < lookPoint || dud < lookPoint) {
    throw new Error(`look_point_not_reached: dog=${dog} dud=${dud} < ${lookPoint} per class (AUC withheld)`);
  }
  const dogVals = dogRows.map((r) => numeric(r[PRIMARY_FEATURE]));
  const dudVals = dudRows.map((r) => numeric(r[PRIMARY_FEATURE]));
  const auc = aucRaw(dogVals, dudVals);
  const ci = bootstrapCi(dogVals, dudVals);
  const baseDogRate = dog / (dog + dud);

  if (lookPoint === 50) {
    // FUTILITY ONLY. Do not expose AUC on the returned public verdict.
    const directionalNull = auc <= 0.55 || (ci && ci.lo <= 0.50);
    return {
      public: { look_point: 50, mode: 'futility_only', directional_null: directionalNull,
        instruction: directionalNull
          ? 'STOP accumulation; mark directional_null; return to sourcing/target review unless user authorizes extension.'
          : 'CONTINUE accumulation toward n>=100. Success cannot be declared at n=50.' },
      sealed: { look_point: 50, primary: PRIMARY_FEATURE, auc, ci, base_dog_rate: baseDogRate, n_dog: dog, n_dud: dud },
    };
  }

  // n=100 / n=130: success-eligible look. Full required controls.
  const tp = cohortRows; // already trades-present by cohort definition
  const aucTradesPresent = aucRaw(
    tp.filter((r) => r.label === 'dog').map((r) => numeric(r[PRIMARY_FEATURE])),
    tp.filter((r) => r.label === 'dud').map((r) => numeric(r[PRIMARY_FEATURE])),
  );
  const stageKeys = [...new Set(cohortRows.map((r) => r.progress_stage))];
  const stageSplit = {};
  for (const sk of stageKeys) {
    const sr = cohortRows.filter((r) => r.progress_stage === sk);
    stageSplit[sk] = {
      n_dog: sr.filter((r) => r.label === 'dog').length,
      n_dud: sr.filter((r) => r.label === 'dud').length,
      auc: aucRaw(sr.filter((r) => r.label === 'dog').map((r) => numeric(r[PRIMARY_FEATURE])),
        sr.filter((r) => r.label === 'dud').map((r) => numeric(r[PRIMARY_FEATURE]))),
    };
  }
  const k = Math.min(30, dog + dud);
  const top20 = topkPrecision(cohortRows, 20);
  const top30 = topkPrecision(cohortRows, Math.min(30, k));
  const top20Lift = top20 !== null ? Number((top20 - baseDogRate).toFixed(4)) : null;
  const top30Lift = top30 !== null ? Number((top30 - baseDogRate).toFixed(4)) : null;
  const stageSurvives = Object.values(stageSplit).some((s) => s.auc !== null && s.auc > 0.55 && s.n_dog >= 10 && s.n_dud >= 10);

  // locked thresholds (Section 8)
  const successCore = auc > 0.60 && ci && ci.lo > 0.55
    && ((top20Lift !== null && top20Lift >= 0.10) || (top30Lift !== null && top30Lift >= 0.10))
    && aucTradesPresent > 0.55 && stageSurvives;
  let verdict;
  if (successCore) verdict = 'success';
  else if (auc <= 0.56) verdict = 'failure';
  else if (lookPoint >= 130) verdict = 'weak_inconclusive';
  else verdict = 'gray_continue_to_130';

  return {
    public: {
      look_point: lookPoint, primary: PRIMARY_FEATURE, verdict,
      auc: Number(auc.toFixed(4)), ci_lo: ci ? Number(ci.lo.toFixed(4)) : null, ci_hi: ci ? Number(ci.hi.toFixed(4)) : null,
      auc_trades_present: aucTradesPresent !== null ? Number(aucTradesPresent.toFixed(4)) : null,
      base_dog_rate: Number(baseDogRate.toFixed(4)),
      top20_precision: top20, top20_lift_pp: top20Lift !== null ? Number((top20Lift * 100).toFixed(1)) : null,
      top30_precision: top30, top30_lift_pp: top30Lift !== null ? Number((top30Lift * 100).toFixed(1)) : null,
      stage_split: stageSplit, stage_survives: stageSurvives,
      n_dog: dog, n_dud: dud,
      decision: verdict === 'success'
        ? 'OOS success: may design sol_curve-only PURE SHADOW ranking gate (no trade/size/exit/contract change).'
        : verdict === 'failure'
          ? 'OOS failure: do NOT tune gate/matrix/RR/liquidity; reassess sourcing or partition capture goal.'
          : 'OOS gray: collect another OOS pack under this prereg or write a new prereg. Change nothing.',
    },
    sealed: null,
  };
}

// ---------- CLI ----------
function parseArgs(argv) {
  const a = { lookPoint: null, iters: 2000 };
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i]; const v = argv[i + 1];
    if (k === '--feature-rows') { a.featureRows = v; i += 1; }
    else if (k === '--pack-id') { a.packId = v; i += 1; }
    else if (k === '--pack-manifest') { a.packManifest = v; i += 1; }
    else if (k === '--training-tokens') { a.trainingTokens = v; i += 1; }
    else if (k === '--cumulative-dir') { a.cumulativeDir = v; i += 1; }
    else if (k === '--out-dir') { a.outDir = v; i += 1; }
    else if (k === '--prereg') { a.prereg = v; i += 1; }
    else if (k === '--prereg-lock') { a.preregLock = v; i += 1; }
    else if (k === '--look-point') { a.lookPoint = Number(v); i += 1; }
  }
  return a;
}
function loadTrainingTokens(p) {
  if (!p) return null;
  const txt = fs.readFileSync(p, 'utf8').trim();
  const set = new Set();
  for (const line of txt.split('\n')) {
    const tok = line.trim().split(/[|,\s]/)[0];
    if (tok) set.add(tok);
  }
  return set;
}
function loadCumulativeSeen(cumPath, provPath, packId) {
  const seen = new Set();
  let ingestedPacks = [];
  if (fs.existsSync(cumPath)) for (const r of readRows(cumPath)) seen.add(sigKey(r));
  if (fs.existsSync(provPath)) ingestedPacks = readJson(provPath).ingested_pack_ids || [];
  return { seen, ingestedPacks, alreadyIngested: ingestedPacks.includes(packId) };
}

function main() {
  const a = parseArgs(process.argv);
  const root = '/Users/boliu/sas-research';
  const prereg = a.prereg || path.join(root, 'claudedocs/oos-sol-curve-unique-buyers-preregister.md');
  const preregLock = a.preregLock || path.join(root, 'claudedocs/oos-sol-curve-unique-buyers-preregister.sha256');
  if (!a.featureRows || !a.packId || !a.cumulativeDir || !a.outDir) {
    console.error('usage: accumulate-oos-features.js --feature-rows <f> --pack-id <id> --training-tokens <f> --cumulative-dir <d> --out-dir <d> [--pack-manifest <f>] [--look-point 50|100|130]');
    process.exit(2);
  }
  // 1. prereg lock (fail-closed)
  const preregSha = verifyPreregLock(prereg, preregLock);
  // training tokens are MANDATORY (Section 2): no exclusion list => pack invalid
  const trainingTokens = loadTrainingTokens(a.trainingTokens);
  if (!trainingTokens) throw new Error('training_tokens_required: Section 2 token exclusion cannot be applied (fail-closed)');

  fs.mkdirSync(a.outDir, { recursive: true });
  fs.mkdirSync(a.cumulativeDir, { recursive: true });
  const cumPath = path.join(a.cumulativeDir, 'cumulative_oos_features.jsonl');
  const provPath = path.join(a.cumulativeDir, 'cumulative_provenance.json');
  const countsPath = path.join(a.cumulativeDir, 'cumulative_counts.json');

  const rows = readRows(a.featureRows);
  const manifest = a.packManifest && fs.existsSync(a.packManifest) ? readJson(a.packManifest) : {};
  const { seen, ingestedPacks, alreadyIngested } = loadCumulativeSeen(cumPath, provPath, a.packId);

  // 4. gates
  const { cohortRows, preCohortRows, stats } = applyGates(rows, { trainingTokens, seenSigKeys: seen });
  const sym = symmetryReport(preCohortRows);

  // 5/6. append (idempotent) + daily artifacts
  const stampedRows = cohortRows.map((r) => ({
    ...r, pack_id: a.packId, prereg_sha256: preregSha,
    pack_commit: manifest.production_commit || manifest.commit || null,
    schema_version: manifest.schema_version || r.schema_version || 'unknown',
  }));
  writeJsonl(path.join(a.outDir, 'daily_feature_rows.jsonl'), stampedRows);
  if (!alreadyIngested) {
    appendJsonl(cumPath, stampedRows);
    fs.writeFileSync(provPath, JSON.stringify({ ingested_pack_ids: [...ingestedPacks, a.packId] }, null, 2));
  }

  // cumulative counts
  const cumRows = fs.existsSync(cumPath) ? readRows(cumPath) : stampedRows;
  const cc = classCounts(cumRows);
  const dailyQa = {
    schema_version: 'oos_daily_qa.v1',
    pack_id: a.packId,
    prereg_sha256: preregSha,
    pack_commit: manifest.production_commit || manifest.commit || null,
    already_ingested: alreadyIngested,
    gates: stats,
    symmetry: sym,
    daily_cohort: { dog: classCounts(cohortRows).dog, dud: classCounts(cohortRows).dud },
    cumulative_cohort: { dog: cc.dog, dud: cc.dud },
    milestones: ALLOWED_LOOK_POINTS.reduce((m, lp) => {
      m[`n${lp}`] = { dog_reached: cc.dog >= lp, dud_reached: cc.dud >= lp, both_reached: cc.dog >= lp && cc.dud >= lp };
      return m;
    }, {}),
    note: 'Measurement/QA only. AUC is NOT computed here unless --look-point is requested AND reached.',
  };
  fs.writeFileSync(path.join(a.outDir, 'daily_qa_report.json'), JSON.stringify(dailyQa, null, 2));
  fs.writeFileSync(countsPath, JSON.stringify(dailyQa.cumulative_cohort, null, 2));

  // 7. lookpoint guard
  let lookOut = { look_point_requested: a.lookPoint, auc_withheld: true,
    reason: a.lookPoint ? 'computed below' : 'no look point requested: counts/QA only, AUC withheld by design' };
  if (a.lookPoint) {
    const res = lookpointAnalysis(cumRows, a.lookPoint); // throws if not reached
    lookOut = { look_point_requested: a.lookPoint, auc_withheld: false, ...res.public };
    if (res.sealed) {
      fs.writeFileSync(path.join(a.outDir, `lookpoint_report_${a.lookPoint}_SEALED.json`),
        JSON.stringify({ ...res.sealed, prereg_sha256: preregSha }, null, 2));
    } else {
      fs.writeFileSync(path.join(a.outDir, `lookpoint_report_${a.lookPoint}.json`),
        JSON.stringify({ ...res.public, prereg_sha256: preregSha }, null, 2));
    }
  }

  console.log(JSON.stringify({
    ok: true, prereg_sha256: preregSha.slice(0, 12), pack_id: a.packId, already_ingested: alreadyIngested,
    daily_cohort: dailyQa.daily_cohort, cumulative_cohort: dailyQa.cumulative_cohort,
    gates: stats, symmetry_pp: { trade_hit: sym.trade_hit_asymmetry_pp, complete_window: sym.complete_window_asymmetry_pp },
    lookpoint: lookOut,
  }, null, 2));
}

export {
  applyGates, classCounts, symmetryReport, aucRaw, bootstrapCi, topkPrecision,
  lookpointAnalysis, verifyPreregLock, hasTrades, sigKey, ALLOWED_LOOK_POINTS,
};

if (process.argv[1] && process.argv[1] === fileURLToPath(import.meta.url)) {
  try { main(); } catch (e) { console.error(`FAIL_CLOSED: ${e.message}`); process.exit(1); }
}
