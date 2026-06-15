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
  'feature_coverage_status', 'unique_buyers', 'progress_stage',
];
const DEGENERATE_STAGES = new Set([null, undefined, 'undefined', 'unknown', 'decode_unavailable', 'none', '']);
// Operationalizes prereg §7 "if usable-rate/trade-hit asymmetry is large":
// cumulative sol_curve usable-rate gap above this (percentage points) blocks success.
const COVERAGE_ASYMMETRY_MAX_PP = 15;
const MIN_STRATUM = 10;
const MIN_UNIQUE_TOKENS = 20;
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
function utcDate(ts) { return new Date(Number(ts) * 1000).toISOString().slice(0, 10); }
function uniqueByToken(rows) {
  const seen = new Set(); const out = [];
  for (const r of rows) { if (!seen.has(r.token_ca)) { seen.add(r.token_ca); out.push(r); } }
  return out;
}
function sourceOf(row) { return row.signal_source || row.source || null; }

// prereg §3/§4: every row in a pack must carry the same schema as the pack, and a
// pack must not mix schemas. Fail-closed otherwise.
function rowSchemaGate(rows, packSchema) {
  const present = [...new Set(rows.map((r) => r.schema_version).filter((v) => v !== undefined && v !== null))];
  const someHave = present.length > 0;
  const someMissing = rows.some((r) => r.schema_version === undefined || r.schema_version === null);
  if (someHave && present.length > 1) {
    throw new Error(`schema_mismatch: pack carries mixed row schemas [${present.join(', ')}] (fail-closed)`);
  }
  if (someHave && someMissing) {
    throw new Error('schema_mismatch: some rows carry schema_version and some do not (fail-closed)');
  }
  if (someHave && present[0] !== packSchema) {
    throw new Error(`schema_mismatch: row schema ${present[0]} != pack schema ${packSchema} (fail-closed)`);
  }
}

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
    excluded_incomplete_window: 0,
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
  // 2. cohort filter: sol_curve AND has_trades AND complete_window (prereg §5:
  //    incomplete coverage must be reported separately, never used as a usable window).
  let cohort = r.filter((row) => {
    if (row.return_domain !== COHORT_DOMAIN || !hasTrades(row)) return false;
    if (row.feature_coverage_status !== 'complete_window') { stats.excluded_incomplete_window += 1; return false; }
    return true;
  });
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
// leave-one-stratum-out survival: remove every row in stratum `label`, require the
// REMAINDER (with enough of both classes) to still separate (AUC>0.55). Used for
// both date and source robustness so that NO single day/source can carry the edge.
function leaveOneOutAuc(rows, inStratum, label) {
  const rem = rows.filter((r) => !inStratum(r));
  const rd = rem.filter((r) => r.label === 'dog');
  const ru = rem.filter((r) => r.label === 'dud');
  const a = aucRaw(rd.map((r) => numeric(r[PRIMARY_FEATURE])), ru.map((r) => numeric(r[PRIMARY_FEATURE])));
  const evaluable = rd.length >= MIN_STRATUM && ru.length >= MIN_STRATUM;
  return {
    left_out: label, evaluable, n_dog: rd.length, n_dud: ru.length,
    auc: a !== null ? Number(a.toFixed(4)) : null,
    // fail-closed: if removing this stratum leaves too little to evaluate, we cannot
    // confirm the signal survives WITHOUT it -> treat as not surviving.
    survives: evaluable && a !== null && a > 0.55,
  };
}

/**
 * The ONLY entry point that computes AUC. It refuses unless a valid look point is
 * reached. n=50 returns a futility verdict WITHOUT exposing the AUC point estimate
 * on the public surface (the caller seals it into a report file).
 */
function lookpointAnalysis(cohortRows, lookPoint, opts = {}) {
  if (!ALLOWED_LOOK_POINTS.includes(lookPoint)) {
    throw new Error(`look_point_not_allowed: ${lookPoint} not in ${ALLOWED_LOOK_POINTS.join('/')}`);
  }
  const { dog, dud, dogRows, dudRows } = classCounts(cohortRows);
  if (dog < lookPoint || dud < lookPoint) {
    throw new Error(`look_point_not_reached: dog=${dog} dud=${dud} < ${lookPoint} per class (AUC withheld)`);
  }

  // COVERAGE-SYMMETRY HARD GATE (prereg §7) — evaluated FIRST, before ANY AUC read
  // or futility decision, for EVERY look point (incl. n=50). A biased dog/dud
  // usable-rate denominator must neither declare success NOR futility-STOP a real
  // signal: a STOP under coverage bias could kill a true edge prematurely. When
  // biased we do not even compute the AUC.
  const coverageAsymmetryPp = numeric(opts.coverageAsymmetryPp);
  const coverageMaxPp = numeric(opts.coverageMaxPp) ?? COVERAGE_ASYMMETRY_MAX_PP;
  const coverageOk = coverageAsymmetryPp === null || Math.abs(coverageAsymmetryPp) <= coverageMaxPp;
  const coverageBlock = { asymmetry_pp: coverageAsymmetryPp, max_allowed_pp: coverageMaxPp, coverage_ok: coverageOk };
  if (!coverageOk) {
    return {
      public: {
        look_point: lookPoint, primary: PRIMARY_FEATURE, verdict: 'coverage_biased_inconclusive',
        coverage: coverageBlock, n_dog: dog, n_dud: dud,
        instruction: 'Coverage asymmetry exceeds the locked threshold: do NOT read AUC and do NOT futility-STOP. Fix/explain dog-vs-dud usable-rate coverage, then re-evaluate under this prereg.',
        decision: 'Coverage-biased: dog/dud usable-rate asymmetry exceeds the locked threshold. Not success, not stop — coverage must be corrected before any AUC interpretation.',
      },
      sealed: null,
    };
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
      public: { look_point: 50, mode: 'futility_only', directional_null: directionalNull, coverage: coverageBlock,
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
  const kMax = Math.min(30, dog + dud);
  const top20 = topkPrecision(cohortRows, 20);
  const top30 = topkPrecision(cohortRows, Math.min(30, kMax));
  const top20Lift = top20 !== null ? Number((top20 - baseDogRate).toFixed(4)) : null;
  const top30Lift = top30 !== null ? Number((top30 - baseDogRate).toFixed(4)) : null;
  // prereg §7/§8: the signal must survive a genuine progress/stage split. A
  // degenerate split (one stage, or all 'undefined'/'decode_unavailable') does
  // NOT satisfy the control -> cannot count as survival.
  const realStageKeys = Object.keys(stageSplit).filter((sk) => !DEGENERATE_STAGES.has(sk));
  const stageSurvives = realStageKeys.length >= 2
    && realStageKeys.some((sk) => {
      const s = stageSplit[sk];
      return s.auc !== null && s.auc > 0.55 && s.n_dog >= MIN_STRATUM && s.n_dud >= MIN_STRATUM;
    });

  // prereg §8.5: success must NOT be driven by one token cluster / one day / one source.
  // (a) unique-token sensitivity
  const dogU = uniqueByToken(dogRows); const dudU = uniqueByToken(dudRows);
  const uniqueTokenAuc = aucRaw(dogU.map((r) => numeric(r[PRIMARY_FEATURE])), dudU.map((r) => numeric(r[PRIMARY_FEATURE])));
  const uniqueTokenSurvives = dogU.length >= MIN_UNIQUE_TOKENS && dudU.length >= MIN_UNIQUE_TOKENS
    && uniqueTokenAuc !== null && uniqueTokenAuc > 0.55;
  // (b) UTC-date split: >=2 dates AND signal survives leave-EACH-day-out (not just
  //     leave-largest): otherwise one small day could carry the entire edge.
  const dates = [...new Set(cohortRows.map((r) => utcDate(r.signal_ts)))];
  let dateSurvives; let dateDetail;
  if (dates.length < 2) {
    dateSurvives = false; dateDetail = { status: 'single_date', n_dates: dates.length };
  } else {
    const loo = dates.map((d) => leaveOneOutAuc(cohortRows, (r) => utcDate(r.signal_ts) === d, d));
    dateSurvives = loo.every((x) => x.survives);
    dateDetail = { status: 'multi_date', n_dates: dates.length, leave_one_date_out: loo };
  }
  // (c) source split: when source metadata exists, require >=2 sources AND survival
  //     of leave-EACH-source-out (multi-source is a precondition, not a proof).
  const sources = [...new Set(cohortRows.map(sourceOf).filter(Boolean))];
  let sourceSurvives; let sourceDetail;
  if (sources.length === 0) {
    sourceSurvives = true; sourceDetail = { status: 'no_source_metadata' };
  } else if (sources.length < 2) {
    sourceSurvives = false; sourceDetail = { status: 'single_source', n_sources: 1 };
  } else {
    const loo = sources.map((s) => leaveOneOutAuc(cohortRows, (r) => sourceOf(r) === s, s));
    sourceSurvives = loo.every((x) => x.survives);
    sourceDetail = { status: 'multi_source', n_sources: sources.length, leave_one_source_out: loo };
  }

  // locked thresholds (Section 8). Coverage already gated above (always ok here).
  const successCore = auc > 0.60 && ci && ci.lo > 0.55
    && ((top20Lift !== null && top20Lift >= 0.10) || (top30Lift !== null && top30Lift >= 0.10))
    && aucTradesPresent > 0.55 && stageSurvives;
  const robustnessOk = uniqueTokenSurvives && dateSurvives && sourceSurvives;
  let verdict;
  if (successCore && robustnessOk) verdict = 'success';
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
      robustness: {
        unique_token_survives: uniqueTokenSurvives, unique_token_auc: uniqueTokenAuc !== null ? Number(uniqueTokenAuc.toFixed(4)) : null,
        n_unique_dog_tokens: dogU.length, n_unique_dud_tokens: dudU.length,
        date_survives: dateSurvives, date_detail: dateDetail,
        source_survives: sourceSurvives, source_detail: sourceDetail,
      },
      coverage: coverageBlock,
      n_dog: dog, n_dud: dud,
      decision: verdict === 'success'
        ? 'OOS success: may design sol_curve-only PURE SHADOW ranking gate (no trade/size/exit/contract change).'
        : verdict === 'failure'
          ? 'OOS failure: do NOT tune gate/matrix/RR/liquidity; reassess sourcing or partition capture goal.'
          : 'OOS gray/inconclusive: collect another OOS pack under this prereg or write a new prereg. Change nothing.',
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
    else if (k === '--schema-version') { a.schemaVersion = v; i += 1; }
    else if (k === '--reveal-sealed-auc') { a.revealSealedAuc = v; i += 1; }
  }
  return a;
}

// prereg §3/§4: dog and dud must share one schema version; a cumulative table
// must never mix schemas. Returns the schema to stamp/lock, or throws fail-closed.
function resolveSchema(manifest, rows, override) {
  const s = override || manifest.schema_version
    || (rows.find((r) => r.schema_version)?.schema_version) || null;
  if (!s || s === 'unknown') {
    throw new Error('schema_version_required: cannot enforce same-schema gate without an explicit feature/cohort schema_version (fail-closed)');
  }
  return s;
}
function schemaGate(packSchema, lockedSchema) {
  if (lockedSchema && lockedSchema !== packSchema) {
    throw new Error(`schema_drift: pack schema ${packSchema} != cumulative locked schema ${lockedSchema}; do not mix. Start a new cumulative dir or re-decode under one schema (fail-closed)`);
  }
  return packSchema;
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
  let lockedSchema = null;
  if (fs.existsSync(cumPath)) for (const r of readRows(cumPath)) seen.add(sigKey(r));
  let coverageTally = { dog: { sol_curve_total: 0, kept: 0 }, dud: { sol_curve_total: 0, kept: 0 } };
  if (fs.existsSync(provPath)) {
    const prov = readJson(provPath);
    ingestedPacks = prov.ingested_pack_ids || [];
    lockedSchema = prov.locked_schema_version || null;
    if (prov.coverage_tally) coverageTally = prov.coverage_tally;
  }
  return { seen, ingestedPacks, lockedSchema, coverageTally, alreadyIngested: ingestedPacks.includes(packId) };
}
function coverageAsymmetryPp(tally) {
  const r = (c) => (c.sol_curve_total > 0 ? c.kept / c.sol_curve_total : null);
  const dr = r(tally.dog); const ur = r(tally.dud);
  return (dr === null || ur === null) ? null : Number(((dr - ur) * 100).toFixed(1));
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
  const { seen, ingestedPacks, lockedSchema, coverageTally, alreadyIngested } = loadCumulativeSeen(cumPath, provPath, a.packId);

  // same-schema gate (fail-closed): require an explicit schema, lock the cumulative to it,
  // and verify every row carries the same schema (no mixed-schema input).
  // WRAPPER CONTRACT: rowSchemaGate permits an all-rows-missing-schema pack (it is then
  // stamped from manifest/override). The daily wrapper MUST therefore guarantee that the
  // manifest/override schema_version IS the actual feature-table schema; otherwise a pack
  // whose rows all lack schema_version could be mis-stamped into the cumulative.
  const packSchema = resolveSchema(manifest, rows, a.schemaVersion);
  schemaGate(packSchema, lockedSchema);
  rowSchemaGate(rows, packSchema);
  const effectiveLockedSchema = lockedSchema || packSchema;

  // 4. gates
  const { cohortRows, preCohortRows, stats } = applyGates(rows, { trainingTokens, seenSigKeys: seen });
  const sym = symmetryReport(preCohortRows);

  // coverage tally: accumulate per-class sol_curve candidate -> cohort survival across packs
  const newTally = JSON.parse(JSON.stringify(coverageTally));
  if (!alreadyIngested) {
    for (const lab of ['dog', 'dud']) {
      const solc = preCohortRows.filter((r) => r.label === lab && r.return_domain === COHORT_DOMAIN).length;
      const kept = cohortRows.filter((r) => r.label === lab).length;
      newTally[lab].sol_curve_total += solc;
      newTally[lab].kept += kept;
    }
  }
  const cumCoverageAsymPp = coverageAsymmetryPp(newTally);

  // 5/6. append (idempotent) + daily artifacts
  const stampedRows = cohortRows.map((r) => ({
    ...r, pack_id: a.packId, prereg_sha256: preregSha,
    pack_commit: manifest.production_commit || manifest.commit || null,
    schema_version: packSchema,
  }));
  writeJsonl(path.join(a.outDir, 'daily_feature_rows.jsonl'), stampedRows);
  if (!alreadyIngested) {
    appendJsonl(cumPath, stampedRows);
    fs.writeFileSync(provPath, JSON.stringify({
      ingested_pack_ids: [...ingestedPacks, a.packId],
      locked_schema_version: effectiveLockedSchema,
      coverage_tally: newTally,
    }, null, 2));
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
    cumulative_coverage_tally: newTally,
    cumulative_coverage_asymmetry_pp: cumCoverageAsymPp,
    coverage_gate: { max_allowed_pp: COVERAGE_ASYMMETRY_MAX_PP, ok: cumCoverageAsymPp === null || Math.abs(cumCoverageAsymPp) <= COVERAGE_ASYMMETRY_MAX_PP },
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
    const res = lookpointAnalysis(cumRows, a.lookPoint, { coverageAsymmetryPp: cumCoverageAsymPp }); // throws if not reached
    lookOut = { look_point_requested: a.lookPoint, auc_withheld: false, ...res.public };
    // Always write the PUBLIC verdict (no AUC value at n=50). The sealed numeric
    // AUC is written ONLY when explicitly revealed via --reveal-sealed-auc <path>,
    // so the default surface can never break the n=50 futility-only discipline.
    fs.writeFileSync(path.join(a.outDir, `lookpoint_report_${a.lookPoint}.json`),
      JSON.stringify({ ...res.public, prereg_sha256: preregSha }, null, 2));
    if (res.sealed && a.revealSealedAuc) {
      fs.writeFileSync(a.revealSealedAuc,
        JSON.stringify({ ...res.sealed, prereg_sha256: preregSha, warning: 'sealed futility AUC; do not use to declare success' }, null, 2));
    }
  }

  console.log(JSON.stringify({
    ok: true, prereg_sha256: preregSha.slice(0, 12), pack_id: a.packId, already_ingested: alreadyIngested,
    daily_cohort: dailyQa.daily_cohort, cumulative_cohort: dailyQa.cumulative_cohort,
    gates: stats, symmetry_pp: { trade_hit: sym.trade_hit_asymmetry_pp, complete_window: sym.complete_window_asymmetry_pp },
    lookpoint: lookOut,
  }, null, 2));
}

// NOTE: raw AUC helpers (aucRaw/bootstrapCi/topkPrecision) are intentionally NOT
// exported. AUC may only be obtained through the guarded `lookpointAnalysis`, so
// no caller (including tests) can compute AUC outside a reached look point.
export {
  applyGates, classCounts, symmetryReport, lookpointAnalysis,
  verifyPreregLock, schemaGate, resolveSchema, rowSchemaGate,
  hasTrades, sigKey, ALLOWED_LOOK_POINTS, COVERAGE_ASYMMETRY_MAX_PP,
};

if (process.argv[1] && process.argv[1] === fileURLToPath(import.meta.url)) {
  try { main(); } catch (e) { console.error(`FAIL_CLOSED: ${e.message}`); process.exit(1); }
}
