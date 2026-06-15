#!/usr/bin/env node
'use strict';

/**
 * build-daily-oos-sol-curve-cohort.js
 *
 * Daily OOS cohort producer for the locked preregistration:
 *   claudedocs/oos-sol-curve-unique-buyers-preregister.md
 *
 * HARD SCOPE (operator decision): produces ONLY the native-domain OOS clean
 * dog/dud cohort for the unique_buyers feature test. NOT a discovery-grade,
 * full-domain label adjudicator.
 *
 * Labeling: the native sustained tier is the observer's already-computed
 * native-unit raw_primary_tier in the frozen snapshot (NOT recomputed from bars:
 * a naive last-bar baseline diverges from the observer anchor and inflates
 * returns; verified). raw_primary_tier == raw_sustained_tier on all formal-
 * eligible rows.
 *
 * FORMAL ELIGIBILITY (raw dog denominator; src/web/dashboard-server.js
 * rawOutcomeEligibleSql + src/analytics/raw-signal-outcomes.js): a row is only
 * eligible when
 *   observation_status='matured' AND kline_covered=1 AND
 *   baseline_confidence IN ('high','medium') AND same_source_path=1 AND
 *   outlier_flag=0 AND sustained_evaluable=1.
 * This producer enforces exactly that, PLUS a native-domain restriction
 * (baseline_price_unit='native') and a unit-suspect guard. Anything else is
 * quarantined (prefer under-collect over dirty-collect).
 *
 * DOMAIN: rows are emitted with return_domain='native_sol', NOT 'sol_curve'.
 * The producer cannot prove a row is bonding-curve (native-eligible rows are a
 * mix of indexed_ohlcv / amm_pool / bonding_curve sources). The downstream chain
 * feature table is the authority that CONFIRMS curve trades and upgrades a row to
 * sol_curve; marking everything sol_curve here would pollute the downstream
 * coverage/symmetry denominator. Source provenance fields are carried for that.
 *
 * Discipline: PURE OFFLINE, deterministic, reproducible; reads only the frozen
 * snapshot. No GMGN, no chain RPC, no external API. GMGN+chain decode is a
 * SEPARATE discovery-grade audit path with its own schema; never mixed in here.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const MAX_SUSTAINED_PCT = 1500; // native sustained peak > 1500% (15x) is unit-suspect -> quarantine
const DOG_TIERS = new Set(['gold', 'silver']);
const DUD_TIERS = new Set(['bronze', 'sub25']);

function confidenceEligible(c) { return c === 'high' || c === 'medium'; }

/**
 * Pure per-signal classifier (testable without a DB). Applies the formal raw-dog
 * eligibility gate + native-domain restriction + unit-suspect guard, THEN labels
 * from raw_primary_tier. Cleaning is symmetric: identical gates run before the
 * dog/dud label is assigned.
 * Returns { disposition: 'not_matured'|'quarantine'|'dog'|'dud', reason?, tier?, extra? }.
 */
function classifySignal(s, opts = {}) {
  const maxSustainedPct = opts.maxSustainedPct ?? MAX_SUSTAINED_PCT;
  // 1) maturation (observer-authoritative: incorporates the 2h horizon)
  if (s.observation_status !== 'matured') return { disposition: 'not_matured' };
  // 2) FORMAL raw-dog eligibility (must match the live denominator exactly)
  if (Number(s.kline_covered) !== 1) return { disposition: 'quarantine', reason: 'not_eligible_kline_uncovered' };
  if (!confidenceEligible(s.baseline_confidence)) return { disposition: 'quarantine', reason: 'not_eligible_baseline_confidence' };
  if (Number(s.same_source_path) !== 1) return { disposition: 'quarantine', reason: 'not_eligible_cross_source_path' };
  if (Number(s.outlier_flag) === 1) return { disposition: 'quarantine', reason: 'not_eligible_outlier' };
  if (Number(s.sustained_evaluable) !== 1) return { disposition: 'quarantine', reason: 'not_eligible_sustained' };
  // 3) native-domain restriction (sol_curve returns must be native/SOL)
  if (s.baseline_price == null || Number(s.baseline_price) <= 0) return { disposition: 'quarantine', reason: 'missing_baseline' };
  if (s.baseline_price_unit !== 'native') return { disposition: 'quarantine', reason: 'non_native_baseline', extra: { baseline_price_unit: s.baseline_price_unit } };
  // 4) native unit-suspect guard
  const sustPct = s.max_sustained_peak_pct == null ? null : Number(s.max_sustained_peak_pct);
  if (sustPct == null || sustPct > maxSustainedPct) return { disposition: 'quarantine', reason: 'native_unit_suspect', extra: { max_sustained_peak_pct: sustPct } };
  // 5) label from formal raw_primary_tier (== raw_sustained_tier on eligible rows)
  const tier = String(s.raw_primary_tier || '').toLowerCase();
  if (DOG_TIERS.has(tier)) return { disposition: 'dog', tier, extra: { sustPct } };
  if (DUD_TIERS.has(tier)) return { disposition: 'dud', tier, extra: { sustPct } };
  return { disposition: 'quarantine', reason: 'unknown_tier', extra: { tier } };
}

function sha256File(p) { return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex'); }

async function openSnapshot(p) {
  const mod = await import('better-sqlite3');
  const Database = mod.default || mod;
  return new Database(p, { readonly: true, fileMustExist: true });
}

function parseArgs(argv) {
  const a = { maxSustainedPct: MAX_SUSTAINED_PCT };
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i]; const v = argv[i + 1];
    if (k === '--snapshot') { a.snapshot = v; i += 1; }
    else if (k === '--out-dir') { a.outDir = v; i += 1; }
    else if (k === '--max-sustained-pct') { a.maxSustainedPct = Number(v); i += 1; }
    else if (k === '--help' || k === '-h') { a.help = true; }
  }
  return a;
}

async function main() {
  const a = parseArgs(process.argv);
  if (a.help || !a.snapshot) {
    console.log('usage: build-daily-oos-sol-curve-cohort.js --snapshot <raw_signal_outcomes.snapshot.db> '
      + '[--out-dir <dir>] [--max-sustained-pct 1500]');
    process.exit(a.help ? 0 : 2);
  }
  if (!fs.existsSync(a.snapshot)) { console.error(`snapshot not found: ${a.snapshot}`); process.exit(2); }
  const outDir = a.outDir || path.join(path.dirname(a.snapshot), 'oos-sol-curve-cohort-native');
  fs.mkdirSync(outDir, { recursive: true });

  const db = await openSnapshot(a.snapshot);
  const snapshotTs = db.prepare("SELECT MAX(timestamp) t FROM raw_price_bars_1m WHERE price_unit='native'").get().t
    || db.prepare('SELECT MAX(timestamp) t FROM raw_price_bars_1m').get().t;
  const signals = db.prepare(`SELECT token_ca, signal_ts, signal_type, route, hard_gate_status, observation_status,
    kline_covered, baseline_confidence, same_source_path, outlier_flag, sustained_evaluable, coverage_reason,
    baseline_price, baseline_price_unit, max_sustained_peak_pct, raw_primary_tier, raw_sustained_tier,
    source_kind, source_family, path_source_kind, provider FROM raw_signal_outcomes`).all();
  db.close();

  const tally = {
    input_signals: signals.length, not_matured: 0,
    quarantine: {
      not_eligible_kline_uncovered: 0, not_eligible_baseline_confidence: 0, not_eligible_cross_source_path: 0,
      not_eligible_outlier: 0, not_eligible_sustained: 0, missing_baseline: 0, non_native_baseline: 0,
      native_unit_suspect: 0, unknown_tier: 0,
    },
    cohort: { dog: 0, dud: 0 }, tier: { gold: 0, silver: 0, bronze: 0, sub25: 0 },
  };
  const dogs = []; const duds = []; const quarantine = [];

  for (const s of signals) {
    const c = classifySignal(s, { maxSustainedPct: a.maxSustainedPct });
    if (c.disposition === 'not_matured') { tally.not_matured += 1; continue; }
    if (c.disposition === 'quarantine') {
      if (tally.quarantine[c.reason] != null) tally.quarantine[c.reason] += 1;
      quarantine.push({ token_ca: s.token_ca, signal_ts: Number(s.signal_ts), quarantine_reason: c.reason, raw_primary_tier: s.raw_primary_tier || null, ...(c.extra || {}) });
      continue;
    }
    if (tally.tier[c.tier] != null) tally.tier[c.tier] += 1;
    const row = {
      token_ca: s.token_ca, signal_ts: Number(s.signal_ts),
      label: c.disposition, effective_tier: c.tier, tier: c.tier,
      // NOT sol_curve: native-domain candidate; downstream chain decode confirms+upgrades to sol_curve.
      return_domain: 'native_sol',
      baseline_price: Number(s.baseline_price), baseline_price_unit: 'native',
      baseline_confidence: s.baseline_confidence, same_source_path: Number(s.same_source_path),
      max_sustained_peak_pct: c.extra.sustPct, corrected_peak_pct: Number((c.extra.sustPct / 100).toFixed(6)),
      adjudication_mode: 'observer_native_primary_tier', label_domain: 'native_sol',
      signal_type: s.signal_type || null, route: s.route || null, hard_gate_status: s.hard_gate_status || null,
      source_kind: s.source_kind || null, source_family: s.source_family || null,
      path_source_kind: s.path_source_kind || null, provider: s.provider || null,
    };
    if (c.disposition === 'dog') { dogs.push(row); tally.cohort.dog += 1; } else { duds.push(row); tally.cohort.dud += 1; }
  }

  const bySig = (x, y) => (x.token_ca < y.token_ca ? -1 : x.token_ca > y.token_ca ? 1 : x.signal_ts - y.signal_ts);
  dogs.sort(bySig); duds.sort(bySig); quarantine.sort(bySig);

  fs.writeFileSync(path.join(outDir, 'clean-dogs.json'), JSON.stringify(dogs, null, 2));
  fs.writeFileSync(path.join(outDir, 'clean-duds.json'), JSON.stringify(duds, null, 2));
  fs.writeFileSync(path.join(outDir, 'quarantine.json'), JSON.stringify(quarantine, null, 2));

  const manifest = {
    schema_version: 'oos_sol_curve_cohort_native.v2',
    scope: 'native-domain OOS clean dog/dud cohort for the unique_buyers feature test. NOT discovery-grade. '
      + 'return_domain=native_sol; downstream chain feature table confirms+upgrades to sol_curve via curve trades.',
    adjudication_mode: 'observer_native_primary_tier',
    label_source: 'frozen snapshot raw_signal_outcomes.raw_primary_tier (== raw_sustained_tier on eligible rows); native-unit baseline_price anchor; NOT recomputed from bars.',
    formal_eligibility: "observation_status='matured' AND kline_covered=1 AND baseline_confidence IN ('high','medium') AND same_source_path=1 AND outlier_flag=0 AND sustained_evaluable=1",
    native_restriction: "baseline_price_unit='native'",
    external_apis_used: false, gmgn_used: false, chain_rpc_used: false, label_domain: 'native_sol',
    known_caveat: 'May undercount true dogs: only formal-eligible + native-baseline + sustained<=' + a.maxSustainedPct
      + '% rows are labeled; everything else is quarantined (prefer fewer-clean over larger-dirty). return_domain is '
      + 'native_sol (NOT sol_curve): the bonding-curve venue is confirmed downstream by the chain decode, not here.',
    source_snapshot: { path: path.resolve(a.snapshot), sha256: sha256File(a.snapshot), snapshot_ts: snapshotTs },
    params: { max_sustained_pct: a.maxSustainedPct, dog_tiers: [...DOG_TIERS], dud_tiers: [...DUD_TIERS] },
    symmetry_note: 'All signals pass identical maturation/eligibility/native/unit-suspect gates BEFORE the dog/dud label is assigned.',
    tally,
    outputs: { clean_dogs: dogs.length, clean_duds: duds.length, quarantined: quarantine.length },
  };
  fs.writeFileSync(path.join(outDir, 'cohort-manifest.json'), JSON.stringify(manifest, null, 2));

  console.log(JSON.stringify({
    ok: true, snapshot_ts: snapshotTs, input_signals: tally.input_signals, not_matured: tally.not_matured,
    quarantine: tally.quarantine, cohort: tally.cohort, tier: tally.tier, out_dir: outDir,
  }, null, 2));
}

if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => { console.error(`build-daily-oos-sol-curve-cohort: ${e.message}`); process.exit(1); });
}

export { classifySignal, confidenceEligible, MAX_SUSTAINED_PCT };
