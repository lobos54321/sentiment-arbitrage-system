#!/usr/bin/env node
'use strict';

/**
 * build-daily-oos-sol-curve-cohort.js
 *
 * Daily OOS cohort producer for the locked preregistration:
 *   claudedocs/oos-sol-curve-unique-buyers-preregister.md
 *
 * HARD SCOPE (per operator decision): produces ONLY the sol_curve OOS clean
 * dog/dud cohort for the unique_buyers feature test. NOT a full discovery-grade,
 * full-domain label adjudicator.
 *
 * Labeling (per operator decision, after verification):
 *   The native sustained tier is the observer's already-computed, native-unit
 *   value in the frozen snapshot (raw_sustained_tier, anchored on the observer's
 *   native baseline_price). We do NOT recompute baseline/peak from raw bars: a
 *   naive last-bar baseline diverges from the observer's anchor and inflates
 *   returns (verified: 32.6% dog agreement). Instead we TRUST the observer's
 *   native-domain fields for clean rows and QUARANTINE anything not cleanly
 *   evaluable in the native domain.
 *
 * Discipline:
 *   - PURE OFFLINE, deterministic, reproducible: reads only a frozen
 *     raw_signal_outcomes.snapshot.db. No GMGN, no chain RPC, no external API,
 *     no network, no time-of-day state. (The live observer computed the native
 *     fields; this producer only reads + gates them.)
 *   - NATIVE/SOL DOMAIN ONLY: requires baseline_price_unit='native'. usd_per_token
 *     baselines (graduated/AMM domain) are quarantined, not mixed in.
 *   - SYMMETRIC CLEANING: every signal passes identical maturation/coverage/
 *     baseline/evaluability/unit-suspect gates BEFORE a dog/dud label is assigned.
 *   - PREFER UNDER-COLLECT OVER DIRTY-COLLECT: not-cleanly-evaluable -> quarantine.
 *
 * Downstream (daily wrapper + accumulator) still re-checks has_trades,
 * complete_window, and coverage symmetry via the Dune/chain feature table.
 *
 * GMGN+chain decode is a SEPARATE discovery-grade audit path with its own
 * schema_version; it must never be mixed into this native-bars-only cumulative.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

// locked native-domain policy (documented; overridable for audit, not for formal OOS)
const MATURATION_SEC = 7200; // a signal must be >= this old (vs snapshot) to label (2h)
const MAX_SUSTAINED_PCT = 1500; // native sustained peak > 1500% (15x) is unit-suspect -> quarantine
// dog = native sustained tier gold|silver; dud = bronze|sub25 (clean same-bucket below-silver)
const DOG_TIERS = new Set(['gold', 'silver']);
const DUD_TIERS = new Set(['bronze', 'sub25']);

function sha256File(p) { return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex'); }

async function openSnapshot(p) {
  const mod = await import('better-sqlite3');
  const Database = mod.default || mod;
  return new Database(p, { readonly: true, fileMustExist: true });
}

function parseArgs(argv) {
  const a = { maturationSec: MATURATION_SEC, maxSustainedPct: MAX_SUSTAINED_PCT };
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i]; const v = argv[i + 1];
    if (k === '--snapshot') { a.snapshot = v; i += 1; }
    else if (k === '--out-dir') { a.outDir = v; i += 1; }
    else if (k === '--snapshot-ts') { a.snapshotTs = Number(v); i += 1; }
    else if (k === '--maturation-sec') { a.maturationSec = Number(v); i += 1; }
    else if (k === '--max-sustained-pct') { a.maxSustainedPct = Number(v); i += 1; }
    else if (k === '--help' || k === '-h') { a.help = true; }
  }
  return a;
}

async function main() {
  const a = parseArgs(process.argv);
  if (a.help || !a.snapshot) {
    console.log('usage: build-daily-oos-sol-curve-cohort.js --snapshot <raw_signal_outcomes.snapshot.db> '
      + '[--out-dir <dir>] [--snapshot-ts <unix>] [--maturation-sec 7200] [--max-sustained-pct 1500]');
    process.exit(a.help ? 0 : 2);
  }
  if (!fs.existsSync(a.snapshot)) { console.error(`snapshot not found: ${a.snapshot}`); process.exit(2); }
  const outDir = a.outDir || path.join(path.dirname(a.snapshot), 'oos-sol-curve-cohort-native');
  fs.mkdirSync(outDir, { recursive: true });

  const db = await openSnapshot(a.snapshot);
  const snapshotTs = a.snapshotTs
    || db.prepare("SELECT MAX(timestamp) t FROM raw_price_bars_1m WHERE price_unit='native'").get().t
    || db.prepare('SELECT MAX(timestamp) t FROM raw_price_bars_1m').get().t;

  const signals = db.prepare(`SELECT token_ca, signal_ts, signal_type, route, hard_gate_status,
    right_censored, matured_at_ts, coverage_reason, baseline_price, baseline_price_unit, baseline_lag_sec,
    max_sustained_peak_pct, raw_sustained_tier, raw_primary_tier, sustained_evaluable
    FROM raw_signal_outcomes`).all();
  db.close();

  const tally = {
    input_signals: signals.length, not_matured: 0,
    quarantine: {
      missing_baseline: 0, non_native_baseline: 0, incomplete_coverage: 0,
      not_evaluable: 0, native_unit_suspect: 0, unknown_tier: 0,
    },
    cohort: { dog: 0, dud: 0 }, tier: { gold: 0, silver: 0, bronze: 0, sub25: 0 },
  };
  const dogs = []; const duds = []; const quarantine = [];
  const qrow = (s, reason, extra = {}) => ({ token_ca: s.token_ca, signal_ts: Number(s.signal_ts), quarantine_reason: reason, raw_sustained_tier: s.raw_sustained_tier || null, ...extra });

  for (const s of signals) {
    const signalTs = Number(s.signal_ts);
    // 1) maturation (deterministic + observer's right_censored)
    if (s.right_censored !== 0 || !(signalTs <= snapshotTs - a.maturationSec)) { tally.not_matured += 1; continue; }
    // 2) native baseline domain gate (sol_curve native domain only)
    if (s.baseline_price == null || Number(s.baseline_price) <= 0) { tally.quarantine.missing_baseline += 1; quarantine.push(qrow(s, 'missing_baseline')); continue; }
    if (s.baseline_price_unit !== 'native') { tally.quarantine.non_native_baseline += 1; quarantine.push(qrow(s, 'non_native_baseline', { baseline_price_unit: s.baseline_price_unit })); continue; }
    // 3) coverage gate
    if (s.coverage_reason !== 'covered') { tally.quarantine.incomplete_coverage += 1; quarantine.push(qrow(s, `incomplete_coverage:${s.coverage_reason || 'null'}`)); continue; }
    // 4) evaluability gate: the SUSTAINED metric must be evaluable. NOTE: raw_primary_tier
    //    ='not_evaluable' is NOT a corruption signal (it fires on clean sustained rows too,
    //    e.g. a clean bronze 43%); corruption is caught by the unit-suspect guard below.
    if (s.sustained_evaluable === 0 || s.raw_sustained_tier == null) {
      tally.quarantine.not_evaluable += 1; quarantine.push(qrow(s, 'not_evaluable')); continue;
    }
    // 5) native unit-suspect guard (sustained peak must be sane in native domain)
    const sustPct = s.max_sustained_peak_pct == null ? null : Number(s.max_sustained_peak_pct);
    if (sustPct == null || sustPct > a.maxSustainedPct) { tally.quarantine.native_unit_suspect += 1; quarantine.push(qrow(s, 'native_unit_suspect', { max_sustained_peak_pct: sustPct })); continue; }
    // 6) label from observer native sustained tier (assigned AFTER symmetric cleaning)
    const tier = String(s.raw_sustained_tier).toLowerCase();
    const isDog = DOG_TIERS.has(tier); const isDud = DUD_TIERS.has(tier);
    if (!isDog && !isDud) { tally.quarantine.unknown_tier += 1; quarantine.push(qrow(s, 'unknown_tier', { tier })); continue; }
    if (tally.tier[tier] != null) tally.tier[tier] += 1;
    const row = {
      token_ca: s.token_ca, signal_ts: signalTs,
      label: isDog ? 'dog' : 'dud', effective_tier: tier, tier,
      return_domain: 'sol_curve', // native-asserted; downstream re-confirms has_trades/complete_window
      baseline_price: Number(s.baseline_price), baseline_price_unit: 'native',
      max_sustained_peak_pct: sustPct, sustained_return_frac: Number((sustPct / 100).toFixed(6)),
      corrected_peak_pct: Number((sustPct / 100).toFixed(6)),
      adjudication_mode: 'observer_native_sustained_tier', label_domain: 'native_sol',
      signal_type: s.signal_type || null, route: s.route || null, hard_gate_status: s.hard_gate_status || null,
      coverage_reason: s.coverage_reason,
    };
    if (isDog) { dogs.push(row); tally.cohort.dog += 1; } else { duds.push(row); tally.cohort.dud += 1; }
  }

  const bySig = (x, y) => (x.token_ca < y.token_ca ? -1 : x.token_ca > y.token_ca ? 1 : x.signal_ts - y.signal_ts);
  dogs.sort(bySig); duds.sort(bySig); quarantine.sort(bySig);

  fs.writeFileSync(path.join(outDir, 'clean-dogs.json'), JSON.stringify(dogs, null, 2));
  fs.writeFileSync(path.join(outDir, 'clean-duds.json'), JSON.stringify(duds, null, 2));
  fs.writeFileSync(path.join(outDir, 'quarantine.json'), JSON.stringify(quarantine, null, 2));

  const manifest = {
    schema_version: 'oos_sol_curve_cohort_native.v1',
    scope: 'sol_curve OOS clean dog/dud cohort ONLY (unique_buyers feature test). NOT a discovery-grade full-domain label adjudicator.',
    adjudication_mode: 'observer_native_sustained_tier',
    label_source: 'frozen snapshot raw_signal_outcomes.raw_sustained_tier (native-unit baseline_price anchor); NOT recomputed from bars.',
    external_apis_used: false, gmgn_used: false, chain_rpc_used: false,
    label_domain: 'native_sol',
    known_caveat: 'May undercount true dogs: rows without a native-unit baseline, without covered klines, marked '
      + `not_evaluable, or with native sustained peak > ${a.maxSustainedPct}% (unit-suspect) are quarantined rather than labeled. `
      + 'Prefer fewer-clean over larger-dirty. Native domain only; graduated/usd_per_token-baseline rows are excluded here.',
    source_snapshot: { path: path.resolve(a.snapshot), sha256: sha256File(a.snapshot), snapshot_ts: snapshotTs },
    params: {
      maturation_sec: a.maturationSec, max_sustained_pct: a.maxSustainedPct,
      dog_tiers: [...DOG_TIERS], dud_tiers: [...DUD_TIERS],
    },
    symmetry_note: 'All signals pass identical maturation/baseline/coverage/evaluability/unit-suspect gates BEFORE the dog/dud label is assigned, so cleaning is symmetric by construction.',
    tally,
    outputs: { clean_dogs: dogs.length, clean_duds: duds.length, quarantined: quarantine.length },
  };
  fs.writeFileSync(path.join(outDir, 'cohort-manifest.json'), JSON.stringify(manifest, null, 2));

  console.log(JSON.stringify({
    ok: true, snapshot_ts: snapshotTs, input_signals: tally.input_signals,
    not_matured: tally.not_matured, quarantine: tally.quarantine,
    cohort: tally.cohort, tier: tally.tier, out_dir: outDir,
  }, null, 2));
}

main().catch((e) => { console.error(`build-daily-oos-sol-curve-cohort: ${e.message}`); process.exit(1); });
