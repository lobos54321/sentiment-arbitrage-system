import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as A from '../scripts/accumulate-oos-features.js';

const BASE = 1780000000; // June 2026, real-scale unix seconds
function row(over = {}) {
  return {
    token_ca: 'tok', signal_ts: 1, label: 'dog', return_domain: 'sol_curve',
    trades_n: 5, feature_coverage_status: 'complete_window', unique_buyers: 10,
    progress_stage: 'mid', schema_version: 'v1', ...over,
  };
}
const noPrev = { trainingTokens: new Set(), seenSigKeys: new Set() };

// strongly separable, robust: unique tokens, >=2 UTC dates, 2 stages
function robustRows(n) {
  const rows = [];
  for (let i = 0; i < n; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: BASE + (i % 3) * 86400 + i, label: 'dog', unique_buyers: 200 + i, progress_stage: i % 2 ? 'mid' : 'late' }));
  for (let i = 0; i < n; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: BASE + (i % 3) * 86400 + i, label: 'dud', unique_buyers: i, progress_stage: i % 2 ? 'mid' : 'late' }));
  return rows;
}
// non-separable (overlapping) feature: AUC ~0.5 -> would directional_null at n=50
function nullRows(n) {
  const rng = (k) => Math.sin(k) * 0.5 + 0.5;
  const rows = [];
  for (let i = 0; i < n; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: BASE + i, label: 'dog', unique_buyers: Math.round(rng(i) * 100) }));
  for (let i = 0; i < n; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: BASE + 1000 + i, label: 'dud', unique_buyers: Math.round(rng(i + 0.5) * 100) }));
  return rows;
}

test('cohort filter keeps only sol_curve + has_trades + complete_window', () => {
  const rows = [row({ token_ca: 'a', signal_ts: 1 }), row({ token_ca: 'b', signal_ts: 2, return_domain: 'usd_gmgn' }),
    row({ token_ca: 'c', signal_ts: 3, trades_n: 0 }), row({ token_ca: 'd', signal_ts: 4, label: 'dud' })];
  const { cohortRows } = A.applyGates(rows, noPrev);
  assert.deepEqual(cohortRows.map((r) => r.token_ca).sort(), ['a', 'd']);
});

test('B1: incomplete_window excluded + counted', () => {
  const rows = [row({ token_ca: 'a', signal_ts: 1 }), row({ token_ca: 'b', signal_ts: 2, feature_coverage_status: 'incomplete_window' }),
    row({ token_ca: 'c', signal_ts: 3, feature_coverage_status: 'decode_unavailable' })];
  const { cohortRows, stats } = A.applyGates(rows, noPrev);
  assert.deepEqual(cohortRows.map((r) => r.token_ca), ['a']);
  assert.equal(stats.excluded_incomplete_window, 2);
});

test('training tokens excluded (before cohort/dedup)', () => {
  const rows = [row({ token_ca: 'train', signal_ts: 1, trades_n: 9 }), row({ token_ca: 'fresh', signal_ts: 2 })];
  const { cohortRows, stats } = A.applyGates(rows, { trainingTokens: new Set(['train']), seenSigKeys: new Set() });
  assert.deepEqual(cohortRows.map((r) => r.token_ca), ['fresh']);
  assert.equal(stats.excluded_training_token_count, 1);
});

test('dedup within + cross-pack (idempotency)', () => {
  const within = A.applyGates([row({ token_ca: 'x', signal_ts: 9 }), row({ token_ca: 'x', signal_ts: 9 })], noPrev);
  assert.equal(within.cohortRows.length, 1);
  const cross = A.applyGates([row({ token_ca: 'x', signal_ts: 9 })], { trainingTokens: new Set(), seenSigKeys: new Set(['x|9']) });
  assert.equal(cross.cohortRows.length, 0);
});

test('B2a: missing progress_stage -> row excluded', () => {
  const rows = [row({ token_ca: 'a', signal_ts: 1 }), { ...row({ token_ca: 'b', signal_ts: 2 }), progress_stage: undefined }];
  const { cohortRows, stats } = A.applyGates(rows, noPrev);
  assert.deepEqual(cohortRows.map((r) => r.token_ca), ['a']);
  assert.equal(stats.excluded_missing_fields, 1);
});

test('symmetry report flags trade-hit asymmetry pre-cohort', () => {
  const s = A.symmetryReport([row({ label: 'dog', trades_n: 5 }), row({ label: 'dog', trades_n: 0 }),
    row({ label: 'dud', trades_n: 5 }), row({ label: 'dud', trades_n: 5 })]);
  assert.equal(s.trade_hit_asymmetry_pp, -50.0);
});

test('GUARD: AUC uncomputable below look point / disallowed look point', () => {
  assert.throws(() => A.lookpointAnalysis(robustRows(10), 50), /look_point_not_reached/);
  assert.throws(() => A.lookpointAnalysis([], 75), /look_point_not_allowed/);
});

test('raw AUC helpers are NOT exported', () => {
  assert.equal(A.aucRaw, undefined); assert.equal(A.bootstrapCi, undefined); assert.equal(A.topkPrecision, undefined);
});

test('n=50 futility-only: public has NO auc; sealed has it', () => {
  const res = A.lookpointAnalysis(robustRows(55), 50);
  assert.equal(res.public.mode, 'futility_only');
  assert.equal('auc' in res.public, false);
  assert.ok(res.sealed && typeof res.sealed.auc === 'number');
  assert.equal(res.public.directional_null, false);
});

test('n=50 non-separable -> directional_null', () => {
  const rng = (n) => Math.sin(n) * 0.5 + 0.5; const rows = [];
  for (let i = 0; i < 60; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: i, label: 'dog', unique_buyers: Math.round(rng(i) * 100) }));
  for (let i = 0; i < 60; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: 1e3 + i, label: 'dud', unique_buyers: Math.round(rng(i + 0.5) * 100) }));
  assert.equal(A.lookpointAnalysis(rows, 50).public.directional_null, true);
});

test('n=100 SUCCESS requires separable + >=2 stage + unique-token + >=2 dates', () => {
  const res = A.lookpointAnalysis(robustRows(110), 100);
  assert.equal(res.public.verdict, 'success');
  assert.ok(res.public.robustness.unique_token_survives && res.public.robustness.date_survives && res.public.robustness.stage_survives !== false);
});

test('B2b: degenerate single-stage cannot success', () => {
  const rows = robustRows(110).map((r) => ({ ...r, progress_stage: 'mid' }));
  const res = A.lookpointAnalysis(rows, 100);
  assert.notEqual(res.public.verdict, 'success');
  assert.equal(res.public.stage_survives, false);
});

test('B-new-1a: single token cluster cannot success (unique-token gate)', () => {
  const rows = [];
  for (let i = 0; i < 110; i += 1) rows.push(row({ token_ca: 'ONE_DOG', signal_ts: BASE + i, label: 'dog', unique_buyers: 200 + i, progress_stage: i % 2 ? 'mid' : 'late' }));
  for (let i = 0; i < 110; i += 1) rows.push(row({ token_ca: 'ONE_DUD', signal_ts: BASE + i, label: 'dud', unique_buyers: i, progress_stage: i % 2 ? 'mid' : 'late' }));
  const res = A.lookpointAnalysis(rows, 100);
  assert.notEqual(res.public.verdict, 'success');
  assert.equal(res.public.robustness.unique_token_survives, false);
});

test('B-new-1b: single UTC day cannot success (date gate)', () => {
  const rows = []; // all same day
  for (let i = 0; i < 110; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: BASE + i, label: 'dog', unique_buyers: 200 + i, progress_stage: i % 2 ? 'mid' : 'late' }));
  for (let i = 0; i < 110; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: BASE + i, label: 'dud', unique_buyers: i, progress_stage: i % 2 ? 'mid' : 'late' }));
  const res = A.lookpointAnalysis(rows, 100);
  assert.notEqual(res.public.verdict, 'success');
  assert.equal(res.public.robustness.date_survives, false);
});

test('B-new-3: coverage asymmetry above threshold -> coverage_biased_inconclusive', () => {
  const res = A.lookpointAnalysis(robustRows(110), 100, { coverageAsymmetryPp: 30 });
  assert.equal(res.public.verdict, 'coverage_biased_inconclusive');
  assert.equal(res.public.coverage.coverage_ok, false);
});

test('R3-1: n=50 + high coverage asymmetry pre-empts futility STOP (no AUC read)', () => {
  // nullRows would directional_null:true (STOP) at n=50; coverage bias must pre-empt
  const res = A.lookpointAnalysis(nullRows(55), 50, { coverageAsymmetryPp: 30 });
  assert.equal(res.public.verdict, 'coverage_biased_inconclusive');
  assert.equal('directional_null' in res.public, false); // not the futility surface
  assert.equal('mode' in res.public, false);
  assert.equal(res.sealed, null); // AUC not even computed/sealed
  // sanity: same data WITHOUT coverage bias does take the futility STOP path
  const base = A.lookpointAnalysis(nullRows(55), 50);
  assert.equal(base.public.directional_null, true);
});

test('R3-2: edge driven by ONE source (leave-one-source-out fails) cannot success', () => {
  const rows = [];
  for (let i = 0; i < 60; i += 1) { // source A: separable, carries the edge
    rows.push(row({ token_ca: `a_d${i}`, signal_ts: BASE + (i % 3) * 86400 + i, label: 'dog', unique_buyers: 200 + i, progress_stage: i % 2 ? 'mid' : 'late', source: 'A' }));
    rows.push(row({ token_ca: `a_u${i}`, signal_ts: BASE + (i % 3) * 86400 + i, label: 'dud', unique_buyers: i, progress_stage: i % 2 ? 'mid' : 'late', source: 'A' }));
  }
  for (let i = 0; i < 60; i += 1) { // source B: null (overlapping), no edge
    rows.push(row({ token_ca: `b_d${i}`, signal_ts: BASE + (i % 3) * 86400 + i, label: 'dog', unique_buyers: 100 + (i % 5), progress_stage: i % 2 ? 'mid' : 'late', source: 'B' }));
    rows.push(row({ token_ca: `b_u${i}`, signal_ts: BASE + (i % 3) * 86400 + i, label: 'dud', unique_buyers: 100 + (i % 5), progress_stage: i % 2 ? 'mid' : 'late', source: 'B' }));
  }
  const res = A.lookpointAnalysis(rows, 100);
  assert.notEqual(res.public.verdict, 'success');
  assert.equal(res.public.robustness.source_survives, false);
});

test('R3-3: edge driven by ONE date (leave-each-day-out, not just largest) cannot success', () => {
  const rows = [];
  for (let i = 0; i < 20; i += 1) { // small day D1: separable, carries the edge
    rows.push(row({ token_ca: `s_d${i}`, signal_ts: BASE + i, label: 'dog', unique_buyers: 200 + i, progress_stage: i % 2 ? 'mid' : 'late' }));
    rows.push(row({ token_ca: `s_u${i}`, signal_ts: BASE + i, label: 'dud', unique_buyers: i, progress_stage: i % 2 ? 'mid' : 'late' }));
  }
  for (let i = 0; i < 90; i += 1) { // large day D2: null (overlapping), no edge
    rows.push(row({ token_ca: `l_d${i}`, signal_ts: BASE + 5 * 86400 + i, label: 'dog', unique_buyers: 100 + (i % 5), progress_stage: i % 2 ? 'mid' : 'late' }));
    rows.push(row({ token_ca: `l_u${i}`, signal_ts: BASE + 5 * 86400 + i, label: 'dud', unique_buyers: 100 + (i % 5), progress_stage: i % 2 ? 'mid' : 'late' }));
  }
  const res = A.lookpointAnalysis(rows, 100);
  assert.notEqual(res.public.verdict, 'success');
  assert.equal(res.public.robustness.date_survives, false);
});

test('B3: rowSchemaGate rejects mixed / mismatched row schema; same/all-missing ok', () => {
  assert.throws(() => A.rowSchemaGate([{ schema_version: 'v1' }, { schema_version: 'v2' }], 'v1'), /schema_mismatch/);
  assert.throws(() => A.rowSchemaGate([{ schema_version: 'v1' }, {}], 'v1'), /schema_mismatch/);
  assert.throws(() => A.rowSchemaGate([{ schema_version: 'v2' }], 'v1'), /schema_mismatch/);
  assert.doesNotThrow(() => A.rowSchemaGate([{ schema_version: 'v1' }, { schema_version: 'v1' }], 'v1'));
  assert.doesNotThrow(() => A.rowSchemaGate([{}, {}], 'v1')); // none carry it -> packSchema stamps
});

test('schemaGate / resolveSchema fail-closed', () => {
  assert.throws(() => A.schemaGate('v2', 'v1'), /schema_drift/);
  assert.throws(() => A.resolveSchema({}, [{}], null), /schema_version_required/);
  assert.equal(A.resolveSchema({ schema_version: 'v3' }, [], null), 'v3');
});
