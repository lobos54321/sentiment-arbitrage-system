import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as A from '../scripts/accumulate-oos-features.js';

function row(over = {}) {
  return {
    token_ca: 'tok', signal_ts: 1, label: 'dog', return_domain: 'sol_curve',
    trades_n: 5, feature_coverage_status: 'complete_window', unique_buyers: 10,
    progress_stage: 'mid', ...over,
  };
}
const noPrev = { trainingTokens: new Set(), seenSigKeys: new Set() };

test('cohort filter keeps only sol_curve + has_trades + complete_window', () => {
  const rows = [
    row({ token_ca: 'a', signal_ts: 1 }),
    row({ token_ca: 'b', signal_ts: 2, return_domain: 'usd_gmgn' }),            // wrong domain
    row({ token_ca: 'c', signal_ts: 3, trades_n: 0 }),                          // no trades
    row({ token_ca: 'd', signal_ts: 4, label: 'dud' }),                        // keep
  ];
  const { cohortRows, stats } = A.applyGates(rows, noPrev);
  assert.deepEqual(cohortRows.map((r) => r.token_ca).sort(), ['a', 'd']);
  assert.equal(stats.input_rows, 4);
});

test('BLOCKER1: incomplete_window rows are excluded from cohort and counted', () => {
  const rows = [
    row({ token_ca: 'a', signal_ts: 1, feature_coverage_status: 'complete_window' }),
    row({ token_ca: 'b', signal_ts: 2, feature_coverage_status: 'incomplete_window' }),
    row({ token_ca: 'c', signal_ts: 3, feature_coverage_status: 'decode_unavailable' }),
  ];
  const { cohortRows, stats } = A.applyGates(rows, noPrev);
  assert.deepEqual(cohortRows.map((r) => r.token_ca), ['a']);
  assert.equal(stats.excluded_incomplete_window, 2);
});

test('training-window tokens are excluded and counted', () => {
  const rows = [row({ token_ca: 'train', signal_ts: 1 }), row({ token_ca: 'fresh', signal_ts: 2 })];
  const { cohortRows, stats } = A.applyGates(rows, { trainingTokens: new Set(['train']), seenSigKeys: new Set() });
  assert.deepEqual(cohortRows.map((r) => r.token_ca), ['fresh']);
  assert.equal(stats.excluded_training_token_count, 1);
});

test('dedup within pack and cross-pack (idempotency)', () => {
  const rows = [row({ token_ca: 'x', signal_ts: 9 }), row({ token_ca: 'x', signal_ts: 9 })];
  const within = A.applyGates(rows, noPrev);
  assert.equal(within.cohortRows.length, 1);
  assert.equal(within.stats.deduped_within_pack, 1);
  const cross = A.applyGates([row({ token_ca: 'x', signal_ts: 9 })], { trainingTokens: new Set(), seenSigKeys: new Set(['x|9']) });
  assert.equal(cross.cohortRows.length, 0);
  assert.equal(cross.stats.excluded_cross_pack_duplicate, 1);
});

test('LOCKED ORDER: training exclusion before cohort/dedup', () => {
  const rows = [row({ token_ca: 'train', signal_ts: 1, return_domain: 'sol_curve', trades_n: 9 })];
  const { cohortRows, stats } = A.applyGates(rows, { trainingTokens: new Set(['train']), seenSigKeys: new Set() });
  assert.equal(cohortRows.length, 0);
  assert.equal(stats.excluded_training_token_count, 1);
});

test('BLOCKER2a: required progress_stage missing -> row excluded', () => {
  const rows = [row({ token_ca: 'a', signal_ts: 1 }), { ...row({ token_ca: 'b', signal_ts: 2 }), progress_stage: undefined }];
  const { cohortRows, stats } = A.applyGates(rows, noPrev);
  assert.deepEqual(cohortRows.map((r) => r.token_ca), ['a']);
  assert.equal(stats.excluded_missing_fields, 1);
});

test('symmetry report measures dog/dud trade-hit asymmetry pre-cohort', () => {
  const pre = [
    row({ label: 'dog', trades_n: 5 }), row({ label: 'dog', trades_n: 0 }),
    row({ label: 'dud', trades_n: 5 }), row({ label: 'dud', trades_n: 5 }),
  ];
  const s = A.symmetryReport(pre);
  assert.equal(s.dog.trade_hit_rate, 0.5);
  assert.equal(s.dud.trade_hit_rate, 1.0);
  assert.equal(s.trade_hit_asymmetry_pp, -50.0);
});

test('GUARD: lookpointAnalysis THROWS below threshold (AUC uncomputable)', () => {
  const rows = [];
  for (let i = 0; i < 10; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: i, label: 'dog' }));
  for (let i = 0; i < 10; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: 100 + i, label: 'dud' }));
  assert.throws(() => A.lookpointAnalysis(rows, 50), /look_point_not_reached/);
});

test('GUARD: disallowed look point throws', () => {
  assert.throws(() => A.lookpointAnalysis([], 75), /look_point_not_allowed/);
});

test('BLOCKER (secondary): raw AUC helpers are NOT exported', () => {
  assert.equal(A.aucRaw, undefined);
  assert.equal(A.bootstrapCi, undefined);
  assert.equal(A.topkPrecision, undefined);
});

test('n=50 futility-only: public verdict has NO auc field; AUC only in sealed', () => {
  const rows = [];
  for (let i = 0; i < 55; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: i, label: 'dog', unique_buyers: 50 + i }));
  for (let i = 0; i < 55; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: 1000 + i, label: 'dud', unique_buyers: i }));
  const res = A.lookpointAnalysis(rows, 50);
  assert.equal(res.public.mode, 'futility_only');
  assert.equal('auc' in res.public, false);
  assert.equal('ci_lo' in res.public, false);
  assert.ok(res.sealed && typeof res.sealed.auc === 'number');
  assert.equal(res.public.directional_null, false);
});

test('n=50 futility: non-separable -> directional_null true', () => {
  const rng = (n) => Math.sin(n) * 0.5 + 0.5;
  const rows = [];
  for (let i = 0; i < 60; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: i, label: 'dog', unique_buyers: Math.round(rng(i) * 100) }));
  for (let i = 0; i < 60; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: 1000 + i, label: 'dud', unique_buyers: Math.round(rng(i + 0.5) * 100) }));
  assert.equal(A.lookpointAnalysis(rows, 50).public.directional_null, true);
});

test('n=100 success with separable data AND a genuine >=2 stage split', () => {
  const rows = [];
  for (let i = 0; i < 110; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: i, label: 'dog', unique_buyers: 200 + i, progress_stage: i % 2 ? 'mid' : 'late' }));
  for (let i = 0; i < 110; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: 1000 + i, label: 'dud', unique_buyers: i, progress_stage: i % 2 ? 'mid' : 'late' }));
  const res = A.lookpointAnalysis(rows, 100);
  assert.equal(res.public.verdict, 'success');
  assert.ok(res.public.auc > 0.6 && res.public.ci_lo > 0.55);
});

test('BLOCKER2b: degenerate single-stage split cannot declare success', () => {
  const rows = []; // strongly separable but ALL one stage -> stage control fails
  for (let i = 0; i < 110; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: i, label: 'dog', unique_buyers: 200 + i, progress_stage: 'mid' }));
  for (let i = 0; i < 110; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: 1000 + i, label: 'dud', unique_buyers: i, progress_stage: 'mid' }));
  const res = A.lookpointAnalysis(rows, 100);
  assert.notEqual(res.public.verdict, 'success');
  assert.equal(res.public.stage_survives, false);
});

test('BLOCKER3: schemaGate rejects schema drift; resolveSchema requires a schema', () => {
  assert.equal(A.schemaGate('v2', null), 'v2');             // first lock ok
  assert.equal(A.schemaGate('v2', 'v2'), 'v2');             // same ok
  assert.throws(() => A.schemaGate('v2', 'v1'), /schema_drift/);
  assert.throws(() => A.resolveSchema({}, [{}], null), /schema_version_required/);
  assert.throws(() => A.resolveSchema({ schema_version: 'unknown' }, [], null), /schema_version_required/);
  assert.equal(A.resolveSchema({ schema_version: 'v3' }, [], null), 'v3');
  assert.equal(A.resolveSchema({}, [], 'override-v4'), 'override-v4');
});
