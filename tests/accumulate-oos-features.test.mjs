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

test('cohort filter keeps only sol_curve + has_trades', () => {
  const rows = [
    row({ token_ca: 'a', signal_ts: 1 }),
    row({ token_ca: 'b', signal_ts: 2, return_domain: 'usd_gmgn' }), // wrong domain
    row({ token_ca: 'c', signal_ts: 3, trades_n: 0 }),               // no trades
    row({ token_ca: 'd', signal_ts: 4, label: 'dud' }),              // sol_curve dud, keep
  ];
  const { cohortRows, stats } = A.applyGates(rows, { trainingTokens: new Set(), seenSigKeys: new Set() });
  assert.deepEqual(cohortRows.map((r) => r.token_ca).sort(), ['a', 'd']);
  assert.equal(stats.input_rows, 4);
});

test('training-window tokens are excluded and counted', () => {
  const rows = [row({ token_ca: 'train', signal_ts: 1 }), row({ token_ca: 'fresh', signal_ts: 2 })];
  const { cohortRows, stats } = A.applyGates(rows, { trainingTokens: new Set(['train']), seenSigKeys: new Set() });
  assert.deepEqual(cohortRows.map((r) => r.token_ca), ['fresh']);
  assert.equal(stats.excluded_training_token_count, 1);
});

test('dedup within pack and cross-pack (idempotency)', () => {
  const rows = [row({ token_ca: 'x', signal_ts: 9 }), row({ token_ca: 'x', signal_ts: 9 })]; // dup
  const within = A.applyGates(rows, { trainingTokens: new Set(), seenSigKeys: new Set() });
  assert.equal(within.cohortRows.length, 1);
  assert.equal(within.stats.deduped_within_pack, 1);
  // cross-pack: already seen
  const cross = A.applyGates([row({ token_ca: 'x', signal_ts: 9 })], { trainingTokens: new Set(), seenSigKeys: new Set(['x|9']) });
  assert.equal(cross.cohortRows.length, 0);
  assert.equal(cross.stats.excluded_cross_pack_duplicate, 1);
});

test('LOCKED ORDER: training exclusion happens before cohort/dedup', () => {
  // a training token that is also sol_curve+has_trades must still be excluded
  const rows = [row({ token_ca: 'train', signal_ts: 1, return_domain: 'sol_curve', trades_n: 9 })];
  const { cohortRows, stats } = A.applyGates(rows, { trainingTokens: new Set(['train']), seenSigKeys: new Set() });
  assert.equal(cohortRows.length, 0);
  assert.equal(stats.excluded_training_token_count, 1);
});

test('symmetry report measures dog/dud trade-hit and complete-window asymmetry pre-cohort', () => {
  const pre = [
    row({ label: 'dog', trades_n: 5, feature_coverage_status: 'complete_window' }),
    row({ label: 'dog', trades_n: 0, feature_coverage_status: 'incomplete_window' }),
    row({ label: 'dud', trades_n: 5, feature_coverage_status: 'complete_window' }),
    row({ label: 'dud', trades_n: 5, feature_coverage_status: 'complete_window' }),
  ];
  const s = A.symmetryReport(pre);
  assert.equal(s.dog.trade_hit_rate, 0.5);   // 1/2
  assert.equal(s.dud.trade_hit_rate, 1.0);   // 2/2
  assert.equal(s.trade_hit_asymmetry_pp, -50.0);
});

test('GUARD: lookpointAnalysis THROWS below threshold (AUC cannot be computed)', () => {
  const rows = [];
  for (let i = 0; i < 10; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: i, label: 'dog' }));
  for (let i = 0; i < 10; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: 100 + i, label: 'dud' }));
  assert.throws(() => A.lookpointAnalysis(rows, 50), /look_point_not_reached/);
});

test('GUARD: disallowed look point throws', () => {
  assert.throws(() => A.lookpointAnalysis([], 75), /look_point_not_allowed/);
});

test('n=50 is futility-only: public verdict has NO auc field (sealed only)', () => {
  const rows = [];
  // separable: dogs high unique_buyers, duds low -> not futile
  for (let i = 0; i < 55; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: i, label: 'dog', unique_buyers: 50 + i }));
  for (let i = 0; i < 55; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: 1000 + i, label: 'dud', unique_buyers: i }));
  const res = A.lookpointAnalysis(rows, 50);
  assert.equal(res.public.mode, 'futility_only');
  assert.equal('auc' in res.public, false, 'n=50 public surface must NOT expose AUC');
  assert.ok(res.sealed && typeof res.sealed.auc === 'number', 'AUC sealed for audit');
  assert.equal(res.public.directional_null, false); // clearly separable -> not null
});

test('n=50 futility: non-separable data -> directional_null true', () => {
  const rng = (n) => Math.sin(n) * 0.5 + 0.5; // deterministic noise, same dist both classes
  const rows = [];
  for (let i = 0; i < 60; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: i, label: 'dog', unique_buyers: Math.round(rng(i) * 100) }));
  for (let i = 0; i < 60; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: 1000 + i, label: 'dud', unique_buyers: Math.round(rng(i + 0.5) * 100) }));
  const res = A.lookpointAnalysis(rows, 50);
  assert.equal(res.public.directional_null, true);
});

test('n=100 success path with strongly separable synthetic data', () => {
  const rows = [];
  for (let i = 0; i < 110; i += 1) rows.push(row({ token_ca: `d${i}`, signal_ts: i, label: 'dog', unique_buyers: 200 + i, progress_stage: i % 2 ? 'mid' : 'late' }));
  for (let i = 0; i < 110; i += 1) rows.push(row({ token_ca: `u${i}`, signal_ts: 1000 + i, label: 'dud', unique_buyers: i, progress_stage: i % 2 ? 'mid' : 'late' }));
  const res = A.lookpointAnalysis(rows, 100);
  assert.equal(res.public.verdict, 'success');
  assert.ok(res.public.auc > 0.6);
  assert.ok(res.public.ci_lo > 0.55);
});

test('aucRaw correctness: perfect separation = 1.0, identical = 0.5', () => {
  assert.equal(A.aucRaw([3, 4, 5], [0, 1, 2]), 1.0);
  assert.equal(A.aucRaw([1, 1], [1, 1]), 0.5);
});
