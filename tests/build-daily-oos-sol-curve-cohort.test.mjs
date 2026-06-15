import { test } from 'node:test';
import assert from 'node:assert/strict';
import { classifySignal, dedupBySignal } from '../scripts/build-daily-oos-sol-curve-cohort.js';

// a fully formal-eligible + native + clean row (gold dog by default)
function row(over = {}) {
  return {
    token_ca: 'tok', signal_ts: 1781300000, observation_status: 'matured',
    kline_covered: 1, baseline_confidence: 'high', same_source_path: 1, outlier_flag: 0, sustained_evaluable: 1,
    baseline_price: 3e-7, baseline_price_unit: 'native', max_sustained_peak_pct: 120, raw_primary_tier: 'gold',
    ...over,
  };
}

test('eligible+native gold -> dog; silver -> dog; bronze/sub25 -> dud', () => {
  assert.equal(classifySignal(row({ raw_primary_tier: 'gold' })).disposition, 'dog');
  assert.equal(classifySignal(row({ raw_primary_tier: 'silver', max_sustained_peak_pct: 60 })).disposition, 'dog');
  assert.equal(classifySignal(row({ raw_primary_tier: 'bronze', max_sustained_peak_pct: 30 })).disposition, 'dud');
  assert.equal(classifySignal(row({ raw_primary_tier: 'sub25', max_sustained_peak_pct: 10 })).disposition, 'dud');
});

test('BLOCKING (codex): low-confidence / kline_covered=0 but tier=gold MUST be excluded', () => {
  // exact reproduction: a gold row that is NOT formal-eligible must never become a dog
  assert.equal(classifySignal(row({ kline_covered: 0, raw_primary_tier: 'gold' })).disposition, 'quarantine');
  assert.equal(classifySignal(row({ kline_covered: 0, raw_primary_tier: 'gold' })).reason, 'not_eligible_kline_uncovered');
  assert.equal(classifySignal(row({ baseline_confidence: 'low', raw_primary_tier: 'gold' })).disposition, 'quarantine');
  assert.equal(classifySignal(row({ baseline_confidence: 'low', raw_primary_tier: 'gold' })).reason, 'not_eligible_baseline_confidence');
});

test('each formal-eligibility gate rejects (gold row, one field bad at a time)', () => {
  assert.equal(classifySignal(row({ same_source_path: 0 })).reason, 'not_eligible_cross_source_path');
  assert.equal(classifySignal(row({ outlier_flag: 1 })).reason, 'not_eligible_outlier');
  assert.equal(classifySignal(row({ sustained_evaluable: 0 })).reason, 'not_eligible_sustained');
  assert.equal(classifySignal(row({ baseline_confidence: null })).reason, 'not_eligible_baseline_confidence');
});

test('not matured -> not_matured (never labeled)', () => {
  assert.equal(classifySignal(row({ observation_status: 'pending' })).disposition, 'not_matured');
  assert.equal(classifySignal(row({ observation_status: null })).disposition, 'not_matured');
});

test('native-domain restriction: usd baseline excluded even if eligible+gold', () => {
  assert.equal(classifySignal(row({ baseline_price_unit: 'usd_per_token' })).reason, 'non_native_baseline');
  assert.equal(classifySignal(row({ baseline_price: null })).reason, 'missing_baseline');
});

test('unit-suspect guard: native sustained > 1500% quarantined (rejects USD/native pollution)', () => {
  assert.equal(classifySignal(row({ max_sustained_peak_pct: 11203.32 })).reason, 'native_unit_suspect');
  assert.equal(classifySignal(row({ max_sustained_peak_pct: 99335175331 })).reason, 'native_unit_suspect');
  assert.equal(classifySignal(row({ max_sustained_peak_pct: null })).reason, 'native_unit_suspect');
  // 1500% exactly is allowed; just above is not
  assert.equal(classifySignal(row({ max_sustained_peak_pct: 1500, raw_primary_tier: 'gold' })).disposition, 'dog');
  assert.equal(classifySignal(row({ max_sustained_peak_pct: 1500.01 })).reason, 'native_unit_suspect');
});

test('gate ORDER: maturation before eligibility before native before unit-suspect', () => {
  // a row failing multiple gates reports the FIRST (most upstream) reason
  assert.equal(classifySignal(row({ observation_status: 'pending', kline_covered: 0 })).disposition, 'not_matured');
  assert.equal(classifySignal(row({ kline_covered: 0, baseline_price_unit: 'usd_per_token' })).reason, 'not_eligible_kline_uncovered');
  assert.equal(classifySignal(row({ baseline_price_unit: 'usd_per_token', max_sustained_peak_pct: 99999 })).reason, 'non_native_baseline');
});

test('unknown/degenerate tier on an otherwise-eligible row -> quarantine, not labeled', () => {
  assert.equal(classifySignal(row({ raw_primary_tier: 'unknown' })).reason, 'unknown_tier');
  assert.equal(classifySignal(row({ raw_primary_tier: null })).reason, 'unknown_tier');
});

// ---- ROOT dedup to prereg unit (token_ca, signal_ts) ----
const drow = (over = {}) => ({ token_ca: 'T', signal_ts: 100, label: 'dog', tier: 'gold', max_sustained_peak_pct: 120, baseline_price: 1e-7, ...over });

test('dedup: same (token,signal_ts) x3 -> 1 row, removed=2 (prereg unit)', () => {
  const out = dedupBySignal([drow(), drow(), drow()]);
  assert.equal(out.rows.length, 1);
  assert.equal(out.removed, 2);
});

test('dedup: keeps the best observation (highest max_sustained_peak_pct), deterministic', () => {
  const out = dedupBySignal([drow({ max_sustained_peak_pct: 60 }), drow({ max_sustained_peak_pct: 300 }), drow({ max_sustained_peak_pct: 120 })]);
  assert.equal(out.rows.length, 1);
  assert.equal(out.rows[0].max_sustained_peak_pct, 300);
  // order-independent: same result regardless of input order
  const out2 = dedupBySignal([drow({ max_sustained_peak_pct: 300 }), drow({ max_sustained_peak_pct: 120 }), drow({ max_sustained_peak_pct: 60 })]);
  assert.equal(out2.rows[0].max_sustained_peak_pct, 300);
});

test('dedup: distinct (token,signal_ts) preserved; conflicting tier resolves to best', () => {
  const out = dedupBySignal([drow({ signal_ts: 100 }), drow({ signal_ts: 200 }), drow({ token_ca: 'U', signal_ts: 100 })]);
  assert.equal(out.rows.length, 3);
  assert.equal(out.removed, 0);
  // same signal observed as dud(bronze, low peak) and dog(gold, high peak) -> keep dog (best)
  const conflict = dedupBySignal([drow({ label: 'dud', tier: 'bronze', max_sustained_peak_pct: 30 }), drow({ label: 'dog', tier: 'gold', max_sustained_peak_pct: 200 })]);
  assert.equal(conflict.rows.length, 1);
  assert.equal(conflict.rows[0].label, 'dog');
});
