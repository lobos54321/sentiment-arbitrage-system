import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildPeakRows,
  splitTierRows,
} from '../scripts/build-chain-truth-tier-worklists.js';

test('buildPeakRows anchors quarantine rows at sustained peak time', () => {
  const rows = buildPeakRows([
    {
      token_ca: 'BAD',
      anchor_ts: 1000,
      cohort: 'quarantine',
      chain_truth_need: 'polluted_peak_window_adjudication',
      visibility_stage: 'unknown',
    },
    {
      token_ca: 'DOG',
      anchor_ts: 1000,
      cohort: 'dog',
      chain_truth_need: 'dark_peak',
      visibility_stage: 'dark',
    },
  ], new Map([
    ['BAD:1000', 300],
    ['DOG:1000', 500],
  ]));

  assert.equal(rows.length, 1);
  assert.equal(rows[0].token_ca, 'BAD');
  assert.equal(rows[0].anchor_ts, 1300);
  assert.equal(rows[0].chain_truth_need, 'polluted_peak_window_adjudication');
  assert.equal(rows[0].visibility_stage, 'peak_window');
});

test('buildPeakRows ignores null peak delay instead of treating it as zero', () => {
  const rows = buildPeakRows([
    {
      token_ca: 'BAD',
      anchor_ts: 1000,
      cohort: 'quarantine',
      chain_truth_need: 'polluted_peak_window_adjudication',
      visibility_stage: 'unknown',
    },
  ], new Map());

  assert.equal(rows.length, 0);
});

test('splitTierRows separates baseline, polluted peak, and native path rows', () => {
  const tiers = splitTierRows([
    {
      token_ca: 'BASE',
      anchor_ts: 1000,
      cohort: 'quarantine',
      chain_truth_need: 'baseline_reconstruction',
      visibility_stage: 'missing_baseline_price',
    },
    {
      token_ca: 'BAD',
      anchor_ts: 1100,
      cohort: 'quarantine',
      chain_truth_need: 'polluted_peak_window_adjudication',
      visibility_stage: 'label_unit_corrupt',
    },
    {
      token_ca: 'NATIVE',
      anchor_ts: 1200,
      cohort: 'quarantine',
      chain_truth_need: 'native_path_reconstruction',
      visibility_stage: 'no_native_bars',
    },
  ], new Map([
    ['BAD:1100', 300],
  ]));

  assert.deepEqual(tiers.baselineRows.map((row) => row.token_ca), ['BASE']);
  assert.deepEqual(tiers.nativePathRows.map((row) => row.token_ca), ['NATIVE']);
  assert.deepEqual(tiers.anchorRows.map((row) => row.token_ca), ['BASE', 'NATIVE']);
  assert.deepEqual(tiers.peakRows.map((row) => row.token_ca), ['BAD']);
  assert.equal(tiers.peakRows[0].anchor_ts, 1400);
});
