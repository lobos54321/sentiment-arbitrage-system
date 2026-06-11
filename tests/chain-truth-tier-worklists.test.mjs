import assert from 'node:assert/strict';
import test from 'node:test';

import { buildPeakRows } from '../scripts/build-chain-truth-tier-worklists.js';

test('buildPeakRows anchors quarantine rows at sustained peak time', () => {
  const rows = buildPeakRows([
    {
      token_ca: 'BAD',
      anchor_ts: 1000,
      cohort: 'quarantine',
      chain_truth_need: 'label_quarantine_adjudication',
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
      chain_truth_need: 'label_quarantine_adjudication',
      visibility_stage: 'unknown',
    },
  ], new Map());

  assert.equal(rows.length, 0);
});

