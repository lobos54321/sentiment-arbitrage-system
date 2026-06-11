import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildReport,
  replayForDelay,
} from '../scripts/run-gmgn-delayed-entry-replay.js';

test('delayed replay computes capture and precision by early 5m volume threshold', () => {
  const dogs = [
    {
      early_5m_volume_usd_sum: 25_000,
      residual_peak_5m_pct: 0.8,
      residual_to_silver_5m: true,
      residual_to_gold_5m: false,
    },
    {
      early_5m_volume_usd_sum: 3_000,
      residual_peak_5m_pct: 1.2,
      residual_to_silver_5m: true,
      residual_to_gold_5m: true,
    },
  ];
  const duds = [
    {
      early_5m_volume_usd_sum: 30_000,
      residual_peak_5m_pct: 0.2,
      residual_to_silver_5m: false,
      residual_to_gold_5m: false,
    },
    {
      early_5m_volume_usd_sum: 40_000,
      residual_peak_5m_pct: 0.7,
      residual_to_silver_5m: true,
      residual_to_gold_5m: false,
    },
  ];

  const row = replayForDelay({ dogs, duds, delayMin: 5, threshold: 20_000 });

  assert.equal(row.selected_dogs, 1);
  assert.equal(row.selected_duds, 2);
  assert.equal(row.dog_silver_capture_n, 1);
  assert.equal(row.dud_reaches_silver_n, 1);
  assert.equal(row.selected_precision_if_silver_target, 0.5);
});

test('buildReport emits all delay/threshold combinations', () => {
  const report = buildReport({
    dogs: [{ early_5m_volume_usd_sum: 0, residual_peak_0m_pct: 1, residual_to_silver_0m: true }],
    duds: [],
    thresholds: [0, 20_000],
  });

  assert.equal(report.rows.length, 6);
  assert.equal(report.inputs.note.includes('T+5m'), true);
});
