import assert from 'node:assert/strict';
import test from 'node:test';

import {
  auc,
  buildReport,
  enrichRows,
  volumeVisibilityStage,
} from '../scripts/run-gmgn-stage-stratified-audit.js';

test('auc returns probability that dog value beats dud value with tie credit', () => {
  assert.equal(auc([2, 4], [1, 3]), 0.75);
  assert.equal(auc([1], [1]), 0.5);
  assert.equal(auc([], [1]), null);
});

test('volume visibility stage buckets first nonzero volume lag', () => {
  assert.equal(volumeVisibilityStage({ first_nonzero_volume_lag_sec: null }), 'volume_not_visible_in_window');
  assert.equal(volumeVisibilityStage({ first_nonzero_volume_lag_sec: -1 }), 'already_volume_visible_at_anchor');
  assert.equal(volumeVisibilityStage({ first_nonzero_volume_lag_sec: 120 }), 'volume_visible_within_5m');
  assert.equal(volumeVisibilityStage({ first_nonzero_volume_lag_sec: 600 }), 'volume_visible_5m_to_15m');
  assert.equal(volumeVisibilityStage({ first_nonzero_volume_lag_sec: 1200 }), 'dark_after_15m');
});

test('enrichRows marks dark peaks before first nonzero volume and attaches decision lag', () => {
  const peakLabels = new Map([
    ['DOG:1000', { tier: 'gold', time_to_sustained_peak_sec: 300, max_sustained_peak_pct: 1.2 }],
    ['DOG2:1000', { tier: 'silver', time_to_sustained_peak_sec: 900, max_sustained_peak_pct: 0.7 }],
  ]);
  const decisionMatches = new Map([
    ['DOG:1000', { decision_ts: 1075, decision_source: 'a_class_decision_events', decision_lag_sec: 75 }],
  ]);

  const rows = enrichRows([
    { token_ca: 'DOG', signal_ts: 1000, first_nonzero_volume_lag_sec: 600 },
    { token_ca: 'DOG2', signal_ts: 1000, first_nonzero_volume_lag_sec: 600 },
  ], { peakLabels, decisionMatches });

  assert.equal(rows[0].peak_confirmation_tier, 'curve_phase_unconfirmed');
  assert.equal(rows[0].decision_lag_sec, 75);
  assert.equal(rows[1].peak_confirmation_tier, 'volume_confirmed');
  assert.equal(rows[1].decision_ts, null);
});

test('buildReport separates full and stage-stratified early volume comparisons', () => {
  const dogs = enrichRows([
    {
      token_ca: 'DOGA',
      signal_ts: 1000,
      first_nonzero_volume_lag_sec: 1200,
      early_15m_volume_usd_sum: 0,
      volume_usd_sum: 100000,
    },
    {
      token_ca: 'DOGB',
      signal_ts: 1000,
      first_nonzero_volume_lag_sec: 60,
      early_15m_volume_usd_sum: 100,
      volume_usd_sum: 50000,
    },
  ]);
  const duds = enrichRows([
    {
      token_ca: 'DUDA',
      signal_ts: 1000,
      first_nonzero_volume_lag_sec: -10,
      early_15m_volume_usd_sum: 50,
      volume_usd_sum: 1000,
    },
    {
      token_ca: 'DUDB',
      signal_ts: 1000,
      first_nonzero_volume_lag_sec: 30,
      early_15m_volume_usd_sum: 10,
      volume_usd_sum: 1000,
    },
  ]);

  const report = buildReport({
    dogs,
    duds,
    args: { dogs: 'dogs.json', duds: 'duds.json', rawDb: '', paperDb: '' },
  });

  assert.equal(report.inputs.dogs_n, 2);
  assert.equal(report.visibility.dogs_by_stage.dark_after_15m, 1);
  assert.equal(report.visibility.duds_by_stage.already_volume_visible_at_anchor, 1);
  assert.equal(report.auc.early_15m_volume_usd_sum.full, 0.5);
  assert.equal(report.auc.early_15m_volume_usd_sum.by_stage.volume_visible_within_5m.auc_all, 1);
});
