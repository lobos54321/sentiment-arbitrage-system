import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildReport,
  classifyFreeSource,
  volumeVisibilityStage,
} from '../scripts/run-free-source-coverage-audit.js';

test('classifies GMGN visibility stages from first nonzero volume lag', () => {
  assert.equal(volumeVisibilityStage({ first_nonzero_volume_lag_sec: null }), 'volume_not_visible_in_window');
  assert.equal(volumeVisibilityStage({ first_nonzero_volume_lag_sec: -60 }), 'already_volume_visible_at_anchor');
  assert.equal(volumeVisibilityStage({ first_nonzero_volume_lag_sec: 300 }), 'volume_visible_within_5m');
  assert.equal(volumeVisibilityStage({ first_nonzero_volume_lag_sec: 800 }), 'volume_visible_5m_to_15m');
  assert.equal(volumeVisibilityStage({ first_nonzero_volume_lag_sec: 1200 }), 'dark_after_15m');
});

test('classifies free-source verdicts and chain-truth needs', () => {
  const enough = classifyFreeSource({
    token_ca: 'READY',
    signal_ts: 1000,
    bars: 10,
    first_nonzero_volume_lag_sec: 0,
    residual_peak_0m_pct: 0.8,
  });
  const dark = classifyFreeSource({
    token_ca: 'DARK',
    signal_ts: 1000,
    bars: 10,
    first_nonzero_volume_lag_sec: 1200,
    residual_peak_0m_pct: 0.8,
  });

  assert.equal(enough.free_source_verdict, 'free_source_enough');
  assert.equal(dark.free_source_verdict, 'chain_truth_required');
  assert.equal(dark.chain_truth_need, 'pre_grad_or_dark_volume_truth');
});

test('buildReport emits targeted chain truth rows', () => {
  const report = buildReport({
    dogRows: [
      { token_ca: 'DOG', signal_ts: 1000, bars: 0, first_nonzero_volume_lag_sec: null },
    ],
    dudRows: [
      { token_ca: 'DUD', signal_ts: 1100, bars: 10, first_nonzero_volume_lag_sec: 0, residual_peak_0m_pct: 0.1 },
    ],
  });

  assert.equal(report.summary.dogs.chain_truth_required_n, 1);
  assert.equal(report.summary.duds.free_source_enough_n, 1);
  assert.equal(report.summary.targeted_chain_truth_rows_n, 1);
});

