import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildReport,
} from '../scripts/run-gmgn-matched-anchor-ceiling.js';

test('matched anchor report compares the same token cohort', () => {
  const signalRows = [
    {
      token_ca: 'A',
      residual_peak_0m_pct: 1.0,
      residual_to_silver_0m: true,
      residual_to_gold_0m: true,
      entry_0m_price: 1,
      entry_0m_ts: 100,
      first_nonzero_volume_ts: 90,
    },
    {
      token_ca: 'B',
      residual_peak_0m_pct: 0.6,
      residual_to_silver_0m: true,
      residual_to_gold_0m: false,
      entry_0m_price: 1,
      entry_0m_ts: 100,
      first_nonzero_volume_ts: 90,
    },
    {
      token_ca: 'UNMATCHED_SIGNAL',
      residual_peak_0m_pct: 4.0,
      residual_to_silver_0m: true,
      residual_to_gold_0m: true,
    },
  ];
  const decisionRows = [
    {
      token_ca: 'A',
      residual_peak_0m_pct: 0.8,
      residual_to_silver_0m: true,
      residual_to_gold_0m: false,
      entry_0m_price: 1.2,
      entry_0m_ts: 110,
      first_nonzero_volume_ts: 90,
    },
    {
      token_ca: 'B',
      residual_peak_0m_pct: 0.4,
      residual_to_silver_0m: false,
      residual_to_gold_0m: false,
      entry_0m_price: 1.1,
      entry_0m_ts: 110,
      first_nonzero_volume_ts: 90,
    },
  ];

  const report = buildReport({ signalRows, decisionRows });
  const row = report.rows.find((item) => item.delay_min === 0);

  assert.equal(report.inputs.matched_tokens_n, 2);
  assert.equal(row.signal_silver_capture_rate, 1);
  assert.equal(row.decision_silver_capture_rate, 0.5);
  assert.equal(row.silver_delta_decision_minus_signal, -0.5);
  assert.equal(row.signal_gold_capture_rate, 0.5);
  assert.equal(row.decision_gold_capture_rate, 0);
  assert.equal(row.anchor_comparison_definition, undefined);
  assert.equal(report.anchor_comparison_definition.includes('Matched by token_ca'), true);
});
