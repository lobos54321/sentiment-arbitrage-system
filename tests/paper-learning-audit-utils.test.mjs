import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildFastFailCounterfactualAudit,
  buildSampleGovernance,
  buildShadowTrailAudit,
} from '../src/web/paper-learning-audit-utils.js';

test('shadow trail audit ranks cohort trail improvements', () => {
  const trades = [
    {
      id: 1,
      symbol: 'DOG',
      entry_mode: 'hard_gate_pass_tiny_probe',
      capital_tier: 'tiny_probe',
      pnl_pct: -0.02,
      peak_pnl: 0.2,
      position_size_sol: 0.002,
    },
  ];
  const audit = buildShadowTrailAudit({ trades, pathSamplesByTrade: new Map() });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].capital_tier, 'tiny_probe');
  assert.equal(audit[0].actual_avg_giveback_pct, 22);
  assert.equal(audit[0].scenarios[0].scenario, 'peak10_lock80');
  assert.equal(audit[0].scenarios[0].improvement_avg_pct, 18);
});

test('fast-fail counterfactual reports missing and quote-clean post-exit coverage', () => {
  const trades = [
    { id: 1, symbol: 'DOG', exit_reason: 'no_follow_fast_fail_20s', pnl_pct: -0.05, peak_pnl: 0.01 },
    { id: 2, symbol: 'CAT', exit_reason: 'trail', pnl_pct: 0.03, peak_pnl: 0.1 },
  ];
  const samples = new Map([
    [1, [{ trade_id: 1, sample_ts: 200, quote_success: 1, quote_pnl: 0.25, mark_pnl: 0.3 }]],
  ]);
  const audit = buildFastFailCounterfactualAudit({ trades, pathSamplesByTrade: samples });
  assert.equal(audit.trades, 1);
  assert.equal(audit.quote_clean_coverage_n, 1);
  assert.equal(audit.regret_20pct_quote_clean_n, 1);
  assert.equal(audit.rows[0].post_exit_quote_clean_peak_pct, 25);
});

test('sample governance keeps small samples in collection mode', () => {
  const rows = buildSampleGovernance([{ entry_mode: 'hard_gate_pass_tiny_probe', fills: 7, wins: 2, avg_pnl: -0.03 }]);
  assert.equal(rows[0].decision, 'continue_sampling');
  assert.equal(rows[0].reason, 'sample_n_below_30');
});
