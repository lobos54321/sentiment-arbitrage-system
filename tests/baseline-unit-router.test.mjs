import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildUnitRouter,
  classifyBaselineUnit,
  directUnitRatio,
} from '../scripts/build-baseline-unit-router.js';

function assertNear(actual, expected, epsilon = 1e-9) {
  assert.ok(Math.abs(actual - expected) <= epsilon, `${actual} not within ${epsilon} of ${expected}`);
}

test('routes graduated rows to GMGN USD domain and flags direct unit-ratio diagnostics', () => {
  const row = {
    token_ca: 'TOKENpump',
    anchor_ts: 1000,
    baseline_route_v1: 'graduated_route_gmgn_amm',
    baseline_price_sol_chain: 4e-7,
  };
  const gmgn = {
    token_ca: 'TOKENpump',
    signal_ts: 1000,
    entry_0m_price: 0.00004,
    first_bar_lag_sec: -10,
  };

  const routed = classifyBaselineUnit(row, gmgn);

  assert.equal(routed.unit_domain, 'usd_gmgn');
  assert.equal(routed.baseline_unit_route, 'graduated_gmgn_amm');
  assert.equal(routed.baseline_price_usd_gmgn, 0.00004);
  assert.equal(routed.baseline_price_sol_chain_for_reference, 4e-7);
  assertNear(routed.direct_usd_per_sol_like_ratio, 100);
  assert.equal(routed.label_unit_suspect, true);
  assert.match(routed.return_calculation_rule, /never divide GMGN USD by curve SOL/);
});

test('keeps active curve rows in native SOL domain and marks spliced returns as required for GMGN peaks', () => {
  const routed = classifyBaselineUnit({
    token_ca: 'TOKENpump',
    anchor_ts: 1000,
    baseline_route_v1: 'curve_active_curve_baseline',
    baseline_price_sol_chain: 1.2e-7,
    baseline_progress_pct: 50,
  });

  assert.equal(routed.unit_domain, 'sol_curve');
  assert.equal(routed.baseline_unit_route, 'curve_active_native');
  assert.equal(routed.baseline_price_sol_curve, 1.2e-7);
  assert.equal(routed.spliced_return_required_if_peak_domain_is_gmgn, true);
  assert.equal(routed.label_unit_suspect, false);
});

test('summarizes route domains and missing GMGN anchor prices', () => {
  const report = buildUnitRouter({
    baselineRows: [
      { token_ca: 'A', anchor_ts: 1, baseline_route_v1: 'graduated_route_gmgn_amm', baseline_price_sol_chain: 4e-7 },
      { token_ca: 'B', anchor_ts: 2, baseline_route_v1: 'graduated_route_gmgn_amm', baseline_price_sol_chain: 4e-7 },
      { token_ca: 'C', anchor_ts: 3, baseline_route_v1: 'quiet_no_curve_trade_near_anchor' },
      { token_ca: 'D', anchor_ts: 4, baseline_route_v1: 'hot_tail_history_incomplete_dune' },
    ],
    gmgnRows: [
      { token_ca: 'A', signal_ts: 1, entry_0m_price: 0.00004 },
    ],
  });

  assert.equal(report.summary.rows_n, 4);
  assert.equal(report.summary.by_unit_domain.usd_gmgn, 1);
  assert.equal(report.summary.by_unit_domain.missing_gmgn_anchor_price, 1);
  assert.equal(report.summary.by_unit_domain.missing_curve_baseline, 1);
  assert.equal(report.summary.by_unit_domain.history_incomplete, 1);
  assert.equal(report.summary.missing_gmgn_anchor_price_n, 1);
});

test('computes direct unit ratio only as a diagnostic', () => {
  assertNear(directUnitRatio({ baseline_price_sol_chain: 2e-7 }, { entry_0m_price: 0.00002 }), 100);
  assert.equal(directUnitRatio({ baseline_price_sol_chain: 0 }, { entry_0m_price: 0.00002 }), null);
});
