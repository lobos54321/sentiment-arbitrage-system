import test from 'node:test';
import assert from 'node:assert/strict';

import {
  classifyPass4Route,
  mergeRoutes,
} from '../scripts/merge-pass4-baseline-route.js';

test('classifies Pass-4 graduated rows for GMGN/AMM repricing', () => {
  const route = classifyPass4Route({
    bars: [{}],
    baseline_progress_pct: 100,
  });
  assert.equal(route, 'graduated_route_gmgn_amm');
});

test('classifies active and near-graduation curve rows', () => {
  assert.equal(classifyPass4Route({ bars: [{}], baseline_progress_pct: 70 }), 'curve_active_curve_baseline');
  assert.equal(classifyPass4Route({ bars: [{}], baseline_progress_pct: 90 }), 'near_graduation_curve_baseline');
});

test('leaves empty Pass-4 rows as missing instead of inventing a route', () => {
  assert.equal(classifyPass4Route({ bars: [], baseline_progress_pct: null }), null);
});

test('merges quiet rows with true Pass-4 evidence at signal granularity', () => {
  const report = mergeRoutes({
    baselineRows: [
      {
        token_ca: 'A',
        anchor_ts: 1000,
        baseline_route_v1: 'quiet_no_curve_trade_near_anchor',
      },
      {
        token_ca: 'A',
        anchor_ts: 2000,
        baseline_route_v1: 'quiet_no_curve_trade_near_anchor',
      },
    ],
    pass4Rows: [
      {
        token_ca: 'A',
        anchor_ts: 1000,
        status: 'ok',
        bars: [{}],
        baseline_progress_pct: 100,
        baseline_price_sol_chain: 4.1088e-7,
        baseline_trade_lag_sec: -100,
        exact_trade_event_n: 1,
      },
      {
        token_ca: 'A',
        anchor_ts: 2000,
        status: 'ok',
        bars: [],
        history_reached_start: false,
      },
    ],
  });

  assert.equal(report.summary.pass4_promoted_rows_n, 1);
  assert.equal(report.summary.pass4_still_missing_rows_n, 1);
  assert.equal(report.rows[0].baseline_route_v1, 'graduated_route_gmgn_amm');
  assert.equal(report.rows[0].baseline_route_previous, 'quiet_no_curve_trade_near_anchor');
  assert.equal(report.rows[0].baseline_route_source, 'true_pass4_last_pre_anchor');
  assert.equal(report.rows[1].baseline_route_v1, 'quiet_no_curve_trade_near_anchor');
  assert.equal(report.rows[1].pass4_note, 'true_pass4_found_no_baseline_trade');
});
