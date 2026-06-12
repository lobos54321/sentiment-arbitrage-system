import test from 'node:test';
import assert from 'node:assert/strict';

import {
  computeUnitAwareReturn,
  tierFromReturnPct,
} from '../scripts/baseline-return-utils.js';

function assertNear(actual, expected, epsilon = 1e-9) {
  assert.ok(Math.abs(actual - expected) <= epsilon, `${actual} != ${expected}`);
}

test('computes GMGN USD-domain returns only against GMGN USD peaks', () => {
  const result = computeUnitAwareReturn(
    {
      unit_domain: 'usd_gmgn',
      baseline_price_usd_gmgn: 0.00001,
      baseline_price_sol_chain_for_reference: 4e-7,
    },
    {
      peak_domain: 'usd_gmgn',
      peak_price_usd_gmgn: 0.00003,
    },
  );
  assert.equal(result.evaluable, true);
  assert.equal(result.return_domain, 'usd_gmgn');
  assertNear(result.return_pct, 2.0);
});

test('computes native curve-domain returns only against native curve peaks', () => {
  const result = computeUnitAwareReturn(
    {
      unit_domain: 'sol_curve',
      baseline_price_sol_curve: 1e-7,
    },
    {
      peak_domain: 'sol_curve',
      peak_price_sol_curve: 2e-7,
    },
  );
  assert.equal(result.evaluable, true);
  assert.equal(result.return_domain, 'sol_curve');
  assertNear(result.return_pct, 1.0);
});

test('uses explicit graduation bridge for cross-domain curve-to-GMGN returns', () => {
  const result = computeUnitAwareReturn(
    {
      unit_domain: 'sol_curve',
      baseline_price_sol_curve: 1e-7,
      graduation_price_sol_curve: 4e-7,
      graduation_price_usd_gmgn: 0.00004,
    },
    {
      peak_domain: 'usd_gmgn',
      peak_price_usd_gmgn: 0.00008,
    },
  );
  assert.equal(result.evaluable, true);
  assert.equal(result.return_domain, 'spliced_curve_to_gmgn');
  assertNear(result.curve_leg_ratio, 4.0);
  assertNear(result.gmgn_leg_ratio, 2.0);
  assertNear(result.return_pct, 7.0);
});

test('refuses to compute cross-domain returns without bridge prices', () => {
  const result = computeUnitAwareReturn(
    {
      unit_domain: 'sol_curve',
      baseline_price_sol_curve: 1e-7,
    },
    {
      peak_domain: 'usd_gmgn',
      peak_price_usd_gmgn: 0.00008,
    },
  );
  assert.equal(result.evaluable, false);
  assert.equal(result.reason, 'missing_splice_bridge_or_peak');
});

test('maps returns to raw tiers', () => {
  assert.equal(tierFromReturnPct(1.0), 'gold');
  assert.equal(tierFromReturnPct(0.5), 'silver');
  assert.equal(tierFromReturnPct(0.25), 'bronze');
  assert.equal(tierFromReturnPct(0.249), 'sub25');
  assert.equal(tierFromReturnPct(null), 'unknown');
});
