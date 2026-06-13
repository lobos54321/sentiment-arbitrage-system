import test from 'node:test';
import assert from 'node:assert/strict';

import { rebuildCohort } from '../scripts/rebuild-chain-truth-rawdog-cohort.js';

function row(overrides = {}) {
  return {
    token_ca: 'TOKENpump',
    signal_ts: 1000,
    tier: 'unknown',
    effective_tier: 'unknown',
    label_status: 'quarantine',
    label_cleaning_reason: 'missing_baseline_price',
    ...overrides,
  };
}

function routed(overrides = {}) {
  return {
    token_ca: 'TOKENpump',
    anchor_ts: 1000,
    ...overrides,
  };
}

test('rebuilds graduated rows using GMGN full-window USD peak and baseline', () => {
  const report = rebuildCohort({
    labelRows: [row()],
    baselineRows: [
      routed({
        unit_domain: 'usd_gmgn',
        baseline_unit_route: 'graduated_gmgn_amm',
        baseline_price_usd_gmgn: 0.00001,
      }),
    ],
    gmgnRows: [
      {
        token_ca: 'TOKENpump',
        signal_ts: 1000,
        entry_0m_price: 0.00001,
        price_max: 0.00003,
        bars: 121,
      },
    ],
    peakRows: [],
  });

  assert.equal(report.summary.clean_rows_n, 1);
  assert.equal(report.rows[0].effective_tier, 'gold');
  assert.equal(report.rows[0].return_domain, 'usd_gmgn');
  assert.equal(report.rows[0].return_calculation_rule, 'usd_gmgn_peak_over_usd_gmgn_baseline');
  assert.equal(report.rows[0].return_peak_source, 'gmgn_full_window_usd_peak');
});

test('rebuilds curve rows from exact SOL-domain peak windows', () => {
  const report = rebuildCohort({
    labelRows: [row()],
    baselineRows: [
      routed({
        unit_domain: 'sol_curve',
        baseline_unit_route: 'curve_active_native',
        baseline_price_sol_curve: 1e-7,
      }),
    ],
    gmgnRows: [],
    peakRows: [
      {
        token_ca: 'TOKENpump',
        anchor_ts: 1000,
        bars: [
          { high: 1.4e-7, close: 1.2e-7 },
          { high: 2.1e-7, close: 2.0e-7 },
        ],
      },
    ],
  });

  assert.equal(report.rows[0].effective_tier, 'gold');
  assert.equal(report.rows[0].return_domain, 'sol_curve');
  assert.equal(report.rows[0].return_peak_source, 'chain_truth_peak_window_exact');
});

test('uses explicit graduation bridge for curve baseline to GMGN peak', () => {
  const report = rebuildCohort({
    labelRows: [row()],
    baselineRows: [
      routed({
        unit_domain: 'sol_curve',
        baseline_unit_route: 'near_graduation_native',
        baseline_price_sol_curve: 1e-7,
        graduation_price_sol_curve: 4e-7,
      }),
    ],
    gmgnRows: [
      {
        token_ca: 'TOKENpump',
        signal_ts: 1000,
        entry_0m_price: 0.00004,
        price_max: 0.00008,
      },
    ],
    peakRows: [],
  });

  assert.equal(report.rows[0].effective_tier, 'gold');
  assert.equal(report.rows[0].return_domain, 'spliced_curve_to_gmgn');
  assert.equal(
    report.rows[0].return_calculation_rule,
    'graduation_sol_over_baseline_sol_times_peak_usd_over_graduation_usd',
  );
});

test('keeps rows in quarantine when bridge or compatible peak is missing', () => {
  const report = rebuildCohort({
    labelRows: [row()],
    baselineRows: [
      routed({
        unit_domain: 'sol_curve',
        baseline_unit_route: 'near_graduation_native',
        baseline_price_sol_curve: 1e-7,
      }),
    ],
    gmgnRows: [
      {
        token_ca: 'TOKENpump',
        signal_ts: 1000,
        entry_0m_price: null,
        price_max: 0.00008,
      },
    ],
    peakRows: [],
  });

  assert.equal(report.rows[0].label_status, 'quarantine');
  assert.equal(report.rows[0].coverage_incomplete, true);
  assert.equal(report.rows[0].label_adjudication_status, 'quarantine_incomplete');
});

test('physical refutation invalidates impossible claims but does not invent a corrected peak', () => {
  const report = rebuildCohort({
    labelRows: [
      row({
        baseline_price: 2e-7,
        recorded_peak_multiple: 100,
        label_cleaning_reason: 'label_unit_corrupt',
      }),
    ],
    baselineRows: [],
    gmgnRows: [],
    peakRows: [],
  });

  assert.equal(report.rows[0].refuted_by_physics, true);
  assert.equal(report.rows[0].label_status, 'quarantine');
  assert.equal(report.rows[0].label_adjudication_status, 'quarantine_refuted_by_physics_needs_corrected_peak');
  assert.equal(report.summary.active_label_unit_suspect_n, 1);
});

test('does not accept physically impossible pumpfun native bar peaks as clean curve returns', () => {
  const report = rebuildCohort({
    labelRows: [
      row({
        token_ca: 'BADpump',
        label_status: 'clean',
        label_cleaning_reason: 'within_tolerance',
        effective_tier: 'gold',
        tier: 'gold',
        baseline_price: 2e-7,
        recorded_peak_multiple: 3,
        observed_max_price: 8e-7,
      }),
    ],
    baselineRows: [],
    gmgnRows: [],
    peakRows: [],
  });

  assert.equal(report.rows[0].label_status, 'quarantine');
  assert.equal(report.rows[0].refuted_by_physics, true);
  assert.equal(report.rows[0].observed_peak_physically_impossible, true);
  assert.equal(report.rows[0].label_adjudication_status, 'quarantine_refuted_by_physics_needs_corrected_peak');
});

test('keeps non-pump native peaks out of the pumpfun physical-limit guard', () => {
  const report = rebuildCohort({
    labelRows: [
      row({
        token_ca: 'OTHER',
        label_status: 'clean',
        label_cleaning_reason: 'within_tolerance',
        effective_tier: 'gold',
        tier: 'gold',
        baseline_price: 2e-7,
        recorded_peak_multiple: 3,
        observed_max_price: 8e-7,
      }),
    ],
    baselineRows: [],
    gmgnRows: [],
    peakRows: [],
  });

  assert.equal(report.rows[0].label_status, 'clean');
  assert.equal(report.rows[0].refuted_by_physics, false);
});

test('quarantines suspicious native-bar repairs without external truth', () => {
  const report = rebuildCohort({
    labelRows: [
      row({
        token_ca: 'OTHER',
        label_status: 'clean',
        label_cleaning_reason: 'missing_recorded_peak_repaired_from_native_bars',
        effective_tier: 'gold',
        tier: 'gold',
        baseline_price: 1e-10,
        observed_max_price: 1e-5,
      }),
    ],
    baselineRows: [],
    gmgnRows: [],
    peakRows: [],
  });

  assert.equal(report.rows[0].label_status, 'quarantine');
  assert.equal(report.rows[0].label_adjudication_reason, 'suspicious_native_return_needs_external_truth');
  assert.equal(report.summary.clean_dog_unique_n, 0);
  assert.equal(report.summary.quarantine_unique_n, 1);
});

test('quarantines suspicious native-bar returns even when the old label was within tolerance', () => {
  const report = rebuildCohort({
    labelRows: [
      row({
        token_ca: 'OTHER',
        label_status: 'clean',
        label_cleaning_reason: 'within_tolerance',
        effective_tier: 'gold',
        tier: 'gold',
        baseline_price: 1e-7,
        observed_max_price: 3e-6,
      }),
    ],
    baselineRows: [],
    gmgnRows: [],
    peakRows: [],
  });

  assert.equal(report.rows[0].label_status, 'quarantine');
  assert.equal(report.rows[0].label_adjudication_reason, 'suspicious_native_return_needs_external_truth');
});

test('rebuilds suspicious native-bar repairs from GMGN external truth when available', () => {
  const report = rebuildCohort({
    labelRows: [
      row({
        token_ca: 'OTHER',
        label_status: 'clean',
        label_cleaning_reason: 'missing_recorded_peak_repaired_from_native_bars',
        effective_tier: 'gold',
        tier: 'gold',
        baseline_price: 1e-10,
        observed_max_price: 1e-5,
      }),
    ],
    baselineRows: [],
    gmgnRows: [
      {
        token_ca: 'OTHER',
        signal_ts: 1000,
        entry_0m_price: 0.00001,
        price_max: 0.000018,
      },
    ],
    peakRows: [],
  });

  assert.equal(report.rows[0].label_status, 'clean');
  assert.equal(report.rows[0].return_domain, 'usd_gmgn');
  assert.equal(report.rows[0].return_baseline_unit_route, 'native_observed_high_return_gmgn_anchor');
  assert.equal(report.rows[0].effective_tier, 'silver');
});

test('original clean rows pass through when no chain truth correction is available', () => {
  const report = rebuildCohort({
    labelRows: [
      row({
        label_status: 'clean',
        label_cleaning_reason: 'within_tolerance',
        effective_tier: 'silver',
        max_sustained_peak_pct: 0.6,
        recorded_peak_multiple: 1.6,
      }),
    ],
    baselineRows: [],
    gmgnRows: [],
    peakRows: [],
  });

  assert.equal(report.rows[0].label_status, 'clean');
  assert.equal(report.rows[0].label_adjudication_status, 'clean_original_label');
  assert.equal(report.rows[0].effective_tier, 'silver');
  assert.equal(report.summary.clean_dog_unique_n, 1);
});
