import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildFollowup,
  classifyQuarantineRow,
} from '../scripts/build-cohort-quarantine-followup-worklists.js';

test('classifies physically refuted rows with no external truth as legal peak work', () => {
  const cls = classifyQuarantineRow({
    token_ca: 'abc123pump',
    label_adjudication_status: 'quarantine_refuted_by_physics_needs_corrected_peak',
    label_adjudication_reason: 'missing_compatible_peak_or_bridge',
    chain_truth_route_present: false,
    gmgn_full_window_present: false,
    peak_window_present: false,
  });

  assert.equal(cls.bucket, 'needs_legal_peak_no_external_truth');
  assert.equal(cls.action, 'gmgn_full_window_or_curve_peak_adjudication');
  assert.equal(cls.priority, 1);
  assert.equal(cls.venue, 'pumpfun');
});

test('routes missing baseline history gaps separately from venue gaps', () => {
  assert.equal(classifyQuarantineRow({
    token_ca: 'abc123pump',
    label_adjudication_reason: 'missing_baseline',
    route_unit_domain: 'history_incomplete',
    refuted_by_physics: true,
  }).bucket, 'history_incomplete_baseline_and_peak');

  assert.equal(classifyQuarantineRow({
    token_ca: 'nonpump',
    label_adjudication_reason: 'missing_baseline',
    route_unit_domain: 'venue_other',
  }).bucket, 'venue_other_missing_baseline');
});

test('buildFollowup preserves repeated signals and emits action counts', () => {
  const report = buildFollowup([
    {
      token_ca: 'repeatpump',
      signal_ts: 1000,
      label_adjudication_status: 'quarantine_refuted_by_physics_needs_corrected_peak',
      label_adjudication_reason: 'missing_compatible_peak_or_bridge',
      chain_truth_route_present: false,
      gmgn_full_window_present: false,
      peak_window_present: false,
    },
    {
      token_ca: 'repeatpump',
      signal_ts: 1100,
      label_adjudication_status: 'quarantine_refuted_by_physics_needs_corrected_peak',
      label_adjudication_reason: 'missing_compatible_peak_or_bridge',
      chain_truth_route_present: false,
      gmgn_full_window_present: false,
      peak_window_present: false,
    },
    {
      token_ca: 'other',
      signal_ts: 1200,
      label_adjudication_reason: 'missing_baseline',
      route_unit_domain: 'venue_other',
    },
  ]);

  assert.equal(report.summary.input_rows_n, 3);
  assert.equal(report.summary.worklist_rows_n, 3);
  assert.equal(report.summary.by_action.gmgn_full_window_or_curve_peak_adjudication, 2);
  assert.equal(report.summary.by_action.venue_baseline_decoder_required, 1);
  assert.deepEqual(report.worklistRows.map((row) => row.signal_ts), [1000, 1100, 1200]);
});
