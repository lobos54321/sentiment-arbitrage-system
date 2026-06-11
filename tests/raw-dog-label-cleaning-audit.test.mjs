import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildReport,
  classifyLabelRow,
} from '../scripts/run-raw-dog-label-cleaning-audit.js';

test('classifies recorded peak labels that exceed stored bar path by threshold', () => {
  const row = classifyLabelRow({
    token_ca: 'DOGpump',
    signal_ts: 1000,
    tier: 'gold',
    baseline_price: 1,
    max_sustained_peak_pct: 12_700,
    observed_max_price: 3,
  }, { threshold: 2 });

  assert.equal(row.label_status, 'quarantine');
  assert.equal(row.label_cleaning_reason, 'label_unit_corrupt');
  assert.equal(Math.round(row.recorded_peak_multiple), 128);
  assert.equal(row.observed_peak_multiple, 3);
  assert.equal(row.label_to_path_ratio > 40, true);
});

test('buildReport summarizes clean and quarantine rows', () => {
  const report = buildReport([
    {
      token_ca: 'CLEAN',
      signal_ts: 1,
      tier: 'silver',
      baseline_price: 1,
      max_sustained_peak_pct: 0.7,
      observed_max_price: 1.8,
    },
    {
      token_ca: 'NOBARS',
      signal_ts: 2,
      tier: 'gold',
      baseline_price: 1,
      max_sustained_peak_pct: 1.5,
      observed_max_price: null,
    },
  ]);

  assert.equal(report.summary.clean_rows_n, 1);
  assert.equal(report.summary.quarantine_rows_n, 1);
  assert.equal(report.summary.by_reason.no_native_bars, 1);
});

