import assert from 'node:assert/strict';
import test from 'node:test';

import {
  baselineConfidence,
  buildRawSignalOutcomeReport,
  computeOutcomeForSignal,
} from '../src/analytics/raw-signal-outcomes.js';

const signal = (overrides = {}) => ({
  id: overrides.id ?? 1,
  token_ca: overrides.token_ca ?? 'DOG',
  symbol: overrides.symbol ?? 'DOG',
  timestamp_sec: overrides.signal_ts ?? 1000,
  signal_type: overrides.signal_type ?? 'ATH',
  hard_gate_status: overrides.hard_gate_status ?? 'PASS',
  ...overrides,
});

const kline = (timestamp, close, overrides = {}) => ({
  token_ca: overrides.token_ca ?? 'DOG',
  timestamp,
  open: overrides.open ?? close,
  high: overrides.high ?? close,
  low: overrides.low ?? close,
  close,
  volume: overrides.volume ?? 100,
  pool_address: overrides.pool_address ?? 'pool-1',
  source: overrides.source ?? 'kline-test',
  price_unit: overrides.price_unit ?? 'native',
});

test('baseline confidence contract gates main denominator', () => {
  assert.equal(baselineConfidence(5), 'high');
  assert.equal(baselineConfidence(30), 'medium');
  assert.equal(baselineConfidence(31), 'low');
  assert.equal(baselineConfidence(301), 'not_evaluable');

  const report = buildRawSignalOutcomeReport({
    nowTs: 10_000,
    horizonSec: 7200,
    signals: [signal()],
    klineRows: [
      kline(1040, 1.0),
      kline(1100, 2.0, { high: 2.0, close: 1.8 }),
      kline(1160, 1.8, { high: 1.9, close: 1.7 }),
    ],
  });

  assert.equal(report.outcomes[0].baseline_confidence, 'low');
  assert.equal(report.summary.raw_denominator_matured_only, 0);
  assert.equal(report.summary.raw_sustained_gold_silver_unique, 0);
  assert.equal(report.summary.denominator_status, 'evidence_unavailable');
});

test('right-censored signals are pending and excluded from denominators', () => {
  const report = buildRawSignalOutcomeReport({
    nowTs: 1500,
    horizonSec: 7200,
    signals: [signal()],
    klineRows: [kline(1000, 1.0), kline(1060, 2.0, { high: 2.0, close: 1.8 })],
  });

  assert.equal(report.summary.total_signals, 1);
  assert.equal(report.summary.pending_signals, 1);
  assert.equal(report.summary.right_censored_open, 1);
  assert.equal(report.summary.raw_denominator_matured_only, 0);
  assert.equal(report.summary.raw_dog_entered_rate, null);
  assert.equal(report.outcomes[0].coverage_reason, 'right_censored_open');
});

test('baseline and path must stay on the same source and unit', () => {
  const outcome = computeOutcomeForSignal(signal(), {
    nowTs: 10_000,
    horizonSec: 7200,
    klineRows: [
      kline(1000, 1.0, { source: 'provider-a' }),
      kline(1060, 2.0, { source: 'provider-b', high: 2.0, close: 1.8 }),
      kline(1120, 1.8, { source: 'provider-b', high: 1.9, close: 1.7 }),
    ],
  });

  assert.equal(outcome.same_source_path, false);
  assert.equal(outcome.coverage_reason, 'cross_source_path');
  assert.equal(outcome.raw_primary_tier, 'not_evaluable');
});

test('wick-only gold is visible but not counted as sustained raw dog', () => {
  const report = buildRawSignalOutcomeReport({
    nowTs: 10_000,
    horizonSec: 7200,
    signals: [signal()],
    klineRows: [
      kline(1000, 1.0),
      kline(1060, 1.05, { high: 2.2, close: 1.05, volume: 100 }),
    ],
  });

  assert.equal(report.summary.raw_wick_gold_silver_unique, 1);
  assert.equal(report.summary.raw_wick_only_gold_silver_unique, 1);
  assert.equal(report.summary.raw_sustained_gold_silver_unique, 0);
  assert.equal(report.coverage.sustained_evaluable_breakdown.not_evaluable, 1);
  assert.equal(report.outcomes[0].sustained_reason, 'insufficient_bars_after_peak');
});

test('entered is not capture unless the trade held to silver or gold', () => {
  const report = buildRawSignalOutcomeReport({
    nowTs: 10_000,
    horizonSec: 7200,
    signals: [signal()],
    klineRows: [
      kline(1000, 1.0),
      kline(1060, 1.75, { high: 1.8, close: 1.75, volume: 500 }),
      kline(1120, 1.65, { high: 1.75, close: 1.65, volume: 450 }),
    ],
    paperTrades: [
      { id: 7, token_ca: 'DOG', entry_ts: 1010, exit_ts: 1100, peak_pnl: 0.2, exit_reason: 'profit_protect_floor' },
    ],
  });

  assert.equal(report.summary.raw_sustained_gold_silver_unique, 1);
  assert.equal(report.summary.raw_gold_silver_entered, 1);
  assert.equal(report.summary.raw_gold_silver_realized, 0);
  assert.equal(report.summary.raw_dog_entered_rate, 1);
  assert.equal(report.summary.raw_dog_realized_rate, 0);
  assert.equal(report.outcomes[0].sold_before_silver, true);
});

test('held-to-silver counts as realized raw dog capture', () => {
  const report = buildRawSignalOutcomeReport({
    nowTs: 10_000,
    horizonSec: 7200,
    signals: [signal()],
    klineRows: [
      kline(1000, 1.0),
      kline(1060, 1.75, { high: 1.8, close: 1.75, volume: 500 }),
      kline(1120, 1.65, { high: 1.75, close: 1.65, volume: 450 }),
    ],
    paperTrades: [
      { id: 8, token_ca: 'DOG', entry_ts: 1010, exit_ts: 1200, peak_pnl: 0.6, exit_reason: 'runner_floor' },
    ],
  });

  assert.equal(report.summary.raw_sustained_gold_silver_unique, 1);
  assert.equal(report.summary.raw_gold_silver_entered, 1);
  assert.equal(report.summary.raw_gold_silver_realized, 1);
  assert.equal(report.summary.raw_dog_realized_rate, 1);
  assert.equal(report.outcomes[0].held_to_silver, true);
});

test('duplicate premium signal rows for the same token do not inflate dog counts', () => {
  const report = buildRawSignalOutcomeReport({
    nowTs: 10_000,
    horizonSec: 7200,
    signals: [
      signal({ id: 1, token_ca: 'DUP', signal_ts: 1000 }),
      signal({ id: 2, token_ca: 'DUP', signal_ts: 1000 }),
      signal({ id: 3, token_ca: 'DUP', signal_ts: 1000 }),
    ],
    klineRows: [
      kline(1000, 1.0, { token_ca: 'DUP' }),
      kline(1060, 1.75, { token_ca: 'DUP', high: 1.8, close: 1.75, volume: 500 }),
      kline(1120, 1.65, { token_ca: 'DUP', high: 1.75, close: 1.65, volume: 450 }),
    ],
    paperTrades: [
      { id: 9, token_ca: 'DUP', entry_ts: 1010, exit_ts: 1200, peak_pnl: 0.6, exit_reason: 'runner_floor' },
    ],
  });

  assert.equal(report.summary.raw_denominator_event_rows, 3);
  assert.equal(report.summary.raw_denominator_matured_only, 1);
  assert.equal(report.summary.raw_sustained_gold_silver_event_rows, 3);
  assert.equal(report.summary.raw_sustained_gold_silver_unique, 1);
  assert.equal(report.summary.raw_gold_silver_entered, 1);
  assert.equal(report.summary.raw_gold_silver_realized, 1);
  assert.equal(report.top_raw_dogs.length, 1);
  assert.equal(report.top_raw_dogs[0].token_ca, 'DUP');
});
