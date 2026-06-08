import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildRawDogDecisionFunnel,
  matchDecisionRecords,
} from '../src/analytics/raw-dog-decision-funnel.js';

const rawDog = (overrides = {}) => ({
  signal_id: overrides.signal_id ?? 1,
  token_ca: overrides.token_ca ?? 'DOG',
  symbol: overrides.symbol ?? 'DOG',
  signal_ts: overrides.signal_ts ?? 1000,
  lifecycle_id: overrides.lifecycle_id ?? 'life-1',
  raw_primary_tier: overrides.raw_primary_tier ?? 'gold',
  max_sustained_peak_pct: overrides.max_sustained_peak_pct ?? 120,
  time_to_sustained_peak_sec: overrides.time_to_sustained_peak_sec ?? 600,
  held_to_silver: overrides.held_to_silver ?? false,
  held_to_gold: overrides.held_to_gold ?? false,
  raw_dog_realized: overrides.raw_dog_realized ?? false,
  ...overrides,
});

const decision = (overrides = {}) => ({
  id: overrides.id ?? 10,
  source_kind: overrides.source_kind ?? 'opportunity_events',
  token_ca: overrides.token_ca ?? 'DOG',
  event_ts: overrides.event_ts ?? 1050,
  lifecycle_id: overrides.lifecycle_id ?? 'life-1',
  action: overrides.action ?? 'BLOCK',
  quote_available: overrides.quote_available ?? 1,
  quote_executable: overrides.quote_executable ?? 1,
  quote_clean: overrides.quote_clean ?? 0,
  route_available: overrides.route_available ?? 1,
  block_cause: overrides.block_cause ?? 'INFRA',
  ...overrides,
});

test('counts unmatched raw dogs as no_decision_record', () => {
  const funnel = buildRawDogDecisionFunnel({
    rawDogs: [rawDog()],
    decisionRecords: [],
  });

  assert.equal(funnel.summary.raw_sustained_dogs, 1);
  assert.equal(funnel.summary.no_decision_record, 1);
  assert.equal(funnel.summary.terminal_buckets.no_decision_record, 1);
  assert.equal(funnel.dogs[0].terminal_bucket, 'no_decision_record');
});

test('uses lifecycle_id match before token time-window fallback', () => {
  const match = matchDecisionRecords(rawDog(), [
    decision({ id: 1, lifecycle_id: 'other-life', event_ts: 1040, quote_clean: 1, action: 'WOULD_ENTER' }),
    decision({ id: 2, lifecycle_id: 'life-1', event_ts: 1050, quote_clean: 0, block_cause: 'POLICY' }),
  ]);

  assert.equal(match.matched_by, 'lifecycle_id');
  assert.equal(match.records.length, 1);
  assert.equal(match.records[0].id, 2);
});

test('falls back to token plus bounded decision-time window', () => {
  const dog = rawDog({ lifecycle_id: null, signal_ts: 1000, time_to_sustained_peak_sec: 180 });
  const funnel = buildRawDogDecisionFunnel({
    rawDogs: [dog],
    decisionRecords: [
      decision({ id: 1, lifecycle_id: null, event_ts: 990, quote_clean: 0, block_cause: 'INFRA' }),
      decision({ id: 2, lifecycle_id: null, event_ts: 1300, quote_clean: 1, action: 'WOULD_ENTER' }),
    ],
  });

  assert.equal(funnel.dogs[0].matched_by, 'token_time_window');
  assert.equal(funnel.dogs[0].decision_record_count, 1);
  assert.equal(funnel.dogs[0].best_decision_record.id, 1);
  assert.equal(funnel.summary.no_decision_record, 0);
  assert.equal(funnel.summary.block_cause.INFRA, 1);
});

test('separates quote-clean, would-enter, entered, and held buckets', () => {
  const dogs = [
    rawDog({ token_ca: 'A', lifecycle_id: 'life-a', raw_dog_realized: true, held_to_gold: true }),
    rawDog({ token_ca: 'B', lifecycle_id: 'life-b' }),
    rawDog({ token_ca: 'C', lifecycle_id: 'life-c' }),
  ];
  const records = [
    decision({ token_ca: 'A', lifecycle_id: 'life-a', event_ts: 1050, quote_clean: 1, action: 'ENTER', did_enter: 1 }),
    decision({ token_ca: 'B', lifecycle_id: 'life-b', event_ts: 1050, quote_clean: 1, action: 'WOULD_ENTER' }),
    decision({ token_ca: 'C', lifecycle_id: 'life-c', event_ts: 1050, quote_clean: 1, action: 'BLOCK' }),
  ];

  const funnel = buildRawDogDecisionFunnel({ rawDogs: dogs, decisionRecords: records });

  assert.equal(funnel.summary.quote_clean, 3);
  assert.equal(funnel.summary.would_enter, 2);
  assert.equal(funnel.summary.entered, 1);
  assert.equal(funnel.summary.held_to_silver_or_gold, 1);
  assert.equal(funnel.summary.terminal_buckets.held_to_silver_or_gold, 1);
  assert.equal(funnel.summary.terminal_buckets.would_enter_not_entered, 1);
  assert.equal(funnel.summary.terminal_buckets.quote_clean_no_would_enter, 1);
});

test('summarizes quote-clean no-would-enter gate reasons and ex-ante volume against duds', () => {
  const dogs = [
    rawDog({
      token_ca: 'DOG_HIGH_VOL',
      lifecycle_id: 'life-high',
      entry_bar_volume: 20_000,
      early_15m_volume: 80_000,
    }),
  ];
  const duds = [
    rawDog({
      token_ca: 'DUD_LOW_VOL',
      lifecycle_id: 'life-dud',
      raw_primary_tier: 'sub25',
      max_sustained_peak_pct: 10,
      entry_bar_volume: 1_000,
      early_15m_volume: 4_000,
    }),
  ];
  const records = [
    decision({
      token_ca: 'DOG_HIGH_VOL',
      lifecycle_id: 'life-high',
      quote_clean: 1,
      action: 'BLOCK',
      hard_blockers_json: JSON.stringify(['expected_rr_below_2']),
      expected_rr: 1.4,
      score: 76,
      source_component: 'matrix',
      source_reason: 'rr_guard',
    }),
    decision({
      token_ca: 'DUD_LOW_VOL',
      lifecycle_id: 'life-dud',
      quote_clean: 1,
      action: 'BLOCK',
      hard_blockers_json: JSON.stringify(['expected_rr_below_2']),
      expected_rr: 1.2,
      score: 65,
      source_component: 'matrix',
      source_reason: 'rr_guard',
    }),
  ];

  const funnel = buildRawDogDecisionFunnel({
    rawDogs: dogs,
    comparisonRows: duds,
    decisionRecords: records,
  });
  const analysis = funnel.summary.quote_clean_no_would_enter_analysis;

  assert.equal(analysis.raw_dogs_n, 1);
  assert.equal(analysis.comparison_duds_n, 1);
  assert.equal(analysis.gate_reason_counts.expected_rr_below_2, 1);
  assert.equal(analysis.source_reason_counts['matrix:rr_guard'], 1);
  assert.equal(analysis.ex_ante_volume.raw_dogs_quote_clean_no_would_enter.entry_bar_volume_q5_or_above_n, 1);
  assert.equal(analysis.ex_ante_volume.comparison_duds_quote_clean_no_would_enter.entry_bar_volume_q5_or_above_n, 0);
});
