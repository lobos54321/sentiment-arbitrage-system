import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildTradeReplay,
  inferLossCause,
  summarizeTradeReplays,
} from '../src/web/trade-replay-utils.js';

test('classifies no-follow losses as entry timing failures', () => {
  const trade = {
    id: 1,
    symbol: 'NOFOLLOW',
    signal_route: 'LOTTO',
    entry_ts: 100,
    exit_ts: 160,
    exit_reason: 'lotto_no_follow_60s (peak=0.0% < 5.0%)',
    pnl_pct: -0.08,
    peak_pnl: 0,
    position_size_sol: 0.003,
    entry_mode: 'lotto_micro_reclaim_tiny_probe',
  };

  const cause = inferLossCause(trade, [{ trade_id: 1, sample_ts: 130, mark_pnl: -0.02 }], []);

  assert.equal(cause.root_cause, 'entry_timing_no_follow');
  assert.ok(cause.tags.includes('timing_zero_peak_entry'));
  assert.ok(cause.tags.includes('timing_no_follow_exit'));
});

test('classifies peak-to-loss paths as exit capture failures', () => {
  const trade = {
    id: 2,
    symbol: 'GIVEBACK',
    signal_route: 'ATH',
    entry_ts: 100,
    exit_ts: 220,
    exit_reason: 'guardian_trail_stop',
    pnl_pct: -0.02,
    peak_pnl: 0.28,
    position_size_sol: 0.003,
    entry_mode: 'ath_matrix_dissonance_tiny_probe',
  };

  const cause = inferLossCause(trade, [{ trade_id: 2, sample_ts: 170, mark_pnl: 0.28, peak_pnl: 0.28 }], []);

  assert.equal(cause.root_cause, 'exit_capture');
  assert.ok(cause.tags.includes('exit_large_giveback'));
  assert.ok(cause.tags.includes('exit_positive_peak_to_loss'));
});

test('execution quote gaps outrank timing symptoms', () => {
  const trade = {
    id: 3,
    symbol: 'GAP',
    signal_route: 'LOTTO',
    entry_ts: 100,
    exit_ts: 130,
    exit_reason: 'lotto_sl (-19.0% <= -18.0%)',
    pnl_pct: -0.18,
    peak_pnl: 0,
    position_size_sol: 0.003,
    exit_execution_audit_json: JSON.stringify({ quoteMarkGapPct: -12.5 }),
  };

  const cause = inferLossCause(trade, [], []);

  assert.equal(cause.root_cause, 'execution_or_accounting_gap');
  assert.ok(cause.tags.includes('execution_exit_quote_mark_gap_8pp'));
});

test('builds sorted replay timeline and root-cause summary', () => {
  const trade = {
    id: 4,
    lifecycle_id: 'Token:100',
    token_ca: 'Token',
    symbol: 'REPLAY',
    signal_route: 'ATH',
    entry_ts: 100,
    exit_ts: 190,
    exit_reason: 'hard_sl (-15.0% <= -15.0%)',
    pnl_pct: -0.15,
    peak_pnl: 0.02,
    position_size_sol: 0.003,
  };
  const replay = buildTradeReplay(
    trade,
    [{ id: 7, trade_id: 4, sample_ts: 150, action: 'hold', mark_pnl: 0.02, peak_pnl: 0.02 }],
    [{ id: 8, event_ts: 90, component: 'entry_decision_contract', event_type: 'entry_audit', decision: 'LIVE', reason: 'pass' }],
  );
  const summary = summarizeTradeReplays([replay]);

  assert.deepEqual(replay.timeline.map((item) => item.type), [
    'entry_audit',
    'trade_entry',
    'path_sample',
    'trade_exit',
  ]);
  assert.equal(summary.trades, 1);
  assert.equal(summary.losses, 1);
  assert.equal(summary.by_root_cause.entry_timing_no_follow.n, 1);
});
