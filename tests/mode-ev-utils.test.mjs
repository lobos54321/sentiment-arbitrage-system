import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildModeEvReport,
  isModeEvQuoteClean,
  modeEvQuoteGapPct,
} from '../src/web/mode-ev-utils.js';

test('quote-clean filter removes quote reprice and high quote gap rows', () => {
  const clean = {
    pnl_pct: 0.1,
    position_size_sol: 0.003,
    exit_execution_audit_json: JSON.stringify({ accountingSource: 'blended_total_sol_received_plus_final_sol', quoteMarkGapPct: 3 }),
  };
  const reprice = {
    pnl_pct: 1.2,
    position_size_sol: 0.003,
    exit_execution_audit_json: JSON.stringify({ accountingSource: 'quote_pnl_reprice(was=blended_total_sol_received_plus_final_sol)', quoteMarkGapPct: 2 }),
  };
  const highGap = {
    pnl_pct: 0.2,
    position_size_sol: 0.003,
    exit_execution_audit_json: JSON.stringify({ accountingSource: 'blended_total_sol_received_plus_final_sol', quoteMarkGapPct: 12 }),
  };

  assert.equal(isModeEvQuoteClean(clean), true);
  assert.equal(isModeEvQuoteClean(reprice), false);
  assert.equal(isModeEvQuoteClean(highGap), false);
  assert.equal(modeEvQuoteGapPct(highGap), 12);
});

test('mode EV report exposes robust outlier diagnostics', () => {
  const rows = [
    { id: 1, token_ca: 'A', entry_mode: 'lotto_not_ath_reclaim_tiny_probe', pnl_pct: 3.4, position_size_sol: 0.003, exit_ts: 10 },
    { id: 2, token_ca: 'B', entry_mode: 'lotto_not_ath_reclaim_tiny_probe', pnl_pct: -0.08, position_size_sol: 0.003, exit_ts: 11 },
    { id: 3, token_ca: 'C', entry_mode: 'lotto_not_ath_reclaim_tiny_probe', pnl_pct: -0.07, position_size_sol: 0.003, exit_ts: 12 },
  ];

  const report = buildModeEvReport(rows, { bootstrapIterations: 500 });
  const mode = report.by_entry_mode[0];

  assert.equal(report.input_rows, 3);
  assert.equal(mode.total, 3);
  assert.equal(mode.bucket, 'tiny_scout');
  assert.equal(mode.median_pnl_pct, -7);
  assert.ok(mode.max_single_trade_contribution_pct > 100);
  assert.equal(mode.pass_unit_economics, false);
});

test('quote-clean mode report evaluates only clean rows', () => {
  const rows = [
    {
      id: 1,
      token_ca: 'A',
      entry_mode: 'ath_micro_reclaim_tiny_probe',
      pnl_pct: -0.05,
      position_size_sol: 0.003,
      exit_ts: 10,
      exit_execution_audit_json: JSON.stringify({ quoteMarkGapPct: 2 }),
    },
    {
      id: 2,
      token_ca: 'B',
      entry_mode: 'ath_micro_reclaim_tiny_probe',
      pnl_pct: 0.5,
      position_size_sol: 0.003,
      exit_ts: 11,
      exit_execution_audit_json: JSON.stringify({ quoteMarkGapPct: 20 }),
    },
  ];

  const report = buildModeEvReport(rows, { clean: 'quote', bootstrapIterations: 500 });
  const mode = report.by_entry_mode[0];

  assert.equal(report.input_rows, 2);
  assert.equal(report.evaluated_rows, 1);
  assert.equal(mode.total, 1);
  assert.equal(mode.total_pnl_sol, -0.00015);
});

test('mode EV report filters revival canary rows by policy version', () => {
  const rows = [
    {
      id: 1,
      token_ca: 'A',
      entry_mode: 'ath_no_kline_tiny_probe',
      pnl_pct: 0.1,
      position_size_sol: 0.003,
      exit_ts: 10,
      monitor_state_json: JSON.stringify({
        revivalCanary: true,
        policyVersion: 'post_d162c067_quote_guard',
        quoteGuardVersion: 'd162c067',
      }),
    },
    {
      id: 2,
      token_ca: 'B',
      entry_mode: 'ath_no_kline_tiny_probe',
      pnl_pct: -0.2,
      position_size_sol: 0.003,
      exit_ts: 11,
      monitor_state_json: JSON.stringify({ policyVersion: 'pre_quote_guard' }),
    },
  ];

  const report = buildModeEvReport(rows, {
    revivalCanary: true,
    policyVersion: 'post_d162c067_quote_guard',
    bootstrapIterations: 500,
  });
  const mode = report.by_entry_mode[0];

  assert.equal(report.input_rows, 2);
  assert.equal(report.evaluated_rows, 1);
  assert.equal(report.revival_canary_rows, 1);
  assert.equal(report.policy_version_rows.post_d162c067_quote_guard, 1);
  assert.equal(report.policy_version_rows.pre_quote_guard, 1);
  assert.equal(mode.total, 1);
  assert.equal(mode.revival_canary_n, 1);
  assert.deepEqual(mode.policy_versions, ['post_d162c067_quote_guard']);
  assert.deepEqual(mode.quote_guard_versions, ['d162c067']);
});
