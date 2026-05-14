import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildPremiumSignalOutcomeAudit,
  tierForPct,
} from '../src/web/premium-signal-outcome-audit-utils.js';

test('premium signal audit counts pass-to-max dogs outside missed attribution', () => {
  const signals = [
    {
      id: 1,
      token_ca: 'A',
      symbol: 'ALPHA',
      timestamp: 1_000_000,
      signal_type: 'ATH',
      market_cap: 50_000,
      hard_gate_status: 'PASS',
    },
    {
      id: 2,
      token_ca: 'A',
      symbol: 'ALPHA',
      timestamp: 1_060_000,
      signal_type: 'ATH',
      market_cap: 180_000,
      hard_gate_status: 'V18_MC_FILTER',
    },
    {
      id: 3,
      token_ca: 'B',
      symbol: 'BETA',
      timestamp: 1_010_000,
      signal_type: 'NEW_TRENDING',
      market_cap: 20_000,
      hard_gate_status: 'PASS',
    },
    {
      id: 4,
      token_ca: 'B',
      symbol: 'BETA',
      timestamp: 1_070_000,
      signal_type: 'NEW_TRENDING',
      market_cap: 27_000,
      hard_gate_status: 'PASS',
    },
  ];

  const audit = buildPremiumSignalOutcomeAudit({
    signals,
    paperTrades: [
      { id: 10, token_ca: 'B', entry_ts: 1010, exit_ts: 1070, entry_mode: 'source_resonance_tiny_probe', pnl_pct: 0.1 },
    ],
    missedAttributions: [
      {
        token_ca: 'A',
        n: 1,
        route: 'ATH',
        component: 'smart_entry',
        decision: 'reject',
        reject_reason: 'matrix_entry_not_ready',
        max_pnl: 1.6,
        tradable_missed: 1,
        would_stop_before_peak: 0,
        tradable_peak_pnl: 1.45,
        first_tradable_pnl: 0.2,
        source_resonance_cohort: 'telegram_gmgn',
        gmgn_pre_seen: 1,
      },
    ],
    sinceTs: 1000,
    generatedAt: '2026-05-13T00:00:00.000Z',
  });

  assert.equal(audit.summary.premium_signal_rows, 4);
  assert.equal(audit.summary.hard_gate_pass_unique, 2);
  assert.equal(audit.summary.pass_to_max_tiers.gold, 1);
  assert.equal(audit.summary.pass_to_max_tiers.bronze, 1);
  assert.equal(audit.summary.stream_dog_unique, 2);
  assert.equal(audit.summary.stream_dog_with_paper_trade_unique, 1);
  assert.equal(audit.summary.stream_dog_without_paper_trade_unique, 1);
  assert.equal(audit.summary.stream_dog_coverage_pct, 50);
  assert.equal(audit.summary.pass_dog_unique, 2);
  assert.equal(audit.summary.pass_dog_without_paper_trade_unique, 1);
  assert.equal(audit.summary.pass_dog_coverage_pct, 50);
  assert.equal(audit.summary.pass_dog_in_missed_attribution_unique, 1);
  assert.equal(audit.summary.coverage_classes.paper_trade, 1);
  assert.equal(audit.summary.coverage_classes.unclassified, 0);
  assert.equal(audit.summary.unclassified_unique, 0);
  assert.equal(audit.uncovered_pass_dogs[0].token_ca, 'A');
  assert.equal(audit.uncovered_pass_dogs[0].pass_to_max_pct, 260);
  assert.equal(audit.uncovered_pass_dogs[0].final_component, 'smart_entry');
  assert.equal(audit.uncovered_pass_dogs[0].final_reason, 'matrix_entry_not_ready');
  assert.equal(audit.uncovered_pass_dogs[0].quote_clean, true);
  assert.equal(audit.uncovered_pass_dogs[0].tradable_peak_pnl_pct, 145);
  assert.equal(audit.uncovered_pass_dogs[0].source_resonance_cohort, 'telegram_gmgn');
  assert.equal(audit.uncovered_pass_dogs[0].gmgn_pre_seen, true);
  assert.equal(audit.uncovered_stream_dogs[0].token_ca, 'A');
  assert.equal(audit.cohort_scoreboard.find((row) => row.cohort === 'telegram_gmgn').gold, 1);
  assert.equal(audit.cohort_scoreboard.find((row) => row.cohort === 'unknown').paper_filled_unique, 1);
  assert.equal(audit.unclassified_tokens.length, 0);
});

test('premium signal audit reads cohort from paper trade monitor state', () => {
  const audit = buildPremiumSignalOutcomeAudit({
    signals: [
      {
        id: 1,
        token_ca: 'C',
        symbol: 'COHORT',
        timestamp: 1_000_000,
        signal_type: 'ATH',
        market_cap: 50_000,
        hard_gate_status: 'PASS',
      },
      {
        id: 2,
        token_ca: 'C',
        symbol: 'COHORT',
        timestamp: 1_060_000,
        signal_type: 'ATH',
        market_cap: 80_000,
        hard_gate_status: 'PASS',
      },
    ],
    paperTrades: [
      {
        id: 10,
        token_ca: 'C',
        entry_ts: 1000,
        exit_ts: 1060,
        entry_mode: 'hard_gate_pass_tiny_probe',
        pnl_pct: 0.12,
        peak_pnl: 0.32,
        monitor_state_json: JSON.stringify({
          resonanceCohort: 'telegram_gmgn_quote_clean',
          gmgnPreSeen: true,
          gmgnLeadTimeSec: 180,
        }),
      },
    ],
    missedAttributions: [],
    sinceTs: 1000,
  });

  const cohort = audit.cohort_scoreboard.find((row) => row.cohort === 'telegram_gmgn_quote_clean');
  assert.equal(audit.top_pass_movers[0].source_resonance_cohort, 'telegram_gmgn_quote_clean');
  assert.equal(cohort.paper_filled_unique, 1);
  assert.equal(cohort.realized_win_rate, 1);
  assert.equal(cohort.avg_realized_pnl_pct, 12);
  assert.equal(cohort.avg_peak_pnl_pct, 32);
});

test('premium signal audit keeps hard-gate pass gaps classified and visible', () => {
  const audit = buildPremiumSignalOutcomeAudit({
    signals: [
      {
        id: 1,
        token_ca: 'GAP',
        symbol: 'GAP',
        timestamp: 1_000_000,
        signal_type: 'ATH',
        market_cap: 50_000,
        hard_gate_status: 'PASS',
      },
      {
        id: 2,
        token_ca: 'GAP',
        symbol: 'GAP',
        timestamp: 1_060_000,
        signal_type: 'ATH',
        market_cap: 100_000,
        hard_gate_status: 'PASS',
      },
    ],
    paperTrades: [],
    missedAttributions: [],
    sinceTs: 1000,
  });

  assert.equal(audit.summary.coverage_classes.unclassified, 0);
  assert.equal(audit.summary.coverage_gap_unique, 1);
  assert.equal(audit.coverage_gap_tokens[0].token_ca, 'GAP');
  assert.equal(audit.coverage_gap_tokens[0].coverage_class, 'observe_only');
  assert.equal(audit.coverage_gap_tokens[0].coverage_reason, 'hard_gate_pass_without_paper_trade_or_missed_attribution');
  assert.deepEqual(audit.coverage_gap_tokens[0].final_blocker, {
    component: 'coverage_audit',
    reason: 'hard_gate_pass_without_paper_trade_or_missed_attribution',
    route: 'PASS',
    decision: 'observe_only',
  });
});

test('premium signal audit classifies observe-only and safety rejects', () => {
  const audit = buildPremiumSignalOutcomeAudit({
    signals: [
      {
        id: 1,
        token_ca: 'OBS',
        symbol: 'OBS',
        timestamp: 1_000_000,
        signal_type: 'NEW_TRENDING',
        market_cap: 50_000,
        hard_gate_status: 'NOT_ATH_PREBUY_KLINE_BLOCK',
      },
      {
        id: 2,
        token_ca: 'SAFE',
        symbol: 'SAFE',
        timestamp: 1_000_000,
        signal_type: 'NEW_TRENDING',
        market_cap: 50_000,
        hard_gate_status: 'GREYLIST',
      },
    ],
    paperTrades: [],
    missedAttributions: [],
    sinceTs: 1000,
  });

  assert.equal(audit.summary.coverage_classes.observe_only, 1);
  assert.equal(audit.summary.coverage_classes.safety_reject, 1);
  assert.equal(audit.summary.coverage_classes.unclassified, 0);
});

test('premium signal audit tiers percentage gains', () => {
  assert.equal(tierForPct(120), 'gold');
  assert.equal(tierForPct(70), 'silver');
  assert.equal(tierForPct(30), 'bronze');
  assert.equal(tierForPct(5), 'sub25');
  assert.equal(tierForPct(null), 'unknown');
});
