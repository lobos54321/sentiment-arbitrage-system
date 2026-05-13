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
    missedAttributions: [],
    sinceTs: 1000,
    generatedAt: '2026-05-13T00:00:00.000Z',
  });

  assert.equal(audit.summary.premium_signal_rows, 4);
  assert.equal(audit.summary.hard_gate_pass_unique, 2);
  assert.equal(audit.summary.pass_to_max_tiers.gold, 1);
  assert.equal(audit.summary.pass_to_max_tiers.bronze, 1);
  assert.equal(audit.summary.pass_dog_unique, 2);
  assert.equal(audit.summary.pass_dog_without_paper_trade_unique, 1);
  assert.equal(audit.summary.pass_dog_in_missed_attribution_unique, 0);
  assert.equal(audit.summary.coverage_classes.paper_trade, 1);
  assert.equal(audit.summary.coverage_classes.unclassified, 1);
  assert.equal(audit.summary.unclassified_unique, 1);
  assert.equal(audit.uncovered_pass_dogs[0].token_ca, 'A');
  assert.equal(audit.uncovered_pass_dogs[0].pass_to_max_pct, 260);
  assert.equal(audit.unclassified_tokens[0].token_ca, 'A');
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
        hard_gate_status: 'NOT_ATH_V17',
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
