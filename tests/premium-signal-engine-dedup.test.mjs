import test from 'node:test';
import assert from 'node:assert/strict';

import { PremiumSignalEngine } from '../src/engines/premium-signal-engine.js';

function makeEngine() {
  const engine = Object.create(PremiumSignalEngine.prototype);
  engine.recentSignals = new Map();
  return engine;
}

test('ATH signal upgrades bypass recent NOT_ATH token dedupe', () => {
  const engine = makeEngine();
  engine.markProcessed('TokenCA', {
    signal_type: 'NEW_TRENDING',
    is_ath: false,
    market_cap: 30_000,
  });

  assert.equal(engine.isDuplicate('TokenCA', {
    signal_type: 'ATH',
    is_ath: true,
    market_cap: 90_000,
    gain_pct: 200,
  }), false);
});

test('same-level ATH dedupe allows materially stronger ATH update', () => {
  const engine = makeEngine();
  engine.markProcessed('TokenCA', {
    signal_type: 'ATH',
    is_ath: true,
    market_cap: 90_000,
    gain_pct: 120,
  });

  assert.equal(engine.isDuplicate('TokenCA', {
    signal_type: 'ATH',
    is_ath: true,
    market_cap: 130_000,
    gain_pct: 180,
  }), false);

  assert.equal(engine.isDuplicate('TokenCA', {
    signal_type: 'ATH',
    is_ath: true,
    market_cap: 131_000,
    gain_pct: 181,
  }), false);
});

test('same weak new-trending signal still dedupes inside five minutes', () => {
  const engine = makeEngine();
  engine.markProcessed('TokenCA', {
    signal_type: 'NEW_TRENDING',
    is_ath: false,
    market_cap: 30_000,
  });

  assert.equal(engine.isDuplicate('TokenCA', {
    signal_type: 'NEW_TRENDING',
    is_ath: false,
    market_cap: 31_000,
  }), true);
});
