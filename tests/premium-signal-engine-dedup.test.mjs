import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';

import {
  PremiumSignalEngine,
  deriveSignalAthStage,
  normalizeSignalTimestampMs,
} from '../src/engines/premium-signal-engine.js';

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

test('saveSignalRecord persists motion trace fields and append-only events', () => {
  const db = new Database(':memory:');
  const engine = Object.create(PremiumSignalEngine.prototype);
  engine.db = db;
  engine.initDB();

  const signal = {
    token_ca: 'MotionToken111111111111111111111111111111111',
    symbol: 'MOVE',
    market_cap: 42000,
    holders: 77,
    top10_pct: 18.5,
    volume_24h: 123456,
    timestamp: 1783000000,
    receive_ts: 1783000000123,
    signal_type: 'ATH',
    is_ath: true,
    signal_source: 'unit_test',
    source_event_id: 'unit:event:1',
    indices: {
      super_index: { current: 91, signal: 82 },
      trade_index: { current: 4, signal: 2 },
    },
    ath_stage: 'ATH2',
    token_supply: 1000000000000,
    token_decimals: 6,
  };

  const id = engine.saveSignalRecord(signal, 'PASS', { action: 'BUY_FULL', confidence: 81, ath_num: 2 }, false);
  assert.ok(id > 0);

  const row = db.prepare('SELECT * FROM premium_signals WHERE id = ?').get(id);
  assert.equal(row.ath_stage, 'ATH2');
  assert.equal(row.token_supply, 1000000000000);
  assert.equal(row.token_decimals, 6);
  assert.deepEqual(JSON.parse(row.indices_json), signal.indices);

  const token = db.prepare('SELECT token_supply, token_decimals FROM tokens WHERE token_ca = ?').get(signal.token_ca);
  assert.equal(token.token_supply, 1000000000000);
  assert.equal(token.token_decimals, 6);

  const events = db.prepare('SELECT domain, event_type, payload_json FROM token_motion_events ORDER BY domain, event_type').all();
  assert.ok(events.some((event) => event.domain === 'perceive' && event.event_type === 'signal_received'));
  assert.ok(events.some((event) => event.domain === 'decide' && event.event_type === 'gate_evaluation'));
  const perceive = events.find((event) => event.domain === 'perceive');
  assert.equal(JSON.parse(perceive.payload_json).ath_stage, 'ATH2');
});

test('token identity hydrate backfills missing supply and decimals without changing decision fields', async () => {
  const db = new Database(':memory:');
  const engine = Object.create(PremiumSignalEngine.prototype);
  engine.db = db;
  engine._tokenIdentityHydrateEnabled = true;
  engine._tokenIdentityHydrateInFlight = new Set();
  engine.solService = {
    async getMintSupplyDecimals(mint) {
      assert.equal(mint, 'HydrateToken1111111111111111111111111111111');
      return { supply: 123456789, decimals: 9, source: 'unit_test_mint_account' };
    },
  };
  engine.initDB();

  const signal = {
    token_ca: 'HydrateToken1111111111111111111111111111111',
    symbol: 'HYDR',
    market_cap: 51000,
    timestamp: 1783000000,
    receive_ts: 1783000000123,
    signal_type: 'NEW_TRENDING',
    is_ath: false,
    signal_source: 'unit_test',
    source_event_id: 'unit:event:hydrate',
  };

  const id = engine.saveSignalRecord(signal, 'PASS', null, false);
  assert.ok(id > 0);
  assert.equal(db.prepare('SELECT token_supply FROM premium_signals WHERE id = ?').get(id).token_supply, null);

  assert.equal(await engine._hydrateTokenIdentity(signal, id), true);

  const row = db.prepare('SELECT hard_gate_status, token_supply, token_decimals FROM premium_signals WHERE id = ?').get(id);
  assert.equal(row.hard_gate_status, 'PASS');
  assert.equal(row.token_supply, 123456789);
  assert.equal(row.token_decimals, 9);

  const token = db.prepare('SELECT token_supply, token_decimals FROM tokens WHERE token_ca = ?').get(signal.token_ca);
  assert.equal(token.token_supply, 123456789);
  assert.equal(token.token_decimals, 9);

  const hydratedEvent = db.prepare(`
    SELECT payload_json FROM token_motion_events
    WHERE domain = 'context' AND event_type = 'token_identity_hydrated'
    LIMIT 1
  `).get();
  assert.ok(hydratedEvent);
  const payload = JSON.parse(hydratedEvent.payload_json);
  assert.equal(payload.source, 'unit_test_mint_account');
  assert.equal(payload.token_supply, 123456789);
  assert.equal(payload.token_decimals, 9);
});

test('motion trace timestamp and ath stage helpers normalize inputs', () => {
  assert.equal(normalizeSignalTimestampMs(1783000000), 1783000000000);
  assert.equal(normalizeSignalTimestampMs(1783000000123), 1783000000123);
  assert.equal(deriveSignalAthStage({ is_ath: true, ath_num: 3 }), 'ATH3');
  assert.equal(deriveSignalAthStage({ is_ath: false }), 'NOT_ATH');
});
