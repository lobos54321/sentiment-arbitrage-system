import assert from 'node:assert/strict';
import { test } from 'node:test';
import Database from 'better-sqlite3';
import {
  ensurePremiumSignalsSchema,
  normalizeRow,
  rowsDiffer,
} from '../scripts/sync-remote-premium-logs.js';

test('remote premium sync normalizes motion trace carrier fields', () => {
  const incoming = normalizeRow({
    id: 42,
    token_ca: 'SyncMotionToken11111111111111111111111111111',
    timestamp: 1783000000,
    is_ath: true,
    ath_num: 2,
    indices: {
      super_index: { current: 88 },
      trade_index: { signal: 3 },
    },
    total_supply: '1000000000000',
    decimals: '6',
  });

  assert.equal(incoming.remoteSignalId, 42);
  assert.equal(incoming.signalType, 'ATH');
  assert.equal(incoming.athStage, 'ATH2');
  assert.deepEqual(JSON.parse(incoming.indicesJson), {
    super_index: { current: 88 },
    trade_index: { signal: 3 },
  });
  assert.equal(incoming.tokenSupply, 1000000000000);
  assert.equal(incoming.tokenDecimals, 6);
});

test('remote premium sync schema carries P5 motion trace columns', () => {
  const db = new Database(':memory:');
  ensurePremiumSignalsSchema(db);

  const signalColumns = db.prepare('PRAGMA table_info(premium_signals)').all().map((row) => row.name);
  assert.ok(signalColumns.includes('indices_json'));
  assert.ok(signalColumns.includes('ath_stage'));
  assert.ok(signalColumns.includes('token_supply'));
  assert.ok(signalColumns.includes('token_decimals'));

  const motionColumns = db.prepare('PRAGMA table_info(token_motion_events)').all().map((row) => row.name);
  assert.ok(motionColumns.includes('mint'));
  assert.ok(motionColumns.includes('signal_id'));
  assert.ok(motionColumns.includes('ts_ms'));
  assert.ok(motionColumns.includes('domain'));
  assert.ok(motionColumns.includes('event_type'));
  db.close();
});

test('remote premium sync detects changed motion trace carrier fields', () => {
  const incoming = normalizeRow({
    id: 43,
    token_ca: 'SyncDiffToken1111111111111111111111111111111',
    timestamp: 1783000001,
    signal_type: 'NEW_TRENDING',
    signal_indices: { ai_index: { current: 71 } },
    token_supply: 5000,
    token_decimals: 9,
  });

  assert.equal(rowsDiffer({
    token_ca: incoming.tokenCa,
    timestamp: incoming.timestamp,
    signal_type: incoming.signalType,
    indices_json: null,
    ath_stage: incoming.athStage,
    token_supply: incoming.tokenSupply,
    token_decimals: incoming.tokenDecimals,
    remote_signal_id: incoming.remoteSignalId,
  }, incoming), true);

  assert.equal(rowsDiffer({
    token_ca: incoming.tokenCa,
    symbol: incoming.symbol,
    market_cap: incoming.marketCap,
    holders: incoming.holders,
    volume_24h: incoming.volume24h,
    top10_pct: incoming.top10Pct,
    age: incoming.age,
    description: incoming.description,
    raw_message: incoming.rawMessage,
    timestamp: incoming.timestamp,
    source_message_ts: incoming.sourceMessageTs,
    receive_ts: incoming.receiveTs,
    signal_type: incoming.signalType,
    is_ath: incoming.isAth,
    parse_status: incoming.parseStatus,
    parse_missing_fields: incoming.parseMissingFields,
    hard_gate_status: incoming.hardGateStatus,
    gate_result: incoming.gateResult,
    ai_action: incoming.aiAction,
    ai_confidence: incoming.aiConfidence,
    ai_narrative_tier: incoming.aiNarrativeTier,
    executed: incoming.executed,
    trade_result: incoming.tradeResult,
    downstream_trade_id: incoming.downstreamTradeId,
    downstream_lifecycle_id: incoming.downstreamLifecycleId,
    indices_json: incoming.indicesJson,
    ath_stage: incoming.athStage,
    token_supply: incoming.tokenSupply,
    token_decimals: incoming.tokenDecimals,
    remote_signal_id: incoming.remoteSignalId,
  }, incoming), false);
});
