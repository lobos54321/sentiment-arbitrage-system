import assert from 'node:assert/strict';
import { test } from 'node:test';
import Database from 'better-sqlite3';
import {
  buildClosedLoopMissedDogSummary,
  boundedIntParam,
  boundedWindowedSinceTs,
  resetPaperReportGateForTest,
  tryBeginPaperReport,
} from '../src/web/dashboard-server.js';

test('boundedIntParam clamps oversized live query parameters', () => {
  const url = new URL('https://example.test/api?event_limit=40000&limit=999');

  assert.equal(boundedIntParam(url, 'event_limit', 3000, 100, 8000), 8000);
  assert.equal(boundedIntParam(url, 'limit', 50, 1, 120), 120);
});

test('boundedWindowedSinceTs clamps hours for live heavy endpoints', () => {
  const url = new URL('https://example.test/api?hours=24');
  const since = boundedWindowedSinceTs(url, 1, 2, { nowSec: 10_000 });

  assert.equal(since, 10_000 - 2 * 3600);
});

test('paper report gate rejects concurrent and cooldown requests', () => {
  resetPaperReportGateForTest();
  const first = tryBeginPaperReport('/api/paper/lifecycle-summary', 1000);
  const concurrent = tryBeginPaperReport('/api/paper/trade-replay', 1001);

  assert.equal(first.allowed, true);
  assert.equal(concurrent.allowed, false);
  assert.equal(concurrent.reason, 'paper_report_busy');

  first.release(2000);
  const cooldown = tryBeginPaperReport('/api/paper/trade-replay', 2001);

  assert.equal(cooldown.allowed, false);
  assert.equal(cooldown.reason, 'paper_report_cooldown');
});

test('closed loop missed dog summary ranks one blocker per token in SQL', () => {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE paper_missed_signal_attribution (
      token_ca TEXT,
      symbol TEXT,
      signal_id INTEGER,
      signal_ts REAL,
      route TEXT,
      component TEXT,
      reject_reason TEXT,
      tradability_status TEXT,
      tradability_reason TEXT,
      tradable_peak_pnl REAL,
      tradable_missed INTEGER,
      would_stop_before_peak INTEGER,
      max_pnl_recorded REAL,
      pnl_24h REAL,
      pnl_60m REAL,
      pnl_15m REAL,
      pnl_5m REAL,
      created_event_ts REAL,
      baseline_ts REAL
    );
  `);
  const insert = db.prepare(`
    INSERT INTO paper_missed_signal_attribution (
      token_ca, symbol, signal_id, signal_ts, route, component, reject_reason,
      tradability_status, tradability_reason, tradable_peak_pnl, tradable_missed,
      would_stop_before_peak, max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m,
      pnl_5m, created_event_ts, baseline_ts
    ) VALUES (
      @token_ca, @symbol, @signal_id, @signal_ts, @route, @component, @reject_reason,
      @tradability_status, @tradability_reason, @tradable_peak_pnl, @tradable_missed,
      @would_stop_before_peak, @max_pnl_recorded, @pnl_24h, @pnl_60m, @pnl_15m,
      @pnl_5m, @created_event_ts, @baseline_ts
    )
  `);
  insert.run({
    token_ca: 'token-a',
    symbol: 'A',
    signal_id: 1,
    signal_ts: 1001,
    route: 'ATH',
    component: 'matrix_evaluator',
    reject_reason: 'weak_matrix',
    tradability_status: 'tradable_reclaim',
    tradability_reason: 'older',
    tradable_peak_pnl: 0.3,
    tradable_missed: 1,
    would_stop_before_peak: 0,
    max_pnl_recorded: 0.3,
    pnl_24h: null,
    pnl_60m: null,
    pnl_15m: null,
    pnl_5m: null,
    created_event_ts: 1001,
    baseline_ts: 1001,
  });
  insert.run({
    token_ca: 'token-a',
    symbol: 'A',
    signal_id: 2,
    signal_ts: 1002,
    route: 'ATH',
    component: 'source_resonance_probe',
    reject_reason: 'scout_quality_buy_pressure_weak',
    tradability_status: 'tradable_reclaim',
    tradability_reason: 'best',
    tradable_peak_pnl: 1.2,
    tradable_missed: 1,
    would_stop_before_peak: 0,
    max_pnl_recorded: 1.2,
    pnl_24h: null,
    pnl_60m: null,
    pnl_15m: null,
    pnl_5m: null,
    created_event_ts: 1002,
    baseline_ts: 1002,
  });
  insert.run({
    token_ca: 'token-b',
    symbol: 'B',
    signal_id: 3,
    signal_ts: 1003,
    route: 'NOT_ATH',
    component: 'matrix_evaluator',
    reject_reason: 'matrices not yet aligned',
    tradability_status: 'stop_before_peak',
    tradability_reason: 'stopped',
    tradable_peak_pnl: 0.7,
    tradable_missed: 1,
    would_stop_before_peak: 1,
    max_pnl_recorded: 0.7,
    pnl_24h: null,
    pnl_60m: null,
    pnl_15m: null,
    pnl_5m: null,
    created_event_ts: 1003,
    baseline_ts: 1003,
  });
  insert.run({
    token_ca: 'token-c',
    symbol: 'C',
    signal_id: 4,
    signal_ts: 1004,
    route: 'LOTTO',
    component: 'discovery_tracking',
    reject_reason: 'tracking_ttl_expired',
    tradability_status: 'tradable_reclaim',
    tradability_reason: 'small',
    tradable_peak_pnl: 0.2,
    tradable_missed: 1,
    would_stop_before_peak: 0,
    max_pnl_recorded: 0.2,
    pnl_24h: null,
    pnl_60m: null,
    pnl_15m: null,
    pnl_5m: null,
    created_event_ts: 1004,
    baseline_ts: 1004,
  });
  insert.run({
    token_ca: 'old-token',
    symbol: 'OLD',
    signal_id: 5,
    signal_ts: 900,
    route: 'ATH',
    component: 'matrix_evaluator',
    reject_reason: 'old',
    tradability_status: 'tradable_reclaim',
    tradability_reason: 'old',
    tradable_peak_pnl: 10,
    tradable_missed: 1,
    would_stop_before_peak: 0,
    max_pnl_recorded: 10,
    pnl_24h: null,
    pnl_60m: null,
    pnl_15m: null,
    pnl_5m: null,
    created_event_ts: 900,
    baseline_ts: 900,
  });

  const summary = buildClosedLoopMissedDogSummary(
    db,
    new Set(['paper_missed_signal_attribution']),
    1000,
    5,
    { includeDetails: true }
  );

  assert.equal(summary.available, true);
  assert.equal(summary.unique_tokens, 3);
  assert.equal(summary.quote_clean_unique, 2);
  assert.equal(summary.quote_clean_dog_unique, 1);
  assert.equal(summary.gold_unique, 1);
  assert.equal(summary.silver_unique, 1);
  assert.equal(summary.bronze_unique, 0);
  assert.equal(summary.top_missed_dogs.length, 2);
  assert.equal(summary.top_missed_dogs[0].token_ca, 'token-a');
  assert.equal(summary.top_missed_dogs[0].final_blocker_key, 'ATH:source_resonance_probe:scout_quality_buy_pressure_weak');
  assert.equal(summary.top_missed_dogs[0].entry_mode_candidate, 'source_resonance_tiny_probe');
  assert.equal(summary.top_missed_dogs[1].token_ca, 'token-b');
  assert.equal(summary.top_missed_dogs[1].quote_clean, false);
  assert.equal(summary.by_final_blocker[0].final_blocker_key, 'ATH:source_resonance_probe:scout_quality_buy_pressure_weak');
  assert.equal(summary.by_final_blocker[0].gold_unique, 1);
  db.close();
});
