import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import Database from 'better-sqlite3';
import {
  buildDogCatchGoalProgress,
  buildStorageHealthSnapshot,
  buildClosedLoopProbeSummary,
  buildClosedLoopMissedDogSummary,
  boundedIntParam,
  boundedWindowedSinceTs,
  dogCatchGoalFromLiveSnapshot,
  missedRecoverySummaryFromLiveSnapshot,
  resetPaperReportGateForTest,
  shouldUseMaterializedMissedRecoverySummary,
  tryBeginPaperReport,
} from '../src/web/dashboard-server.js';

test('storage health reports db markers and disk snapshot without opening sqlite', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'storage-health-'));
  const paper = join(dir, 'paper_trades.db');
  fs.writeFileSync(paper, 'sqlite-placeholder');
  fs.writeFileSync(`${paper}.integrity_error`, 'malformed page');
  fs.writeFileSync(join(dir, 'preflight.log'), '[preflight] checkpoint failed');

  const snapshot = buildStorageHealthSnapshot({
    projectRoot: dir,
    dataDir: dir,
    includeFileStats: true,
    includePreflightTail: true,
    paperDbPath: paper,
    signalDbPath: join(dir, 'sentiment_arb.db'),
    klineDbPath: join(dir, 'kline_cache.db'),
    lifecycleDbPath: join(dir, 'lifecycle_tracks.db'),
  });

  assert.equal(snapshot.db_files.find((row) => row.label === 'paper_trades').exists, true);
  assert.match(snapshot.integrity_error, /malformed page/);
  assert.match(snapshot.preflight_tail, /checkpoint failed/);
});

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

test('boundedWindowedSinceTs supports explicit 24h review windows', () => {
  const url = new URL('https://example.test/api?hours=24');
  const since = boundedWindowedSinceTs(url, 2, 24, { nowSec: 100_000 });

  assert.equal(since, 100_000 - 24 * 3600);
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

test('missed recovery summary uses materialized snapshots for 2h default window', () => {
  assert.equal(shouldUseMaterializedMissedRecoverySummary(2, false), true);
  assert.equal(shouldUseMaterializedMissedRecoverySummary(8, false), true);
  assert.equal(shouldUseMaterializedMissedRecoverySummary(2, true), false);
  assert.equal(shouldUseMaterializedMissedRecoverySummary(1, false), false);
});

test('materialized missed recovery summary excludes stop-before-peak rows from clean dogs', () => {
  const summary = missedRecoverySummaryFromLiveSnapshot({
    snapshot_id: 'paper_live_2h_test',
    generated_at: '2026-05-21T00:00:00Z',
    window: { since_ts: 100, since_iso: '2026-05-21T00:00:00Z' },
    missed: {
      overall: {
        unique_tokens: 2,
        gold_unique: 1,
        quote_executable_unique: 2,
      },
      by_gate: [],
      top_dogs: [
        {
          token_ca: 'StopFirst',
          symbol: 'STOP',
          quote_exec: 1,
          tradable_missed: 1,
          would_stop_before_peak: 1,
          max_pnl: 10,
        },
        {
          token_ca: 'CleanDog',
          symbol: 'CLEAN',
          quote_exec: 1,
          tradable_missed: 1,
          would_stop_before_peak: 0,
          max_pnl: 2,
        },
      ],
    },
  }, { dbPath: '/tmp/paper.db', requestedHours: 2, limit: 10 });

  assert.deepEqual(
    summary.top_clean_quote_dogs.map((row) => row.token_ca),
    ['CleanDog']
  );
});

test('dog catch goal progress uses peak wins and clean missed dogs', () => {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE paper_trades (
      token_ca TEXT,
      entry_ts REAL,
      exit_ts REAL,
      pnl_pct REAL,
      trusted_peak_pnl REAL,
      position_size_sol REAL
    );
    CREATE TABLE paper_missed_signal_attribution (
      token_ca TEXT,
      signal_ts REAL,
      created_event_ts REAL,
      baseline_ts REAL,
      tradable_missed INTEGER,
      would_stop_before_peak INTEGER,
      executable_peak_pnl REAL
    );
  `);
  db.prepare(`
    INSERT INTO paper_trades (token_ca, entry_ts, exit_ts, pnl_pct, trusted_peak_pnl, position_size_sol)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run('caught-dog', 1001, 1010, 1.0, 0.7, 0.002);
  db.prepare(`
    INSERT INTO paper_trades (token_ca, entry_ts, exit_ts, pnl_pct, trusted_peak_pnl, position_size_sol)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run('small-loser', 1002, 1011, -0.1, 0.1, 0.002);
  db.prepare(`
    INSERT INTO paper_missed_signal_attribution (
      token_ca, signal_ts, created_event_ts, baseline_ts, tradable_missed,
      would_stop_before_peak, executable_peak_pnl
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('missed-dog', 1003, 1003, 1003, 1, 0, 0.6);
  db.prepare(`
    INSERT INTO paper_missed_signal_attribution (
      token_ca, signal_ts, created_event_ts, baseline_ts, tradable_missed,
      would_stop_before_peak, executable_peak_pnl
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('stop-first', 1004, 1004, 1004, 1, 1, 2.0);

  const progress = buildDogCatchGoalProgress(
    db,
    new Set(['paper_trades', 'paper_missed_signal_attribution']),
    1000,
    { targetCatchRate: 0.60, targetWinRate: 0.55, targetRoi: 0.40 }
  );

  assert.equal(progress.trades.fills, 2);
  assert.equal(progress.trades.peak_wins, 1);
  assert.equal(progress.trades.captured_gold_silver_unique, 1);
  assert.equal(progress.missed.clean_gold_silver_unique, 1);
  assert.equal(progress.goal.eligible_gold_silver_unique, 2);
  assert.equal(progress.goal.clean_gold_silver_capture_rate, 0.5);
  assert.deepEqual(progress.goal.blockers, [
    'clean_gold_silver_capture_rate_below_target',
    'peak_win_rate_below_target',
  ]);
  db.close();
});

test('dog catch goal can be served from materialized live snapshot section', () => {
  const snapshot = {
    snapshot_id: 'paper_live_2h_test',
    generated_at: '2026-05-21T00:00:00Z',
    dog_catch_goal: {
      available: true,
      since_ts: 1000,
      trades: { fills: 1, peak_wins: 1, captured_gold_silver_unique: 1 },
      missed: {
        clean_gold_silver_unique: 2,
        clean_gold_unique: 1,
        clean_silver_unique: 1,
        by_blocker: [{ route: 'LOTTO', reject_reason: 'tracking_ttl_expired', gold_n: 1 }],
      },
      goal: {
        eligible_gold_silver_unique: 3,
        captured_gold_silver_unique: 1,
        clean_gold_silver_capture_rate: 1 / 3,
        pass: false,
        blockers: ['clean_gold_silver_capture_rate_below_target'],
      },
    },
  };

  const progress = dogCatchGoalFromLiveSnapshot(snapshot, {
    dbPath: '/tmp/paper.db',
    requestedHours: 2,
  });

  assert.equal(progress.materialized, true);
  assert.equal(progress.materialized_snapshot_id, 'paper_live_2h_test');
  assert.equal(progress.goal.eligible_gold_silver_unique, 3);
  assert.equal(progress.missed.by_blocker[0].reject_reason, 'tracking_ttl_expired');
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
    token_ca: 'token-mark-only',
    symbol: 'MARK',
    signal_id: 6,
    signal_ts: 1005,
    route: 'ATH',
    component: 'matrix_evaluator',
    reject_reason: 'mark_spike',
    tradability_status: 'tradable_reclaim',
    tradability_reason: 'mark_only',
    tradable_peak_pnl: null,
    tradable_missed: 1,
    would_stop_before_peak: 0,
    max_pnl_recorded: 1.3,
    pnl_24h: null,
    pnl_60m: null,
    pnl_15m: null,
    pnl_5m: null,
    created_event_ts: 1005,
    baseline_ts: 1005,
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
  assert.equal(summary.unique_tokens, 4);
  assert.equal(summary.quote_clean_unique, 3);
  assert.equal(summary.quote_clean_dog_unique, 1);
  assert.equal(summary.gold_unique, 1);
  assert.equal(summary.silver_unique, 1);
  assert.equal(summary.bronze_unique, 0);
  assert.equal(summary.mark_only_gold_unique, 1);
  assert.equal(summary.top_missed_dogs.length, 3);
  assert.equal(summary.top_missed_dogs[0].token_ca, 'token-a');
  assert.equal(summary.top_missed_dogs[0].final_blocker_key, 'ATH:source_resonance_probe:scout_quality_buy_pressure_weak');
  assert.equal(summary.top_missed_dogs[0].entry_mode_candidate, 'source_resonance_tiny_probe');
  assert.equal(summary.top_missed_dogs[1].token_ca, 'token-b');
  assert.equal(summary.top_missed_dogs[1].quote_clean, false);
  assert.equal(summary.top_missed_dogs[2].token_ca, 'token-mark-only');
  assert.equal(summary.top_missed_dogs[2].peak_trust_status, 'mark_only_peak_untrusted');
  assert.equal(summary.by_final_blocker[0].final_blocker_key, 'ATH:source_resonance_probe:scout_quality_buy_pressure_weak');
  assert.equal(summary.by_final_blocker[0].gold_unique, 1);

  const summaryOnly = buildClosedLoopMissedDogSummary(
    db,
    new Set(['paper_missed_signal_attribution']),
    1000,
    5,
    { includeDetails: false }
  );
  assert.equal(summaryOnly.unique_tokens, 4);
  assert.equal(summaryOnly.quote_clean_unique, 3);
  assert.equal(summaryOnly.quote_clean_dog_unique, 1);
  assert.equal(summaryOnly.gold_unique, 1);
  assert.equal(summaryOnly.silver_unique, 1);
  assert.equal(summaryOnly.mark_only_gold_unique, 1);
  assert.deepEqual(summaryOnly.top_missed_dogs, []);
  assert.deepEqual(summaryOnly.by_final_blocker, []);
  db.close();
});

test('closed loop missed dog summary excludes tokens already caught by paper trades', () => {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE paper_missed_signal_attribution (
      token_ca TEXT,
      symbol TEXT,
      signal_ts REAL,
      route TEXT,
      component TEXT,
      reject_reason TEXT,
      tradable_peak_pnl REAL,
      tradable_missed INTEGER,
      would_stop_before_peak INTEGER,
      max_pnl_recorded REAL,
      created_event_ts REAL,
      baseline_ts REAL
    );
    CREATE TABLE paper_trades (
      token_ca TEXT,
      entry_ts REAL
    );
  `);
  const insertMissed = db.prepare(`
    INSERT INTO paper_missed_signal_attribution (
      token_ca, symbol, signal_ts, route, component, reject_reason,
      tradable_peak_pnl, tradable_missed, would_stop_before_peak,
      max_pnl_recorded, created_event_ts, baseline_ts
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  insertMissed.run('caught-token', 'CAUGHT', 1001, 'ATH', 'source_resonance_probe', 'scout_quality_buy_pressure_weak', 1.2, 1, 0, 1.2, 1001, 1001);
  insertMissed.run('missed-token', 'MISSED', 1002, 'LOTTO', 'discovery_tracking', 'tracking_ttl_expired', 0.7, 1, 0, 0.7, 1002, 1002);
  db.prepare('INSERT INTO paper_trades (token_ca, entry_ts) VALUES (?, ?)').run('caught-token', 1005);

  const summary = buildClosedLoopMissedDogSummary(
    db,
    new Set(['paper_missed_signal_attribution', 'paper_trades']),
    1000,
    5,
    { includeDetails: true }
  );

  assert.equal(summary.unique_tokens, 1);
  assert.equal(summary.gold_unique, 0);
  assert.equal(summary.silver_unique, 1);
  assert.equal(summary.top_missed_dogs.length, 1);
  assert.equal(summary.top_missed_dogs[0].token_ca, 'missed-token');

  const summaryOnly = buildClosedLoopMissedDogSummary(
    db,
    new Set(['paper_missed_signal_attribution', 'paper_trades']),
    1000,
    5,
    { includeDetails: false }
  );
  assert.equal(summaryOnly.unique_tokens, 1);
  assert.equal(summaryOnly.silver_unique, 1);
  db.close();
});

test('closed loop probe summary uses recent trade window with exit fallback', () => {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE paper_decision_events (
      event_ts REAL,
      token_ca TEXT,
      component TEXT,
      event_type TEXT,
      decision TEXT,
      reason TEXT
    );
    CREATE TABLE paper_trades (
      entry_ts REAL,
      exit_ts REAL,
      entry_mode TEXT,
      token_ca TEXT,
      pnl_pct REAL,
      peak_pnl REAL
    );
  `);
  db.prepare(`
    INSERT INTO paper_decision_events (
      event_ts, token_ca, component, event_type, decision, reason
    ) VALUES (?, ?, ?, ?, ?, ?)
  `).run(1001, 'token-a', 'hard_gate_pass_probe', 'pending_entry', 'accept', 'armed');
  db.prepare(`
    INSERT INTO paper_decision_events (
      event_ts, token_ca, component, event_type, decision, reason
    ) VALUES (?, ?, ?, ?, ?, ?)
  `).run(1003, 'token-pre', 'pre_pass_resonance_probe', 'pending_entry', 'accept', 'armed');
  const insertTrade = db.prepare(`
    INSERT INTO paper_trades (
      entry_ts, exit_ts, entry_mode, token_ca, pnl_pct, peak_pnl
    ) VALUES (?, ?, ?, ?, ?, ?)
  `);
  insertTrade.run(1001, 1010, 'hard_gate_pass_tiny_probe', 'token-a', 0.2, 0.5);
  insertTrade.run(null, 1002, 'hard_gate_pass_tiny_probe', 'token-b', -0.1, 0.1);
  insertTrade.run(1003, 1009, 'pre_pass_resonance_tiny_probe', 'token-pre', 0.4, 0.6);
  insertTrade.run(900, 950, 'hard_gate_pass_tiny_probe', 'old-token', 4.0, 4.0);

  const summary = buildClosedLoopProbeSummary(
    db,
    new Set(['paper_decision_events', 'paper_trades']),
    1000
  );

  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.armed_events, 1);
  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.armed_unique, 1);
  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.fills, 2);
  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.fill_unique, 2);
  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.wins, 1);
  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.avg_pnl_pct, 5);
  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.max_peak_pnl_pct, 50);
  assert.equal(summary.by_mode.pre_pass_resonance_tiny_probe.armed_unique, 1);
  assert.equal(summary.by_mode.pre_pass_resonance_tiny_probe.fills, 1);
  assert.equal(summary.by_mode.pre_pass_resonance_tiny_probe.avg_pnl_pct, 40);
  db.close();
});
