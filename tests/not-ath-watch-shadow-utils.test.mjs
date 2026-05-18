import assert from 'node:assert/strict';
import { test } from 'node:test';
import Database from 'better-sqlite3';

import { buildNotAthRelaxedShadowCohorts } from '../src/web/not-ath-watch-shadow-utils.js';

function makeDb() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE lotto_not_ath_watch_shadow_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      token_ca TEXT NOT NULL,
      signal_ts INTEGER,
      symbol TEXT,
      parent_blocker TEXT NOT NULL,
      snapshot_ts REAL,
      first_seen_ts REAL,
      horizon_sec INTEGER,
      mark_price REAL,
      quote_price REAL,
      quote_gap_pct REAL,
      spread_pct REAL,
      liquidity_usd REAL,
      quote_clean INTEGER,
      activity_reclaim INTEGER,
      volume_reclaim INTEGER,
      momentum_reclaim INTEGER,
      snapshot_pass INTEGER
    );
    CREATE TABLE paper_missed_signal_attribution (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_event_ts REAL NOT NULL,
      token_ca TEXT NOT NULL,
      symbol TEXT,
      signal_ts INTEGER,
      route TEXT,
      component TEXT NOT NULL,
      reject_reason TEXT,
      baseline_price REAL,
      pnl_5m REAL,
      pnl_15m REAL,
      pnl_60m REAL,
      pnl_24h REAL,
      max_pnl_recorded REAL,
      tradable_missed INTEGER,
      tradable_peak_pnl REAL,
      would_stop_before_peak INTEGER,
      tradability_status TEXT
    );
  `);
  return db;
}

function insertSnapshot(db, row) {
  db.prepare(`
    INSERT INTO lotto_not_ath_watch_shadow_snapshots (
      token_ca, signal_ts, symbol, parent_blocker, snapshot_ts, first_seen_ts,
      horizon_sec, mark_price, quote_price, quote_gap_pct, spread_pct,
      liquidity_usd, quote_clean, activity_reclaim, volume_reclaim,
      momentum_reclaim, snapshot_pass
    ) VALUES (
      @token_ca, @signal_ts, @symbol, 'not_ath_v17', @snapshot_ts, @first_seen_ts,
      @horizon_sec, @mark_price, @quote_price, @quote_gap_pct, @spread_pct,
      @liquidity_usd, @quote_clean, @activity_reclaim, @volume_reclaim,
      @momentum_reclaim, @snapshot_pass
    )
  `).run({
    mark_price: 0.00001,
    quote_price: 0.0000102,
    quote_gap_pct: 2,
    spread_pct: 2,
    liquidity_usd: 12000,
    quote_clean: 1,
    activity_reclaim: 1,
    volume_reclaim: 1,
    momentum_reclaim: 1,
    snapshot_pass: 1,
    first_seen_ts: 1000,
    ...row,
  });
}

function insertMissed(db, row) {
  db.prepare(`
    INSERT INTO paper_missed_signal_attribution (
      created_event_ts, token_ca, symbol, signal_ts, route, component,
      reject_reason, baseline_price, pnl_5m, pnl_15m, pnl_60m, pnl_24h,
      max_pnl_recorded, tradable_missed, tradable_peak_pnl,
      would_stop_before_peak, tradability_status
    ) VALUES (
      @created_event_ts, @token_ca, @symbol, @signal_ts, 'LOTTO', 'upstream_gate',
      'not_ath_v17', 1, @pnl_5m, @pnl_15m, @pnl_60m, @pnl_24h,
      @max_pnl_recorded, @tradable_missed, @tradable_peak_pnl,
      @would_stop_before_peak, @tradability_status
    )
  `).run({
    created_event_ts: 1000,
    pnl_5m: 0.05,
    pnl_15m: 0.1,
    pnl_60m: null,
    pnl_24h: null,
    max_pnl_recorded: 0,
    tradable_missed: 1,
    tradable_peak_pnl: null,
    would_stop_before_peak: 0,
    tradability_status: 'tradable_reclaim',
    ...row,
  });
}

test('not-ath relaxed shadow cohorts separate strict pass from wider recall', () => {
  const db = makeDb();

  insertSnapshot(db, { token_ca: 'StrictCA', symbol: 'STRICT', signal_ts: 100, snapshot_ts: 1000, horizon_sec: 0 });
  insertSnapshot(db, { token_ca: 'StrictCA', symbol: 'STRICT', signal_ts: 100, snapshot_ts: 1300, horizon_sec: 300 });
  insertMissed(db, { token_ca: 'StrictCA', symbol: 'STRICT', signal_ts: 100, max_pnl_recorded: 1.2, tradable_peak_pnl: 1.2 });

  insertSnapshot(db, {
    token_ca: 'LooseCA',
    symbol: 'LOOSE',
    signal_ts: 200,
    snapshot_ts: 1000,
    horizon_sec: 0,
    volume_reclaim: 0,
    momentum_reclaim: 0,
    snapshot_pass: 0,
  });
  insertMissed(db, { token_ca: 'LooseCA', symbol: 'LOOSE', signal_ts: 200, max_pnl_recorded: 0.7, tradable_peak_pnl: 0.7 });

  insertSnapshot(db, {
    token_ca: 'LowLiqCA',
    symbol: 'LOWLIQ',
    signal_ts: 300,
    snapshot_ts: 1000,
    horizon_sec: 0,
    liquidity_usd: 3000,
    quote_clean: 0,
    snapshot_pass: 0,
  });
  insertMissed(db, { token_ca: 'LowLiqCA', symbol: 'LOWLIQ', signal_ts: 300, max_pnl_recorded: 0.4, tradable_peak_pnl: 0.4 });

  insertSnapshot(db, { token_ca: 'LateCA', symbol: 'LATE', signal_ts: 400, snapshot_ts: 3100, horizon_sec: 2100 });
  insertMissed(db, { token_ca: 'LateCA', symbol: 'LATE', signal_ts: 400, max_pnl_recorded: 1.5, tradable_peak_pnl: 1.5 });

  const report = buildNotAthRelaxedShadowCohorts(db, { limit: 20 });
  const byCohort = Object.fromEntries(report.cohorts.map((row) => [row.cohort, row]));

  assert.equal(report.available, true);
  assert.equal(byCohort.two_snapshot_strict.candidates, 1);
  assert.equal(byCohort.two_snapshot_strict.gold_n, 1);
  assert.equal(byCohort.quote_clean_no_double_confirm.candidates, 2);
  assert.equal(byCohort.quote_clean_no_double_confirm.gold_n, 1);
  assert.equal(byCohort.quote_clean_no_double_confirm.silver_n, 1);
  assert.equal(byCohort.relaxed_liquidity_floor.candidates, 1);
  assert.equal(byCohort.relaxed_liquidity_floor.bronze_n, 1);
  assert.equal(byCohort.relaxed_age_window.candidates, 1);
  assert.equal(byCohort.relaxed_age_window.gold_n, 1);
  assert.equal(report.top_hits[0].symbol, 'LATE');
  assert.equal(report.top_hits[0].max_pnl, 1.5);

  db.close();
});
