import assert from 'node:assert/strict';
import test from 'node:test';
import Database from 'better-sqlite3';

import {
  aggregateSwapsToRawPriceBars,
  buildRawSignalObservations,
  mergePreferredPathRows,
  normalizeBondingCurveTransaction,
  normalizeRawPathBar,
} from '../src/analytics/raw-path-observer.js';
import {
  barsToRawPathRows,
  fetchIndexedRawRows,
  getProviderBackoff,
  isProviderRateLimitError,
  rankSignalsForBackfill,
  selectUniqueSignals,
  setProviderBackoff,
} from '../scripts/run-raw-path-observer.js';

const signal = (overrides = {}) => ({
  id: overrides.id ?? 1,
  token_ca: overrides.token_ca ?? 'DOG',
  symbol: overrides.symbol ?? 'DOG',
  timestamp_sec: overrides.signal_ts ?? 1000,
  ...overrides,
});

test('aggregates bonding-curve swaps into raw 1m bars with source kind preserved', () => {
  const bars = aggregateSwapsToRawPriceBars([
    {
      token_ca: 'DOGpump',
      block_time: 1000,
      price: 1.0,
      volume: 2,
      source: 'helius_bonding_curve',
      source_kind: 'bonding_curve',
    },
    {
      token_ca: 'DOGpump',
      block_time: 1010,
      price: 1.5,
      volume: 3,
      source: 'helius_bonding_curve',
      source_kind: 'bonding_curve',
    },
  ]);

  assert.equal(bars.length, 1);
  assert.equal(bars[0].source_kind, 'bonding_curve');
  assert.equal(bars[0].pool_address, 'bonding_curve:DOGpump');
  assert.equal(bars[0].open, 1.0);
  assert.equal(bars[0].high, 1.5);
  assert.equal(bars[0].close, 1.5);
  assert.equal(bars[0].volume, 5);
  assert.equal(bars[0].trade_count, 2);
});

test('pump.fun mint without AMM pool is treated as bonding-curve path', () => {
  const bars = aggregateSwapsToRawPriceBars([
    {
      token_ca: 'Aw5SxKyYhXFdZj2BHCqs11UaV5ohwpFQjauB9jFhpump',
      block_time: 1000,
      price: 1.0,
      volume: 2,
      source: 'helius',
    },
  ]);

  assert.equal(bars.length, 1);
  assert.equal(bars[0].source_kind, 'bonding_curve');
  assert.equal(bars[0].provider, 'helius_bonding_curve');
  assert.equal(bars[0].pool_address, 'bonding_curve:Aw5SxKyYhXFdZj2BHCqs11UaV5ohwpFQjauB9jFhpump');
});

test('normalizes Helius bonding-curve transactions from token and native transfers', () => {
  const tokenCa = 'Aw5SxKyYhXFdZj2BHCqs11UaV5ohwpFQjauB9jFhpump';
  const trade = normalizeBondingCurveTransaction({
    signature: 'sig-1',
    timestamp: 1000,
    slot: 7,
    tokenTransfers: [
      {
        mint: tokenCa,
        tokenAmount: 1000,
      },
    ],
    nativeTransfers: [
      { amount: 500_000_000 },
    ],
  }, { tokenCa });

  assert.equal(trade.source_kind, 'bonding_curve');
  assert.equal(trade.provider, 'helius_bonding_curve');
  assert.equal(trade.pool_address, `bonding_curve:${tokenCa}`);
  assert.equal(trade.baseAmount, 1000);
  assert.equal(trade.quoteAmount, 0.5);
  assert.equal(trade.price, 0.0005);
});


test('raw path preference keeps one stream per token and prefers raw over legacy kline', () => {
  const rawPathRows = [
    normalizeRawPathBar({
      token_ca: 'DOG',
      timestamp: 1000,
      open: 1,
      high: 1,
      low: 1,
      close: 1,
      pool_address: 'amm-1',
      provider: 'helius_amm_pool',
      source_kind: 'amm_pool',
    }),
    normalizeRawPathBar({
      token_ca: 'DOG',
      timestamp: 1060,
      open: 1.1,
      high: 1.2,
      low: 1.1,
      close: 1.2,
      pool_address: 'amm-1',
      provider: 'helius_amm_pool',
      source_kind: 'amm_pool',
    }),
  ];
  const legacyRows = [
    {
      token_ca: 'DOG',
      timestamp: 1000,
      open: 2,
      high: 2,
      low: 2,
      close: 2,
      pool_address: 'legacy-pool',
      provider: 'geckoterminal',
      source_kind: 'indexed_ohlcv',
    },
  ];

  const preferred = mergePreferredPathRows({
    signals: [signal()],
    rawPathRows,
    klineRows: legacyRows,
  });

  assert.equal(preferred.rows.length, 2);
  assert.equal(preferred.rows[0].provider, 'helius_amm_pool');
  assert.equal(preferred.rows[0].pool_address, 'amm-1');
  assert.equal(preferred.decisions['DOG:1000'].source, 'raw_price_bars_1m');
});

test('raw path preference uses the stream compatible with each signal anchor', () => {
  const rawPathRows = [
    normalizeRawPathBar({
      token_ca: 'DOGpump',
      timestamp: 2200,
      open: 2,
      high: 2.1,
      low: 1.9,
      close: 2,
      volume: 1000,
      pool_address: 'gmgn-post-graduation',
      provider: 'gmgn',
      source_kind: 'indexed_ohlcv',
    }),
    normalizeRawPathBar({
      token_ca: 'DOGpump',
      timestamp: 2260,
      open: 2,
      high: 2.2,
      low: 2,
      close: 2.1,
      volume: 900,
      pool_address: 'gmgn-post-graduation',
      provider: 'gmgn',
      source_kind: 'indexed_ohlcv',
    }),
  ];
  const legacyRows = [
    {
      token_ca: 'DOGpump',
      timestamp: 1000,
      open: 1,
      high: 1,
      low: 1,
      close: 1,
      volume: 0,
      pool_address: 'gecko-virtual-pool',
      provider: 'geckoterminal',
      source_kind: 'indexed_ohlcv',
    },
    {
      token_ca: 'DOGpump',
      timestamp: 1060,
      open: 1,
      high: 1.8,
      low: 1,
      close: 1.7,
      volume: 0,
      pool_address: 'gecko-virtual-pool',
      provider: 'geckoterminal',
      source_kind: 'indexed_ohlcv',
    },
  ];

  const preferred = mergePreferredPathRows({
    signals: [signal({ token_ca: 'DOGpump', signal_ts: 1000 })],
    rawPathRows,
    klineRows: legacyRows,
  });

  assert.equal(preferred.rows.length, 2);
  assert.equal(preferred.rows[0].provider, 'geckoterminal');
  assert.equal(preferred.rows[0].pool_address, 'gecko-virtual-pool');
  assert.equal(preferred.decisions['DOGpump:1000'].source, 'legacy_kline_1m');
  assert.equal(preferred.decisions['DOGpump:1000'].provider, 'geckoterminal');
});

test('raw path preference can keep different streams for different anchors of the same token', () => {
  const rawPathRows = [
    normalizeRawPathBar({
      token_ca: 'DOGpump',
      timestamp: 2200,
      open: 2,
      high: 2.1,
      low: 1.9,
      close: 2,
      volume: 1000,
      pool_address: 'gmgn-post-graduation',
      provider: 'gmgn',
      source_kind: 'indexed_ohlcv',
    }),
    normalizeRawPathBar({
      token_ca: 'DOGpump',
      timestamp: 2260,
      open: 2,
      high: 2.2,
      low: 2,
      close: 2.1,
      volume: 900,
      pool_address: 'gmgn-post-graduation',
      provider: 'gmgn',
      source_kind: 'indexed_ohlcv',
    }),
  ];
  const legacyRows = [
    {
      token_ca: 'DOGpump',
      timestamp: 1000,
      open: 1,
      high: 1,
      low: 1,
      close: 1,
      volume: 0,
      pool_address: 'gecko-virtual-pool',
      provider: 'geckoterminal',
      source_kind: 'indexed_ohlcv',
    },
    {
      token_ca: 'DOGpump',
      timestamp: 1060,
      open: 1,
      high: 1.8,
      low: 1,
      close: 1.7,
      volume: 0,
      pool_address: 'gecko-virtual-pool',
      provider: 'geckoterminal',
      source_kind: 'indexed_ohlcv',
    },
  ];

  const preferred = mergePreferredPathRows({
    signals: [
      signal({ id: 1, token_ca: 'DOGpump', signal_ts: 1000 }),
      signal({ id: 2, token_ca: 'DOGpump', signal_ts: 2200 }),
    ],
    rawPathRows,
    klineRows: legacyRows,
  });

  assert.equal(preferred.rows.length, 4);
  assert.equal(preferred.decisions['DOGpump:1000'].provider, 'geckoterminal');
  assert.equal(preferred.decisions['DOGpump:2200'].provider, 'gmgn');
});

test('raw path observer prioritizes signals with no existing path over cache-covered latest tokens', () => {
  const signalDb = new Database(':memory:');
  const rawDb = new Database(':memory:');
  signalDb.exec(`
    CREATE TABLE kline_1m (
      token_ca TEXT,
      timestamp INTEGER,
      high REAL,
      low REAL,
      close REAL
    )
  `);
  rawDb.exec(`
    CREATE TABLE raw_price_bars_1m (
      token_ca TEXT,
      timestamp INTEGER
    )
  `);
  const service = {
    getBars(tokenCa) {
      if (tokenCa === 'CACHE_COVERED') {
        return [
          { token_ca: tokenCa, timestamp: 1000 },
          { token_ca: tokenCa, timestamp: 1060 },
        ];
      }
      return [];
    },
  };

  const ranked = rankSignalsForBackfill([
    signal({ id: 1, token_ca: 'CACHE_COVERED', signal_ts: 1000 }),
    signal({ id: 2, token_ca: 'MISSING_PATH', signal_ts: 1000 }),
  ], {
    signalDb,
    rawDb,
    service,
    now: 10_000,
    horizonSec: 7200,
  });

  assert.equal(ranked[0].token_ca, 'MISSING_PATH');
  assert.equal(ranked[0].raw_path_selection_reason, 'no_raw_legacy_or_cache_path');
  assert.equal(ranked[1].token_ca, 'CACHE_COVERED');
  assert.equal(ranked[1].raw_path_selection_reason, 'cache_or_legacy_only_no_raw_path');
  signalDb.close();
  rawDb.close();
});

test('raw path observer keeps distinct signal anchors for the same token', () => {
  const selected = selectUniqueSignals([
    signal({ id: 1, token_ca: 'DOG', signal_ts: 1000 }),
    signal({ id: 2, token_ca: 'DOG', signal_ts: 5000 }),
    signal({ id: 3, token_ca: 'DOG', signal_ts: 5000 }),
  ], 10);

  assert.equal(selected.length, 2);
  assert.deepEqual(selected.map((row) => row.signal_ts_sec), [5000, 1000]);
  assert.equal(selected[0].id, 3);
});

test('raw path observer prioritizes anchor gaps recorded by raw signal observations', () => {
  const signalDb = new Database(':memory:');
  const rawDb = new Database(':memory:');
  signalDb.exec(`
    CREATE TABLE kline_1m (
      token_ca TEXT,
      timestamp INTEGER,
      high REAL,
      low REAL,
      close REAL
    )
  `);
  rawDb.exec(`
    CREATE TABLE raw_price_bars_1m (
      token_ca TEXT,
      timestamp INTEGER,
      volume REAL,
      provider TEXT,
      source_kind TEXT
    );
    CREATE TABLE raw_signal_observations (
      signal_id TEXT,
      token_ca TEXT NOT NULL,
      signal_ts INTEGER NOT NULL,
      status TEXT,
      coverage_reason TEXT,
      path_row_count INTEGER,
      first_bar_ts INTEGER,
      first_bar_lag_sec INTEGER,
      early_15m_bar_count INTEGER,
      early_15m_bar_coverage_pct REAL,
      early_15m_complete INTEGER,
      updated_at INTEGER
    )
  `);
  const insertBar = rawDb.prepare(`
    INSERT INTO raw_price_bars_1m (token_ca, timestamp, volume, provider, source_kind)
    VALUES (?, ?, ?, ?, ?)
  `);
  for (let idx = 0; idx < 12; idx += 1) {
    insertBar.run('DOG', 1000 + idx * 60, 10, 'gmgn', 'indexed_ohlcv');
    insertBar.run('OK', 1000 + idx * 60, 10, 'gmgn', 'indexed_ohlcv');
  }
  rawDb.prepare(`
    INSERT INTO raw_signal_observations (
      signal_id, token_ca, signal_ts, status, coverage_reason,
      path_row_count, first_bar_lag_sec, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run('1', 'DOG', 1000, 'matured', 'baseline_after_max_lag', 12, 1200, 2000);

  const ranked = rankSignalsForBackfill([
    signal({ id: 1, token_ca: 'DOG', signal_ts: 1000 }),
    signal({ id: 2, token_ca: 'OK', signal_ts: 1000 }),
  ], {
    signalDb,
    rawDb,
    service: { getBars() { return []; } },
    now: 10_000,
    horizonSec: 7200,
  });

  assert.equal(ranked[0].token_ca, 'DOG');
  assert.equal(ranked[0].raw_path_selection_reason, 'raw_observation_baseline_after_max_lag');
  assert.equal(ranked[0].raw_path_observation_needs_anchor_backfill, true);
  assert.equal(ranked[1].token_ca, 'OK');
  assert.equal(ranked[1].raw_path_selection_reason, 'already_path_covered');
  signalDb.close();
  rawDb.close();
});

test('raw path observer prioritizes denominator coverage failures from raw outcomes', () => {
  const signalDb = new Database(':memory:');
  const rawDb = new Database(':memory:');
  signalDb.exec(`
    CREATE TABLE kline_1m (
      token_ca TEXT,
      timestamp INTEGER,
      high REAL,
      low REAL,
      close REAL
    )
  `);
  rawDb.exec(`
    CREATE TABLE raw_price_bars_1m (
      token_ca TEXT,
      timestamp INTEGER,
      volume REAL,
      provider TEXT,
      source_kind TEXT
    );
    CREATE TABLE raw_signal_outcomes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      token_ca TEXT NOT NULL,
      signal_ts INTEGER NOT NULL,
      observation_status TEXT,
      coverage_reason TEXT,
      kline_covered INTEGER,
      baseline_ts INTEGER,
      baseline_lag_sec REAL,
      first_bar_ts INTEGER,
      first_bar_lag_sec INTEGER,
      early_15m_bar_count INTEGER,
      early_15m_bar_coverage_pct REAL,
      early_15m_complete INTEGER,
      updated_at INTEGER
    );
    CREATE TABLE raw_signal_observations (
      signal_id TEXT,
      token_ca TEXT NOT NULL,
      signal_ts INTEGER NOT NULL,
      status TEXT,
      coverage_reason TEXT,
      path_row_count INTEGER,
      first_bar_ts INTEGER,
      first_bar_lag_sec INTEGER,
      early_15m_bar_count INTEGER,
      early_15m_bar_coverage_pct REAL,
      early_15m_complete INTEGER,
      updated_at INTEGER
    )
  `);
  const insertBar = rawDb.prepare(`
    INSERT INTO raw_price_bars_1m (token_ca, timestamp, volume, provider, source_kind)
    VALUES (?, ?, ?, ?, ?)
  `);
  for (let idx = 0; idx < 12; idx += 1) {
    insertBar.run('DOG', 1000 + idx * 60, 10, 'gmgn', 'indexed_ohlcv');
    insertBar.run('OK', 1000 + idx * 60, 10, 'gmgn', 'indexed_ohlcv');
  }
  rawDb.prepare(`
    INSERT INTO raw_signal_outcomes (
      token_ca, signal_ts, observation_status, coverage_reason,
      kline_covered, baseline_lag_sec, first_bar_lag_sec, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run('DOG', 1000, 'matured', 'no_kline_after_anchor', 0, null, null, 3000);
  rawDb.prepare(`
    INSERT INTO raw_signal_observations (
      signal_id, token_ca, signal_ts, status, coverage_reason,
      path_row_count, first_bar_lag_sec, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run('1', 'DOG', 1000, 'matured', 'raw_path_after_early_window', 12, 900, 2000);

  const ranked = rankSignalsForBackfill([
    signal({ id: 1, token_ca: 'DOG', signal_ts: 1000 }),
    signal({ id: 2, token_ca: 'OK', signal_ts: 1000 }),
  ], {
    signalDb,
    rawDb,
    service: { getBars() { return []; } },
    now: 10_000,
    horizonSec: 7200,
  });

  assert.equal(ranked[0].token_ca, 'DOG');
  assert.equal(ranked[0].raw_path_selection_reason, 'raw_outcome_no_kline_after_anchor');
  assert.equal(ranked[0].raw_path_outcome_needs_anchor_backfill, true);
  assert.equal(ranked[0].raw_path_observation_needs_anchor_backfill, true);
  signalDb.close();
  rawDb.close();
});

test('raw path observer can prefer recent signals within the same backfill priority', () => {
  const signalDb = new Database(':memory:');
  const rawDb = new Database(':memory:');
  signalDb.exec(`
    CREATE TABLE kline_1m (
      token_ca TEXT,
      timestamp INTEGER,
      high REAL,
      low REAL,
      close REAL
    )
  `);
  rawDb.exec(`
    CREATE TABLE raw_price_bars_1m (
      token_ca TEXT,
      timestamp INTEGER,
      volume REAL,
      provider TEXT,
      source_kind TEXT
    );
    CREATE TABLE raw_signal_outcomes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      token_ca TEXT NOT NULL,
      signal_ts INTEGER NOT NULL,
      observation_status TEXT,
      coverage_reason TEXT,
      kline_covered INTEGER,
      baseline_ts INTEGER,
      baseline_lag_sec REAL,
      first_bar_ts INTEGER,
      first_bar_lag_sec INTEGER,
      early_15m_bar_count INTEGER,
      early_15m_bar_coverage_pct REAL,
      early_15m_complete INTEGER,
      updated_at INTEGER
    )
  `);
  const insertOutcome = rawDb.prepare(`
    INSERT INTO raw_signal_outcomes (
      token_ca, signal_ts, observation_status, coverage_reason, kline_covered, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?)
  `);
  insertOutcome.run('OLD', 1000, 'matured', 'no_kline_for_token', 0, 2000);
  insertOutcome.run('NEW', 5000, 'matured', 'no_kline_for_token', 0, 2000);

  const ranked = rankSignalsForBackfill([
    signal({ id: 1, token_ca: 'OLD', signal_ts: 1000 }),
    signal({ id: 2, token_ca: 'NEW', signal_ts: 5000 }),
  ], {
    signalDb,
    rawDb,
    service: { getBars() { return []; } },
    now: 20_000,
    horizonSec: 7200,
    recencyFirst: true,
  });

  assert.equal(ranked[0].token_ca, 'NEW');
  assert.equal(ranked[0].raw_path_selection_reason, 'raw_outcome_no_kline_for_token');
  assert.equal(ranked[1].token_ca, 'OLD');
  signalDb.close();
  rawDb.close();
});

test('raw path observer requeues Gecko zero-volume paths for GMGN volume enrichment', () => {
  const signalDb = new Database(':memory:');
  const rawDb = new Database(':memory:');
  signalDb.exec(`
    CREATE TABLE kline_1m (
      token_ca TEXT,
      timestamp INTEGER,
      high REAL,
      low REAL,
      close REAL
    )
  `);
  rawDb.exec(`
    CREATE TABLE raw_price_bars_1m (
      token_ca TEXT,
      timestamp INTEGER,
      volume REAL,
      provider TEXT,
      source_kind TEXT
    )
  `);
  const insert = rawDb.prepare(`
    INSERT INTO raw_price_bars_1m (token_ca, timestamp, volume, provider, source_kind)
    VALUES (?, ?, ?, ?, ?)
  `);
  for (let idx = 0; idx < 12; idx += 1) {
    insert.run('ZERO_VOL', 1000 + idx * 60, 0, 'geckoterminal', 'indexed_ohlcv');
    insert.run('GMGN_VOL', 1000 + idx * 60, 100 + idx, 'gmgn', 'indexed_ohlcv');
  }

  const ranked = rankSignalsForBackfill([
    signal({ id: 1, token_ca: 'GMGN_VOL', signal_ts: 1000 }),
    signal({ id: 2, token_ca: 'ZERO_VOL', signal_ts: 1000 }),
  ], {
    signalDb,
    rawDb,
    service: { getBars() { return []; } },
    now: 10_000,
    horizonSec: 7200,
  });

  assert.equal(ranked[0].token_ca, 'ZERO_VOL');
  assert.equal(ranked[0].raw_path_selection_reason, 'raw_path_zero_volume_needs_gmgn_enrichment');
  assert.equal(ranked[0].raw_path_needs_volume_enrichment, true);
  assert.equal(ranked[0].raw_path_gecko_zero_volume_bars, 12);
  assert.equal(ranked[0].raw_path_nonzero_volume_bars, 0);
  assert.equal(ranked[1].token_ca, 'GMGN_VOL');
  assert.equal(ranked[1].raw_path_selection_reason, 'already_path_covered');
  signalDb.close();
  rawDb.close();
});

test('raw path observer records provider backoff for Helius rate limits', () => {
  const rawDb = new Database(':memory:');
  assert.equal(isProviderRateLimitError('HTTP 429: {"message":"max usage reached"}'), true);
  assert.equal(isProviderRateLimitError('no_pool'), false);

  const state = setProviderBackoff(rawDb, 'helius', 'HTTP 429: max usage reached', {
    nowTs: 1000,
    cooldownSec: 120,
  });
  assert.equal(state.active, true);
  assert.equal(state.cooldown_until, 1120);
  assert.equal(state.cooldown_seconds_remaining, 120);

  const later = getProviderBackoff(rawDb, 'helius', 1121);
  assert.equal(later.active, false);
  assert.equal(later.cooldown_seconds_remaining, 0);
  rawDb.close();
});

test('indexed fallback converts OHLCV into raw rows without requiring Helius backfill', async () => {
  let seenParams = null;
  let seenOptions = null;
  const result = await fetchIndexedRawRows({
    async fetchOhlcvWindow(params, options) {
      seenParams = params;
      seenOptions = options;
      return {
        provider: 'geckoterminal',
        poolAddress: 'PoolIndexed111',
        priceUnit: 'native',
        bars: [
          { timestamp: 1000, open: 1, high: 1.2, low: 0.9, close: 1.1, volume: 10 },
          { timestamp: 1060, open: 1.1, high: 1.4, low: 1.0, close: 1.3, volume: 12 },
        ],
      };
    },
  }, {
    tokenCa: 'DOG',
    signalTsSec: 1000,
    endTs: 1120,
  });

  assert.equal(seenParams.tokenCa, 'DOG');
  assert.equal(seenOptions.skipBackfill, true);
  assert.equal(result.rawRows.length, 2);
  assert.equal(result.rawRows[0].provider, 'geckoterminal');
  assert.equal(result.rawRows[0].source_kind, 'indexed_ohlcv');
  assert.equal(result.rawRows[0].source_family, 'third_party_kline');
  assert.equal(result.rawRows[0].pool_address, 'PoolIndexed111');
});

test('indexed OHLCV rows keep a synthetic same-source stream when no AMM pool exists', () => {
  const rows = barsToRawPathRows({
    tokenCa: 'DOG',
    provider: 'gmgn',
    priceUnit: 'USD_PER_TOKEN',
    bars: [
      { timestamp: 1000, open: 1, high: 2, low: 0.8, close: 1.5, volume: 100 },
    ],
  });

  assert.equal(rows.length, 1);
  assert.equal(rows[0].pool_address, 'indexed_ohlcv:gmgn:DOG');
  assert.equal(rows[0].provider, 'gmgn');
  assert.equal(rows[0].source_kind, 'indexed_ohlcv');
  assert.equal(rows[0].source_family, 'third_party_kline');
  assert.equal(rows[0].price_unit, 'usd_per_token');
});

test('raw signal observations distinguish right-censored and early-window incomplete paths', () => {
  const observations = buildRawSignalObservations({
    signals: [
      signal({ id: 1, token_ca: 'PENDING', signal_ts: 9000 }),
      signal({ id: 2, token_ca: 'LATE', signal_ts: 1000 }),
    ],
    pathRows: [
      normalizeRawPathBar({
        token_ca: 'LATE',
        timestamp: 1900,
        open: 1,
        high: 1,
        low: 1,
        close: 1,
        pool_address: 'amm-late',
        provider: 'helius_amm_pool',
        source_kind: 'amm_pool',
      }),
    ],
    nowTs: 10_000,
    horizonSec: 7200,
    earlyWindowSec: 900,
  });

  const pending = observations.find((row) => row.token_ca === 'PENDING');
  const late = observations.find((row) => row.token_ca === 'LATE');
  assert.equal(pending.status, 'pending');
  assert.equal(pending.coverage_reason, 'right_censored_open');
  assert.equal(late.status, 'matured');
  assert.equal(late.early_15m_complete, false);
  assert.equal(late.first_bar_lag_sec, 900);
  assert.equal(late.coverage_reason, 'covered');
});
