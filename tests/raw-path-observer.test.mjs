import assert from 'node:assert/strict';
import test from 'node:test';

import {
  aggregateSwapsToRawPriceBars,
  buildRawSignalObservations,
  mergePreferredPathRows,
  normalizeBondingCurveTransaction,
  normalizeRawPathBar,
} from '../src/analytics/raw-path-observer.js';

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
  assert.equal(preferred.decisions.DOG.source, 'raw_price_bars_1m');
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
