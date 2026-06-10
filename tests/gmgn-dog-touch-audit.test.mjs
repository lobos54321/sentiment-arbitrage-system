import assert from 'node:assert/strict';
import fs from 'fs';
import os from 'os';
import path from 'path';
import test from 'node:test';

import {
  loadAnchorsFromRawDogJson,
  summarizeGmgnRaw,
} from '../scripts/run-gmgn-dog-touch-audit.js';

test('loads unique sustained gold/silver anchors from raw dog JSON', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'gmgn-touch-test-'));
  const file = path.join(dir, 'raw-dog.json');
  fs.writeFileSync(file, JSON.stringify({
    report: {
      top_raw_dogs: [
        {
          signal_id: 1,
          token_ca: 'Dog111pump',
          signal_ts: 1000,
          raw_sustained_tier: 'gold',
          provider: 'geckoterminal',
          source_kind: 'indexed_ohlcv',
        },
        {
          signal_id: 2,
          token_ca: 'Dog111pump',
          signal_ts: 1000,
          raw_sustained_tier: 'gold',
        },
        {
          signal_id: 3,
          token_ca: 'Dog222pump',
          signal_ts: 2000,
          raw_sustained_tier: 'silver',
        },
        {
          signal_id: 4,
          token_ca: 'Dog333pump',
          signal_ts: 3000,
          raw_sustained_tier: 'bronze',
        },
      ],
      missed_raw_dogs: [
        {
          signal_id: 5,
          token_ca: 'Dog444pump',
          signal_ts: 4000,
          raw_primary_tier: 'gold',
        },
      ],
    },
  }));

  const anchors = loadAnchorsFromRawDogJson(file);

  assert.deepEqual(anchors.map((row) => `${row.token_ca}:${row.signal_ts}`), [
    'Dog111pump:1000',
    'Dog222pump:2000',
    'Dog444pump:4000',
  ]);
  assert.equal(anchors[0].provider, 'geckoterminal');
  assert.equal(anchors[0].source_kind, 'indexed_ohlcv');
});

test('summarizes GMGN object kline volume and early 15m coverage', () => {
  const signalTs = 1_780_000_000;
  const raw = {
    list: [
      { time: (signalTs - 60) * 1000, open: '1', high: '1.2', low: '0.9', close: '1.1', volume: '0', amount: '0' },
      { time: signalTs * 1000, open: '1.1', high: '1.5', low: '1.0', close: '1.4', volume: '1200', amount: '1000000' },
      { time: (signalTs + 60) * 1000, open: '1.4', high: '1.8', low: '1.3', close: '1.7', volume: '0', amount: '0' },
      { time: (signalTs + 16 * 60) * 1000, open: '1.7', high: '2.0', low: '1.6', close: '1.9', volume: '700', amount: '500000' },
    ],
  };

  const summary = summarizeGmgnRaw(raw, { signalTs, preSec: 60, postSec: 7200 });

  assert.equal(summary.bars, 4);
  assert.equal(summary.nonzero_volume_bars, 2);
  assert.equal(summary.early_5m_bars, 2);
  assert.equal(summary.early_5m_nonzero_volume_bars, 1);
  assert.equal(summary.early_5m_volume_usd_sum, 1200);
  assert.equal(summary.early_5m_amount_sum, 1000000);
  assert.equal(summary.early_15m_bars, 2);
  assert.equal(summary.early_15m_nonzero_volume_bars, 1);
  assert.equal(summary.early_15m_volume_usd_sum, 1200);
  assert.equal(summary.early_15m_amount_sum, 1000000);
  assert.equal(summary.first_bar_lag_sec, -60);
  assert.equal(summary.first_nonzero_volume_lag_sec, 0);
  assert.equal(summary.volume_usd_sum, 1900);
});

test('summarizes GMGN array kline responses', () => {
  const signalTs = 1_780_010_000;
  const summary = summarizeGmgnRaw([
    [signalTs, 1, 2, 0.5, 1.5, 50, 1000],
    [signalTs + 60, 1.5, 2.5, 1.4, 2.2, 0, 0],
  ], { signalTs, preSec: 0, postSec: 120 });

  assert.equal(summary.bars, 2);
  assert.equal(summary.nonzero_volume_bars, 1);
  assert.equal(summary.early_5m_volume_usd_sum, 50);
  assert.equal(summary.early_15m_volume_usd_sum, 50);
  assert.equal(summary.amount_sum, 1000);
  assert.equal(summary.price_max, 2.5);
});
