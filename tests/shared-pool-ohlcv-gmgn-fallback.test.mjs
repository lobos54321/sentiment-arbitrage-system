import test from 'node:test';
import assert from 'node:assert/strict';

import { SharedPoolOhlcvClient } from '../src/market-data/shared-pool-ohclv-client.js';

function createRuntime(fetchJsonResult = { ok: false, error: 'no_ohlcv', rateLimited: false }) {
  return {
    async getCache() { return null; },
    async setCache() {},
    async setSharedCooldown() {},
    async runSingleFlight(_key, producer) { return producer(); },
    async fetchJson() { return fetchJsonResult; },
    async close() {},
  };
}

test('shared OHLCV uses GMGN kline fallback when provider returns no bars', async () => {
  const tokenCa = 'TokenGmgnFallback111111111111111111111111111';
  const poolAddress = 'PoolGmgnFallback111111111111111111111111111';
  const startTs = 1_777_900_000;
  const endTs = startTs + 5 * 60;
  const upserted = [];

  const client = new SharedPoolOhlcvClient({
    evaluator: {
      maxHistoricalBars: 20,
      klineCacheDbPath: ':memory:',
    },
  }, {
    runtime: createRuntime(),
    repository: {
      getBars() { return []; },
      upsertBars(ca, pool, bars, provider) {
        upserted.push({ ca, pool, bars, provider });
      },
      upsertPoolMapping() {},
    },
    poolResolver: {
      async resolvePool() {
        return { provider: 'geckoterminal', poolAddress };
      },
    },
    backfillService: {
      async backfillWindow() {
        return { provider: null, poolAddress, bars: [], error: 'no_ohlcv' };
      },
    },
    gmgnKlineFallbackEnabled: true,
    async gmgnKlineFetcher() {
      return {
        list: [0, 1, 2, 3, 4].map((idx) => ({
          time: (startTs + idx * 60) * 1000,
          open: String(0.000001 + idx * 0.00000001),
          high: String(0.0000012 + idx * 0.00000001),
          low: String(0.0000009 + idx * 0.00000001),
          close: String(0.0000011 + idx * 0.00000001),
          volume: String(1000 + idx),
        })),
      };
    },
  });

  const result = await client.fetchOhlcvWindow({
    tokenCa,
    poolAddress,
    signalTsSec: startTs,
    startTs,
    endTs,
    bars: 5,
  }, {
    minBars: 5,
    windows: [endTs],
    limit: 5,
  });

  assert.equal(result.provider, 'gmgn');
  assert.equal(result.priceUnit, 'USD_PER_TOKEN');
  assert.equal(result.volumeUnit, 'USD');
  assert.equal(result.bars.length, 5);
  assert.equal(upserted[0].provider, 'gmgn');
});

test('shared OHLCV can skip Helius backfill and still use indexed fallback', async () => {
  const tokenCa = 'TokenSkipBackfill11111111111111111111111111';
  const poolAddress = 'PoolSkipBackfill11111111111111111111111111';
  const startTs = 1_777_910_000;
  const endTs = startTs + 2 * 60;
  let backfillCalls = 0;

  const client = new SharedPoolOhlcvClient({
    evaluator: {
      maxHistoricalBars: 20,
      klineCacheDbPath: ':memory:',
    },
  }, {
    runtime: createRuntime(),
    repository: {
      getBars() { return []; },
      upsertBars() {},
      upsertPoolMapping() {},
    },
    poolResolver: {
      async resolvePool() {
        return { provider: 'geckoterminal', poolAddress };
      },
    },
    backfillService: {
      async backfillWindow() {
        backfillCalls += 1;
        throw new Error('backfill_should_not_run');
      },
    },
    gmgnKlineFallbackEnabled: true,
    async gmgnKlineFetcher() {
      return {
        list: [0, 1].map((idx) => ({
          time: (startTs + idx * 60) * 1000,
          open: String(0.000002 + idx * 0.00000001),
          high: String(0.0000022 + idx * 0.00000001),
          low: String(0.0000019 + idx * 0.00000001),
          close: String(0.0000021 + idx * 0.00000001),
          volume: String(2000 + idx),
        })),
      };
    },
  });

  const result = await client.fetchOhlcvWindow({
    tokenCa,
    poolAddress,
    signalTsSec: startTs,
    startTs,
    endTs,
    bars: 2,
  }, {
    minBars: 2,
    skipBackfill: true,
    windows: [endTs],
    limit: 2,
  });

  assert.equal(backfillCalls, 0);
  assert.equal(result.provider, 'gmgn');
  assert.equal(result.bars.length, 2);
});
