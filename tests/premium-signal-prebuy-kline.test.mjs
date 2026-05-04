import test from 'node:test';
import assert from 'node:assert/strict';

import { PremiumSignalEngine } from '../src/engines/premium-signal-engine.js';

test('prebuy kline backfill requests OHLCV before the signal timestamp', async () => {
  const tokenCa = 'TokenPrebuy111111111111111111111111111111111';
  const poolAddress = 'PoolPrebuy111111111111111111111111111111111';
  const signalTsSec = 1_777_891_568;
  const targetBars = 5;

  let fetchParams = null;
  let fetchOptions = null;
  let savedBars = null;

  const engine = Object.create(PremiumSignalEngine.prototype);
  engine._poolCache = new Map();
  engine.marketDataBackfill = {
    getBarsBefore() {
      return [];
    },
    async backfillWindow() {
      return {
        provider: null,
        poolAddress: null,
        error: 'helius_disabled',
        signaturesFetched: 0,
        transactionsFetched: 0,
        tradesInserted: 0,
        barsWritten: 0,
        cacheHit: false,
      };
    },
  };
  engine.sharedMarketData = {
    async resolvePool() {
      return { provider: 'geckoterminal', poolAddress, error: null };
    },
    async fetchOhlcvWindow(params, options) {
      fetchParams = params;
      fetchOptions = options;
      return {
        provider: 'geckoterminal',
        poolAddress,
        bars: [5, 4, 3, 2, 1].map((minutesAgo, idx) => ({
          timestamp: signalTsSec - minutesAgo * 60,
          open: 0.000001 + idx * 0.00000001,
          high: 0.0000011 + idx * 0.00000001,
          low: 0.0000009 + idx * 0.00000001,
          close: 0.00000105 + idx * 0.00000001,
          volume: 1000 + idx,
        })),
        error: null,
        rateLimited: false,
      };
    },
    async fetchRecentOhlcvByPool() {
      throw new Error('prebuy backfill must not request post-signal recent OHLCV');
    },
  };
  engine._saveKlineBars = (ca, pool, bars) => {
    savedBars = { ca, pool, bars };
  };

  const result = await engine._backfillPrebuyKlines(tokenCa, signalTsSec, targetBars);

  assert.equal(fetchParams.tokenCa, tokenCa);
  assert.equal(fetchParams.poolAddress, poolAddress);
  assert.equal(fetchParams.signalTsSec, signalTsSec);
  assert.ok(fetchParams.startTs < signalTsSec);
  assert.equal(fetchParams.endTs, signalTsSec - 60);
  assert.equal(fetchOptions.minBars, targetBars);
  assert.equal(result.enough, true);
  assert.equal(result.totalBefore, targetBars);
  assert.equal(result.provider, 'geckoterminal');
  assert.equal(savedBars.ca, tokenCa);
  assert.equal(savedBars.pool, poolAddress);
  assert.equal(savedBars.bars.length, targetBars);
  assert.ok(savedBars.bars.every((bar) => bar.ts < signalTsSec));
});
