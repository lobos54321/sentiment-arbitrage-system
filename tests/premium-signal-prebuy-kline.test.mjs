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

test('prebuy kline backfill respects provider cooldown before resolving pools', async () => {
  const tokenCa = 'TokenCooldown111111111111111111111111111111111';
  const signalTsSec = 1_777_891_568;
  const targetBars = 5;
  let resolvePoolCalled = false;

  const engine = Object.create(PremiumSignalEngine.prototype);
  engine._poolCache = new Map();
  engine._klineApiCooldownUntil = Date.now() + 60_000;
  engine._prebuyBackfillProviderCooldownUntil = new Map();
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
      resolvePoolCalled = true;
      return { provider: 'geckoterminal', poolAddress: 'Pool', error: null };
    },
    async fetchOhlcvWindow() {
      throw new Error('cooldown should skip provider OHLCV calls');
    },
  };

  const result = await engine._backfillPrebuyKlines(tokenCa, signalTsSec, targetBars);

  assert.equal(result.enough, false);
  assert.equal(result.reason, 'RATE_LIMITED');
  assert.equal(result.provider, 'shared_market_data');
  assert.ok(result.cooldownMs > 0);
  assert.equal(resolvePoolCalled, false);
});

test('prebuy kline backfill uses GMGN fallback during shared provider cooldown', async () => {
  const tokenCa = 'TokenGmgnFallback111111111111111111111111111';
  const signalTsSec = 1_777_891_568;
  const targetBars = 5;
  let savedBars = null;

  const engine = Object.create(PremiumSignalEngine.prototype);
  engine._poolCache = new Map();
  engine._klineApiCooldownUntil = Date.now() + 60_000;
  engine._prebuyBackfillProviderCooldownUntil = new Map();
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
    async fetchGmgnKlineWindow() {
      return {
        ok: true,
        provider: 'gmgn',
        bars: [5, 4, 3, 2, 1].map((minutesAgo, idx) => ({
          timestamp: signalTsSec - minutesAgo * 60,
          open: 0.000001 + idx * 0.00000001,
          high: 0.0000011 + idx * 0.00000001,
          low: 0.0000009 + idx * 0.00000001,
          close: 0.00000105 + idx * 0.00000001,
          volume: 1000 + idx,
        })),
        rateLimited: false,
      };
    },
    async resolvePool() {
      throw new Error('cooldown path should not resolve pools before GMGN fallback');
    },
  };
  engine._saveKlineBars = (ca, pool, bars, scores) => {
    savedBars = { ca, pool, bars, scores };
  };

  const result = await engine._backfillPrebuyKlines(tokenCa, signalTsSec, targetBars);

  assert.equal(result.enough, true);
  assert.equal(result.provider, 'gmgn');
  assert.equal(result.fallbackProvider, 'gmgn');
  assert.equal(result.totalBefore, targetBars);
  assert.equal(savedBars.ca, tokenCa);
  assert.equal(savedBars.scores.source, 'gmgn');
  assert.ok(result.providerAttempts.some((attempt) => attempt.provider === 'shared_market_data' && attempt.reason === 'RATE_LIMITED'));
  assert.ok(result.providerAttempts.some((attempt) => attempt.provider === 'gmgn' && attempt.ok === true));
});

test('strong NOT_ATH unknown data enters retry watch instead of final block', async () => {
  const tokenCa = 'TokenRetryWatch111111111111111111111111111111';
  const signalTsSec = 1_777_891_568;
  const saved = [];
  const engine = Object.create(PremiumSignalEngine.prototype);
  engine.shadowMode = false;
  engine.livePositionMonitor = null;
  engine.stats = {
    not_ath_prebuy_kline_pass: 0,
    not_ath_prebuy_kline_unknown_data: 0,
    not_ath_prebuy_kline_block: 0,
  };
  engine._notAthPrebuyUnknownDataFailClosed = true;
  engine._notAthPrebuyRetryEnabled = true;
  engine._notAthPrebuyRetryDelayMs = 60_000;
  engine._notAthPrebuyRetryMaxAttempts = 1;
  engine._notAthPrebuyRetryWatch = new Map();
  engine._backfillPrebuyKlines = async () => ({
    enough: false,
    provider: 'shared_market_data',
    reason: 'RATE_LIMITED',
    providerAttempts: [
      { provider: 'shared_market_data', ok: false, reason: 'RATE_LIMITED', rateLimited: true },
    ],
  });
  engine._checkKline = async () => ({
    gateStatus: 'UNKNOWN_DATA',
    reason: 'RATE_LIMITED',
    provider: 'external_api',
    failOpenPrevented: true,
  });
  engine.saveSignalRecord = (signal, status, ai, executed, linkage) => {
    saved.push({ status, linkage });
  };

  const result = await engine._executeNotAth(tokenCa, {
    token_ca: tokenCa,
    symbol: 'RETRY',
    timestamp: signalTsSec,
    market_cap: 50_000,
    volume_24h: 120_000,
    indices: {
      super_index: { current: 86 },
      trade_index: { current: 2 },
      address_index: { current: 4 },
      security_index: { current: 20 },
    },
  });

  assert.equal(result.action, 'SKIP');
  assert.match(result.reason, /^prebuy_retry_watch:/);
  assert.equal(saved[0].status, 'NOT_ATH_PREBUY_KLINE_RETRY_QUEUED');
  assert.equal(saved[0].linkage.gateResult.dataConfidence, 'proxy_only');
  assert.equal(saved[0].linkage.gateResult.retryWatch.queued, true);
  for (const retry of engine._notAthPrebuyRetryWatch.values()) {
    clearTimeout(retry.timer);
  }
  engine._notAthPrebuyRetryWatch.clear();
});
