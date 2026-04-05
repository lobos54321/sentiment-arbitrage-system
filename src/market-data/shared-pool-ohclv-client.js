import autonomyConfig from '../config/autonomy-config.js';
import { KlineRepository } from './kline-repository.js';
import { PoolResolver } from './pool-resolver.js';
import { MarketDataBackfillService } from './market-data-backfill-service.js';
import { MARKET_DATA_REASON, SharedMarketRuntime, isMarketDataFlagEnabled, normalizeMarketDataReason } from './shared-market-runtime.js';

function normalizePoolId(poolAddress) {
  if (!poolAddress) return null;
  return String(poolAddress).startsWith('solana_') ? String(poolAddress) : `solana_${poolAddress}`;
}

function normalizeBars(list = []) {
  return list.map(([timestamp, open, high, low, close, volume]) => ({
    timestamp: Number(timestamp),
    open: Number(open),
    high: Number(high),
    low: Number(low),
    close: Number(close),
    volume: Number(volume || 0)
  }));
}

export class SharedPoolOhlcvClient {
  constructor(config = autonomyConfig, options = {}) {
    this.config = config;
    this.runtime = options.runtime || new SharedMarketRuntime({ namespace: 'market-data:pool-ohclv' });
    this.repository = options.repository || new KlineRepository(config.evaluator.klineCacheDbPath);
    this.poolResolver = options.poolResolver || new PoolResolver({ repository: this.repository });
    this.backfillService = options.backfillService || new MarketDataBackfillService(config, {
      repository: this.repository,
      poolResolver: this.poolResolver
    });
    this.sharedPoolResolutionEnabled = options.sharedPoolResolutionEnabled ?? isMarketDataFlagEnabled('MARKET_DATA_SHARED_POOL_RESOLUTION', true);
    this.sharedOhlcvEnabled = options.sharedOhlcvEnabled ?? isMarketDataFlagEnabled('MARKET_DATA_SHARED_OHLCV', true);
  }

  getRepository() {
    return this.repository;
  }

  async resolvePool(tokenCa, options = {}) {
    if (!tokenCa) {
      return {
        provider: null,
        poolAddress: null,
        error: 'missing_token',
        reason: MARKET_DATA_REASON.MISSING_INPUTS,
        rateLimited: false,
        fetchedAt: Date.now(),
        source: 'shared-pool-client'
      };
    }

    if (!this.sharedPoolResolutionEnabled && options.legacyResolver) {
      return options.legacyResolver(tokenCa);
    }

    const cacheKey = `pool:${tokenCa}`;
    const ttlMs = options.cacheTtlMs ?? 15 * 60 * 1000;
    const cached = await this.runtime.getCache(cacheKey);
    if (cached) {
      return { ...cached, cacheHit: true };
    }

    const result = await this.runtime.runSingleFlight(`resolve-pool:${tokenCa}`, async () => {
      const resolved = await this.poolResolver.resolvePool(tokenCa);
      const normalized = {
        provider: resolved.provider || null,
        poolAddress: resolved.poolAddress || null,
        error: resolved.error || null,
        reason: resolved.reason || normalizeMarketDataReason({
          error: resolved.error || null,
          rateLimited: resolved.rateLimited
        }),
        rateLimited: Boolean(resolved.rateLimited || resolved.error === 'cooldown_active'),
        fetchedAt: Date.now(),
        source: 'shared-pool-client',
        cacheHit: false
      };
      if (normalized.poolAddress) {
        await this.runtime.setCache(cacheKey, normalized, ttlMs);
      }
      return normalized;
    }, { distributed: true, cacheKey });

    return result;
  }

  getCachedBars(tokenCa, startTs, endTs, minBars = 1) {
    const bars = this.repository.getBars(tokenCa, startTs, endTs);
    if (bars.length >= minBars) {
      return {
        provider: 'cache',
        poolAddress: bars[0]?.pool_address || null,
        bars,
        error: null,
        rateLimited: false,
        fetchedAt: Date.now(),
        cacheHit: true,
        source: 'shared-pool-client'
      };
    }
    return null;
  }

  async backfillWindow(params = {}) {
    const result = await this.backfillService.backfillWindow(params);
    return {
      provider: result.provider || null,
      poolAddress: result.poolAddress || null,
      bars: result.bars || [],
      error: result.error || null,
      reason: result.reason || normalizeMarketDataReason({ error: result.error || null, rateLimited: result.error === 'cooldown_active' }),
      rateLimited: result.error === 'cooldown_active',
      fetchedAt: Date.now(),
      cacheHit: Boolean(result.cacheHit),
      source: 'shared-pool-client',
      metrics: {
        tradesInserted: result.tradesInserted || 0,
        barsWritten: result.barsWritten || 0,
        signaturesFetched: result.signaturesFetched || 0,
        transactionsFetched: result.transactionsFetched || 0,
        poolProvider: result.poolProvider || null
      }
    };
  }

  async fetchOhlcvWindow({ tokenCa, signalTsSec = Math.floor(Date.now() / 1000), poolAddress = null, bars = this.config.evaluator.maxHistoricalBars, startTs = null, endTs = null } = {}, options = {}) {
    const windowStart = startTs ?? signalTsSec;
    const windowEnd = endTs ?? (signalTsSec + bars * 60);
    const minBars = options.minBars ?? 1;

    const cached = this.getCachedBars(tokenCa, windowStart, windowEnd, minBars);
    if (cached) {
      return cached;
    }

    if (!this.sharedOhlcvEnabled && options.legacyFetcher) {
      return options.legacyFetcher({ tokenCa, signalTsSec, poolAddress, bars, startTs, endTs });
    }

    const backfillResult = await this.backfillWindow({
      tokenCa,
      signalTsSec,
      startTs: windowStart,
      endTs: windowEnd,
      minBars,
      poolAddress
    });
    if ((backfillResult.bars?.length || 0) >= minBars) {
      return backfillResult;
    }

    const resolvedPool = poolAddress || backfillResult.poolAddress || (await this.resolvePool(tokenCa)).poolAddress;
    if (!resolvedPool) {
      return {
        provider: backfillResult.provider || null,
        poolAddress: null,
        bars: backfillResult.bars || [],
        error: backfillResult.error || 'no_pool',
        reason: backfillResult.reason || normalizeMarketDataReason({ error: backfillResult.error || 'no_pool', rateLimited: false }) || MARKET_DATA_REASON.NO_POOL,
        rateLimited: false,
        fetchedAt: Date.now(),
        cacheHit: false,
        source: 'shared-pool-client'
      };
    }

    const cacheKey = `ohlcv:${tokenCa}:${resolvedPool}:${windowStart}:${windowEnd}:${bars}`;
    const ttlMs = options.cacheTtlMs ?? 60 * 1000;
    const cachedProviderResult = await this.runtime.getCache(cacheKey);
    if (cachedProviderResult) {
      return { ...cachedProviderResult, cacheHit: true };
    }

    const windows = options.windows || [windowEnd, signalTsSec + 600, signalTsSec + 3600];
    const limit = options.limit || Math.max(20, Math.min(200, bars));

    const providerResult = await this.runtime.runSingleFlight(`ohlcv:${cacheKey}`, async () => {
      const byTs = new Map();
      let lastError = null;

      for (const beforeTimestamp of windows.filter(Boolean)) {
        const geckoResponse = await this.runtime.fetchJson(
          `https://api.geckoterminal.com/api/v2/networks/solana/pools/${resolvedPool}/ohlcv/minute?aggregate=1&limit=${limit}&before_timestamp=${beforeTimestamp}&token=base`,
          {
            provider: 'geckoterminal',
            source: 'GECKOTERMINAL',
            requestKey: `${cacheKey}:before:${beforeTimestamp}`,
            cacheTtlMs: ttlMs,
            cooldownMs: 30000,
            limiter: { requestsPerSecond: 0.2, burstCapacity: 1 },
            timeout: 20000,
            maxRetries: 2,
            initialDelay: 2500,
            maxDelay: 20000
          }
        );

        if (!geckoResponse.ok) {
          lastError = geckoResponse.error || lastError;
          if (geckoResponse.rateLimited) {
            break;
          }
          continue;
        }

        const list = geckoResponse.data?.data?.attributes?.ohlcv_list || [];
        for (const bar of normalizeBars(list)) {
          if (!Number.isFinite(bar.timestamp)) continue;
          if (bar.timestamp < windowStart || bar.timestamp > windowEnd) continue;
          if (!byTs.has(bar.timestamp)) {
            byTs.set(bar.timestamp, bar);
          }
        }
      }

      const rateLimited = lastError === 'cooldown_active' || /429/.test(String(lastError || ''));
      const normalized = {
        provider: byTs.size ? 'geckoterminal' : (backfillResult.provider || null),
        poolAddress: resolvedPool,
        bars: [...byTs.values()].sort((a, b) => a.timestamp - b.timestamp).slice(-bars),
        error: byTs.size ? null : (lastError || backfillResult.error || 'no_ohlcv'),
        reason: byTs.size ? null : (backfillResult.reason || normalizeMarketDataReason({
          error: lastError || backfillResult.error || 'no_ohlcv',
          rateLimited
        }) || MARKET_DATA_REASON.UNKNOWN_DATA),
        rateLimited,
        fetchedAt: Date.now(),
        cacheHit: false,
        source: 'shared-pool-client'
      };

      if (normalized.bars.length) {
        this.repository.upsertBars(tokenCa, resolvedPool, normalized.bars, normalized.provider || 'geckoterminal');
        this.repository.upsertPoolMapping(tokenCa, resolvedPool, normalized.provider || 'geckoterminal');
        await this.runtime.setCache(cacheKey, normalized, ttlMs);
        await this.runtime.setCache(
          `ohlcv-latest:${tokenCa}`,
          [...normalized.bars].sort((a, b) => b.timestamp - a.timestamp).slice(0, 20),
          ttlMs
        );
      }

      return normalized;
    }, { distributed: true, cacheKey });

    return providerResult;
  }

  async fetchRecentOhlcvByPool(tokenCa, poolAddress, options = {}) {
    const signalTsSec = Number(options.signalTsSec || Math.floor(Date.now() / 1000));
    const bars = Number(options.bars || this.config.evaluator.maxHistoricalBars);
    const beforeTimestamps = Array.isArray(options.beforeTimestamps) && options.beforeTimestamps.length
      ? options.beforeTimestamps
      : (Array.isArray(options.windows) && options.windows.length ? options.windows : [signalTsSec + bars * 60]);

    const normalizedWindows = beforeTimestamps
      .map((value) => (value == null ? signalTsSec + bars * 60 : Number(value)))
      .filter((value) => Number.isFinite(value) && value > 0);

    return this.fetchOhlcvWindow({
      tokenCa,
      poolAddress,
      signalTsSec,
      bars,
      startTs: signalTsSec,
      endTs: signalTsSec + bars * 60,
    }, {
      minBars: options.minBars ?? 1,
      windows: normalizedWindows.length ? normalizedWindows : [signalTsSec + bars * 60],
      limit: options.limit,
      cacheTtlMs: options.cacheTtlMs,
    });
  }

  async close() {
    await this.runtime.close();
  }
}

export function createSharedPoolOhlcvClient(config = autonomyConfig, options = {}) {
  return new SharedPoolOhlcvClient(config, options);
}

export default SharedPoolOhlcvClient;
