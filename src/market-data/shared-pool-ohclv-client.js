import autonomyConfig from '../config/autonomy-config.js';
import { KlineRepository } from './kline-repository.js';
import { PoolResolver } from './pool-resolver.js';
import { MarketDataBackfillService } from './market-data-backfill-service.js';
import { MARKET_DATA_REASON, SharedMarketRuntime, isMarketDataFlagEnabled, normalizeMarketDataReason } from './shared-market-runtime.js';
import { normalizeUnixTimestampSec } from '../utils/time-normalization.js';
import { execFile } from 'child_process';
import fs from 'fs';
import path from 'path';
import { promisify } from 'util';

const execFileAsync = promisify(execFile);

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

function normalizeGmgnKlineBars(list = [], startTs = 0, endTs = Number.MAX_SAFE_INTEGER) {
  return list.map((item) => {
    const timestamp = Math.floor(Number(item?.time || 0) / 1000);
    return {
      timestamp,
      open: Number(item?.open),
      high: Number(item?.high),
      low: Number(item?.low),
      close: Number(item?.close),
      volume: Number(item?.volume || 0)
    };
  }).filter((bar) => (
    Number.isFinite(bar.timestamp)
    && Number.isFinite(bar.open)
    && Number.isFinite(bar.high)
    && Number.isFinite(bar.low)
    && Number.isFinite(bar.close)
    && bar.timestamp >= startTs
    && bar.timestamp <= endTs
  ));
}

function countNonZeroVolumeBars(bars = []) {
  return bars.reduce((count, bar) => count + (Number(bar?.volume || 0) > 0 ? 1 : 0), 0);
}

function resolveGmgnCliPath() {
  const localPath = path.join(process.cwd(), 'node_modules', '.bin', process.platform === 'win32' ? 'gmgn-cli.cmd' : 'gmgn-cli');
  if (fs.existsSync(localPath)) return localPath;
  return 'gmgn-cli';
}

function envNumber(name, defaultValue) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) && value > 0 ? value : defaultValue;
}

export function normalizeMarketDataTimestampSec(value, fallbackSec = Math.floor(Date.now() / 1000)) {
  return normalizeUnixTimestampSec(value, fallbackSec);
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
    this.gmgnKlineFallbackEnabled = options.gmgnKlineFallbackEnabled ?? isMarketDataFlagEnabled('MARKET_DATA_GMGN_KLINE_FALLBACK', true);
    this.gmgnKlineFetcher = options.gmgnKlineFetcher || null;
    this.gmgnCliPath = options.gmgnCliPath || resolveGmgnCliPath();
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

  async fetchGmgnKlineWindow({ tokenCa, startTs, endTs, minBars = 1, bars = 20, cacheKey = '', ttlMs = 60_000 } = {}) {
    if (!this.gmgnKlineFallbackEnabled) {
      return { ok: false, provider: 'gmgn', bars: [], error: 'gmgn_kline_fallback_disabled', rateLimited: false };
    }
    if (!tokenCa) {
      return { ok: false, provider: 'gmgn', bars: [], error: 'missing_token', rateLimited: false };
    }
    if (!this.gmgnKlineFetcher && !process.env.GMGN_API_KEY) {
      return { ok: false, provider: 'gmgn', bars: [], error: 'gmgn_api_key_missing', rateLimited: false };
    }

    const requestKey = `gmgn-kline:${cacheKey || `${tokenCa}:${startTs}:${endTs}:${bars}`}`;
    const cached = await this.runtime.getCache(requestKey);
    if (cached) return { ...cached, cacheHit: true };

    const execute = async () => {
      try {
        let raw;
        if (this.gmgnKlineFetcher) {
          raw = await this.gmgnKlineFetcher({ tokenCa, startTs, endTs, bars });
        } else {
          const cooldownRemainingMs = await this.runtime.getSharedCooldown('gmgn-kline');
          if (cooldownRemainingMs > 0) {
            return {
              ok: false,
              provider: 'gmgn',
              bars: [],
              error: 'gmgn_cooldown_active',
              reason: MARKET_DATA_REASON.COOLDOWN_ACTIVE,
              rateLimited: true,
              fetchedAt: Date.now(),
              cooldownRemainingMs,
              cacheHit: false,
              source: 'gmgn-market-kline',
              priceUnit: 'USD_PER_TOKEN',
              volumeUnit: 'USD',
            };
          }
          await this.runtime.throttle('gmgn-kline', {
            requestsPerSecond: envNumber('MARKET_DATA_GMGN_KLINE_RPS', 0.25),
            burstCapacity: envNumber('MARKET_DATA_GMGN_KLINE_BURST', 1),
          });
          const { stdout } = await execFileAsync(this.gmgnCliPath, [
            'market',
            'kline',
            '--chain',
            'sol',
            '--address',
            tokenCa,
            '--resolution',
            '1m',
            '--from',
            String(startTs),
            '--to',
            String(endTs),
            '--raw',
          ], {
            env: process.env,
            timeout: 20_000,
            maxBuffer: 2 * 1024 * 1024,
          });
          raw = JSON.parse(String(stdout || '{}'));
        }

        const list = Array.isArray(raw?.list) ? raw.list : (Array.isArray(raw) ? raw : []);
        const normalizedBars = normalizeGmgnKlineBars(list, startTs, endTs)
          .sort((a, b) => a.timestamp - b.timestamp)
          .slice(-bars);
        const result = {
          ok: normalizedBars.length >= minBars,
          provider: 'gmgn',
          poolAddress: null,
          bars: normalizedBars,
          error: normalizedBars.length >= minBars ? null : 'gmgn_no_ohlcv',
          reason: normalizedBars.length >= minBars ? null : MARKET_DATA_REASON.UNKNOWN_DATA,
          rateLimited: false,
          fetchedAt: Date.now(),
          cacheHit: false,
          source: 'gmgn-market-kline',
          priceUnit: 'USD_PER_TOKEN',
          volumeUnit: 'USD',
        };
        await this.runtime.setCache(requestKey, result, ttlMs);
        return result;
      } catch (error) {
        const message = String(error?.message || error || 'gmgn_kline_failed');
        const rateLimited = /429|rate.?limit|RATE_LIMIT/i.test(message);
        if (rateLimited) {
          await this.runtime.setSharedCooldown('gmgn-kline', 60_000);
        }
        return {
          ok: false,
          provider: 'gmgn',
          bars: [],
          error: rateLimited ? 'gmgn_rate_limited' : message.slice(0, 180),
          reason: normalizeMarketDataReason({ error: message, rateLimited }) || MARKET_DATA_REASON.UPSTREAM_UNAVAILABLE,
          rateLimited,
          fetchedAt: Date.now(),
          cacheHit: false,
          source: 'gmgn-market-kline',
          priceUnit: 'USD_PER_TOKEN',
          volumeUnit: 'USD',
        };
      }
    };

    return this.runtime.runSingleFlight(`fetch:${requestKey}`, execute, {
      distributed: true,
      cacheKey: requestKey,
    });
  }

  async fetchOhlcvWindow({ tokenCa, signalTsSec: rawSignalTsSec = Math.floor(Date.now() / 1000), poolAddress = null, bars = this.config.evaluator.maxHistoricalBars, startTs = null, endTs = null } = {}, options = {}) {
    const signalTsSec = normalizeMarketDataTimestampSec(rawSignalTsSec);
    const windowStart = startTs == null ? signalTsSec : normalizeMarketDataTimestampSec(startTs, signalTsSec);
    const windowEnd = endTs == null ? signalTsSec + bars * 60 : normalizeMarketDataTimestampSec(endTs, signalTsSec + bars * 60);
    const minBars = options.minBars ?? 1;

    const cached = this.getCachedBars(tokenCa, windowStart, windowEnd, minBars);
    if (cached) {
      const cachedNonZeroVolumeBars = countNonZeroVolumeBars(cached.bars);
      const cacheNeedsVolumeFallback = Boolean(options.preferGmgnKlineWithVolume)
        && cachedNonZeroVolumeBars < (options.minNonzeroVolumeBars ?? 1);
      if (!cacheNeedsVolumeFallback) {
        return cached;
      }
    }

    if (!this.sharedOhlcvEnabled && options.legacyFetcher) {
      return options.legacyFetcher({ tokenCa, signalTsSec, poolAddress, bars, startTs, endTs });
    }

    let backfillResult = {
      provider: null,
      poolAddress,
      bars: [],
      error: options.skipBackfill ? 'backfill_skipped' : null,
      reason: null,
      rateLimited: false,
      cacheHit: false,
    };
    if (!options.skipBackfill) {
      backfillResult = await this.backfillWindow({
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
    }

    const resolvedPool = poolAddress || backfillResult.poolAddress || (await this.resolvePool(tokenCa)).poolAddress;
    if (!resolvedPool) {
      const gmgnNoPoolFallback = await this.fetchGmgnKlineWindow({
        tokenCa,
        startTs: windowStart,
        endTs: windowEnd,
        minBars,
        bars,
        cacheKey: `no-pool:${tokenCa}:${windowStart}:${windowEnd}:${bars}`,
        ttlMs: options.cacheTtlMs ?? 60 * 1000,
      });
      if ((gmgnNoPoolFallback.bars?.length || 0) >= minBars) {
        return {
          provider: 'gmgn',
          poolAddress: null,
          bars: gmgnNoPoolFallback.bars,
          error: null,
          reason: null,
          rateLimited: Boolean(gmgnNoPoolFallback.rateLimited),
          fetchedAt: Date.now(),
          cacheHit: Boolean(gmgnNoPoolFallback.cacheHit),
          source: 'shared-pool-client',
          priceUnit: 'USD_PER_TOKEN',
          volumeUnit: 'USD',
          fallbackProvider: 'gmgn',
        };
      }
      return {
        provider: backfillResult.provider || null,
        poolAddress: null,
        bars: backfillResult.bars || [],
        error: backfillResult.error || 'no_pool',
        reason: backfillResult.reason || normalizeMarketDataReason({ error: backfillResult.error || 'no_pool', rateLimited: false }) || MARKET_DATA_REASON.NO_POOL,
        rateLimited: Boolean(gmgnNoPoolFallback.rateLimited),
        fetchedAt: Date.now(),
        cacheHit: false,
        source: 'shared-pool-client',
        fallbackProvider: 'gmgn',
        fallbackError: gmgnNoPoolFallback.error || null
      };
    }

    const cacheKey = `ohlcv:${tokenCa}:${resolvedPool}:${windowStart}:${windowEnd}:${bars}`;
    const ttlMs = options.cacheTtlMs ?? 60 * 1000;
    const cachedProviderResult = await this.runtime.getCache(cacheKey);
    if (cachedProviderResult) {
      return { ...cachedProviderResult, cacheHit: true };
    }

    const windows = (options.windows || [windowEnd, signalTsSec + 600, signalTsSec + 3600])
      .map((value) => normalizeMarketDataTimestampSec(value, windowEnd))
      .filter((value) => Number.isFinite(value) && value > 0);
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
      let gmgnFallback = null;
      let finalBars = [...byTs.values()].sort((a, b) => a.timestamp - b.timestamp).slice(-bars);
      let finalProvider = byTs.size ? 'geckoterminal' : (backfillResult.provider || null);
      let finalError = byTs.size ? null : (lastError || backfillResult.error || 'no_ohlcv');
      let finalReason = byTs.size ? null : (backfillResult.reason || normalizeMarketDataReason({
        error: lastError || backfillResult.error || 'no_ohlcv',
        rateLimited
      }) || MARKET_DATA_REASON.UNKNOWN_DATA);

      const nonZeroVolumeBars = countNonZeroVolumeBars(finalBars);
      const preferGmgnForVolume = Boolean(options.preferGmgnKlineWithVolume)
        && finalProvider === 'geckoterminal'
        && finalBars.length >= minBars
        && nonZeroVolumeBars < (options.minNonzeroVolumeBars ?? 1);

      if (finalBars.length < minBars || preferGmgnForVolume) {
        gmgnFallback = await this.fetchGmgnKlineWindow({
          tokenCa,
          startTs: windowStart,
          endTs: windowEnd,
          minBars,
          bars,
          cacheKey,
          ttlMs,
        });
        if ((gmgnFallback.bars?.length || 0) >= minBars) {
          finalBars = gmgnFallback.bars;
          finalProvider = 'gmgn';
          finalError = null;
          finalReason = null;
        } else if (!finalError && !preferGmgnForVolume && gmgnFallback.error) {
          finalError = gmgnFallback.error;
          finalReason = gmgnFallback.reason || MARKET_DATA_REASON.UNKNOWN_DATA;
        }
      }

      const normalized = {
        provider: finalProvider,
        poolAddress: resolvedPool,
        bars: finalBars,
        error: finalError,
        reason: finalReason,
        rateLimited: rateLimited || Boolean(gmgnFallback?.rateLimited),
        fetchedAt: Date.now(),
        cacheHit: false,
        source: 'shared-pool-client',
        priceUnit: finalProvider === 'gmgn' ? 'USD_PER_TOKEN' : undefined,
        volumeUnit: finalProvider === 'gmgn' ? 'USD' : undefined,
        fallbackProvider: gmgnFallback ? 'gmgn' : null,
        fallbackReason: gmgnFallback && preferGmgnForVolume ? 'gecko_zero_volume' : null,
        fallbackError: gmgnFallback && !(gmgnFallback.bars?.length >= minBars) ? gmgnFallback.error : null
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
    const signalTsSec = normalizeMarketDataTimestampSec(options.signalTsSec);
    const bars = Number(options.bars || this.config.evaluator.maxHistoricalBars);
    const beforeTimestamps = Array.isArray(options.beforeTimestamps) && options.beforeTimestamps.length
      ? options.beforeTimestamps
      : (Array.isArray(options.windows) && options.windows.length ? options.windows : [signalTsSec + bars * 60]);

    const normalizedWindows = beforeTimestamps
      .map((value) => normalizeMarketDataTimestampSec(value, signalTsSec + bars * 60))
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
