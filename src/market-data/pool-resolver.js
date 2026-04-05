import { MARKET_DATA_REASON, SharedMarketRuntime, normalizeMarketDataReason } from './shared-market-runtime.js';

function normalizePoolAddress(poolAddress) {
  if (!poolAddress) return null;
  return String(poolAddress).replace(/^solana_/, '').trim() || null;
}

export class PoolResolver {
  constructor({ repository, timeoutMs = 15000, runtime = null } = {}) {
    this.repository = repository;
    this.timeoutMs = timeoutMs;
    this.memoryCache = new Map();
    this.runtime = runtime || new SharedMarketRuntime({ namespace: 'market-data:pool-resolver' });
  }

  #remember(tokenCa, poolAddress, provider) {
    const normalizedPoolAddress = normalizePoolAddress(poolAddress);
    if (!tokenCa || !normalizedPoolAddress) return null;
    const result = { poolAddress: normalizedPoolAddress, provider, error: null };
    this.memoryCache.set(tokenCa, result);
    if (provider && provider !== 'memory') {
      this.repository?.upsertPoolMapping(tokenCa, normalizedPoolAddress, provider);
    }
    return result;
  }

  async #fetchGeckoTerminalPool(tokenCa) {
    const response = await this.runtime.fetchJson(`https://api.geckoterminal.com/api/v2/networks/solana/tokens/${tokenCa}/pools?page=1`, {
      provider: 'geckoterminal',
      source: 'GECKOTERMINAL',
      requestKey: `pool:gecko:${tokenCa}`,
      cacheTtlMs: 15 * 60 * 1000,
      cooldownMs: 30000,
      limiter: { requestsPerSecond: 0.2, burstCapacity: 1 },
      timeout: this.timeoutMs,
      maxRetries: 2,
      initialDelay: 2500,
      maxDelay: 20000,
      silent: true,
      headers: { accept: 'application/json' }
    });

    if (!response?.ok) {
      return {
        poolAddress: null,
        provider: null,
        error: response?.error || 'request_failed',
        reason: response?.reason || normalizeMarketDataReason({
          error: response?.error || 'request_failed',
          status: response?.status,
          rateLimited: response?.rateLimited
        }) || MARKET_DATA_REASON.UPSTREAM_UNAVAILABLE,
        rateLimited: Boolean(response?.rateLimited)
      };
    }

    const pool = response?.data?.data?.[0] || response?.data?.[0] || null;
    const poolAddress = normalizePoolAddress(pool?.attributes?.address || pool?.id);
    if (!poolAddress) {
      return { poolAddress: null, provider: null, error: 'no_pool', reason: MARKET_DATA_REASON.NO_POOL, rateLimited: false };
    }

    return this.#remember(tokenCa, poolAddress, 'geckoterminal');
  }

  async #fetchDexScreenerPool(tokenCa) {
    const response = await this.runtime.fetchJson(`https://api.dexscreener.com/latest/dex/tokens/${tokenCa}`, {
      provider: 'dexscreener',
      source: 'DEXSCREENER',
      requestKey: `pool:dex:${tokenCa}`,
      cacheTtlMs: 5 * 60 * 1000,
      cooldownMs: 5000,
      limiter: { requestsPerSecond: 0.75, burstCapacity: 1 },
      timeout: this.timeoutMs,
      maxRetries: 3,
      initialDelay: 1000,
      silent: true,
      headers: { accept: 'application/json' }
    });

    if (!response?.ok) {
      return { poolAddress: null, provider: null, error: response?.error || 'request_failed' };
    }

    const pairs = response?.data?.pairs || [];
    const solPairs = pairs.filter((pair) => pair.chainId === 'solana');
    const bestPair = (solPairs.length ? solPairs : pairs)
      .sort((a, b) => (Number(b.liquidity?.usd || 0) - Number(a.liquidity?.usd || 0)))[0] || null;

    if (!bestPair?.pairAddress) {
      return { poolAddress: null, provider: null, error: 'no_pool', reason: MARKET_DATA_REASON.NO_POOL, rateLimited: false };
    }

    return this.#remember(tokenCa, bestPair.pairAddress, 'dexscreener');
  }

  async resolvePool(tokenCa) {
    if (!tokenCa) {
      return { poolAddress: null, provider: null, error: 'missing_token', reason: MARKET_DATA_REASON.MISSING_INPUTS, rateLimited: false };
    }

    const memoryHit = this.memoryCache.get(tokenCa);
    if (memoryHit?.poolAddress) {
      return { poolAddress: memoryHit.poolAddress, provider: 'memory', error: null };
    }

    const mapped = this.repository?.getPoolMapping(tokenCa);
    const mappedPool = normalizePoolAddress(mapped?.pool_address || mapped?.poolAddress);
    if (mappedPool) {
      return this.#remember(tokenCa, mappedPool, mapped.provider || 'pool_mapping');
    }

    const cursorHint = this.repository?.getLatestCursorPoolHint(tokenCa);
    const cursorPool = normalizePoolAddress(cursorHint?.pool_address);
    if (cursorPool) {
      return this.#remember(tokenCa, cursorPool, 'cursor');
    }

    const tradeHint = this.repository?.getLikelyTradePoolHint(tokenCa);
    const tradePool = normalizePoolAddress(tradeHint?.pool_address);
    if (tradePool) {
      return this.#remember(tokenCa, tradePool, 'helius_trades');
    }

    const klineHint = this.repository?.getLatestKlinePoolHint(tokenCa);
    const klinePool = normalizePoolAddress(klineHint?.pool_address);
    if (klinePool) {
      return this.#remember(tokenCa, klinePool, 'kline_1m');
    }

    try {
      const geckoResult = await this.#fetchGeckoTerminalPool(tokenCa);
      if (geckoResult.poolAddress) {
        return geckoResult;
      }

      const dexResult = await this.#fetchDexScreenerPool(tokenCa);
      if (dexResult.poolAddress) {
        return dexResult;
      }

      return {
        poolAddress: null,
        provider: null,
        error: dexResult.error || geckoResult.error || 'no_pool'
      };
    } catch (error) {
      return { poolAddress: null, provider: null, error: error.message };
    }
  }
}

export default PoolResolver;
