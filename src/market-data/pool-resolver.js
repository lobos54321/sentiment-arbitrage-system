import { fetchWithRetry } from '../utils/fetch-with-retry.js';

export class PoolResolver {
  constructor({ repository, timeoutMs = 15000 } = {}) {
    this.repository = repository;
    this.timeoutMs = timeoutMs;
    this.memoryCache = new Map();
  }

  async resolvePool(tokenCa) {
    if (!tokenCa) {
      return { poolAddress: null, provider: null, error: 'missing_token' };
    }

    const cached = this.memoryCache.get(tokenCa) || this.repository?.getPoolMapping(tokenCa);
    if (cached?.pool_address || cached?.poolAddress) {
      const poolAddress = cached.pool_address || cached.poolAddress;
      this.memoryCache.set(tokenCa, { poolAddress, provider: cached.provider || 'cache' });
      return { poolAddress, provider: cached.provider || 'cache', error: null };
    }

    try {
      const response = await fetchWithRetry(`https://api.dexscreener.com/latest/dex/tokens/${tokenCa}`, {
        source: 'DEXSCREENER',
        timeout: this.timeoutMs,
        maxRetries: 3,
        initialDelay: 1000,
        silent: true,
        headers: { accept: 'application/json' }
      });
      if (response?.error) {
        return { poolAddress: null, provider: null, error: response.error };
      }

      const pairs = response?.pairs || [];
      const solPairs = pairs.filter((pair) => pair.chainId === 'solana');
      const bestPair = (solPairs.length ? solPairs : pairs)
        .sort((a, b) => (Number(b.liquidity?.usd || 0) - Number(a.liquidity?.usd || 0)))[0] || null;

      if (!bestPair?.pairAddress) {
        return { poolAddress: null, provider: null, error: 'no_pool' };
      }

      const result = { poolAddress: bestPair.pairAddress, provider: 'dexscreener', error: null };
      this.memoryCache.set(tokenCa, result);
      this.repository?.upsertPoolMapping(tokenCa, bestPair.pairAddress, result.provider);
      return result;
    } catch (error) {
      return { poolAddress: null, provider: null, error: error.message };
    }
  }
}

export default PoolResolver;
