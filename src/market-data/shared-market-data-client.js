import autonomyConfig from '../config/autonomy-config.js';
import { SharedQuoteClient } from './shared-quote-client.js';
import { SharedPoolOhlcvClient } from './shared-pool-ohclv-client.js';

export class SharedMarketDataClient {
  constructor(config = autonomyConfig, options = {}) {
    this.config = config;
    this.poolOhlcvClient = options.poolOhlcvClient || new SharedPoolOhlcvClient(config, options);
    this.quoteClient = options.quoteClient || new SharedQuoteClient(config, options);
  }

  async resolvePool(tokenCa, options = {}) {
    const result = await this.poolOhlcvClient.resolvePool(tokenCa, options);
    return {
      ...result,
      provenance: {
        capability: 'pool-resolution',
        provider: result.provider || null,
        source: result.source || 'shared-market-data-client',
        reason: result.error || null,
        poolAddress: result.poolAddress || null,
      },
    };
  }

  async fetchOhlcvWindow(params = {}, options = {}) {
    const result = await this.poolOhlcvClient.fetchOhlcvWindow(params, options);
    return {
      ...result,
      provenance: {
        capability: 'ohlcv',
        provider: result.provider || null,
        source: result.source || 'shared-market-data-client',
        reason: result.error || null,
        poolAddress: result.poolAddress || null,
        barCount: Array.isArray(result.bars) ? result.bars.length : 0,
      },
    };
  }

  async fetchRecentOhlcvByPool(tokenCa, poolAddress, options = {}) {
    const result = await this.poolOhlcvClient.fetchRecentOhlcvByPool(tokenCa, poolAddress, options);
    return {
      ...result,
      provenance: {
        capability: 'ohlcv-recent',
        provider: result.provider || null,
        source: result.source || 'shared-market-data-client',
        reason: result.error || null,
        poolAddress: result.poolAddress || poolAddress || null,
        barCount: Array.isArray(result.bars) ? result.bars.length : 0,
      },
    };
  }

  async getQuoteWithDexFallback(params = {}, options = {}) {
    const result = await this.quoteClient.getQuoteWithDexFallback(params, options);
    return {
      ...result,
      provenance: {
        capability: 'quote',
        provider: result.provider || null,
        source: result.source || 'shared-market-data-client',
        reason: result.reason || result.error || null,
        usedDexFallback: result.provider === 'dex-fallback',
      },
    };
  }

  async getLivePriceSnapshot({ tokenCA, amount, outputMint, includeDexPair = true } = {}, options = {}) {
    const result = await this.getQuoteWithDexFallback({ tokenCA, amount, outputMint }, options);
    return {
      provider: result.provider,
      source: result.source || 'shared-market-data-client',
      fetchedAt: result.fetchedAt || Date.now(),
      rateLimited: Boolean(result.rateLimited),
      error: result.error || null,
      reason: result.reason || null,
      quote: result.quote || null,
      dexPair: includeDexPair ? (result.dexPair || null) : null,
      priceNative: result.priceNative ?? null,
      priceUsd: result.priceUsd ?? null,
      marketCap: result.marketCap ?? null,
    };
  }

  async close() {
    await Promise.allSettled([
      this.poolOhlcvClient?.close?.(),
      this.quoteClient?.close?.(),
    ]);
  }
}

export function createSharedMarketDataClient(config = autonomyConfig, options = {}) {
  return new SharedMarketDataClient(config, options);
}

export default SharedMarketDataClient;
