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
    return this.poolOhlcvClient.resolvePool(tokenCa, options);
  }

  async fetchOhlcvWindow(params = {}, options = {}) {
    return this.poolOhlcvClient.fetchOhlcvWindow(params, options);
  }

  async fetchRecentOhlcvByPool(tokenCa, poolAddress, options = {}) {
    return this.poolOhlcvClient.fetchRecentOhlcvByPool(tokenCa, poolAddress, options);
  }

  async getQuoteWithDexFallback(params = {}, options = {}) {
    return this.quoteClient.getQuoteWithDexFallback(params, options);
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
