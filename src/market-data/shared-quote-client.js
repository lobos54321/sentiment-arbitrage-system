import autonomyConfig from '../config/autonomy-config.js';
import { SharedMarketRuntime, isMarketDataFlagEnabled } from './shared-market-runtime.js';

const SOL_MINT = 'So11111111111111111111111111111111111111112';

function parseDexPairPrice(pair = {}) {
  const priceUsd = parseFloat(pair.priceUsd || 0);
  const priceNative = parseFloat(pair.priceNative || 0);
  return {
    priceUsd: priceUsd > 0 ? priceUsd : null,
    priceNative: priceNative > 0 ? priceNative : null,
    marketCap: pair.marketCap || null,
    pair
  };
}

function classifyQuoteResponse(data = {}) {
  const responseMessage = String(data.error || data.message || '');
  if (data.errorCode === 'ROUTE_NOT_FOUND' || data.errorCode === 'COULD_NOT_FIND_ANY_ROUTE' || /no route|could not find/i.test(responseMessage)) {
    return 'no_route';
  }
  if (data.errorCode === 'TOKEN_NOT_TRADABLE' || /not tradable/i.test(responseMessage)) {
    return 'token_not_tradable';
  }
  if (!data.outAmount) {
    return 'null_response';
  }
  return null;
}

export class SharedQuoteClient {
  constructor(config = autonomyConfig, options = {}) {
    this.config = config;
    this.runtime = options.runtime || new SharedMarketRuntime({ namespace: 'market-data:quotes' });
    this.jupiterApiKey = options.jupiterApiKey || process.env.JUPITER_API_KEY || '';
    this.sharedQuotesEnabled = options.sharedQuotesEnabled ?? isMarketDataFlagEnabled('MARKET_DATA_SHARED_QUOTES', true);
  }

  async getSwapQuote({ inputMint, amount, outputMint = SOL_MINT } = {}, options = {}) {
    if (!(inputMint && Number(amount) > 0)) {
      return {
        ok: false,
        provider: 'jupiter-ultra',
        quote: null,
        error: 'missing_quote_inputs',
        reason: 'missing_quote_inputs',
        rateLimited: false,
        fetchedAt: Date.now(),
        source: 'shared-quote-client'
      };
    }

    if (!this.sharedQuotesEnabled && options.legacyFetcher) {
      return options.legacyFetcher(inputMint, amount, outputMint);
    }

    const params = new URLSearchParams({
      inputMint,
      outputMint,
      amount: String(amount)
    });
    const headers = {};
    if (this.jupiterApiKey) headers['x-api-key'] = this.jupiterApiKey;

    const response = await this.runtime.fetchJson(
      `https://api.jup.ag/ultra/v1/order?${params.toString()}`,
      {
        provider: 'jupiter-ultra',
        source: 'JUPITER',
        requestKey: `quote:${inputMint}:${outputMint}:${amount}`,
        cacheTtlMs: options.cacheTtlMs ?? 1500,
        cooldownMs: options.cooldownMs ?? 5000,
        limiter: { requestsPerSecond: 6, burstCapacity: 2 },
        timeout: options.timeout ?? 5000,
        maxRetries: 1,
        initialDelay: 500,
        maxDelay: 2000,
        headers,
        retryOn403: false
      }
    );

    if (!response.ok) {
      const reason = response.rateLimited ? 'rate_limited_429' : (response.error || 'quote_request_failed');
      return {
        ok: false,
        provider: 'jupiter-ultra',
        quote: null,
        error: response.error || null,
        reason,
        rateLimited: response.rateLimited,
        fetchedAt: response.fetchedAt || Date.now(),
        source: 'shared-quote-client'
      };
    }

    const quote = response.data;
    const reason = classifyQuoteResponse(quote);
    return {
      ok: !reason,
      provider: 'jupiter-ultra',
      quote,
      error: reason,
      reason,
      rateLimited: false,
      fetchedAt: response.fetchedAt || Date.now(),
      source: 'shared-quote-client',
      cacheHit: response.cacheHit || false
    };
  }

  async getBestDexPair(tokenCA, options = {}) {
    if (!tokenCA) {
      return {
        ok: false,
        provider: 'dexscreener',
        pair: null,
        error: 'missing_token',
        rateLimited: false,
        fetchedAt: Date.now(),
        source: 'shared-quote-client'
      };
    }

    if (!this.sharedQuotesEnabled && options.legacyFetcher) {
      return options.legacyFetcher(tokenCA);
    }

    const response = await this.runtime.fetchJson(
      `https://api.dexscreener.com/latest/dex/tokens/${tokenCA}`,
      {
        provider: 'dexscreener',
        source: 'DEXSCREENER',
        requestKey: `dex-pair:${tokenCA}`,
        cacheTtlMs: options.cacheTtlMs ?? 5000,
        cooldownMs: options.cooldownMs ?? 3000,
        limiter: { requestsPerSecond: 0.75, burstCapacity: 1 },
        timeout: options.timeout ?? 10000,
        maxRetries: 3,
        initialDelay: 1000,
        maxDelay: 8000
      }
    );

    if (!response.ok) {
      return {
        ok: false,
        provider: 'dexscreener',
        pair: null,
        error: response.error || 'dex_request_failed',
        rateLimited: response.rateLimited,
        fetchedAt: response.fetchedAt || Date.now(),
        source: 'shared-quote-client'
      };
    }

    const pairs = response.data?.pairs || [];
    const solPairs = pairs.filter((pair) => pair.chainId === 'solana');
    const bestPair = (solPairs.length ? solPairs : pairs)
      .filter((pair) => pair.baseToken?.address === tokenCA || pair.quoteToken?.address === tokenCA)
      .sort((a, b) => Number(b.liquidity?.usd || 0) - Number(a.liquidity?.usd || 0))[0] || null;

    return {
      ok: Boolean(bestPair),
      provider: 'dexscreener',
      pair: bestPair,
      error: bestPair ? null : 'no_pair',
      rateLimited: false,
      fetchedAt: response.fetchedAt || Date.now(),
      source: 'shared-quote-client',
      cacheHit: response.cacheHit || false
    };
  }

  async getQuoteWithDexFallback({ tokenCA, amount, outputMint = SOL_MINT } = {}, options = {}) {
    const quoteResult = await this.getSwapQuote({ inputMint: tokenCA, amount, outputMint }, options);
    if (quoteResult.ok) {
      return {
        provider: quoteResult.provider,
        quote: quoteResult.quote,
        dexPair: null,
        priceNative: null,
        priceUsd: null,
        marketCap: null,
        error: null,
        reason: null,
        rateLimited: false,
        fetchedAt: quoteResult.fetchedAt,
        source: 'shared-quote-client'
      };
    }

    if (quoteResult.reason === 'rate_limited_429') {
      return {
        provider: quoteResult.provider,
        quote: null,
        dexPair: null,
        priceNative: null,
        priceUsd: null,
        marketCap: null,
        error: quoteResult.error,
        reason: quoteResult.reason,
        rateLimited: true,
        fetchedAt: quoteResult.fetchedAt,
        source: 'shared-quote-client'
      };
    }

    const dexResult = await this.getBestDexPair(tokenCA, options);
    const parsed = dexResult.ok ? parseDexPairPrice(dexResult.pair) : { priceNative: null, priceUsd: null, marketCap: null, pair: null };

    return {
      provider: dexResult.ok ? 'dex-fallback' : quoteResult.provider,
      quote: null,
      dexPair: dexResult.pair || null,
      priceNative: parsed.priceNative,
      priceUsd: parsed.priceUsd,
      marketCap: parsed.marketCap,
      error: dexResult.ok ? null : (dexResult.error || quoteResult.error),
      reason: quoteResult.reason || dexResult.error || null,
      rateLimited: Boolean(dexResult.rateLimited),
      fetchedAt: dexResult.fetchedAt || quoteResult.fetchedAt || Date.now(),
      source: 'shared-quote-client'
    };
  }

  async close() {
    await this.runtime.close();
  }
}

export function createSharedQuoteClient(config = autonomyConfig, options = {}) {
  return new SharedQuoteClient(config, options);
}

export { SOL_MINT, classifyQuoteResponse };
export default SharedQuoteClient;
