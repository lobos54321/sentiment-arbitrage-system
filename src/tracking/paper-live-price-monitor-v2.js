/**
 * Live Price Monitor V2
 *
 * 统一价格源：Jupiter Ultra Quote 优先，DexScreener fallback。
 * - Ultra /order 提供可交易卖出报价（SOL per token）
 * - 非 429 的 miss/no-route/null-response 立即尝试 Dex fallback
 * - Dex 定时轮询继续补充 MC，并在缺失时兜底价格
 * - 保持 priceCache / price-update 事件形状兼容下游消费者
 */

import { EventEmitter } from 'events';
import axios from 'axios';
import { createClient } from 'redis';

const SOL_MINT = 'So11111111111111111111111111111111111111112';
const PAPER_MARK_QUOTE_FRACTION = 0.02;

function getPaperMarkQuoteAmount(tokenAmountRaw, tokenDecimals = 6) {
  const normalizedAmount = Math.trunc(Number(tokenAmountRaw || 0));
  const normalizedDecimals = Math.max(0, Math.trunc(Number(tokenDecimals || 0)));

  if (!(normalizedAmount > 0)) {
    return 0;
  }

  const minQuoteAmount = Math.max(1, Math.trunc(10 ** normalizedDecimals));
  if (normalizedAmount <= minQuoteAmount) {
    return normalizedAmount;
  }

  return Math.min(
    normalizedAmount,
    Math.max(minQuoteAmount, Math.ceil(normalizedAmount * PAPER_MARK_QUOTE_FRACTION))
  );
}

export class LivePriceMonitorV2 extends EventEmitter {
  constructor(jupiterExecutor, options = {}) {
    super();

    this.jupiterExecutor = jupiterExecutor;

    // 价格缓存 Map<tokenCA, {price, timestamp, source, mc, ...}>
    this.priceCache = new Map();

    // 监控的 token 列表 Map<tokenCA, {tokenAmount, decimals}>
    this.watchList = new Map();

    // 轮询定时器
    this.priceInterval = null;
    this.dexInterval = null;

    // 配置
    this.priceIntervalMs = 1500;
    this.dexIntervalMs = 10000;
    this.jupiterApiKey = process.env.JUPITER_API_KEY || '';
    this.cacheFreshMs = 15000;
    this.cacheStaleMs = 30000;
    this.querySpacingMs = 100;
    this.rateLimitCooldownMs = 5000;
    this.redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
    this.redisEnabled = options.redisEnabled ?? (process.env.REDIS_ENABLED !== 'false');
    this.redisClient = null;
    this.redisConnectPromise = null;
    this.redisFailed = false;
    this.isRunning = false;

    // 统计
    this.stats = {
      quote_queries: 0,
      quote_hits_ultra: 0,
      quote_rate_limited_429: 0,
      quote_no_route: 0,
      quote_null_response: 0,
      quote_network_errors: 0,
      dex_fallback_queries: 0,
      dex_fallback_hits: 0,
      dex_fallback_failures: 0,
      dex_queries: 0,
      cache_misses: 0,
      cache_stale_reads: 0,
      errors: 0
    };

    // 防止 setInterval 回调叠加
    this._queryRunning = false;

    // 单 token 连续失败计数（只计真正失败，不计 429）
    this._tokenFailCounts = new Map();

    // 最近失败原因 / 限速冷却
    this._lastFailureByToken = new Map();
    this._rateLimitCooldownUntil = 0;

    console.log('📡 [LivePriceMonitorV2] 初始化 - Ultra 优先 + Dex fallback');
  }

  /**
   * 添加 token 到监控列表
   * @param {string} tokenCA
   * @param {number} tokenAmount - raw amount
   * @param {number} decimals
   */
  addToken(tokenCA, tokenAmount = 1e6, decimals = 6) {
    const existing = this.watchList.get(tokenCA);
    this.watchList.set(tokenCA, {
      tokenAmount: tokenAmount || existing?.tokenAmount || 1e6,
      decimals: decimals ?? existing?.decimals ?? 6
    });
    console.log(`📡 [LivePriceMonitorV2] 添加监控: ${tokenCA.substring(0, 8)}... | ${this.watchList.size} 个`);
  }

  /**
   * 移除 token
   */
  removeToken(tokenCA) {
    this.watchList.delete(tokenCA);
    this.priceCache.delete(tokenCA);
    this._tokenFailCounts.delete(tokenCA);
    this._lastFailureByToken.delete(tokenCA);
  }

  /**
   * 启动价格监控
   */
  start() {
    if (this.isRunning) return;
    this.isRunning = true;

    this._ensureRedisClient();
    this.priceInterval = setInterval(() => this._queryAllPrices(), this.priceIntervalMs);
    this.dexInterval = setInterval(() => this._queryDexScreener(), this.dexIntervalMs);

    console.log(`✅ [LivePriceMonitorV2] 启动 | Ultra: ${this.priceIntervalMs}ms | DexScreener: ${this.dexIntervalMs}ms`);
  }

  /**
   * 停止
   */
  async stop() {
    this.isRunning = false;
    if (this.priceInterval) {
      clearInterval(this.priceInterval);
      this.priceInterval = null;
    }
    if (this.dexInterval) {
      clearInterval(this.dexInterval);
      this.dexInterval = null;
    }
    await this._closeRedisClient();
    console.log(`⏹️  [LivePriceMonitorV2] 已停止 | 统计: ${JSON.stringify(this.getStats())}`);
  }

  /**
   * 获取 token 当前价格（保持向后兼容）
   */
  getPrice(tokenCA) {
    return this.priceCache.get(tokenCA) || null;
  }

  /**
   * 返回价格状态，供外部区分 fresh/stale/miss
   */
  getPriceStatus(tokenCA, options = {}) {
    const now = options.now || Date.now();
    const staleAfterMs = options.staleAfterMs || this.cacheStaleMs;
    const cached = this.priceCache.get(tokenCA);
    const lastFailure = this._lastFailureByToken.get(tokenCA) || null;

    if (!cached) {
      return {
        state: 'missing',
        cacheState: 'missing',
        source: null,
        ageMs: null,
        hasCache: false,
        lastFailureType: lastFailure?.type || null,
        lastFailureAt: lastFailure?.at || null,
        data: null
      };
    }

    const ageMs = now - cached.timestamp;
    const isFresh = ageMs < staleAfterMs;

    return {
      state: isFresh ? 'fresh' : 'stale',
      cacheState: isFresh ? 'fresh' : 'stale',
      source: cached.source,
      ageMs,
      hasCache: true,
      lastFailureType: cached.lastFailureType || lastFailure?.type || null,
      lastFailureAt: cached.lastFailureAt || lastFailure?.at || null,
      data: cached
    };
  }

  /**
   * 返回缓存；如果是 stale 会计数
   */
  getCachedPrice(tokenCA, options = {}) {
    const status = this.getPriceStatus(tokenCA, options);
    if (status.state === 'missing') {
      this.stats.cache_misses++;
      return null;
    }
    if (status.state === 'stale') {
      this.stats.cache_stale_reads++;
    }
    return status.data;
  }

  /**
   * 查询所有 token 的实时价格（串行，避免放大限速）
   */
  async _queryAllPrices() {
    if (this._queryRunning) return;
    if (this.watchList.size === 0) return;

    const now = Date.now();
    if (now < this._rateLimitCooldownUntil) {
      return;
    }

    this._queryRunning = true;
    const tokens = [...this.watchList.entries()];

    try {
      for (const [tokenCA, meta] of tokens) {
        const shouldContinue = await this._pollTokenPrice(tokenCA, meta);
        if (!shouldContinue) {
          break;
        }
        await this._sleep(this.querySpacingMs);
      }
    } finally {
      this._queryRunning = false;
    }
  }

  async _pollTokenPrice(tokenCA, { tokenAmount, decimals }) {
    this.stats.quote_queries++;

    try {
      const quoteResult = await this._getSwapQuote(tokenCA, tokenAmount);

      if (quoteResult.ok && quoteResult.quote?.outAmount) {
        this._tokenFailCounts.delete(tokenCA);
        this._clearTokenFailure(tokenCA);
        this._storeUltraQuote(tokenCA, quoteResult.quote, tokenAmount, decimals);
        return true;
      }

      if (quoteResult.reason === 'rate_limited_429') {
        this._markRateLimited(tokenCA);
        return false;
      }

      this._recordQuoteMiss(tokenCA, quoteResult.reason);
      await this._tryDexFallback(tokenCA, { reason: quoteResult.reason });
      return true;
    } catch (error) {
      this.stats.quote_network_errors++;
      this.stats.errors++;
      this._markTokenFailure(tokenCA, 'quote_network_error', error.message);
      await this._tryDexFallback(tokenCA, { reason: 'quote_network_error', error });
      return true;
    }
  }

  _storeUltraQuote(tokenCA, quote, tokenAmount, decimals) {
    const outSol = parseInt(quote.outAmount, 10) / 1e9;
    const actualTokenAmount = tokenAmount / Math.pow(10, decimals);
    const solPrice = actualTokenAmount > 0 ? outSol / actualTokenAmount : 0;
    const cached = this.priceCache.get(tokenCA);
    const timestamp = Date.now();

    const entry = {
      ...cached,
      nativePrice: solPrice,
      price: solPrice,
      outSol,
      timestamp,
      source: 'jupiter-quote',
      mc: cached?.mc || null,
      cacheState: 'fresh',
      lastFailureType: null,
      lastFailureAt: null
    };

    this.priceCache.set(tokenCA, entry);
    this.stats.quote_hits_ultra++;

    this.emit('price-update', {
      tokenCA,
      price: solPrice,
      outSol,
      mc: entry.mc,
      timestamp,
      source: 'jupiter-quote'
    });

    void this._publishRedisPrice(tokenCA, {
      price_sol: entry.price,
      price_usd: entry.usdPrice || cached?.usdPrice || null,
      source: 'jupiter-quote',
      timestamp: entry.timestamp,
      mc: entry.mc || null
    });
  }

  _recordQuoteMiss(tokenCA, reason) {
    if (reason === 'no_route') {
      this.stats.quote_no_route++;
    } else {
      this.stats.quote_null_response++;
    }
    this._markTokenFailure(tokenCA, reason, `Ultra miss: ${reason}`);
  }

  _markRateLimited(tokenCA) {
    this.stats.quote_rate_limited_429++;
    this.stats.errors++;
    this._rateLimitCooldownUntil = Date.now() + this.rateLimitCooldownMs;
    this._recordFailure(tokenCA, 'rate_limited_429', 'Jupiter Ultra API rate limited');

    if (this.stats.quote_rate_limited_429 <= 10 || this.stats.quote_rate_limited_429 % 25 === 0) {
      console.warn(`⚠️  [LivePriceMonitorV2] Ultra 429 限速，冷却 ${this.rateLimitCooldownMs}ms (累计 ${this.stats.quote_rate_limited_429} 次)`);
    }
  }

  _markTokenFailure(tokenCA, type, message) {
    const prev = this._tokenFailCounts.get(tokenCA) || 0;
    this._tokenFailCounts.set(tokenCA, prev + 1);
    this._recordFailure(tokenCA, type, message);
  }

  _recordFailure(tokenCA, type, message) {
    const failure = { type, message, at: Date.now() };
    this._lastFailureByToken.set(tokenCA, failure);

    const cached = this.priceCache.get(tokenCA);
    if (cached) {
      cached.lastFailureType = type;
      cached.lastFailureAt = failure.at;
      cached.cacheState = (Date.now() - cached.timestamp) < this.cacheStaleMs ? 'fresh' : 'stale';
      this.priceCache.set(tokenCA, cached);
    }
  }

  _clearTokenFailure(tokenCA) {
    this._lastFailureByToken.delete(tokenCA);
    const cached = this.priceCache.get(tokenCA);
    if (cached) {
      cached.lastFailureType = null;
      cached.lastFailureAt = null;
      cached.cacheState = 'fresh';
      this.priceCache.set(tokenCA, cached);
    }
  }

  /**
   * 获取 token 连续失败次数（只统计非 429）
   */
  getTokenFailCount(tokenCA) {
    return this._tokenFailCounts.get(tokenCA) || 0;
  }

  /**
   * 使用 Ultra /order 获取报价，并显式区分失败类型
   */
  async _getSwapQuote(inputMint, amount) {
    try {
      const headers = {};
      if (this.jupiterApiKey) headers['x-api-key'] = this.jupiterApiKey;

      const params = new URLSearchParams({
        inputMint,
        outputMint: SOL_MINT,
        amount: amount.toString()
      });

      const res = await axios.get(`https://api.jup.ag/ultra/v1/order?${params.toString()}`, {
        headers,
        timeout: 5000
      });

      const data = res.data;
      if (!data) {
        return { ok: false, reason: 'null_response', quote: null };
      }

      if (data.errorCode === 'ROUTE_NOT_FOUND' || data.errorCode === 'COULD_NOT_FIND_ANY_ROUTE') {
        return { ok: false, reason: 'no_route', quote: data };
      }

      const responseMessage = String(data.error || data.message || '');
      if (data.errorCode === 'TOKEN_NOT_TRADABLE' || /not tradable/i.test(responseMessage)) {
        return { ok: false, reason: 'token_not_tradable', quote: data };
      }

      if (!data.outAmount) {
        return { ok: false, reason: 'null_response', quote: data };
      }

      return { ok: true, reason: null, quote: data };
    } catch (error) {
      if (error.response?.status === 429) {
        return { ok: false, reason: 'rate_limited_429', quote: null };
      }

      if (error.response?.status === 400) {
        const errorCode = error.response?.data?.errorCode;
        const responseMessage = String(error.response?.data?.error || error.response?.data?.message || '');
        if (errorCode === 'ROUTE_NOT_FOUND' || errorCode === 'COULD_NOT_FIND_ANY_ROUTE') {
          return { ok: false, reason: 'no_route', quote: error.response?.data || null };
        }
        if (errorCode === 'TOKEN_NOT_TRADABLE' || /not tradable/i.test(responseMessage)) {
          return { ok: false, reason: 'token_not_tradable', quote: error.response?.data || null };
        }
        return { ok: false, reason: 'null_response', quote: error.response?.data || null };
      }

      const networkError = new Error(error.message || 'Ultra quote request failed');
      networkError.cause = error;
      throw networkError;
    }
  }

  async _tryDexFallback(tokenCA, context = {}) {
    this.stats.dex_fallback_queries++;

    try {
      const pair = await this._fetchBestDexPair(tokenCA);
      if (!pair) {
        this.stats.dex_fallback_failures++;
        this._recordFailure(tokenCA, context.reason || 'dex_fallback_miss', 'Dex fallback pair not found');
        return null;
      }

      const entry = this._storeDexFallback(tokenCA, pair, context.reason || 'dex_fallback');
      this.stats.dex_fallback_hits++;
      this._tokenFailCounts.delete(tokenCA);
      return entry;
    } catch (error) {
      this.stats.dex_fallback_failures++;
      this.stats.errors++;
      this._recordFailure(tokenCA, 'dex_fallback_error', error.message);
      return null;
    }
  }

  _storeDexFallback(tokenCA, pair, reason) {
    const priceUsd = parseFloat(pair.priceUsd || 0);
    const priceNative = parseFloat(pair.priceNative || 0);
    const price = priceNative > 0 ? priceNative : priceUsd;
    const timestamp = Date.now();
    const cached = this.priceCache.get(tokenCA);
    const mc = pair.marketCap || cached?.mc || null;

    const entry = {
      ...cached,
      nativePrice: priceNative > 0 ? priceNative : null,
      price,
      usdPrice: priceUsd > 0 ? priceUsd : cached?.usdPrice,
      mc,
      timestamp,
      source: 'dex-fallback',
      cacheState: 'fresh',
      lastFailureType: reason || null,
      lastFailureAt: reason ? timestamp : null
    };

    this.priceCache.set(tokenCA, entry);

    this.emit('price-update', {
      tokenCA,
      price: entry.price,
      usdPrice: entry.usdPrice,
      mc: entry.mc,
      timestamp,
      source: 'dex-fallback'
    });

    void this._publishRedisPrice(tokenCA, {
      price_sol: priceNative > 0 ? priceNative : null,
      price_usd: priceUsd > 0 ? priceUsd : null,
      source: 'dex-fallback',
      timestamp: entry.timestamp,
      mc: entry.mc || null
    });

    return entry;
  }

  async _fetchBestDexPair(tokenCA) {
    const res = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${tokenCA}`, {
      timeout: 10000
    });

    const pairs = res.data?.pairs || [];
    let bestPair = null;

    for (const pair of pairs) {
      const addr = pair.baseToken?.address;
      if (addr !== tokenCA) continue;
      if (!bestPair || (pair.liquidity?.usd || 0) > (bestPair.liquidity?.usd || 0)) {
        bestPair = pair;
      }
    }

    return bestPair;
  }

  /**
   * DexScreener 定时查询：补充 MC；Ultra/Dex 都没 fresh 数据时继续兜底
   */
  async _queryDexScreener() {
    if (this.watchList.size === 0) return;

    const tokens = [...this.watchList.keys()];
    this.stats.dex_queries++;

    try {
      const batchSize = 30;
      for (let i = 0; i < tokens.length; i += batchSize) {
        const batch = tokens.slice(i, i + batchSize);
        const res = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${batch.join(',')}`, {
          timeout: 10000
        });

        const pairs = res.data?.pairs || [];
        const bestPairs = new Map();

        for (const pair of pairs) {
          const addr = pair.baseToken?.address;
          if (!addr) continue;
          const existing = bestPairs.get(addr);
          if (!existing || (pair.liquidity?.usd || 0) > (existing.liquidity?.usd || 0)) {
            bestPairs.set(addr, pair);
          }
        }

        for (const tokenCA of batch) {
          const pair = bestPairs.get(tokenCA);
          if (!pair) continue;

          const cached = this.priceCache.get(tokenCA);
          const now = Date.now();
          const mc = pair.marketCap || cached?.mc || null;

          if (cached) {
            const nextCached = {
              ...cached,
              mc,
              cacheState: (now - cached.timestamp) < this.cacheStaleMs ? 'fresh' : 'stale'
            };
            this.priceCache.set(tokenCA, nextCached);

            // 如果已有 fresh Ultra 数据，只补 MC 不覆盖价格
            if (cached.source === 'jupiter-quote' && (now - cached.timestamp) < this.cacheFreshMs) {
              continue;
            }
          }

          // 没缓存或缓存过旧时，用 Dex 继续兜底价格
          this._storeDexFallback(tokenCA, pair, 'dex_periodic_refresh');
        }
      }
    } catch (error) {
      this.stats.errors++;
      if (this.stats.errors <= 10 || this.stats.errors % 20 === 0) {
        console.warn(`⚠️  [LivePriceMonitorV2] DexScreener 查询失败: ${error.message}`);
      }
    }
  }

  async _ensureRedisClient() {
    if (!this.redisEnabled || this.redisFailed) {
      return null;
    }

    if (this.redisClient?.isOpen) {
      return this.redisClient;
    }

    if (!this.redisClient) {
      this.redisClient = createClient({ url: this.redisUrl });
      this.redisClient.on('error', (error) => {
        if (!this.redisFailed) {
          console.warn(`⚠️  [LivePriceMonitorV2] Redis 不可用，跳过发布: ${error.message}`);
        }
        this.redisFailed = true;
      });
    }

    if (!this.redisConnectPromise) {
      this.redisConnectPromise = this.redisClient.connect()
        .then(() => {
          this.redisFailed = false;
          return this.redisClient;
        })
        .catch((error) => {
          this.redisFailed = true;
          console.warn(`⚠️  [LivePriceMonitorV2] Redis 连接失败，跳过发布: ${error.message}`);
          return null;
        })
        .finally(() => {
          this.redisConnectPromise = null;
        });
    }

    return this.redisConnectPromise;
  }

  async _publishRedisPrice(tokenCA, payload) {
    const client = await this._ensureRedisClient();
    if (!client?.isOpen) {
      return;
    }

    try {
      await client.set(`live_price:${tokenCA}`, JSON.stringify(payload));
    } catch (error) {
      this.redisFailed = true;
      console.warn(`⚠️  [LivePriceMonitorV2] Redis 发布失败 ${tokenCA.substring(0, 8)}...: ${error.message}`);
    }
  }

  async _closeRedisClient() {
    const client = this.redisClient;
    this.redisConnectPromise = null;
    this.redisClient = null;
    if (!client?.isOpen) {
      return;
    }

    try {
      await client.quit();
    } catch (error) {
      console.warn(`⚠️  [LivePriceMonitorV2] Redis 关闭失败: ${error.message}`);
    }
  }

  getStats() {
    return {
      ...this.stats,
      watching: this.watchList.size,
      cached: this.priceCache.size,
      is_running: this.isRunning,
      rate_limit_cooldown_until: this._rateLimitCooldownUntil,
      rate_limit_cooldown_remaining_ms: Math.max(0, this._rateLimitCooldownUntil - Date.now())
    };
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

function classifyPaperLiteQuoteFailure(quote = {}) {
  const responseMessage = String(quote?.error || quote?.message || '');
  const errorCode = quote?.errorCode;
  if (errorCode === 'TOKEN_NOT_TRADABLE' || /not tradable/i.test(responseMessage)) {
    return 'token_not_tradable';
  }
  if (errorCode === 'ROUTE_NOT_FOUND' || errorCode === 'COULD_NOT_FIND_ANY_ROUTE') {
    return 'no_route';
  }
  if (/no route|could not find/i.test(responseMessage)) {
    return 'no_route';
  }
  return null;
}

async function detectPaperTerminalSellFailure({ tokenCA, tokenAmountRaw, executor = null } = {}) {
  const normalizedAmount = Math.trunc(Number(tokenAmountRaw || 0));
  if (!(tokenCA && normalizedAmount > 0)) {
    return null;
  }

  const nativeExecutor = executor?.executor || executor;
  const getLiteSwapQuote = nativeExecutor?._getLiteSwapQuote;
  if (typeof getLiteSwapQuote !== 'function') {
    return null;
  }

  try {
    const liteQuote = await getLiteSwapQuote.call(nativeExecutor, tokenCA, SOL_MINT, normalizedAmount, { slippageBps: 500 });
    return classifyPaperLiteQuoteFailure(liteQuote);
  } catch {
    return null;
  }
}

export async function getPaperManagedMark({ tokenCA, tokenAmountRaw, tokenDecimals = 6, executor = null } = {}) {
  const monitor = new LivePriceMonitorV2(executor, { redisEnabled: false });
  const normalizedAmount = Number(tokenAmountRaw || 0);
  const normalizedDecimals = Number(tokenDecimals || 6);
  const markQuoteAmount = getPaperMarkQuoteAmount(normalizedAmount, normalizedDecimals);

  if (!(tokenCA && normalizedAmount > 0 && markQuoteAmount > 0)) {
    return {
      ok: false,
      failureReason: 'missing_mark_inputs',
      source: 'invalid',
      routeAvailable: null,
      quoteFailureReason: 'missing_mark_inputs',
      quotedOutAmount: null,
    };
  }

  let quoteResult;
  try {
    quoteResult = await monitor._getSwapQuote(tokenCA, markQuoteAmount);
  } catch (error) {
    quoteResult = {
      ok: false,
      reason: error?.response?.status === 401 ? 'unauthorized' : 'quote_request_failed',
      quote: null,
      error,
    };
  }

  if (quoteResult.ok && quoteResult.quote?.outAmount) {
    monitor._storeUltraQuote(tokenCA, quoteResult.quote, markQuoteAmount, normalizedDecimals);
    const cached = monitor.priceCache.get(tokenCA);
    return {
      ok: true,
      source: 'jupiter-quote-full',
      currentPrice: cached?.usdPrice || null,
      currentPriceSol: cached?.nativePrice ?? cached?.price ?? null,
      quoteTsSec: cached?.timestamp ? Math.floor(cached.timestamp / 1000) : Math.floor(Date.now() / 1000),
      routeAvailable: true,
      quoteFailureReason: null,
      quotedOutAmount: null,
    };
  }

  const fallback = await monitor._tryDexFallback(tokenCA, { reason: quoteResult.reason || 'dex_fallback' });
  let terminalSellFailure = ['no_route', 'token_not_tradable'].includes(quoteResult.reason) ? quoteResult.reason : null;
  if (!terminalSellFailure && fallback) {
    terminalSellFailure = await detectPaperTerminalSellFailure({
      tokenCA,
      tokenAmountRaw: normalizedAmount,
      executor,
    });
  }

  if (fallback) {
    return {
      ok: true,
      source: 'dex-fallback',
      currentPrice: fallback.usdPrice || null,
      currentPriceSol: fallback.nativePrice ?? (fallback.usdPrice ? null : (fallback.price || null)),
      quoteTsSec: fallback.timestamp ? Math.floor(fallback.timestamp / 1000) : Math.floor(Date.now() / 1000),
      routeAvailable: terminalSellFailure ? false : null,
      quoteFailureReason: terminalSellFailure || quoteResult.reason || null,
      quotedOutAmount: null,
    };
  }

  return {
    ok: false,
    source: 'unavailable',
    routeAvailable: ['no_route', 'token_not_tradable'].includes(quoteResult.reason) ? false : null,
    quoteFailureReason: quoteResult.reason || 'mark_unavailable',
    quotedOutAmount: null,
  };
}

export default LivePriceMonitorV2;
