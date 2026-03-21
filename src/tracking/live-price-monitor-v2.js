/**
 * Live Price Monitor V2
 *
 * 🔧 统一价格源：使用 Jupiter Swap Quote 作为唯一价格源
 * - 不再使用 Jupiter Price API（市场价格，不准确）
 * - 直接使用 Jupiter Swap Quote（实际可交易价格）
 * - 每次价格更新时，模拟卖出获取真实报价
 * - 价格单位：SOL per token
 *
 * 优势：
 * - ✅ 使用实际可交易价格，最准确
 * - ✅ 考虑了滑点和流动性
 * - ✅ 与卖出时的报价一致
 * - ✅ 不需要 SOL/USD 汇率转换
 */

import { EventEmitter } from 'events';
import axios from 'axios';

const SOL_MINT = 'So11111111111111111111111111111111111111112';

export class LivePriceMonitorV2 extends EventEmitter {
  constructor(jupiterExecutor) {
    super();

    this.jupiterExecutor = jupiterExecutor;

    // 价格缓存 Map<tokenCA, {price, timestamp, source}>
    this.priceCache = new Map();

    // 监控的 token 列表 Map<tokenCA, {tokenAmount, decimals}>
    this.watchList = new Map();

    // 轮询定时器
    this.priceInterval = null;
    this.dexInterval = null;

    // 配置
    this.priceIntervalMs = 1500;   // 1.5 秒（避免 rate limit）
    this.dexIntervalMs = 10000;    // 10 秒备用（获取 MC）
    this.jupiterApiKey = process.env.JUPITER_API_KEY || '';
    this.isRunning = false;

    // 统计
    this.stats = {
      quote_queries: 0,
      quote_hits: 0,
      quote_failures: 0,
      dex_queries: 0,
      errors: 0
    };

    // 防止 setInterval 回调叠加
    this._queryRunning = false;
    // 单 token 连续失败计数（只计非429错误）
    this._tokenFailCounts = new Map();

    console.log('📡 [LivePriceMonitorV2] 初始化 - 使用 Jupiter Swap Quote');
  }

  /**
   * 添加 token 到监控列表
   * @param {string} tokenCA - Token 地址
   * @param {number} tokenAmount - Token 数量（raw amount，含 decimals）
   * @param {number} decimals - Token decimals
   */
  addToken(tokenCA, tokenAmount = 1e6, decimals = 6) {
    // tokenAmount/decimals 仅用于价格换算，K线采集不需要
    this.watchList.set(tokenCA, { tokenAmount, decimals });
    console.log(`📡 [LivePriceMonitorV2] 添加监控: ${tokenCA.substring(0, 8)}... | ${this.watchList.size} 个`);
  }

  /**
   * 移除 token
   */
  removeToken(tokenCA) {
    this.watchList.delete(tokenCA);
    this.priceCache.delete(tokenCA);
  }

  /**
   * 启动价格监控
   */
  start() {
    if (this.isRunning) return;
    this.isRunning = true;

    // Jupiter Quote 轮询
    this.priceInterval = setInterval(() => this._queryAllPrices(), this.priceIntervalMs);

    // DexScreener 备用轮询（获取 MC）
    this.dexInterval = setInterval(() => this._queryDexScreener(), this.dexIntervalMs);

    console.log(`✅ [LivePriceMonitorV2] 启动 | Quote 间隔: ${this.priceIntervalMs}ms | DexScreener: ${this.dexIntervalMs}ms`);
  }

  /**
   * 停止
   */
  stop() {
    this.isRunning = false;
    if (this.priceInterval) {
      clearInterval(this.priceInterval);
      this.priceInterval = null;
    }
    if (this.dexInterval) {
      clearInterval(this.dexInterval);
      this.dexInterval = null;
    }
    console.log(`⏹️  [LivePriceMonitorV2] 已停止 | 统计: ${JSON.stringify(this.stats)}`);
  }

  /**
   * 获取 token 当前价格
   */
  getPrice(tokenCA) {
    return this.priceCache.get(tokenCA) || null;
  }

  /**
   * 查询所有 token 的实时价格（使用 Jupiter Swap Quote）
   */
  async _queryAllPrices() {
    // 防止 setInterval 叠加：上一次轮询未完成时跳过本次
    if (this._queryRunning) return;
    if (this.watchList.size === 0) return;

    this._queryRunning = true;
    const tokens = [...this.watchList.entries()];
    this.stats.quote_queries++;

    try {
      // 逐个查询（避免并发过多触发 rate limit）
      for (const [tokenCA, { tokenAmount, decimals }] of tokens) {
        try {
          // 使用 Jupiter Quote API 获取实际可交易价格
          const quote = await this._getSwapQuote(tokenCA, tokenAmount);

          if (quote && quote.outAmount) {
            // 查询成功 → 重置该 token 的失败计数
            this._tokenFailCounts.delete(tokenCA);

            // 计算 SOL per token 价格
            const outSol = parseInt(quote.outAmount) / 1e9; // lamports to SOL
            const actualTokenAmount = tokenAmount / Math.pow(10, decimals);
            const solPrice = outSol / actualTokenAmount;

            const cached = this.priceCache.get(tokenCA);

            this.priceCache.set(tokenCA, {
              price: solPrice,  // SOL per token
              outSol,           // 卖出可得 SOL
              timestamp: Date.now(),
              source: 'jupiter-quote',
              mc: cached?.mc || null  // MC 由 DexScreener 补充
            });

            this.stats.quote_hits++;

            // Emit 价格更新事件
            this.emit('price-update', {
              tokenCA,
              price: solPrice,
              outSol,
              mc: cached?.mc || null,
              timestamp: Date.now(),
              source: 'jupiter-quote'
            });
          } else {
            // 路由找不到（非限速）→ 记录该 token 的失败次数
            const prev = this._tokenFailCounts.get(tokenCA) || 0;
            this._tokenFailCounts.set(tokenCA, prev + 1);
            this.stats.quote_failures++;
          }
        } catch (error) {
          if (error._is429) {
            // Jupiter API 限速 → 不计入 token 失败（是全局问题，不是单 token 死亡）
            this.stats.errors++;
            if (this.stats.errors <= 10 || this.stats.errors % 50 === 0) {
              console.warn(`⚠️  [LivePriceMonitorV2] Jupiter API 限速 (429)，跳过本轮 (累计 ${this.stats.errors} 次)`);
            }
          } else {
            // 真正的查询失败 → 计入 token 失败
            const prev = this._tokenFailCounts.get(tokenCA) || 0;
            this._tokenFailCounts.set(tokenCA, prev + 1);
            this.stats.errors++;
            if (this.stats.errors <= 10 || this.stats.errors % 50 === 0) {
              console.warn(`⚠️  [LivePriceMonitorV2] Quote 查询失败 (${this.stats.errors}): ${error.message}`);
            }
          }
        }

        // 避免 rate limit，每次查询间隔 100ms
        await new Promise(r => setTimeout(r, 100));
      }
    } finally {
      this._queryRunning = false;
    }
  }

  /**
   * 获取 token 连续失败次数（用于外部判断是否为流动性枯竭）
   * 只统计非429错误，避免把 API 限速误判为 token 死亡
   */
  getTokenFailCount(tokenCA) {
    return this._tokenFailCounts.get(tokenCA) || 0;
  }

  /**
   * 获取 Jupiter Ultra Order（仅用于报价，不传 taker 则不生成交易）
   * 使用 Ultra /order 端点，与实际交易执行一致
   * @param {string} inputMint - Token 地址
   * @param {number} amount - Token 数量（raw amount）
   */
  async _getSwapQuote(inputMint, amount) {
    try {
      const headers = {};
      if (this.jupiterApiKey) headers['x-api-key'] = this.jupiterApiKey;

      // 使用 Ultra /order 端点（与交易执行一致的价格源）
      // 不传 taker → 只返回报价，不生成交易
      const params = new URLSearchParams({
        inputMint,
        outputMint: SOL_MINT,
        amount: amount.toString()
      });

      const res = await axios.get(`https://api.jup.ag/ultra/v1/order?${params.toString()}`, {
        headers,
        timeout: 5000
      });

      return res.data;
    } catch (error) {
      // 区分 429 限速（全局问题）vs 其他错误（可能是 token 流动性问题）
      if (error.response?.status === 429) {
        const rateErr = new Error('Jupiter API rate limit (429)');
        rateErr._is429 = true;
        throw rateErr;
      }
      // 其他错误（路由超时、网络抖动等）→ 返回 null，由调用方计入 token 失败
      return null;
    }
  }

  /**
   * DexScreener 备用查询（获取 MC）
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
        for (const pair of pairs) {
          if (pair.baseToken?.address && pair.marketCap) {
            const addr = pair.baseToken.address;
            const cached = this.priceCache.get(addr);
            if (cached) {
              cached.mc = pair.marketCap;
              this.priceCache.set(addr, cached);
            }
          }
        }
      }
    } catch (error) {
      // 静默处理
    }
  }
}

export default LivePriceMonitorV2;
