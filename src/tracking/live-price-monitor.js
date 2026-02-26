/**
 * Live Price Monitor
 *
 * 实时价格引擎，替代 shadow tracker 的 10 秒 DexScreener 轮询
 * - Jupiter Price API v2 每 1.5 秒批量查询所有持仓 token 价格
 * - DexScreener 每 10 秒作为备用（Jupiter 没价格时用）
 * - EventEmitter 模式，每次价格更新 emit `price-update` 事件
 */

import { EventEmitter } from 'events';
import axios from 'axios';

export class LivePriceMonitor extends EventEmitter {
  constructor() {
    super();

    // 价格缓存 Map<tokenCA, {price, mc, timestamp, source}>
    this.priceCache = new Map();

    // 监控的 token 列表
    this.watchList = new Set();

    // 轮询定时器
    this.jupiterInterval = null;
    this.dexInterval = null;

    // 配置
    this.jupiterIntervalMs = 1500;  // 1.5 秒
    this.dexIntervalMs = 10000;     // 10 秒备用
    this.jupiterApiKey = process.env.JUPITER_API_KEY || '';
    this.isRunning = false;

    // 统计
    this.stats = {
      jupiter_queries: 0,
      jupiter_hits: 0,
      jupiter_misses: 0,
      dex_queries: 0,
      dex_hits: 0,
      errors: 0
    };

    console.log('📡 [LivePriceMonitor] 初始化');
  }

  /**
   * 添加 token 到监控列表
   */
  addToken(tokenCA) {
    this.watchList.add(tokenCA);
    console.log(`📡 [LivePriceMonitor] 添加监控: ${tokenCA.substring(0, 8)}... (共 ${this.watchList.size} 个)`);
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

    // Jupiter 主轮询 1.5s
    this.jupiterInterval = setInterval(() => this._queryJupiter(), this.jupiterIntervalMs);

    // DexScreener 备用轮询 10s
    this.dexInterval = setInterval(() => this._queryDexScreener(), this.dexIntervalMs);

    console.log(`✅ [LivePriceMonitor] 启动 | Jupiter: ${this.jupiterIntervalMs}ms | DexScreener: ${this.dexIntervalMs}ms | API Key: ${this.jupiterApiKey ? '✅' : '❌ (需要 JUPITER_API_KEY)'}`);
  }

  /**
   * 停止
   */
  stop() {
    this.isRunning = false;
    if (this.jupiterInterval) {
      clearInterval(this.jupiterInterval);
      this.jupiterInterval = null;
    }
    if (this.dexInterval) {
      clearInterval(this.dexInterval);
      this.dexInterval = null;
    }
    console.log(`⏹️  [LivePriceMonitor] 已停止 | 统计: ${JSON.stringify(this.stats)}`);
  }

  /**
   * 获取 token 当前价格
   */
  getPrice(tokenCA) {
    return this.priceCache.get(tokenCA) || null;
  }

  /**
   * Jupiter Price API v3 批量查询
   * https://dev.jup.ag/docs/price-api
   */
  async _queryJupiter() {
    if (this.watchList.size === 0) return;
    if (!this.jupiterApiKey) return; // v3 必须有 API key

    const tokens = [...this.watchList];
    this.stats.jupiter_queries++;

    try {
      const ids = tokens.join(',');
      const res = await axios.get('https://api.jup.ag/price/v3', {
        params: { ids },
        headers: { 'x-api-key': this.jupiterApiKey },
        timeout: 3000
      });

      // v3 响应格式：直接 {tokenCA: {usdPrice, liquidity, decimals, ...}}
      const data = res.data;
      if (!data) return;

      for (const tokenCA of tokens) {
        const priceData = data[tokenCA];
        if (priceData && priceData.usdPrice) {
          const price = parseFloat(priceData.usdPrice);
          const cached = this.priceCache.get(tokenCA);

          this.priceCache.set(tokenCA, {
            price,
            mc: cached?.mc || null, // MC 由 DexScreener 补充
            liquidity: priceData.liquidity || null,
            timestamp: Date.now(),
            source: 'jupiter'
          });

          this.stats.jupiter_hits++;

          // Emit 价格更新事件
          this.emit('price-update', {
            tokenCA,
            price,
            mc: this.priceCache.get(tokenCA).mc,
            timestamp: Date.now(),
            source: 'jupiter'
          });
        } else {
          this.stats.jupiter_misses++;
        }
      }
    } catch (error) {
      this.stats.errors++;
      // 静默处理，DexScreener 会兜底
      if (this.stats.errors % 20 === 1) {
        console.warn(`⚠️  [LivePriceMonitor] Jupiter 查询失败: ${error.message}`);
      }
    }
  }

  /**
   * DexScreener 备用查询（补充 MC 数据 + Jupiter 没价格的 token）
   */
  async _queryDexScreener() {
    if (this.watchList.size === 0) return;

    const tokens = [...this.watchList];
    this.stats.dex_queries++;

    // DexScreener 批量查询，最多 30 个
    const batchSize = 30;
    for (let i = 0; i < tokens.length; i += batchSize) {
      const batch = tokens.slice(i, i + batchSize);
      try {
        const res = await axios.get(
          `https://api.dexscreener.com/latest/dex/tokens/${batch.join(',')}`,
          { timeout: 10000 }
        );

        const pairs = res.data?.pairs || [];
        // 按 token 分组，取最高流动性的 pair
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

          const price = parseFloat(pair.priceUsd || 0);
          const mc = pair.marketCap || 0;
          const cached = this.priceCache.get(tokenCA);

          // 如果 Jupiter 已有更新的价格，补充 MC 并 emit
          if (cached && cached.source === 'jupiter' && (Date.now() - cached.timestamp) < 5000) {
            cached.mc = mc;
            this.priceCache.set(tokenCA, cached);

            // MC 更新也要通知下游
            this.emit('price-update', {
              tokenCA,
              price: cached.price,
              mc,
              timestamp: Date.now(),
              source: 'jupiter'
            });
          } else {
            // Jupiter 没价格，用 DexScreener 的
            this.priceCache.set(tokenCA, {
              price,
              mc,
              timestamp: Date.now(),
              source: 'dexscreener'
            });

            this.stats.dex_hits++;

            this.emit('price-update', {
              tokenCA,
              price,
              mc,
              timestamp: Date.now(),
              source: 'dexscreener'
            });
          }
        }
      } catch (error) {
        this.stats.errors++;
        if (this.stats.errors % 10 === 1) {
          console.warn(`⚠️  [LivePriceMonitor] DexScreener 查询失败: ${error.message}`);
        }
      }
    }
  }

  /**
   * 获取统计
   */
  getStats() {
    return {
      ...this.stats,
      watching: this.watchList.size,
      cached: this.priceCache.size,
      is_running: this.isRunning
    };
  }
}

export default LivePriceMonitor;
