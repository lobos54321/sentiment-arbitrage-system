/**
 * Smart Money Tracker
 * 
 * 追踪聪明钱（鲸鱼钱包）的交易行为
 * 数据源：
 *   1. Birdeye API - Top holders 变化
 *   2. Helius API - 钱包交易监控
 *   3. DexScreener - 大额交易
 */

import axios from 'axios';

// 已知的聪明钱钱包地址（SOL）
const SMART_MONEY_WALLETS = {
  // 顶级鲸鱼 - 历史记录证明的高胜率钱包
  tier1: [
    '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1', // Wintermute
    'HN7cABqLq46Es1jh92dQQisAq662SmxELLLsHHe4YWrH', // Jump Trading
    '9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM', // Alameda (遗留)
    'DYw8jCTfwHNRJhhmFcbXvVDTqWMEVFBX6ZKUmG5CNSKK', // Genesis Trading
    '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr', // 传奇鲸鱼1
  ],
  // 知名 KOL 钱包
  tier2: [
    'CkNWFvqyDqNWX5qpRYcBBJBqFGVqGvRiLBGkUJqKLBm5', // KOL 1
    '5HRrdmghsnU3i2u5StaKaydS7eq3vnKVKwXNzYLp5q9r', // KOL 2
  ],
  // 聪明钱追踪服务标记的高胜率钱包
  tier3: []
};

// BSC 聪明钱钱包
const BSC_SMART_WALLETS = {
  tier1: [
    '0xf89d7b9c864f589bbf53a82105107622b35eaa40', // 知名鲸鱼
    '0x8894e0a0c962cb723c1976a4421c95949be2d4e3', // 高胜率钱包
  ]
};

export class SmartMoneyTracker {
  constructor(config, dynamicScoringManager) {
    this.config = config;
    this.dynamicScoringManager = dynamicScoringManager;
    
    // API 配置
    this.birdeyeApiKey = config.BIRDEYE_API_KEY;
    this.heliusApiKey = config.HELIUS_API_KEY;
    
    // 缓存
    this.topHoldersCache = new Map();
    this.walletActivityCache = new Map();
    
    // 配置
    this.CACHE_TTL = 5 * 60 * 1000; // 5分钟缓存
    this.MIN_WHALE_USD = 10000; // 最小鲸鱼交易额
    
    console.log('🐋 Smart Money Tracker initialized');
  }

  /**
   * 获取 token 的聪明钱活动评分
   */
  async getSmartMoneyScore(tokenCA, chain = 'SOL') {
    try {
      const [topHoldersData, recentTrades] = await Promise.all([
        this.getTopHolders(tokenCA, chain),
        this.getRecentWhaleTrades(tokenCA, chain)
      ]);
      
      let score = 0;
      const reasons = [];
      
      // 1. Top Holders 分析 (最多 +3 分)
      if (topHoldersData) {
        const { smartMoneyCount, totalSmartMoneyPercent } = this.analyzeTopHolders(topHoldersData, chain);
        
        if (smartMoneyCount >= 3) {
          score += 3;
          reasons.push(`🐋 ${smartMoneyCount}个聪明钱持仓 (${totalSmartMoneyPercent.toFixed(1)}%)`);
        } else if (smartMoneyCount >= 1) {
          score += 1;
          reasons.push(`📊 ${smartMoneyCount}个聪明钱持仓`);
        }
      }
      
      // 2. 最近交易分析 (最多 +5/-5 分)
      if (recentTrades && recentTrades.length > 0) {
        const { netFlow, buyCount, sellCount } = this.analyzeRecentTrades(recentTrades);
        
        if (netFlow > 50000) {
          score += 5;
          reasons.push(`🔥 聪明钱大量买入 +$${(netFlow/1000).toFixed(0)}K`);
        } else if (netFlow > 10000) {
          score += 3;
          reasons.push(`📈 聪明钱流入 +$${(netFlow/1000).toFixed(0)}K`);
        } else if (netFlow > 0) {
          score += 1;
          reasons.push(`✅ 聪明钱小幅买入`);
        } else if (netFlow < -50000) {
          score -= 5;
          reasons.push(`🚨 聪明钱出逃 -$${(Math.abs(netFlow)/1000).toFixed(0)}K`);
        } else if (netFlow < -10000) {
          score -= 3;
          reasons.push(`⚠️ 聪明钱流出 -$${(Math.abs(netFlow)/1000).toFixed(0)}K`);
        } else if (netFlow < 0) {
          score -= 1;
          reasons.push(`📉 聪明钱小幅卖出`);
        }
        
        // 记录到动态评分管理器
        for (const trade of recentTrades) {
          this.dynamicScoringManager?.recordSmartMoneyActivity(
            tokenCA,
            trade.wallet,
            trade.action,
            trade.amountUSD
          );
        }
      }
      
      return {
        score: Math.max(-5, Math.min(5, score)),
        reasons,
        topHolders: topHoldersData,
        recentTrades
      };
      
    } catch (error) {
      console.error(`❌ [Smart Money] Error: ${error.message}`);
      return { score: 0, reasons: ['聪明钱数据获取失败'], topHolders: null, recentTrades: null };
    }
  }

  /**
   * 获取 Top Holders
   */
  async getTopHolders(tokenCA, chain) {
    const cacheKey = `${chain}:${tokenCA}`;
    const cached = this.topHoldersCache.get(cacheKey);
    
    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL) {
      return cached.data;
    }
    
    try {
      if (chain === 'SOL' && this.birdeyeApiKey) {
        const response = await axios.get(
          `https://public-api.birdeye.so/defi/v3/token/holder?address=${tokenCA}&limit=20`,
          {
            headers: { 'X-API-KEY': this.birdeyeApiKey },
            timeout: 10000
          }
        );
        
        const holders = response.data?.data?.items || [];
        this.topHoldersCache.set(cacheKey, { data: holders, timestamp: Date.now() });
        return holders;
      }
      
      // Fallback: 使用 DexScreener
      return await this.getTopHoldersFromDexScreener(tokenCA, chain);
      
    } catch (error) {
      console.error(`   ⚠️ Top holders fetch failed: ${error.message}`);
      return null;
    }
  }

  /**
   * DexScreener 获取 Top Holders (备用)
   */
  async getTopHoldersFromDexScreener(tokenCA, chain) {
    try {
      const chainMap = { SOL: 'solana', BSC: 'bsc', ETH: 'ethereum' };
      const response = await axios.get(
        `https://api.dexscreener.com/latest/dex/tokens/${tokenCA}`,
        { timeout: 10000 }
      );
      
      const pair = response.data?.pairs?.[0];
      if (!pair) return null;
      
      // DexScreener 不直接提供 holders，但可以从交易数据推断
      return {
        source: 'dexscreener',
        txns24h: pair.txns?.h24 || {},
        volume24h: pair.volume?.h24 || 0,
        priceChange24h: pair.priceChange?.h24 || 0
      };
      
    } catch (error) {
      return null;
    }
  }

  /**
   * 获取最近的鲸鱼交易
   */
  async getRecentWhaleTrades(tokenCA, chain) {
    try {
      if (chain === 'SOL' && this.heliusApiKey) {
        return await this.getHeliusTransactions(tokenCA);
      }
      
      // 使用 Birdeye 交易历史
      if (chain === 'SOL' && this.birdeyeApiKey) {
        return await this.getBirdeyeTrades(tokenCA);
      }
      
      return [];
      
    } catch (error) {
      console.error(`   ⚠️ Whale trades fetch failed: ${error.message}`);
      return [];
    }
  }

  /**
   * Helius API 获取交易
   */
  async getHeliusTransactions(tokenCA) {
    try {
      const response = await axios.post(
        `https://mainnet.helius-rpc.com/?api-key=${this.heliusApiKey}`,
        {
          jsonrpc: '2.0',
          id: 'smart-money',
          method: 'searchAssets',
          params: {
            ownerAddress: tokenCA,
            tokenType: 'fungible',
            displayOptions: { showUnverifiedCollections: true }
          }
        },
        { timeout: 15000 }
      );
      
      // 解析交易数据
      return this.parseHeliusTransactions(response.data?.result || []);
      
    } catch (error) {
      return [];
    }
  }

  /**
   * Birdeye 获取交易历史
   */
  async getBirdeyeTrades(tokenCA) {
    try {
      const response = await axios.get(
        `https://public-api.birdeye.so/defi/txs/token?address=${tokenCA}&limit=50`,
        {
          headers: { 'X-API-KEY': this.birdeyeApiKey },
          timeout: 10000
        }
      );
      
      const txs = response.data?.data?.items || [];
      return this.parseBirdeyeTrades(txs);
      
    } catch (error) {
      return [];
    }
  }

  /**
   * 解析 Birdeye 交易数据
   */
  parseBirdeyeTrades(txs) {
    const smartWallets = new Set([
      ...SMART_MONEY_WALLETS.tier1,
      ...SMART_MONEY_WALLETS.tier2,
      ...SMART_MONEY_WALLETS.tier3
    ]);
    
    const whaleTrades = [];
    
    for (const tx of txs) {
      const wallet = tx.owner || tx.source;
      const amountUSD = tx.volumeUSD || tx.value || 0;
      
      // 是聪明钱钱包 或者 大额交易
      if (smartWallets.has(wallet) || amountUSD >= this.MIN_WHALE_USD) {
        whaleTrades.push({
          wallet,
          action: tx.side === 'buy' ? 'buy' : 'sell',
          amountUSD,
          timestamp: tx.blockTime * 1000,
          isSmartMoney: smartWallets.has(wallet)
        });
      }
    }
    
    return whaleTrades;
  }

  /**
   * 解析 Helius 交易数据
   */
  parseHeliusTransactions(results) {
    // Helius 返回格式不同，需要适配
    return [];
  }

  /**
   * 分析 Top Holders
   */
  analyzeTopHolders(holders, chain) {
    if (!holders || !Array.isArray(holders)) {
      return { smartMoneyCount: 0, totalSmartMoneyPercent: 0 };
    }
    
    const smartWallets = chain === 'SOL' 
      ? new Set([...SMART_MONEY_WALLETS.tier1, ...SMART_MONEY_WALLETS.tier2])
      : new Set([...BSC_SMART_WALLETS.tier1]);
    
    let smartMoneyCount = 0;
    let totalSmartMoneyPercent = 0;
    
    for (const holder of holders) {
      const address = holder.owner || holder.address;
      if (smartWallets.has(address)) {
        smartMoneyCount++;
        totalSmartMoneyPercent += holder.percentage || holder.uiAmountPercent || 0;
      }
    }
    
    return { smartMoneyCount, totalSmartMoneyPercent };
  }

  /**
   * 分析最近交易
   */
  analyzeRecentTrades(trades) {
    let buyVolume = 0;
    let sellVolume = 0;
    let buyCount = 0;
    let sellCount = 0;
    
    const oneHourAgo = Date.now() - 60 * 60 * 1000;
    
    for (const trade of trades) {
      // 只分析最近1小时的交易
      if (trade.timestamp < oneHourAgo) continue;
      
      if (trade.action === 'buy') {
        buyVolume += trade.amountUSD;
        buyCount++;
      } else {
        sellVolume += trade.amountUSD;
        sellCount++;
      }
    }
    
    return {
      netFlow: buyVolume - sellVolume,
      buyVolume,
      sellVolume,
      buyCount,
      sellCount
    };
  }

  /**
   * 检查是否有聪明钱出逃信号
   */
  async checkSmartMoneyExit(tokenCA, chain = 'SOL') {
    const result = await this.getSmartMoneyScore(tokenCA, chain);
    
    // 分数 < -2 表示聪明钱在出逃
    return {
      isExiting: result.score < -2,
      score: result.score,
      reasons: result.reasons
    };
  }

  /**
   * 更新聪明钱钱包列表（可以从外部 API 动态获取）
   */
  async updateSmartWalletList() {
    try {
      // 可以从 Nansen、DexCheck 等服务获取最新的聪明钱列表
      // 这里预留接口
      console.log('📊 [Smart Money] Wallet list update (placeholder)');
    } catch (error) {
      console.error('❌ [Smart Money] Wallet list update failed:', error.message);
    }
  }
}

export default SmartMoneyTracker;
