/**
 * Smart Money Scout (引擎 A)
 * 
 * 独立监听聪明钱交易，快速小仓进场
 * 触发时跳过 TG Spread 评分，使用简化版 Hard Gate
 * 
 * 特点：
 *   - 独立于 TG 信号触发
 *   - 小仓位试探 (0.05-0.1 SOL)
 *   - 快速决策 (3-5秒)
 *   - 为引擎 B 提供实盘验证
 */

import { SmartMoneyTracker } from '../tracking/smart-money-tracker.js';
import { HardGateFilter } from '../gates/hard-gates.js';
import axios from 'axios';

// 尝试导入 DeBot Scout
let debotScout = null;
try {
  debotScout = (await import('../inputs/debot-scout.js')).default;
} catch (e) {
  console.log('⚠️ DeBot Scout not available, using fallback mode');
}

// Scout 配置
const SCOUT_CONFIG = {
  // 仓位配置
  POSITION_SIZE_SOL: 0.05,  // 试探仓位
  MAX_POSITION_SIZE_SOL: 0.1,  // 最大仓位
  
  // 触发阈值
  MIN_BUY_USD: 5000,  // 最小买入金额触发
  MIN_SMART_WALLETS: 1,  // 最少聪明钱钱包数
  
  // 时间配置
  SCAN_INTERVAL: 10000,  // 扫描间隔 10秒
  DECISION_TIMEOUT: 5000,  // 决策超时 5秒
  
  // 风控
  MAX_CONCURRENT_SCOUTS: 2,  // 最大同时 Scout 仓位
  COOLDOWN_MINUTES: 15,  // 同一 token 冷却时间
};

// 高质量聪明钱钱包 (WinRate > 40%, PnL > $50k)
const VERIFIED_SMART_WALLETS = {
  SOL: [
    // Tier 1: 顶级鲸鱼
    '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1',  // Wintermute
    'HN7cABqLq46Es1jh92dQQisAq662SmxELLLsHHe4YWrH',  // Jump Trading
    'DYw8jCTfwHNRJhhmFcbXvVDTqWMEVFBX6ZKUmG5CNSKK',  // Genesis
    '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr',  // 传奇鲸鱼1
    
    // Tier 2: 高胜率钱包 (需要持续更新)
    // 'ADDRESS_HERE',
  ],
  BSC: [
    '0xf89d7b9c864f589bbf53a82105107622b35eaa40',
    '0x8894e0a0c962cb723c1976a4421c95949be2d4e3',
  ]
};

export class SmartMoneyScout {
  constructor(config, chainProviders, gmgnExecutor, database) {
    this.config = config;
    this.chainProviders = chainProviders;
    this.gmgnExecutor = gmgnExecutor;
    this.database = database;
    
    // 初始化组件
    this.smartMoneyTracker = new SmartMoneyTracker(config, null);
    this.hardGate = new HardGateFilter(chainProviders, config);
    
    // 状态管理
    this.activeScouts = new Map();  // tokenCA -> scoutPosition
    this.cooldownTokens = new Map();  // tokenCA -> timestamp
    this.isRunning = false;
    
    // Helius WebSocket (如果有)
    this.heliusApiKey = config.HELIUS_API_KEY;
    
    console.log('🔭 Smart Money Scout (引擎 A) initialized');
    console.log(`   仓位: ${SCOUT_CONFIG.POSITION_SIZE_SOL} SOL`);
    console.log(`   最大同时 Scout: ${SCOUT_CONFIG.MAX_CONCURRENT_SCOUTS}`);
    console.log(`   监控钱包: ${VERIFIED_SMART_WALLETS.SOL.length} (SOL)`);
  }

  /**
   * 启动 Scout 引擎
   */
  async start() {
    if (this.isRunning) {
      console.log('⚠️ Scout already running');
      return;
    }
    
    this.isRunning = true;
    console.log('🚀 Smart Money Scout started');
    
    // 方案 A: 使用 DeBot API (推荐)
    if (debotScout && process.env.DEBOT_COOKIE) {
      this.startDeBotMode();
    }
    
    // 方案 B: 使用 Helius WebSocket 实时监听
    if (this.heliusApiKey) {
      this.startHeliusWebSocket();
    }
    
    // 方案 C: 轮询模式 (备用)
    this.startPollingMode();
  }

  /**
   * DeBot 模式 - 使用 DeBot API 获取聪明钱信号
   */
  startDeBotMode() {
    console.log('🤖 Starting DeBot Scout mode...');
    
    // 监听 DeBot 的 hunter-signal 事件
    debotScout.on('hunter-signal', async (signal) => {
      console.log(`\n🎯 [DeBot] Hunter Signal: ${signal.tokenSymbol} (${signal.chain})`);
      console.log(`   聪明钱: ${signal.smartMoney.online} online, ${signal.smartMoney.total} total`);
      
      // 转换为统一的 trade 格式
      const trade = {
        tokenCA: signal.tokenAddress,
        chain: signal.chain,
        wallet: 'DeBot_SmartMoney',
        amountUSD: signal.market.volume24h || 10000,
        action: 'buy',
        timestamp: Date.now(),
        source: 'DeBot',
        
        // DeBot 特有数据
        debotData: {
          smartMoneyOnline: signal.smartMoney.online,
          smartMoneyTotal: signal.smartMoney.total,
          activityScore: signal.activityScore,
          security: signal.security,
          tags: signal.tags
        }
      };
      
      // 执行 Scout 流程
      await this.executeScoutTrade(trade);
    });
    
    // 启动 DeBot Scout
    debotScout.start();
    console.log('   ✅ DeBot Scout active');
  }

  /**
   * Helius WebSocket 实时监听
   */
  startHeliusWebSocket() {
    // Helius Enhanced WebSocket 监听聪明钱钱包交易
    // 注意: 需要 Helius Pro 账户才能监听多个钱包
    console.log('📡 Attempting Helius WebSocket connection...');
    
    try {
      // 监听指定钱包的交易
      const wallets = VERIFIED_SMART_WALLETS.SOL;
      
      // Helius WebSocket URL
      // const wsUrl = `wss://atlas-mainnet.helius-rpc.com/?api-key=${this.heliusApiKey}`;
      
      // TODO: 实现 WebSocket 连接
      // 当前使用轮询模式作为替代
      console.log('   ⚠️ WebSocket not implemented, using polling mode');
      
    } catch (error) {
      console.error('❌ Helius WebSocket failed:', error.message);
    }
  }

  /**
   * 轮询模式监控
   */
  startPollingMode() {
    console.log('🔄 Scout polling mode started');
    
    setInterval(async () => {
      if (!this.isRunning) return;
      
      try {
        await this.scanSmartMoneyActivity();
      } catch (error) {
        console.error('❌ Scout scan error:', error.message);
      }
    }, SCOUT_CONFIG.SCAN_INTERVAL);
  }

  /**
   * 扫描聪明钱活动
   */
  async scanSmartMoneyActivity() {
    // 检查是否达到最大 Scout 数
    if (this.activeScouts.size >= SCOUT_CONFIG.MAX_CONCURRENT_SCOUTS) {
      return;
    }
    
    try {
      // 获取聪明钱最近交易
      const recentTrades = await this.getRecentSmartMoneyTrades();
      
      for (const trade of recentTrades) {
        // 跳过卖出交易
        if (trade.action !== 'buy') continue;
        
        // 跳过金额太小的交易
        if (trade.amountUSD < SCOUT_CONFIG.MIN_BUY_USD) continue;
        
        // 检查冷却
        if (this.isInCooldown(trade.tokenCA)) continue;
        
        // 检查是否已有仓位
        if (this.activeScouts.has(trade.tokenCA)) continue;
        
        // 触发 Scout 信号！
        console.log(`\n🔭 [SCOUT] Smart Money Signal Detected!`);
        console.log(`   Token: ${trade.tokenCA.slice(0, 8)}...`);
        console.log(`   Wallet: ${trade.wallet.slice(0, 8)}...`);
        console.log(`   Amount: $${trade.amountUSD.toLocaleString()}`);
        
        // 执行 Scout 流程
        await this.executeScoutTrade(trade);
      }
      
    } catch (error) {
      console.error('❌ Smart money scan error:', error.message);
    }
  }

  /**
   * 获取聪明钱最近交易
   */
  async getRecentSmartMoneyTrades() {
    const trades = [];
    
    try {
      // 使用 Birdeye API 获取钱包交易
      if (this.config.BIRDEYE_API_KEY) {
        for (const wallet of VERIFIED_SMART_WALLETS.SOL.slice(0, 5)) {
          const walletTrades = await this.getWalletRecentTrades(wallet);
          trades.push(...walletTrades);
        }
      }
      
      // 使用 Helius API 获取交易
      if (this.heliusApiKey) {
        const heliusTrades = await this.getHeliusRecentTrades();
        trades.push(...heliusTrades);
      }
      
    } catch (error) {
      console.error('❌ Get smart money trades error:', error.message);
    }
    
    return trades;
  }

  /**
   * 获取钱包最近交易 (Birdeye)
   */
  async getWalletRecentTrades(wallet) {
    try {
      const response = await axios.get(
        `https://public-api.birdeye.so/v1/wallet/tx_list?wallet=${wallet}&limit=10`,
        {
          headers: { 'X-API-KEY': this.config.BIRDEYE_API_KEY },
          timeout: 10000
        }
      );
      
      const txs = response.data?.data || [];
      const fiveMinutesAgo = Date.now() - 5 * 60 * 1000;
      
      return txs
        .filter(tx => tx.blockTime * 1000 > fiveMinutesAgo)
        .filter(tx => tx.side === 'buy')
        .map(tx => ({
          wallet,
          tokenCA: tx.tokenAddress,
          action: 'buy',
          amountUSD: tx.volumeUSD || 0,
          timestamp: tx.blockTime * 1000,
          chain: 'SOL'
        }));
        
    } catch (error) {
      return [];
    }
  }

  /**
   * 获取 Helius 最近交易
   */
  async getHeliusRecentTrades() {
    // TODO: 实现 Helius API 调用
    return [];
  }

  /**
   * 执行 Scout 交易
   */
  async executeScoutTrade(trade) {
    const { tokenCA, wallet, amountUSD, chain } = trade;
    
    try {
      // Step 1: 简化版 Hard Gate (只检查核心安全)
      console.log(`   🚧 [1/3] Quick Hard Gate...`);
      
      const hardGateResult = await this.quickHardGate(tokenCA, chain);
      
      if (hardGateResult.status === 'REJECT') {
        console.log(`   ❌ Hard Gate REJECT: ${hardGateResult.reason}`);
        this.setCooldown(tokenCA);
        return;
      }
      
      // Step 2: 快速评估 (跳过 TG Spread)
      console.log(`   📊 [2/3] Quick Evaluation...`);
      
      const evaluation = await this.quickEvaluation(tokenCA, chain, trade);
      
      if (!evaluation.pass) {
        console.log(`   ⚠️ Evaluation failed: ${evaluation.reason}`);
        this.setCooldown(tokenCA);
        return;
      }
      
      // Step 3: 执行交易
      console.log(`   💰 [3/3] Executing Scout Trade...`);
      
      const positionSize = this.calculateScoutPosition(evaluation.confidence);
      
      // 记录 Scout 仓位
      const scoutPosition = {
        tokenCA,
        chain,
        entryPrice: evaluation.currentPrice,
        positionSize,
        smartWallet: wallet,
        smartMoneyAmount: amountUSD,
        timestamp: Date.now(),
        status: 'pending'
      };
      
      this.activeScouts.set(tokenCA, scoutPosition);
      
      // 如果是 Shadow 模式，只记录不执行
      if (this.config.SHADOW_MODE !== 'false') {
        console.log(`   🎭 [SHADOW] Scout position recorded (no actual trade)`);
        scoutPosition.status = 'shadow';
        
        // 记录到数据库
        await this.recordScoutPosition(scoutPosition);
        
      } else {
        // 实盘执行
        const result = await this.gmgnExecutor?.executeBuy(tokenCA, positionSize);
        
        if (result?.success) {
          console.log(`   ✅ Scout trade executed: ${positionSize} SOL`);
          scoutPosition.status = 'active';
          scoutPosition.txHash = result.txHash;
        } else {
          console.log(`   ❌ Scout trade failed: ${result?.error}`);
          scoutPosition.status = 'failed';
          this.activeScouts.delete(tokenCA);
        }
      }
      
      this.setCooldown(tokenCA);
      
    } catch (error) {
      console.error(`❌ Scout trade error: ${error.message}`);
      this.setCooldown(tokenCA);
    }
  }

  /**
   * 快速 Hard Gate (简化版)
   */
  async quickHardGate(tokenCA, chain) {
    try {
      // 只检查最核心的安全项
      const snapshot = await this.chainProviders[chain]?.getTokenSnapshot(tokenCA);
      
      if (!snapshot) {
        return { status: 'REJECT', reason: 'Cannot get token data' };
      }
      
      // 1. 貔貅检测
      if (snapshot.honeypot?.isHoneypot) {
        return { status: 'REJECT', reason: 'Honeypot detected' };
      }
      
      // 2. Pump.fun 特殊处理
      const isPumpFun = tokenCA.toLowerCase().includes('pump');
      
      if (!isPumpFun) {
        // 非 Pump.fun: 检查权限
        if (chain === 'SOL') {
          if (snapshot.mintAuthority && snapshot.mintAuthority !== 'disabled') {
            return { status: 'REJECT', reason: 'Mint authority not disabled' };
          }
        }
        
        // 检查 LP
        if (!snapshot.lpBurned && !snapshot.lpLocked) {
          return { status: 'GREYLIST', reason: 'LP not burned/locked' };
        }
      }
      
      // 3. Top 10 持仓检查 (Pump.fun 加倍严格)
      const top10Threshold = isPumpFun ? 0.25 : 0.30;  // Pump.fun: 25%, 其他: 30%
      
      if (snapshot.top10Percent > top10Threshold * 100) {
        return { status: 'REJECT', reason: `Top10 too high: ${snapshot.top10Percent}%` };
      }
      
      return { status: 'PASS', snapshot };
      
    } catch (error) {
      return { status: 'REJECT', reason: error.message };
    }
  }

  /**
   * 快速评估 (跳过 TG Spread)
   */
  async quickEvaluation(tokenCA, chain, trade) {
    try {
      const provider = this.chainProviders[chain];
      const snapshot = await provider?.getTokenSnapshot(tokenCA);
      
      if (!snapshot) {
        return { pass: false, reason: 'Cannot get snapshot' };
      }
      
      // 1. 流动性检查
      const minLiquidity = chain === 'SOL' ? 10000 : 20000;
      if (snapshot.liquidity < minLiquidity) {
        return { pass: false, reason: `Low liquidity: $${snapshot.liquidity}` };
      }
      
      // 2. 市值检查 (太大的不玩)
      if (snapshot.marketCap > 5000000) {  // > $5M
        return { pass: false, reason: `Market cap too high: $${snapshot.marketCap}` };
      }
      
      // 3. 计算置信度
      let confidence = 0.5;  // 基础置信度
      
      // 聪明钱买入金额加成
      if (trade.amountUSD > 50000) confidence += 0.3;
      else if (trade.amountUSD > 20000) confidence += 0.2;
      else if (trade.amountUSD > 10000) confidence += 0.1;
      
      // 流动性加成
      if (snapshot.liquidity > 100000) confidence += 0.1;
      
      return {
        pass: true,
        confidence: Math.min(1, confidence),
        currentPrice: snapshot.price,
        liquidity: snapshot.liquidity,
        marketCap: snapshot.marketCap
      };
      
    } catch (error) {
      return { pass: false, reason: error.message };
    }
  }

  /**
   * 计算 Scout 仓位
   */
  calculateScoutPosition(confidence) {
    const base = SCOUT_CONFIG.POSITION_SIZE_SOL;
    const max = SCOUT_CONFIG.MAX_POSITION_SIZE_SOL;
    
    // 根据置信度调整仓位
    const position = base + (max - base) * confidence;
    
    return Math.min(max, Math.max(base, position));
  }

  /**
   * 记录 Scout 仓位到数据库
   */
  async recordScoutPosition(position) {
    try {
      const db = this.database;
      if (!db) return;
      
      await db.run(`
        INSERT INTO scout_positions (
          token_ca, chain, entry_price, position_size,
          smart_wallet, smart_money_amount, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        position.tokenCA,
        position.chain,
        position.entryPrice,
        position.positionSize,
        position.smartWallet,
        position.smartMoneyAmount,
        position.status,
        new Date().toISOString()
      ]);
      
    } catch (error) {
      console.error('❌ Record scout position error:', error.message);
    }
  }

  /**
   * 冷却管理
   */
  isInCooldown(tokenCA) {
    const cooldownUntil = this.cooldownTokens.get(tokenCA);
    if (!cooldownUntil) return false;
    return Date.now() < cooldownUntil;
  }

  setCooldown(tokenCA) {
    const cooldownUntil = Date.now() + SCOUT_CONFIG.COOLDOWN_MINUTES * 60 * 1000;
    this.cooldownTokens.set(tokenCA, cooldownUntil);
  }

  /**
   * 获取活跃 Scout 仓位
   */
  getActiveScouts() {
    return Array.from(this.activeScouts.values());
  }

  /**
   * 关闭 Scout 仓位 (当引擎 B 确认时调用)
   */
  confirmScoutPosition(tokenCA) {
    const scout = this.activeScouts.get(tokenCA);
    if (scout) {
      scout.confirmed = true;
      scout.confirmedAt = Date.now();
      console.log(`✅ [SCOUT] Position confirmed by Trend Engine: ${tokenCA.slice(0, 8)}...`);
    }
  }

  /**
   * 停止 Scout 引擎
   */
  stop() {
    this.isRunning = false;
    console.log('🛑 Smart Money Scout stopped');
  }
}

export default SmartMoneyScout;
