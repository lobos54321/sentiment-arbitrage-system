/**
 * Premium Signal Engine — v13
 *
 * 独立的信号处理引擎，专门处理付费频道信号
 * Pipeline: 信号 → 预检 → 链上快照 → Hard Gates → v13条件过滤 → 执行
 * 
 * v13: 黄金策略 — 基于24h/997信号回测的数据驱动入场
 * - 只交易ATH信号（删除CORE/AUX通道）
 * - 7个硬性条件: ATH>=3, Super>=150, Trade<15, AI增长>=30%, Security>=30, LP>=$10K
 * - 3级信号: S级(0.04 SOL) / A级(0.03) / B级(0.02)
 * - 提升条件: GainX<3 (+1级), AI增长>=100% (+1级)
 */

import fs from 'fs';
import path from 'path';
import { SolanaSnapshotService } from '../inputs/chain-snapshot-sol.js';
import { HardGateFilter } from '../gates/hard-gates.js';
import { ExitGateFilter } from '../gates/exit-gates.js';
import { PositionSizer } from '../decision/position-sizer.js';
import { GMGNTelegramExecutor } from '../execution/gmgn-telegram-executor.js';
import { JupiterUltraExecutor } from '../execution/jupiter-ultra-executor.js';
import ClaudeAnalyst from '../utils/claude-analyst.js';
import { generatePremiumBuyPrompt } from '../prompts/premium-signal-prompts.js';
import { TelegramBuzzScanner } from '../social/telegram-buzz.js';
import { ShadowPnlTracker } from '../tracking/shadow-pnl-tracker.js';
import axios from 'axios';

export class PremiumSignalEngine {
  constructor(config, db) {
    this.config = config;
    this.db = db;

    // 配置
    this.shadowMode = process.env.SHADOW_MODE !== 'false';
    this.autoBuyEnabled = process.env.AUTO_BUY_ENABLED === 'true';
    this.positionSol = parseFloat(process.env.PREMIUM_POSITION_SOL || '0.12');
    this.maxPositions = parseInt(process.env.PREMIUM_MAX_POSITIONS || '8');

    // 服务实例
    this.solService = new SolanaSnapshotService(config);
    this.hardGateFilter = new HardGateFilter(config);
    this.exitGateFilter = new ExitGateFilter(config);
    this.positionSizer = new PositionSizer(config, db);
    this.executor = new GMGNTelegramExecutor(config, db);
    this.jupiterExecutor = null; // 实盘模式下初始化
    this.livePositionMonitor = null; // 外部注入
    this.livePriceMonitor = null; // 外部注入（shadow 也可用）
    this.buzzScanner = null; // 需要 setTelegramClient 初始化
    this.shadowTracker = new ShadowPnlTracker();

    // 去重（短期 5 分钟）
    this.recentSignals = new Map(); // token_ca → timestamp
    // 🔧 Symbol级去重（15分钟窗口）— 防止同名不同CA的仿盘
    this.recentSymbols = new Map(); // symbol → timestamp
    // 🔧 退出后冷却（10分钟）— 防止退出后立即再买同名代币
    this.exitCooldown = new Map(); // symbol → timestamp
    // 信号历史（长期追踪重复信号）
    this.signalHistory = new Map(); // token_ca → { count, firstSeen, lastSeen, symbol }

    // v13: SOL市场环境暂停标志
    this._solMarketPaused = false;
    this._solMarketCheckInterval = null;

    // ATH计数持久化路径
    this._athCountsPath = path.join(process.cwd(), 'data', 'ath_counts.json');

    // v14: 观察列表 (ATH#1只观察, 等待ATH#2确认后买入)
    // tokenCA → { symbol, mc1, idx1, entryTime }
    this._watchlist = new Map();
    this._watchlistPath = path.join(process.cwd(), 'data', 'watchlist.json');

    // 统计
    this.stats = {
      signals_received: 0,
      duplicates_skipped: 0,
      precheck_failed: 0,
      snapshot_failed: 0,
      hard_gate_rejected: 0,
      ai_skipped: 0,
      position_denied: 0,
      exit_gate_rejected: 0,
      executed: 0,
      shadow_logged: 0,
      errors: 0
    };

    console.log('\n' + '─'.repeat(60));
    console.log('💎 [Premium Engine] 初始化');
    console.log(`   模式: ${this.shadowMode ? '🎭 SHADOW' : '💰 LIVE'}`);
    console.log(`   自动买入: ${this.autoBuyEnabled ? '✅' : '❌'}`);
    console.log(`   仓位: ${this.positionSol} SOL`);
    console.log(`   最大持仓: ${this.maxPositions}`);
    console.log('─'.repeat(60) + '\n');
  }

  /**
   * 设置 Jupiter 执行器和实盘仓位监控（由外部注入）
   */
  setLiveComponents(jupiterExecutor, livePositionMonitor) {
    this.jupiterExecutor = jupiterExecutor;
    this.livePositionMonitor = livePositionMonitor;
    console.log('✅ [Premium Engine] Jupiter + LivePositionMonitor 已注入');
  }

  /**
   * 设置 LivePriceMonitor（shadow 模式也可用）
   */
  setLivePriceMonitor(priceMonitor) {
    this.livePriceMonitor = priceMonitor;
    // 同时注入到 Shadow Tracker
    this.shadowTracker.setLivePriceMonitor(priceMonitor);
    console.log('✅ [Premium Engine] LivePriceMonitor 已注入');
  }

  /**
   * 初始化所有服务
   */
  async initialize() {
    try {
      // 初始化 Claude AI
      ClaudeAnalyst.init();

      // 初始化执行器（非 shadow 模式）
      if (!this.shadowMode && this.autoBuyEnabled) {
        // 优先用 Jupiter，fallback 到 GMGN Telegram
        if (this.jupiterExecutor) {
          console.log('✅ [Premium Engine] 使用 Jupiter Swap 执行器');
        } else {
          await this.executor.initialize();
          console.log('✅ [Premium Engine] 使用 GMGN Telegram 执行器 (fallback)');
        }
      }

      // 初始化数据库表
      this.initDB();

      // v13: 加载持久化的ATH计数
      this._loadAthCounts();

      // v14: 加载观察列表
      this._loadWatchlist();

      // v14: 从ATH计数中重建观察列表（处理重部署/过渡期）
      const timeout = 2 * 60 * 60 * 1000;
      const now = Date.now();
      let rebuilt = 0;
      for (const [ca, history] of this.signalHistory) {
        if (history.athCount === 1 && history.mc1 && !this._watchlist.has(ca)) {
          const entryTime = history.lastSeen || now;
          if (now - entryTime < timeout) {
            this._watchlist.set(ca, {
              symbol: history.symbol || 'UNKNOWN',
              mc1: history.mc1,
              entryTime: entryTime
            });
            rebuilt++;
          }
        }
      }
      if (rebuilt > 0) {
        this._saveWatchlist();
        console.log(`🔄 [v14] 从ATH计数重建${rebuilt}个观察列表条目`);
      }

      // v13: 启动SOL市场环境检查（每5分钟）
      this._startSolMarketCheck();

      // v14: 观察列表清理（每10分钟清理超时条目，2小时无ATH#2则移除）
      this._watchlistCleanupInterval = setInterval(() => {
        const now = Date.now();
        const timeout = 2 * 60 * 60 * 1000; // 2小时
        let cleaned = 0;
        for (const [ca, item] of this._watchlist) {
          if (now - item.entryTime > timeout) {
            console.log(`🧹 [v14] 观察列表清理: $${item.symbol || ca.slice(0,8)} 超过2小时无ATH#2，移除`);
            this._watchlist.delete(ca);
            cleaned++;
          }
        }
        if (cleaned > 0) this._saveWatchlist();
      }, 10 * 60 * 1000);

      console.log('✅ [Premium Engine] 所有服务初始化完成');

      // 启动 Shadow PnL 追踪
      if (this.shadowMode) {
        this.shadowTracker.start();
      }
    } catch (error) {
      console.error('❌ [Premium Engine] 初始化失败:', error.message);
      throw error;
    }
  }

  /**
   * 设置 Telegram client（用于 Buzz 搜索）
   */
  setTelegramClient(client) {
    this.buzzScanner = new TelegramBuzzScanner(client);
    console.log('✅ [Premium Engine] Telegram Buzz Scanner 已启用');
  }

  /**
   * 初始化数据库表
   */
  initDB() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS premium_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_ca TEXT NOT NULL,
        symbol TEXT,
        market_cap REAL,
        holders INTEGER,
        volume_24h REAL,
        top10_pct REAL,
        age TEXT,
        description TEXT,
        timestamp INTEGER NOT NULL,
        hard_gate_status TEXT,
        ai_action TEXT,
        ai_confidence INTEGER,
        ai_narrative_tier TEXT,
        executed INTEGER DEFAULT 0,
        trade_result TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS tokens (
        token_ca TEXT PRIMARY KEY,
        chain TEXT NOT NULL DEFAULT 'SOL',
        symbol TEXT,
        name TEXT,
        first_seen_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
        mc_at_signal REAL,
        rating TEXT,
        action TEXT,
        position_tier TEXT,
        position_size REAL,
        auto_buy_enabled INTEGER,
        decision_reasons TEXT,
        decision_timestamp INTEGER,
        created_at INTEGER DEFAULT (strftime('%s', 'now'))
      )
    `);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_ca TEXT NOT NULL,
        chain TEXT NOT NULL,
        entry_time INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
        entry_price REAL NOT NULL DEFAULT 0,
        position_size REAL NOT NULL DEFAULT 0,
        position_unit TEXT NOT NULL DEFAULT 'SOL',
        position_tier TEXT,
        score REAL,
        rating TEXT,
        action TEXT,
        hard_status TEXT,
        exit_status TEXT,
        exit_times TEXT,
        exit_prices TEXT,
        exit_percentages TEXT,
        realized_pnl REAL,
        max_up_2h REAL,
        max_dd_2h REAL,
        hold_duration_minutes INTEGER,
        execution_slippage REAL,
        fail_count INTEGER DEFAULT 0,
        rug_flag INTEGER DEFAULT 0,
        cannot_exit_flag INTEGER DEFAULT 0,
        exit_reason TEXT,
        gmgn_tx_hash TEXT,
        gmgn_order_id TEXT,
        timestamp INTEGER,
        symbol TEXT,
        narrative TEXT,
        status TEXT,
        is_simulation INTEGER DEFAULT 1,
        created_at INTEGER DEFAULT (strftime('%s', 'now')),
        updated_at INTEGER DEFAULT (strftime('%s', 'now'))
      )
    `);

    // 兼容旧数据库：补缺失列
    const addCol = (table, col, type) => {
      try { this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${col} ${type}`); } catch (e) { /* 已存在 */ }
    };
    addCol('trades', 'timestamp', 'INTEGER');
    addCol('trades', 'symbol', 'TEXT');
    addCol('trades', 'narrative', 'TEXT');
    addCol('trades', 'status', 'TEXT');
    addCol('trades', 'is_simulation', 'INTEGER DEFAULT 1');
    addCol('trades', 'entry_time', 'INTEGER');
    addCol('trades', 'entry_price', 'REAL');
  }

  /**
   * 处理信号 - 主 Pipeline
   */
  async processSignal(signal) {
    this.stats.signals_received++;
    const ca = signal.token_ca;
    const shortCA = ca.substring(0, 8);

    console.log('\n' + '═'.repeat(60));
    console.log(`💎 [Premium] 新信号: $${signal.symbol || shortCA} | MC: $${signal.market_cap ? (signal.market_cap / 1000).toFixed(1) + 'K' : '?'}`);
    console.log('═'.repeat(60));

    try {
      // Step 1: 记录信号历史 + 短期去重
      const history = this.signalHistory.get(ca);
      if (history) {
        history.count++;
        history.lastSeen = Date.now();
        if (signal.market_cap > 0) history.latestMC = signal.market_cap;
      } else {
        this.signalHistory.set(ca, {
          count: 1,
          firstSeen: Date.now(),
          lastSeen: Date.now(),
          symbol: signal.symbol,
          firstMC: signal.market_cap || 0,
          latestMC: signal.market_cap || 0,
          lastScore: 0  // 用于重复信号评分比较
        });
      }

      if (this.isDuplicate(ca)) {
        this.stats.duplicates_skipped++;
        console.log(`⏭️  [去重] ${shortCA}... 5分钟内已处理，跳过`);
        return { action: 'SKIP', reason: 'duplicate' };
      }
      this.markProcessed(ca);

      // 🔧 Symbol级去重（15分钟窗口）— 防止同名不同CA的仿盘
      const symbol = signal.symbol;
      const lastSymbolSeen = this.recentSymbols.get(symbol);
      if (lastSymbolSeen && (Date.now() - lastSymbolSeen) < 15 * 60 * 1000) {
        this.stats.duplicates_skipped++;
        console.log(`⏭️  [Symbol去重] $${symbol} 15分钟内已处理过同名代币，跳过 (CA: ${shortCA}...)`);
        return { action: 'SKIP', reason: 'symbol_duplicate' };
      }

      // 🔧 退出冷却检查（10分钟）— 防止退出后立即重买同名代币
      const cooldownUntil = this.exitCooldown.get(symbol);
      if (cooldownUntil && Date.now() < cooldownUntil) {
        const remainSec = Math.round((cooldownUntil - Date.now()) / 1000);
        console.log(`⏭️  [冷却中] $${symbol} 退出后冷却期，剩余${remainSec}s，跳过 (CA: ${shortCA}...)`);
        return { action: 'SKIP', reason: 'exit_cooldown' };
      }

      // Step 2: 预检 - 频道自带的 freeze/mint 数据
      // 只有明确标记为 false（❌）才拦截，未知的放过让链上快照验证
      if (signal.freeze_ok === false && signal.mint_ok === false && signal.market_cap > 0) {
        this.stats.precheck_failed++;
        console.log(`🚫 [预检] freeze=${signal.freeze_ok} mint=${signal.mint_ok} → 跳过`);
        this.saveSignalRecord(signal, 'PRECHECK_FAIL', null);
        return { action: 'SKIP', reason: 'precheck_failed' };
      }
      console.log(`✅ [预检] freeze=${signal.freeze_ok ?? '未知'} mint=${signal.mint_ok ?? '未知'} → 继续`);

      // Step 3: 链上快照
      let snapshot;
      try {
        console.log('📡 [链上] 获取 Solana 快照...');
        snapshot = await this.solService.getSnapshot(ca);
        if (!snapshot) throw new Error('快照为空');
        snapshot.chain = 'SOL';
        snapshot.token_ca = ca;
        console.log(`✅ [链上] 流动性: ${snapshot.liquidity ? snapshot.liquidity.toFixed(2) + ' SOL' : '未知'}`);
      } catch (error) {
        this.stats.snapshot_failed++;
        console.log(`⚠️  [链上] 快照获取失败: ${error.message}，使用频道数据继续`);
        // 构造最小快照用于 hard gates
        snapshot = {
          token_ca: ca,
          chain: 'SOL',
          freeze_authority: signal.freeze_ok ? 'Disabled' : 'Unknown',
          mint_authority: signal.mint_ok ? 'Disabled' : 'Unknown',
          liquidity: null,
          top10_percent: signal.top10_pct || null
        };
      }

      // Step 4: Hard Gates
      console.log('🚨 [Hard Gates] 安全检查...');
      const gateResult = this.hardGateFilter.evaluate(snapshot);
      if (gateResult.status === 'REJECT') {
        this.stats.hard_gate_rejected++;
        console.log(`🚫 [Hard Gates] REJECT: ${gateResult.reasons.join(', ')}`);
        this.saveSignalRecord(signal, 'REJECT', null);
        return { action: 'SKIP', reason: 'hard_gate_reject', details: gateResult.reasons };
      }
      const isGreylist = gateResult.status === 'GREYLIST';
      console.log(`✅ [Hard Gates] ${gateResult.status}${isGreylist ? ' ⚠️ (灰名单)' : ''}`);

      // Step 5: DexScreener 数据 + 量化评分
      console.log('📊 [评分] 获取 DexScreener 数据...');
      let dexData = null;
      try {
        const dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${ca}`, { timeout: 8000 });
        const pair = dexRes.data?.pairs?.[0];
        if (pair) {
          dexData = {
            buy_count_24h: pair.txns?.h24?.buys || 0,
            sell_count_24h: pair.txns?.h24?.sells || 0,
            buy_count_1h: pair.txns?.h1?.buys || 0,
            sell_count_1h: pair.txns?.h1?.sells || 0,
            buy_count_5m: pair.txns?.m5?.buys || 0,
            sell_count_5m: pair.txns?.m5?.sells || 0,
            volume_24h: pair.volume?.h24 || 0,
            price_change_5m: pair.priceChange?.m5 || 0,
            price_change_1h: pair.priceChange?.h1 || 0,
            liquidity_usd: pair.liquidity?.usd || 0,
            market_cap: pair.marketCap || pair.fdv || 0,
          };
          const buyRatio = dexData.sell_count_24h > 0 ? (dexData.buy_count_24h / dexData.sell_count_24h).toFixed(2) : '∞';
          console.log(`  📊 买=${dexData.buy_count_24h}/卖=${dexData.sell_count_24h} (比=${buyRatio}) | 量=$${(dexData.volume_24h/1000).toFixed(1)}K | 流动性=$${(dexData.liquidity_usd/1000).toFixed(1)}K`);
        }
      } catch (e) {
        console.log(`  ⚠️ DexScreener 失败: ${e.message}`);
      }

      // ===== v14: DCA两阶段入场逻辑 =====
      // 核心依据: 48h/1992条信号回测，ATH#1小仓+ATH#2 DCA加仓 = 50.8% WR, +1076% 总回报
      // ATH#1: Trade≤5, MC≤$75K → 买0.02 SOL (探索仓)
      // ATH#2: MC增长≥1.2x, Security≤25, Address≥10 → 买入0.07 SOL (v14.4)
      const isATH = signal.is_ath === true;
      const idx = signal.indices;
      let mc = dexData?.market_cap || signal.market_cap || 0;

      // MC 补充（Jupiter fallback）
      if (mc === 0 && this.livePriceMonitor) {
        const jupiterPrice = this.livePriceMonitor.priceCache.get(ca);
        if (jupiterPrice && jupiterPrice.mc && (Date.now() - jupiterPrice.timestamp) < 30000) {
          mc = jupiterPrice.mc;
        }
      }

      const liqUsd = dexData?.liquidity_usd || 0;

      // v14条件1: 只交易ATH信号
      if (!isATH) {
        console.log(`⏭️ [v14] $${signal.symbol} 非ATH信号 → 不交易`);
        this.saveSignalRecord(signal, 'NOT_ATH_V14', null);
        return { action: 'SKIP', reason: 'not_ath_v14' };
      }

      // ATH计数器（在signalHistory中维护，持久化到JSON）
      const sigHistory = this.signalHistory.get(ca);
      const prevAthCount = sigHistory ? (sigHistory.athCount || 0) : 0;
      if (sigHistory) {
        sigHistory.athCount = prevAthCount + 1;
        if (idx?.super_index) {
          sigHistory.lastSuperIndex = idx.super_index.current;
          if (!sigHistory.firstSuperIndex) sigHistory.firstSuperIndex = idx.super_index.signal;
        }
        // v14: 记录ATH#1时的MC（用于ATH#2 MC增长计算）
        if (prevAthCount === 0 && mc > 0) {
          sigHistory.mc1 = mc;
        }
      }
      const currentAthNum = prevAthCount + 1;
      this._saveAthCounts(); // 持久化

      // 读取常用指标
      const tradeCurrent = idx?.trade_index?.current || 0;
      const securityCurrent = idx?.security_index?.current || 0;
      const superCurrent = idx?.super_index?.current || 0;
      const addressCurrent = idx?.address_index?.current || 0;

      // ===== 全局开关: SOL市场环境 =====
      if (this._solMarketPaused) {
        console.log(`⏸️ [v14] SOL 24h跌>10%，市场暂停中，不开新仓`);
        return { action: 'SKIP', reason: 'market_paused' };
      }

      // ===== v14 两阶段入场分支 (ATH#1观察 + ATH#2买入) =====
      let finalSize, tradeConviction;

      if (currentAthNum === 1) {
        // ====== ATH#1: 只观察，不买入 ======
        const ath1Reasons = [];
        if (tradeCurrent < 4) ath1Reasons.push(`Trade=${tradeCurrent}<4(流动性不足)`);
        if (mc > 75000) ath1Reasons.push(`MC=$${(mc/1000).toFixed(1)}K>$75K`);

        if (ath1Reasons.length > 0) {
          console.log(`⏭️ [v14] $${signal.symbol} ATH#1 过滤不通过: ${ath1Reasons.join(' | ')}`);
          this.saveSignalRecord(signal, 'V14_ATH1_FILTER', null);
          return { action: 'SKIP', reason: 'v14_ath1_filter', details: ath1Reasons };
        }

        // ATH#1通过筛选 → 加入观察列表，不买入
        this._watchlist.set(ca, { symbol: signal.symbol, mc1: mc, idx1: idx, entryTime: Date.now() });
        this._saveWatchlist();
        console.log(`👀 [v14] $${signal.symbol} ATH#1 ✅ 加入观察列表! Trade=${tradeCurrent} MC=$${(mc/1000).toFixed(1)}K → 等待ATH#2确认`);
        this.saveSignalRecord(signal, 'V14_ATH1_WATCH', null);

        // 注册价格追踪
        if (this.livePriceMonitor) this.livePriceMonitor.addToken(ca);

        return { action: 'WATCH', reason: 'v14_ath1_observe', symbol: signal.symbol };

      } else if (currentAthNum === 2) {
        // ====== ATH#2: 确认后买入 (0.07 SOL) ======
        const watchItem = this._watchlist.get(ca);

        if (!watchItem) {
          console.log(`⏭️ [v14] $${signal.symbol} ATH#2 但不在观察列表中（ATH#1被过滤或超时），跳过`);
          this.saveSignalRecord(signal, 'V14_ATH2_NOT_WATCHED', null);
          return { action: 'SKIP', reason: 'v14_ath2_not_watched' };
        }

        // ATH#2 筛选条件 (v14.4: MC≥1.2x, Sec≤25, Address≥10)
        const mcGrowth = watchItem.mc1 > 0 ? mc / watchItem.mc1 : 0;
        const ath2Reasons = [];
        if (mcGrowth < 1.2) ath2Reasons.push(`MC增长=${mcGrowth.toFixed(2)}x<1.2x`);
        if (securityCurrent > 25) ath2Reasons.push(`Security=${securityCurrent}>25(波动性不足)`);
        if (addressCurrent < 10) ath2Reasons.push(`Address=${addressCurrent}<10(持币地址不足)`);

        if (ath2Reasons.length > 0) {
          console.log(`⏭️ [v14] $${signal.symbol} ATH#2 过滤不通过: ${ath2Reasons.join(' | ')} (MC1=$${(watchItem.mc1/1000).toFixed(1)}K→MC2=$${(mc/1000).toFixed(1)}K)`);
          this._watchlist.delete(ca);
          this._saveWatchlist();
          this.saveSignalRecord(signal, 'V14_ATH2_FILTER', null);
          return { action: 'SKIP', reason: 'v14_ath2_filter', details: ath2Reasons };
        }

        // 并发仓位检查 (最多2个)
        const currentPositionCount = this.livePositionMonitor?.positions?.size || 0;
        if (currentPositionCount >= 2) {
          console.log(`⏭️ [v14] 当前${currentPositionCount}个持仓 >= 2 → 不开新仓`);
          return { action: 'SKIP', reason: 'max_positions_v14' };
        }

        finalSize = 0.07;
        tradeConviction = 'HIGH';
        console.log(`🎯 [v14.4] $${signal.symbol} ATH#2 ✅ 确认买入! MC增长=${mcGrowth.toFixed(2)}x Security=${securityCurrent} Address=${addressCurrent} → 买 ${finalSize} SOL`);
        console.log(`  MC1=$${(watchItem.mc1/1000).toFixed(1)}K → MC2=$${(mc/1000).toFixed(1)}K | 间隔=${((Date.now() - watchItem.entryTime)/60000).toFixed(1)}min`);

      } else {
        // ATH#3+: 不再入场
        console.log(`⏭️ [v14] $${signal.symbol} ATH#${currentAthNum} ≥3 → 不再入场`);
        this.saveSignalRecord(signal, 'V14_ATH3_PLUS', null);
        return { action: 'SKIP', reason: 'v14_ath3_plus', ath_num: currentAthNum };
      }

      // 构造兼容的AI结果
      const aiResult = {
        action: 'BUY_FULL',
        confidence: 90,
        narrative_tier: 'CONFIRMED',
        narrative_reason: `v14: ATH#2确认 Trade=${tradeCurrent} Security=${securityCurrent} MC=$${(mc/1000).toFixed(1)}K`,
        entry_timing: 'OPTIMAL',
        stop_loss_percent: 25
      };

      // 已持仓检查
      const alreadyHolding = this.livePositionMonitor?.positions?.has(ca);
      if (alreadyHolding) {
        console.log(`⏭️ [已持仓] $${signal.symbol} ATH#2 已有仓位，跳过`);
        this._watchlist.delete(ca);
        this._saveWatchlist();
        return { action: 'SKIP', reason: 'already_holding' };
      }

      // Shadow已持仓检查
      if (this.shadowTracker.hasOpenPosition(ca)) {
        console.log(`⏭️ [已持仓] $${signal.symbol} Shadow已有未平仓持仓，跳过`);
        return { action: 'SKIP', reason: 'already_in_position' };
      }

      // Exit Gates
      console.log('🚪 [Exit Gates] 退出可行性检查...');
      const exitResult = this.exitGateFilter.evaluate(snapshot, finalSize);
      if (exitResult.status === 'REJECT') {
        this.stats.exit_gate_rejected++;
        console.log(`🚫 [Exit Gates] REJECT: ${exitResult.reasons.join(', ')}`);
        this.saveSignalRecord(signal, gateResult.status, aiResult);
        return { action: 'SKIP', reason: 'exit_gate_reject', details: exitResult.reasons };
      }
      console.log(`✅ [Exit Gates] ${exitResult.status}`);

      // ===== 执行交易 =====
      if (this.shadowMode) {
        this.stats.shadow_logged++;
        console.log(`🎭 [SHADOW] 模拟买入 $${signal.symbol} | ${finalSize} SOL`);
        this.saveSignalRecord(signal, gateResult.status, aiResult, true);
        this.saveShadowTrade(signal, aiResult, finalSize);

        let entryMC = dexData?.market_cap || signal.market_cap || 0;
        if (this.livePriceMonitor) {
          const livePrice = this.livePriceMonitor.priceCache.get(ca);
          if (livePrice && livePrice.mc && (Date.now() - livePrice.timestamp) < 10000) {
            entryMC = livePrice.mc;
          }
        }

        this.shadowTracker.addPosition(ca, signal.symbol || 'UNKNOWN', entryMC, aiResult.confidence);
        this._watchlist.delete(ca);
        this._saveWatchlist();
        if (this.livePriceMonitor) this.livePriceMonitor.addToken(ca);

        return { action: 'SHADOW_BUY', size: finalSize, ai: aiResult };
      }

      if (!this.autoBuyEnabled) {
        console.log(`📋 [通知] 建议买入 $${signal.symbol} | ${finalSize} SOL (自动买入未开启)`);
        this.saveSignalRecord(signal, gateResult.status, aiResult, false);
        return { action: 'NOTIFY', size: finalSize, ai: aiResult };
      }

      // 实盘买入前：检查链上 SOL 余额
      if (this.jupiterExecutor) {
        try {
          const solBalance = await this.jupiterExecutor.getSolBalance();
          const minRequired = finalSize + 0.025;
          if (solBalance < minRequired) {
            console.log(`⛔ [余额不足] SOL余额: ${solBalance.toFixed(4)} < 需要: ${minRequired.toFixed(4)} → 跳过 $${signal.symbol}`);
            this.saveSignalRecord(signal, gateResult.status, aiResult, false);
            return { action: 'SKIP_INSUFFICIENT_BALANCE', balance: solBalance, required: minRequired };
          }
          console.log(`💰 [余额检查] SOL余额: ${solBalance.toFixed(4)} ≥ 需要: ${minRequired.toFixed(4)} → 可以买入`);
        } catch (e) {
          console.warn(`⚠️ [余额检查] 查询失败: ${e.message}，继续买入`);
        }
      }

      // 实盘执行 (ATH#2确认买入)
      console.log(`💰 [执行] ATH#2确认买入 $${signal.symbol} | ${finalSize} SOL...`);

      try {
        let tradeResult;

        if (this.jupiterExecutor) {
          tradeResult = await this.jupiterExecutor.buy(ca, finalSize, {
            liquidity: dexData?.liquidity_usd || 0,
            mc: mc || 0
          });

          if (tradeResult.success && this.livePositionMonitor) {
            const entryMC = dexData?.market_cap || signal.market_cap || 0;

            // 等待余额更新
            await new Promise(r => setTimeout(r, 3000));
            const balance = await this.jupiterExecutor.getTokenBalance(ca);

            if (balance.amount <= 0) {
              console.error(`❌ [验证失败] 买入交易发送但余额为0，交易可能失败`);
              this.stats.errors++;
              return { action: 'EXEC_FAILED', reason: '买入后余额为0' };
            }

            const tokenAmount = balance.amount;
            const tokenDecimals = balance.decimals || 6;
            console.log(`📦 [Token] 验证余额: ${tokenAmount} tokens (decimals: ${tokenDecimals})`);

            const actualTokenAmount = tokenAmount / Math.pow(10, tokenDecimals);
            const entryPrice = finalSize / actualTokenAmount;
            console.log(`💰 [Entry] 入场价格: ${entryPrice.toFixed(10)} SOL/token | ${finalSize} SOL → ${actualTokenAmount.toFixed(2)} tokens`);

            this.livePositionMonitor.addPosition(
              ca, signal.symbol, entryPrice, entryMC, finalSize,
              tokenAmount, tokenDecimals, tradeConviction
            );

            // 从观察列表移除
            this._watchlist.delete(ca);
            this._saveWatchlist();
            console.log(`🎯 [v14] ATH#2确认买入完成 $${signal.symbol} | ${finalSize} SOL`);

            this.recentSymbols.set(signal.symbol, Date.now());
          }
        } else {
          tradeResult = await this.executor.executeBuy(ca, 'SOL', finalSize);
        }

        this.stats.executed++;
        console.log(`✅ [执行] 交易成功: ${JSON.stringify(tradeResult)}`);
        this.saveSignalRecord(signal, gateResult.status, aiResult, true);
        return { action: 'EXECUTED', size: finalSize, ai: aiResult, trade: tradeResult };
      } catch (execError) {
        this.stats.errors++;
        console.error(`❌ [执行] 交易失败: ${execError.message}`);
        this.saveSignalRecord(signal, gateResult.status, aiResult, false);
        return { action: 'EXEC_FAILED', reason: execError.message };
      }

    } catch (error) {
      this.stats.errors++;
      console.error(`❌ [Premium] Pipeline 异常: ${error.message}`);
      return { action: 'ERROR', reason: error.message };
    }
  }

  /**
   * 去重检查 (5分钟窗口)
   */
  isDuplicate(tokenCA) {
    const lastSeen = this.recentSignals.get(tokenCA);
    if (!lastSeen) return false;
    return (Date.now() - lastSeen) < 5 * 60 * 1000;
  }

  markProcessed(tokenCA) {
    this.recentSignals.set(tokenCA, Date.now());
    // 清理过期记录
    const cutoff = Date.now() - 10 * 60 * 1000;
    for (const [ca, ts] of this.recentSignals) {
      if (ts < cutoff) this.recentSignals.delete(ca);
    }
  }

  /**
   * 🔧 标记退出冷却（10分钟内同symbol不再买入）
   */
  markExitCooldown(symbol) {
    this.exitCooldown.set(symbol, Date.now() + 10 * 60 * 1000);
    // 清理过期冷却记录
    for (const [sym, until] of this.exitCooldown) {
      if (until < Date.now()) this.exitCooldown.delete(sym);
    }
    // 同时清理过期symbol记录
    const symCutoff = Date.now() - 15 * 60 * 1000;
    for (const [sym, ts] of this.recentSymbols) {
      if (ts < symCutoff) this.recentSymbols.delete(sym);
    }
  }

  /**
   * 保存信号记录到数据库
   */
  saveSignalRecord(signal, gateStatus, aiResult, executed = false) {
    try {
      this.db.prepare(`
        INSERT INTO premium_signals (
          token_ca, symbol, market_cap, holders, volume_24h, top10_pct,
          age, description, timestamp, hard_gate_status,
          ai_action, ai_confidence, ai_narrative_tier, executed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        signal.token_ca,
        signal.symbol || null,
        signal.market_cap || null,
        signal.holders || null,
        signal.volume_24h || null,
        signal.top10_pct || null,
        signal.age || null,
        signal.description || null,
        signal.timestamp || Date.now(),
        gateStatus,
        aiResult?.action || null,
        aiResult?.confidence || null,
        aiResult?.narrative_tier || null,
        executed ? 1 : 0
      );
    } catch (error) {
      console.error('❌ [DB] 保存信号记录失败:', error.message);
    }
  }

  /**
   * 保存 Shadow 交易记录
   */
  saveShadowTrade(signal, aiResult, positionSize) {
    try {
      // 先确保 tokens 表有记录（外键约束）
      this.db.prepare(`
        INSERT OR IGNORE INTO tokens (token_ca, chain, symbol, first_seen_at, mc_at_signal) VALUES (?, 'SOL', ?, ?, ?)
      `).run(signal.token_ca, signal.symbol || null, Math.floor(Date.now() / 1000), signal.market_cap || null);

      const now = Date.now();
      this.db.prepare(`
        INSERT INTO trades (
          token_ca, chain, action, position_size, entry_time, entry_price, timestamp,
          symbol, narrative, rating, status, is_simulation
        ) VALUES (?, 'SOL', 'BUY', ?, ?, 0, ?, ?, ?, ?, 'OPEN', 1)
      `).run(
        signal.token_ca,
        positionSize,
        Math.floor(now / 1000),  // entry_time (seconds)
        now,                      // timestamp (milliseconds)
        signal.symbol || null,
        aiResult.narrative_reason || null,
        aiResult.narrative_tier || null
      );
    } catch (error) {
      console.warn('⚠️  [DB] Shadow 交易记录保存失败 (非关键):', error.message);
    }
  }

  // ===== v13: ATH计数持久化 =====

  /**
   * 保存ATH计数到JSON文件（容器重启后恢复）
   */
  _saveAthCounts() {
    try {
      const data = {};
      for (const [ca, history] of this.signalHistory) {
        if (history.athCount && history.athCount > 0) {
          data[ca] = {
            athCount: history.athCount,
            symbol: history.symbol,
            firstSeen: history.firstSeen,
            lastSeen: history.lastSeen,
            firstSuperIndex: history.firstSuperIndex || null,
            lastSuperIndex: history.lastSuperIndex || null,
            mc1: history.mc1 || null
          };
        }
      }
      const dir = path.dirname(this._athCountsPath);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(this._athCountsPath, JSON.stringify(data, null, 2));
    } catch (e) {
      console.warn(`⚠️ [ATH] 保存ATH计数失败: ${e.message}`);
    }
  }

  /**
   * 从JSON文件加载ATH计数（启动时调用）
   */
  _loadAthCounts() {
    try {
      if (!fs.existsSync(this._athCountsPath)) {
        console.log('📝 [ATH] 无历史ATH文件，从零开始');
        return;
      }
      const raw = fs.readFileSync(this._athCountsPath, 'utf-8');
      const data = JSON.parse(raw);
      let loaded = 0;
      for (const [ca, info] of Object.entries(data)) {
        const existing = this.signalHistory.get(ca);
        if (existing) {
          existing.athCount = Math.max(existing.athCount || 0, info.athCount);
          if (info.mc1 && !existing.mc1) existing.mc1 = info.mc1;
        } else {
          this.signalHistory.set(ca, {
            count: info.athCount,
            firstSeen: info.firstSeen || Date.now(),
            lastSeen: info.lastSeen || Date.now(),
            symbol: info.symbol || 'UNKNOWN',
            firstMC: 0,
            latestMC: 0,
            lastScore: 0,
            athCount: info.athCount,
            firstSuperIndex: info.firstSuperIndex || null,
            lastSuperIndex: info.lastSuperIndex || null,
            mc1: info.mc1 || null
          });
        }
        loaded++;
      }
      console.log(`✅ [ATH] 已加载${loaded}个代币的ATH计数`);
    } catch (e) {
      console.warn(`⚠️ [ATH] 加载ATH计数失败: ${e.message}`);
    }
  }

  // ===== v13: SOL市场环境检查 =====

  // ===== v14: 观察列表持久化 =====

  _saveWatchlist() {
    try {
      const data = {};
      for (const [ca, item] of this._watchlist) {
        data[ca] = {
          symbol: item.symbol,
          mc1: item.mc1,
          entryTime: item.entryTime
        };
      }
      const dir = path.dirname(this._watchlistPath);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(this._watchlistPath, JSON.stringify(data, null, 2));
    } catch (e) {
      console.warn(`⚠️ [v14] 保存观察列表失败: ${e.message}`);
    }
  }

  _loadWatchlist() {
    try {
      if (!fs.existsSync(this._watchlistPath)) {
        console.log('📝 [v14] 无历史观察列表文件，从零开始');
        return;
      }
      const raw = fs.readFileSync(this._watchlistPath, 'utf-8');
      const data = JSON.parse(raw);
      const timeout = 2 * 60 * 60 * 1000; // 2小时
      const now = Date.now();
      let loaded = 0, expired = 0;
      for (const [ca, info] of Object.entries(data)) {
        if (now - info.entryTime > timeout) {
          expired++;
          continue;
        }
        this._watchlist.set(ca, info);
        loaded++;
      }
      console.log(`✅ [v14] 已加载${loaded}个观察列表条目${expired > 0 ? ` (${expired}个已超时)` : ''}`);
    } catch (e) {
      console.warn(`⚠️ [v14] 加载观察列表失败: ${e.message}`);
    }
  }

  /**
   * 启动定期SOL市场检查（每5分钟）
   */
  _startSolMarketCheck() {
    // 立即检查一次
    this._checkSolMarket();
    // 每5分钟检查
    this._solMarketCheckInterval = setInterval(() => this._checkSolMarket(), 5 * 60 * 1000);
    console.log('✅ [SOL市场] 市场环境检查已启动（每5分钟）');
  }

  /**
   * 检查SOL 24h价格变化，跌>10%则暂停开仓
   */
  async _checkSolMarket() {
    try {
      const res = await axios.get('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd&include_24hr_change=true', { timeout: 10000 });
      const change24h = res.data?.solana?.usd_24h_change || 0;
      const wasPaused = this._solMarketPaused;
      this._solMarketPaused = change24h < -10;
      if (this._solMarketPaused && !wasPaused) {
        console.log(`⚠️ [SOL市场] SOL 24h变化: ${change24h.toFixed(1)}% → 暂停开仓`);
      } else if (!this._solMarketPaused && wasPaused) {
        console.log(`✅ [SOL市场] SOL 24h变化: ${change24h.toFixed(1)}% → 恢复开仓`);
      }
    } catch (e) {
      // CoinGecko限流时不改变状态
      console.log(`⚠️ [SOL市场] 价格查询失败: ${e.message}，保持当前状态`);
    }
  }

  /**
   * 停止引擎
   */
  async stop() {
    if (this._solMarketCheckInterval) {
      clearInterval(this._solMarketCheckInterval);
      this._solMarketCheckInterval = null;
    }
    // 停止前保存ATH计数
    this._saveAthCounts();
    if (this.shadowTracker) {
      this.shadowTracker.stop();
    }
    if (this.livePositionMonitor) {
      this.livePositionMonitor.stop();
    }
    if (!this.shadowMode && !this.jupiterExecutor) {
      await this.executor.disconnect();
    }
    console.log('⏹️  [Premium Engine] 已停止');
  }

  /**
   * 获取统计信息
   */
  getStats() {
    return {
      ...this.stats,
      mode: this.shadowMode ? 'SHADOW' : 'LIVE',
      position_sol: this.positionSol,
      max_positions: this.maxPositions,
      dedup_cache_size: this.recentSignals.size
    };
  }
}

export default PremiumSignalEngine;
