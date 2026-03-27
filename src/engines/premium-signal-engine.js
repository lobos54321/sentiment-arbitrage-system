/**
 * Premium Signal Engine — v18
 *
 * 独立的信号处理引擎，专门处理付费频道信号
 * Pipeline: 信号 → 预检 → 链上快照 → Hard Gates → v18条件过滤 → 执行
 * 
 * v18: 精准掐头去尾 + 非对称收割
 * - ATH#1直接入场
 * - 入场条件: MC 30-300K + Super_cur 80-1000 + SupΔ≥5 + Trade_cur≥1&TΔ≥1 + Addr_cur≥3 + Sec_cur≥15
 * - 仓位: 0.06 SOL
 * - 出场: ASYMMETRIC (SL-35%→TP1@50%卖60%→SL移至0%→TP2@100%/TP3@200%/TP4@500% + 15分死水/30分大限)
 * - 回测(18h): 40笔, 65%WR, ROI=+16%, 盈亏比1.26
 */

import fs from 'fs';
import path from 'path';
import { atomicWriteJSON } from '../utils/atomic-write.js';
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
import { RiskManager } from '../risk/risk-manager.js';
import { MarketDataBackfillService } from '../market-data/market-data-backfill-service.js';
import axios from 'axios';

export class PremiumSignalEngine {
  constructor(config, db) {
    this.config = config;
    this.db = db;
    this.riskManager = new RiskManager(config, db);

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
    this.paperStrategyRegistry = null;
    this.paperTradeRecorder = null;
    this.marketDataBackfill = new MarketDataBackfillService();
    this._poolCache = new Map();
    this._klineResultCache = new Map();
    this._klineApiCooldownUntil = 0;
    this._lastKlineRateLimitLogAt = 0;
    this._klinePriming = new Map();
    this._klinePrimeCooldownUntil = 0;
    this._lastKlinePrimeLogAt = 0;
    this._klinePrimeMinGapMs = parseInt(process.env.KLINE_PRIME_MIN_GAP_MS || '30000', 10);

    // 去重（短期 5 分钟）
    this.recentSignals = new Map(); // token_ca → timestamp
    // 🔧 Symbol级去重（15分钟窗口）— 防止同名不同CA的仿盘
    this.recentSymbols = new Map(); // symbol → timestamp
    // 🔧 退出后冷却（10分钟）— 防止退出后立即再买同名代币
    this.exitCooldown = new Map(); // symbol → timestamp
    // 信号历史（长期追踪重复信号）
    this.signalHistory = new Map(); // token_ca → { count, firstSeen, lastSeen, symbol }

    // 每小时清理超过24h的信号历史，防止内存泄漏
    setInterval(() => this._cleanupSignalHistory(), 60 * 60 * 1000);

    // v13: SOL市场环境暂停标志
    this._solMarketPaused = false;
    this._solMarketCheckInterval = null;

    // ATH计数持久化路径
    this._athCountsPath = path.join(process.cwd(), 'data', 'ath_counts.json');

    // v17: ATH#1直接入场，不再需要观察列表
    // 保留Map以兼容旧代码引用
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

  _checkEntryRisk(symbol, context = 'ENTRY') {
    if (this.shadowMode) {
      return { allowed: true, reason: 'SHADOW_SKIP_RISK' };
    }

    const riskCheck = this.riskManager.canTrade();
    if (!riskCheck.allowed) {
      console.log(`🛡️ [RISK] ${context === 'NOT_ATH' ? `$${symbol} ` : ''}风控拒绝: ${riskCheck.reason}${context === 'ATH' ? ` | $${symbol} ATH#1 计数未消耗` : ''}`);
    }
    return riskCheck;
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

      // v17: 不再需要观察列表（ATH#1直接入场）
      this._loadWatchlist(); // 兼容旧数据

      // v13: 启动SOL市场环境检查（每5分钟）
      this._startSolMarketCheck();

      // v17: 观察列表不再使用，清理旧数据
      this._watchlist.clear();
      this._saveWatchlist();

      console.log('✅ [Premium Engine] 所有服务初始化完成');

      // JS Shadow 交易链路已停用，Paper Trade 由独立 monitor 负责
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
        pool_address TEXT,
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
    addCol('trades', 'pool_address', 'TEXT');

    // K线评分数据表
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS kline_1m (
        token_ca TEXT NOT NULL,
        pool_address TEXT NOT NULL DEFAULT '',
        timestamp INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL DEFAULT 0,
        score INTEGER,
        pullback REAL,
        vol_ratio REAL,
        wick_ratio REAL,
        ema21 REAL,
        source TEXT DEFAULT 'geckoterminal',
        created_at INTEGER DEFAULT (strftime('%s', 'now')),
        PRIMARY KEY (token_ca, timestamp)
      )
    `);
    this.db.exec(`CREATE INDEX IF NOT EXISTS idx_kline_1m_ts ON kline_1m(token_ca, timestamp)`);
    // 兼容旧库：补新增列
    addCol('kline_1m', 'score', 'INTEGER');
    addCol('kline_1m', 'pullback', 'REAL');
    addCol('kline_1m', 'vol_ratio', 'REAL');
    addCol('kline_1m', 'wick_ratio', 'REAL');
    addCol('kline_1m', 'ema21', 'REAL');
    addCol('kline_1m', 'source', 'TEXT');
    this._klineInsertStmt = this.db.prepare(`
      INSERT OR REPLACE INTO kline_1m (token_ca, pool_address, timestamp, open, high, low, close, volume, score, pullback, vol_ratio, wick_ratio, ema21, source)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'geckoterminal')
    `);
  }

  /**
   * 处理信号 - 主 Pipeline
   */
  async processSignal(signal) {
    this.stats.signals_received++;
    const ca = signal.token_ca;
    const shortCA = ca.substring(0, 8);
    const t0 = Date.now();

    console.log('\n' + '═'.repeat(60));
    console.log(`💎 [Premium] 新信号: $${signal.symbol || shortCA} | MC: $${signal.market_cap ? (signal.market_cap / 1000).toFixed(1) + 'K' : '?'}`);
    console.log('═'.repeat(60));

    try {
      // ─── Step 1: 去重 + 冷却检查 (全内存, ~0ms) ───────────────
      const history = this.signalHistory.get(ca);
      if (history) {
        history.count++;
        history.lastSeen = Date.now();
        if (signal.market_cap > 0) history.latestMC = signal.market_cap;
      } else {
        this.signalHistory.set(ca, {
          count: 1, firstSeen: Date.now(), lastSeen: Date.now(),
          symbol: signal.symbol, firstMC: signal.market_cap || 0,
          latestMC: signal.market_cap || 0, lastScore: 0
        });
      }

      if (this.isDuplicate(ca)) {
        this.stats.duplicates_skipped++;
        console.log(`⏭️  [去重] ${shortCA}... 5分钟内已处理，跳过`);
        return { action: 'SKIP', reason: 'duplicate' };
      }
      this.markProcessed(ca);

      const symbol = signal.symbol;
      const lastSymbolSeen = this.recentSymbols.get(symbol);
      if (lastSymbolSeen && (Date.now() - lastSymbolSeen) < 15 * 60 * 1000) {
        this.stats.duplicates_skipped++;
        console.log(`⏭️  [Symbol去重] $${symbol} 15分钟内已处理过同名代币，跳过`);
        return { action: 'SKIP', reason: 'symbol_duplicate' };
      }

      const cooldownUntil = this.exitCooldown.get(symbol);
      if (cooldownUntil && Date.now() < cooldownUntil) {
        const remainSec = Math.round((cooldownUntil - Date.now()) / 1000);
        console.log(`⏭️  [冷却中] $${symbol} 退出后冷却期，剩余${remainSec}s，跳过`);
        return { action: 'SKIP', reason: 'exit_cooldown' };
      }

      // ─── Step 2: ATH 检查 — 最先过滤，非ATH检查 super_index (~0ms) ───
      const isATH = signal.is_ath === true;
      const signalIndices = signal.indices || {};
      const superIndex = signalIndices?.super_index?.current || signalIndices?.super_index?.value || 0;

      if (!isATH) {
        // NOT_ATH 信号：需要 super_index >= 80 才允许交易
        if (superIndex < 80) {
          console.log(`⏭️ [NOT_ATH] $${signal.symbol} Super=${superIndex}<80 → 跳过`);
          this.saveSignalRecord(signal, 'NOT_ATH_V17', null);
          this._trackForKline(ca, signal.symbol);
          return { action: 'SKIP', reason: 'not_ath_v17' };
        }
        // NOT_ATH + super>=80：独立执行路径 — 红K + vol≥2000
        return await this._executeNotAth(ca, signal);
      }

      // ─── Step 3: 市场环境 + Freeze/Mint 预检 (全内存, ~0ms) ───
      if (this._solMarketPaused) {
        console.log(`⏸️ [v17] SOL 24h跌>10%，市场暂停中，不开新仓`);
        return { action: 'SKIP', reason: 'market_paused' };
      }

      if (signal.freeze_ok === false || signal.mint_ok === false) {
        this.stats.precheck_failed++;
        console.log(`🚫 [预检] freeze=${signal.freeze_ok} mint=${signal.mint_ok} → 跳过`);
        this.saveSignalRecord(signal, 'PRECHECK_FAIL', null);
        return { action: 'SKIP', reason: 'precheck_failed' };
      }
      if (signal.freeze_ok === null || signal.mint_ok === null) {
        console.log(`⚠️ [预检] freeze=${signal.freeze_ok} mint=${signal.mint_ok} 未知(ATH信号无此数据)，继续`);
      }

      // ─── Step 4: v18 所有过滤 — 全部来自 signal.indices (~0ms) ─
      const idx = signal.indices;
      this.recordPaperComparisons(signal);
      const superCurrent = idx?.super_index?.current || 0;
      const superSignal  = idx?.super_index?.signal  || 0;
      const superDelta   = superCurrent - superSignal;
      const tradeCurrent = idx?.trade_index?.current || 0;
      const tradeSignal  = idx?.trade_index?.signal  || 0;
      const tradeDelta   = tradeCurrent - tradeSignal;
      const addressCurrent  = idx?.address_index?.current  || 0;
      const securityCurrent = idx?.security_index?.current || 0;

      // ATH 计数器 — 先读取但不递增，等所有过滤通过后才提交
      const sigHistory = this.signalHistory.get(ca);
      const prevAthCount = sigHistory ? (sigHistory.athCount || 0) : 0;
      const currentAthNum = prevAthCount + 1;

      if (currentAthNum !== 1) {
        console.log(`⏭️ [v17] $${signal.symbol} ATH#${currentAthNum} → 仅ATH#1入场`);
        this.saveSignalRecord(signal, 'V17_NOT_ATH1', null);
        return { action: 'SKIP', reason: 'v17_only_ath1' };
      }

      // ─── 风控检查（在 ATH 计数提交之前）─────────────────────────
      // 风控拒绝不应消耗 ATH#1 — 损失限额重置后该 token 仍应有机会进场
      const riskCheck = this._checkEntryRisk(signal.symbol, 'ATH');
      if (!riskCheck.allowed) {
        this.saveSignalRecord(signal, 'RISK_BLOCKED', null);
        return { action: 'SKIP', reason: `risk: ${riskCheck.reason}` };
      }

      // 风控通过，立即提交 ATH 计数 — 即使后续过滤拒绝，也标记此 token 已经处理过 ATH#1
      if (!sigHistory) {
        this.signalHistory.set(ca, { athCount: 1, lastSeen: Date.now() });
      } else {
        sigHistory.athCount = (sigHistory.athCount || 0) + 1;
        sigHistory.lastSeen = Date.now();
      }
      this._saveAthCounts();

      // 已持仓检查
      if (this.livePositionMonitor?.positions?.has(ca)) {
        console.log(`⏭️ [v17] $${signal.symbol} 已持仓 → 跳过`);
        return { action: 'SKIP', reason: 'already_holding' };
      }
      if (this.shadowMode && this.shadowTracker.hasOpenPosition(ca)) {
        console.log(`⏭️ [已持仓] $${signal.symbol} Shadow已有未平仓持仓，跳过`);
        return { action: 'SKIP', reason: 'already_in_position' };
      }

      // 仓位槽位检查 — 只算在险仓位
      const allPositions = this.livePositionMonitor?.positions;
      const atRiskCount = allPositions ? [...allPositions.values()].filter(p => !p.tp1).length : 0;
      const moonBagCount = (allPositions?.size || 0) - atRiskCount;
      if (atRiskCount >= 5) {
        console.log(`⏭️ [v18] 在险仓位 ${atRiskCount}/5 已满 → 不开新仓 (${moonBagCount}个零成本登月仓不占槽)`);
        return { action: 'SKIP', reason: 'max_atrisk_positions' };
      }
      if (moonBagCount > 0) {
        console.log(`   ℹ️ [槽位] 在险: ${atRiskCount}/5 | 零成本登月仓: ${moonBagCount}个`);
      }

      // v18 指标过滤 (全部 signal.indices，无网络请求)
      const mc = signal.market_cap || 0;
      if (mc < 30000 || mc > 300000) {
        console.log(`⏭️ [v18] MC=$${(mc/1000).toFixed(1)}K 不在$30-300K → 跳过`);
        this.saveSignalRecord(signal, 'V18_MC_FILTER', null);
        return { action: 'SKIP', reason: 'v18_mc_filter', mc };
      }
      if (superCurrent < 80 || superCurrent > 1000) {
        console.log(`⏭️ [v18] Super_cur=${superCurrent} 不在80-1000 → 跳过`);
        this.saveSignalRecord(signal, 'V18_SUPERCUR_FILTER', null);
        return { action: 'SKIP', reason: 'v18_supercur_filter', superCurrent };
      }
      if (superDelta < 5) {
        console.log(`⏭️ [v18] SupΔ=${superDelta}<5 → 跳过`);
        this.saveSignalRecord(signal, 'V18_SUPDELTA_FILTER', null);
        return { action: 'SKIP', reason: 'v18_supdelta_filter', superDelta };
      }
      if (tradeCurrent < 1) {
        console.log(`⏭️ [v18] Trade_cur=${tradeCurrent}<1 → 跳过`);
        this.saveSignalRecord(signal, 'V18_TRADECUR_FILTER', null);
        return { action: 'SKIP', reason: 'v18_tradecur_filter', tradeCurrent };
      }
      if (tradeDelta < 1) {
        console.log(`⏭️ [v18] TΔ=${tradeDelta}<1 → 跳过`);
        this.saveSignalRecord(signal, 'V18_TRADEDELTA_FILTER', null);
        return { action: 'SKIP', reason: 'v18_tradedelta_filter', tradeDelta };
      }
      if (addressCurrent < 3) {
        console.log(`⏭️ [v18] Addr_cur=${addressCurrent}<3 → 跳过`);
        this.saveSignalRecord(signal, 'V18_ADDR_FILTER', null);
        return { action: 'SKIP', reason: 'v18_addr_filter', addressCurrent };
      }
      if (securityCurrent < 15) {
        console.log(`⏭️ [v18] Sec_cur=${securityCurrent}<15 → 跳过`);
        this.saveSignalRecord(signal, 'V18_SEC_FILTER', null);
        return { action: 'SKIP', reason: 'v18_sec_filter', securityCurrent };
      }

      // ─── Step 5: 唯一网络请求 — 实时价格查询 + 防追高 (~200ms) ─
      // 优先 livePriceMonitor 缓存（0ms），否则查 Jupiter Price API
      const liveMC = this._getCachedMC(ca);
      if (liveMC > 0 && mc > 0 && liveMC > mc * 1.20) {
        const premium = ((liveMC / mc - 1) * 100).toFixed(1);
        console.log(`🚫 [防追高] $${signal.symbol} 信号MC=$${(mc/1000).toFixed(1)}K → 实时MC=$${(liveMC/1000).toFixed(1)}K (溢价+${premium}% > 20%) → 放弃`);
        this.saveSignalRecord(signal, 'ANTI_CHASE', null);
        return { action: 'SKIP', reason: 'anti_chase', premium: parseFloat(premium) };
      }

      const elapsed = Date.now() - t0;
      console.log(`🎯 [v18] $${signal.symbol} ATH#1 ✅ MC=$${(mc/1000).toFixed(1)}K Super=${superCurrent}(Δ${superDelta}) Trade=${tradeCurrent}(Δ${tradeDelta}) Addr=${addressCurrent} Sec=${securityCurrent} | 决策耗时:${elapsed}ms`);

      // 所有过滤通过，补充 sigHistory 元数据（ATH 计数已在过滤前提交）
      const updatedHistory = this.signalHistory.get(ca);
      if (updatedHistory) {
        if (idx?.super_index) {
          updatedHistory.lastSuperIndex = superCurrent;
          if (!updatedHistory.firstSuperIndex) updatedHistory.firstSuperIndex = superSignal;
        }
        if (prevAthCount === 0 && signal.market_cap > 0) updatedHistory.mc1 = signal.market_cap;
      }

      const finalSize = 0.06;
      const exitStrategy = 'ASYMMETRIC';
      const tradeConviction = 'HIGH';

      const aiResult = {
        action: 'BUY_FULL', confidence: 90,
        narrative_tier: 'CONFIRMED',
        narrative_reason: `v18: ATH#1 MC=$${(mc/1000).toFixed(1)}K Super_cur=${superCurrent} SupΔ=${superDelta} TΔ=${tradeDelta} Addr=${addressCurrent} Sec=${securityCurrent}`,
        entry_timing: 'OPTIMAL', stop_loss_percent: 35,
        exitStrategy
      };

      // ─── Step 6: 执行 ─────────────────────────────────────────

      // FBR过滤：检查第一根K线是否绿色
      const fbrResult = await this._checkFBR(ca);
      if (!fbrResult.passed) {
        console.log(`🚫 [FBR] $${signal.symbol} 第一根K线红色 (open=${fbrResult.open?.toFixed(10)} close=${fbrResult.close?.toFixed(10)} FBR=${fbrResult.fbr?.toFixed(2)}%) → 跳过`);
        this.saveSignalRecord(signal, 'FBR_FAIL', aiResult, false);
        return { action: 'SKIP', reason: 'fbr_failed', fbr: fbrResult.fbr };
      }
      if (fbrResult.reason !== 'error_skip') {
        console.log(`✅ [FBR] $${signal.symbol} 第一根K线绿色 (FBR=${fbrResult.fbr?.toFixed(2)}%)`);
      }

      if (this.shadowMode) {
        console.log(`📋 [PAPER_ONLY] Shadow执行已停用，$${signal.symbol} 仅记录信号，不在JS引擎内开模拟仓`);
        this.saveSignalRecord(signal, 'PASS', aiResult, false);
        return { action: 'PAPER_ONLY_SKIP', reason: 'shadow_disabled', ai: aiResult };
      }

      if (!this.autoBuyEnabled) {
        console.log(`📋 [通知] 建议买入 $${signal.symbol} | ${finalSize} SOL (自动买入未开启)`);
        this.saveSignalRecord(signal, 'PASS', aiResult, false);
        return { action: 'NOTIFY', size: finalSize, ai: aiResult };
      }

      // SOL 余额检查
      if (this.jupiterExecutor) {
        try {
          const solBalance = await this.jupiterExecutor.getSolBalance();
          const minRequired = finalSize + 0.025;
          if (solBalance < minRequired) {
            console.log(`⛔ [余额不足] SOL余额: ${solBalance.toFixed(4)} < 需要: ${minRequired.toFixed(4)} → 跳过`);
            this.saveSignalRecord(signal, 'PASS', aiResult, false);
            return { action: 'SKIP_INSUFFICIENT_BALANCE', balance: solBalance, required: minRequired };
          }
          console.log(`💰 [余额] ${solBalance.toFixed(4)} SOL ≥ ${minRequired.toFixed(4)} → 可买入`);
        } catch (e) {
          console.warn(`⚠️ [余额检查] 查询失败: ${e.message}，继续`);
        }
      }

      console.log(`💰 [执行] 买入 $${signal.symbol} | ${finalSize} SOL | ${exitStrategy}...`);

      try {
        let tradeResult;
        if (this.jupiterExecutor) {
          tradeResult = await this.jupiterExecutor.buy(ca, finalSize, { mc: mc || 0 });

          if (tradeResult.success && this.livePositionMonitor) {
            // 等待余额更新
            await new Promise(r => setTimeout(r, 3000));
            const balance = await this.jupiterExecutor.getTokenBalance(ca);

            if (balance.amount <= 0) {
              console.error(`❌ [验证失败] 买入后余额为0，交易可能失败`);
              this.stats.errors++;
              return { action: 'EXEC_FAILED', reason: '买入后余额为0' };
            }

            const tokenAmount = balance.amount;
            const tokenDecimals = balance.decimals || 6;
            const actualTokenAmount = tokenAmount / Math.pow(10, tokenDecimals);
            const entryPrice = finalSize / actualTokenAmount;
            const entryMC = liveMC || mc;
            console.log(`💰 [Entry] ${entryPrice.toFixed(10)} SOL/token | ${finalSize} SOL → ${actualTokenAmount.toFixed(2)} tokens`);

            this.livePositionMonitor.addPosition(
              ca, signal.symbol, entryPrice, entryMC, finalSize,
              tokenAmount, tokenDecimals, tradeConviction, exitStrategy
            );
            this._watchlist.delete(ca);
            this.recentSymbols.set(signal.symbol, Date.now());
            // 注意：不要在这里调用 livePriceMonitor.addToken(ca) — addPosition 内部已正确注册（含 tokenAmount/decimals）
            // 重复调用只传 ca 会覆盖 V2 的 watchList 导致 tokenAmount=undefined，价格监控完全失效
            console.log(`🎯 [v18] 买入完成 $${signal.symbol} | 总耗时:${Date.now()-t0}ms`);
          }
        } else {
          tradeResult = await this.executor.executeBuy(ca, 'SOL', finalSize);
        }

        this.stats.executed++;
        this.saveSignalRecord(signal, 'PASS', aiResult, true);
        return { action: 'EXECUTED', size: finalSize, ai: aiResult, trade: tradeResult };
      } catch (execError) {
        this.stats.errors++;
        console.error(`❌ [执行] 交易失败: ${execError.message}`);
        this.saveSignalRecord(signal, 'PASS', aiResult, false);
        return { action: 'EXEC_FAILED', reason: execError.message };
      }

    } catch (error) {
      this.stats.errors++;
      console.error(`❌ [Premium] Pipeline 异常: ${error.message}`);
      return { action: 'ERROR', reason: error.message };
    }
  }

  /**
   * 从 livePriceMonitor 缓存中获取实时 MC（无网络请求）
   */
  _getCachedMC(ca) {
    if (!this.livePriceMonitor) return 0;
    const cached = this.livePriceMonitor.priceCache.get(ca);
    if (cached && cached.mc && (Date.now() - cached.timestamp) < 30000) {
      return cached.mc;
    }
    return 0;
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
   * 📊 NOT_ATH K线采集：临时加入价格监控，到期自动移除
   * 用途：为 Trending 信号回测积累完整 K 线数据
   * 限制：最多同时监控 50 个 NOT_ATH token，25 分钟后自动移除
   */
  _trackForKline(ca, symbol) {
    if (!this.livePriceMonitor) return;

    // 初始化追踪 map
    if (!this._klineTrackers) this._klineTrackers = new Map();

    // 已在追踪中，跳过
    if (this._klineTrackers.has(ca)) return;

    // 限制最大同时追踪数，防止 watchList 膨胀
    if (this._klineTrackers.size >= 30) {
      return;
    }

    this.livePriceMonitor.addToken(ca);
    const timer = setTimeout(() => {
      this.livePriceMonitor.removeToken(ca);
      this._klineTrackers.delete(ca);
    }, 20 * 60 * 1000); // 20 分钟

    this._klineTrackers.set(ca, timer);
    console.log(`📊 [KlineTrack] $${symbol} 加入临时监控 20min (追踪中: ${this._klineTrackers.size})`);
  }

  async _primeKlineCache(tokenCA, symbol = tokenCA.substring(0, 8)) {
    return false;
  }

  async _backfillPrebuyKlines(tokenCA, signalTsSec, targetBars = 5) {
    const existingBefore = this.marketDataBackfill.getBarsBefore(tokenCA, signalTsSec, targetBars).length;

    if (existingBefore >= targetBars) {
      return { fetched: 0, existingBefore, totalBefore: existingBefore, enough: true, provider: 'local' };
    }

    const lookbackStart = signalTsSec - Math.max(targetBars * 60, 30 * 60);
    const heliusResult = await this.marketDataBackfill.backfillWindow({
      tokenCa: tokenCA,
      signalTsSec,
      startTs: lookbackStart,
      endTs: signalTsSec - 60,
      minBars: targetBars
    });
    const heliusBarsBefore = this.marketDataBackfill.getBarsBefore(tokenCA, signalTsSec, Math.max(targetBars, 60)).length;

    if (heliusBarsBefore >= targetBars) {
      if (heliusResult.poolAddress) {
        this._poolCache.set(tokenCA, heliusResult.poolAddress);
      }
      console.log(`📊 [HeliusBackfill] ${tokenCA.substring(0, 8)} sigs=${heliusResult.signaturesFetched} txs=${heliusResult.transactionsFetched} trades=${heliusResult.tradesInserted} bars=${heliusResult.barsWritten} cacheHit=${heliusResult.cacheHit}`);
      return {
        fetched: Math.max(0, heliusBarsBefore - existingBefore),
        existingBefore,
        totalBefore: heliusBarsBefore,
        enough: true,
        provider: 'helius',
        poolAddress: heliusResult.poolAddress || null,
        metrics: heliusResult
      };
    }

    let poolAddress = heliusResult.poolAddress || this._poolCache?.get(tokenCA) || null;
    if (!poolAddress) {
      try {
        const dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${tokenCA}`, { timeout: 5000 });
        const pairs = dexRes.data?.pairs || [];
        const solPairs = pairs.filter(p => p.chainId === 'solana');
        const pool = solPairs?.length
          ? solPairs.sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0))[0]
          : pairs[0];
        poolAddress = pool?.pairAddress || null;
        if (poolAddress) this._poolCache.set(tokenCA, poolAddress);
      } catch (error) {
        if (error?.response?.status === 429) {
          this._klinePrimeCooldownUntil = Date.now() + 120_000;
        }
        return { fetched: 0, existingBefore, totalBefore: heliusBarsBefore, enough: false, provider: heliusResult.provider || null, reason: error.message };
      }
    }

    if (!poolAddress) {
      return { fetched: 0, existingBefore, totalBefore: heliusBarsBefore, enough: false, provider: heliusResult.provider || null, reason: heliusResult.error || 'no_pool' };
    }

    const windows = [signalTsSec + 600, signalTsSec + 3600];
    const byTs = new Map();
    const limit = Math.max(20, targetBars * 4);

    for (const windowEnd of windows) {
      try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/pools/${poolAddress}/ohlcv/minute?aggregate=1&limit=${limit}&before_timestamp=${windowEnd}&token=base`;
        const geckoRes = await axios.get(url, { timeout: 20000 });
        const list = geckoRes.data?.data?.attributes?.ohlcv_list || [];
        for (const row of list) {
          const ts = Number(row[0]);
          if (!Number.isFinite(ts) || ts >= signalTsSec || byTs.has(ts)) continue;
          byTs.set(ts, {
            ts,
            open: Number(row[1]),
            high: Number(row[2]),
            low: Number(row[3]),
            close: Number(row[4]),
            volume: Number(row[5])
          });
        }
      } catch (error) {
        if (error?.response?.status === 429) {
          this._klineApiCooldownUntil = Date.now() + 120_000;
          this._klinePrimeCooldownUntil = Date.now() + 120_000;
          return { fetched: byTs.size, existingBefore, totalBefore: heliusBarsBefore + byTs.size, enough: heliusBarsBefore + byTs.size >= targetBars, provider: 'geckoterminal', poolAddress, reason: 'rate_limited' };
        }
      }
    }

    const bars = [...byTs.values()].sort((a, b) => a.ts - b.ts);
    if (bars.length) {
      this._saveKlineBars(tokenCA, poolAddress, bars, {});
    }

    const totalBefore = heliusBarsBefore + bars.length;
    return {
      fetched: Math.max(0, totalBefore - existingBefore),
      existingBefore,
      totalBefore,
      enough: totalBefore >= targetBars,
      provider: bars.length ? 'geckoterminal' : heliusResult.provider,
      poolAddress,
      metrics: heliusResult
    };
  }

  async _waitForFreshLocalKlines(tokenCA, minBars = 4, waitMs = 1200) {
    const startedAt = Date.now();
    while (Date.now() - startedAt < waitMs) {
      try {
        const rows = this.db.prepare(`
          SELECT timestamp
          FROM kline_1m
          WHERE token_ca = ?
          ORDER BY timestamp DESC
          LIMIT ?
        `).all(tokenCA, minBars);
        if (rows?.length >= minBars) {
          const newestTs = Number(rows[0].timestamp || 0);
          if (Date.now() - newestTs * 1000 < 10 * 60_000) {
            return true;
          }
        }
      } catch {
        return false;
      }
      await new Promise(r => setTimeout(r, 150));
    }
    return false;
  }

  recordPaperComparisons(signal) {
    if (!this.paperStrategyRegistry || !this.paperTradeRecorder || process.env.AUTONOMY_PAPER_SHADOW !== 'true') {
      return;
    }

    try {
      const baseline = this.paperStrategyRegistry.getBaseline();
      const challenger = this.paperStrategyRegistry.getChallenger();
      const strategies = [
        baseline ? { role: 'baseline', candidate: baseline } : null,
        challenger ? { role: 'challenger', candidate: challenger } : null
      ].filter(Boolean);

      for (const item of strategies) {
        const decision = this.paperStrategyRegistry.evaluateSignal(signal, item.candidate);
        this.paperTradeRecorder.recordDecision({
          tradeId: `${item.role}-${item.candidate.id}-${signal.token_ca}-${signal.timestamp || Date.now()}`,
          strategyId: item.candidate.id,
          strategyRole: item.role,
          tokenCa: signal.token_ca,
          symbol: signal.symbol,
          chain: 'SOL',
          signalSource: 'premium',
          entryContext: {
            marketCap: signal.market_cap || 0,
            isAth: !!signal.is_ath
          },
          decisionContext: decision,
          paperEntry: decision.action === 'BUY' ? { enteredAt: new Date().toISOString(), marketCap: signal.market_cap || 0 } : {},
          paperExit: {},
          pnl: null,
          exitReason: null,
          status: decision.action === 'BUY' ? 'open' : 'skipped',
          createdAt: new Date().toISOString()
        });
      }
    } catch (error) {
      console.warn(`⚠️ [Paper Registry] comparison failed: ${error.message}`);
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
      const poolAddress = signal.pool_address || this._poolCache?.get(signal.token_ca) || null;
      this.db.prepare(`
        INSERT INTO trades (
          token_ca, chain, action, position_size, entry_time, entry_price, pool_address, timestamp,
          symbol, narrative, rating, status, is_simulation
        ) VALUES (?, 'SOL', 'BUY', ?, ?, 0, ?, ?, ?, ?, ?, 'OPEN', 1)
      `).run(
        signal.token_ca,
        positionSize,
        Math.floor(now / 1000),  // entry_time (seconds)
        poolAddress,
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
   * 清理超过24h的信号历史，防止内存泄漏
   */
  _cleanupSignalHistory() {
    const cutoff = Date.now() - 24 * 60 * 60 * 1000;
    let cleaned = 0;
    for (const [ca, history] of this.signalHistory) {
      if (history.lastSeen && history.lastSeen < cutoff) {
        this.signalHistory.delete(ca);
        cleaned++;
      }
    }
    if (cleaned > 0) {
      console.log(`🧹 [Cleanup] 清理了 ${cleaned} 条过期信号历史 (剩余 ${this.signalHistory.size})`);
    }
  }

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
      // 原子写入，防止崩溃时数据损坏
      atomicWriteJSON(this._athCountsPath, data).catch((err) => {
        console.warn(`⚠️ [ATH] 原子写入失败: ${err.message}`);
      });
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

  // ===== v16: 观察列表持久化 =====

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
      console.warn(`⚠️ [v16] 保存观察列表失败: ${e.message}`);
    }
  }

  _loadWatchlist() {
    try {
      if (!fs.existsSync(this._watchlistPath)) {
        console.log('📝 [v16] 无历史观察列表文件，从零开始');
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
      console.log(`✅ [v16] 已加载${loaded}个观察列表条目${expired > 0 ? ` (${expired}个已超时)` : ''}`);
    } catch (e) {
      console.warn(`⚠️ [v16] 加载观察列表失败: ${e.message}`);
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
    try { this.marketDataBackfill?.close(); } catch {}
    if (this._solMarketCheckInterval) {
      clearInterval(this._solMarketCheckInterval);
      this._solMarketCheckInterval = null;
    }
    // 清理 NOT_ATH K线追踪定时器
    if (this._klineTrackers) {
      for (const timer of this._klineTrackers.values()) clearTimeout(timer);
      this._klineTrackers.clear();
    }
    // 停止前保存ATH计数
    this._saveAthCounts();
    if (this.shadowMode && this.shadowTracker) {
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
      mode: this.shadowMode ? 'PAPER_ONLY' : 'LIVE',
      position_sol: this.positionSol,
      max_positions: this.maxPositions,
      dedup_cache_size: this.recentSignals.size
    };
  }

  /**
   * NOT_ATH 执行路径：super>=80 的 NOT_ATH 信号独立执行
   * 统一3维度评分：趋势强度(+1) + 守住支撑(+1) + 成交量增加(+1) score≥2执行
   * @param {string} ca - Token CA
   * @param {string} ca - Token CA
   * @param {object} signal - 信号对象
   */
  async _executeNotAth(ca, signal) {
    const symbol = signal.symbol || ca.substring(0, 8);

    // 已持仓检查
    if (this.livePositionMonitor?.positions?.has(ca)) {
      console.log(`⏭️ [NOT_ATH] $${symbol} 已持仓 → 跳过`);
      return { action: 'SKIP', reason: 'already_holding' };
    }
    if (this.shadowMode && this.shadowTracker.hasOpenPosition(ca)) {
      console.log(`⏭️ [NOT_ATH] $${symbol} Shadow已有未平仓持仓，跳过`);
      return { action: 'SKIP', reason: 'already_in_position' };
    }

    // K线评分检查（先回填信号前 5 根，再判断）
    const signalTsSec = Math.floor((signal.timestamp || Date.now()) / 1000);
    const backfillResult = await this._backfillPrebuyKlines(ca, signalTsSec, 5);
    const prebarsCount = backfillResult.totalBefore || 0;
    const prebuyEnough = backfillResult.enough === true;

    if (!prebuyEnough) {
      console.log(`⚠️ [NOT_ATH] $${symbol} pre-buy K线不足: ${prebarsCount}/5${backfillResult.reason ? ` | ${backfillResult.reason}` : ''}`);
      this.saveSignalRecord(signal, 'INSUFFICIENT_KLINE', null);
      return { action: 'SKIP', reason: 'insufficient_kline', prebarsCount, backfillResult };
    }

    const klineResult = await this._checkKline(ca, { isATH: false });
    const klineBypassed = !klineResult.passed && (klineResult.reason === 'rate_limited' || klineResult.reason === 'error_skip');
    if (!klineResult.passed) {
      if (klineBypassed) {
        console.log(`⚠️ [NOT_ATH] $${symbol} K线检查暂不可用，跳过K线拦截: ${klineResult.reason}`);
      } else {
        const reasonMap = {
          no_pool: '无流动性池',
          no_bars: 'K线数据缺失',
          no_history: '历史K线不足(<3根)',
          not_red_bar: '非红色K线',
          high_vol_calm: '高成交量+低波动',
          high_vol: '成交量偏高',
          inactive: '动量不足(不活跃)',
          rate_limited: 'K线接口限流',
          error_skip: 'K线查询异常'
        };
        const detail = klineResult.score !== undefined ? `score=${klineResult.score}` : '';
        console.log(`🚫 [NOT_ATH] $${symbol} K线过滤失败: ${reasonMap[klineResult.reason] || klineResult.reason}${detail ? ` (${detail})` : ''}`);
        this.saveSignalRecord(signal, 'RED_K_FAIL', null);
        return { action: 'SKIP', reason: `red_k_${klineResult.reason}` };
      }
    }

    // 打印评分详情
    if (klineBypassed) {
      console.log(`📊 [NOT_ATH] $${symbol} K线限流/异常，按新币兜底逻辑继续`);
    } else if (klineResult.score !== undefined) {
      const { score, isRed, lowVolume, isActive, momFromLag1, avgVol3 } = klineResult;
      console.log(`📊 [NOT_ATH] $${symbol} 评分: ${score}分 | RED:${isRed} | lowVol:${lowVolume} | active:${isActive} | mom:${momFromLag1?.toFixed(1) ?? 'N/A'}%`);
    } else {
      console.log(`📊 [NOT_ATH] $${symbol} 新币逻辑通过`);
    }

    // 执行 Shadow Buy
    const finalSize = 0.06;
    const exitStrategy = 'NOT_ATH';
    const tradeConviction = 'HIGH';

    if (this.shadowMode) {
      console.log(`📋 [PAPER_ONLY] Shadow执行已停用，$${symbol} 仅记录信号，不在JS引擎内开模拟仓`);
      this.saveSignalRecord(signal, 'PASS', null, false);
      return { action: 'PAPER_ONLY_SKIP', reason: 'shadow_disabled' };
    }

    if (!this.autoBuyEnabled) {
      console.log(`📋 [通知] 建议买入 $${symbol} | ${finalSize} SOL (自动买入未开启)`);
      this.saveSignalRecord(signal, 'PASS', null, false);
      return { action: 'NOTIFY', size: finalSize };
    }

    // SOL 余额检查
    if (this.jupiterExecutor) {
      try {
        const solBalance = await this.jupiterExecutor.getSolBalance();
        const minRequired = finalSize + 0.025;
        if (solBalance < minRequired) {
          console.log(`💤 [LIVE未就绪] SOL余额: ${solBalance.toFixed(4)} < 需要: ${minRequired.toFixed(4)}，本次仅保留 paper 结果`);
          this.saveSignalRecord(signal, 'PASS', null, false);
          return { action: 'SKIP_INSUFFICIENT_BALANCE', balance: solBalance, required: minRequired };
        }
      } catch (e) {
        console.warn(`⚠️ [余额检查] 查询失败: ${e.message}，继续`);
      }
    }

    const riskCheck = this._checkEntryRisk(symbol, 'NOT_ATH');
    if (!riskCheck.allowed) {
      this.saveSignalRecord(signal, 'RISK_BLOCKED', null);
      return { action: 'SKIP', reason: `risk: ${riskCheck.reason}` };
    }

    console.log(`💰 [执行] 买入 $${symbol} | ${finalSize} SOL | ${exitStrategy}...`);

    try {
      let tradeResult;
      if (this.jupiterExecutor) {
        tradeResult = await this.jupiterExecutor.buy(ca, finalSize, { mc: signal.market_cap || 0 });
        if (tradeResult.success && this.livePositionMonitor) {
          await new Promise(r => setTimeout(r, 3000));
          const balance = await this.jupiterExecutor.getTokenBalance(ca);
          if (balance.amount <= 0) {
            console.error(`❌ [验证失败] 买入后余额为0，交易可能失败`);
            this.stats.errors++;
            return { action: 'EXEC_FAILED', reason: '买入后余额为0' };
          }

          const tokenAmount = balance.amount;
          const tokenDecimals = balance.decimals || 6;
          const actualTokenAmount = tokenAmount / Math.pow(10, tokenDecimals);
          const entryPrice = finalSize / actualTokenAmount;
          const entryMC = this._getCachedMC(ca) || signal.market_cap || 0;
          console.log(`💰 [Entry] ${entryPrice.toFixed(10)} SOL/token | ${finalSize} SOL → ${actualTokenAmount.toFixed(2)} tokens`);

          this.livePositionMonitor.addPosition(
            ca, symbol, entryPrice, entryMC, finalSize,
            tokenAmount, tokenDecimals, tradeConviction, exitStrategy
          );
        }
      } else {
        tradeResult = { success: false, reason: 'no_executor' };
      }

      if (tradeResult.success) {
        this.stats.buy_success++;
        console.log(`✅ [成交] $${symbol} 买入成功`);
        this.saveSignalRecord(signal, 'BOUGHT', null, false);
        return { action: 'BUY', size: finalSize };
      } else {
        this.stats.buy_failed++;
        console.log(`❌ [买入失败] $${symbol}: ${tradeResult.reason}`);
        this.saveSignalRecord(signal, 'BUY_FAIL', null, false);
        return { action: 'BUY_FAILED', reason: tradeResult.reason };
      }
    } catch (error) {
      this.stats.errors++;
      console.error(`❌ [执行异常] $${symbol}: ${error.message}`);
      return { action: 'ERROR', reason: error.message };
    }
  }

  /**
   * K线评分：统一逻辑，3维度评分≥2执行
   * NOT_ATH: 趋势强度(+1) + 守住支撑(+1) + 成交量增加(+1)
   * @param {string} tokenCA - 代币CA
   * @param {object} options - { isATH: boolean }
   * @returns {Promise<{passed, close, open, fbr, volume, reason, score?, trendOk?, holdsSupport?, volIncreasing?, greenCount?, minLow?}>}
   */
  async _checkKline(tokenCA, options = {}) {
    const { isATH = true } = options;
    const cacheKey = `${tokenCA}:${isATH ? 'ath' : 'notath'}`;
    const nowMs = Date.now();
    const cachedResult = this._klineResultCache.get(cacheKey);
    if (cachedResult && nowMs - cachedResult.at < 30_000) {
      return cachedResult.result;
    }

    const persistKlineResult = (result) => {
      this._klineResultCache.set(cacheKey, { at: nowMs, result });
      return result;
    };

    const scoreBars = (bars, poolAddress = '') => {
      if (!bars?.length) return { passed: false, reason: 'no_bars' };

      const current = bars[0];
      const prev = bars[1] || null;
      const fbr = current.open > 0 ? ((current.close - current.open) / current.open) * 100 : 0;

      if (isATH) {
        const passed = current.close > current.open;
        this._saveKlineBars(tokenCA, poolAddress, bars, {});
        return { passed, close: current.close, open: current.open, fbr, volume: current.volume,
                 reason: passed ? 'pass' : 'not_green_bar' };
      }

      if (!prev || bars.length < 3) return { passed: false, reason: 'no_history' };

      const isRed = current.close < current.open;
      if (!isRed) return { passed: false, reason: 'not_red_bar' };

      const prev3 = bars.slice(1, 4);
      const avgVol3 = prev3.reduce((sum, b) => sum + b.volume, 0) / 3;
      const lowVolume = current.volume <= avgVol3;
      const momFromLag1 = prev3[0].close > 0
        ? ((current.close - prev3[0].close) / prev3[0].close) * 100
        : 0;
      const isActive = Math.abs(momFromLag1) > 30;

      let score = 2;
      if (lowVolume) score += 1;
      if (isActive) score += 1;

      const passed = score >= 3;

      let reason;
      if (passed) {
        reason = 'pass';
      } else if (!lowVolume && !isActive) {
        reason = 'high_vol_calm';
      } else if (!lowVolume) {
        reason = 'high_vol';
      } else {
        reason = 'inactive';
      }

      this._saveKlineBars(tokenCA, poolAddress, bars, {
        score, isRed, lowVolume, isActive,
        momFromLag1, avgVol3
      });

      return { passed, close: current.close, open: current.open, fbr,
               volume: current.volume, reason, score,
               isRed, lowVolume, isActive,
               momFromLag1, avgVol3 };
    };

    try {
      // 1) 优先尝试本地 K 线缓存，避免重复打外部接口
      try {
        const cachedBars = this.db.prepare(`
          SELECT pool_address, timestamp, open, high, low, close, volume
          FROM kline_1m
          WHERE token_ca = ?
          ORDER BY timestamp DESC
          LIMIT 20
        `).all(tokenCA);
        if (cachedBars?.length >= (isATH ? 1 : 4)) {
          const bars = cachedBars.map(row => ({
            ts: Number(row.timestamp),
            open: Number(row.open),
            high: Number(row.high),
            low: Number(row.low),
            close: Number(row.close),
            volume: Number(row.volume)
          }));
          const freshEnough = nowMs - (bars[0].ts * 1000) < 10 * 60_000;
          if (freshEnough) {
            return persistKlineResult(scoreBars(bars, cachedBars[0]?.pool_address || this._poolCache.get(tokenCA) || ''));
          }
        }
      } catch (dbError) {
        console.warn(`⚠️ [K线检查] ${tokenCA.substring(0,8)} 读取本地缓存失败: ${dbError.message}`);
      }

      // 2) 对新币先等待一次异步预热结果，避免首跳就现场打外部接口
      if (!isATH && this._klinePriming.has(tokenCA)) {
        await Promise.race([
          this._klinePriming.get(tokenCA),
          new Promise(resolve => setTimeout(resolve, 1200))
        ]);
        try {
          const primedBars = this.db.prepare(`
            SELECT pool_address, timestamp, open, high, low, close, volume
            FROM kline_1m
            WHERE token_ca = ?
            ORDER BY timestamp DESC
            LIMIT 20
          `).all(tokenCA);
          if (primedBars?.length >= 4) {
            const bars = primedBars.map(row => ({
              ts: Number(row.timestamp),
              open: Number(row.open),
              high: Number(row.high),
              low: Number(row.low),
              close: Number(row.close),
              volume: Number(row.volume)
            }));
            const freshEnough = nowMs - (bars[0].ts * 1000) < 10 * 60_000;
            if (freshEnough) {
              return persistKlineResult(scoreBars(bars, primedBars[0]?.pool_address || this._poolCache.get(tokenCA) || ''));
            }
          }
        } catch {}
      }

      // 3) 如果刚被限流过，短时间内不再继续打外部接口
      if (nowMs < this._klineApiCooldownUntil) {
        return persistKlineResult({ passed: false, reason: 'rate_limited' });
      }

      // 4) 从外部接口获取 pool 地址
      let poolAddress = this._poolCache?.get(tokenCA);
      if (!poolAddress) {
        const dexRes = await axios.get(
          `https://api.dexscreener.com/latest/dex/tokens/${tokenCA}`,
          { timeout: 5000 }
        );
        const pairs = dexRes.data?.pairs;
        if (!pairs?.length) return persistKlineResult({ passed: false, reason: 'no_pool' });
        const solPairs = pairs.filter(p => p.chainId === 'solana');
        const pool = solPairs?.length
          ? solPairs.sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0))[0]
          : pairs[0];
        if (!pool?.pairAddress) return persistKlineResult({ passed: false, reason: 'no_pool' });
        this._poolCache.set(tokenCA, pool.pairAddress);
        poolAddress = pool.pairAddress;
      }

      // 5) 获取6根K线（仅在本地缓存不可用时）
      const geckoRes = await axios.get(
        `https://api.geckoterminal.com/api/v2/networks/solana/pools/${poolAddress}/ohlcv/minute?aggregate=1&limit=6`,
        { timeout: 5000 }
      );
      const ohlcv = geckoRes.data?.data?.attributes?.ohlcv_list;
      if (!ohlcv?.length) return persistKlineResult({ passed: false, reason: 'no_bars' });

      const bars = ohlcv.map(row => ({
        ts: Number(row[0]), open: Number(row[1]), high: Number(row[2]),
        low: Number(row[3]), close: Number(row[4]), volume: Number(row[5])
      }));

      return persistKlineResult(scoreBars(bars, poolAddress));
    } catch (error) {
      const status = error?.response?.status;
      if (status === 429) {
        this._klineApiCooldownUntil = Date.now() + 120_000;
        this._klinePrimeCooldownUntil = Date.now() + 120_000;
        if (!this._lastKlineRateLimitLogAt || Date.now() - this._lastKlineRateLimitLogAt > 15_000) {
          console.warn(`⚠️ [K线检查] ${tokenCA.substring(0,8)} 接口限流: ${error.message} | 120s 内复用缓存/跳过外部查询`);
          this._lastKlineRateLimitLogAt = Date.now();
        }
        return persistKlineResult({ passed: false, reason: 'rate_limited' });
      }
      console.warn(`⚠️ [K线检查] ${tokenCA.substring(0,8)} 检查失败: ${error.message}`);
      return persistKlineResult({ passed: false, reason: 'error_skip' });
    }
  }

  /**
   * 保存K线数据到本地DB，供未来回测使用
   * @param {string} tokenCA
   * @param {string} poolAddress
   * @param {Array} bars - K线数组
   * @param {object} scores - 评分数据 {score, pullback, volRatio, wickRatio, ema21, isNewCoin}
   */
  _saveKlineBars(tokenCA, poolAddress, bars, scores = {}) {
    if (!this._klineInsertStmt || !bars?.length) return;
    try {
      for (const bar of bars) {
        this._klineInsertStmt.run(
          tokenCA,
          poolAddress || '',
          bar.ts,          // timestamp (秒)
          bar.open,
          bar.high,
          bar.low,
          bar.close,
          bar.volume,
          scores.score ?? null,
          scores.pullback ?? null,
          scores.volRatio ?? null,
          scores.wickRatio ?? null,
          scores.ema21 ?? null
        );
      }
    } catch (e) {
      // 静默失败，不影响交易
      console.warn(`⚠️ [K线DB] $${tokenCA.substring(0,8)} 写入失败: ${e.message}`);
    }
  }
}

export default PremiumSignalEngine;
