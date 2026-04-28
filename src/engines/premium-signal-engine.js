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
import ClaudeAnalyst from '../utils/claude-analyst.js';
import { generatePremiumBuyPrompt } from '../prompts/premium-signal-prompts.js';
import { TelegramBuzzScanner } from '../social/telegram-buzz.js';
import { ShadowPnlTracker } from '../tracking/shadow-pnl-tracker.js';
import { RiskManager } from '../risk/risk-manager.js';
import { MarketDataBackfillService } from '../market-data/market-data-backfill-service.js';
import { SharedMarketDataClient } from '../market-data/shared-market-data-client.js';
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
    this.liveExecution = null; // 实盘模式下注入共享执行器
    this.livePositionMonitor = null; // 外部注入
    this.livePriceMonitor = null; // 外部注入（shadow 也可用）
    this.buzzScanner = null; // 需要 setTelegramClient 初始化
    this.shadowTracker = new ShadowPnlTracker();
    this.paperStrategyRegistry = null;
    this.paperTradeRecorder = null;
    this.marketDataBackfill = new MarketDataBackfillService();
    this.sharedMarketData = new SharedMarketDataClient(config, {
      repository: this.marketDataBackfill.repository,
      poolResolver: this.marketDataBackfill.poolResolver,
      backfillService: this.marketDataBackfill,
    });
    this._poolCache = new Map();
    this._klineResultCache = new Map();
    this._klineApiCooldownUntil = 0;
    this._lastKlineRateLimitLogAt = 0;
    this._klinePriming = new Map();
    this._klinePrimeCooldownUntil = 0;
    this._lastKlinePrimeLogAt = 0;
    this._klinePrimeMinGapMs = parseInt(process.env.KLINE_PRIME_MIN_GAP_MS || '30000', 10);
    this._klineLocalFreshnessSec = parseInt(process.env.KLINE_LOCAL_FRESHNESS_SEC || '120', 10);
    this._klineProviderFreshnessSec = parseInt(process.env.KLINE_PROVIDER_FRESHNESS_SEC || '120', 10);
    this._notAthPrebuyUnknownDataFailClosed = process.env.NOT_ATH_PREBUY_UNKNOWN_DATA_FAIL_OPEN !== 'true';

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
      not_ath_prebuy_kline_pass: 0,
      not_ath_prebuy_kline_unknown_data: 0,
      not_ath_prebuy_kline_block: 0,
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
  setLiveComponents(liveExecution, livePositionMonitor) {
    this.liveExecution = liveExecution;
    this.livePositionMonitor = livePositionMonitor;
    console.log('✅ [Premium Engine] LiveExecution + LivePositionMonitor 已注入');
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
        if (this.liveExecution) {
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
        raw_message TEXT,
        timestamp INTEGER NOT NULL,
        source_message_ts INTEGER,
        receive_ts INTEGER,
        signal_type TEXT,
        is_ath INTEGER DEFAULT 0,
        signal_source TEXT,
        source_event_id TEXT,
        parse_status TEXT,
        parse_missing_fields TEXT,
        hard_gate_status TEXT,
        gate_result TEXT,
        ai_action TEXT,
        ai_confidence INTEGER,
        ai_narrative_tier TEXT,
        executed INTEGER DEFAULT 0,
        trade_result TEXT,
        downstream_trade_id INTEGER,
        downstream_lifecycle_id TEXT,
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
        premium_signal_id INTEGER,
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
    const addSqlColumn = (sql) => {
      try { this.db.exec(sql); } catch {}
    };
    addSqlColumn(`ALTER TABLE premium_signals ADD COLUMN raw_message TEXT`);
    addSqlColumn(`ALTER TABLE premium_signals ADD COLUMN source_message_ts INTEGER`);
    addSqlColumn(`ALTER TABLE premium_signals ADD COLUMN receive_ts INTEGER`);
    addSqlColumn(`ALTER TABLE premium_signals ADD COLUMN signal_type TEXT`);
    addSqlColumn(`ALTER TABLE premium_signals ADD COLUMN is_ath INTEGER DEFAULT 0`);
    addSqlColumn(`ALTER TABLE premium_signals ADD COLUMN signal_source TEXT`);
    addSqlColumn(`ALTER TABLE premium_signals ADD COLUMN source_event_id TEXT`);
    addSqlColumn(`ALTER TABLE premium_signals ADD COLUMN parse_status TEXT`);
    addSqlColumn(`ALTER TABLE premium_signals ADD COLUMN parse_missing_fields TEXT`);
    addSqlColumn(`ALTER TABLE premium_signals ADD COLUMN gate_result TEXT`);
    addSqlColumn(`ALTER TABLE premium_signals ADD COLUMN downstream_trade_id INTEGER`);
    addSqlColumn(`ALTER TABLE premium_signals ADD COLUMN downstream_lifecycle_id TEXT`);
    addSqlColumn(`ALTER TABLE trades ADD COLUMN premium_signal_id INTEGER`);
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

      // ─── Step 1.5: 体积过滤 (剔除无效垃圾盘) ───
      // 对于 MC < 16k 且 Volume < 38k 的情况，流动性极差且滑点极高，直接拦截
      if ((signal.market_cap !== null && signal.market_cap < 16000) && 
          (signal.volume_24h !== null && signal.volume_24h < 38000)) {
        this.stats.precheck_failed = (this.stats.precheck_failed || 0) + 1;
        console.log(`⏭️ [体积过滤] $${signal.symbol} MC=$${signal.market_cap} Vol=$${signal.volume_24h} (MC<16K 且 Vol<38K) → 跳过极低流动性盘`);
        this.saveSignalRecord(signal, 'ILLIQUID_JUNK', null);
        return { action: 'SKIP', reason: 'illiquid_junk' };
      }

      // ─── Step 2: ATH 检查 — 最先过滤，非ATH检查 super_index (~0ms) ───
      const isATH = signal.is_ath === true;
      const signalIndices = signal.indices || {};
      const superIndex = signalIndices?.super_index?.current || signalIndices?.super_index?.value || 0;

      if (!isATH) {
        // NOT_ATH 信号：需要 super_index >= 70 才允许交易
        if (superIndex < 70) {
          console.log(`⏭️ [NOT_ATH] $${signal.symbol} Super=${superIndex}<70 → 跳过`);
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

      // ATH 计数器 — 记录第几次看到此币的 ATH 信号
      const sigHistory = this.signalHistory.get(ca);
      const prevAthCount = sigHistory ? (sigHistory.athCount || 0) : 0;
      const currentAthNum = prevAthCount + 1;

      // 提交 ATH 计数（不论是否入场，ATH# 就是 ATH#）
      if (!sigHistory) {
        this.signalHistory.set(ca, { athCount: 1, lastSeen: Date.now() });
      } else {
        sigHistory.athCount = (sigHistory.athCount || 0) + 1;
        sigHistory.lastSeen = Date.now();
      }
      this._saveAthCounts();

      // ─── 风控检查 ─────────────────────────────────────────
      const riskCheck = this._checkEntryRisk(signal.symbol, 'ATH');
      if (!riskCheck.allowed) {
        this.saveSignalRecord(signal, 'RISK_BLOCKED', null);
        return { action: 'SKIP', reason: `risk: ${riskCheck.reason}` };
      }

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

      // ─── ATH 基本安全检查（Kelly 统一由 Python 处理）─────────────
      const mc = signal.market_cap || 0;
      const liveMC = this._getCachedMC(ca);
      const effectiveMC = (liveMC > 0) ? liveMC : mc;

      if (effectiveMC < 20000) {
        console.log(`⏭️ [ATH] $${signal.symbol} MC=$${(effectiveMC/1000).toFixed(1)}K < $20K → 流动性太差`);
        this.saveSignalRecord(signal, 'V18_MC_FILTER', null);
        return { action: 'SKIP', reason: 'mc_too_low', mc: effectiveMC };
      }
      if (effectiveMC > 500000) {
        console.log(`⏭️ [ATH] $${signal.symbol} MC=$${(effectiveMC/1000).toFixed(1)}K > $500K → MC过高，早期机会已过`);
        this.saveSignalRecord(signal, 'V18_MC_FILTER', null);
        return { action: 'SKIP', reason: 'mc_too_high', mc: effectiveMC };
      }

      // 补充 sigHistory 元数据
      const updatedHistory = this.signalHistory.get(ca);
      if (updatedHistory) {
        if (idx?.super_index) {
          updatedHistory.lastSuperIndex = superCurrent;
          if (!updatedHistory.firstSuperIndex) updatedHistory.firstSuperIndex = superSignal;
        }
        if (prevAthCount === 0 && signal.market_cap > 0) updatedHistory.mc1 = signal.market_cap;
      }

      const finalSize = 0.06;  // 默认值，Python Kelly 会覆盖
      const exitStrategy = 'ASYMMETRIC';
      const tradeConviction = 'HIGH';

      const elapsed = Date.now() - t0;
      console.log(
        `🎯 [ATH] $${signal.symbol} ATH#${currentAthNum} ✅ PASS | ` +
        `MC=$${(effectiveMC/1000).toFixed(1)}K Super=${superCurrent} | ` +
        `→ 交给观察列表 + 统一Kelly | 决策耗时:${elapsed}ms`
      );

      const aiResult = {
        action: 'BUY_FULL', confidence: 80,
        narrative_tier: 'CONFIRMED',
        narrative_reason: `ATH#${currentAthNum} MC=$${(effectiveMC/1000).toFixed(1)}K Super=${superCurrent} → unified Kelly in Python`,
        entry_timing: 'OPTIMAL', stop_loss_percent: 35,
        exitStrategy,
        ath_num: currentAthNum  // 传给 Python Kelly 使用
      };

      // ─── Step 6: 执行 ─────────────────────────────────────────

      // FBR过滤：检查第一根K线是否绿色（仅在方法存在时）
      if (typeof this._checkFBR === 'function') {
        const fbrResult = await this._checkFBR(ca);
        if (!fbrResult.passed) {
          console.log(`🚫 [FBR] $${signal.symbol} 第一根K线红色 (open=${fbrResult.open?.toFixed(10)} close=${fbrResult.close?.toFixed(10)} FBR=${fbrResult.fbr?.toFixed(2)}%) → 跳过`);
          this.saveSignalRecord(signal, 'FBR_FAIL', aiResult, false);
          return { action: 'SKIP', reason: 'fbr_failed', fbr: fbrResult.fbr };
        }
        if (fbrResult.reason !== 'error_skip') {
          console.log(`✅ [FBR] $${signal.symbol} 第一根K线绿色 (FBR=${fbrResult.fbr?.toFixed(2)}%)`);
        }
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
      if (this.liveExecution) {
        try {
          const solBalance = await this.liveExecution.getSolBalance();
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
        if (this.liveExecution) {
          tradeResult = await this.liveExecution.buy(ca, finalSize, { mc: mc || 0 });

          if (tradeResult.success && this.livePositionMonitor) {
            // 等待余额更新
            await new Promise(r => setTimeout(r, 3000));
            const balance = await this.liveExecution.getTokenBalance(ca);

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
      const resolvedPool = await this.sharedMarketData.resolvePool(tokenCA);
      poolAddress = resolvedPool.poolAddress || null;
      if (poolAddress) {
        this._poolCache.set(tokenCA, poolAddress);
      } else {
        if (resolvedPool.rateLimited) {
          this._klinePrimeCooldownUntil = Date.now() + 120_000;
        }
        return {
          fetched: 0,
          existingBefore,
          totalBefore: heliusBarsBefore,
          enough: false,
          provider: resolvedPool.provider || heliusResult.provider || null,
          reason: resolvedPool.error || heliusResult.error || 'no_pool',
        };
      }
    }

    const ohlcvResult = await this.sharedMarketData.fetchRecentOhlcvByPool(tokenCA, poolAddress, {
      signalTsSec,
      bars: Math.max(20, targetBars * 4),
      beforeTimestamps: [signalTsSec + 600, signalTsSec + 3600],
      allowDexFallback: false,
    });

    if (ohlcvResult.rateLimited) {
      this._klineApiCooldownUntil = Date.now() + 120_000;
      this._klinePrimeCooldownUntil = Date.now() + 120_000;
      return {
        fetched: 0,
        existingBefore,
        totalBefore: heliusBarsBefore,
        enough: heliusBarsBefore >= targetBars,
        provider: ohlcvResult.provider || heliusResult.provider || null,
        poolAddress,
        reason: 'RATE_LIMITED',
      };
    }

    const bars = (ohlcvResult.bars || [])
      .filter((bar) => Number.isFinite(bar.timestamp) && bar.timestamp < signalTsSec)
      .map((bar) => ({
        ts: bar.timestamp,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
      }));

    if (bars.length) {
      this._saveKlineBars(tokenCA, poolAddress, bars, {});
    }

    const totalBefore = heliusBarsBefore + bars.length;
    return {
      fetched: Math.max(0, totalBefore - existingBefore),
      existingBefore,
      totalBefore,
      enough: totalBefore >= targetBars,
      provider: bars.length ? (ohlcvResult.provider || 'geckoterminal') : heliusResult.provider,
      poolAddress,
      metrics: heliusResult,
      reason: bars.length ? null : (ohlcvResult.error || null),
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
        const newestTs = Number(rows?.[0]?.timestamp || 0);
        const freshnessMs = newestTs > 0 ? (Date.now() - newestTs * 1000) : null;
        if (rows?.length >= minBars && Number.isFinite(freshnessMs) && freshnessMs < 10 * 60_000) {
          return {
            ok: true,
            rowCount: rows.length,
            newestTs,
            freshnessSec: Math.max(0, Math.round(freshnessMs / 1000)),
            timedOut: false,
          };
        }
      } catch (error) {
        return {
          ok: false,
          rowCount: 0,
          newestTs: null,
          freshnessSec: null,
          timedOut: false,
          error: error.message,
        };
      }
      await new Promise(r => setTimeout(r, 150));
    }
    return {
      ok: false,
      rowCount: 0,
      newestTs: null,
      freshnessSec: null,
      timedOut: true,
    };
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
  saveSignalRecord(signal, gateStatus, aiResult, executed = false, linkage = {}) {
    try {
      const parseMissingFields = Array.isArray(signal.parse_missing_fields)
        ? signal.parse_missing_fields.filter(Boolean)
        : [];
      const receiveTs = Number(signal.receive_ts || signal.timestamp || Date.now());
      const sourceMessageTs = Number(signal.source_message_ts || 0) || null;
      const signalType = signal.signal_type || (signal.is_ath ? 'ATH' : 'NEW_TRENDING');
      const signalSource = signal.signal_source || signal.source || (signal.is_ath ? 'premium_channel_ath' : 'premium_channel');
      const sourceEventId = signal.source_event_id
        || [signalSource, signal.token_ca, sourceMessageTs || receiveTs, signalType].filter(Boolean).join(':');
      const parseStatus = signal.parse_status || (parseMissingFields.length ? 'partial' : 'parsed');
      const inheritedGateResult = linkage.gateResult && typeof linkage.gateResult === 'object'
        ? linkage.gateResult
        : (signal._prebuyGateResult && typeof signal._prebuyGateResult === 'object' ? signal._prebuyGateResult : null);
      const gatePayload = {
        status: gateStatus,
        executed: executed ? 1 : 0,
        aiAction: aiResult?.action || null,
        aiConfidence: aiResult?.confidence || null,
        ...(inheritedGateResult || {}),
      };
      if (inheritedGateResult && typeof inheritedGateResult === 'object') {
        gatePayload.auditVersion = inheritedGateResult.auditVersion || 2;
        gatePayload.gateDecision = inheritedGateResult.gateDecision || gatePayload.gateDecision || null;
        gatePayload.gateReason = inheritedGateResult.gateReason || gatePayload.gateReason || null;
      }
      const result = this.db.prepare(`
        INSERT INTO premium_signals (
          token_ca, symbol, market_cap, holders, volume_24h, top10_pct,
          age, description, raw_message, timestamp, source_message_ts, receive_ts,
          signal_type, is_ath, signal_source, source_event_id, parse_status, parse_missing_fields,
          hard_gate_status, gate_result,
          ai_action, ai_confidence, ai_narrative_tier, executed,
          downstream_trade_id, downstream_lifecycle_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        signal.token_ca,
        signal.symbol || null,
        signal.market_cap || null,
        signal.holders || null,
        signal.volume_24h || null,
        signal.top10_pct || null,
        signal.age || null,
        signal.description || null,
        signal.raw_message || signal.description || null,
        signal.timestamp || receiveTs,
        sourceMessageTs,
        receiveTs,
        signalType,
        signal.is_ath ? 1 : 0,
        signalSource,
        sourceEventId,
        parseStatus,
        parseMissingFields.length ? JSON.stringify(parseMissingFields) : null,
        gateStatus,
        JSON.stringify(gatePayload),
        aiResult?.action || null,
        aiResult?.confidence || null,
        aiResult?.narrative_tier || null,
        executed ? 1 : 0,
        linkage.tradeId || null,
        linkage.lifecycleId || null,
      );
      signal._premiumSignalId = Number(result.lastInsertRowid || 0) || null;
      return signal._premiumSignalId;
    } catch (error) {
      console.error('❌ [DB] 保存信号记录失败:', error.message);
      return null;
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
          token_ca, premium_signal_id, chain, action, position_size, entry_time, entry_price, pool_address, timestamp,
          symbol, narrative, rating, status, is_simulation
        ) VALUES (?, ?, 'SOL', 'BUY', ?, ?, 0, ?, ?, ?, ?, ?, 'OPEN', 1)
      `).run(
        signal.token_ca,
        signal._premiumSignalId || null,
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
    if (!this.shadowMode && !this.liveExecution) {
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

    const signalTsSec = Number(signal.signal_ts || signal.timestamp || Math.floor(Date.now() / 1000));

    try {
      const backfill = await this._backfillPrebuyKlines(ca, signalTsSec, 5);
      const waitResult = backfill?.enough
        ? await this._waitForFreshLocalKlines(ca, 4, 1200)
        : null;

      const klineCheck = await this._checkKline(ca, { isATH: false, signalTsSec });
      const structuralFailures = new Set(['not_red_bar', 'support_break', 'high_vol', 'high_vol_calm', 'inactive']);
      const gateDecision = klineCheck?.gateStatus || (klineCheck?.passed ? 'PASS' : 'UNKNOWN_DATA');
      const blockedStructural = gateDecision === 'BLOCK' && structuralFailures.has(klineCheck?.reason);
      const unknownDataBlocked = gateDecision === 'UNKNOWN_DATA' && this._notAthPrebuyUnknownDataFailClosed;
      const decisionContinuedToBuy = gateDecision !== 'BLOCK' && !unknownDataBlocked;
      const normalizeUnknownReason = (reason, check, backfillResult, waitState) => {
        if (gateDecision !== 'UNKNOWN_DATA') return reason || 'unknown';
        if (reason === 'RATE_LIMITED') return backfillResult?.reason === 'RATE_LIMITED' ? 'backfill_rate_limited' : 'provider_rate_limited';
        if (reason === 'stale_local_bars' || reason === 'stale_primed_bars') return reason;
        if (reason === 'no_history') return 'insufficient_bars';
        if (reason === 'no_bars') {
          if (backfillResult?.reason === 'no_pool') return 'no_pool';
          return 'provider_no_bars';
        }
        if (reason === 'error_skip') return 'provider_error';
        if (waitState && backfillResult?.enough && !waitState.ok) {
          return waitState.error ? 'local_wait_error' : 'local_wait_timeout';
        }
        return reason || 'unknown_data';
      };
      const normalizedUnknownReason = normalizeUnknownReason(klineCheck?.reason, klineCheck, backfill, waitResult);
      const prebuyGateResult = {
        auditVersion: 2,
        gateName: 'NOT_ATH_PREBUY_KLINE',
        gateDecision,
        gateReason: gateDecision === 'UNKNOWN_DATA' ? normalizedUnknownReason : (klineCheck?.reason || 'unknown'),
        provider: klineCheck?.provider || backfill?.provider || null,
        poolAddress: klineCheck?.poolAddress || backfill?.poolAddress || null,
        freshnessSec: Number.isFinite(klineCheck?.freshnessSec) ? klineCheck.freshnessSec : null,
        barCountSeen: Number.isFinite(klineCheck?.barCountSeen)
          ? klineCheck.barCountSeen
          : (Number.isFinite(backfill?.totalBefore) ? backfill.totalBefore : null),
        signalTsSec,
        supportBreakPct: Number.isFinite(klineCheck?.supportBreakPct) ? klineCheck.supportBreakPct : null,
        avgVol3: Number.isFinite(klineCheck?.avgVol3) ? klineCheck.avgVol3 : null,
        momFromLag1: Number.isFinite(klineCheck?.momFromLag1) ? klineCheck.momFromLag1 : null,
        decisionContinuedToBuy,
        blockedStructural,
        unknownDataBlocked,
        observability: {
          backfillAttempted: Boolean(backfill),
          backfillEnough: Boolean(backfill?.enough),
          freshLocalBarsObserved: Boolean(waitResult?.ok),
          localWaitTimedOut: Boolean(waitResult?.timedOut),
          localWaitError: waitResult?.error || null,
          dataSource: klineCheck?.provider || backfill?.provider || null,
          truthSource: klineCheck?.truthSource || null,
          failOpenPrevented: Boolean(klineCheck?.failOpenPrevented),
          failClosedApplied: unknownDataBlocked,
          providerDataState: gateDecision === 'UNKNOWN_DATA' ? normalizedUnknownReason : 'scored',
        },
        backfill: backfill ? {
          fetched: Number.isFinite(backfill.fetched) ? backfill.fetched : null,
          existingBefore: Number.isFinite(backfill.existingBefore) ? backfill.existingBefore : null,
          totalBefore: Number.isFinite(backfill.totalBefore) ? backfill.totalBefore : null,
          enough: Boolean(backfill.enough),
          provider: backfill.provider || null,
          poolAddress: backfill.poolAddress || null,
          reason: backfill.reason || null,
        } : null,
        localWait: waitResult ? {
          ok: Boolean(waitResult.ok),
          rowCount: Number.isFinite(waitResult.rowCount) ? waitResult.rowCount : null,
          newestTs: Number.isFinite(waitResult.newestTs) ? waitResult.newestTs : null,
          freshnessSec: Number.isFinite(waitResult.freshnessSec) ? waitResult.freshnessSec : null,
          timedOut: Boolean(waitResult.timedOut),
          error: waitResult.error || null,
        } : null,
      };
      signal._prebuyGateResult = prebuyGateResult;
      const logParts = [
        'NOT_ATH_PREBUY_KLINE',
        `symbol=${symbol}`,
        `token=${ca}`,
        `decision=${gateDecision}`,
        `reason=${prebuyGateResult.gateReason}`,
        `continue=${decisionContinuedToBuy ? 1 : 0}`,
        `blockedStructural=${blockedStructural ? 1 : 0}`,
        unknownDataBlocked ? 'unknownDataBlocked=1' : null,
        prebuyGateResult.provider ? `provider=${prebuyGateResult.provider}` : 'provider=-',
        prebuyGateResult.poolAddress ? `poolAddress=${prebuyGateResult.poolAddress}` : 'poolAddress=-',
        Number.isFinite(prebuyGateResult.freshnessSec) ? `freshnessSec=${prebuyGateResult.freshnessSec}` : 'freshnessSec=-',
        Number.isFinite(prebuyGateResult.barCountSeen) ? `barCountSeen=${prebuyGateResult.barCountSeen}` : 'barCountSeen=-',
        Number.isFinite(prebuyGateResult.signalTsSec) ? `signalTsSec=${prebuyGateResult.signalTsSec}` : 'signalTsSec=-',
        Number.isFinite(prebuyGateResult.supportBreakPct) ? `supportBreakPct=${prebuyGateResult.supportBreakPct.toFixed(2)}` : null,
        Number.isFinite(prebuyGateResult.avgVol3) ? `avgVol3=${prebuyGateResult.avgVol3.toFixed(2)}` : null,
        Number.isFinite(prebuyGateResult.momFromLag1) ? `momFromLag1=${prebuyGateResult.momFromLag1.toFixed(2)}` : null,
        prebuyGateResult.backfill?.provider ? `backfillProvider=${prebuyGateResult.backfill.provider}` : 'backfillProvider=-',
        Number.isFinite(prebuyGateResult.backfill?.totalBefore) ? `backfillBars=${prebuyGateResult.backfill.totalBefore}` : 'backfillBars=-',
        prebuyGateResult.backfill?.reason ? `backfillReason=${prebuyGateResult.backfill.reason}` : 'backfillReason=-',
        prebuyGateResult.localWait ? `localWaitOk=${prebuyGateResult.localWait.ok ? 1 : 0}` : 'localWaitOk=-',
        prebuyGateResult.localWait?.timedOut ? 'localWaitTimedOut=1' : null,
      ].filter(Boolean).join(' ');

      if (gateDecision === 'PASS') {
        this.stats.not_ath_prebuy_kline_pass++;
      } else if (gateDecision === 'UNKNOWN_DATA') {
        this.stats.not_ath_prebuy_kline_unknown_data++;
      }

      if (blockedStructural) {
        this.stats.not_ath_prebuy_kline_block++;
        console.log(logParts);
        this.saveSignalRecord(signal, `NOT_ATH_PREBUY_KLINE_${gateDecision}`, null, false, { gateResult: prebuyGateResult });
        return { action: 'SKIP', reason: klineCheck.reason };
      }

      if (unknownDataBlocked) {
        this.stats.not_ath_prebuy_kline_block++;
        console.log(logParts);
        this.saveSignalRecord(signal, 'NOT_ATH_PREBUY_KLINE_UNKNOWN_DATA_BLOCKED', null, false, { gateResult: prebuyGateResult });
        return { action: 'SKIP', reason: `prebuy_unknown_data:${normalizedUnknownReason}` };
      }

      console.log(logParts);
    } catch (error) {
      this.stats.not_ath_prebuy_kline_unknown_data++;
      const unknownDataBlocked = this._notAthPrebuyUnknownDataFailClosed;
      const prebuyGateResult = {
        gateName: 'NOT_ATH_PREBUY_KLINE',
        gateDecision: 'UNKNOWN_DATA',
        gateReason: 'prebuy_exception',
        provider: null,
        poolAddress: null,
        freshnessSec: null,
        barCountSeen: null,
        signalTsSec,
        supportBreakPct: null,
        avgVol3: null,
        momFromLag1: null,
        decisionContinuedToBuy: !unknownDataBlocked,
        blockedStructural: false,
        unknownDataBlocked,
        observability: {
          backfillAttempted: false,
          backfillEnough: false,
          freshLocalBarsObserved: false,
          localWaitTimedOut: false,
          localWaitError: error.message,
          dataSource: null,
          failClosedApplied: unknownDataBlocked,
          providerDataState: 'prebuy_exception',
        },
        backfill: null,
        localWait: null,
        error: error.message,
        auditVersion: 2,
      };
      signal._prebuyGateResult = prebuyGateResult;
      console.warn([
        'NOT_ATH_PREBUY_KLINE',
        `symbol=${symbol}`,
        `token=${ca}`,
        'decision=UNKNOWN_DATA',
        'reason=prebuy_exception',
        `continue=${unknownDataBlocked ? 0 : 1}`,
        'blockedStructural=0',
        unknownDataBlocked ? 'unknownDataBlocked=1' : null,
        `signalTsSec=${signalTsSec}`,
        `error=${JSON.stringify(error.message)}`,
      ].filter(Boolean).join(' '));
      if (unknownDataBlocked) {
        this.stats.not_ath_prebuy_kline_block++;
        this.saveSignalRecord(signal, 'NOT_ATH_PREBUY_KLINE_UNKNOWN_DATA_BLOCKED', null, false, { gateResult: prebuyGateResult });
        return { action: 'SKIP', reason: 'prebuy_exception' };
      }
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
    if (this.liveExecution) {
      try {
        const solBalance = await this.liveExecution.getSolBalance();
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
      if (this.liveExecution) {
        tradeResult = await this.liveExecution.buy(ca, finalSize, { mc: signal.market_cap || 0 });
        if (tradeResult.success && this.livePositionMonitor) {
          await new Promise(r => setTimeout(r, 3000));
          const balance = await this.liveExecution.getTokenBalance(ca);
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
        this.saveSignalRecord(signal, 'PASS', null, true);
        return { action: 'BUY', size: finalSize };
      } else {
        this.stats.buy_failed++;
        console.log(`❌ [买入失败] $${symbol}: ${tradeResult.reason}`);
        this.saveSignalRecord(signal, 'PASS', null, false);
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
    const { isATH = true, signalTsSec = Math.floor(Date.now() / 1000) } = options;
    const cacheKey = `${tokenCA}:${isATH ? 'ath' : 'notath'}`;
    const nowMs = Date.now();
    const cachedResult = this._klineResultCache.get(cacheKey);
    if (cachedResult && nowMs - cachedResult.at < 30_000) {
      return cachedResult.result;
    }

    const persistKlineResult = (result) => {
      const normalized = {
        gateStatus: result?.gateStatus || (result?.passed ? 'PASS' : 'UNKNOWN_DATA'),
        provider: result?.provider || null,
        poolAddress: result?.poolAddress || null,
        freshnessSec: result?.freshnessSec ?? null,
        ...result,
      };
      this._klineResultCache.set(cacheKey, { at: nowMs, result: normalized });
      return normalized;
    };

    const scoreBars = (bars, poolAddress = '', metadata = {}) => {
      if (!bars?.length) return { passed: false, gateStatus: 'UNKNOWN_DATA', reason: 'no_bars', barCountSeen: 0, ...metadata };

      const current = bars[0];
      const prev = bars[1] || null;
      const fbr = current.open > 0 ? ((current.close - current.open) / current.open) * 100 : 0;

      if (isATH) {
        const passed = current.close > current.open;
        this._saveKlineBars(tokenCA, poolAddress, bars, {});
        return { passed, gateStatus: passed ? 'PASS' : 'BLOCK', close: current.close, open: current.open, fbr, volume: current.volume,
                 reason: passed ? 'pass' : 'not_green_bar', barCountSeen: bars.length, ...metadata };
      }

      if (!prev || bars.length < 3) return { passed: false, gateStatus: 'UNKNOWN_DATA', reason: 'no_history', barCountSeen: bars.length, ...metadata };

      const isRed = current.close < current.open;
      if (!isRed) return { passed: false, gateStatus: 'BLOCK', reason: 'not_red_bar', currentClose: current.close, barCountSeen: bars.length, ...metadata };

      const prev3 = bars.slice(1, 4);
      const minPrevLow = Math.min(...prev3.map(bar => Number(bar.low)));
      const supportBreak = current.close < minPrevLow;
      const supportBreakPct = minPrevLow > 0
        ? ((current.close - minPrevLow) / minPrevLow) * 100
        : 0;
      const avgVol3 = prev3.reduce((sum, b) => sum + b.volume, 0) / 3;
      const lowVolume = current.volume <= avgVol3;
      const momFromLag1 = prev3[0].close > 0
        ? ((current.close - prev3[0].close) / prev3[0].close) * 100
        : 0;
      const isActive = Math.abs(momFromLag1) > 30;

      let score = 2;
      if (!supportBreak) score += 1;
      if (lowVolume) score += 1;
      if (isActive) score += 1;

      const passed = score >= 4;

      let reason;
      if (passed) {
        reason = 'pass';
      } else if (supportBreak) {
        reason = 'support_break';
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

      return { passed, gateStatus: passed ? 'PASS' : 'BLOCK', close: current.close, open: current.open, fbr,
               volume: current.volume, reason, score,
               isRed, lowVolume, isActive, supportBreak,
               currentClose: current.close, minPrevLow, supportBreakPct,
               momFromLag1, avgVol3, barCountSeen: bars.length, ...metadata };
    };

    try {
      const repository = this.marketDataBackfill?.repository;
      const readLocalBars = (limit = 20) => {
        if (repository?.getLatestBars) {
          return repository.getLatestBars(tokenCA, limit) || [];
        }
        return this.db.prepare(`
          SELECT pool_address, timestamp, open, high, low, close, volume, provider, fetched_at
          FROM kline_1m
          WHERE token_ca = ?
          ORDER BY timestamp DESC
          LIMIT ?
        `).all(tokenCA, limit);
      };

      // 1) 优先尝试本地 K 线 truth source（repository/kline_1m），避免重复打外部接口
      try {
        const cachedBars = readLocalBars(20);
        if (cachedBars?.length >= (isATH ? 1 : 4)) {
          const bars = cachedBars.map(row => ({
            ts: Number(row.timestamp),
            open: Number(row.open),
            high: Number(row.high),
            low: Number(row.low),
            close: Number(row.close),
            volume: Number(row.volume)
          }));
          const freshnessSec = Math.max(0, signalTsSec - Number(bars[0].ts));
          const freshEnough = freshnessSec <= this._klineLocalFreshnessSec;
          if (freshEnough) {
            return persistKlineResult(scoreBars(bars, cachedBars[0]?.pool_address || this._poolCache.get(tokenCA) || '', {
              provider: 'local_cache',
              poolAddress: cachedBars[0]?.pool_address || this._poolCache.get(tokenCA) || null,
              freshnessSec,
              truthSource: 'kline_1m',
            }));
          }
          return persistKlineResult({ passed: false, gateStatus: 'UNKNOWN_DATA', reason: 'stale_local_bars', provider: 'local_cache', poolAddress: cachedBars[0]?.pool_address || this._poolCache.get(tokenCA) || null, freshnessSec, truthSource: 'kline_1m' });
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
          const primedBars = readLocalBars(20);
          if (primedBars?.length >= 4) {
            const bars = primedBars.map(row => ({
              ts: Number(row.timestamp),
              open: Number(row.open),
              high: Number(row.high),
              low: Number(row.low),
              close: Number(row.close),
              volume: Number(row.volume)
            }));
            const freshnessSec = Math.max(0, signalTsSec - Number(bars[0].ts));
            const freshEnough = freshnessSec <= this._klineLocalFreshnessSec;
            if (freshEnough) {
              return persistKlineResult(scoreBars(bars, primedBars[0]?.pool_address || this._poolCache.get(tokenCA) || '', {
                provider: 'local_primed',
                poolAddress: primedBars[0]?.pool_address || this._poolCache.get(tokenCA) || null,
                freshnessSec,
                truthSource: 'kline_1m',
              }));
            }
            return persistKlineResult({ passed: false, gateStatus: 'UNKNOWN_DATA', reason: 'stale_primed_bars', provider: 'local_primed', poolAddress: primedBars[0]?.pool_address || this._poolCache.get(tokenCA) || null, freshnessSec, truthSource: 'kline_1m' });
          }
        } catch {}
      }

      // 3) 如果刚被限流过，短时间内不再继续打外部接口
      if (nowMs < this._klineApiCooldownUntil) {
        return persistKlineResult({
          passed: false,
          gateStatus: 'UNKNOWN_DATA',
          reason: 'RATE_LIMITED',
          provider: 'external_api',
          truthSource: 'kline_1m',
          failOpenPrevented: true,
        });
      }

      // 4) 从共享 client 获取 pool 地址与 provider bars
      let poolAddress = this._poolCache?.get(tokenCA);
      if (!poolAddress) {
        const resolvedPool = await this.sharedMarketData.resolvePool(tokenCA);
        if (!resolvedPool.poolAddress) {
          return persistKlineResult({
            passed: false,
            gateStatus: 'UNKNOWN_DATA',
            reason: resolvedPool.error || 'no_pool',
            provider: resolvedPool.provider || 'shared_market_data',
            provenance: resolvedPool.provenance || null,
            truthSource: 'shared_market_data',
            failOpenPrevented: true,
          });
        }
        this._poolCache.set(tokenCA, resolvedPool.poolAddress);
        poolAddress = resolvedPool.poolAddress;
      }

      const ohlcvResult = await this.sharedMarketData.fetchRecentOhlcvByPool(tokenCA, poolAddress, {
        signalTsSec,
        bars: 6,
        beforeTimestamps: [null],
        allowDexFallback: false,
      });
      if (!ohlcvResult.bars?.length) {
        return persistKlineResult({
          passed: false,
          gateStatus: 'UNKNOWN_DATA',
          reason: ohlcvResult.error || 'no_bars',
          provider: ohlcvResult.provider || 'shared_market_data',
          poolAddress,
          provenance: ohlcvResult.provenance || null,
          truthSource: 'shared_market_data',
          failOpenPrevented: true,
        });
      }

      const bars = ohlcvResult.bars.map((bar) => ({
        ts: Number(bar.timestamp),
        open: Number(bar.open),
        high: Number(bar.high),
        low: Number(bar.low),
        close: Number(bar.close),
        volume: Number(bar.volume),
      }));
      const freshnessSec = Math.max(0, signalTsSec - Number(bars[0]?.ts || 0));
      if (freshnessSec > this._klineProviderFreshnessSec) {
        return persistKlineResult({
          passed: false,
          gateStatus: 'UNKNOWN_DATA',
          reason: 'stale_provider_bars',
          provider: ohlcvResult.provider || 'shared_market_data',
          poolAddress,
          freshnessSec,
          truthSource: 'shared_market_data',
          failOpenPrevented: true,
        });
      }

      return persistKlineResult(scoreBars(bars, poolAddress, {
        provider: ohlcvResult.provider || 'shared_market_data',
        poolAddress,
        freshnessSec,
        truthSource: 'shared_market_data',
      }));
    } catch (error) {
      const status = error?.response?.status;
      if (status === 429) {
        this._klineApiCooldownUntil = Date.now() + 120_000;
        this._klinePrimeCooldownUntil = Date.now() + 120_000;
        if (!this._lastKlineRateLimitLogAt || Date.now() - this._lastKlineRateLimitLogAt > 15_000) {
          console.warn(`⚠️ [K线检查] ${tokenCA.substring(0,8)} 接口限流: ${error.message} | 120s 内复用缓存/跳过外部查询`);
          this._lastKlineRateLimitLogAt = Date.now();
        }
        return persistKlineResult({ passed: false, gateStatus: 'UNKNOWN_DATA', reason: 'RATE_LIMITED', provider: 'external_api' });
      }
      console.warn(`⚠️ [K线检查] ${tokenCA.substring(0,8)} 检查失败: ${error.message}`);
      return persistKlineResult({ passed: false, gateStatus: 'UNKNOWN_DATA', reason: 'error_skip', provider: 'external_api' });
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
