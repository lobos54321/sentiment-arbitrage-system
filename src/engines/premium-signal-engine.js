/**
 * Premium Signal Engine — v19
 *
 * 独立的信号处理引擎，专门处理付费频道信号
 * Pipeline: 信号 → 预检 → 链上快照 → Hard Gates → v19条件过滤 → 执行
 *
 * v19: ATH#1 原策略 + NOT_ATH 涨速过滤新路径
 * - ATH#1: 同v18，MC 30-300K + Super_cur 80-1000 + SupΔ≥5 + Trade/Addr/Sec过滤
 * - NOT_ATH: MC<30K + 实时涨速≥30%(DexScreener priceChange.m5) + super_index≥100 + ai_index≥40 + media_index≥60
 *   → 回测(86笔): WR=27%, EV=+7.9%/笔, 大亏率0%
 * - 仓位: 0.06 SOL
 * - ATH出场: ASYMMETRIC (SL-35%→TP1@50%卖60%→SL移至0%→TP2@100% + 15分死水/30分大限)
 * - NOT_ATH出场: NOT_ATH (SL-15%→TP1@80%卖60%→SL移至0%→TP2@100% + 8分死水/15分大限)
 * - 回测(3天 Mar16-18): ATH 87笔 +0.69SOL | NOT_ATH 86笔 +0.41SOL
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
      errors: 0,
      // 漏斗计数器（每层被过滤掉的数量）
      funnel: {
        f0_received:       0,  // 原始信号数
        f1_after_dedup:    0,  // 去重后
        f2_is_ath:         0,  // 通过ATH检查
        f3_is_ath1:        0,  // 通过ATH#1检查
        f4_precheck:       0,  // 通过freeze/mint预检
        f5_mc:             0,  // 通过MC过滤
        f6_super:          0,  // 通过Super指标
        f7_trade:          0,  // 通过Trade指标
        f8_addr:           0,  // 通过Addr指标
        f9_sec:            0,  // 通过Sec指标
        f10_position_slot: 0,  // 通过仓位槽位检查
        f11_anti_chase:    0,  // 通过防追高
        f12_executed:      0,  // 最终执行（含shadow）
        // 被各层过滤掉的数量
        drop_dedup:        0,
        drop_not_ath:      0,
        drop_not_ath1:     0,
        drop_precheck:     0,
        drop_mc:           0,
        drop_super:        0,
        drop_trade:        0,
        drop_addr:         0,
        drop_sec:          0,
        drop_position_full:0,
        drop_anti_chase:   0,
        drop_already_hold: 0,
      }
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

      // v17: 不再需要观察列表（ATH#1直接入场）
      this._loadWatchlist(); // 兼容旧数据

      // v13: 启动SOL市场环境检查（每5分钟）
      this._startSolMarketCheck();

      // v17: 观察列表不再使用，清理旧数据
      this._watchlist.clear();
      this._saveWatchlist();

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
    this.stats.funnel.f0_received++;
    if (this.stats.funnel.f0_received % 50 === 0) this.printFunnel();
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
        this.stats.funnel.drop_dedup++;
        console.log(`⏭️  [去重] ${shortCA}... 5分钟内已处理，跳过`);
        return { action: 'SKIP', reason: 'duplicate' };
      }
      this.markProcessed(ca);

      const symbol = signal.symbol;
      const lastSymbolSeen = this.recentSymbols.get(symbol);
      if (lastSymbolSeen && (Date.now() - lastSymbolSeen) < 15 * 60 * 1000) {
        this.stats.duplicates_skipped++;
        this.stats.funnel.drop_dedup++;
        console.log(`⏭️  [Symbol去重] $${symbol} 15分钟内已处理过同名代币，跳过`);
        return { action: 'SKIP', reason: 'symbol_duplicate' };
      }

      const cooldownUntil = this.exitCooldown.get(symbol);
      if (cooldownUntil && Date.now() < cooldownUntil) {
        const remainSec = Math.round((cooldownUntil - Date.now()) / 1000);
        this.stats.funnel.drop_dedup++;
        console.log(`⏭️  [冷却中] $${symbol} 退出后冷却期，剩余${remainSec}s，跳过`);
        return { action: 'SKIP', reason: 'exit_cooldown' };
      }
      this.stats.funnel.f1_after_dedup++;

      // ─── Step 2: ATH 检查 — 非ATH走独立的涨速过滤路径 (~300ms) ───
      const isATH = signal.is_ath === true;
      if (!isATH) {
        this.stats.funnel.drop_not_ath++;
        // v19: NOT_ATH 涨速过滤路径
        return await this._handleNotAthSignal(signal, t0);
      }
      this.stats.funnel.f2_is_ath++;

      // ─── Step 3: 市场环境 + Freeze/Mint 预检 (全内存, ~0ms) ───
      if (this._solMarketPaused) {
        this.stats.funnel.drop_precheck++;
        console.log(`⏸️ [v17] SOL 24h跌>10%，市场暂停中，不开新仓`);
        return { action: 'SKIP', reason: 'market_paused' };
      }

      if (signal.freeze_ok === false || signal.mint_ok === false) {
        this.stats.precheck_failed++;
        this.stats.funnel.drop_precheck++;
        console.log(`🚫 [预检] freeze=${signal.freeze_ok} mint=${signal.mint_ok} → 跳过`);
        this.saveSignalRecord(signal, 'PRECHECK_FAIL', null);
        return { action: 'SKIP', reason: 'precheck_failed' };
      }
      if (signal.freeze_ok === null || signal.mint_ok === null) {
        console.log(`⚠️ [预检] freeze=${signal.freeze_ok} mint=${signal.mint_ok} 未知(ATH信号无此数据)，继续`);
      }
      this.stats.funnel.f4_precheck++;

      // ─── Step 4: v18 所有过滤 — 全部来自 signal.indices (~0ms) ─
      const idx = signal.indices;
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
        this.stats.funnel.drop_not_ath1++;
        console.log(`⏭️ [v17] $${signal.symbol} ATH#${currentAthNum} → 仅ATH#1入场`);
        this.saveSignalRecord(signal, 'V17_NOT_ATH1', null);
        return { action: 'SKIP', reason: 'v17_only_ath1' };
      }
      this.stats.funnel.f3_is_ath1++;

      // ─── 风控检查（在 ATH 计数提交之前）─────────────────────────
      // 风控拒绝不应消耗 ATH#1 — 损失限额重置后该 token 仍应有机会进场
      const riskCheck = this.riskManager.canTrade();
      if (!riskCheck.allowed) {
        console.log(`🛡️ [RISK] 风控拒绝: ${riskCheck.reason} | $${signal.symbol} ATH#1 计数未消耗`);
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
        this.stats.funnel.drop_already_hold++;
        console.log(`⏭️ [v17] $${signal.symbol} 已持仓 → 跳过`);
        return { action: 'SKIP', reason: 'already_holding' };
      }
      if (this.shadowTracker.hasOpenPosition(ca)) {
        this.stats.funnel.drop_already_hold++;
        console.log(`⏭️ [已持仓] $${signal.symbol} Shadow已有未平仓持仓，跳过`);
        return { action: 'SKIP', reason: 'already_in_position' };
      }

      // 仓位槽位检查 — 只算在险仓位
      const allPositions = this.livePositionMonitor?.positions;
      const atRiskCount = allPositions ? [...allPositions.values()].filter(p => !p.tp1).length : 0;
      const moonBagCount = (allPositions?.size || 0) - atRiskCount;
      if (atRiskCount >= 5) {
        this.stats.funnel.drop_position_full++;
        console.log(`⏭️ [v18] 在险仓位 ${atRiskCount}/5 已满 → 不开新仓 (${moonBagCount}个零成本登月仓不占槽)`);
        return { action: 'SKIP', reason: 'max_atrisk_positions' };
      }
      if (moonBagCount > 0) {
        console.log(`   ℹ️ [槽位] 在险: ${atRiskCount}/5 | 零成本登月仓: ${moonBagCount}个`);
      }

      // v18 指标过滤 (全部 signal.indices，无网络请求)
      const mc = signal.market_cap || 0;
      if (mc < 30000 || mc > 300000) {
        this.stats.funnel.drop_mc++;
        console.log(`⏭️ [v18] MC=$${(mc/1000).toFixed(1)}K 不在$30-300K → 跳过`);
        this.saveSignalRecord(signal, 'V18_MC_FILTER', null);
        return { action: 'SKIP', reason: 'v18_mc_filter', mc };
      }
      this.stats.funnel.f5_mc++;
      if (superCurrent < 80 || superCurrent > 1000) {
        this.stats.funnel.drop_super++;
        console.log(`⏭️ [v18] Super_cur=${superCurrent} 不在80-1000 → 跳过`);
        this.saveSignalRecord(signal, 'V18_SUPERCUR_FILTER', null);
        return { action: 'SKIP', reason: 'v18_supercur_filter', superCurrent };
      }
      if (superDelta < 5) {
        this.stats.funnel.drop_super++;
        console.log(`⏭️ [v18] SupΔ=${superDelta}<5 → 跳过`);
        this.saveSignalRecord(signal, 'V18_SUPDELTA_FILTER', null);
        return { action: 'SKIP', reason: 'v18_supdelta_filter', superDelta };
      }
      this.stats.funnel.f6_super++;
      if (tradeCurrent < 1) {
        this.stats.funnel.drop_trade++;
        console.log(`⏭️ [v18] Trade_cur=${tradeCurrent}<1 → 跳过`);
        this.saveSignalRecord(signal, 'V18_TRADECUR_FILTER', null);
        return { action: 'SKIP', reason: 'v18_tradecur_filter', tradeCurrent };
      }
      if (tradeDelta < 1) {
        this.stats.funnel.drop_trade++;
        console.log(`⏭️ [v18] TΔ=${tradeDelta}<1 → 跳过`);
        this.saveSignalRecord(signal, 'V18_TRADEDELTA_FILTER', null);
        return { action: 'SKIP', reason: 'v18_tradedelta_filter', tradeDelta };
      }
      this.stats.funnel.f7_trade++;
      if (addressCurrent < 3) {
        this.stats.funnel.drop_addr++;
        console.log(`⏭️ [v18] Addr_cur=${addressCurrent}<3 → 跳过`);
        this.saveSignalRecord(signal, 'V18_ADDR_FILTER', null);
        return { action: 'SKIP', reason: 'v18_addr_filter', addressCurrent };
      }
      this.stats.funnel.f8_addr++;
      if (securityCurrent < 15) {
        this.stats.funnel.drop_sec++;
        console.log(`⏭️ [v18] Sec_cur=${securityCurrent}<15 → 跳过`);
        this.saveSignalRecord(signal, 'V18_SEC_FILTER', null);
        return { action: 'SKIP', reason: 'v18_sec_filter', securityCurrent };
      }
      this.stats.funnel.f9_sec++;
      this.stats.funnel.f10_position_slot++; // 到这里仓位槽位已通过（检查在上面）

      // ─── Step 5: 唯一网络请求 — 实时价格查询 + 防追高 (~200ms) ─
      // 优先 livePriceMonitor 缓存（0ms），否则查 Jupiter Price API
      const liveMC = this._getCachedMC(ca);
      if (liveMC > 0 && mc > 0 && liveMC > mc * 1.20) {
        const premium = ((liveMC / mc - 1) * 100).toFixed(1);
        this.stats.funnel.drop_anti_chase++;
        console.log(`🚫 [防追高] $${signal.symbol} 信号MC=$${(mc/1000).toFixed(1)}K → 实时MC=$${(liveMC/1000).toFixed(1)}K (溢价+${premium}% > 20%) → 放弃`);
        this.saveSignalRecord(signal, 'ANTI_CHASE', null);
        return { action: 'SKIP', reason: 'anti_chase', premium: parseFloat(premium) };
      }
      this.stats.funnel.f11_anti_chase++;

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
      if (this.shadowMode) {
        this.stats.shadow_logged++;
        this.stats.funnel.f12_executed++;
        console.log(`🎭 [SHADOW] 模拟买入 $${signal.symbol} | ${finalSize} SOL`);
        this.saveSignalRecord(signal, 'PASS', aiResult, true);
        this.saveShadowTrade(signal, aiResult, finalSize);
        const entryMC = liveMC || mc;
        this.shadowTracker.addPosition(ca, signal.symbol || 'UNKNOWN', entryMC, aiResult.confidence);
        this._watchlist.delete(ca);
        this._saveWatchlist();
        this.recentSymbols.set(signal.symbol, Date.now());
        // Shadow 模式也不需要重复 addToken — shadowTracker 有自己的价格逻辑
        return { action: 'SHADOW_BUY', size: finalSize, ai: aiResult };
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
        this.stats.funnel.f12_executed++;
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
   * v19: NOT_ATH 涨速过滤路径
   * 条件: MC<30K + DexScreener priceChange.m5 ≥ 20% + super_index ≥ 100
   * 回测胜率 78%, EV +0.023 SOL/笔
   */
  async _handleNotAthSignal(signal, t0) {
    const ca = signal.token_ca;
    const symbol = signal.symbol || ca.slice(0, 8);
    const mc = signal.market_cap || 0;

    // 1. MC 硬过滤：仅处理 MC < 30K
    if (mc >= 30000) {
      console.log(`⏭️ [v19/NOT_ATH] $${symbol} MC=$${(mc/1000).toFixed(1)}K ≥ 30K → 跳过`);
      this.saveSignalRecord(signal, 'NOT_ATH_MC_TOO_HIGH', null);
      return { action: 'SKIP', reason: 'not_ath_mc_too_high', mc };
    }

    // 2. super_index 软过滤：需 ≥ 100
    const superIdx = signal.super_index || 0;
    if (superIdx < 100) {
      console.log(`⏭️ [v19/NOT_ATH] $${symbol} super_index=${superIdx} < 100 → 跳过`);
      this.saveSignalRecord(signal, 'NOT_ATH_SI_LOW', null);
      return { action: 'SKIP', reason: 'not_ath_si_low', superIdx };
    }

    // 2b. ai_index 过滤：需 ≥ 40 (回测优化结果)
    // ai_index 在 signal.indices.ai_index 里，不在顶层
    const aiIdx = signal.indices?.ai_index ?? null;
    if (aiIdx !== null && aiIdx < 40) {
      console.log(`⏭️ [v19/NOT_ATH] $${symbol} ai_index=${aiIdx} < 40 → 跳过`);
      this.saveSignalRecord(signal, 'NOT_ATH_AI_LOW', null);
      return { action: 'SKIP', reason: 'not_ath_ai_low', aiIdx };
    }

    // 2c. media_index 过滤：需 ≥ 60 (回测: EV +7.9%→+11.6%, WR 26.7%→30.4%)
    const mediaIdx = signal.indices?.media_index ?? null;
    if (mediaIdx !== null && mediaIdx < 60) {
      console.log(`⏭️ [v19/NOT_ATH] $${symbol} media_index=${mediaIdx} < 60 → 跳过`);
      this.saveSignalRecord(signal, 'NOT_ATH_MEDIA_LOW', null);
      return { action: 'SKIP', reason: 'not_ath_media_low', mediaIdx };
    }

    // 3. 实时涨速检查：调用 DexScreener priceChange.m5
    let priceChangePct = null;
    try {
      const dsUrl = `https://api.dexscreener.com/latest/dex/tokens/${ca}`;
      const res = await axios.get(dsUrl, {
        timeout: 5000,
        headers: { 'User-Agent': 'Mozilla/5.0' }
      });
      const pair = res.data?.pairs?.[0];
      if (pair?.priceChange?.m5 !== undefined) {
        priceChangePct = pair.priceChange.m5;  // 已经是百分比，如 +25.3 表示涨25.3%
      }
    } catch (err) {
      console.log(`⚠️ [v19/NOT_ATH] $${symbol} DexScreener 请求失败: ${err.message}，跳过`);
      this.saveSignalRecord(signal, 'NOT_ATH_DS_FAIL', null);
      return { action: 'SKIP', reason: 'not_ath_ds_fail' };
    }

    if (priceChangePct === null) {
      console.log(`⏭️ [v19/NOT_ATH] $${symbol} DexScreener 无m5数据 → 跳过`);
      this.saveSignalRecord(signal, 'NOT_ATH_NO_M5', null);
      return { action: 'SKIP', reason: 'not_ath_no_m5' };
    }

    const VEL_THRESHOLD = 30; // 5分钟涨速 ≥ 30% (回测优化: 原20%→30%)
    if (priceChangePct < VEL_THRESHOLD) {
      console.log(`⏭️ [v19/NOT_ATH] $${symbol} 5min涨速=${priceChangePct.toFixed(1)}% < ${VEL_THRESHOLD}% → 跳过`);
      this.saveSignalRecord(signal, 'NOT_ATH_VEL_LOW', null);
      return { action: 'SKIP', reason: 'not_ath_vel_low', velocity: priceChangePct };
    }

    // 4. 已持仓检查
    if (this.livePositionMonitor?.positions?.has(ca) || this.shadowTracker.hasOpenPosition(ca)) {
      console.log(`⏭️ [v19/NOT_ATH] $${symbol} 已持仓 → 跳过`);
      return { action: 'SKIP', reason: 'already_in_position' };
    }

    const elapsed = Date.now() - t0;
    const aiIdxStr   = aiIdx    !== null ? ` AI=${aiIdx}`       : '';
    const mediaIdxStr = mediaIdx !== null ? ` Media=${mediaIdx}` : '';
    console.log(`🚀 [v19/NOT_ATH] $${symbol} ✅ MC=$${(mc/1000).toFixed(1)}K SI=${superIdx}${aiIdxStr}${mediaIdxStr} 5min涨速=+${priceChangePct.toFixed(1)}% | 决策耗时:${elapsed}ms`);

    const finalSize = 0.06;
    const aiResult = {
      action: 'BUY_FULL', confidence: 80,
      narrative_tier: 'CONFIRMED',
      narrative_reason: `v19/NOT_ATH: MC=$${(mc/1000).toFixed(1)}K SI=${superIdx}${aiIdxStr}${mediaIdxStr} vel5m=+${priceChangePct.toFixed(1)}%`,
      entry_timing: 'MOMENTUM', stop_loss_percent: 15,
      exitStrategy: 'NOT_ATH'
    };

    if (this.shadowMode) {
      this.stats.shadow_logged++;
      this.stats.funnel.f12_executed++;
      console.log(`🎭 [SHADOW/NOT_ATH] 模拟买入 $${symbol} | ${finalSize} SOL`);
      this.saveSignalRecord(signal, 'PASS', aiResult, true);
      this.saveShadowTrade(signal, aiResult, finalSize);
      const liveMC = this._getCachedMC(ca);
      this.shadowTracker.addPosition(ca, symbol, liveMC || mc, aiResult.confidence);
      if (this.livePriceMonitor) this.livePriceMonitor.addToken(ca);
      return { action: 'SHADOW_BUY', size: finalSize, ai: aiResult };
    }

    if (!this.autoBuyEnabled) {
      console.log(`📋 [通知/NOT_ATH] 建议买入 $${symbol} | ${finalSize} SOL (自动买入未开启)`);
      this.saveSignalRecord(signal, 'PASS', aiResult, false);
      return { action: 'NOTIFY', size: finalSize, ai: aiResult };
    }

    // 实盘执行
    if (this.jupiterExecutor) {
      try {
        const solBalance = await this.jupiterExecutor.getSolBalance();
        const minRequired = finalSize + 0.025;
        if (solBalance < minRequired) {
          console.log(`⛔ [NOT_ATH/余额不足] ${solBalance.toFixed(4)} < ${minRequired.toFixed(4)} → 跳过`);
          this.saveSignalRecord(signal, 'PASS', aiResult, false);
          return { action: 'SKIP_INSUFFICIENT_BALANCE', balance: solBalance };
        }
      } catch (e) {
        console.warn(`⚠️ [NOT_ATH/余额检查失败] ${e.message}`);
      }
    }
    console.log(`💰 [NOT_ATH/执行] 买入 $${symbol} | ${finalSize} SOL...`);
    try {
      this.stats.executed++;
      this.stats.funnel.f12_executed++;
      let tradeResult;
      if (this.jupiterExecutor) {
        tradeResult = await this.jupiterExecutor.buy(ca, finalSize, { mc: mc || 0 });
        if (tradeResult.success && this.livePositionMonitor) {
          await new Promise(r => setTimeout(r, 3000));
          const balance = await this.jupiterExecutor.getTokenBalance(ca);
          if (balance.amount > 0) {
            const tokenDecimals = balance.decimals || 6;
            const actualTokenAmount = balance.amount / Math.pow(10, tokenDecimals);
            const entryPrice = finalSize / actualTokenAmount;
            const liveMC = this._getCachedMC(ca);
            this.livePositionMonitor.addPosition(
              ca, symbol, entryPrice, liveMC || mc, finalSize,
              balance.amount, tokenDecimals, 'MEDIUM', 'NOT_ATH'
            );
            if (this.livePriceMonitor) this.livePriceMonitor.addToken(ca);
          }
        }
      }
      this.saveSignalRecord(signal, 'PASS', aiResult, true);
      return { action: 'BUY', size: finalSize, ai: aiResult, exec: tradeResult };
    } catch (err) {
      console.error(`❌ [v19/NOT_ATH] 执行失败: ${err.message}`);
      this.stats.errors++;
      return { action: 'ERROR', error: err.message };
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
   * 打印信号漏斗统计
   */
  printFunnel() {
    const f = this.stats.funnel;
    const total = f.f0_received;
    if (total === 0) return;

    const pct = (n) => total > 0 ? ((n / total) * 100).toFixed(1) + '%' : '0%';
    const bar = (n) => {
      const filled = Math.round((n / total) * 20);
      return '█'.repeat(filled) + '░'.repeat(20 - filled);
    };

    console.log('\n' + '═'.repeat(65));
    console.log('📊 [信号漏斗] 过滤效率统计');
    console.log('═'.repeat(65));
    console.log(`  原始信号          ${String(f.f0_received).padStart(4)}  ${bar(f.f0_received)}  100%`);
    console.log(`  去重后            ${String(f.f1_after_dedup).padStart(4)}  ${bar(f.f1_after_dedup)}  ${pct(f.f1_after_dedup)}  (丢弃${f.drop_dedup})`);
    console.log(`  是ATH信号         ${String(f.f2_is_ath).padStart(4)}  ${bar(f.f2_is_ath)}  ${pct(f.f2_is_ath)}  (丢弃${f.drop_not_ath})`);
    console.log(`  是ATH#1           ${String(f.f3_is_ath1).padStart(4)}  ${bar(f.f3_is_ath1)}  ${pct(f.f3_is_ath1)}  (丢弃${f.drop_not_ath1})`);
    console.log(`  预检通过          ${String(f.f4_precheck).padStart(4)}  ${bar(f.f4_precheck)}  ${pct(f.f4_precheck)}  (丢弃${f.drop_precheck})`);
    console.log(`  MC $30-300K       ${String(f.f5_mc).padStart(4)}  ${bar(f.f5_mc)}  ${pct(f.f5_mc)}  (丢弃${f.drop_mc})`);
    console.log(`  Super指标         ${String(f.f6_super).padStart(4)}  ${bar(f.f6_super)}  ${pct(f.f6_super)}  (丢弃${f.drop_super})`);
    console.log(`  Trade指标         ${String(f.f7_trade).padStart(4)}  ${bar(f.f7_trade)}  ${pct(f.f7_trade)}  (丢弃${f.drop_trade})`);
    console.log(`  Addr指标          ${String(f.f8_addr).padStart(4)}  ${bar(f.f8_addr)}  ${pct(f.f8_addr)}  (丢弃${f.drop_addr})`);
    console.log(`  Sec指标           ${String(f.f9_sec).padStart(4)}  ${bar(f.f9_sec)}  ${pct(f.f9_sec)}  (丢弃${f.drop_sec})`);
    console.log(`  仓位槽位          ${String(f.f10_position_slot).padStart(4)}  ${bar(f.f10_position_slot)}  ${pct(f.f10_position_slot)}  (丢弃${f.drop_position_full + f.drop_already_hold})`);
    console.log(`  防追高            ${String(f.f11_anti_chase).padStart(4)}  ${bar(f.f11_anti_chase)}  ${pct(f.f11_anti_chase)}  (丢弃${f.drop_anti_chase})`);
    console.log('─'.repeat(65));
    console.log(`  ✅ 最终执行       ${String(f.f12_executed).padStart(4)}  ${bar(f.f12_executed)}  ${pct(f.f12_executed)}`);
    console.log('═'.repeat(65) + '\n');
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
