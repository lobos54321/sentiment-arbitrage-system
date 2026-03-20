/**
 * Premium Signal Engine — v20 (BALANCED)
 *
 * 独立的信号处理引擎，专门处理付费频道信号
 * Pipeline: 信号 → 预检 → NOT_ATH均衡过滤 → 执行
 *
 * v20: 纯 NOT_ATH 均衡策略（ATH#1 路径已禁用）
 * - 入场过滤: is_ath=false | super_index≥120 | ai_index 50-79 | market_cap≤20K
 * - 不再依赖 DexScreener 涨速（去掉网络延迟，提升执行速度）
 * - 仓位: 0.06 SOL，最大并发 ≤ 8
 * - 出场: SL-20% | TP1@+80%卖60%→SL移至成本 | DW=10根 | MH=20根
 * - 回测(6天 Mar13-18, 真实GeckoTerminal 1分钟K线):
 *   26笔 | WR=65.4% | EV=+21.3%/笔 | 日均8笔(03/17-18) | 月回+104%(3SOL)
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
      // Symbol 级去重：同名代币 15 分钟内只处理一次（防仿盘）
      const lastSymbolSeen = this.recentSymbols.get(symbol);
      if (lastSymbolSeen && (Date.now() - lastSymbolSeen) < 15 * 60 * 1000) {
        this.stats.duplicates_skipped++;
        this.stats.funnel.drop_dedup++;
        console.log(`⏭️  [Symbol去重] $${symbol} 15分钟内已处理过同名代币，跳过`);
        return { action: 'SKIP', reason: 'symbol_duplicate' };
      }
      // 通过去重检查后，记录本次处理时间
      if (symbol) this.recentSymbols.set(symbol, Date.now());

      const cooldownUntil = this.exitCooldown.get(symbol);
      if (cooldownUntil && Date.now() < cooldownUntil) {
        const remainSec = Math.round((cooldownUntil - Date.now()) / 1000);
        this.stats.funnel.drop_dedup++;
        console.log(`⏭️  [冷却中] $${symbol} 退出后冷却期，剩余${remainSec}s，跳过`);
        return { action: 'SKIP', reason: 'exit_cooldown' };
      }
      this.stats.funnel.f1_after_dedup++;

      // ─── Step 2: v20 — ATH#1 路径已禁用，所有信号走 NOT_ATH 均衡过滤 ───
      // ATH 信号回测 WR=20%，EV 为负；NOT_ATH 均衡型 WR=65%, EV=+21%
      const isATH = signal.is_ath === true;
      if (isATH) {
        this.stats.funnel.drop_not_ath++;
        console.log(`⏭️ [v20] $${signal.symbol} ATH信号 → ATH路径已禁用，跳过`);
        this.saveSignalRecord(signal, 'ATH_DISABLED_V20', null);
        return { action: 'SKIP', reason: 'ath_disabled_v20' };
      }
      // NOT_ATH → 均衡过滤路径
      this.stats.funnel.drop_not_ath++;
      return await this._handleNotAthSignal(signal, t0);

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
   * 从 Egeye 指数对象中提取数值
   * 兼容两种格式:
   *   - 平铺数值:     signal.super_index = 128
   *   - 嵌套对象:     signal.indices.super_index = { value: 128 }
   *   - ATH 增量格式: signal.indices.super_index = { signal: 116, current: 124, growth: 6.9 }
   *
   * @param {number|Object|null} raw
   * @returns {number|null}
   */
  _getIdxVal(raw) {
    if (raw == null) return null;
    if (typeof raw === 'number') return raw;
    if (typeof raw === 'object') {
      if (typeof raw.value   === 'number') return raw.value;
      if (typeof raw.current === 'number') return raw.current;
      if (typeof raw.signal  === 'number') return raw.signal;
    }
    return null;
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

    // 0. 风控检查（每日亏损上限、连亏熔断、最大仓位）
    const riskCheck = this.riskManager.canTrade();
    if (!riskCheck.allowed) {
      console.log(`🛡️ [RISK] 风控拒绝: ${riskCheck.reason} | $${symbol}`);
      this.saveSignalRecord(signal, 'RISK_BLOCKED', null);
      return { action: 'SKIP', reason: `risk: ${riskCheck.reason}` };
    }

    // 1. MC 硬过滤：仅处理 MC ≤ 20K (v20 BALANCED)
    if (mc > 20000) {
      console.log(`⏭️ [v20/BALANCED] $${symbol} MC=$${(mc/1000).toFixed(1)}K > 20K → 跳过`);
      this.saveSignalRecord(signal, 'BALANCED_MC_TOO_HIGH', null);
      return { action: 'SKIP', reason: 'balanced_mc_too_high', mc };
    }

    // 2. super_index 过滤：需 ≥ 120 (v20 BALANCED)
    const superIdx = this._getIdxVal(signal.indices?.super_index ?? signal.super_index) ?? 0;
    if (superIdx < 120) {
      console.log(`⏭️ [v20/BALANCED] $${symbol} super_index=${superIdx} < 120 → 跳过`);
      this.saveSignalRecord(signal, 'BALANCED_SI_LOW', null);
      return { action: 'SKIP', reason: 'balanced_si_low', superIdx };
    }

    // 2b. ai_index 带状过滤：需在 50-79 区间 (v20 BALANCED)
    const aiIdx = this._getIdxVal(signal.indices?.ai_index ?? signal.ai_index);
    if (aiIdx !== null && (aiIdx < 50 || aiIdx >= 80)) {
      console.log(`⏭️ [v20/BALANCED] $${symbol} ai_index=${aiIdx} 不在50-79区间 → 跳过`);
      this.saveSignalRecord(signal, 'BALANCED_AI_OOB', null);
      return { action: 'SKIP', reason: 'balanced_ai_out_of_band', aiIdx };
    }

    // ─── 2c. 多维指数垃圾过滤 (v21) — 利用 Egeye 6 个未用指数 ────────
    // trade_index: 真实交易活跃度。过低 = 假量/无人接盘
    const tradeIdx = this._getIdxVal(signal.indices?.trade_index ?? signal.trade_index);
    if (tradeIdx !== null && tradeIdx < 50) {
      console.log(`⏭️ [v21/MULTI] $${symbol} trade_index=${tradeIdx} < 50 → 交易活跃度差，跳过`);
      this.saveSignalRecord(signal, 'MULTI_TRADE_LOW', null);
      return { action: 'SKIP', reason: 'multi_trade_low', tradeIdx };
    }

    // security_index: 合约安全评分。过低 = 存在蜜罐/税率/Owner风险
    const secIdx = this._getIdxVal(signal.indices?.security_index ?? signal.security_index);
    if (secIdx !== null && secIdx < 60) {
      console.log(`⏭️ [v21/MULTI] $${symbol} security_index=${secIdx} < 60 → 安全风险，跳过`);
      this.saveSignalRecord(signal, 'MULTI_SEC_LOW', null);
      return { action: 'SKIP', reason: 'multi_security_low', secIdx };
    }

    // address_index: 地址质量/分散度。过低 = 鲸鱼/团队集中持仓 = 砸盘风险
    const addrIdx = this._getIdxVal(signal.indices?.address_index ?? signal.address_index);
    if (addrIdx !== null && addrIdx < 40) {
      console.log(`⏭️ [v21/MULTI] $${symbol} address_index=${addrIdx} < 40 → 地址集中，跳过`);
      this.saveSignalRecord(signal, 'MULTI_ADDR_LOW', null);
      return { action: 'SKIP', reason: 'multi_address_low', addrIdx };
    }

    // ─── 2d. 链上基础指标垃圾过滤 (v21) ─────────────────────────────
    // Top10 持仓过高：前10地址持仓 > 60% = 庄家控盘，随时砸盘
    const top10 = signal.top10_pct || 0;
    if (top10 > 60) {
      console.log(`⏭️ [v21/MULTI] $${symbol} top10_pct=${top10.toFixed(1)}% > 60% → 鲸鱼集中，跳过`);
      this.saveSignalRecord(signal, 'MULTI_TOP10_HIGH', null);
      return { action: 'SKIP', reason: 'multi_top10_high', top10 };
    }

    // 持有人太少：holders < 50 = 流通极差，无接盘
    const holders = signal.holders || 0;
    if (holders > 0 && holders < 50) {
      console.log(`⏭️ [v21/MULTI] $${symbol} holders=${holders} < 50 → 持有人极少，跳过`);
      this.saveSignalRecord(signal, 'MULTI_HOLDERS_LOW', null);
      return { action: 'SKIP', reason: 'multi_holders_low', holders };
    }

    // media_index: v20 不限制（≥0 全通过）

    // 3. 软涨速检查（DexScreener m5）：DS失败继续，m5 < -5% 才跳过
    let priceChangePct = null;
    try {
      const dsUrl = `https://api.dexscreener.com/latest/dex/tokens/${ca}`;
      const res = await axios.get(dsUrl, { timeout: 5000, headers: { 'User-Agent': 'Mozilla/5.0' } });
      const pair = res.data?.pairs?.[0];
      if (pair?.priceChange?.m5 !== undefined) priceChangePct = pair.priceChange.m5;
    } catch (err) {
      console.log(`⚠️ [v20/BALANCED] $${symbol} DexScreener失败: ${err.message}，继续执行`);
    }
    if (priceChangePct !== null && priceChangePct < -5) {
      console.log(`⏭️ [v20/BALANCED] $${symbol} 5min涨速=${priceChangePct.toFixed(1)}% < -5%（价格下跌中）→ 跳过`);
      this.saveSignalRecord(signal, 'BALANCED_FALLING', null);
      return { action: 'SKIP', reason: 'balanced_falling', velocity: priceChangePct };
    }

    // 4. 已持仓检查
    if (this.livePositionMonitor?.positions?.has(ca) || this.shadowTracker.hasOpenPosition(ca)) {
      console.log(`⏭️ [v20/BALANCED] $${symbol} 已持仓 → 跳过`);
      return { action: 'SKIP', reason: 'already_in_position' };
    }

    const elapsed = Date.now() - t0;
    const aiIdxStr  = aiIdx    !== null ? ` AI=${aiIdx}`         : '';
    const trIdxStr  = tradeIdx !== null ? ` TR=${tradeIdx}`      : '';
    const secIdxStr = secIdx   !== null ? ` SEC=${secIdx}`       : '';
    const addrIdxStr= addrIdx  !== null ? ` ADDR=${addrIdx}`     : '';
    const top10Str  = top10 > 0         ? ` top10=${top10.toFixed(0)}%` : '';
    const velStr    = priceChangePct !== null ? ` vel5m=${priceChangePct >= 0 ? '+' : ''}${priceChangePct.toFixed(1)}%` : '';
    console.log(`🚀 [v21/BALANCED] $${symbol} ✅ MC=$${(mc/1000).toFixed(1)}K SI=${superIdx}${aiIdxStr}${trIdxStr}${secIdxStr}${addrIdxStr}${top10Str}${velStr} | 决策耗时:${elapsed}ms`);

    const finalSize = 0.06;
    const aiResult = {
      action: 'BUY_FULL', confidence: 85,
      narrative_tier: 'CONFIRMED',
      narrative_reason: `v21/BALANCED: MC=$${(mc/1000).toFixed(1)}K SI=${superIdx}${aiIdxStr}${trIdxStr}${secIdxStr}${addrIdxStr}`,
      entry_timing: 'MOMENTUM', stop_loss_percent: 15,
      exitStrategy: 'NOT_ATH'
    };

    if (this.shadowMode) {
      this.stats.shadow_logged++;
      this.stats.funnel.f12_executed++;
      console.log(`🎭 [SHADOW/BALANCED] 模拟买入 $${symbol} | ${finalSize} SOL`);
      this.saveSignalRecord(signal, 'PASS', aiResult, true);
      this.saveShadowTrade(signal, aiResult, finalSize);
      const liveMC = this._getCachedMC(ca);
      this.shadowTracker.addPosition(ca, symbol, liveMC || mc, aiResult.confidence);
      if (this.livePriceMonitor) this.livePriceMonitor.addToken(ca);
      return { action: 'SHADOW_BUY', size: finalSize, ai: aiResult };
    }

    if (!this.autoBuyEnabled) {
      console.log(`📋 [通知/BALANCED] 建议买入 $${symbol} | ${finalSize} SOL (自动买入未开启)`);
      this.saveSignalRecord(signal, 'PASS', aiResult, false);
      return { action: 'NOTIFY', size: finalSize, ai: aiResult };
    }

    // 实盘执行
    if (this.jupiterExecutor) {
      try {
        const solBalance = await this.jupiterExecutor.getSolBalance();
        const minRequired = finalSize + 0.025;
        if (solBalance < minRequired) {
          console.log(`⛔ [BALANCED/余额不足] ${solBalance.toFixed(4)} < ${minRequired.toFixed(4)} → 跳过`);
          this.saveSignalRecord(signal, 'PASS', aiResult, false);
          return { action: 'SKIP_INSUFFICIENT_BALANCE', balance: solBalance };
        }
      } catch (e) {
        console.warn(`⚠️ [BALANCED/余额检查失败] ${e.message}`);
      }
    }
    console.log(`💰 [BALANCED/执行] 买入 $${symbol} | ${finalSize} SOL...`);
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
      console.error(`❌ [v20/BALANCED] 执行失败: ${err.message}`);
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
      // 原子写入，防止崩溃时 JSON 文件损坏
      atomicWriteJSON(this._watchlistPath, data).catch((err) => {
        console.warn(`⚠️ [v16] 观察列表原子写入失败: ${err.message}`);
      });
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
