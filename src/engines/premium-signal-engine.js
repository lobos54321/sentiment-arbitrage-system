/**
 * Premium Signal Engine
 *
 * 独立的信号处理引擎，专门处理付费频道信号
 * Pipeline: 信号 → 预检 → 链上快照 → Hard Gates → AI分析 → 仓位 → Exit Gates → 执行
 */

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

      // 量化评分
      let score = 0;
      const scoreDetails = [];

      if (dexData) {
        const buyRatio = dexData.sell_count_24h > 0 ? dexData.buy_count_24h / dexData.sell_count_24h : 0;
        if (buyRatio > 1.5) { score += 25; scoreDetails.push(`买卖比${buyRatio.toFixed(2)}(+25)`); }
        else if (buyRatio > 1.2) { score += 15; scoreDetails.push(`买卖比${buyRatio.toFixed(2)}(+15)`); }
        else if (buyRatio < 0.7) { score -= 20; scoreDetails.push(`买卖比${buyRatio.toFixed(2)}(-20)`); }
        else if (buyRatio < 1.0) { score -= 10; scoreDetails.push(`买卖比${buyRatio.toFixed(2)}(-10)`); }

        if (dexData.volume_24h > 50000) { score += 20; scoreDetails.push(`量$${(dexData.volume_24h/1000).toFixed(0)}K(+20)`); }
        else if (dexData.volume_24h > 20000) { score += 10; scoreDetails.push(`量$${(dexData.volume_24h/1000).toFixed(0)}K(+10)`); }
        else if (dexData.volume_24h < 5000) { score -= 10; scoreDetails.push(`量低(-10)`); }

        const liqUsd = dexData.liquidity_usd > 0 ? dexData.liquidity_usd : (snapshot?.liquidity ? snapshot.liquidity * 150 : 0);
        if (liqUsd > 10000) { score += 15; scoreDetails.push(`流动性$${(liqUsd/1000).toFixed(0)}K(+15)`); }
        else if (liqUsd > 3000) { score += 5; scoreDetails.push(`流动性$${(liqUsd/1000).toFixed(0)}K(+5)`); }
        else if (liqUsd < 1000 && dexData.volume_24h < 10000) { score -= 15; scoreDetails.push(`流动性不足(-15)`); }

        // v9: 5m动量评分（区分ATH和New Trending信号）
        // 数据教训: 噜噜(5m+172%→-24.8%), Hamburg(5m+256%→-27.0%) — 买在wick顶
        // ATH信号的5m动量=突破动量(正面), New Trending的5m过高=pump已结束(负面)
        const isATHSignal = signal.is_ath === true;
        if (!isATHSignal && dexData.price_change_5m > 200) {
          // v9: New Trending + 5m>200% → 直接跳过（pump已经结束，买入就是接盘）
          console.log(`⛔ [v9:5m过热] $${signal.symbol} 5m+${dexData.price_change_5m.toFixed(0)}% > 200% → 非ATH信号，pump已过`);
          this.saveSignalRecord(signal, '5M_OVERHEAT', null);
          return { action: 'SKIP', reason: '5m_overheat' };
        } else if (!isATHSignal && dexData.price_change_5m > 150) {
          // 5m 150-200%: 不加分不减分（风险中性区）
          scoreDetails.push(`5m+${dexData.price_change_5m}%(0⚠️)`);
        } else if (dexData.price_change_5m > 100) { score += 15; scoreDetails.push(`5m+${dexData.price_change_5m}%(+15)`); }
        else if (dexData.price_change_5m > 50) { score += 20; scoreDetails.push(`5m+${dexData.price_change_5m}%(+20)`); }
        else if (dexData.price_change_5m > 10) { score += 15; scoreDetails.push(`5m+${dexData.price_change_5m}%(+15)`); }
        else if (dexData.price_change_5m > 5) { score += 10; scoreDetails.push(`5m+${dexData.price_change_5m}%(+10)`); }
        else if (dexData.price_change_5m < -20) { score -= 15; scoreDetails.push(`5m${dexData.price_change_5m}%(-15)`); }

        // 1小时买入活跃度
        if (dexData.buy_count_1h > 50) { score += 10; scoreDetails.push(`1h买${dexData.buy_count_1h}(+10)`); }
      }

      // MC 评分（优先用 DexScreener 数据，fallback 到 Jupiter）
      // 实测304笔: 0-10K +50.5% | 10-20K +17.8% | 20-30K +10.3% | 30-40K +31.8% | 40K+ 打平或亏
      let mc = dexData?.market_cap || signal.market_cap || 0;

      // 如果 MC=0，尝试从 Jupiter Price API 获取
      if (mc === 0 && this.livePriceMonitor) {
        const jupiterPrice = this.livePriceMonitor.priceCache.get(ca);
        if (jupiterPrice && jupiterPrice.mc && (Date.now() - jupiterPrice.timestamp) < 30000) {
          mc = jupiterPrice.mc;
          console.log(`  📡 [Jupiter补充] MC: $${(mc/1000).toFixed(1)}K`);
        }
      }

      // MC 仍然为 0，跳过（无法评估）
      if (mc === 0) {
        console.log(`⏭️ [无MC] $${signal.symbol} DexScreener + Jupiter 都没有 MC 数据，跳过`);
        this.saveSignalRecord(signal, 'NO_MC_DATA', null);
        return { action: 'SKIP', reason: 'no_mc_data' };
      }

      if (mc > 0) {
        if (mc >= 5000 && mc < 15000) { score += 15; scoreDetails.push(`MC甜蜜区(+15)`); }
        else if (mc >= 15000) { score -= 25; scoreDetails.push(`MC过高${(mc/1000).toFixed(0)}K(-25)`); }
      }

      if (snapshot) {
        if (snapshot.wash_flag === 'HIGH') {
          console.log(`⛔ [评分] 洗盘HIGH → 直接SKIP`);
          this.saveSignalRecord(signal, 'WASH_HIGH', null);
          return { action: 'SKIP', reason: 'wash_high' };
        }
        if (snapshot.wash_flag === 'MEDIUM') { score -= 5; scoreDetails.push(`洗盘MED(-5)`); }
        if (snapshot.top10_percent > 50) { score -= 20; scoreDetails.push(`Top10>${snapshot.top10_percent}%(-20)`); }
        if (snapshot.key_risk_wallets?.length >= 3) { score -= 15; scoreDetails.push(`风险钱包(-15)`); }
      }

      // 5m 下跌超过 -10% 不追跌
      if (dexData && dexData.price_change_5m < -10) {
        console.log(`⛔ [评分] 5m下跌${dexData.price_change_5m.toFixed(1)}% → 直接SKIP`);
        this.saveSignalRecord(signal, '5M_DUMP', null);
        return { action: 'SKIP', reason: '5m_dump' };
      }

      if (signal.freeze_ok && signal.mint_ok) { score += 10; scoreDetails.push(`安全✅(+10)`); }

      // Telegram 传播度（数据验证：$PUMP 14群50条 → 赢了，TG>10群是强信号）
      if (this.buzzScanner && signal.symbol) {
        try {
          const buzz = await this.buzzScanner.scan(signal.symbol, ca);
          if (buzz.score > 0) {
            score += buzz.score;
            scoreDetails.push(`TG热度${buzz.uniqueGroups}群${buzz.mentions}条(+${buzz.score})`);
            // TG > 10 群额外加分（强传播信号）
            if (buzz.uniqueGroups >= 10) {
              score += 10;
              scoreDetails.push(`TG超热${buzz.uniqueGroups}群(+10)`);
            }
          }
        } catch (e) {
          // 非关键，忽略
        }
      }

      // ===== v8: Egeye AI Index 评分（漏斗第一层：信号源质量）=====
      // 7个指标: Super Index(综合), AI Index(AI预测), Trade Index(交易活跃),
      //          Security Index(安全), Address Index(地址增长), Viral Index(病毒传播), Media Index(媒体)
      const idx = signal.indices;
      let superIndexVal = 0;  // 用于决策信念等级
      let isHighConviction = false;
      if (idx) {
        console.log(`  🔮 [Egeye指数]${idx.super_index ? ` Super=${idx.super_index.current}(${idx.super_index.growth > 0 ? '+' : ''}${idx.super_index.growth.toFixed(0)}%)` : ''}${idx.security_index ? ` Security=${idx.security_index.current}` : ''}${idx.viral_index ? ` Viral=${idx.viral_index.current}` : ''}${idx.trade_index ? ` Trade=${idx.trade_index.current}` : ''}${idx.ai_index ? ` AI=${idx.ai_index.current}` : ''}`);

        // Super Index — 信号源综合评分，权重最大
        // CAINE Super=115→2283(9.72x价格), BRUH Super=85→186(7.06x价格)
        if (idx.super_index) {
          superIndexVal = idx.super_index.current;
          if (superIndexVal >= 120) { score += 30; scoreDetails.push(`Super指数${superIndexVal}(+30)`); }
          else if (superIndexVal >= 100) { score += 20; scoreDetails.push(`Super指数${superIndexVal}(+20)`); }
          else if (superIndexVal >= 80) { score += 10; scoreDetails.push(`Super指数${superIndexVal}(+10)`); }
          else if (superIndexVal < 50) { score -= 10; scoreDetails.push(`Super指数低${superIndexVal}(-10)`); }
          // Super Index 增长速度 — 增长快意味着信号源认为币在加速上涨
          if (idx.super_index.growth > 50) { score += 10; scoreDetails.push(`Super增长${idx.super_index.growth.toFixed(0)}%(+10)`); }
        }

        // Security Index — 安全性，低=高风险
        if (idx.security_index) {
          if (idx.security_index.current < 20) { score -= 15; scoreDetails.push(`安全指数低${idx.security_index.current}(-15)`); }
          else if (idx.security_index.current >= 40) { score += 5; scoreDetails.push(`安全OK${idx.security_index.current}(+5)`); }
        }

        // Viral Index — 病毒传播度，从0增长说明正在扩散
        if (idx.viral_index && idx.viral_index.current > 0) {
          score += 10; scoreDetails.push(`Viral${idx.viral_index.current}(+10)`);
          // Viral从0暴涨 = 正在爆发
          if (idx.viral_index.signal === 0 && idx.viral_index.current >= 5) {
            score += 10; scoreDetails.push(`Viral爆发0→${idx.viral_index.current}(+10)`);
          }
        }

        // Trade Index — 交易活跃度
        if (idx.trade_index && idx.trade_index.current >= 3) {
          score += 5; scoreDetails.push(`Trade活跃${idx.trade_index.current}(+5)`);
        }

        // Address Index — 地址增长
        if (idx.address_index && idx.address_index.current >= 10) {
          score += 5; scoreDetails.push(`地址增长${idx.address_index.current}(+5)`);
        }

        // 高信念判定: Super Index >= 100 + Security >= 30
        isHighConviction = superIndexVal >= 100 &&
          (!idx.security_index || idx.security_index.current >= 30);
      }

      // 重复信号追踪
      const sigHistory = this.signalHistory.get(ca);
      const isRepeat = sigHistory && sigHistory.count >= 2;
      const mcMultiple = (sigHistory && sigHistory.firstMC > 0 && dexData?.market_cap > 0)
        ? dexData.market_cap / sigHistory.firstMC
        : 0;
      const isDoubled = isRepeat && mcMultiple >= 1.8 && mcMultiple <= 3.0;

      if (isRepeat) {
        scoreDetails.push(`重复${sigHistory.count}次|MC${mcMultiple.toFixed(1)}x`);
      }

      // ATH 翻倍信号检测
      const isATH = signal.is_ath === true;
      const athGain = signal.gain_pct || 0;
      const isATHDoubled = isATH && athGain >= 80 && athGain <= 300; // 涨了 80-300%

      if (isATH) {
        scoreDetails.push(`ATH+${athGain.toFixed(0)}%`);
        console.log(`📈 [ATH信号] $${signal.symbol} 涨了${athGain}% | source=${signal.source} | is_ath=${signal.is_ath}`);
      }

      // v8 漏斗策略入场规则（信号源Index + 链上数据 + TG传播度多维筛选）
      // 关键发现: Score=75的交易2W/5L=29%WR, Score≥80的4W/1L=80%WR
      // 但纯分数不够 — BRUH(Score=40)涨了7x, CAINE(Super=115)涨了9.72x
      // v8: 结合Egeye Signal Index + DexScreener链上数据 → 漏斗式筛选
      let scoreAction = 'SKIP';
      let tieredPositionSize = this.positionSol;
      let entryTier = 'NONE'; // CORE / AUX / ATH_RE
      let tradeConviction = 'NORMAL'; // NORMAL / HIGH → 传递给退出策略

      const bsr = dexData && dexData.sell_count_24h > 0
        ? dexData.buy_count_24h / dexData.sell_count_24h
        : 0;

      // 重复信号处理
      const prevScore = sigHistory?.lastScore || 0;
      const scoreImproved = sigHistory && (score >= 85 || score - prevScore >= 15);
      if (sigHistory) sigHistory.lastScore = score;
      else if (this.signalHistory.has(ca)) this.signalHistory.get(ca).lastScore = score;

      // === ATH重复信号专用通道（v8新增：67%WR历史验证）===
      // ATH 2次+ = 信号源为我们二次确认了金狗
      const athCount = sigHistory ? sigHistory.athCount || 0 : 0;
      if (isATH && sigHistory) {
        sigHistory.athCount = (sigHistory.athCount || 0) + 1;
        // 记录ATH时的Super Index（观察增长趋势）
        if (idx?.super_index) {
          sigHistory.lastSuperIndex = idx.super_index.current;
          if (!sigHistory.firstSuperIndex) sigHistory.firstSuperIndex = idx.super_index.signal;
        }
      }

      const alreadyHolding = this.livePositionMonitor?.positions?.has(ca);

      if (alreadyHolding) {
        console.log(`⏭️ [已持仓] $${signal.symbol} 已有仓位，不加仓`);
      } else if (isATH && athCount >= 1 && mc <= 50000) {
        // ATH Re-Entry: 2次ATH信号（重复ATH = 金狗确认，历史67%WR）
        // MC上限放宽到50K（ATH信号的币已经涨了，但仍有上升空间）
        const superGrowth = sigHistory?.firstSuperIndex && idx?.super_index
          ? ((idx.super_index.current / sigHistory.firstSuperIndex - 1) * 100).toFixed(0)
          : '?';
        scoreAction = 'BUY_FULL';
        tieredPositionSize = 0.04;
        entryTier = 'ATH_RE';
        tradeConviction = 'HIGH';
        console.log(`🔥 [ATH_RE] $${signal.symbol} 第${athCount+1}次ATH | MC=$${(mc/1000).toFixed(1)}K | gain+${athGain.toFixed(0)}% | Super增长${superGrowth}% → 买 0.04 SOL`);
      } else if (mc >= 50000) {
        console.log(`⏭️ [MC过高] $${signal.symbol} MC=$${(mc/1000).toFixed(1)}K >= 50K → 不买`);
      } else if (mc >= 15000 && !isATH) {
        console.log(`⏭️ [MC过高] $${signal.symbol} MC=$${(mc/1000).toFixed(1)}K >= 15K → 不买（非ATH信号）`);
      } else if (score < 75) {
        // v8: 回调到75（因为有了Index数据后筛选更精准, 且BRUH Score=40但Super=85涨了7x）
        // 如果没有Index数据则仍需75分；有高Super Index时可以放宽
        const hasStrongIndex = superIndexVal >= 80;
        if (!hasStrongIndex) {
          console.log(`⏭️ [评分不足] $${signal.symbol} MC=$${(mc/1000).toFixed(1)}K 评分${score}<75 无强Index → 不够`);
        } else {
          // Super Index >= 80 但分数不到75 → 仍然可以考虑（BRUH案例）
          console.log(`🔮 [Index补救] $${signal.symbol} 评分${score}<75 但Super=${superIndexVal}>=80 → 继续评估`);
          // 不设scoreAction，继续往下走MC判断
        }
      }

      // 如果还没SKIP也没BUY，根据MC区间决策
      if (scoreAction === 'SKIP' && !alreadyHolding) {
        // 活跃度检查（所有非ATH_RE通道都需要）
        if (dexData && dexData.buy_count_1h < 100) {
          console.log(`⏭️ [活跃度低] $${signal.symbol} 1h买入仅${dexData.buy_count_1h}<100 → 流动性不足`);
        } else if (mc >= 5000 && mc < 10000 && score >= 75) {
          // CORE: MC $5-10K, Score≥75
          scoreAction = 'BUY_FULL';
          tieredPositionSize = 0.05;
          entryTier = 'CORE';
          if (isHighConviction) tradeConviction = 'HIGH';
          console.log(`💎 [CORE] $${signal.symbol} MC=$${(mc/1000).toFixed(1)}K 评分${score}>=75${isHighConviction ? ' 🔥高信念' : ''} → 买 0.05 SOL`);
        } else if (mc >= 5000 && mc < 10000 && superIndexVal >= 80) {
          // CORE via Index: MC $5-10K, Score<75但Super>=80（BRUH案例修复）
          scoreAction = 'BUY_FULL';
          tieredPositionSize = 0.04; // 稍小仓位
          entryTier = 'CORE';
          tradeConviction = 'HIGH';
          console.log(`🔮 [CORE-IDX] $${signal.symbol} MC=$${(mc/1000).toFixed(1)}K Super=${superIndexVal}>=80 → 买 0.04 SOL`);
        } else if (mc >= 10000 && mc < 15000 && score >= 75 && bsr >= 1.2) {
          // AUX: MC $10-15K, Score≥75, BSR≥1.2
          scoreAction = 'BUY_FULL';
          tieredPositionSize = 0.04;
          entryTier = 'AUX';
          if (isHighConviction) tradeConviction = 'HIGH';
          console.log(`💎 [AUX] $${signal.symbol} MC=$${(mc/1000).toFixed(1)}K 评分${score}>=75 BSR=${bsr.toFixed(2)}>=1.2${isHighConviction ? ' 🔥高信念' : ''} → 买 0.04 SOL`);
        } else if (mc >= 10000 && mc < 15000 && superIndexVal >= 80) {
          // AUX via Index: MC $10-15K, Super>=80
          scoreAction = 'BUY_FULL';
          tieredPositionSize = 0.03; // 小仓位
          entryTier = 'AUX';
          tradeConviction = 'HIGH';
          console.log(`🔮 [AUX-IDX] $${signal.symbol} MC=$${(mc/1000).toFixed(1)}K Super=${superIndexVal}>=80 → 买 0.03 SOL`);
        } else if (mc < 5000) {
          console.log(`⏭️ [MC过低] $${signal.symbol} MC=$${(mc/1000).toFixed(1)}K < 5K → 不买`);
        }
      }

      console.log(`📈 [评分] ${score}分 [${scoreDetails.join(' | ')}] → ${scoreAction}${tradeConviction === 'HIGH' ? ' 🔥高信念' : ''}`);

      if (scoreAction === 'SKIP') {
        this.stats.ai_skipped++;
        console.log(`⏭️  [评分] SKIP: 评分${score}分不足`);
        const skipResult = { action: 'SKIP', confidence: score, narrative_tier: 'D', narrative_reason: scoreDetails.join(', '), entry_timing: 'LATE', stop_loss_percent: 20 };
        this.saveSignalRecord(signal, gateResult.status, skipResult);
        return { action: 'SKIP', reason: 'score_too_low', details: skipResult };
      }

      // Step 5b: AI 二次过滤（暂停 — 直接使用量化评分）
      let aiResult;
      {
        // 🔧 AI 暂停：跳过 Claude 分析，直接用量化评分结果
        console.log(`⏸️  [AI] 已暂停，使用量化评分继续`);
        aiResult = {
          action: scoreAction,
          confidence: score,
          narrative_tier: score >= 50 ? 'A' : score >= 30 ? 'B' : 'D',
          narrative_reason: scoreDetails.join(', ') + ' (AI disabled)',
          entry_timing: dexData?.price_change_5m > 5 ? 'OPTIMAL' : 'EARLY',
          stop_loss_percent: 20
        };

        // AI 置信度太低也跳过
        if (aiResult.confidence < 30) {
          this.stats.ai_skipped++;
          console.log(`⏭️  [AI低信心] 置信度 ${aiResult.confidence} < 30 → SKIP`);
          this.saveSignalRecord(signal, gateResult.status, aiResult);
          return { action: 'SKIP', reason: 'ai_low_confidence', details: aiResult };
        }

        // FINAL策略: MC > 20K 已在入场规则中过滤, 无需额外AI置信度检查

        // AI 返回 BUY_HALF 也允许，后面按半仓处理
        if (aiResult.action === 'BUY_HALF') {
          console.log(`📊 [AI] BUY_HALF → 半仓买入`);
        }
      }

      console.log(`✅ [AI] ${aiResult.action} | 叙事: ${aiResult.narrative_tier} | 置信度: ${aiResult.confidence} | 时机: ${aiResult.entry_timing}`);

      // 灰名单 + AI 置信度低 → 跳过
      if (isGreylist && aiResult.confidence < 60) {
        this.stats.ai_skipped++;
        console.log(`⏭️  [决策] 灰名单 + 低置信度(${aiResult.confidence}) → 跳过`);
        this.saveSignalRecord(signal, 'GREYLIST_LOW_CONF', aiResult);
        return { action: 'SKIP', reason: 'greylist_low_confidence' };
      }

      // Step 6: 仓位检查（使用 MC 梯度仓位）
      const positionSize = tieredPositionSize;
      const decision = {
        action: 'AUTO_BUY',
        chain: 'SOL',
        auto_buy_enabled: this.autoBuyEnabled,
        position: positionSize
      };
      const tokenData = {
        token_ca: ca,
        symbol: signal.symbol,
        narrative: aiResult.narrative_reason
      };

      const positionCheck = this.shadowMode ? { allowed: true } : await this.positionSizer.canOpenPosition(decision, tokenData);
      if (!positionCheck.allowed) {
        this.stats.position_denied++;
        console.log(`🚫 [仓位] 不允许: ${positionCheck.reason}`);
        this.saveSignalRecord(signal, gateResult.status, aiResult);
        return { action: 'SKIP', reason: 'position_denied', details: positionCheck.reason };
      }
      const finalSize = positionCheck.adjusted_size || positionSize;
      console.log(`✅ [仓位] 允许开仓: ${finalSize} SOL`);

      // Step 7: Exit Gates
      console.log('🚪 [Exit Gates] 退出可行性检查...');
      const exitResult = this.exitGateFilter.evaluate(snapshot, finalSize);
      if (exitResult.status === 'REJECT') {
        this.stats.exit_gate_rejected++;
        console.log(`🚫 [Exit Gates] REJECT: ${exitResult.reasons.join(', ')}`);
        this.saveSignalRecord(signal, gateResult.status, aiResult);
        return { action: 'SKIP', reason: 'exit_gate_reject', details: exitResult.reasons };
      }
      console.log(`✅ [Exit Gates] ${exitResult.status}`);

      // Step 8: 检查是否已持仓
      if (this.shadowTracker.hasOpenPosition(ca)) {
        console.log(`⏭️ [已持仓] $${signal.symbol} 已有未平仓持仓，跳过`);
        return { action: 'SKIP', reason: 'already_in_position' };
      }

      // Step 9: 执行交易
      if (this.shadowMode) {
        this.stats.shadow_logged++;
        console.log(`🎭 [SHADOW] 模拟买入 $${signal.symbol} | ${finalSize} SOL | 叙事: ${aiResult.narrative_tier}`);
        this.saveSignalRecord(signal, gateResult.status, aiResult, true);
        this.saveShadowTrade(signal, aiResult, finalSize);

        // 优先用 LivePriceMonitor（Jupiter 实时价格）作为入场 MC
        let entryMC = dexData?.market_cap || signal.market_cap || 0;
        if (this.livePriceMonitor) {
          const livePrice = this.livePriceMonitor.priceCache.get(ca);
          if (livePrice && livePrice.mc && (Date.now() - livePrice.timestamp) < 10000) {
            console.log(`📡 [入场MC] Jupiter实时: $${(livePrice.mc/1000).toFixed(1)}K (DexScreener: $${(entryMC/1000).toFixed(1)}K)`);
            entryMC = livePrice.mc;
          }
        }
        this.shadowTracker.addPosition(ca, signal.symbol || 'UNKNOWN', entryMC, aiResult.confidence);

        // Shadow 模式也注册到 LivePriceMonitor（验证价格追踪）
        if (this.livePriceMonitor) {
          this.livePriceMonitor.addToken(ca);
        }

        return { action: 'SHADOW_BUY', size: finalSize, ai: aiResult };
      }

      if (!this.autoBuyEnabled) {
        console.log(`📋 [通知] 建议买入 $${signal.symbol} | ${finalSize} SOL (自动买入未开启)`);
        this.saveSignalRecord(signal, gateResult.status, aiResult, false);
        return { action: 'NOTIFY', size: finalSize, ai: aiResult };
      }

      // 🔧 实盘买入前：检查链上 SOL 余额
      if (this.jupiterExecutor) {
        try {
          const solBalance = await this.jupiterExecutor.getSolBalance();
          const minRequired = finalSize + 0.025; // 买入金额 + gas(~0.005) + Jito tip(~0.005) + priority fee + rent 预留
          if (solBalance < minRequired) {
            console.log(`⛔ [余额不足] SOL余额: ${solBalance.toFixed(4)} < 需要: ${minRequired.toFixed(4)} → 跳过买入 $${signal.symbol}`);
            this.saveSignalRecord(signal, gateResult.status, aiResult, false);
            return { action: 'SKIP_INSUFFICIENT_BALANCE', balance: solBalance, required: minRequired };
          }
          console.log(`💰 [余额检查] SOL余额: ${solBalance.toFixed(4)} ≥ 需要: ${minRequired.toFixed(4)} → 可以买入`);
        } catch (e) {
          console.warn(`⚠️ [余额检查] 查询失败: ${e.message}，继续买入`);
        }
      }

      // 实盘执行 (不重试，避免重复花 SOL)
      const maxBuyAttempts = 1;
      let lastError = null;

      for (let buyAttempt = 1; buyAttempt <= maxBuyAttempts; buyAttempt++) {
        console.log(`💰 [执行] 买入 $${signal.symbol} | ${finalSize} SOL... ${buyAttempt > 1 ? `(重试 ${buyAttempt}/${maxBuyAttempts})` : ''}`);

        try {
          let tradeResult;

          if (this.jupiterExecutor) {
            // Jupiter Swap 买入（传流动性+MC用于自适应滑点）
            tradeResult = await this.jupiterExecutor.buy(ca, finalSize, {
              liquidity: dexData?.liquidity_usd || 0,
              mc: mc || 0
            });

            // 注册到 LivePositionMonitor
            if (tradeResult.success && this.livePositionMonitor) {
              const entryMC = dexData?.market_cap || signal.market_cap || 0;

              // Ultra V3 /execute 同步确认，3秒足够余额更新
              await new Promise(r => setTimeout(r, 3000));
              const balance = await this.jupiterExecutor.getTokenBalance(ca);

              if (balance.amount <= 0) {
                console.error(`❌ [验证失败] 买入交易发送但余额为0，交易可能失败`);
                lastError = '买入后余额为0，交易可能失败';
                if (buyAttempt < maxBuyAttempts) {
                  console.log(`   🔄 等待3秒后重试...`);
                  await new Promise(r => setTimeout(r, 3000));
                  continue;  // 重试
                }
                this.stats.errors++;
                return { action: 'EXEC_FAILED', reason: lastError };
              }

              // 使用实际余额，而不是报价
              const tokenAmount = balance.amount;
              const tokenDecimals = balance.decimals || 6;
              console.log(`📦 [Token] 验证余额: ${tokenAmount} tokens (decimals: ${tokenDecimals})`);

              // 🔧 关键修复：使用实际交易价格计算 entryPrice (SOL 单位)
              // entryPrice = SOL 花费 / token 数量 (考虑 decimals)
              const actualTokenAmount = tokenAmount / Math.pow(10, tokenDecimals);
              const entryPrice = finalSize / actualTokenAmount;  // SOL per token
              console.log(`💰 [Entry] 入场价格: ${entryPrice.toFixed(10)} SOL/token | ${finalSize} SOL → ${actualTokenAmount.toFixed(2)} tokens`);

              this.livePositionMonitor.addPosition(
                ca,
                signal.symbol,
                entryPrice,
                entryMC,
                finalSize,
                tokenAmount,
                tokenDecimals,
                tradeConviction  // v8: 'NORMAL' or 'HIGH' — 高信念模式使用更宽松的退出策略
              );

              // 🔧 标记Symbol已处理（15分钟窗口）
              this.recentSymbols.set(signal.symbol, Date.now());
            }
          } else {
            // Fallback: GMGN Telegram 执行
            tradeResult = await this.executor.executeBuy(ca, 'SOL', finalSize);
          }

          this.stats.executed++;
          console.log(`✅ [执行] 交易成功: ${JSON.stringify(tradeResult)}`);
          this.saveSignalRecord(signal, gateResult.status, aiResult, true);
          return { action: 'EXECUTED', size: finalSize, ai: aiResult, trade: tradeResult };
        } catch (execError) {
          lastError = execError.message;
          console.error(`❌ [执行] 交易失败 (${buyAttempt}/${maxBuyAttempts}): ${execError.message}`);

          if (buyAttempt < maxBuyAttempts) {
            console.log(`   🔄 等待3秒后重试...`);
            await new Promise(r => setTimeout(r, 3000));
          }
        }
      }

      // 所有重试都失败
      this.stats.errors++;
      this.saveSignalRecord(signal, gateResult.status, aiResult, false);
      return { action: 'EXEC_FAILED', reason: lastError || '买入失败' };

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

  /**
   * 停止引擎
   */
  async stop() {
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
