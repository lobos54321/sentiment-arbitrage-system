/**
 * Position Monitor v7.0 - 翻倍出本 + AI动态管理策略
 *
 * 核心策略（猎手思维）：
 * 1. 止损：分级止损 + 时间止损
 * 2. 翻倍出本：+100% 卖 50%（收回本金，剩余全是利润）
 * 3. 利润仓：AI 实时监控，动态决定卖出时机
 * 4. 紧急逃生：Dev出逃/聪明钱出逃/流动性崩溃 → 立即全卖
 *
 * v7.0: 所有策略参数从 strategy.js 统一读取
 */

import { SolanaSnapshotService } from '../inputs/chain-snapshot-sol.js';
import { BSCSnapshotService } from '../inputs/chain-snapshot-bsc.js';
import { GMGNTelegramExecutor } from './gmgn-telegram-executor.js';
import { ExitCooldownService } from '../database/exit-cooldown.js';
// v7.5 REMOVED: import debotScout - 已删除旧API模式，getCurrentSmartMoney 使用 fallback
// v6.7 死狗学习池
import deadDogPool from '../risk/dead-dog-pool.js';
// v6.9 智能退出引擎
import { SmartExitEngine } from '../engines/smart-exit.js';
// v7.0 统一策略配置
import { EXIT_STRATEGY } from '../config/strategy.js';
// v7.1 AI Analyst for smart exit
import AIAnalyst from '../utils/ai-analyst.js';

export class PositionMonitorV2 {
  constructor(config, db) {
    this.config = config;
    this.db = db;

    // Services
    this.solService = new SolanaSnapshotService(config);
    this.bscService = new BSCSnapshotService(config);
    this.executor = new GMGNTelegramExecutor(config, db);
    this.exitCooldown = new ExitCooldownService(db);

    // v6.9 智能退出引擎
    this.smartExitEngine = new SmartExitEngine({
      enabled: config.SMART_EXIT_ENABLED !== false
    });

    // Monitor settings
    this.pollIntervalMs = config.POSITION_MONITOR_INTERVAL_MS || 60000;
    this.isRunning = false;

    // v7.0: 从 strategy.js 读取策略配置
    this.strategy = EXIT_STRATEGY;

    console.log('📊 Position Monitor v8.0 (AI驱动) initialized');
    console.log('   策略：叙事层级止损 + 叙事止盈 + 跟聪明钱 + AI确认');
    const nsl = this.strategy.NARRATIVE_STOP_LOSS || {};
    console.log(`   止损：TIER_S ${(nsl.TIER_S || -0.70) * 100}% | TIER_A ${(nsl.TIER_A || -0.60) * 100}% | TIER_B ${(nsl.TIER_B || -0.50) * 100}% | TIER_C ${(nsl.TIER_C || -0.40) * 100}%`);
    console.log(`   聪明钱容忍度：减少 ${this.strategy.SM_EXIT_BUFFER} 个以上才恐慌卖`);
    console.log(`   叙事止盈：TIER_S=10x+ | TIER_A=5x | TIER_B=2x | TIER_C=1x`);

    // v8.0 AI 确认器
    this.aiAnalyst = AIAnalyst;
  }

  /**
   * v6.7 获取基于叙事等级的止盈配置
   * @param {string} intentionTier - 叙事等级 (TIER_S/A/B/C)
   * @returns {Object} 止盈配置
   */
  getTakeProfitConfig(intentionTier) {
    const narrativeConfig = this.strategy.NARRATIVE_TAKE_PROFIT;

    // 根据叙事等级获取配置，默认使用普通配置
    if (intentionTier && narrativeConfig[intentionTier]) {
      return narrativeConfig[intentionTier];
    }

    // 默认使用标准配置
    return this.strategy.TAKE_PROFIT;
  }


  /**
   * 启动监控循环
   */
  async start() {
    if (this.isRunning) {
      console.log('⚠️  Position Monitor already running');
      return;
    }

    this.isRunning = true;
    console.log('▶️  Position Monitor v3 started');

    // v7.1: 初始化 AI Client 并注入到 SmartExitEngine
    if (process.env.AI_EXIT_ENABLED === 'true' && process.env.XAI_API_KEY) {
      try {
        const aiAnalyst = new AIAnalyst();
        aiAnalyst.init();
        this.smartExitEngine.setAIClient(aiAnalyst);
        console.log('   🤖 AI Exit enabled for SmartExitEngine');
      } catch (e) {
        console.log(`   ⚠️ AI Exit init failed: ${e.message}`);
      }
    }

    // 初始监控
    await this.monitorAllPositions();

    // 循环监控
    this.monitorInterval = setInterval(async () => {
      try {
        await this.monitorAllPositions();
      } catch (error) {
        console.error('❌ Monitor loop error:', error.message);
      }
    }, this.pollIntervalMs);
  }

  /**
   * 停止监控
   */
  stop() {
    if (this.monitorInterval) {
      clearInterval(this.monitorInterval);
      this.monitorInterval = null;
    }
    this.isRunning = false;
    console.log('⏹️  Position Monitor v3 stopped');
  }

  /**
   * 监控所有持仓
   */
  async monitorAllPositions() {
    try {
      const positions = this.db.prepare(`
        SELECT * FROM positions
        WHERE status IN ('open', 'partial')
        ORDER BY entry_time ASC
      `).all();

      if (positions.length === 0) {
        return;
      }

      console.log(`\n🔍 [Monitor] 监控 ${positions.length} 个持仓...`);

      for (const position of positions) {
        await this.monitorPosition(position);
      }

    } catch (error) {
      console.error('❌ Monitor all positions error:', error.message);
    }
  }

  /**
   * 监控单个持仓
   */
  async monitorPosition(position) {
    const chain = position.chain;
    const tokenCA = position.token_ca;
    const symbol = position.symbol || tokenCA.substring(0, 8);
    const isShadow = position.is_shadow === 1;

    try {
      // 1. 获取当前快照
      const snapshot = await this.getChainSnapshot(chain, tokenCA);
      if (!snapshot || !snapshot.current_price) {
        console.log(`   ⚠️  [${symbol}] 获取快照失败，跳过`);
        return;
      }

      // 2. 计算盈亏
      const pnl = this.calculatePnL(position, snapshot.current_price);

      // 3. 更新最高价记录
      await this.updateHighWaterMark(position, snapshot.current_price);

      // 4. 获取市场信号
      const signals = await this.getMarketSignals(position, snapshot);

      // 4.5 v6.9 智能退出引擎评估 (优先级高于传统策略)
      const smartExitData = {
        price: snapshot.current_price,
        volume: snapshot.volume_24h || 0,
        liquidity: snapshot.liquidity_usd || 0,
        smartMoneyCount: snapshot.smart_wallet_online || signals.current_smart_money || 0,
        tgMentions: signals.current_heat || 0,
        slippageBps: snapshot.slippage_sell_20pct || 0,
        sentiment: signals.sentiment || 'neutral'
      };
      // v7.5 改用异步方法，启用 AI 实时补刀分析
      const smartExitResult = await this.smartExitEngine.evaluateAsync(position, smartExitData, { useAI: true });

      // 如果智能引擎决定退出，优先执行
      if (smartExitResult.action !== 'HOLD' && smartExitResult.action !== 'ALERT') {
        console.log(`   ⚡ [SmartExit] ${symbol}: ${smartExitResult.action} - ${smartExitResult.reason}`);

        // v7.3 記錄是否使用了 AI 退出
        const isAIExit = smartExitResult.action.includes('AI_') ||
          smartExitResult.aiAnalysis?.aiUsed === true;
        if (isAIExit) {
          try {
            this.db.prepare(`
              UPDATE positions SET ai_exit_used = 1 WHERE id = ?
            `).run(position.id);
          } catch (e) { /* ignore */ }
        }

        const decision = {
          action: smartExitResult.action,
          sell_percent: smartExitResult.action.includes('PROFIT') ? 50 : 100,
          reason: `[SmartExit] ${smartExitResult.reason}`
        };
        await this.executeDecision(position, decision, snapshot, pnl, isShadow);
        return;
      }

      // 5. 根据持仓状态决定策略 (传统逻辑)
      let decision;
      if (position.status === 'open' && !position.breakeven_done) {
        // 未保本阶段
        decision = await this.evaluatePreBreakeven(position, snapshot, pnl, signals);
      } else {
        // 已保本，利润仓阶段
        decision = await this.evaluateProfitPosition(position, snapshot, pnl, signals);
      }

      // 6. 执行决策
      if (decision.action !== 'HOLD') {
        await this.executeDecision(position, decision, snapshot, pnl, isShadow);
      } else {
        const statusEmoji = position.breakeven_done ? '💰' : '📊';
        console.log(`   ${statusEmoji} [${symbol}] 持有 | PnL: ${pnl.pnl_percent >= 0 ? '+' : ''}${pnl.pnl_percent.toFixed(1)}% | ${decision.reason}`);

        // v8.0 保存实时 PnL 和止盈策略到数据库，供 Dashboard 显示
        try {
          this.db.prepare(`
            UPDATE positions 
            SET current_pnl = ?, 
                tier_strategy = ?,
                last_monitor_time = datetime('now')
            WHERE id = ?
          `).run(
            pnl.pnl_percent,
            decision.reason,
            position.id
          );
        } catch (e) {
          // 如果列不存在，尝试添加
          if (e.message.includes('no column')) {
            try {
              this.db.exec(`ALTER TABLE positions ADD COLUMN current_pnl REAL DEFAULT 0`);
              this.db.exec(`ALTER TABLE positions ADD COLUMN tier_strategy TEXT DEFAULT ''`);
              this.db.exec(`ALTER TABLE positions ADD COLUMN last_monitor_time TEXT`);
              console.log('   📊 已添加 current_pnl, tier_strategy 列');
            } catch (alterErr) { /* ignore if column already exists */ }
          }
        }
      }


    } catch (error) {
      console.error(`❌ Monitor position error [${symbol}]:`, error.message);
    }
  }

  /**
   * 未出本阶段的决策（翻倍前）
   * 🔥 v6.0 Diamond Hands: 分级止损 + 聪明钱容忍度 + 趋势保护
   */
  async evaluatePreBreakeven(position, snapshot, pnl, signals) {
    const chain = position.chain;
    const tokenCA = position.token_ca;
    const symbol = position.symbol || tokenCA.substring(0, 8);

    // 计算持仓时间
    const entryTimeStr = position.entry_time || position.created_at;
    const entryTime = new Date(entryTimeStr.includes('Z') || entryTimeStr.includes('+')
      ? entryTimeStr
      : entryTimeStr + 'Z');
    const holdingMinutes = (Date.now() - entryTime.getTime()) / 1000 / 60;
    const timeStopMinutes = chain === 'SOL'
      ? this.strategy.TIME_STOP.SOL_MINUTES
      : this.strategy.TIME_STOP.BSC_MINUTES;

    // 🛑 1. 基于叙事层级的止损 (v8.0)
    // 从 position 获取 AI 给出的叙事等级
    const intentionTier = position.intention_tier || position.intentionTier || 'DEFAULT';
    const narrativeStopLoss = this.strategy.NARRATIVE_STOP_LOSS || {};
    let stopLossThreshold = narrativeStopLoss[intentionTier] || narrativeStopLoss.DEFAULT || -0.50;

    const tierEmoji = {
      'TIER_S': '🏆',
      'TIER_A': '🥇',
      'TIER_B': '🥈',
      'TIER_C': '⚠️',
      'DEFAULT': '📊'
    }[intentionTier] || '📊';

    // 🔒 v6.8 金库模式：已出本的仓位使用超宽止损
    // 判断条件：已卖出过（double_profit_taken 或 half_profit_taken）或 remaining < entry
    const isVaulted = position.double_profit_taken ||
      position.half_profit_taken ||
      (position.remaining_percent && position.remaining_percent < 100);

    if (isVaulted && this.strategy.VAULT_MODE?.ENABLED) {
      stopLossThreshold = this.strategy.VAULT_MODE.STOP_LOSS; // -80%
      if (!position._vaultLogShown) {
        console.log(`   🔒 [${symbol}] 金库模式：已出本仓位，止损放宽到 ${stopLossThreshold * 100}%`);
        position._vaultLogShown = true;
      }
    }

    // 检查价格止损 (v8.1 修复: pnl.pnl_percent 是百分比如 -45.3, stopLossThreshold 是小数如 -0.40)
    if (pnl.pnl_percent / 100 <= stopLossThreshold) {
      // 🛡️ K线趋势保护：如果有反弹迹象，暂缓卖出
      if (this.strategy.TREND_PROTECTION && snapshot) {
        const lastPrice = position.last_price || position.entry_price;
        const currentPrice = snapshot.current_price || snapshot.price || pnl.current_price;
        const priceChange = (currentPrice - lastPrice) / lastPrice;

        if (priceChange > this.strategy.TREND_PROTECTION.BOUNCE_THRESHOLD) {
          console.log(`   🛡️ [${symbol}] 触发止损但有反弹迹象 (+${(priceChange * 100).toFixed(1)}%)，暂缓卖出`);
          return {
            action: 'HOLD',
            reason: `触发止损但反弹中 (${pnl.pnl_percent.toFixed(1)}%, 反弹+${(priceChange * 100).toFixed(1)}%)`
          };
        }
      }

      // 🤖 v8.0 AI 确认机制：在止损前询问 AI 是否应该卖出
      // 只有设置了 AI Analyst 才启用
      if (this.aiAnalyst && !position._aiStopLossChecked) {
        console.log(`   🤖 [${symbol}] 触发 ${intentionTier} 止损 (${pnl.pnl_percent.toFixed(1)}%)，询问 AI 确认...`);
        try {
          const aiDecision = await this.aiAnalyst.shouldStopLoss(position, pnl, snapshot);
          position._aiStopLossChecked = true;
          position._aiStopLossTime = Date.now();

          if (aiDecision && aiDecision.action === 'HOLD') {
            console.log(`   🤖 [${symbol}] AI 建议继续持有: ${aiDecision.reason || '观察中'}`);
            return {
              action: 'HOLD',
              reason: `AI建议持有 (${intentionTier}: ${pnl.pnl_percent.toFixed(1)}%, AI: ${aiDecision.reason || '观察'})`
            };
          } else {
            console.log(`   🤖 [${symbol}] AI 确认执行止损: ${aiDecision?.reason || '趋势转弱'}`);
          }
        } catch (e) {
          console.log(`   ⚠️ [${symbol}] AI 确认失败: ${e.message}，执行止损`);
        }
      }

      return {
        action: 'STOP_LOSS',
        sell_percent: 100,
        reason: `${tierEmoji} ${intentionTier}止损：${pnl.pnl_percent.toFixed(1)}% < ${stopLossThreshold * 100}%`
      };
    }

    // ⏰ 时间止损已移除 (v8.0) - 由 AI 判断卖出时机

    // 3. 检查流动性崩溃 (需要持仓5分钟以上)
    const MIN_HOLD_FOR_EMERGENCY = 5;
    const canTriggerEmergency = holdingMinutes >= MIN_HOLD_FOR_EMERGENCY;

    if (signals.liquidity_ratio < this.strategy.EMERGENCY.LIQUIDITY_DROP && canTriggerEmergency) {
      return {
        action: 'EMERGENCY_EXIT',
        sell_percent: 100,
        reason: `流动性崩溃：${(signals.liquidity_ratio * 100).toFixed(0)}%`
      };
    }

    // 4. 检查 Dev 出逃 (需要持仓5分钟以上)
    if (signals.dev_dumping && canTriggerEmergency) {
      return {
        action: 'EMERGENCY_EXIT',
        sell_percent: 100,
        reason: `🚨 Dev 出逃 (持仓${holdingMinutes.toFixed(0)}分钟)`
      };
    }

    // 5. 检查聪明钱出逃（需要持仓5分钟以上）
    if (signals.smart_money_exit && canTriggerEmergency) {
      return {
        action: 'EMERGENCY_EXIT',
        sell_percent: 100,
        reason: `🚨 聪明钱出逃 (持仓${holdingMinutes.toFixed(0)}分钟)`
      };
    }

    // 5.5 🆕 v9.4 滞涨止损 - 优化参数 + High Water Mark保护
    // 修复问题：之前20分钟+5%上限太激进，踢掉了正在起飞的币
    // 今晚案例：AIVA涨过+65%被踢、牛马涨过+31%被踢、LOCAL涨过+12%被踢
    const STAGNANT_MINUTES = 35;      // v9.4: 20→35分钟，给更多起飞时间
    const STAGNANT_LOSS_MIN = -12;    // v9.4: -15→-12，稍微收紧亏损容忍
    const STAGNANT_GAIN_MAX = 20;     // v9.4: 5→20，不踢正在涨的币
    const HWM_PROTECTION_THRESHOLD = 15; // v9.4: 曾涨过15%以上不触发STAGNANT

    if (holdingMinutes >= STAGNANT_MINUTES) {
      // v9.4: High Water Mark 保护 - 曾涨过的币不应该被STAGNANT踢
      const highWaterMark = position.high_water_mark || position.entry_price;
      const entryPrice = position.entry_price || 0;
      const maxGainPercent = entryPrice > 0 ? ((highWaterMark / entryPrice) - 1) * 100 : 0;

      if (maxGainPercent >= HWM_PROTECTION_THRESHOLD) {
        // 曾涨过15%以上，不触发STAGNANT，使用利润回撤保护
        console.log(`   🛡️ [${symbol}] HWM保护: 曾涨+${maxGainPercent.toFixed(1)}%，跳过STAGNANT检查`);
      } else {
        const isStagnant = pnl.pnl_percent >= STAGNANT_LOSS_MIN && pnl.pnl_percent <= STAGNANT_GAIN_MAX;
        if (isStagnant && this.strategy.FOLLOW_SMART_MONEY) {
          const smStatus = await this.getCurrentSmartMoney(tokenCA, chain);
          const entrySM = position.entry_smart_money || position.smart_wallet_online || 0;
          const smDelta = smStatus.count - entrySM;

          // 聪明钱减少才触发滞涨止损（更保守）
          if (smDelta < 0) {
            console.log(`   💤 [${symbol}] 滞涨止损: ${holdingMinutes.toFixed(0)}min + 收益${pnl.pnl_percent.toFixed(1)}% + SM${smDelta}`);
            return {
              action: 'STAGNANT_EXIT',
              sell_percent: 100,
              reason: `滞涨止损：${holdingMinutes.toFixed(0)}min无起色 (${pnl.pnl_percent.toFixed(1)}%, SM${smDelta})`
            };
          }
        }
      }
    }

    // ═══════════════════════════════════════════════════════════════
    // v6.7 叙事等级止盈 (Grand Narrative Take-Profit)
    // 根据叙事立意等级使用不同的止盈阈值
    // ═══════════════════════════════════════════════════════════════
    // 复用止损部分已获取的 intentionTier
    const tpConfig = this.getTakeProfitConfig(intentionTier);

    // 显示当前使用的止盈策略
    if (intentionTier && this.strategy.NARRATIVE_TAKE_PROFIT[intentionTier]) {
      // 只在第一次显示
      if (!position._shownTierLog) {
        const tierEmoji = intentionTier === 'TIER_S' ? '🏆' : intentionTier === 'TIER_A' ? '🥇' : intentionTier === 'TIER_B' ? '🥈' : '⚠️';
        console.log(`   ${tierEmoji} [${symbol}] 使用 ${intentionTier} 止盈策略`);
        position._shownTierLog = true;
      }
    }

    // 6. 🎯 检查 MEGA 止盈 (仅 TIER_S)
    if (tpConfig.MEGA && pnl.pnl_percent >= tpConfig.MEGA.trigger * 100) {
      return {
        action: 'MEGA_PROFIT',
        sell_percent: tpConfig.MEGA.sell,
        reason: `🚀 ${intentionTier || ''} MEGA止盈：+${pnl.pnl_percent.toFixed(1)}% ≥ +${tpConfig.MEGA.trigger * 100}%`
      };
    }

    // 7. 🎯 检查 MOON 止盈
    if (tpConfig.MOON && pnl.pnl_percent >= tpConfig.MOON.trigger * 100) {
      return {
        action: 'MOON_PROFIT',
        sell_percent: tpConfig.MOON.sell,
        reason: `🌙 ${intentionTier || ''} MOON止盈：+${pnl.pnl_percent.toFixed(1)}% ≥ +${tpConfig.MOON.trigger * 100}%`
      };
    }

    // 8. 🎯 检查 TRIPLE 止盈
    if (tpConfig.TRIPLE && pnl.pnl_percent >= tpConfig.TRIPLE.trigger * 100) {
      return {
        action: 'TRIPLE_PROFIT',
        sell_percent: tpConfig.TRIPLE.sell,
        reason: `🎯 ${intentionTier || ''} 三重止盈：+${pnl.pnl_percent.toFixed(1)}% ≥ +${tpConfig.TRIPLE.trigger * 100}%`
      };
    }

    // 9. 🎯 检查翻倍出本触发
    const doubleTrigger = tpConfig.DOUBLE?.trigger || 1.0;
    const doubleSell = tpConfig.DOUBLE?.sell || 50;
    if (pnl.pnl_percent >= doubleTrigger * 100) {
      return {
        action: 'BREAKEVEN',
        sell_percent: doubleSell,
        reason: `🎯 ${intentionTier || ''} 翻倍出本：+${pnl.pnl_percent.toFixed(1)}% ≥ +${doubleTrigger * 100}%`
      };
    }

    // 10. 🎯 中途止盈
    const halfTrigger = tpConfig.HALF?.trigger || 0.50;
    const halfSell = tpConfig.HALF?.sell || 30;
    if (pnl.pnl_percent >= halfTrigger * 100 && !position.half_profit_taken) {
      return {
        action: 'HALF_PROFIT',
        sell_percent: halfSell,
        reason: `💰 ${intentionTier || ''} 中途锁利：+${pnl.pnl_percent.toFixed(1)}% ≥ +${halfTrigger * 100}%，卖${halfSell}%`
      };
    }

    // 11. 🎯 AI 目标价止盈
    if (this.strategy.AI_TARGET_PRICE?.ENABLED && position.exit_price) {
      const aiExitPrice = parseFloat(position.exit_price);
      const currentPrice = parseFloat(pnl.current_price || 0);
      if (aiExitPrice > 0 && currentPrice >= aiExitPrice) {
        const sellRatio = (this.strategy.AI_TARGET_PRICE.SELL_RATIO || 0.80) * 100;
        return {
          action: 'AI_TARGET_REACHED',
          sell_percent: sellRatio,
          reason: `🎯 达到AI预测价 $${aiExitPrice.toFixed(8)} (当前 $${currentPrice.toFixed(8)})`
        };
      }
    }

    return {
      action: 'HOLD',
      reason: `等待止盈 (${intentionTier || '默认'}: 当前 ${pnl.pnl_percent >= 0 ? '+' : ''}${pnl.pnl_percent.toFixed(1)}%, 目标 +${doubleTrigger * 100}%, 持仓 ${holdingMinutes.toFixed(0)}min)`
    };
  }

  /**
   * 利润仓阶段的 AI 动态决策（Free Moonbag 阶段）
   */
  async evaluateProfitPosition(position, snapshot, pnl, signals) {
    const reasons = [];
    let sellSignals = 0;

    // ========================================
    // 1. 紧急逃生条件（立即全卖，不问价格）
    // ========================================

    // 流动性崩溃
    if (signals.liquidity_ratio < this.strategy.EMERGENCY.LIQUIDITY_DROP) {
      return {
        action: 'EMERGENCY_EXIT',
        sell_percent: 100,
        reason: `🚨 流动性崩溃：${(signals.liquidity_ratio * 100).toFixed(0)}%`
      };
    }

    // Dev 出逃
    if (signals.dev_dumping) {
      return {
        action: 'EMERGENCY_EXIT',
        sell_percent: 100,
        reason: `🚨 Dev 出逃：持仓下降 ${(Math.abs(signals.dev_balance_change) * 100).toFixed(0)}%`
      };
    }

    // 聪明钱出逃
    if (signals.smart_money_exit) {
      return {
        action: 'EMERGENCY_EXIT',
        sell_percent: 100,
        reason: `🚨 聪明钱出逃`
      };
    }

    // Rug 迹象
    if (signals.rug_detected) {
      return {
        action: 'EMERGENCY_EXIT',
        sell_percent: 100,
        reason: `🚨 Rug 迹象`
      };
    }

    // ========================================
    // 1.5 v6.8 追踪止损 (Trailing Stop)
    // 利润达标后，从最高点回撤一定比例触发卖出
    // ========================================
    const ts = this.strategy.TRAILING_STOP;
    if (ts?.ENABLED) {
      const highWaterMark = position.high_water_mark || position.entry_price;
      const entryPrice = position.entry_price || 0;
      const currentPrice = snapshot.current_price || pnl.current_price || 0;

      // 计算最高盈利和当前盈利
      const maxProfit = entryPrice > 0 ? (highWaterMark - entryPrice) / entryPrice : 0;
      const currentProfit = pnl.pnl_percent / 100;

      // 只有达到一定利润后才启用追踪止损
      if (maxProfit >= ts.TRIGGER_AFTER_PROFIT) {
        // 计算从最高点的回撤
        const drawdownFromHigh = highWaterMark > 0
          ? (highWaterMark - currentPrice) / highWaterMark
          : 0;

        // 检查是否触发追踪止损
        if (drawdownFromHigh >= ts.CALLBACK_PERCENT) {
          // 确保至少保住最低利润
          if (currentProfit >= ts.MIN_PROFIT_LOCK) {
            const symbol = position.symbol || position.token_ca?.slice(0, 8);
            console.log(`   🎯 [${symbol}] 追踪止损触发！最高+${(maxProfit * 100).toFixed(0)}% → 回撤${(drawdownFromHigh * 100).toFixed(0)}%`);
            return {
              action: 'TRAILING_STOP',
              sell_percent: 100,  // 清掉剩余仓位
              reason: `🎯 追踪止损：最高+${(maxProfit * 100).toFixed(0)}%，回撤${(drawdownFromHigh * 100).toFixed(0)}%，锁定+${(currentProfit * 100).toFixed(0)}%`
            };
          }
        }
      }
    }

    // ========================================
    // 2. 逐步卖出条件（每触发一个卖 1/3）
    // ========================================

    // 热度下降
    if (signals.heat_ratio < this.strategy.HEAT_DECAY_THRESHOLD) {
      sellSignals++;
      reasons.push(`热度↓${(signals.heat_ratio * 100).toFixed(0)}%`);
    }

    // 聪明钱减持
    if (signals.smart_money_selling) {
      sellSignals++;
      reasons.push(`聪明钱减持`);
    }

    // 横盘太久
    if (signals.sideways_minutes > this.strategy.SIDEWAYS_TIMEOUT_MINUTES) {
      sellSignals++;
      reasons.push(`横盘${signals.sideways_minutes.toFixed(0)}分钟`);
    }

    // 从最高点回撤过多
    if (signals.drawdown_from_high > this.strategy.MAX_DRAWDOWN_FROM_HIGH) {
      sellSignals++;
      reasons.push(`回撤${(signals.drawdown_from_high * 100).toFixed(0)}%`);
    }

    // 根据信号数量决定卖出比例
    if (sellSignals >= 3) {
      return {
        action: 'PROFIT_TAKE',
        sell_percent: 100, // 全卖
        reason: `多重信号 (${sellSignals}): ${reasons.join(', ')}`
      };
    } else if (sellSignals >= 2) {
      return {
        action: 'PROFIT_TAKE',
        sell_percent: 50, // 卖一半
        reason: `警告信号 (${sellSignals}): ${reasons.join(', ')}`
      };
    } else if (sellSignals >= 1) {
      return {
        action: 'PROFIT_TAKE',
        sell_percent: 33, // 卖 1/3
        reason: `信号: ${reasons.join(', ')}`
      };
    }

    // ========================================
    // 3. 继续持有条件（死拿等百倍）
    // ========================================
    const holdReasons = [];
    if (signals.heat_rising) holdReasons.push('热度↑');
    if (signals.smart_money_buying) holdReasons.push('聪明钱加仓');
    if (signals.new_catalyst) holdReasons.push('新催化剂');
    if (signals.liquidity_healthy) holdReasons.push('流动性健康');

    return {
      action: 'HOLD',
      reason: holdReasons.length > 0 ? `🚀 死拿: ${holdReasons.join(', ')}` : '无卖出信号，继续持有'
    };
  }

  /**
   * 获取市场信号（MVP 3.0 增强版）
   */
  async getMarketSignals(position, snapshot) {
    const signals = {
      // 流动性
      liquidity_ratio: 1.0,
      liquidity_healthy: true,

      // 热度
      heat_ratio: 1.0,
      heat_rising: false,

      // 聪明钱
      smart_money_selling: false,
      smart_money_buying: false,
      smart_money_exit: false,

      // Dev 监控（新增）
      dev_dumping: false,
      dev_balance_change: 0,

      // 价格
      drawdown_from_high: 0,
      sideways_minutes: 0,

      // 风险
      rug_detected: false,
      new_catalyst: false,
    };

    try {
      // 流动性比较
      const entryLiquidity = position.entry_liquidity_usd || snapshot.liquidity_usd;
      if (entryLiquidity > 0) {
        signals.liquidity_ratio = (snapshot.liquidity_usd || 0) / entryLiquidity;
        signals.liquidity_healthy = signals.liquidity_ratio >= 0.7;
      }

      // 最高价回撤
      const highPrice = position.high_water_mark || position.entry_price;
      if (highPrice > 0) {
        signals.drawdown_from_high = (highPrice - snapshot.current_price) / highPrice;
      }

      // 热度比较（从 TG 信号表）
      const currentHeat = await this.getCurrentHeat(position.token_ca);
      const entryHeat = position.entry_tg_accel || 1;
      if (entryHeat > 0) {
        signals.heat_ratio = currentHeat / entryHeat;
        signals.heat_rising = signals.heat_ratio > 1.2;
      }

      // 横盘检测
      signals.sideways_minutes = this.calculateSidewaysTime(position);

      // 聪明钱动向（基于 Top10 变化）
      const entryTop10 = position.entry_top10_holders || 0;
      const currentTop10 = snapshot.top10_percent || 0;
      const top10Change = currentTop10 - entryTop10;

      if (top10Change > 10) {
        signals.smart_money_buying = true;
      } else if (top10Change < -15) {
        signals.smart_money_selling = true;
      }

      // 聪明钱出逃判定（Top10 快速下降超过 50%）- v7.0 放宽阈值
      if (top10Change < -50) {
        signals.smart_money_exit = true;
      }

      // Dev 监控（简化版 - 基于 Top1 持仓变化）v7.0 放宽阈值
      // 如果 Top1 持仓大幅下降（假设 Top1 是 Dev）
      const entryTop1 = position.entry_top1_holder || 0;
      const currentTop1 = snapshot.top1_percent || 0;
      if (entryTop1 > 0 && currentTop1 < entryTop1 * 0.7) {
        // Top1 持仓下降超过 30% (原来是 10%)
        signals.dev_dumping = true;
        signals.dev_balance_change = (currentTop1 - entryTop1) / entryTop1;
      }

    } catch (error) {
      console.error('❌ Get market signals error:', error.message);
    }

    return signals;
  }

  /**
   * 获取当前热度
   */
  async getCurrentHeat(tokenCA) {
    try {
      const result = this.db.prepare(`
        SELECT COUNT(*) as count
        FROM telegram_signals
        WHERE token_ca = ?
        AND created_at > strftime('%s', 'now', '-15 minutes')
      `).get(tokenCA);
      return result?.count || 0;
    } catch (error) {
      return 0;
    }
  }

  /**
   * 计算横盘时间
   */
  calculateSidewaysTime(position) {
    if (!position.last_significant_move) {
      return 0;
    }
    const lastMove = new Date(position.last_significant_move);
    const now = new Date();
    return (now - lastMove) / 1000 / 60; // 分钟
  }

  /**
   * 🔥 v4.0 获取当前聪明钱数量（统一逻辑的核心）
   * v7.5: 旧 DeBot API 已移除，返回 fallback 值（不影响退出逻辑）
   */
  async getCurrentSmartMoney(tokenCA, chain) {
    // v7.5: debotScout 已移除，聪明钱数据现在通过 Playwright 版本在入场时获取
    // 退出监控不再实时查询聪明钱，使用 position 记录的原始数据
    return { count: -1, aiScore: 0, note: 'v7.5: 使用入场时记录的数据' };
  }

  /**
   * 更新最高价记录
   */
  async updateHighWaterMark(position, currentPrice) {
    try {
      const highWaterMark = position.high_water_mark || position.entry_price || 0;
      if (currentPrice > highWaterMark) {
        this.db.prepare(`
          UPDATE positions
          SET high_water_mark = ?,
              last_significant_move = datetime('now')
          WHERE id = ?
        `).run(currentPrice, position.id);
      }
    } catch (error) {
      // 忽略
    }
  }

  /**
   * 执行决策
   */
  async executeDecision(position, decision, snapshot, pnl, isShadow) {
    const symbol = position.symbol || position.token_ca.substring(0, 8);
    const { action, sell_percent, reason } = decision;

    console.log(`\n🎯 [${symbol}] ${action}`);
    console.log(`   原因: ${reason}`);
    console.log(`   卖出: ${sell_percent}%`);
    console.log(`   PnL: ${pnl.pnl_percent >= 0 ? '+' : ''}${pnl.pnl_percent.toFixed(2)}%`);

    if (isShadow || this.config.SHADOW_MODE) {
      // Shadow 模式：只记录，不执行
      await this.recordShadowTrade(position, decision, snapshot, pnl);
    } else {
      // 实盘模式：执行卖出
      await this.executeRealTrade(position, decision, snapshot, pnl);
    }
  }

  /**
   * 记录 Shadow 交易
   */
  async recordShadowTrade(position, decision, snapshot, pnl) {
    const { action, sell_percent } = decision;
    const sellAmount = (position.remaining_percent || 100) * sell_percent / 100;
    const newRemaining = (position.remaining_percent || 100) - sellAmount;

    try {
      if (action === 'BREAKEVEN') {
        // 保本操作
        this.db.prepare(`
          UPDATE positions
          SET breakeven_done = 1,
              breakeven_time = datetime('now'),
              breakeven_price = ?,
              breakeven_sell_percent = ?,
              remaining_percent = ?,
              status = 'partial',
              updated_at = strftime('%s', 'now')
          WHERE id = ?
        `).run(
          snapshot.current_price,
          sell_percent,
          newRemaining,
          position.id
        );
        console.log(`   ✅ [Shadow] 保本完成，剩余 ${newRemaining.toFixed(0)}% 利润仓`);

      } else if (sell_percent >= 100 || newRemaining <= 0) {
        // 全部卖出 (v9.4: 添加 pnl_native 和 pnl_usd 记录)
        this.db.prepare(`
          UPDATE positions
          SET status = 'closed',
              exit_time = datetime('now'),
              exit_price = ?,
              exit_type = ?,
              pnl_percent = ?,
              pnl_native = ?,
              pnl_usd = ?,
              remaining_percent = 0,
              updated_at = strftime('%s', 'now')
          WHERE id = ?
        `).run(
          snapshot.current_price,
          action,
          pnl.pnl_percent,
          pnl.pnl_native,
          pnl.pnl_usd,
          position.id
        );
        console.log(`   ✅ [Shadow] 仓位已平，PnL: ${pnl.pnl_percent >= 0 ? '+' : ''}${pnl.pnl_percent.toFixed(2)}% ($${pnl.pnl_usd.toFixed(2)})`);

        // 🔥 将 token 加入冷却列表，防止立即重新买入
        this.exitCooldown.addToCooldown(
          position.token_ca,
          position.chain,
          action,
          pnl.pnl_percent,
          position.id
        );

        // ═══════════════════════════════════════════════════════════════
        // v6.7 死狗学习池 - 记录失败交易
        // 只有亏损交易才记录，用于学习失败模式
        // ═══════════════════════════════════════════════════════════════
        const isLoss = pnl.pnl_percent < -10;  // 亏损超过10%才记录
        const isFailureExit = ['STOP_LOSS', 'TIME_STOP', 'EMERGENCY_EXIT', 'STAGNANT_EXIT'].includes(action);
        if (isLoss && isFailureExit) {
          try {
            await deadDogPool.recordDeath({
              symbol: position.symbol,
              name: position.symbol,
              tokenCA: position.token_ca,
              chain: position.chain,
              marketCap: snapshot.market_cap || 0,
              creator: position.creator || null,
              narrative: position.narrative || null
            }, action, pnl.pnl_percent);
          } catch (e) {
            console.error('   ⚠️ [DeadDogPool] 记录失败:', e.message);
          }
        }

      } else {
        // 部分卖出
        this.db.prepare(`
          UPDATE positions
          SET remaining_percent = ?,
              status = 'partial',
              last_partial_sell_time = datetime('now'),
              last_partial_sell_price = ?,
              updated_at = strftime('%s', 'now')
          WHERE id = ?
        `).run(
          newRemaining,
          snapshot.current_price,
          position.id
        );
        console.log(`   ✅ [Shadow] 部分卖出 ${sellAmount.toFixed(0)}%，剩余 ${newRemaining.toFixed(0)}% (状态已更新为 partial)`);
      }

    } catch (error) {
      console.error('❌ Record shadow trade error:', error.message);
    }
  }

  /**
   * 执行实盘交易
   */
  async executeRealTrade(position, decision, snapshot, pnl) {
    const { action, sell_percent } = decision;

    try {
      // v7.5 傳回整個 position 對象，讓執行器能直接獲取 token_ca
      const sellResult = await this.executor.executeSell(
        position,
        `[Monitor] ${action}: ${reason} (PnL: ${pnl.pnl_percent.toFixed(1)}%)`
      );

      if (sellResult.success) {
        // 更新数据库（与 shadow 类似）
        await this.recordShadowTrade(position, decision, snapshot, pnl);
        console.log(`   ✅ [Live] 交易执行成功，TX: ${sellResult.tx_hash || 'pending'}`);
      } else {
        console.error(`   ❌ [Live] 交易执行失败: ${sellResult.error}`);
      }

    } catch (error) {
      console.error('❌ Execute real trade error:', error.message);
    }
  }

  /**
   * 获取链上快照
   */
  async getChainSnapshot(chain, tokenCA) {
    try {
      const service = chain === 'SOL' ? this.solService : this.bscService;
      return await service.getSnapshot(tokenCA);
    } catch (error) {
      return null;
    }
  }

  /**
   * 计算盈亏
   * v9.4: 添加 pnl_native 和 pnl_usd 计算，修复数据记录问题
   */
  calculatePnL(position, currentPrice) {
    const entryPrice = position.entry_price || 0;
    const positionSize = position.position_size_native || 0;
    const chain = position.chain;

    if (entryPrice === 0 || currentPrice === 0) {
      return { pnl_percent: 0, pnl_native: 0, pnl_usd: 0 };
    }

    const pnlPercent = ((currentPrice - entryPrice) / entryPrice) * 100;
    const remainingPercent = position.remaining_percent || 100;
    const effectivePnl = pnlPercent * remainingPercent / 100;

    // v9.4: 计算原生代币盈亏 (SOL/BNB)
    const pnlNative = positionSize * (pnlPercent / 100);

    // v9.4: 估算 USD 盈亏 (SOL~$200, BNB~$700)
    const nativeToUSD = chain === 'SOL' ? 200 : 700;
    const pnlUSD = pnlNative * nativeToUSD;

    return {
      current_price: currentPrice,
      entry_price: entryPrice,
      pnl_percent: pnlPercent,
      pnl_native: pnlNative,
      pnl_usd: pnlUSD,
      effective_pnl: effectivePnl,
      remaining_percent: remainingPercent
    };
  }

  /**
   * 获取状态
   */
  getStatus() {
    try {
      const stats = this.db.prepare(`
        SELECT
          COUNT(*) as total,
          SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) as open,
          SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) as breakeven,
          SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) as closed
        FROM positions
      `).get();

      return {
        is_running: this.isRunning,
        strategy: 'v3 - 翻倍出本 + AI动态管理 (MVP 3.0)',
        positions: stats
      };
    } catch (error) {
      return { is_running: this.isRunning, error: error.message };
    }
  }
}

export default PositionMonitorV2;
