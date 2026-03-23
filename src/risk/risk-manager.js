/**
 * 风险管理系统
 *
 * 核心职责：
 * 1. 入场标准控制（Score ≥ 70）
 * 2. 时间衰减因子
 * 3. 危险信号检测
 * 4. 资金管理（2% 上限，最多 3 仓，连亏暂停）
 * 5. 负反馈机制
 * 6. 🛑 v6.7 物理熔断 - 连亏3笔强制休息4小时
 */

import notifier from '../utils/notifier.js';
import { RISK } from '../config/strategy.js';

export class RiskManager {
  constructor(config, db) {
    this.config = config;
    this.db = db;

    // 风险参数 - 从 strategy.js 统一配置读取
    this.params = RISK;
    this.shadowMode = this.config?.SHADOW_MODE === true || process.env.SHADOW_MODE === 'true';

    // 状态追踪
    this.state = {
      consecutiveLosses: 0,
      pausedUntil: null,
      todayTrades: 0,
      todayLosses: 0,
    };

    this.initializeState();
    console.log('🛡️  Risk Manager initialized');
    console.log(`   最低入场分数: ${this.params.MIN_SCORE_TO_TRADE}`);
    console.log(`   单笔上限: ${this.params.MAX_POSITION_PERCENT * 100}%`);
    console.log(`   最大持仓: ${this.params.MAX_CONCURRENT_POSITIONS}`);
    console.log(`   连亏暂停: ${this.params.CIRCUIT_BREAKER.CONSECUTIVE_LOSS_PAUSE} 笔`);
  }

  /**
   * 初始化状态（从数据库恢复）
   * v9.4: 支持交易周期重置，只统计重置时间点之后的记录
   */
  initializeState() {
    try {
      // v9.4: 获取交易周期重置时间戳
      let resetTs = 0;
      try {
        const resetState = this.db.prepare(`
          SELECT value FROM system_state WHERE key = 'trading_cycle_reset_ts'
        `).get();
        if (resetState && resetState.value) {
          resetTs = parseInt(resetState.value, 10);
          console.log(`   🔄 交易周期重置时间: ${new Date(resetTs * 1000).toLocaleString()}`);
        }
      } catch (e) { /* ignore */ }

      // 加载手动"今日亏损重置"时间戳（用于"重新开始"）
      try {
        const dlReset = this.db.prepare(`
          SELECT value FROM system_state WHERE key = 'daily_loss_reset_ts'
        `).get();
        if (dlReset?.value) {
          this._dailyLossResetTs = parseInt(dlReset.value, 10);
          console.log(`   🔄 今日亏损统计起点: ${new Date(this._dailyLossResetTs).toLocaleString()}`);
        }
      } catch (e) { /* ignore */ }

      // 获取连续亏损次数（只统计重置时间点之后的记录）
      const recentTrades = this.db.prepare(`
        SELECT exit_pnl
        FROM live_positions
        WHERE status = 'closed'
        AND closed_at > ? * 1000
        ORDER BY closed_at DESC
        LIMIT 10
      `).all(resetTs);

      let consecutiveLosses = 0;
      for (const trade of recentTrades) {
        if (trade.exit_pnl < 0) {
          consecutiveLosses++;
        } else {
          break;
        }
      }

      // 检查是否在暂停期
      const pauseState = this.db.prepare(`
        SELECT value, expires_at FROM system_state WHERE key = 'trading_paused'
      `).get();

      if (pauseState?.expires_at && pauseState.expires_at > Date.now() / 1000) {
        this.state.pausedUntil = new Date(pauseState.expires_at * 1000);
        this.state.consecutiveLosses = consecutiveLosses;
      } else {
        this.state.pausedUntil = null;
        this.state.consecutiveLosses = 0;
        if (pauseState) {
          try {
            this.db.prepare(`DELETE FROM system_state WHERE key = 'trading_paused'`).run();
          } catch (e) { /* ignore */ }
        }
      }

      console.log(`   当前连续亏损: ${this.state.consecutiveLosses}`);
      if (this.state.pausedUntil) {
        console.log(`   ⚠️ 交易暂停至: ${this.state.pausedUntil.toLocaleString()}`);
      }

      // v9.4: 从数据库恢复最近的熔断时间（用于恢复期仓位减半）
      try {
        const breakerRows = this.db.prepare(`
          SELECT key FROM system_state
          WHERE key LIKE 'circuit_breaker_%'
        `).all();

        const breakerTimes = breakerRows
          .map(row => Number(row.key.replace('circuit_breaker_', '')))
          .filter(ts => Number.isFinite(ts));

        if (breakerTimes.length > 0) {
          const ts = Math.max(...breakerTimes);
          const recoveryHours = this.params.CIRCUIT_BREAKER.RECOVERY_PERIOD_HOURS || 4;
          const recoveryPeriodMs = recoveryHours * 60 * 60 * 1000;

          if (Date.now() - ts < recoveryPeriodMs) {
            this._lastCircuitBreakerTime = ts;
            const remaining = Math.ceil((recoveryPeriodMs - (Date.now() - ts)) / 60000);
            console.log(`   🔥 熔断恢复期: 还剩 ${remaining} 分钟，仓位减半`);
          }
        }
      } catch (e) { /* ignore */ }

      // 恢复熔断激活标志（_circuitBreakerTriggered）
      try {
        const cbState = this.db.prepare(`
          SELECT value FROM system_state WHERE key = 'circuit_breaker_active'
        `).get();
        if (cbState?.value) {
          const cbTime = parseInt(cbState.value, 10);
          const recoveryMs = (this.params.CIRCUIT_BREAKER?.RECOVERY_PERIOD_HOURS || 4) * 3600000;
          if (Number.isFinite(cbTime) && Date.now() - cbTime < recoveryMs) {
            this._lastCircuitBreakerTime = this._lastCircuitBreakerTime || cbTime;
            this._circuitBreakerTriggered = !!this.state.pausedUntil;
            this._circuitBreakerLogged = true;
            if (this._circuitBreakerTriggered) {
              console.log(`   🚨 熔断状态已从DB恢复，仍在恢复期内`);
            }
          } else {
            this._circuitBreakerTriggered = false;
            try {
              this.db.prepare(`DELETE FROM system_state WHERE key = 'circuit_breaker_active'`).run();
            } catch (e) { /* ignore */ }
          }
        }
      } catch (e) { /* ignore */ }

    } catch (error) {
      // 忽略初始化错误
    }
  }

  /**
   * 检查是否可以交易
   * @returns {{ allowed: boolean, reason: string }}
   */
  canTrade() {
    // 1. 检查是否在暂停期
    if (this.state.pausedUntil && new Date() < this.state.pausedUntil) {
      const remaining = Math.ceil((this.state.pausedUntil - new Date()) / 1000 / 60);
      return {
        allowed: false,
        reason: `交易暂停中，还剩 ${remaining} 分钟`
      };
    }

    // 1.5 检查每日净亏损限制 (收回SOL - 投入SOL)
    const dailyLossLimit = this.params.DAILY_LOSS_LIMIT?.SOL || 0.3;
    try {
      const todayStart = new Date();
      todayStart.setHours(0, 0, 0, 0);
      const todayRows = this.db.prepare(`
        SELECT COALESCE(SUM(entry_sol), 0) as total_spent,
               COALESCE(SUM(CASE WHEN total_sol_received > 0 THEN total_sol_received ELSE 0 END), 0) as total_received
        FROM live_positions
        WHERE status = 'closed' AND closed_at >= ?
      `).get(this._getDailyStartTs()); // 支持手动重置：max(今日零点, 手动重置时间戳)
      if (todayRows) {
        const netPnl = todayRows.total_received - todayRows.total_spent;
        if (netPnl <= -dailyLossLimit) {
          return {
            allowed: false,
            reason: `今日净亏损 ${netPnl.toFixed(4)} SOL，达到每日上限 -${dailyLossLimit} SOL`
          };
        }
      }
    } catch (e) { /* live_positions表可能不存在 */ }

    if (this.state.pausedUntil && new Date() >= this.state.pausedUntil) {
      this.resumeTrading();
    }

    // 3. 检查当前持仓数 (v6.3 升级: 支持金狗额外槽和 Swap)
    const openPositions = this.getOpenPositionsCount();
    const baseLimit = this.params.MAX_CONCURRENT_POSITIONS;
    const totalLimit = this.params.MAX_TOTAL_POSITIONS;

    if (openPositions >= totalLimit) {
      // 完全满仓，即使是金狗也进不来
      return {
        allowed: false,
        reason: `已有 ${openPositions} 个持仓，达到绝对上限 ${totalLimit}`
      };
    }

    if (openPositions >= baseLimit) {
      // 超过基础持仓，需要检查是否可以使用金狗预留槽
      return {
        allowed: false,
        reason: `已有 ${openPositions} 个持仓，达到基础上限 ${baseLimit}`,
        needGoldenSlot: true,  // 标记需要金狗权限才能进入
        maxPositions: totalLimit
      };
    }

    // 4. 🛡️ v9.4 胜率过低时启用防御模式
    // 修复: 之前只允许TIER_S，但数据显示TIER_S表现最差
    // 新策略: 禁止TIER_S(大热避险)，只允许TIER_A/B
    const stats = this.getRecentStats();
    if (stats.totalTrades >= this.params.CIRCUIT_BREAKER.MIN_TRADES_FOR_STATS) {
      if (stats.winRate < this.params.CIRCUIT_BREAKER.WIN_RATE_THRESHOLD) {
        // 🛑 不再触发 AI 复盘（AI 会建议降低阈值，这是错误的方向！）
        // 改为：启用防御模式，提高门槛而不是降低
        if (!this._defensiveModeActive) {
          this._defensiveModeActive = true;
          console.log(`\n🛡️ [RISK] 胜率过低 (${(stats.winRate * 100).toFixed(1)}%)，已启用防御模式`);
          console.log(`   📈 最低分数: ${this.params.MIN_SCORE_TO_TRADE} → ${this.params.MIN_SCORE_TO_TRADE + this.params.DEFENSIVE_MODE.MIN_SCORE_BOOST}`);
          console.log(`   💰 仓位倍数: ${this.params.DEFENSIVE_MODE.POSITION_MULTIPLIER}x`);
          console.log(`   🚫 禁止叙事: ${this.params.DEFENSIVE_MODE.FORBIDDEN_TIERS?.join(', ') || 'TIER_S'} (大热避险)`);

          // 发送通知
          notifier.systemStatus('防御模式启用', `
胜率过低 (${(stats.winRate * 100).toFixed(1)}%)，系统已自动启用防御模式：
- 最低分数提高 ${this.params.DEFENSIVE_MODE.MIN_SCORE_BOOST} 分
- 仓位减半
- 禁止 ${this.params.DEFENSIVE_MODE.FORBIDDEN_TIERS?.join('/') || 'TIER_S'} (大热避险)
- 只允许 ${this.params.DEFENSIVE_MODE.ALLOWED_TIERS?.join('/') || 'TIER_A/B'}

*逆风时避开大热点，选择稳健标的*
          `).catch(() => { });
        }
        // 继续交易，但使用更严格的标准（在 evaluateSignal 中检查）
      } else if (stats.winRate >= 0.4 && this._defensiveModeActive) {
        // 胜率恢复到 40%，退出防御模式
        this._defensiveModeActive = false;
        console.log(`\n✅ [RISK] 胜率恢复 (${(stats.winRate * 100).toFixed(1)}%)，已退出防御模式`);
        notifier.systemStatus('防御模式解除', `胜率恢复到 ${(stats.winRate * 100).toFixed(1)}%`).catch(() => { });
      }
    }

    // 5. 检查每日亏损上限
    const dailyPnL = this.getTodayPnL();
    if (dailyPnL.sol <= -(this.params.DAILY_LOSS_LIMIT?.SOL || 0.5)) {
      return {
        allowed: false,
        reason: `今日 SOL 亏损 ${Math.abs(dailyPnL.sol).toFixed(2)} 已达上限 ${this.params.DAILY_LOSS_LIMIT?.SOL || 0.5}`
      };
    }
    if (dailyPnL.bnb <= -(this.params.DAILY_LOSS_LIMIT?.BNB || 0.1)) {
      return {
        allowed: false,
        reason: `今日 BNB 亏损 ${Math.abs(dailyPnL.bnb).toFixed(2)} 已达上限 ${this.params.DAILY_LOSS_LIMIT?.BNB || 0.1}`
      };
    }

    return { allowed: true, reason: 'OK' };
  }

  /**
   * 获取今日统计起点（ms）= max(今日零点, 手动重置时间戳)
   * 用于所有"今日亏损"查询，支持手动"重新开始"
   */
  _getDailyStartTs() {
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    return Math.max(todayStart.getTime(), this._dailyLossResetTs || 0);
  }

  /**
   * 重置今日亏损统计（用于"重新开始"场景）
   * 将统计起点设为当前时间，之前的亏损不再计入今日上限
   */
  resetDailyLoss() {
    this._dailyLossResetTs = Date.now();
    try {
      this.db.prepare(`
        INSERT OR REPLACE INTO system_state (key, value)
        VALUES ('daily_loss_reset_ts', ?)
      `).run(this._dailyLossResetTs.toString());
    } catch (e) { /* ignore */ }
    console.log(`\n✅ [手动] 今日亏损统计已重置 — 从 ${new Date(this._dailyLossResetTs).toLocaleString()} 起重新计算\n`);
  }

  /**
   * 获取今日盈亏统计
   */
  getTodayPnL() {
    try {
      const result = this.db.prepare(`
        SELECT COALESCE(SUM(total_sol_received - entry_sol), 0) as total_pnl
        FROM live_positions
        WHERE status = 'closed'
        AND closed_at >= ?
        AND total_sol_received >= 0
      `).get(this._getDailyStartTs());

      return { sol: result.total_pnl || 0, bnb: 0 };
    } catch (error) {
      return { sol: 0, bnb: 0 };
    }
  }

  /**
   * 评估信号是否值得交易
   * @param {object} signal - 信号对象
   * @param {number} score - AI 评分
   * @param {object} snapshot - 链上快照
   * @param {object} options - 额外选项 { narrativeTier, smartMoneyCount }
   * @returns {{ allowed: boolean, adjustedScore: number, reason: string, defensiveMode: boolean }}
   */
  evaluateSignal(signal, score, snapshot, options = {}) {
    let adjustedScore = score;
    const warnings = [];
    const { narrativeTier, smartMoneyCount } = options;

    // 🛡️ v6.7 防御模式检查
    const isDefensive = this._defensiveModeActive && this.params.DEFENSIVE_MODE.ENABLED;
    const minScore = isDefensive
      ? this.params.MIN_SCORE_TO_TRADE + this.params.DEFENSIVE_MODE.MIN_SCORE_BOOST
      : this.params.MIN_SCORE_TO_TRADE;

    // 1. 基础分数检查（防御模式时门槛更高）
    if (score < minScore) {
      return {
        allowed: false,
        adjustedScore: score,
        reason: isDefensive
          ? `🛡️ 防御模式: 分数 ${score} < ${minScore}（需要 ${this.params.MIN_SCORE_TO_TRADE}+${this.params.DEFENSIVE_MODE.MIN_SCORE_BOOST}）`
          : `分数 ${score} < ${minScore}（最低标准）`,
        defensiveMode: isDefensive
      };
    }

    // ═══════════════════════════════════════════════════════════════
    // 🚫 v9.5 全局禁用叙事等级检查 (独立于防御模式，始终生效)
    // 数据支撑: TIER_B 52笔交易，19.2%胜率，-$83.99总亏损
    // ═══════════════════════════════════════════════════════════════
    if (narrativeTier && this.params.HARD_GATES?.FORBIDDEN_TIERS) {
      const globalForbidden = this.params.HARD_GATES.FORBIDDEN_TIERS;
      if (globalForbidden.includes(narrativeTier)) {
        return {
          allowed: false,
          adjustedScore: score,
          reason: `🚫 全局禁用: ${narrativeTier} 在禁止列表中 (历史胜率过低)`,
          defensiveMode: isDefensive
        };
      }
    }

    // ═══════════════════════════════════════════════════════════════
    // 🛡️ v9.4 防御模式：禁止TIER_S (大热避险)，只允许TIER_A/B
    // 修复: 之前"只允许TIER_S"，但TIER_S表现最差(-28.5%)
    // 新策略: 逆风局避开大热点，选择稳健标的
    // ═══════════════════════════════════════════════════════════════
    if (isDefensive) {
      const forbiddenTiers = this.params.DEFENSIVE_MODE.FORBIDDEN_TIERS || ['TIER_S'];
      const allowedTiers = this.params.DEFENSIVE_MODE.ALLOWED_TIERS || ['TIER_A', 'TIER_B'];

      if (narrativeTier) {
        // 检查是否在禁止列表中
        if (forbiddenTiers.includes(narrativeTier)) {
          return {
            allowed: false,
            adjustedScore: score,
            reason: `🛡️ 防御模式: 禁止 ${narrativeTier} (大热避险)，只允许 ${allowedTiers.join('/')}`,
            defensiveMode: true
          };
        }
        // 检查是否在允许列表中 (如果有明确限制)
        if (allowedTiers.length > 0 && !allowedTiers.includes(narrativeTier)) {
          // TIER_C（未评级）也受限 — 防御模式下只允许明确评级的优质标的
          return {
            allowed: false,
            adjustedScore: score,
            reason: `🛡️ 防御模式: ${narrativeTier} 不在允许列表 ${allowedTiers.join('/')}`,
            defensiveMode: true
          };
        }
      }
    }

    // 🛡️ v6.7 防御模式：聪明钱门槛提高
    if (isDefensive && this.params.DEFENSIVE_MODE.SM_THRESHOLD_BOOST) {
      const smRequired = 1 + this.params.DEFENSIVE_MODE.SM_THRESHOLD_BOOST; // 默认1 + 提升3 = 4
      if (smartMoneyCount !== undefined && smartMoneyCount < smRequired) {
        return {
          allowed: false,
          adjustedScore: score,
          reason: `🛡️ 防御模式: 需要 ${smRequired}+ 聪明钱，当前 ${smartMoneyCount}`,
          defensiveMode: true
        };
      }
    }

    if (isDefensive) {
      warnings.push('🛡️ 防御模式');
    }

    // 2. 时间衰减
    const signalAge = this.getSignalAgeMinutes(signal);
    if (signalAge > this.params.TIME_DECAY.EXPIRED_MINUTES) {
      return {
        allowed: false,
        adjustedScore: 0,
        reason: `信号已过期（${signalAge.toFixed(0)} 分钟前）`
      };
    } else if (signalAge > this.params.TIME_DECAY.STALE_MINUTES) {
      adjustedScore *= this.params.TIME_DECAY.STALE_MULTIPLIER;
      warnings.push(`时间衰减 -20%（${signalAge.toFixed(0)} 分钟）`);
    }

    // 3. 危险信号检测
    const dangerScore = this.calculateDangerScore(snapshot);
    if (dangerScore > this.params.MAX_DANGER_SCORE) {
      return {
        allowed: false,
        adjustedScore: adjustedScore,
        reason: `危险分数 ${dangerScore} > ${this.params.MAX_DANGER_SCORE}`
      };
    }
    if (dangerScore > 0) {
      warnings.push(`危险分数: ${dangerScore}`);
    }

    // 4. 调整后分数再次检查
    if (adjustedScore < this.params.MIN_SCORE_TO_TRADE) {
      return {
        allowed: false,
        adjustedScore: adjustedScore,
        reason: `调整后分数 ${adjustedScore.toFixed(0)} < ${this.params.MIN_SCORE_TO_TRADE}`
      };
    }

    return {
      allowed: true,
      adjustedScore: adjustedScore,
      reason: warnings.length > 0 ? `通过（${warnings.join(', ')}）` : '通过'
    };
  }

  /**
   * 计算信号年龄（分钟）
   */
  getSignalAgeMinutes(signal) {
    const signalTime = new Date(signal.timestamp).getTime();
    return (Date.now() - signalTime) / 1000 / 60;
  }

  /**
   * 计算危险分数
   */
  calculateDangerScore(snapshot) {
    let dangerScore = 0;

    if (!snapshot) return 0;

    // LP 未锁定或即将解锁
    if (snapshot.lp_locked === false || snapshot.lp_unlock_days < 7) {
      dangerScore += this.params.DANGER_SIGNALS.LP_UNLOCK_SOON;
    }

    // 合约未放弃
    if (snapshot.owner_type && !['Renounced', 'Burned'].includes(snapshot.owner_type)) {
      dangerScore += this.params.DANGER_SIGNALS.OWNER_NOT_RENOUNCED;
    }

    // 高税率
    const totalTax = (snapshot.tax_buy || 0) + (snapshot.tax_sell || 0);
    if (totalTax > 10) {
      dangerScore += this.params.DANGER_SIGNALS.HIGH_TAX;
    }

    // 蜜罐检测
    if (snapshot.honeypot === true || snapshot.is_honeypot === true) {
      dangerScore += this.params.DANGER_SIGNALS.HONEYPOT_RISK;
    }

    // 开发者持仓高
    if (snapshot.dev_holdings_percent > 10) {
      dangerScore += this.params.DANGER_SIGNALS.DEV_HOLDING_HIGH;
    }

    // Top10 持仓过高（可能是聪明钱准备出货）
    if (snapshot.top10_percent > 50) {
      dangerScore += this.params.DANGER_SIGNALS.SMART_MONEY_EXITING;
    }

    return dangerScore;
  }

  /**
   * 计算仓位大小
   * @param {string} chain - SOL/BSC
   * @param {number} score - 评分
   * @returns {{ size: number, unit: string, defensiveMode: boolean }}
   */
  calculatePositionSize(chain, score) {
    const totalCapital = chain === 'SOL'
      ? this.config.TOTAL_CAPITAL_SOL
      : this.config.TOTAL_CAPITAL_BNB;

    // 最大单笔 = 总资金 * 2%
    let maxSize = totalCapital * this.params.MAX_POSITION_PERCENT;

    // 根据分数调整
    // 70-80分：50% 仓位
    // 80-90分：75% 仓位
    // 90-100分：100% 仓位
    let sizeMultiplier = 0.5;
    if (score >= 90) {
      sizeMultiplier = 1.0;
    } else if (score >= 80) {
      sizeMultiplier = 0.75;
    }

    // 🛡️ v6.7 防御模式：仓位减半
    const isDefensive = this._defensiveModeActive && this.params.DEFENSIVE_MODE.ENABLED;
    if (isDefensive) {
      sizeMultiplier *= this.params.DEFENSIVE_MODE.POSITION_MULTIPLIER;
    }

    // ═══════════════════════════════════════════════════════════════
    // 🔥 v9.4 熔断恢复期：仓位再减半
    // 熔断触发后4小时内，即使继续交易也要降低仓位
    // 与防御模式叠加：防御0.5 x 恢复期0.5 = 0.25仓位
    // ═══════════════════════════════════════════════════════════════
    const isInRecovery = this._isInCircuitBreakerRecovery();
    if (isInRecovery) {
      const recoveryMultiplier = this.params.CIRCUIT_BREAKER.RECOVERY_POSITION_MULTIPLIER || 0.5;
      sizeMultiplier *= recoveryMultiplier;
      console.log(`   🔥 [RISK] 熔断恢复期：仓位 x${recoveryMultiplier}`);
    }

    const finalSize = maxSize * sizeMultiplier;

    return {
      size: finalSize,
      unit: chain,
      maxSize: maxSize,
      multiplier: sizeMultiplier,
      defensiveMode: isDefensive,
      inRecovery: isInRecovery  // v9.4: 熔断恢复期标记
    };
  }

  /**
   * 记录交易结果
   */
  recordTradeResult(isWin) {
    if (isWin) {
      this.state.consecutiveLosses = 0;
      // 盈利时重置熔断记录标志，允许下次连亏时再次记录
      this._circuitBreakerLogged = false;
      if (!this.state.pausedUntil) {
        this._circuitBreakerTriggered = false;
      }
      return;
    }

    this.state.consecutiveLosses++;

    const threshold = this.params.CIRCUIT_BREAKER?.CONSECUTIVE_LOSS_PAUSE || 8;
    if (this.state.consecutiveLosses >= threshold && !this._circuitBreakerTriggered) {
      this.triggerCircuitBreaker();
    }
  }

  /**
   * 暂停交易
   */
  pauseTrading(lossCount = this.state.consecutiveLosses) {
    const pauseUntil = new Date();
    pauseUntil.setHours(pauseUntil.getHours() + this.params.CIRCUIT_BREAKER.PAUSE_DURATION_HOURS);
    this.state.pausedUntil = pauseUntil;

    try {
      this.db.prepare(`
        INSERT OR REPLACE INTO system_state (key, value, expires_at)
        VALUES ('trading_paused', 'true', ?)
      `).run(Math.floor(pauseUntil.getTime() / 1000));
    } catch (error) {
      // 忽略
    }

    console.log(`\n⚠️  交易已暂停至 ${pauseUntil.toLocaleString()}`);
    console.log(`   原因：连续亏损 ${lossCount} 笔\n`);
  }

  /**
   * 手动暂停交易（指定小时数）
   */
  manualPause(hours = 4) {
    const pauseUntil = new Date();
    pauseUntil.setHours(pauseUntil.getHours() + hours);
    this.state.pausedUntil = pauseUntil;

    try {
      this.db.prepare(`
        INSERT OR REPLACE INTO system_state (key, value, expires_at)
        VALUES ('trading_paused', 'true', ?)
      `).run(Math.floor(pauseUntil.getTime() / 1000));
    } catch (error) {
      // 忽略
    }

    console.log(`\n🛑 [手动] 交易已暂停至 ${pauseUntil.toLocaleString()} (${hours}小时)\n`);
  }

  /**
   * 恢复交易
   */
  resumeTrading() {
    this.state.pausedUntil = null;
    this.state.consecutiveLosses = 0;
    this._circuitBreakerTriggered = false;
    this._circuitBreakerLogged = false;

    try {
      this.db.prepare(`DELETE FROM system_state WHERE key = 'trading_paused'`).run();
      this.db.prepare(`DELETE FROM system_state WHERE key = 'circuit_breaker_active'`).run();
    } catch (error) {
      // 忽略
    }

    console.log(`\n✅ [手动] 交易已恢复，连亏计数已重置\n`);
  }

  /**
   * 🛑 v6.7 物理熔断机制
   * 连续亏损达到阈值时，强制停止交易并发送微信通知
   */
  triggerCircuitBreaker() {
    // 防止重复触发
    if (this._circuitBreakerTriggered) {
      return;
    }
    this._circuitBreakerTriggered = true;

    const lossCount = this.state.consecutiveLosses;
    const triggerTime = Date.now();
    this._lastCircuitBreakerTime = triggerTime;
    this._circuitBreakerLogged = true;
    this._circuitBreakerCount = (this._circuitBreakerCount || 0) + 1;

    // 获取最近亏损的交易
    let recentLosses = [];
    try {
      recentLosses = this.db.prepare(`
        SELECT symbol, exit_pnl, closed_at
        FROM live_positions
        WHERE status = 'closed' AND exit_pnl < 0
        ORDER BY closed_at DESC
        LIMIT ?
      `).all(lossCount);
    } catch (e) {
      // ignore
    }

    // 记录到数据库用于后续分析 / 恢复期恢复
    try {
      this.db.prepare(`
        INSERT INTO system_state (key, value, expires_at)
        VALUES ('circuit_breaker_' || ?, ?, ?)
      `).run(triggerTime, lossCount, Math.floor(triggerTime / 1000));
    } catch (e) { /* ignore */ }

    try {
      this.db.prepare(`
        INSERT OR REPLACE INTO system_state (key, value)
        VALUES ('circuit_breaker_active', ?)
      `).run(triggerTime.toString());
    } catch (e) { /* ignore */ }

    const shouldPause = !this.shadowMode;
    if (shouldPause) {
      this.pauseTrading(lossCount);
    }

    const lossDetails = recentLosses
      .map((t, i) => `${i + 1}. ${t.symbol}: ${t.exit_pnl?.toFixed(1)}%`)
      .join('\n');

    const resumeTime = shouldPause
      ? (this.state.pausedUntil?.toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' }) || '未知')
      : 'Shadow模式不停机';

    const content = shouldPause ? `
## 🛑 物理熔断触发！

**连续亏损**: ${lossCount} 笔
**暂停时长**: ${this.params.CIRCUIT_BREAKER.PAUSE_DURATION_HOURS} 小时
**恢复时间**: ${resumeTime}

### 最近亏损交易:
${lossDetails || '(无记录)'}

---

⚠️ **系统已强制停止交易**

这是好事！连续亏损说明当前市场状态不适合交易。
休息一下，等市场冷静后再战。

*记住：不亏就是赚！*
` : `
连续亏损 ${lossCount} 笔，已触发第 ${this._circuitBreakerCount} 次熔断记录。

📝 **模拟盘模式：继续运行采集数据**
🔬 实盘模式下会暂停 ${this.params.CIRCUIT_BREAKER.PAUSE_DURATION_HOURS} 小时

### 最近亏损交易:
${lossDetails || '(无记录)'}
`;

    const notifierCall = shouldPause
      ? notifier.critical('物理熔断触发', content)
      : notifier.systemStatus('静默熔断触发', content);

    notifierCall.catch(e => {
      console.error('[RISK] 熔断通知发送失败:', e.message);
    });

    console.log(shouldPause ? `
╔══════════════════════════════════════════════════════════════╗
║  🛑  物理熔断触发！连续亏损 ${lossCount} 笔                       ║
║  ⏸️   交易暂停至 ${resumeTime}           ║
║  💡  这是保护机制，不是故障！                                ║
╚══════════════════════════════════════════════════════════════╝
` : `
╔══════════════════════════════════════════════════════════════╗
║  🚨 [静默熔断 #${this._circuitBreakerCount}] 连亏 ${lossCount} 笔！                        ║
║  📝 实盘模式下此刻会暂停 ${this.params.CIRCUIT_BREAKER.PAUSE_DURATION_HOURS} 小时                          ║
║  🧪 模拟盘继续运行，采集极端数据...                          ║
╚══════════════════════════════════════════════════════════════╝
`);

    if (shouldPause) {
      this.state.consecutiveLosses = 0;
      setTimeout(() => {
        this._circuitBreakerTriggered = false;
        console.log('[RISK] 🔓 熔断期结束，交易已恢复');
        notifier.systemStatus('熔断期结束', '交易已恢复，请关注接下来的交易表现。').catch(() => { });
      }, this.params.CIRCUIT_BREAKER.PAUSE_DURATION_HOURS * 60 * 60 * 1000);
    } else {
      this._circuitBreakerTriggered = false;
    }
  }

  /**
   * 获取当前持仓数
   */
  getOpenPositionsCount() {
    try {
      const result = this.db.prepare(`
        SELECT COUNT(*) as count FROM live_positions WHERE status = 'open'
      `).get();
      return result?.count || 0;
    } catch (error) {
      return 0;
    }
  }

  /**
   * 获取近期统计
   */
  getRecentStats() {
    try {
      const stats = this.db.prepare(`
        SELECT
          COUNT(*) as total,
          SUM(CASE WHEN exit_pnl > 0 THEN 1 ELSE 0 END) as wins
        FROM live_positions
        WHERE status = 'closed'
        AND closed_at > strftime('%s', 'now', '-7 days') * 1000
      `).get();

      return {
        totalTrades: stats?.total || 0,
        wins: stats?.wins || 0,
        winRate: stats?.total > 0 ? stats.wins / stats.total : 0
      };
    } catch (error) {
      return { totalTrades: 0, wins: 0, winRate: 0 };
    }
  }

  /**
   * 检查是否处于防御模式
   */
  isDefensiveModeActive() {
    return this._defensiveModeActive && this.params.DEFENSIVE_MODE.ENABLED;
  }

  /**
   * 🔥 v9.4 检查是否在熔断恢复期
   * 熔断触发后的指定时间内（默认4小时），系统处于恢复期
   * 恢复期内仓位减半，降低连续亏损的风险
   * @returns {boolean}
   */
  _isInCircuitBreakerRecovery() {
    if (!this._lastCircuitBreakerTime) {
      return false;
    }

    const recoveryHours = this.params.CIRCUIT_BREAKER.RECOVERY_PERIOD_HOURS || 4;
    const recoveryPeriodMs = recoveryHours * 60 * 60 * 1000;
    const elapsed = Date.now() - this._lastCircuitBreakerTime;

    // 如果恢复期结束，清除记录
    if (elapsed >= recoveryPeriodMs) {
      this._lastCircuitBreakerTime = null;
      return false;
    }

    return true;
  }

  /**
   * 获取状态
   */
  getStatus() {
    const stats = this.getRecentStats();
    const isDefensive = this.isDefensiveModeActive();
    const isInRecovery = this._isInCircuitBreakerRecovery();

    return {
      canTrade: this.canTrade(),
      consecutiveLosses: this.state.consecutiveLosses,
      pausedUntil: this.state.pausedUntil,
      openPositions: this.getOpenPositionsCount(),
      maxPositions: this.params.MAX_TOTAL_POSITIONS,
      basePositions: this.params.MAX_CONCURRENT_POSITIONS,
      goldenSlots: this.params.GOLDEN_EXTRA_SLOTS,
      recentStats: stats,
      minScore: isDefensive
        ? this.params.MIN_SCORE_TO_TRADE + this.params.DEFENSIVE_MODE.MIN_SCORE_BOOST
        : this.params.MIN_SCORE_TO_TRADE,
      defensiveMode: isDefensive,
      circuitBreakerActive: this._circuitBreakerTriggered || false,
      // v9.4: 熔断恢复期状态
      inRecoveryPeriod: isInRecovery,
      recoveryTimeRemaining: isInRecovery
        ? Math.ceil((this.params.CIRCUIT_BREAKER.RECOVERY_PERIOD_HOURS * 60 * 60 * 1000 - (Date.now() - this._lastCircuitBreakerTime)) / 60000)
        : 0,
      dailyNetPnlSol: this._getDailyNetPnl(),
      dailyLossLimitSol: this.params.DAILY_LOSS_LIMIT?.SOL || 0.3
    };
  }

  _getDailyNetPnl() {
    try {
      const todayStart = new Date();
      todayStart.setHours(0, 0, 0, 0);
      const row = this.db.prepare(`
        SELECT COALESCE(SUM(entry_sol), 0) as total_spent,
               COALESCE(SUM(CASE WHEN total_sol_received > 0 THEN total_sol_received ELSE 0 END), 0) as total_received
        FROM live_positions
        WHERE status = 'closed' AND closed_at >= ?
      `).get(this._getDailyStartTs());
      return row ? +(row.total_received - row.total_spent).toFixed(4) : 0;
    } catch (e) { return 0; }
  }

  /**
   * 🔄 获取可替换的持仓 (v6.3 Swap 机制)
   * 找到表现最差的持仓，用于腾笼换鸟
   * @returns {{ position: object, canSwap: boolean, reason: string }}
   */
  getSwapCandidate() {
    const config = this.params.SWAP_CONFIG;

    if (!config.ENABLED) {
      return { canSwap: false, reason: 'Swap 机制未启用' };
    }

    try {
      // 获取所有开放持仓，按实时盈亏排序（亏损最多的在前）
      // open 仓位 exit_pnl 为 NULL，用 partial sell 收入估算
      const positions = this.db.prepare(`
        SELECT
          id, symbol, token_ca, entry_sol, total_sol_received,
          CASE
            WHEN entry_sol > 0 THEN (total_sol_received - entry_sol) / entry_sol * 100
            ELSE 0
          END as estimated_pnl,
          created_at, entry_sol as position_size
        FROM live_positions
        WHERE status = 'open'
        ORDER BY estimated_pnl ASC, created_at ASC
      `).all();

      if (positions.length === 0) {
        return { canSwap: false, reason: '无持仓可替换' };
      }

      const now = Date.now();
      const minHoldTime = config.MIN_HOLD_TIME_MINUTES * 60 * 1000;

      // 找到符合替换条件的最差持仓
      for (const pos of positions) {
        const holdTime = now - pos.created_at;
        const pnl = pos.estimated_pnl || 0;
        const smDelta = 0; // live_positions 没有 sm 字段

        // 检查所有条件
        const isLoss = pnl < 0;
        const isOldEnough = holdTime >= minHoldTime;

        if (config.REQUIRE_LOSS && !isLoss) continue;
        if (!isOldEnough) continue;

        // 找到了可替换的持仓
        return {
          canSwap: true,
          position: pos,
          reason: `可替换 ${pos.symbol} (PnL: ${pnl.toFixed(1)}%, 持有${(holdTime / 60000).toFixed(0)}min)`
        };
      }

      return {
        canSwap: false,
        reason: '无符合条件的持仓可替换（需要亏损+持有30min+SM流出）'
      };

    } catch (error) {
      return { canSwap: false, reason: `查询失败: ${error.message}` };
    }
  }

  /**
   * 🔄 检查是否可以使用金狗预留槽或 Swap
   * @param {object} signal - 新信号
   * @param {number} score - 评分
   * @param {string} tag - 标签 (GOLDEN/SILVER/etc)
   * @returns {{ allowed: boolean, reason: string, swapPosition?: object }}
   */
  canUseGoldenSlotOrSwap(signal, score, tag) {
    const openPositions = this.getOpenPositionsCount();
    const baseLimit = this.params.MAX_CONCURRENT_POSITIONS;
    const totalLimit = this.params.MAX_TOTAL_POSITIONS;
    const config = this.params.SWAP_CONFIG;

    // 1. 还没满基础持仓，直接通过
    if (openPositions < baseLimit) {
      return { allowed: true, reason: '基础槽位可用' };
    }

    // 2. 检查是否是金狗
    const isGolden = tag === 'GOLDEN' && score >= config.MIN_SCORE_TO_SWAP;

    if (!isGolden) {
      return {
        allowed: false,
        reason: `非金狗信号 (${tag}/${score}分) 不能使用预留槽`
      };
    }

    // 3. 金狗信号，检查预留槽
    if (openPositions < totalLimit) {
      console.log(`   🥇 [RISK] 金狗信号使用预留槽 (${openPositions + 1}/${totalLimit})`);
      return { allowed: true, reason: '使用金狗预留槽', isGoldenSlot: true };
    }

    // 4. 完全满仓，尝试 Swap
    if (config.ENABLED) {
      const swapResult = this.getSwapCandidate();
      if (swapResult.canSwap) {
        console.log(`   🔄 [RISK] 腾笼换鸟: ${swapResult.reason}`);
        return {
          allowed: true,
          reason: '触发腾笼换鸟',
          swapPosition: swapResult.position
        };
      }
      return { allowed: false, reason: swapResult.reason };
    }

    return { allowed: false, reason: '持仓已满且 Swap 未启用' };
  }
}

export default RiskManager;
