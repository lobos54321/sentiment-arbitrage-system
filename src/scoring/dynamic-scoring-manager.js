/**
 * Dynamic Scoring Manager
 * 
 * 管理所有动态评分机制：
 * 1. 叙事热度追踪 - 定时查 Twitter，更新叙事权重
 * 2. 信号聚合 - 10分钟内多频道提同一币，分数累加
 * 3. 时间衰减 - 信号越老，分数越低
 * 4. 链上实时监控 - 聪明钱动向影响 Graph 分数
 * 5. 信号源胜率 - 每周统计，动态调整 Source 分数
 */

export class DynamicScoringManager {
  constructor(config, db) {
    this.config = config;
    this.db = db;
    
    // 缓存
    this.narrativeHeatCache = new Map(); // 叙事热度缓存
    this.signalAggregationCache = new Map(); // 信号聚合缓存
    this.channelPerformanceCache = new Map(); // 频道表现缓存
    this.smartMoneyCache = new Map(); // 聪明钱动向缓存
    
    // 配置
    this.SIGNAL_AGGREGATION_WINDOW = 10 * 60 * 1000; // 10分钟
    this.TIME_DECAY_HALF_LIFE = 5 * 60 * 1000; // 5分钟半衰期
    this.NARRATIVE_UPDATE_INTERVAL = 30 * 60 * 1000; // 30分钟更新叙事
    this.CHANNEL_STATS_UPDATE_INTERVAL = 24 * 60 * 60 * 1000; // 24小时更新频道统计
    
    // 初始化数据库表
    this.initTables();
    
    // 启动定时任务
    this.startPeriodicUpdates();
  }

  /**
   * 初始化数据库表
   */
  initTables() {
    try {
      // 叙事热度追踪表
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS narrative_heat (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          narrative_name TEXT NOT NULL,
          heat_score REAL DEFAULT 0,
          twitter_mentions_1h INTEGER DEFAULT 0,
          twitter_engagement_1h INTEGER DEFAULT 0,
          token_count_24h INTEGER DEFAULT 0,
          avg_performance REAL DEFAULT 0,
          updated_at INTEGER NOT NULL,
          UNIQUE(narrative_name)
        )
      `);

      // 信号聚合表
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS signal_aggregation (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          token_ca TEXT NOT NULL,
          first_seen_at INTEGER NOT NULL,
          channel_count INTEGER DEFAULT 1,
          channels TEXT DEFAULT '[]',
          total_score_boost REAL DEFAULT 0,
          updated_at INTEGER NOT NULL
        )
      `);

      // 频道历史表现表
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS channel_performance (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          channel_name TEXT NOT NULL UNIQUE,
          total_signals INTEGER DEFAULT 0,
          winning_signals INTEGER DEFAULT 0,
          total_pnl REAL DEFAULT 0,
          avg_pnl REAL DEFAULT 0,
          win_rate REAL DEFAULT 0,
          performance_score REAL DEFAULT 5,
          last_updated INTEGER NOT NULL
        )
      `);

      // 聪明钱动向表
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS smart_money_activity (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          token_ca TEXT NOT NULL,
          wallet_address TEXT NOT NULL,
          action TEXT NOT NULL,
          amount_usd REAL DEFAULT 0,
          timestamp INTEGER NOT NULL
        )
      `);

      console.log('✅ [Dynamic Scoring] Database tables initialized');
    } catch (error) {
      console.error('❌ [Dynamic Scoring] Table init failed:', error.message);
    }
  }

  /**
   * 启动定时更新任务
   */
  startPeriodicUpdates() {
    // 每30分钟更新叙事热度
    setInterval(() => this.updateNarrativeHeat(), this.NARRATIVE_UPDATE_INTERVAL);
    
    // 每24小时更新频道统计
    setInterval(() => this.updateChannelPerformance(), this.CHANNEL_STATS_UPDATE_INTERVAL);
    
    // 每分钟清理过期的信号聚合数据
    setInterval(() => this.cleanupExpiredAggregations(), 60 * 1000);
    
    console.log('📊 [Dynamic Scoring] Periodic updates started');
  }

  // ==========================================
  // 1. 叙事热度追踪
  // ==========================================

  /**
   * 更新叙事热度（定时任务调用）
   */
  async updateNarrativeHeat() {
    console.log('🔥 [Dynamic Scoring] Updating narrative heat...');
    
    // 获取过去24小时所有交易的叙事
    try {
      const oneDayAgo = Math.floor(Date.now() / 1000) - (24 * 60 * 60);
      
      const narrativeStats = this.db.prepare(`
        SELECT 
          sd.narrative_name,
          COUNT(*) as token_count,
          AVG(p.pnl_percent) as avg_performance,
          SUM(CASE WHEN p.pnl_percent > 0 THEN 1 ELSE 0 END) as winning_count
        FROM score_details sd
        LEFT JOIN positions p ON sd.token_ca = p.token_ca
        WHERE sd.calculated_at >= ? * 1000
          AND sd.narrative_name IS NOT NULL
        GROUP BY sd.narrative_name
      `).all(oneDayAgo);

      for (const stat of narrativeStats) {
        const heatScore = this.calculateNarrativeHeatScore(stat);
        
        this.db.prepare(`
          INSERT OR REPLACE INTO narrative_heat 
          (narrative_name, heat_score, token_count_24h, avg_performance, updated_at)
          VALUES (?, ?, ?, ?, ?)
        `).run(
          stat.narrative_name,
          heatScore,
          stat.token_count,
          stat.avg_performance || 0,
          Date.now()
        );

        // 更新缓存
        this.narrativeHeatCache.set(stat.narrative_name, {
          heatScore,
          tokenCount: stat.token_count,
          avgPerformance: stat.avg_performance || 0,
          updatedAt: Date.now()
        });
      }

      console.log(`   ✅ Updated ${narrativeStats.length} narratives`);
    } catch (error) {
      console.error('❌ [Dynamic Scoring] Narrative heat update failed:', error.message);
    }
  }

  /**
   * 计算叙事热度分数
   */
  calculateNarrativeHeatScore(stat) {
    let score = 50; // 基础分
    
    // Token 数量加成 (最多 +20)
    if (stat.token_count >= 10) score += 20;
    else if (stat.token_count >= 5) score += 15;
    else if (stat.token_count >= 3) score += 10;
    else if (stat.token_count >= 1) score += 5;
    
    // 表现加成 (最多 +30)
    const avgPerf = stat.avg_performance || 0;
    if (avgPerf >= 100) score += 30;
    else if (avgPerf >= 50) score += 20;
    else if (avgPerf >= 20) score += 10;
    else if (avgPerf >= 0) score += 5;
    else score -= 10; // 负收益扣分
    
    // 胜率加成
    const winRate = stat.winning_count / stat.token_count;
    if (winRate >= 0.6) score += 10;
    else if (winRate >= 0.4) score += 5;
    
    return Math.max(0, Math.min(100, score));
  }

  /**
   * 获取叙事热度调整系数
   */
  getNarrativeHeatMultiplier(narrativeName) {
    const cached = this.narrativeHeatCache.get(narrativeName);
    
    if (cached && Date.now() - cached.updatedAt < this.NARRATIVE_UPDATE_INTERVAL) {
      // 热度 0-100 映射到 0.5-1.5 的系数
      return 0.5 + (cached.heatScore / 100);
    }
    
    // 从数据库查询
    try {
      const result = this.db.prepare(`
        SELECT heat_score FROM narrative_heat WHERE narrative_name = ?
      `).get(narrativeName);
      
      if (result) {
        return 0.5 + (result.heat_score / 100);
      }
    } catch (e) {}
    
    return 1.0; // 默认无调整
  }

  // ==========================================
  // 2. 信号聚合（多频道提同一币）
  // ==========================================

  /**
   * 记录新信号并计算聚合加成
   */
  recordSignalAndGetBoost(tokenCA, channelName) {
    const now = Date.now();
    const windowStart = now - this.SIGNAL_AGGREGATION_WINDOW;
    
    // 查询最近10分钟内的信号
    let aggregation = this.signalAggregationCache.get(tokenCA);
    
    if (!aggregation || aggregation.firstSeenAt < windowStart) {
      // 新信号或者已过期
      aggregation = {
        tokenCA,
        firstSeenAt: now,
        channels: new Set([channelName]),
        updatedAt: now
      };
    } else {
      // 添加新频道
      aggregation.channels.add(channelName);
      aggregation.updatedAt = now;
    }
    
    this.signalAggregationCache.set(tokenCA, aggregation);
    
    // 计算加成分数
    const channelCount = aggregation.channels.size;
    let boost = 0;
    
    // 多频道加成逻辑
    if (channelCount >= 5) {
      boost = 15; // 5+ 频道 = +15分
      console.log(`   🔥 [Signal Aggregation] ${tokenCA.slice(0,8)}... - ${channelCount} channels = +${boost}pts`);
    } else if (channelCount >= 3) {
      boost = 10; // 3-4 频道 = +10分
      console.log(`   📈 [Signal Aggregation] ${tokenCA.slice(0,8)}... - ${channelCount} channels = +${boost}pts`);
    } else if (channelCount >= 2) {
      boost = 5; // 2 频道 = +5分
      console.log(`   📊 [Signal Aggregation] ${tokenCA.slice(0,8)}... - ${channelCount} channels = +${boost}pts`);
    }
    
    // 持久化到数据库
    try {
      this.db.prepare(`
        INSERT OR REPLACE INTO signal_aggregation 
        (token_ca, first_seen_at, channel_count, channels, total_score_boost, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
      `).run(
        tokenCA,
        aggregation.firstSeenAt,
        channelCount,
        JSON.stringify([...aggregation.channels]),
        boost,
        now
      );
    } catch (e) {}
    
    return {
      boost,
      channelCount,
      channels: [...aggregation.channels],
      isFirst: channelCount === 1
    };
  }

  /**
   * 获取信号聚合信息
   */
  getSignalAggregation(tokenCA) {
    const cached = this.signalAggregationCache.get(tokenCA);
    const now = Date.now();
    
    if (cached && (now - cached.firstSeenAt) < this.SIGNAL_AGGREGATION_WINDOW) {
      return {
        channelCount: cached.channels.size,
        channels: [...cached.channels],
        ageMinutes: Math.round((now - cached.firstSeenAt) / 60000)
      };
    }
    
    return { channelCount: 0, channels: [], ageMinutes: 0 };
  }

  // ==========================================
  // 3. 时间衰减机制
  // ==========================================

  /**
   * 计算时间衰减系数
   * 
   * 使用指数衰减：score * e^(-t/τ)
   * τ = 半衰期（5分钟）
   * 
   * 0分钟: 100%
   * 5分钟: 50%
   * 10分钟: 25%
   * 15分钟: 12.5%
   */
  calculateTimeDecay(firstSeenAt) {
    const now = Date.now();
    const ageMs = now - firstSeenAt;
    
    // 指数衰减
    const decayFactor = Math.exp(-ageMs / this.TIME_DECAY_HALF_LIFE);
    
    // 限制最低衰减到 0.1（10%）
    return Math.max(0.1, decayFactor);
  }

  /**
   * 获取带时间衰减的分数调整
   */
  getTimeDecayAdjustment(tokenCA) {
    const aggregation = this.signalAggregationCache.get(tokenCA);
    
    if (!aggregation) {
      return { multiplier: 1.0, ageMinutes: 0 };
    }
    
    const multiplier = this.calculateTimeDecay(aggregation.firstSeenAt);
    const ageMinutes = Math.round((Date.now() - aggregation.firstSeenAt) / 60000);
    
    return { multiplier, ageMinutes };
  }

  // ==========================================
  // 4. 链上实时监控（聪明钱）
  // ==========================================

  /**
   * 记录聪明钱活动
   */
  recordSmartMoneyActivity(tokenCA, wallet, action, amountUSD) {
    try {
      this.db.prepare(`
        INSERT INTO smart_money_activity (token_ca, wallet_address, action, amount_usd, timestamp)
        VALUES (?, ?, ?, ?, ?)
      `).run(tokenCA, wallet, action, amountUSD, Date.now());
      
      // 更新缓存
      const key = tokenCA;
      const activities = this.smartMoneyCache.get(key) || [];
      activities.push({ wallet, action, amountUSD, timestamp: Date.now() });
      this.smartMoneyCache.set(key, activities.slice(-20)); // 保留最近20条
      
    } catch (e) {}
  }

  /**
   * 获取聪明钱评分调整
   * 
   * 买入 = 加分
   * 卖出 = 减分
   * 大额操作权重更高
   */
  getSmartMoneyAdjustment(tokenCA) {
    const oneHourAgo = Date.now() - (60 * 60 * 1000);
    
    // 从缓存获取
    const activities = this.smartMoneyCache.get(tokenCA) || [];
    const recentActivities = activities.filter(a => a.timestamp >= oneHourAgo);
    
    if (recentActivities.length === 0) {
      // 从数据库查询
      try {
        const dbActivities = this.db.prepare(`
          SELECT action, amount_usd, timestamp FROM smart_money_activity
          WHERE token_ca = ? AND timestamp >= ?
          ORDER BY timestamp DESC
          LIMIT 20
        `).all(tokenCA, oneHourAgo);
        
        if (dbActivities.length === 0) {
          return { adjustment: 0, reason: '无聪明钱数据' };
        }
        
        return this.calculateSmartMoneyScore(dbActivities);
      } catch (e) {
        return { adjustment: 0, reason: '查询失败' };
      }
    }
    
    return this.calculateSmartMoneyScore(recentActivities);
  }

  calculateSmartMoneyScore(activities) {
    let buyVolume = 0;
    let sellVolume = 0;
    
    for (const activity of activities) {
      if (activity.action === 'buy') {
        buyVolume += activity.amount_usd || activity.amountUSD || 0;
      } else if (activity.action === 'sell') {
        sellVolume += activity.amount_usd || activity.amountUSD || 0;
      }
    }
    
    const netFlow = buyVolume - sellVolume;
    let adjustment = 0;
    let reason = '';
    
    if (netFlow > 10000) {
      adjustment = 5;
      reason = `🐋 聪明钱净流入 $${(netFlow/1000).toFixed(0)}K`;
    } else if (netFlow > 5000) {
      adjustment = 3;
      reason = `📈 聪明钱流入 $${(netFlow/1000).toFixed(0)}K`;
    } else if (netFlow > 0) {
      adjustment = 1;
      reason = `轻微流入 $${netFlow.toFixed(0)}`;
    } else if (netFlow < -10000) {
      adjustment = -5;
      reason = `⚠️ 聪明钱出逃 $${(Math.abs(netFlow)/1000).toFixed(0)}K`;
    } else if (netFlow < -5000) {
      adjustment = -3;
      reason = `⚠️ 聪明钱流出 $${(Math.abs(netFlow)/1000).toFixed(0)}K`;
    } else if (netFlow < 0) {
      adjustment = -1;
      reason = `轻微流出`;
    } else {
      reason = '聪明钱持平';
    }
    
    return { adjustment, reason, buyVolume, sellVolume, netFlow };
  }

  // ==========================================
  // 5. 信号源胜率动态更新
  // ==========================================

  /**
   * 更新频道表现统计（定时任务调用）
   */
  async updateChannelPerformance() {
    console.log('📊 [Dynamic Scoring] Updating channel performance...');
    
    try {
      const sevenDaysAgo = Math.floor(Date.now() / 1000) - (7 * 24 * 60 * 60);
      
      const channelStats = this.db.prepare(`
        SELECT 
          s.channel_name,
          COUNT(DISTINCT p.id) as total_signals,
          SUM(CASE WHEN p.pnl_percent > 0 THEN 1 ELSE 0 END) as winning_signals,
          SUM(p.pnl_percent) as total_pnl,
          AVG(p.pnl_percent) as avg_pnl
        FROM telegram_signals s
        LEFT JOIN positions p ON s.id = p.signal_id
        WHERE s.created_at >= ?
          AND p.status = 'closed'
        GROUP BY s.channel_name
      `).all(sevenDaysAgo);

      for (const stat of channelStats) {
        const winRate = stat.total_signals > 0 
          ? (stat.winning_signals / stat.total_signals) * 100 
          : 50;
        
        // 计算表现分数 (0-10)
        let performanceScore = 5; // 基础分
        
        // 胜率调整
        if (winRate >= 60) performanceScore += 2;
        else if (winRate >= 50) performanceScore += 1;
        else if (winRate < 40) performanceScore -= 1;
        else if (winRate < 30) performanceScore -= 2;
        
        // 平均收益调整
        const avgPnl = stat.avg_pnl || 0;
        if (avgPnl >= 50) performanceScore += 2;
        else if (avgPnl >= 20) performanceScore += 1;
        else if (avgPnl < 0) performanceScore -= 1;
        else if (avgPnl < -20) performanceScore -= 2;
        
        // 样本量调整
        if (stat.total_signals >= 10) performanceScore += 1;
        
        performanceScore = Math.max(0, Math.min(10, performanceScore));
        
        this.db.prepare(`
          INSERT OR REPLACE INTO channel_performance 
          (channel_name, total_signals, winning_signals, total_pnl, avg_pnl, win_rate, performance_score, last_updated)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
          stat.channel_name,
          stat.total_signals,
          stat.winning_signals,
          stat.total_pnl || 0,
          avgPnl,
          winRate,
          performanceScore,
          Date.now()
        );

        // 更新缓存
        this.channelPerformanceCache.set(stat.channel_name, {
          totalSignals: stat.total_signals,
          winRate,
          avgPnl,
          performanceScore,
          updatedAt: Date.now()
        });
      }

      console.log(`   ✅ Updated ${channelStats.length} channels`);
    } catch (error) {
      console.error('❌ [Dynamic Scoring] Channel performance update failed:', error.message);
    }
  }

  /**
   * 获取频道表现分数
   */
  getChannelPerformanceScore(channelName) {
    // 从缓存获取
    const cached = this.channelPerformanceCache.get(channelName);
    if (cached && Date.now() - cached.updatedAt < this.CHANNEL_STATS_UPDATE_INTERVAL) {
      return cached.performanceScore;
    }
    
    // 从数据库查询
    try {
      const result = this.db.prepare(`
        SELECT performance_score FROM channel_performance WHERE channel_name = ?
      `).get(channelName);
      
      if (result) {
        return result.performance_score;
      }
    } catch (e) {}
    
    return 5; // 默认中等分数
  }

  // ==========================================
  // 综合动态评分
  // ==========================================

  /**
   * 获取所有动态调整
   * @param {string} tokenCA - Token address
   * @param {string} channelName - Channel name
   * @param {string} narrativeName - Narrative name
   * @param {number} smartWalletOnline - v6.9 聪明钱在线数量（用于早期入场奖励）
   */
  getAllDynamicAdjustments(tokenCA, channelName, narrativeName, smartWalletOnline = 0) {
    // 1. 信号聚合加成
    const aggregation = this.recordSignalAndGetBoost(tokenCA, channelName);

    // 2. 时间衰减
    const timeDecay = this.getTimeDecayAdjustment(tokenCA);

    // 3. 叙事热度
    const narrativeMultiplier = this.getNarrativeHeatMultiplier(narrativeName);

    // 4. 聪明钱净流入调整
    const smartMoney = this.getSmartMoneyAdjustment(tokenCA);

    // 5. 频道表现
    const channelScore = this.getChannelPerformanceScore(channelName);

    // 🐦 v6.9 早期入场奖励 (Early Entry Bonus)
    // 数据分析显示: SM少 = 早期入场 = 更好的风险回报
    // SM=1: +15分, SM=2: +10分, SM=3: +5分, SM>=4: 0分
    const earlyEntryBonus = this.calculateEarlyEntryBonus(smartWalletOnline);

    return {
      aggregation: {
        boost: aggregation.boost,
        channelCount: aggregation.channelCount,
        isFirst: aggregation.isFirst
      },
      timeDecay: {
        multiplier: timeDecay.multiplier,
        ageMinutes: timeDecay.ageMinutes
      },
      narrative: {
        multiplier: narrativeMultiplier
      },
      smartMoney: {
        adjustment: smartMoney.adjustment,
        reason: smartMoney.reason,
        netFlow: smartMoney.netFlow
      },
      channel: {
        performanceScore: channelScore
      },
      // v6.9 新增
      earlyEntry: {
        bonus: earlyEntryBonus.bonus,
        smCount: smartWalletOnline,
        reason: earlyEntryBonus.reason
      }
    };
  }

  /**
   * 🐦 v6.9 早期入场奖励计算
   *
   * 逻辑: 聪明钱越少 = 越早期 = 越值得奖励
   * - SM=1: +15分 (最早，第一批聪明钱)
   * - SM=2: +10分 (早期)
   * - SM=3: +5分  (中早期)
   * - SM>=4: 0分  (已经有足够追踪者，非早期)
   */
  calculateEarlyEntryBonus(smartWalletOnline) {
    const smCount = smartWalletOnline || 0;

    if (smCount === 1) {
      return { bonus: 15, reason: '🐦 第一聪明钱 +15分' };
    } else if (smCount === 2) {
      return { bonus: 10, reason: '🐦 早期入场(SM=2) +10分' };
    } else if (smCount === 3) {
      return { bonus: 5, reason: '🐦 中早期(SM=3) +5分' };
    } else {
      return { bonus: 0, reason: smCount > 0 ? `${smCount}个SM，非早期` : '无SM数据' };
    }
  }

  /**
   * 应用动态调整到基础分数
   */
  applyDynamicAdjustments(baseScore, adjustments) {
    let finalScore = baseScore;
    const reasons = [];

    // 1. 信号聚合加成（直接加分）
    if (adjustments.aggregation.boost > 0) {
      finalScore += adjustments.aggregation.boost;
      reasons.push(`📡 ${adjustments.aggregation.channelCount}频道聚合 +${adjustments.aggregation.boost}pts`);
    }

    // 2. 时间衰减（乘法）
    if (adjustments.timeDecay.multiplier < 1.0) {
      const decayPenalty = Math.round((1 - adjustments.timeDecay.multiplier) * baseScore);
      finalScore -= decayPenalty;
      reasons.push(`⏰ 信号已${adjustments.timeDecay.ageMinutes}分钟 -${decayPenalty}pts`);
    }

    // 3. 叙事热度（乘法，只对叙事部分）
    // 这个在叙事计算时应用

    // 4. 聪明钱净流入调整（直接加减）
    if (adjustments.smartMoney.adjustment !== 0) {
      finalScore += adjustments.smartMoney.adjustment;
      reasons.push(adjustments.smartMoney.reason);
    }

    // 5. 频道表现会在 Source 计算时使用

    // 🐦 v6.9 早期入场奖励（直接加分）
    if (adjustments.earlyEntry && adjustments.earlyEntry.bonus > 0) {
      finalScore += adjustments.earlyEntry.bonus;
      reasons.push(adjustments.earlyEntry.reason);
      console.log(`   🐦 [v6.9] 早期入场奖励: SM=${adjustments.earlyEntry.smCount} +${adjustments.earlyEntry.bonus}分`);
    }

    return {
      score: Math.max(0, Math.min(100, Math.round(finalScore))),
      reasons
    };
  }

  // ==========================================
  // 清理过期数据
  // ==========================================

  cleanupExpiredAggregations() {
    const cutoff = Date.now() - this.SIGNAL_AGGREGATION_WINDOW;
    
    // 清理内存缓存
    for (const [key, value] of this.signalAggregationCache) {
      if (value.firstSeenAt < cutoff) {
        this.signalAggregationCache.delete(key);
      }
    }
    
    // 清理数据库
    try {
      this.db.prepare(`
        DELETE FROM signal_aggregation WHERE first_seen_at < ?
      `).run(cutoff);
    } catch (e) {}
  }
}

export default DynamicScoringManager;
