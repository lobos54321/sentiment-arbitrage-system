/**
 * Shadow Mode Price Tracker
 * 
 * 在影子模式下追踪信号的价格变化，用于评估信号源质量
 * 
 * 工作流程：
 * 1. 信号进入时记录初始价格
 * 2. 定期检查价格（5分钟、15分钟、1小时）
 * 3. 计算模拟 PnL 并更新信号源表现
 */

export class ShadowPriceTracker {
  constructor(config, db, solService, bscService, sourceOptimizer) {
    this.config = config;
    this.db = db;
    this.solService = solService;
    this.bscService = bscService;
    this.sourceOptimizer = sourceOptimizer;
    
    // 追踪中的信号
    this.trackedSignals = new Map();
    
    // 最大追踪时间（1小时后停止追踪）
    this.MAX_TRACK_TIME = 60 * 60 * 1000;
    
    // 初始化数据库表
    this.initializeDatabase();
    
    // 启动定时检查
    this.startPriceChecker();

    // 启动时回填最近信号（用于面板立即有数据）
    this.bootstrapRecentSignals();
  }

  initializeDatabase() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS shadow_price_tracking (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_outcome_id INTEGER,
        token_ca TEXT NOT NULL,
        chain TEXT NOT NULL,
        source_type TEXT,
        source_id TEXT,
        
        -- 初始数据
        entry_price REAL,
        entry_time INTEGER,
        entry_liquidity REAL,
        
        -- 5分钟检查点
        price_5m REAL,
        pnl_5m REAL,
        
        -- 15分钟检查点
        price_15m REAL,
        pnl_15m REAL,
        
        -- 1小时检查点
        price_1h REAL,
        pnl_1h REAL,
        
        -- 最高/最低价
        max_price REAL,
        max_pnl REAL,
        min_price REAL,
        min_pnl REAL,
        
        -- 状态
        status TEXT DEFAULT 'tracking',  -- 'tracking', 'completed', 'failed'
        completed_at INTEGER,
        
        created_at INTEGER DEFAULT (strftime('%s', 'now'))
      );
      
      CREATE INDEX IF NOT EXISTS idx_shadow_token ON shadow_price_tracking(token_ca, chain);
      CREATE INDEX IF NOT EXISTS idx_shadow_status ON shadow_price_tracking(status);
    `);
  }

  /**
   * 开始追踪一个信号
   */
  trackSignal(tokenCA, chain, entryPrice, entryLiquidity, sourceType, sourceId, signalOutcomeId = null) {
    const now = Date.now();
    const trackingId = `${chain}:${tokenCA}`;
    
    // 避免重复追踪
    if (this.trackedSignals.has(trackingId)) {
      return;
    }
    
    // 插入数据库
    const result = this.db.prepare(`
      INSERT INTO shadow_price_tracking (
        signal_outcome_id, token_ca, chain, source_type, source_id,
        entry_price, entry_time, entry_liquidity, max_price, max_pnl, min_price, min_pnl
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0)
    `).run(
      signalOutcomeId, tokenCA, chain, sourceType, sourceId,
      entryPrice, Math.floor(now / 1000), entryLiquidity,
      entryPrice, entryPrice
    );
    
    // 添加到内存追踪
    this.trackedSignals.set(trackingId, {
      id: result.lastInsertRowid,
      tokenCA,
      chain,
      sourceType,
      sourceId,
      signalOutcomeId,
      entryPrice,
      entryTime: now,
      checkpoints: {
        '5m': false,
        '15m': false,
        '1h': false
      },
      maxPrice: entryPrice,
      minPrice: entryPrice
    });
    
    console.log(`   📊 [Shadow] Tracking ${tokenCA.substring(0, 8)}... (entry: $${entryPrice?.toFixed(10) || 'N/A'})`);
  }

  /**
   * 启动定时价格检查器
   */
  startPriceChecker() {
    // 每分钟检查一次
    setInterval(() => this.checkPrices(), 60 * 1000);
    console.log('📊 [Shadow] Price tracker started (checking every 1 min)');
  }

  /**
   * 启动时回填最近一批信号，避免面板长时间无数据
   */
  async bootstrapRecentSignals() {
    try {
      const since = Math.floor(Date.now() / 1000) - 60 * 60; // 近 60 分钟
      const signals = this.db.prepare(`
        SELECT id, token_ca, chain, channel_name
        FROM telegram_signals
        WHERE created_at >= ?
        ORDER BY created_at DESC
        LIMIT 20
      `).all(since);

      for (const s of signals) {
        const exists = this.db.prepare(`
          SELECT 1 FROM shadow_price_tracking
          WHERE token_ca = ? AND chain = ?
          LIMIT 1
        `).get(s.token_ca, s.chain);

        if (exists) continue;

        const service = s.chain === 'SOL' ? this.solService : this.bscService;
        const snapshot = await service.getSnapshot(s.token_ca);
        if (!snapshot?.current_price) continue;

        this.trackSignal(
          s.token_ca,
          s.chain,
          snapshot.current_price,
          snapshot.liquidity_usd || 0,
          'telegram',
          s.channel_name,
          s.id
        );
      }
    } catch (e) {
      // ignore
    }
  }

  /**
   * 检查所有追踪中的信号价格
   */
  async checkPrices() {
    const now = Date.now();
    
    for (const [trackingId, signal] of this.trackedSignals) {
      const elapsed = now - signal.entryTime;
      
      // 超过最大追踪时间，完成追踪
      if (elapsed > this.MAX_TRACK_TIME) {
        await this.completeTracking(trackingId, signal);
        continue;
      }
      
      try {
        // 获取当前价格
        const service = signal.chain === 'SOL' ? this.solService : this.bscService;
        const snapshot = await service.getSnapshot(signal.tokenCA);
        
        if (!snapshot || !snapshot.current_price) {
          continue;
        }
        
        const currentPrice = snapshot.current_price;
        const pnl = signal.entryPrice > 0 
          ? ((currentPrice - signal.entryPrice) / signal.entryPrice) * 100 
          : 0;
        
        // 更新最高/最低价
        if (currentPrice > signal.maxPrice) {
          signal.maxPrice = currentPrice;
          this.db.prepare(`
            UPDATE shadow_price_tracking SET max_price = ?, max_pnl = ? WHERE id = ?
          `).run(currentPrice, pnl, signal.id);
        }
        if (currentPrice < signal.minPrice) {
          signal.minPrice = currentPrice;
          this.db.prepare(`
            UPDATE shadow_price_tracking SET min_price = ?, min_pnl = ? WHERE id = ?
          `).run(currentPrice, pnl, signal.id);
        }
        
        // 检查时间点
        // 5分钟检查点
        if (!signal.checkpoints['5m'] && elapsed >= 5 * 60 * 1000) {
          signal.checkpoints['5m'] = true;
          this.db.prepare(`
            UPDATE shadow_price_tracking SET price_5m = ?, pnl_5m = ? WHERE id = ?
          `).run(currentPrice, pnl, signal.id);
          console.log(`   📊 [Shadow] ${signal.tokenCA.substring(0, 8)}... 5min: ${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}%`);
        }
        
        // 15分钟检查点
        if (!signal.checkpoints['15m'] && elapsed >= 15 * 60 * 1000) {
          signal.checkpoints['15m'] = true;
          this.db.prepare(`
            UPDATE shadow_price_tracking SET price_15m = ?, pnl_15m = ? WHERE id = ?
          `).run(currentPrice, pnl, signal.id);
          console.log(`   📊 [Shadow] ${signal.tokenCA.substring(0, 8)}... 15min: ${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}%`);
        }
        
        // 1小时检查点
        if (!signal.checkpoints['1h'] && elapsed >= 60 * 60 * 1000) {
          signal.checkpoints['1h'] = true;
          this.db.prepare(`
            UPDATE shadow_price_tracking SET price_1h = ?, pnl_1h = ? WHERE id = ?
          `).run(currentPrice, pnl, signal.id);
          console.log(`   📊 [Shadow] ${signal.tokenCA.substring(0, 8)}... 1hour: ${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}%`);
        }
        
      } catch (error) {
        // 静默处理错误，继续追踪其他信号
      }
    }
  }

  /**
   * 完成追踪并更新信号源表现
   */
  async completeTracking(trackingId, signal) {
    const now = Math.floor(Date.now() / 1000);
    
    // 获取追踪数据
    const trackingData = this.db.prepare(`
      SELECT * FROM shadow_price_tracking WHERE id = ?
    `).get(signal.id);
    
    if (trackingData) {
      // 使用 15 分钟 PnL 作为主要评估指标
      const finalPnl = trackingData.pnl_15m || trackingData.pnl_5m || 0;
      const maxPnl = trackingData.max_pnl || 0;
      
      // 更新信号源表现
      if (signal.sourceType && signal.sourceId && signal.signalOutcomeId) {
        const isWinner = finalPnl > 0 ? 1 : 0;
        const exitPrice = trackingData.price_15m || trackingData.price_5m || signal.entryPrice;
        
        // 更新 signal_outcomes 表
        this.db.prepare(`
          UPDATE signal_outcomes SET
            exit_price = ?,
            exit_time = ?,
            exit_reason = 'shadow_15m',
            pnl_percent = ?,
            is_winner = ?,
            max_gain_percent = ?
          WHERE id = ?
        `).run(exitPrice, now, finalPnl, isWinner, maxPnl, signal.signalOutcomeId);
        
        // 触发信号源表现更新
        if (this.sourceOptimizer) {
          this.sourceOptimizer.updateSourcePerformance(signal.sourceType, signal.sourceId);
        }
      }
      
      // 标记追踪完成
      this.db.prepare(`
        UPDATE shadow_price_tracking SET status = 'completed', completed_at = ? WHERE id = ?
      `).run(now, signal.id);
      
      console.log(`   📊 [Shadow] Completed tracking ${signal.tokenCA.substring(0, 8)}... Final PnL: ${finalPnl >= 0 ? '+' : ''}${finalPnl.toFixed(1)}%, Max: +${maxPnl.toFixed(1)}%`);
    }
    
    // 从内存中移除
    this.trackedSignals.delete(trackingId);
  }

  /**
   * 获取追踪统计
   */
  getStats() {
    const stats = this.db.prepare(`
      SELECT 
        COUNT(*) as total,
        AVG(pnl_15m) as avg_pnl_15m,
        AVG(max_pnl) as avg_max_pnl,
        SUM(CASE WHEN pnl_15m > 0 THEN 1 ELSE 0 END) as winners,
        SUM(CASE WHEN pnl_15m <= 0 THEN 1 ELSE 0 END) as losers
      FROM shadow_price_tracking
      WHERE status = 'completed'
    `).get();
    
    return {
      total: stats.total || 0,
      avgPnl15m: stats.avg_pnl_15m || 0,
      avgMaxPnl: stats.avg_max_pnl || 0,
      winRate: stats.total > 0 ? (stats.winners / stats.total) * 100 : 0,
      winners: stats.winners || 0,
      losers: stats.losers || 0
    };
  }

  /**
   * 获取按信号源分组的统计
   */
  getStatsBySource() {
    return this.db.prepare(`
      SELECT 
        source_id,
        COUNT(*) as total,
        AVG(pnl_15m) as avg_pnl,
        AVG(max_pnl) as avg_max_pnl,
        SUM(CASE WHEN pnl_15m > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
      FROM shadow_price_tracking
      WHERE status = 'completed' AND source_id IS NOT NULL
      GROUP BY source_id
      ORDER BY avg_pnl DESC
    `).all();
  }
}

export default ShadowPriceTracker;
