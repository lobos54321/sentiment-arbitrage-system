/**
 * Hunter Performance Tracker v7.1
 *
 * Closed-loop performance tracking for hunter profiles
 * Tracks actual trade outcomes by hunter type to optimize future signals
 *
 * Features:
 * - Per-hunter-type win rate tracking (Fox, Turtle, Wolf, Eagle)
 * - Dynamic weight adjustment based on recent performance
 * - Signal source attribution
 * - Rolling 7-day / 30-day statistics
 */

import { HUNTER_TYPES } from '../inputs/ultra-human-sniper-v2.js';

export class HunterPerformanceTracker {
  constructor(db) {
    this.db = db;

    // Performance cache (updated periodically)
    this.performanceCache = {
      FOX: { wins: 0, losses: 0, totalPnL: 0, avgPnL: 0, trades: 0 },
      TURTLE: { wins: 0, losses: 0, totalPnL: 0, avgPnL: 0, trades: 0 },
      WOLF: { wins: 0, losses: 0, totalPnL: 0, avgPnL: 0, trades: 0 },
      EAGLE: { wins: 0, losses: 0, totalPnL: 0, avgPnL: 0, trades: 0 },
      BOT: { wins: 0, losses: 0, totalPnL: 0, avgPnL: 0, trades: 0 },
      NORMAL: { wins: 0, losses: 0, totalPnL: 0, avgPnL: 0, trades: 0 },
      UNKNOWN: { wins: 0, losses: 0, totalPnL: 0, avgPnL: 0, trades: 0 }
    };

    // Dynamic weight adjustments based on performance
    this.dynamicWeights = {
      FOX: 1.0,
      TURTLE: 1.0,
      WOLF: 1.0,
      EAGLE: 0.8,  // Default lower for Eagle (too fast for 60s polling)
      BOT: 0.3,
      NORMAL: 0.5,
      UNKNOWN: 0.5
    };

    // Configuration
    this.config = {
      ROLLING_DAYS: 7,              // Calculate stats over last 7 days
      MIN_TRADES_FOR_ADJUSTMENT: 5, // Minimum trades to adjust weights
      WEIGHT_MIN: 0.2,              // Minimum weight multiplier
      WEIGHT_MAX: 1.5,              // Maximum weight multiplier
      PERFORMANCE_UPDATE_INTERVAL: 30 * 60 * 1000  // Update every 30 minutes
    };

    this._initializeSchema();
    this._loadPerformance();

    // Schedule periodic updates
    setInterval(() => this._loadPerformance(), this.config.PERFORMANCE_UPDATE_INTERVAL);

    console.log('[HunterPerformance] 🎯 Hunter Performance Tracker initialized');
  }

  /**
   * Initialize database schema for tracking
   */
  _initializeSchema() {
    try {
      // Add hunter_type column to positions if not exists
      const columns = this.db.prepare("PRAGMA table_info(positions)").all();
      const hasHunterType = columns.some(col => col.name === 'hunter_type');

      if (!hasHunterType) {
        this.db.prepare(`
          ALTER TABLE positions ADD COLUMN hunter_type TEXT DEFAULT 'UNKNOWN'
        `).run();
        console.log('[HunterPerformance] Added hunter_type column to positions table');
      }

      // Create hunter_performance table for aggregated stats
      this.db.prepare(`
        CREATE TABLE IF NOT EXISTS hunter_performance (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          hunter_type TEXT NOT NULL,
          period TEXT NOT NULL,
          wins INTEGER DEFAULT 0,
          losses INTEGER DEFAULT 0,
          total_pnl_percent REAL DEFAULT 0,
          avg_pnl_percent REAL DEFAULT 0,
          best_trade_pnl REAL DEFAULT 0,
          worst_trade_pnl REAL DEFAULT 0,
          avg_hold_time_minutes REAL DEFAULT 0,
          calculated_at INTEGER NOT NULL,
          UNIQUE(hunter_type, period)
        )
      `).run();

      // Create hunter_signals table for signal attribution
      this.db.prepare(`
        CREATE TABLE IF NOT EXISTS hunter_signals (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          hunter_address TEXT NOT NULL,
          hunter_type TEXT NOT NULL,
          token_ca TEXT NOT NULL,
          signal_time INTEGER NOT NULL,
          position_id INTEGER,
          outcome TEXT,
          pnl_percent REAL,
          created_at INTEGER DEFAULT (strftime('%s', 'now'))
        )
      `).run();

    } catch (error) {
      console.error('[HunterPerformance] Schema initialization error:', error.message);
    }
  }

  /**
   * Load performance data from database
   */
  _loadPerformance() {
    try {
      const rollingDays = this.config.ROLLING_DAYS;

      // Get performance by hunter type for rolling period
      const stats = this.db.prepare(`
        SELECT
          COALESCE(hunter_type, 'UNKNOWN') as hunter_type,
          COUNT(*) as trades,
          SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
          SUM(CASE WHEN pnl_percent <= 0 THEN 1 ELSE 0 END) as losses,
          SUM(pnl_percent) as total_pnl,
          AVG(pnl_percent) as avg_pnl,
          MAX(pnl_percent) as best_trade,
          MIN(pnl_percent) as worst_trade
        FROM positions
        WHERE status = 'closed'
        AND exit_time > datetime('now', '-${rollingDays} days')
        GROUP BY hunter_type
      `).all();

      // Update cache
      for (const row of stats) {
        const type = row.hunter_type || 'UNKNOWN';
        if (this.performanceCache[type]) {
          this.performanceCache[type] = {
            wins: row.wins || 0,
            losses: row.losses || 0,
            totalPnL: row.total_pnl || 0,
            avgPnL: row.avg_pnl || 0,
            trades: row.trades || 0,
            bestTrade: row.best_trade || 0,
            worstTrade: row.worst_trade || 0
          };
        }
      }

      // Recalculate dynamic weights
      this._updateDynamicWeights();

      console.log('[HunterPerformance] 📊 Performance data loaded');
      this._logPerformanceSummary();

    } catch (error) {
      console.error('[HunterPerformance] Load performance error:', error.message);
    }
  }

  /**
   * Update dynamic weights based on recent performance
   */
  _updateDynamicWeights() {
    const minTrades = this.config.MIN_TRADES_FOR_ADJUSTMENT;
    const minWeight = this.config.WEIGHT_MIN;
    const maxWeight = this.config.WEIGHT_MAX;

    // Calculate baseline (average win rate across all types)
    let totalWins = 0, totalLosses = 0;
    for (const type of Object.keys(this.performanceCache)) {
      const perf = this.performanceCache[type];
      totalWins += perf.wins;
      totalLosses += perf.losses;
    }
    const baselineWinRate = totalWins / Math.max(1, totalWins + totalLosses);

    // Adjust weights for each hunter type
    for (const type of Object.keys(this.performanceCache)) {
      const perf = this.performanceCache[type];

      if (perf.trades < minTrades) {
        // Not enough data, use default
        continue;
      }

      const winRate = perf.wins / Math.max(1, perf.trades);
      const avgPnL = perf.avgPnL;

      // Weight adjustment formula:
      // - Win rate above baseline → increase weight
      // - Positive avg PnL → increase weight
      // - Combination of both
      let weightMultiplier = 1.0;

      // Win rate factor: +/- 30% based on deviation from baseline
      if (baselineWinRate > 0) {
        const winRateDeviation = (winRate - baselineWinRate) / baselineWinRate;
        weightMultiplier += winRateDeviation * 0.3;
      }

      // PnL factor: +/- 20% based on avg PnL
      if (avgPnL > 0) {
        weightMultiplier += Math.min(0.2, avgPnL / 100);
      } else if (avgPnL < 0) {
        weightMultiplier += Math.max(-0.2, avgPnL / 100);
      }

      // Clamp to bounds
      this.dynamicWeights[type] = Math.max(minWeight, Math.min(maxWeight, weightMultiplier));
    }
  }

  /**
   * Log performance summary
   */
  _logPerformanceSummary() {
    console.log('\n[HunterPerformance] ═══════════════════════════════════════════════════════');
    console.log('[HunterPerformance] 📊 Last 7 Days Performance:');

    for (const [type, perf] of Object.entries(this.performanceCache)) {
      if (perf.trades === 0) continue;

      const winRate = (perf.wins / perf.trades * 100).toFixed(1);
      const weight = this.dynamicWeights[type].toFixed(2);
      const emoji = HUNTER_TYPES[type]?.emoji || '❓';

      console.log(`   ${emoji} ${type.padEnd(7)}: ${perf.trades} trades | WR: ${winRate}% | AvgPnL: ${perf.avgPnL.toFixed(1)}% | Weight: ${weight}x`);
    }
    console.log('[HunterPerformance] ═══════════════════════════════════════════════════════\n');
  }

  /**
   * Record a signal from a hunter
   * @param {string} hunterAddress - Hunter wallet address
   * @param {string} hunterType - Hunter type (FOX, TURTLE, etc.)
   * @param {string} tokenCa - Token contract address
   */
  recordSignal(hunterAddress, hunterType, tokenCa) {
    try {
      this.db.prepare(`
        INSERT INTO hunter_signals (hunter_address, hunter_type, token_ca, signal_time)
        VALUES (?, ?, ?, strftime('%s', 'now'))
      `).run(hunterAddress, hunterType || 'UNKNOWN', tokenCa);
    } catch (error) {
      // Ignore duplicates
    }
  }

  /**
   * Link a position to its originating hunter signal
   * @param {number} positionId - Position ID
   * @param {string} hunterType - Hunter type
   * @param {string} tokenCa - Token contract address
   */
  linkPositionToHunter(positionId, hunterType, tokenCa) {
    try {
      // Update position with hunter type
      this.db.prepare(`
        UPDATE positions SET hunter_type = ? WHERE id = ?
      `).run(hunterType || 'UNKNOWN', positionId);

      // Link signal to position
      this.db.prepare(`
        UPDATE hunter_signals
        SET position_id = ?
        WHERE token_ca = ? AND position_id IS NULL
        ORDER BY signal_time DESC
        LIMIT 1
      `).run(positionId, tokenCa);

    } catch (error) {
      console.error('[HunterPerformance] Link position error:', error.message);
    }
  }

  /**
   * Record trade outcome for a position
   * @param {number} positionId - Position ID
   * @param {number} pnlPercent - PnL percentage
   */
  recordOutcome(positionId, pnlPercent) {
    try {
      const outcome = pnlPercent > 0 ? 'WIN' : 'LOSS';

      this.db.prepare(`
        UPDATE hunter_signals
        SET outcome = ?, pnl_percent = ?
        WHERE position_id = ?
      `).run(outcome, pnlPercent, positionId);

    } catch (error) {
      console.error('[HunterPerformance] Record outcome error:', error.message);
    }
  }

  /**
   * Get dynamic weight for a hunter type
   * @param {string} hunterType - Hunter type
   * @returns {number} Weight multiplier
   */
  getWeight(hunterType) {
    return this.dynamicWeights[hunterType] || this.dynamicWeights.UNKNOWN;
  }

  /**
   * Get performance stats for a hunter type
   * @param {string} hunterType - Hunter type
   * @returns {object} Performance stats
   */
  getPerformance(hunterType) {
    return this.performanceCache[hunterType] || this.performanceCache.UNKNOWN;
  }

  /**
   * Get all performance stats
   * @returns {object} All performance stats
   */
  getAllPerformance() {
    return {
      stats: this.performanceCache,
      weights: this.dynamicWeights,
      config: this.config
    };
  }

  /**
   * Adjust signal score based on hunter performance
   * @param {number} baseScore - Base signal score
   * @param {string} hunterType - Hunter type
   * @returns {number} Adjusted score
   */
  adjustScore(baseScore, hunterType) {
    const weight = this.getWeight(hunterType);
    const perf = this.getPerformance(hunterType);

    // Apply weight
    let adjustedScore = baseScore * weight;

    // Additional adjustments based on recent performance
    if (perf.trades >= this.config.MIN_TRADES_FOR_ADJUSTMENT) {
      // If hunter type has been performing well recently, boost score
      const winRate = perf.wins / perf.trades;
      if (winRate > 0.6) {
        adjustedScore *= 1.1;  // 10% boost for >60% win rate
      } else if (winRate < 0.3) {
        adjustedScore *= 0.8;  // 20% penalty for <30% win rate
      }
    }

    return Math.round(adjustedScore);
  }

  /**
   * Get best performing hunter types
   * @param {number} minTrades - Minimum trades to qualify
   * @returns {Array} Sorted hunter types by performance
   */
  getBestPerformers(minTrades = 3) {
    const performers = [];

    for (const [type, perf] of Object.entries(this.performanceCache)) {
      if (perf.trades < minTrades) continue;

      const winRate = perf.wins / perf.trades;
      const score = winRate * 50 + (perf.avgPnL > 0 ? perf.avgPnL : 0);

      performers.push({
        type,
        emoji: HUNTER_TYPES[type]?.emoji || '❓',
        trades: perf.trades,
        winRate,
        avgPnL: perf.avgPnL,
        score,
        weight: this.dynamicWeights[type]
      });
    }

    return performers.sort((a, b) => b.score - a.score);
  }

  /**
   * Force refresh performance data
   */
  refresh() {
    this._loadPerformance();
  }

  /**
   * Get status for monitoring
   */
  getStatus() {
    const bestPerformers = this.getBestPerformers();

    return {
      cacheAge: 'live',
      totalTracked: Object.values(this.performanceCache).reduce((sum, p) => sum + p.trades, 0),
      bestPerformers: bestPerformers.slice(0, 3),
      dynamicWeights: this.dynamicWeights
    };
  }
}

export default HunterPerformanceTracker;
