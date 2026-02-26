/**
 * Position Sizer with Cooldown Controls
 *
 * Implements position sizing and trading constraints:
 * - Same token cooldown (30min)
 * - Same narrative limit (max 3 concurrent)
 * - Max concurrent positions (10)
 * - Max daily trades (50)
 * - Capital allocation across chains
 *
 * Critical Safety: ALWAYS check cooldowns before allowing trade
 */

export class PositionSizer {
  constructor(config, db) {
    this.config = config;
    this.db = db;
    this.cooldowns = config.cooldowns;
    this.limits = config.position_limits;
    this.capital = {
      SOL: parseFloat(config.total_capital_sol) || 10.0,
      BSC: parseFloat(config.total_capital_bnb) || 1.0
    };
  }

  /**
   * Main entry: Check if position can be opened
   *
   * @param {Object} decision - Decision from DecisionMatrix
   * @param {Object} tokenData - Token metadata (symbol, name, narrative)
   * @returns {Object} {allowed, reason, adjusted_size, constraints}
   */
  async canOpenPosition(decision, tokenData) {
    console.log(`🔍 [Position Sizer] Checking constraints for ${tokenData.token_ca}`);

    // Check 1: Auto Buy enabled?
    if (!decision.auto_buy_enabled) {
      return {
        allowed: false,
        reason: 'Auto Buy disabled - manual approval required',
        adjusted_size: null,
        constraints: {auto_buy: false}
      };
    }

    // Check 2: Action is AUTO_BUY or BUY_WITH_CONFIRM?
    if (decision.action === 'REJECT' || decision.action === 'WATCH') {
      return {
        allowed: false,
        reason: `Action is ${decision.action} - no buy signal`,
        adjusted_size: null,
        constraints: {action: decision.action}
      };
    }

    // Check 3: Same token cooldown (30min)
    const tokenCooldown = await this.checkTokenCooldown(tokenData.token_ca);
    if (!tokenCooldown.allowed) {
      return {
        allowed: false,
        reason: tokenCooldown.reason,
        adjusted_size: null,
        constraints: {token_cooldown: tokenCooldown}
      };
    }

    // Check 4: Same narrative limit (max 3 concurrent)
    const narrativeLimit = await this.checkNarrativeLimit(tokenData);
    if (!narrativeLimit.allowed) {
      return {
        allowed: false,
        reason: narrativeLimit.reason,
        adjusted_size: null,
        constraints: {narrative_limit: narrativeLimit}
      };
    }

    // Check 5: Max concurrent positions (10)
    const concurrentLimit = await this.checkConcurrentLimit(decision.chain);
    if (!concurrentLimit.allowed) {
      return {
        allowed: false,
        reason: concurrentLimit.reason,
        adjusted_size: null,
        constraints: {concurrent_limit: concurrentLimit}
      };
    }

    // Check 6: Max daily trades (50)
    const dailyLimit = await this.checkDailyTradeLimit();
    if (!dailyLimit.allowed) {
      return {
        allowed: false,
        reason: dailyLimit.reason,
        adjusted_size: null,
        constraints: {daily_limit: dailyLimit}
      };
    }

    // Check 7: Capital availability
    const capitalCheck = await this.checkCapitalAvailability(
      decision.chain,
      decision.position_size
    );

    if (!capitalCheck.allowed) {
      return {
        allowed: false,
        reason: capitalCheck.reason,
        adjusted_size: capitalCheck.available_size,
        constraints: {capital: capitalCheck}
      };
    }

    // All checks passed
    return {
      allowed: true,
      reason: 'All constraints satisfied',
      adjusted_size: decision.position_size,
      constraints: {
        token_cooldown: tokenCooldown,
        narrative_limit: narrativeLimit,
        concurrent_limit: concurrentLimit,
        daily_limit: dailyLimit,
        capital: capitalCheck
      }
    };
  }

  /**
   * Check same token cooldown (30min from last trade)
   */
  async checkTokenCooldown(tokenCA) {
    try {
      const stmt = this.db.prepare(`
        SELECT MAX(timestamp) as last_trade
        FROM trades
        WHERE token_ca = ?
          AND action IN ('BUY', 'SELL')
      `);

      const result = stmt.get(tokenCA);
      const lastTrade = result?.last_trade;

      if (!lastTrade) {
        return {allowed: true, last_trade: null};
      }

      const timeSince = Date.now() - lastTrade;
      const cooldownMs = this.cooldowns.same_token_min * 60 * 1000;

      if (timeSince < cooldownMs) {
        const remainingMin = Math.ceil((cooldownMs - timeSince) / 60000);
        return {
          allowed: false,
          reason: `Same token cooldown: ${remainingMin} min remaining`,
          last_trade: lastTrade,
          remaining_ms: cooldownMs - timeSince
        };
      }

      return {allowed: true, last_trade: lastTrade};
    } catch (error) {
      console.error('❌ [Position Sizer] Token cooldown check failed:', error.message);
      // Fail safe - deny on error
      return {allowed: false, reason: 'Cooldown check failed - safety block'};
    }
  }

  /**
   * Check same narrative limit (max 3 concurrent positions)
   */
  async checkNarrativeLimit(tokenData) {
    try {
      // Extract narrative from token name/symbol
      const narrative = this.extractNarrative(tokenData);

      if (!narrative) {
        return {allowed: true, narrative: null, current_count: 0};
      }

      // Count open positions with same narrative
      const stmt = this.db.prepare(`
        SELECT COUNT(*) as count
        FROM trades
        WHERE status = 'OPEN'
          AND narrative = ?
      `);

      const result = stmt.get(narrative);
      const currentCount = result?.count || 0;

      if (currentCount >= this.cooldowns.same_narrative_max_concurrent) {
        return {
          allowed: false,
          reason: `Narrative limit reached: ${currentCount}/${this.cooldowns.same_narrative_max_concurrent} ${narrative} positions open`,
          narrative,
          current_count: currentCount
        };
      }

      return {allowed: true, narrative, current_count: currentCount};
    } catch (error) {
      console.error('❌ [Position Sizer] Narrative limit check failed:', error.message);
      return {allowed: false, reason: 'Narrative check failed - safety block'};
    }
  }

  /**
   * Extract narrative from token metadata
   */
  extractNarrative(tokenData) {
    const symbol = (tokenData.symbol || '').toUpperCase();
    const name = (tokenData.name || '').toUpperCase();

    const narratives = ['AI', 'AGENT', 'MEME', 'TRUMP', 'ELON', 'DEFI', 'GAMEFI', 'NFT'];

    for (const narrative of narratives) {
      if (symbol.includes(narrative) || name.includes(narrative)) {
        return narrative;
      }
    }

    return null;
  }

  /**
   * Check max concurrent positions limit
   */
  async checkConcurrentLimit(chain) {
    try {
      const stmt = this.db.prepare(`
        SELECT COUNT(*) as count
        FROM trades
        WHERE status = 'OPEN'
          AND chain = ?
      `);

      const result = stmt.get(chain);
      const currentCount = result?.count || 0;

      if (currentCount >= this.limits.max_concurrent_positions) {
        return {
          allowed: false,
          reason: `Max concurrent positions reached: ${currentCount}/${this.limits.max_concurrent_positions}`,
          current_count: currentCount
        };
      }

      return {allowed: true, current_count: currentCount};
    } catch (error) {
      console.error('❌ [Position Sizer] Concurrent limit check failed:', error.message);
      return {allowed: false, reason: 'Concurrent check failed - safety block'};
    }
  }

  /**
   * Check daily trade limit
   */
  async checkDailyTradeLimit() {
    try {
      const oneDayAgo = Date.now() - 24 * 60 * 60 * 1000;

      const stmt = this.db.prepare(`
        SELECT COUNT(*) as count
        FROM trades
        WHERE timestamp > ?
          AND action = 'BUY'
      `);

      const result = stmt.get(oneDayAgo);
      const dailyCount = result?.count || 0;

      if (dailyCount >= this.limits.max_daily_trades) {
        return {
          allowed: false,
          reason: `Daily trade limit reached: ${dailyCount}/${this.limits.max_daily_trades}`,
          daily_count: dailyCount
        };
      }

      return {allowed: true, daily_count: dailyCount};
    } catch (error) {
      console.error('❌ [Position Sizer] Daily limit check failed:', error.message);
      return {allowed: false, reason: 'Daily limit check failed - safety block'};
    }
  }

  /**
   * Check capital availability and adjust if needed
   */
  async checkCapitalAvailability(chain, requestedSize) {
    try {
      // Get current allocated capital (sum of open positions)
      const stmt = this.db.prepare(`
        SELECT COALESCE(SUM(position_size), 0) as allocated
        FROM trades
        WHERE status = 'OPEN'
          AND chain = ?
      `);

      const result = stmt.get(chain);
      const allocated = result?.allocated || 0;

      const totalCapital = this.capital[chain];
      const available = totalCapital - allocated;

      if (available <= 0) {
        return {
          allowed: false,
          reason: `No capital available on ${chain} (${allocated}/${totalCapital} allocated)`,
          allocated,
          total: totalCapital,
          available: 0,
          available_size: null
        };
      }

      if (requestedSize > available) {
        // Can still trade but with reduced size
        return {
          allowed: true, // Allow with adjustment
          reason: `Capital limited: using ${available.toFixed(4)} ${chain} (requested ${requestedSize})`,
          allocated,
          total: totalCapital,
          available,
          available_size: available
        };
      }

      return {
        allowed: true,
        allocated,
        total: totalCapital,
        available,
        available_size: requestedSize
      };
    } catch (error) {
      console.error('❌ [Position Sizer] Capital check failed:', error.message);
      return {allowed: false, reason: 'Capital check failed - safety block'};
    }
  }

  /**
   * Reserve capital for a position (to be called before trade execution)
   */
  async reserveCapital(tokenCA, chain, size) {
    // This is implicitly handled by creating the trade record
    // The checkCapitalAvailability will see OPEN positions and exclude that capital
    console.log(`💰 [Position Sizer] Reserving ${size} ${chain} for ${tokenCA}`);
    return true;
  }

  /**
   * Release capital when position closes
   */
  async releaseCapital(tokenCA) {
    // This is implicitly handled by updating trade status to CLOSED
    console.log(`💰 [Position Sizer] Releasing capital for ${tokenCA}`);
    return true;
  }

  /**
   * Get current capital status
   */
  async getCapitalStatus() {
    try {
      const status = {};

      for (const chain of ['SOL', 'BSC']) {
        const stmt = this.db.prepare(`
          SELECT COALESCE(SUM(position_size), 0) as allocated
          FROM trades
          WHERE status = 'OPEN'
            AND chain = ?
        `);

        const result = stmt.get(chain);
        const allocated = result?.allocated || 0;
        const total = this.capital[chain];

        status[chain] = {
          total,
          allocated,
          available: total - allocated,
          utilization: total > 0 ? (allocated / total * 100).toFixed(1) + '%' : '0%'
        };
      }

      return status;
    } catch (error) {
      console.error('❌ [Position Sizer] Failed to get capital status:', error.message);
      return null;
    }
  }

  /**
   * Get cooldown status summary
   */
  async getCooldownStatus() {
    try {
      // Get recent trades for visualization
      const stmt = this.db.prepare(`
        SELECT
          token_ca,
          chain,
          timestamp,
          action
        FROM trades
        WHERE timestamp > ?
        ORDER BY timestamp DESC
        LIMIT 20
      `);

      const thirtyMinAgo = Date.now() - 30 * 60 * 1000;
      const recentTrades = stmt.all(thirtyMinAgo);

      return {
        recent_trades: recentTrades.length,
        window: '30min',
        trades: recentTrades
      };
    } catch (error) {
      console.error('❌ [Position Sizer] Failed to get cooldown status:', error.message);
      return null;
    }
  }
}

export default PositionSizer;
