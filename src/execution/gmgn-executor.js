/**
 * GMGN Executor
 *
 * Interface for executing trades through GMGN API
 *
 * Key Features:
 * - SOL买入（Anti-MEV模式）
 * - BSC买入（滑点10-20%）
 * - 追高检查（5分钟+50% → 不追）
 * - Shadow模式支持
 * - 交易记录持久化
 *
 * GMGN API Documentation:
 * https://gmgn.ai/defi/quotation/v1/docs
 */

import fetch from 'node-fetch';

export class GMGNExecutor {
  constructor(config, db) {
    this.config = config;
    this.db = db;
    this.apiKey = process.env.GMGN_API_KEY || '';
    this.walletAddress = process.env.GMGN_WALLET_ADDRESS || '';
    this.shadowMode = process.env.SHADOW_MODE === 'true';
    this.baseURL = 'https://gmgn.ai/defi/quotation/v1';
  }

  /**
   * Main entry: Execute buy order
   *
   * @param {Object} tradeParams - Trade parameters
   *   {
   *     token_ca, chain, position_size, rating, decision
   *   }
   * @param {Object} tokenData - Token metadata for tracking
   * @returns {Object} {success, trade_id, tx_hash, error}
   */
  async executeBuy(tradeParams, tokenData) {
    console.log(`💰 [GMGN Executor] Executing BUY for ${tradeParams.chain}/${tradeParams.token_ca}`);

    // Pre-flight checks
    const preflightCheck = await this.preflightCheck(tradeParams, tokenData);
    if (!preflightCheck.allowed) {
      console.log(`⚠️ [GMGN Executor] Pre-flight check failed: ${preflightCheck.reason}`);
      return {
        success: false,
        error: preflightCheck.reason,
        preflight_failed: true
      };
    }

    // Shadow mode - simulate only
    if (this.shadowMode) {
      return this.simulateBuy(tradeParams, tokenData);
    }

    // Real execution
    try {
      if (tradeParams.chain === 'SOL') {
        return await this.executeSolanaBuy(tradeParams, tokenData);
      } else if (tradeParams.chain === 'BSC') {
        return await this.executeBSCBuy(tradeParams, tokenData);
      } else {
        return {
          success: false,
          error: 'Unsupported chain'
        };
      }
    } catch (error) {
      console.error('❌ [GMGN Executor] Buy execution failed:', error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Pre-flight checks before buying
   */
  async preflightCheck(tradeParams, tokenData) {
    // Check 1: Price surge check (avoid chasing pumps)
    const surgeCheck = await this.checkPriceSurge(tradeParams.token_ca, tradeParams.chain);
    if (surgeCheck.is_surging) {
      return {
        allowed: false,
        reason: surgeCheck.reason
      };
    }

    // Check 2: Wallet configured
    if (!this.walletAddress) {
      return {
        allowed: false,
        reason: 'GMGN wallet address not configured'
      };
    }

    // Check 3: API Key configured (if not shadow mode)
    if (!this.shadowMode && !this.apiKey) {
      return {
        allowed: false,
        reason: 'GMGN API key not configured'
      };
    }

    // Check 4: Verify position size is valid
    if (!tradeParams.position_size || tradeParams.position_size <= 0) {
      return {
        allowed: false,
        reason: 'Invalid position size'
      };
    }

    return {allowed: true};
  }

  /**
   * Check for price surge (avoid chasing +50% in 5min)
   */
  async checkPriceSurge(tokenCA, chain) {
    try {
      // Fetch recent price data from DexScreener
      const url = `https://api.dexscreener.com/latest/dex/tokens/${tokenCA}`;
      const response = await fetch(url);
      const data = await response.json();

      if (!data.pairs || data.pairs.length === 0) {
        return {is_surging: false}; // No data - allow
      }

      const pair = data.pairs[0];
      const priceChange5m = pair.priceChange?.m5;

      if (priceChange5m === null || priceChange5m === undefined) {
        return {is_surging: false}; // No 5min data - allow
      }

      // Reject if +50% or more in 5min
      if (priceChange5m >= 50) {
        return {
          is_surging: true,
          reason: `Price surged ${priceChange5m.toFixed(1)}% in 5min - avoiding chase`,
          price_change_5m: priceChange5m
        };
      }

      return {
        is_surging: false,
        price_change_5m: priceChange5m
      };
    } catch (error) {
      console.error('❌ [GMGN Executor] Price surge check failed:', error.message);
      // On error, allow trade (fail open)
      return {is_surging: false};
    }
  }

  /**
   * Execute Solana buy (Anti-MEV mode)
   */
  async executeSolanaBuy(tradeParams, tokenData) {
    console.log(`🟣 [GMGN Executor] Executing Solana buy: ${tradeParams.position_size} SOL`);

    // GMGN Solana buy endpoint
    const endpoint = `${this.baseURL}/sol/buy`;

    const payload = {
      wallet: this.walletAddress,
      token: tradeParams.token_ca,
      amount: tradeParams.position_size,
      slippage: 10, // 10% slippage (conservative)
      priority_fee: 'medium', // Anti-MEV setting
      mode: 'anti_mev' // Prevent frontrunning
    };

    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this.apiKey}`
        },
        body: JSON.stringify(payload)
      });

      const result = await response.json();

      if (response.ok && result.success) {
        // Persist trade record
        const tradeId = await this.persistTrade({
          ...tradeParams,
          ...tokenData,
          tx_hash: result.tx_hash,
          executed_price: result.execution_price,
          tokens_received: result.tokens_received,
          actual_slippage: result.slippage
        });

        console.log(`✅ [GMGN Executor] Solana buy successful: ${result.tx_hash}`);

        return {
          success: true,
          trade_id: tradeId,
          tx_hash: result.tx_hash,
          execution_price: result.execution_price,
          tokens_received: result.tokens_received,
          slippage: result.slippage
        };
      } else {
        console.error('❌ [GMGN Executor] Solana buy failed:', result.error);
        return {
          success: false,
          error: result.error || 'Unknown GMGN error'
        };
      }
    } catch (error) {
      console.error('❌ [GMGN Executor] Solana buy request failed:', error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Execute BSC buy (higher slippage tolerance)
   */
  async executeBSCBuy(tradeParams, tokenData) {
    console.log(`🟡 [GMGN Executor] Executing BSC buy: ${tradeParams.position_size} BNB`);

    // GMGN BSC buy endpoint
    const endpoint = `${this.baseURL}/bsc/buy`;

    const payload = {
      wallet: this.walletAddress,
      token: tradeParams.token_ca,
      amount: tradeParams.position_size,
      slippage: 20, // 20% slippage (BSC needs higher tolerance)
      gas_price: 'medium'
    };

    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this.apiKey}`
        },
        body: JSON.stringify(payload)
      });

      const result = await response.json();

      if (response.ok && result.success) {
        // Persist trade record
        const tradeId = await this.persistTrade({
          ...tradeParams,
          ...tokenData,
          tx_hash: result.tx_hash,
          executed_price: result.execution_price,
          tokens_received: result.tokens_received,
          actual_slippage: result.slippage
        });

        console.log(`✅ [GMGN Executor] BSC buy successful: ${result.tx_hash}`);

        return {
          success: true,
          trade_id: tradeId,
          tx_hash: result.tx_hash,
          execution_price: result.execution_price,
          tokens_received: result.tokens_received,
          slippage: result.slippage
        };
      } else {
        console.error('❌ [GMGN Executor] BSC buy failed:', result.error);
        return {
          success: false,
          error: result.error || 'Unknown GMGN error'
        };
      }
    } catch (error) {
      console.error('❌ [GMGN Executor] BSC buy request failed:', error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Simulate buy for shadow mode
   */
  async simulateBuy(tradeParams, tokenData) {
    console.log(`🎭 [GMGN Executor] SHADOW MODE - Simulating buy`);

    // Generate fake transaction hash
    const fakeTxHash = `SHADOW_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    // Persist simulated trade
    const tradeId = await this.persistTrade({
      ...tradeParams,
      ...tokenData,
      tx_hash: fakeTxHash,
      executed_price: null,
      tokens_received: null,
      actual_slippage: null,
      is_simulation: true
    });

    return {
      success: true,
      trade_id: tradeId,
      tx_hash: fakeTxHash,
      simulation: true,
      message: 'Shadow mode - no real trade executed'
    };
  }

  /**
   * Execute sell order
   */
  async executeSell(tradeId, exitReason) {
    console.log(`💸 [GMGN Executor] Executing SELL for trade ${tradeId}`);

    // Get trade record
    const trade = this.getTradeById(tradeId);

    if (!trade) {
      return {
        success: false,
        error: 'Trade not found'
      };
    }

    if (trade.status === 'CLOSED') {
      return {
        success: false,
        error: 'Trade already closed'
      };
    }

    // Shadow mode - simulate only
    if (this.shadowMode || trade.is_simulation) {
      return this.simulateSell(tradeId, exitReason);
    }

    // Real execution
    try {
      if (trade.chain === 'SOL') {
        return await this.executeSolanaSell(trade, exitReason);
      } else if (trade.chain === 'BSC') {
        return await this.executeBSCSell(trade, exitReason);
      }
    } catch (error) {
      console.error('❌ [GMGN Executor] Sell execution failed:', error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Execute Solana sell
   */
  async executeSolanaSell(trade, exitReason) {
    const endpoint = `${this.baseURL}/sol/sell`;

    const payload = {
      wallet: this.walletAddress,
      token: trade.token_ca,
      amount: 'all', // Sell entire position
      slippage: 15, // Higher slippage for exits
      priority_fee: 'high' // Fast exit
    };

    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.apiKey}`
      },
      body: JSON.stringify(payload)
    });

    const result = await response.json();

    if (response.ok && result.success) {
      // Update trade record
      await this.closeTradeRecord(trade.id, result, exitReason);

      console.log(`✅ [GMGN Executor] Solana sell successful: ${result.tx_hash}`);

      return {
        success: true,
        tx_hash: result.tx_hash,
        execution_price: result.execution_price,
        amount_received: result.amount_received,
        pnl: result.amount_received - trade.position_size,
        pnl_percent: ((result.amount_received - trade.position_size) / trade.position_size * 100).toFixed(2)
      };
    } else {
      return {
        success: false,
        error: result.error || 'Unknown GMGN error'
      };
    }
  }

  /**
   * Execute BSC sell
   */
  async executeBSCSell(trade, exitReason) {
    const endpoint = `${this.baseURL}/bsc/sell`;

    const payload = {
      wallet: this.walletAddress,
      token: trade.token_ca,
      amount: 'all',
      slippage: 25, // Very high slippage for emergency exits
      gas_price: 'high'
    };

    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.apiKey}`
      },
      body: JSON.stringify(payload)
    });

    const result = await response.json();

    if (response.ok && result.success) {
      await this.closeTradeRecord(trade.id, result, exitReason);

      console.log(`✅ [GMGN Executor] BSC sell successful: ${result.tx_hash}`);

      return {
        success: true,
        tx_hash: result.tx_hash,
        execution_price: result.execution_price,
        amount_received: result.amount_received,
        pnl: result.amount_received - trade.position_size,
        pnl_percent: ((result.amount_received - trade.position_size) / trade.position_size * 100).toFixed(2)
      };
    } else {
      return {
        success: false,
        error: result.error || 'Unknown GMGN error'
      };
    }
  }

  /**
   * Simulate sell for shadow mode
   */
  async simulateSell(tradeId, exitReason) {
    console.log(`🎭 [GMGN Executor] SHADOW MODE - Simulating sell`);

    const fakeTxHash = `SHADOW_SELL_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    // Get trade to calculate simulated PnL
    const trade = this.getTradeById(tradeId);

    // Update trade record
    const stmt = this.db.prepare(`
      UPDATE trades
      SET
        status = 'CLOSED',
        exit_timestamp = ?,
        exit_reason = ?,
        exit_tx_hash = ?,
        is_simulation = 1
      WHERE id = ?
    `);

    stmt.run(Date.now(), exitReason, fakeTxHash, tradeId);

    console.log(`✅ [GMGN Executor] Shadow sell recorded: ${fakeTxHash}`);

    return {
      success: true,
      tx_hash: fakeTxHash,
      simulation: true,
      message: 'Shadow mode - no real trade executed'
    };
  }

  /**
   * Persist trade record to database
   */
  async persistTrade(tradeData) {
    try {
      const stmt = this.db.prepare(`
        INSERT INTO trades (
          token_ca,
          chain,
          symbol,
          name,
          narrative,
          rating,
          action,
          position_tier,
          position_size,
          executed_price,
          tokens_received,
          actual_slippage,
          tx_hash,
          timestamp,
          status,
          is_simulation
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
      `);

      const info = stmt.run(
        tradeData.token_ca,
        tradeData.chain,
        tradeData.symbol || null,
        tradeData.name || null,
        tradeData.narrative || null,
        tradeData.rating,
        'BUY',
        tradeData.position_tier,
        tradeData.position_size,
        tradeData.executed_price || null,
        tradeData.tokens_received || null,
        tradeData.actual_slippage || null,
        tradeData.tx_hash,
        Date.now(),
        tradeData.is_simulation ? 1 : 0
      );

      console.log(`✅ [GMGN Executor] Trade persisted: ID ${info.lastInsertRowid}`);
      return info.lastInsertRowid;
    } catch (error) {
      console.error('❌ [GMGN Executor] Failed to persist trade:', error.message);
      return null;
    }
  }

  /**
   * Close trade record after sell
   */
  async closeTradeRecord(tradeId, sellResult, exitReason) {
    try {
      const stmt = this.db.prepare(`
        UPDATE trades
        SET
          status = 'CLOSED',
          exit_timestamp = ?,
          exit_price = ?,
          exit_amount_received = ?,
          exit_tx_hash = ?,
          exit_reason = ?,
          pnl = ?,
          pnl_percent = ?
        WHERE id = ?
      `);

      const pnl = sellResult.amount_received - sellResult.position_size;
      const pnlPercent = (pnl / sellResult.position_size * 100).toFixed(2);

      stmt.run(
        Date.now(),
        sellResult.execution_price,
        sellResult.amount_received,
        sellResult.tx_hash,
        exitReason,
        pnl,
        pnlPercent,
        tradeId
      );

      console.log(`✅ [GMGN Executor] Trade closed: ID ${tradeId}`);
    } catch (error) {
      console.error('❌ [GMGN Executor] Failed to close trade record:', error.message);
    }
  }

  /**
   * Get trade by ID
   */
  getTradeById(tradeId) {
    try {
      const stmt = this.db.prepare('SELECT * FROM trades WHERE id = ?');
      return stmt.get(tradeId);
    } catch (error) {
      console.error('❌ [GMGN Executor] Failed to get trade:', error.message);
      return null;
    }
  }

  /**
   * Get all open positions
   */
  getOpenPositions(chain = null) {
    try {
      let query = 'SELECT * FROM trades WHERE status = \'OPEN\'';
      let params = [];

      if (chain) {
        query += ' AND chain = ?';
        params.push(chain);
      }

      query += ' ORDER BY timestamp DESC';

      const stmt = this.db.prepare(query);
      return params.length > 0 ? stmt.all(...params) : stmt.all();
    } catch (error) {
      console.error('❌ [GMGN Executor] Failed to get open positions:', error.message);
      return [];
    }
  }
}

export default GMGNExecutor;
