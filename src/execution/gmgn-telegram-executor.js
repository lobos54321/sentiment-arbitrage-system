/**
 * GMGN Telegram Bot Executor
 *
 * 通过 Telegram 自动化控制 GMGN Bot 执行交易
 *
 * 方案：脚本 → Telegram API → GMGN Bot → 交易执行
 *
 * 优势：
 * - 无需 GMGN API 白名单
 * - Anti-MEV 保护内置
 * - 私钥由 GMGN 托管（避免本地管理风险）
 * - 实现简单快速
 *
 * 前置要求：
 * 1. 在 GMGN Telegram Bot 中开启 Auto Buy/Sell
 * 2. 获取 Telegram API credentials (api_id, api_hash)
 * 3. 设置好 GMGN Bot 的交易参数
 */

import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import input from 'input';

export class GMGNTelegramExecutor {
  constructor(config, db) {
    this.config = config;
    this.db = db;

    // Telegram API credentials (from https://my.telegram.org)
    this.apiId = parseInt(process.env.TELEGRAM_API_ID || '0');
    this.apiHash = process.env.TELEGRAM_API_HASH || '';
    this.sessionString = process.env.TELEGRAM_SESSION || '';

    // GMGN Bot usernames
    this.gmgnBots = {
      SOL: '@GMGN_sol_bot',
      BSC: '@GMGN_bsc_bot'
    };

    this.shadowMode = process.env.SHADOW_MODE === 'true';
    this.client = null;
    this.isInitialized = false;
  }

  /**
   * Initialize Telegram client
   */
  async initialize() {
    if (this.isInitialized) return;

    console.log('🔗 [GMGN Telegram] Initializing Telegram client...');

    if (!this.apiId || !this.apiHash) {
      console.error('❌ [GMGN Telegram] Missing TELEGRAM_API_ID or TELEGRAM_API_HASH');
      console.error('Please get them from https://my.telegram.org/apps');
      throw new Error('Telegram API credentials not configured');
    }

    const session = new StringSession(this.sessionString);
    this.client = new TelegramClient(session, this.apiId, this.apiHash, {
      connectionRetries: 5,
    });

    await this.client.start({
      phoneNumber: async () => await input.text('Please enter your phone number: '),
      password: async () => await input.text('Please enter your password: '),
      phoneCode: async () => await input.text('Please enter the code you received: '),
      onError: (err) => console.error('❌ [GMGN Telegram] Login error:', err),
    });

    // Save session string for next time
    const savedSession = this.client.session.save();
    console.log('💾 [GMGN Telegram] Save this session string to .env as TELEGRAM_SESSION:');
    console.log(savedSession);

    this.isInitialized = true;
    console.log('✅ [GMGN Telegram] Client initialized successfully');
  }

  /**
   * Execute buy order via GMGN Bot
   */
  async executeBuy(tradeParams, tokenData) {
    console.log(`💰 [GMGN Telegram] Executing BUY for ${tradeParams.chain}/${tradeParams.token_ca}`);

    // Pre-flight checks
    const preflightCheck = await this.preflightCheck(tradeParams, tokenData);
    if (!preflightCheck.allowed) {
      console.log(`⚠️ [GMGN Telegram] Pre-flight check failed: ${preflightCheck.reason}`);
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

    try {
      // Initialize client if needed
      if (!this.isInitialized) {
        await this.initialize();
      }

      const botUsername = this.gmgnBots[tradeParams.chain];

      // Send token CA to GMGN Bot
      // Bot will auto-buy if Auto Buy is enabled with preset amount
      await this.client.sendMessage(botUsername, {
        message: tradeParams.token_ca
      });

      console.log(`✅ [GMGN Telegram] Buy signal sent to ${botUsername}`);

      // Wait for confirmation (optional - can be improved with message listening)
      await this.sleep(2000);

      // Get latest message from bot to confirm
      const confirmation = await this.getLatestBotMessage(botUsername);

      // Persist trade record
      const tradeId = await this.persistTrade({
        ...tradeParams,
        ...tokenData,
        tx_hash: confirmation?.tx_hash || 'PENDING',
        executed_price: confirmation?.price || null,
        tokens_received: confirmation?.tokens || null,
        bot_username: botUsername
      });

      return {
        success: true,
        trade_id: tradeId,
        method: 'telegram_bot',
        bot: botUsername,
        tx_hash: confirmation?.tx_hash,
        message: 'Buy signal sent to GMGN Bot - check Telegram for confirmation'
      };

    } catch (error) {
      console.error('❌ [GMGN Telegram] Buy execution failed:', error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Execute sell order via GMGN Bot
   */
  async executeSell(tradeId, exitReason) {
    console.log(`💸 [GMGN Telegram] Executing SELL for trade ${tradeId}`);

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

    try {
      if (!this.isInitialized) {
        await this.initialize();
      }

      const botUsername = this.gmgnBots[trade.chain];

      // Send sell command to GMGN Bot
      // Format: /sell {token_ca} or use button interface
      await this.client.sendMessage(botUsername, {
        message: `/sell ${trade.token_ca}`
      });

      console.log(`✅ [GMGN Telegram] Sell signal sent to ${botUsername}`);

      // Wait for confirmation
      await this.sleep(2000);

      const confirmation = await this.getLatestBotMessage(botUsername);

      // Update trade record
      await this.closeTradeRecord(tradeId, confirmation, exitReason);

      return {
        success: true,
        method: 'telegram_bot',
        bot: botUsername,
        tx_hash: confirmation?.tx_hash,
        message: 'Sell signal sent to GMGN Bot - check Telegram for confirmation'
      };

    } catch (error) {
      console.error('❌ [GMGN Telegram] Sell execution failed:', error.message);
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

    // Check 2: Telegram client configured
    if (!this.apiId || !this.apiHash) {
      return {
        allowed: false,
        reason: 'Telegram API credentials not configured'
      };
    }

    // Check 3: Verify position size is valid
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
      const url = `https://api.dexscreener.com/latest/dex/tokens/${tokenCA}`;
      const response = await fetch(url);
      const data = await response.json();

      if (!data.pairs || data.pairs.length === 0) {
        return {is_surging: false};
      }

      const pair = data.pairs[0];
      const priceChange5m = pair.priceChange?.m5;

      if (priceChange5m === null || priceChange5m === undefined) {
        return {is_surging: false};
      }

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
      console.error('❌ Price surge check failed:', error.message);
      return {is_surging: false};
    }
  }

  /**
   * Get latest message from GMGN Bot (for confirmation parsing)
   */
  async getLatestBotMessage(botUsername) {
    try {
      const messages = await this.client.getMessages(botUsername, { limit: 1 });

      if (!messages || messages.length === 0) {
        return null;
      }

      const message = messages[0];
      const text = message.message || '';

      // Parse confirmation (adjust regex based on actual bot response format)
      const txHashMatch = text.match(/tx[:\s]+([a-zA-Z0-9]{64,88})/i);
      const priceMatch = text.match(/price[:\s]+\$?([\d.]+)/i);
      const tokensMatch = text.match(/tokens?[:\s]+([\d,.]+)/i);

      return {
        tx_hash: txHashMatch ? txHashMatch[1] : null,
        price: priceMatch ? parseFloat(priceMatch[1]) : null,
        tokens: tokensMatch ? parseFloat(tokensMatch[1].replace(/,/g, '')) : null,
        raw_message: text
      };
    } catch (error) {
      console.error('❌ Failed to get bot message:', error.message);
      return null;
    }
  }

  /**
   * Simulate buy for shadow mode
   */
  async simulateBuy(tradeParams, tokenData) {
    console.log(`🎭 [GMGN Telegram] SHADOW MODE - Simulating buy`);

    const fakeTxHash = `SHADOW_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

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
   * Simulate sell for shadow mode
   */
  async simulateSell(tradeId, exitReason) {
    console.log(`🎭 [GMGN Telegram] SHADOW MODE - Simulating sell`);

    const fakeTxHash = `SHADOW_SELL_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

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

    return {
      success: true,
      tx_hash: fakeTxHash,
      simulation: true,
      message: 'Shadow mode - no real trade executed'
    };
  }

  /**
   * Persist trade record
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

      console.log(`✅ [GMGN Telegram] Trade persisted: ID ${info.lastInsertRowid}`);
      return info.lastInsertRowid;
    } catch (error) {
      console.error('❌ Failed to persist trade:', error.message);
      return null;
    }
  }

  /**
   * Close trade record
   */
  async closeTradeRecord(tradeId, sellResult, exitReason) {
    try {
      const stmt = this.db.prepare(`
        UPDATE trades
        SET
          status = 'CLOSED',
          exit_timestamp = ?,
          exit_tx_hash = ?,
          exit_reason = ?
        WHERE id = ?
      `);

      stmt.run(
        Date.now(),
        sellResult?.tx_hash || 'PENDING',
        exitReason,
        tradeId
      );

      console.log(`✅ [GMGN Telegram] Trade closed: ID ${tradeId}`);
    } catch (error) {
      console.error('❌ Failed to close trade:', error.message);
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
      console.error('❌ Failed to get trade:', error.message);
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
      console.error('❌ Failed to get open positions:', error.message);
      return [];
    }
  }

  /**
   * Utility: Sleep
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Disconnect client
   */
  async disconnect() {
    if (this.client && this.isInitialized) {
      await this.client.disconnect();
      this.isInitialized = false;
      console.log('👋 [GMGN Telegram] Client disconnected');
    }
  }
}

export default GMGNTelegramExecutor;
