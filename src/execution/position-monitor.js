/**
 * Position Monitor - Exit Strategy and Risk Management
 *
 * Three-tier exit strategy:
 * 1. Risk Exit - Immediate sell on critical chain risks
 * 2. Sentiment Decay - Exit when TG acceleration declines
 * 3. Standard SOP - Time-based and profit-target exits
 *
 * Monitoring frequency: Every 1-3 minutes per position
 */

import Database from 'better-sqlite3';
import { SolanaSnapshotService } from '../inputs/chain-snapshot-sol.js';
import { BSCSnapshotService } from '../inputs/chain-snapshot-bsc.js';
import { TelegramService } from '../inputs/telegram-signals.js';
import { GMGNTelegramExecutor } from './gmgn-telegram-executor.js';

export class PositionMonitor {
  constructor(config, db) {
    this.config = config;
    this.db = db;

    // Services
    this.solService = new SolanaSnapshotService(config);
    this.bscService = new BSCSnapshotService(config);
    this.telegramService = new TelegramService(config, db);
    this.executor = new GMGNTelegramExecutor(config, db);

    // Monitor settings
    this.pollIntervalMs = config.POSITION_MONITOR_INTERVAL_MS || 120000; // 2 minutes
    this.isRunning = false;

    // Exit thresholds
    this.thresholds = {
      // Risk exit triggers (immediate)
      MAX_TOP10_INCREASE: 15, // Top10 concentration increase >15%
      MAX_SLIPPAGE_DETERIORATION: 3.0, // Slippage multiplied by 3x
      CRITICAL_WALLET_DUMP_PERCENT: 30, // Key wallet dumps >30% of holdings

      // Sentiment decay triggers
      TG_ACCEL_DECAY_THRESHOLD: 0.5, // Exit if tg_accel drops below entry value * 0.5
      TG_ACCEL_NEGATIVE: -10, // Exit if acceleration turns deeply negative

      // Standard SOP
      MAX_HOLD_TIME_MINUTES: 180, // 3 hours max hold
      PROFIT_TARGET_1: 0.30, // +30% → sell 50%
      PROFIT_TARGET_2: 0.50, // +50% → sell remaining 50%
      STOP_LOSS: -0.20 // -20% → exit all
    };

    console.log('📊 Position Monitor initialized');
    console.log(`   Poll interval: ${this.pollIntervalMs / 1000}s`);
  }

  /**
   * Start monitoring loop
   */
  async start() {
    if (this.isRunning) {
      console.log('⚠️  Position Monitor already running');
      return;
    }

    this.isRunning = true;
    console.log('▶️  Position Monitor started');

    // Initial monitoring pass
    await this.monitorAllPositions();

    // Set up recurring monitoring
    this.monitorInterval = setInterval(async () => {
      try {
        await this.monitorAllPositions();
      } catch (error) {
        console.error('❌ Monitor loop error:', error.message);
      }
    }, this.pollIntervalMs);
  }

  /**
   * Stop monitoring loop
   */
  stop() {
    if (this.monitorInterval) {
      clearInterval(this.monitorInterval);
      this.monitorInterval = null;
    }
    this.isRunning = false;
    console.log('⏹️  Position Monitor stopped');
  }

  /**
   * Monitor all open positions
   */
  async monitorAllPositions() {
    try {
      const positions = this.db.prepare(`
        SELECT * FROM positions
        WHERE status = 'open'
        ORDER BY entry_time ASC
      `).all();

      if (positions.length === 0) {
        return;
      }

      console.log(`\n🔍 Monitoring ${positions.length} open position(s)...`);

      for (const position of positions) {
        await this.monitorPosition(position);
      }

    } catch (error) {
      console.error('❌ Monitor all positions error:', error.message);
    }
  }

  /**
   * Monitor individual position
   */
  async monitorPosition(position) {
    const chain = position.chain;
    const tokenCA = position.token_ca;
    const symbol = position.symbol || tokenCA.substring(0, 8);

    try {
      console.log(`\n📍 [${chain}] ${symbol} | Entry: $${position.entry_price?.toFixed(8)} | Size: ${position.position_size_native?.toFixed(4)}`);

      // 1. Get current chain snapshot
      const snapshot = await this.getChainSnapshot(chain, tokenCA);

      if (!snapshot) {
        console.log(`   ⚠️  Failed to get snapshot, skip this cycle`);
        return;
      }

      // 2. Get current TG sentiment
      const tgData = await this.getTelegramSentiment(tokenCA);

      // 3. Calculate P&L
      const pnl = this.calculatePnL(position, snapshot.current_price);

      // 4. Check exit triggers (priority order)
      const exitDecision = this.evaluateExitTriggers(position, snapshot, tgData, pnl);

      // 5. Execute exit if triggered
      if (exitDecision.shouldExit) {
        await this.executeExit(position, exitDecision, snapshot.current_price, pnl);
      } else {
        console.log(`   ✅ Hold - P&L: ${pnl.pnl_percent > 0 ? '+' : ''}${pnl.pnl_percent.toFixed(2)}% | ${exitDecision.reason}`);
      }

    } catch (error) {
      console.error(`❌ Monitor position error [${symbol}]:`, error.message);
    }
  }

  /**
   * Get current chain snapshot
   */
  async getChainSnapshot(chain, tokenCA) {
    try {
      const service = chain === 'SOL' ? this.solService : this.bscService;
      const snapshot = await service.getSnapshot(tokenCA);

      if (!snapshot || !snapshot.current_price) {
        return null;
      }

      return snapshot;
    } catch (error) {
      console.error(`❌ Get snapshot error:`, error.message);
      return null;
    }
  }

  /**
   * Get current Telegram sentiment
   */
  async getTelegramSentiment(tokenCA) {
    try {
      // Get recent messages (last 5 minutes)
      const messages = this.db.prepare(`
        SELECT * FROM telegram_signals
        WHERE token_ca = ?
        AND timestamp > datetime('now', '-5 minutes')
        ORDER BY timestamp DESC
      `).all(tokenCA);

      if (messages.length === 0) {
        return { tg_accel: 0, message_count: 0 };
      }

      // Calculate current acceleration
      const recentCount = messages.length;
      const tg_accel = recentCount * 12; // messages per hour (5min window * 12)

      return {
        tg_accel,
        message_count: recentCount,
        latest_message_time: messages[0].timestamp
      };

    } catch (error) {
      console.error('❌ Get TG sentiment error:', error.message);
      return { tg_accel: 0, message_count: 0 };
    }
  }

  /**
   * Calculate P&L
   */
  calculatePnL(position, currentPrice) {
    const entryPrice = position.entry_price || 0;
    const positionSize = position.position_size_native || 0;
    const chain = position.chain;

    if (entryPrice === 0 || currentPrice === 0) {
      return {
        current_price: currentPrice,
        entry_price: entryPrice,
        pnl_percent: 0,
        pnl_native: 0,
        pnl_usd: 0
      };
    }

    const pnlPercent = ((currentPrice - entryPrice) / entryPrice) * 100;
    const pnlNative = (currentPrice - entryPrice) * positionSize / entryPrice;

    // Rough USD conversion (would use real price feed in production)
    const nativeToUSD = chain === 'SOL' ? 100 : 300; // SOL ≈ $100, BNB ≈ $300
    const pnlUSD = pnlNative * nativeToUSD;

    return {
      current_price: currentPrice,
      entry_price: entryPrice,
      pnl_percent: pnlPercent,
      pnl_native: pnlNative,
      pnl_usd: pnlUSD
    };
  }

  /**
   * Evaluate exit triggers (three-tier strategy)
   */
  evaluateExitTriggers(position, snapshot, tgData, pnl) {
    const exitReason = [];
    let shouldExit = false;
    let exitType = 'hold';
    let sellPercent = 0;

    // ======================
    // TIER 1: RISK EXIT (Immediate, 100%)
    // ======================

    // 1.1 Key wallet dump
    const keyWalletDumps = this.checkKeyWalletDumps(position, snapshot);
    if (keyWalletDumps.detected) {
      shouldExit = true;
      exitType = 'risk_exit';
      sellPercent = 100;
      exitReason.push(`Key wallet dumped ${keyWalletDumps.dump_percent.toFixed(1)}%`);
    }

    // 1.2 Top10 concentration increase
    const top10Change = snapshot.top10_holders - (position.entry_top10_holders || 0);
    if (top10Change > this.thresholds.MAX_TOP10_INCREASE) {
      shouldExit = true;
      exitType = 'risk_exit';
      sellPercent = 100;
      exitReason.push(`Top10 concentration +${top10Change.toFixed(1)}%`);
    }

    // 1.3 Slippage deterioration
    const slippageMultiplier = snapshot.slippage_bps_1sol / (position.entry_slippage_bps || 1);
    if (slippageMultiplier > this.thresholds.MAX_SLIPPAGE_DETERIORATION) {
      shouldExit = true;
      exitType = 'risk_exit';
      sellPercent = 100;
      exitReason.push(`Slippage deteriorated ${slippageMultiplier.toFixed(1)}x`);
    }

    // ======================
    // TIER 2: SENTIMENT DECAY (Full exit)
    // ======================

    if (!shouldExit) {
      const entryTgAccel = position.entry_tg_accel || 0;
      const currentTgAccel = tgData.tg_accel || 0;

      // 2.1 TG acceleration decay
      if (entryTgAccel > 0 && currentTgAccel < entryTgAccel * this.thresholds.TG_ACCEL_DECAY_THRESHOLD) {
        shouldExit = true;
        exitType = 'sentiment_decay';
        sellPercent = 100;
        exitReason.push(`TG accel decayed: ${currentTgAccel.toFixed(1)} < ${(entryTgAccel * this.thresholds.TG_ACCEL_DECAY_THRESHOLD).toFixed(1)}`);
      }

      // 2.2 Negative acceleration
      if (currentTgAccel < this.thresholds.TG_ACCEL_NEGATIVE) {
        shouldExit = true;
        exitType = 'sentiment_decay';
        sellPercent = 100;
        exitReason.push(`TG accel deeply negative: ${currentTgAccel.toFixed(1)}`);
      }
    }

    // ======================
    // TIER 3: STANDARD SOP (Time/Profit-based)
    // ======================

    if (!shouldExit) {
      // 3.1 Stop loss
      if (pnl.pnl_percent < this.thresholds.STOP_LOSS * 100) {
        shouldExit = true;
        exitType = 'stop_loss';
        sellPercent = 100;
        exitReason.push(`Stop loss: ${pnl.pnl_percent.toFixed(2)}%`);
      }

      // 3.2 Profit targets
      if (pnl.pnl_percent >= this.thresholds.PROFIT_TARGET_2 * 100) {
        shouldExit = true;
        exitType = 'profit_target';
        sellPercent = position.remaining_percent || 100; // Sell remaining
        exitReason.push(`Profit target 2: +${pnl.pnl_percent.toFixed(2)}%`);
      } else if (pnl.pnl_percent >= this.thresholds.PROFIT_TARGET_1 * 100 && !position.partial_exit_1) {
        shouldExit = true;
        exitType = 'profit_target';
        sellPercent = 50; // Sell half
        exitReason.push(`Profit target 1: +${pnl.pnl_percent.toFixed(2)}%`);
      }

      // 3.3 Max hold time
      const holdTimeMinutes = this.getHoldTimeMinutes(position.entry_time);
      if (holdTimeMinutes > this.thresholds.MAX_HOLD_TIME_MINUTES) {
        shouldExit = true;
        exitType = 'max_hold_time';
        sellPercent = 100;
        exitReason.push(`Max hold time: ${holdTimeMinutes.toFixed(0)}min`);
      }
    }

    return {
      shouldExit,
      exitType,
      sellPercent,
      reason: exitReason.length > 0 ? exitReason.join(', ') : 'All checks passed'
    };
  }

  /**
   * Check for key wallet dumps
   */
  checkKeyWalletDumps(position, snapshot) {
    // Compare entry risk wallets with current
    const entryRiskWallets = JSON.parse(position.entry_risk_wallets || '[]');
    const currentRiskWallets = snapshot.risk_wallets || [];

    for (const entryWallet of entryRiskWallets) {
      const currentWallet = currentRiskWallets.find(w => w.address === entryWallet.address);

      if (!currentWallet) {
        // Wallet completely dumped
        return { detected: true, dump_percent: 100, wallet: entryWallet.address };
      }

      // Check if wallet reduced holdings significantly
      const balanceChange = ((currentWallet.balance - entryWallet.balance) / entryWallet.balance) * 100;
      if (balanceChange < -this.thresholds.CRITICAL_WALLET_DUMP_PERCENT) {
        return {
          detected: true,
          dump_percent: Math.abs(balanceChange),
          wallet: entryWallet.address
        };
      }
    }

    return { detected: false };
  }

  /**
   * Get hold time in minutes
   */
  getHoldTimeMinutes(entryTime) {
    const entry = new Date(entryTime);
    const now = new Date();
    return (now - entry) / 1000 / 60; // minutes
  }

  /**
   * Execute exit
   */
  async executeExit(position, exitDecision, currentPrice, pnl) {
    const { exitType, sellPercent, reason } = exitDecision;
    const symbol = position.symbol || position.token_ca.substring(0, 8);

    console.log(`\n🚨 EXIT TRIGGERED [${position.chain}] ${symbol}`);
    console.log(`   Type: ${exitType}`);
    console.log(`   Reason: ${reason}`);
    console.log(`   Sell: ${sellPercent}%`);
    console.log(`   P&L: ${pnl.pnl_percent > 0 ? '+' : ''}${pnl.pnl_percent.toFixed(2)}% (${pnl.pnl_native > 0 ? '+' : ''}${pnl.pnl_native.toFixed(4)} ${position.chain})`);

    try {
      // Execute sell via GMGN
      const sellResult = await this.executor.executeSell({
        chain: position.chain,
        token_ca: position.token_ca,
        sell_percent: sellPercent,
        position_id: position.id
      });

      if (sellResult.success) {
        // Update position in database
        if (sellPercent === 100) {
          // Full exit
          this.db.prepare(`
            UPDATE positions
            SET status = 'closed',
                exit_time = datetime('now'),
                exit_price = ?,
                exit_type = ?,
                pnl_percent = ?,
                pnl_native = ?,
                pnl_usd = ?,
                exit_tx_hash = ?
            WHERE id = ?
          `).run(
            currentPrice,
            exitType,
            pnl.pnl_percent,
            pnl.pnl_native,
            pnl.pnl_usd,
            sellResult.tx_hash || null,
            position.id
          );

          console.log(`   ✅ Position closed - Exit recorded`);

        } else {
          // Partial exit (profit target 1)
          this.db.prepare(`
            UPDATE positions
            SET partial_exit_1 = 1,
                remaining_percent = ?,
                partial_exit_1_time = datetime('now'),
                partial_exit_1_price = ?
            WHERE id = ?
          `).run(
            100 - sellPercent,
            currentPrice,
            position.id
          );

          console.log(`   ✅ Partial exit - ${100 - sellPercent}% remaining`);
        }

        // Send notification
        await this.sendExitNotification(position, exitDecision, pnl, sellResult);

      } else {
        console.error(`   ❌ Exit execution failed: ${sellResult.error}`);
      }

    } catch (error) {
      console.error(`   ❌ Exit execution error:`, error.message);
    }
  }

  /**
   * Send exit notification via Telegram
   */
  async sendExitNotification(position, exitDecision, pnl, sellResult) {
    const adminChatId = process.env.TELEGRAM_ADMIN_CHAT_ID;
    if (!adminChatId) return;

    const symbol = position.symbol || position.token_ca.substring(0, 8);
    const { exitType, sellPercent, reason } = exitDecision;

    const message = `
🔴 **EXIT EXECUTED**

**Token**: ${symbol} (${position.chain})
**Exit Type**: ${exitType}
**Sell**: ${sellPercent}%

**P&L**: ${pnl.pnl_percent > 0 ? '+' : ''}${pnl.pnl_percent.toFixed(2)}%
**P&L (${position.chain})**: ${pnl.pnl_native > 0 ? '+' : ''}${pnl.pnl_native.toFixed(4)}
**P&L (USD)**: ${pnl.pnl_usd > 0 ? '+' : ''}$${pnl.pnl_usd.toFixed(2)}

**Reason**: ${reason}

**Entry**: $${position.entry_price?.toFixed(8)}
**Exit**: $${pnl.current_price?.toFixed(8)}
**Hold Time**: ${this.getHoldTimeMinutes(position.entry_time).toFixed(0)}min

**TX**: \`${sellResult.tx_hash || 'Pending'}\`
    `.trim();

    try {
      const TelegramBot = (await import('node-telegram-bot-api')).default;
      const bot = new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: false });
      await bot.sendMessage(adminChatId, message, { parse_mode: 'Markdown' });
    } catch (error) {
      console.error('❌ Send notification error:', error.message);
    }
  }

  /**
   * Get monitor status
   */
  getStatus() {
    const positions = this.db.prepare(`
      SELECT
        COUNT(*) as total_open,
        SUM(CASE WHEN chain = 'SOL' THEN 1 ELSE 0 END) as sol_positions,
        SUM(CASE WHEN chain = 'BSC' THEN 1 ELSE 0 END) as bsc_positions
      FROM positions
      WHERE status = 'open'
    `).get();

    return {
      is_running: this.isRunning,
      poll_interval_seconds: this.pollIntervalMs / 1000,
      open_positions: positions.total_open,
      sol_positions: positions.sol_positions,
      bsc_positions: positions.bsc_positions
    };
  }
}
