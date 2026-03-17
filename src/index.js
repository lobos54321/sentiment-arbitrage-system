/**
 * Sentiment Arbitrage System - Main Entry Point
 * MVP 2.0 - Production-Ready On-Chain Sentiment Arbitrage
 *
 * Architecture:
 * 1. Telegram Signal Listener → Captures market signals
 * 2. Chain Snapshot → Real-time on-chain data (SOL/BSC)
 * 3. Hard Gates → Binary quality filters (liquidity, security, slippage)
 * 4. Soft Alpha Score → Multi-factor scoring (TG spread, holder quality, momentum)
 * 5. Decision Matrix → Buy/Greylist/Reject based on scores
 * 6. Position Sizer → Kelly-optimized position sizing
 * 7. GMGN Executor → Telegram Bot-based execution
 * 8. Position Monitor → Three-tier exit strategy
 */

import dotenv from 'dotenv';
import Database from 'better-sqlite3';
import { TelegramUserListener } from './inputs/telegram-user-listener.js';
import { SolanaSnapshotService } from './inputs/chain-snapshot-sol.js';
import { BSCSnapshotService } from './inputs/chain-snapshot-bsc.js';
import { HardGateFilter } from './gates/hard-gates.js';
import { SoftAlphaScorer } from './scoring/soft-alpha-score.js';
import { DecisionMatrix } from './decision/decision-matrix.js';
import { PositionSizer } from './decision/position-sizer.js';
import { GMGNTelegramExecutor } from './execution/gmgn-telegram-executor.js';
import { PositionMonitor } from './execution/position-monitor.js';
import GrokTwitterClient from './social/grok-twitter-client.js';
import { PermanentBlacklistService } from './database/permanent-blacklist.js';
import { PremiumChannelListener } from './inputs/premium-channel-listener.js';
import { PremiumSignalEngine } from './engines/premium-signal-engine.js';
import { JupiterUltraExecutor } from './execution/jupiter-ultra-executor.js';
import { LivePriceMonitor } from './tracking/live-price-monitor.js';
import { LivePriceMonitorV2 } from './tracking/live-price-monitor-v2.js';
import { startDashboardServer } from './web/dashboard-server.js';
import { LivePositionMonitor } from './execution/live-position-monitor.js';

dotenv.config();

// 全局兜底：防止 async EventEmitter 回调的未捕获 rejection 导致进程崩溃
process.on('unhandledRejection', (reason, promise) => {
  console.error('🔴 [GLOBAL] Unhandled Promise Rejection:', reason);
});

process.on('uncaughtException', (error) => {
  console.error('🔴 [GLOBAL] Uncaught Exception:', error);
  // 给一点时间让日志写完，然后退出（uncaughtException 后状态不可信）
  setTimeout(() => process.exit(1), 3000);
});

class SentimentArbitrageSystem {
  constructor() {
    this.config = this.loadConfig();
    this.db = new Database(this.config.DB_PATH);

    // Initialize services
    this.telegramService = new TelegramUserListener(this.config, this.db);
    this.solService = new SolanaSnapshotService(this.config);
    this.bscService = new BSCSnapshotService(this.config);
    this.hardGateService = new HardGateFilter(this.config);
    this.softScorer = new SoftAlphaScorer(this.config, this.db);
    this.decisionEngine = new DecisionMatrix(this.config, this.db);
    this.positionSizer = new PositionSizer(this.config, this.db);
    this.executor = new GMGNTelegramExecutor(this.config, this.db);
    this.positionMonitor = new PositionMonitor(this.config, this.db);
    this.grokClient = new GrokTwitterClient();
    this.blacklistService = new PermanentBlacklistService(this.db);

    // System state
    this.isRunning = false;
    this.processedSignals = new Set();
    this.stats = {
      signals_received: 0,
      hard_gate_passed: 0,
      soft_score_computed: 0,
      buy_decisions: 0,
      greylist_decisions: 0,
      reject_decisions: 0,
      executions_success: 0,
      executions_failed: 0
    };

    console.log('\n' + '═'.repeat(80));
    console.log('🤖 SENTIMENT ARBITRAGE SYSTEM v2.0');
    console.log('═'.repeat(80));
    console.log(`Mode: ${this.config.SHADOW_MODE ? '🎭 SHADOW' : '💰 LIVE'}`);
    console.log(`Auto Buy: ${this.config.AUTO_BUY_ENABLED ? '✅ Enabled' : '❌ Disabled'}`);
    console.log(`Database: ${this.config.DB_PATH}`);
    console.log('═'.repeat(80) + '\n');
  }

  /**
   * Load configuration from environment
   */
  loadConfig() {
    return {
      // Database
      DB_PATH: process.env.DB_PATH || './data/sentiment_arb.db',

      // System mode
      NODE_ENV: process.env.NODE_ENV || 'development',
      SHADOW_MODE: process.env.SHADOW_MODE === 'true',
      AUTO_BUY_ENABLED: process.env.AUTO_BUY_ENABLED === 'true',
      LOG_LEVEL: process.env.LOG_LEVEL || 'info',

      // Safety limits
      MAX_CONCURRENT_POSITIONS: parseInt(process.env.MAX_CONCURRENT_POSITIONS || '10'),
      MAX_DAILY_TRADES: parseInt(process.env.MAX_DAILY_TRADES || '50'),
      TOTAL_CAPITAL_SOL: parseFloat(process.env.TOTAL_CAPITAL_SOL || '10.0'),
      TOTAL_CAPITAL_BNB: parseFloat(process.env.TOTAL_CAPITAL_BNB || '1.0'),

      // Position monitor
      POSITION_MONITOR_INTERVAL_MS: 120000, // 2 minutes

      // Signal processing
      SIGNAL_POLL_INTERVAL_MS: 30000, // 30 seconds
      MIN_SIGNAL_INTERVAL_MS: 60000, // Don't reprocess same token within 1 minute

      // Soft score weights (total = 1.0)
      soft_score_weights: {
        Narrative: 0.25,
        Influence: 0.25,
        TG_Spread: 0.30,
        Graph: 0.10,
        Source: 0.10
      },

      // Soft score thresholds
      soft_score_thresholds: {
        tg_spread: {
          excellent_channels: 8,
          good_channels: 5,
          min_channels: 3,
          max_cluster_penalty: 20
        },
        holder_quality: {
          max_top10_concentration: 30,
          min_unique_holders: 100,
          risk_wallet_threshold: 50
        },
        momentum: {
          price_change_24h_min: 10,
          volume_increase_min: 2.0
        },
        security: {
          min_security_score: 60
        },
        x_validation: {
          min_unique_authors: 2,
          multiplier_below_threshold: 0.8
        }
      },

      // Hard gate thresholds
      hard_gate_thresholds: {
        SOL: {
          min_liquidity_usd: 10000,
          min_holders: 50,
          max_top10_percent: 50,
          max_slippage_bps: 200,
          max_tax_percent: 5
        },
        BSC: {
          min_liquidity_usd: 20000,
          min_holders: 100,
          max_top10_percent: 60,
          max_slippage_bps: 300,
          max_tax_percent: 5,
          owner_safe_types: ['Renounced', 'MultiSig', 'TimeLock', 'Burned']
        }
      },

      // Decision matrix configuration
      decision_matrix: {
        rules: [
          { score_min: 80, score_max: 100, rating: 'S', action: 'AUTO_BUY', position_tier: 'large' },
          { score_min: 65, score_max: 79, rating: 'A', action: 'AUTO_BUY', position_tier: 'medium' },
          { score_min: 50, score_max: 64, rating: 'B', action: 'AUTO_BUY', position_tier: 'small' },
          { score_min: 35, score_max: 49, rating: 'C', action: 'WATCH_ONLY', position_tier: null },
          { score_min: 0, score_max: 34, rating: 'F', action: 'REJECT', position_tier: null }
        ]
      },

      // Position size templates
      position_templates: {
        SOL: {
          large: { sol: 2.0, usd_approx: 200 },
          medium: { sol: 1.0, usd_approx: 100 },
          small: { sol: 0.5, usd_approx: 50 }
        },
        BSC: {
          large: { bnb: 0.5, usd_approx: 200 },
          medium: { bnb: 0.25, usd_approx: 100 },
          small: { bnb: 0.125, usd_approx: 50 }
        }
      },

      // Cooldown periods
      cooldowns: {
        same_token_minutes: 60,
        same_narrative_minutes: 30,
        failed_trade_minutes: 15
      },

      // Position limits
      position_limits: {
        max_concurrent: 10,
        max_daily_trades: 50,
        max_per_narrative: 3
      },

      // Capital allocation
      total_capital_sol: process.env.TOTAL_CAPITAL_SOL || '10.0',
      total_capital_bnb: process.env.TOTAL_CAPITAL_BNB || '1.0'
    };
  }

  /**
   * Start the system
   */
  async start() {
    try {
      console.log('▶️  Starting Sentiment Arbitrage System...\n');

      // 1. Start Telegram listener
      console.log('📱 Starting Telegram signal listener...');
      await this.telegramService.start();
      // Expose telegram service globally for API access
      global.__telegramService = this.telegramService;
      console.log('   ✅ Telegram listener active\n');

      // 2. Start position monitor
      console.log('📊 Starting position monitor...');
      await this.positionMonitor.start();
      console.log('   ✅ Position monitor active\n');

      // 3. Start signal processing loop
      this.isRunning = true;
      this.startSignalProcessingLoop();

      console.log('✅ System fully operational!\n');
      console.log('━'.repeat(80));
      console.log('Waiting for signals...\n');

    } catch (error) {
      console.error('❌ System startup failed:', error);
      throw error;
    }
  }

  /**
   * Signal processing loop
   */
  startSignalProcessingLoop() {
    this.signalInterval = setInterval(async () => {
      try {
        await this.processNewSignals();
      } catch (error) {
        console.error('❌ Signal processing error:', error.message);
      }
    }, this.config.SIGNAL_POLL_INTERVAL_MS);
  }

  /**
   * Process new signals from Telegram
   */
  async processNewSignals() {
    try {
      // Get unprocessed signals
      const signals = this.db.prepare(`
        SELECT * FROM telegram_signals
        WHERE processed = 0
        ORDER BY timestamp ASC
        LIMIT 10
      `).all();

      for (const signal of signals) {
        await this.processSignal(signal);
      }

    } catch (error) {
      console.error('❌ Process new signals error:', error.message);
    }
  }

  /**
   * Process individual signal through complete pipeline
   */
  async processSignal(signal) {
    const { id, token_ca, chain, channel_name } = signal;
    const symbol = token_ca.substring(0, 8);

    try {
      // Check if recently processed
      const cacheKey = `${chain}:${token_ca}`;
      if (this.processedSignals.has(cacheKey)) {
        const lastProcessed = this.processedSignals.get(cacheKey);
        if (Date.now() - lastProcessed < this.config.MIN_SIGNAL_INTERVAL_MS) {
          this.markSignalProcessed(id);
          return;
        }
      }

      console.log('\n' + '─'.repeat(80));
      console.log(`🔔 NEW SIGNAL: ${symbol} (${chain}) from ${channel_name}`);
      console.log('─'.repeat(80));

      this.stats.signals_received++;

      // ==========================================
      // STEP 0: PERMANENT BLACKLIST CHECK
      // ==========================================
      const blacklistRecord = this.blacklistService.isBlacklisted(token_ca, chain);
      if (blacklistRecord) {
        console.log(`\n🚫 [0/7] PERMANENT BLACKLIST HIT`);
        console.log(`   Token: ${chain}/${token_ca}`);
        console.log(`   Reason: ${blacklistRecord.blacklist_reason}`);
        console.log(`   Blacklisted: ${new Date(blacklistRecord.blacklist_timestamp).toISOString()}`);
        console.log(`   ❌ REJECTED - Permanent blacklist (不再处理)`);
        this.markSignalProcessed(id);
        this.stats.reject_decisions++;
        return;
      }

      // ==========================================
      // STEP 1: CHAIN SNAPSHOT + TOKEN METADATA
      // ==========================================
      console.log('\n📊 [1/7] Fetching chain snapshot...');
      const snapshot = await this.getChainSnapshot(chain, token_ca);

      if (!snapshot) {
        console.log('   ❌ Failed to get snapshot - REJECT');
        this.markSignalProcessed(id);
        this.stats.reject_decisions++;
        return;
      }

      console.log(`   ✅ Snapshot: Price=$${snapshot.current_price?.toFixed(10)}, Liquidity=$${(snapshot.liquidity_usd || 0).toFixed(0)}`);

      // Get Token Metadata (name, symbol, description) for Narrative detection
      let tokenMetadata = {
        token_ca,
        chain,
        name: null,
        symbol: symbol || null,  // Use signal symbol as fallback
        description: null
      };

      try {
        const service = chain === 'SOL' ? this.solService : this.bscService;

        // Only fetch metadata if service has getTokenMetadata method
        if (typeof service.getTokenMetadata === 'function') {
          const metadata = await service.getTokenMetadata(token_ca);
          tokenMetadata = {
            token_ca,
            chain,
            name: metadata.name || null,
            symbol: metadata.symbol || symbol || null,  // Fallback to signal symbol
            description: metadata.description || null
          };
        }
      } catch (error) {
        console.log(`   ⚠️  Token metadata fetch failed: ${error.message}`);
        // Continue with null metadata - Narrative score will be 0
      }

      // ==========================================
      // STEP 2: HARD GATES
      // ==========================================
      console.log('\n🚧 [2/7] Running hard gates...');
      const gateResult = await this.hardGateService.evaluate(snapshot, chain);

      // Handle REJECT status
      if (gateResult.status === 'REJECT') {
        const reasonText = (gateResult.reasons || []).join(', ') || 'Unknown reason';
        console.log(`   ❌ Hard gate REJECT: ${reasonText}`);
        this.markSignalProcessed(id);
        this.stats.reject_decisions++;
        return;
      }

      // Handle GREYLIST status
      if (gateResult.status === 'GREYLIST') {
        const reasonText = (gateResult.reasons || []).join(', ') || 'Unknown data';
        console.log(`   ⚠️  Hard gate GREYLIST: ${reasonText}`);
        // Continue processing but log as greylist
        this.stats.greylist_decisions++;
      } else {
        console.log(`   ✅ All hard gates passed (PASS)`);
        this.stats.hard_gate_passed++;
      }

      // ==========================================
      // STEP 3: SOFT ALPHA SCORE
      // ==========================================
      console.log('\n📈 [3/7] Computing soft alpha score...');

      // Collect Twitter data using Grok API
      let twitterData = null;
      try {
        console.log('   🐦 Searching Twitter via Grok API...');
        twitterData = await this.grokClient.searchToken(
          snapshot.symbol || token_ca.substring(0, 8),
          token_ca,
          15  // 15-minute window
        );
        console.log(`   ✅ Twitter: ${twitterData.mention_count} mentions, ${twitterData.engagement} engagement`);
      } catch (error) {
        console.log(`   ⚠️  Twitter search failed: ${error.message}`);
        // Continue without Twitter data
        twitterData = {
          mention_count: 0,
          unique_authors: 0,
          engagement: 0,
          sentiment: 'neutral',
          kol_count: 0
        };
      }

      // Prepare data structures for soft scorer
      const socialData = {
        // Telegram data
        total_mentions: 1,
        unique_channels: 1,
        channels: [signal.channel_name],
        message_timestamp: signal.timestamp,

        // Twitter data (from Grok API)
        twitter_mentions: twitterData.mention_count,
        twitter_unique_authors: twitterData.unique_authors,
        twitter_kol_count: twitterData.kol_count,
        twitter_engagement: twitterData.engagement,
        twitter_sentiment: twitterData.sentiment
      };

      // Use tokenMetadata (from Step 1) for Narrative detection
      // If metadata fetch failed, tokenMetadata will have null values
      const scoreResult = await this.softScorer.calculate(socialData, tokenMetadata);

      console.log(`   📊 Score: ${scoreResult.score}/100`);
      console.log(`   Components:`);
      console.log(`      - Narrative: ${scoreResult.breakdown.narrative.score.toFixed(1)}`);
      console.log(`      - Influence: ${scoreResult.breakdown.influence.score.toFixed(1)}`);
      console.log(`      - TG Spread: ${scoreResult.breakdown.tg_spread.score.toFixed(1)}`);
      console.log(`      - Graph: ${scoreResult.breakdown.graph.score.toFixed(1)}`);
      console.log(`      - Source: ${scoreResult.breakdown.source.score.toFixed(1)}`);

      this.stats.soft_score_computed++;

      // ==========================================
      // STEP 4: DECISION MATRIX
      // ==========================================
      console.log('\n🎯 [4/7] Making decision...');

      // Build evaluation object for decision engine
      const evaluation = {
        token_ca: token_ca,
        chain: chain,
        hard_gate: gateResult,
        exit_gate: { status: 'PASS', reasons: [] }, // Exit gate not yet implemented
        soft_score: scoreResult
      };

      const decision = this.decisionEngine.decide(evaluation);

      console.log(`   Decision: ${decision.action} (Rating: ${decision.rating})`);
      const reasonText = Array.isArray(decision.reasons) ? decision.reasons[0] : 'Unknown';
      console.log(`   Reason: ${reasonText}`);

      if (decision.action === 'REJECT') {
        console.log(`   ❌ Rejected`);
        this.markSignalProcessed(id);
        this.stats.reject_decisions++;
        return;
      }

      if (decision.action === 'WATCH_ONLY' || decision.action === 'WATCH') {
        console.log(`   ⚠️  Watch only - manual verification required`);
        this.markSignalProcessed(id);
        this.stats.greylist_decisions++;
        return;
      }

      // AUTO_BUY or BUY_WITH_CONFIRM
      if (decision.action === 'AUTO_BUY' || decision.action === 'BUY_WITH_CONFIRM') {
        console.log(`   ✅ BUY signal - proceeding to position sizing`);
        this.stats.buy_decisions++;
      } else {
        // Unexpected action - log warning
        console.log(`   ⚠️  Unexpected action: ${decision.action}`);
        this.markSignalProcessed(id);
        return;
      }

      // ==========================================
      // STEP 5: POSITION SIZING
      // ==========================================
      console.log('\n💰 [5/7] Calculating position size...');

      // Reuse tokenData from Step 3
      const positionCheck = await this.positionSizer.canOpenPosition(decision, tokenData);

      if (!positionCheck.allowed) {
        console.log(`   ❌ Cannot trade: ${positionCheck.reason}`);
        this.markSignalProcessed(id);
        return;
      }

      console.log(`   ✅ Position approved`);
      if (positionCheck.adjusted_size) {
        console.log(`      Size: ${positionCheck.adjusted_size.amount} ${chain}`);
        console.log(`      (~$${positionCheck.adjusted_size.usd_value} USD)`);
      }

      // ==========================================
      // STEP 6: EXECUTION
      // ==========================================
      console.log('\n⚡ [6/7] Executing trade...');

      if (!this.config.AUTO_BUY_ENABLED) {
        console.log(`   ⏸️  Auto-buy disabled - Skipping execution`);
        this.markSignalProcessed(id);
        return;
      }

      const tradeParams = {
        chain,
        token_ca,
        position_size: positionCheck.adjusted_size || decision.position_size,
        max_slippage_bps: 500, // 5%
        symbol: snapshot.symbol || 'Unknown'
      };

      const executionResult = await this.executor.executeBuy(tradeParams);

      if (executionResult.success) {
        console.log(`   ✅ Execution successful!`);
        console.log(`      Trade ID: ${executionResult.trade_id}`);
        console.log(`      Method: ${executionResult.method}`);
        if (executionResult.tx_hash) {
          console.log(`      TX: ${executionResult.tx_hash}`);
        }
        this.stats.executions_success++;

        // Record position
        this.recordPosition(signal, snapshot, scoreResult, positionSize, executionResult);

      } else {
        console.log(`   ❌ Execution failed: ${executionResult.error}`);
        this.stats.executions_failed++;
      }

      // ==========================================
      // STEP 7: MARK PROCESSED
      // ==========================================
      this.markSignalProcessed(id);
      this.processedSignals.set(cacheKey, Date.now());

      console.log('\n✅ Signal processing complete');
      console.log('─'.repeat(80) + '\n');

    } catch (error) {
      console.error(`❌ Process signal error [${symbol}]:`, error.message);
      this.markSignalProcessed(id);
    }
  }

  /**
   * Get chain snapshot
   */
  async getChainSnapshot(chain, tokenCA) {
    try {
      const service = chain === 'SOL' ? this.solService : this.bscService;
      return await service.getSnapshot(tokenCA);
    } catch (error) {
      console.error('❌ Get snapshot error:', error.message);
      return null;
    }
  }

  /**
   * Record position in database
   */
  recordPosition(signal, snapshot, scoreResult, positionSize, executionResult) {
    try {
      this.db.prepare(`
        INSERT INTO positions (
          chain, token_ca, symbol, signal_id,
          entry_time, entry_price, position_size_native, position_size_usd,
          alpha_score, confidence, kelly_fraction,
          entry_liquidity_usd, entry_top10_holders, entry_slippage_bps,
          entry_tg_accel, entry_risk_wallets,
          trade_id, entry_tx_hash, status
        ) VALUES (?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')
      `).run(
        signal.chain,
        signal.token_ca,
        snapshot.symbol || signal.token_ca.substring(0, 8),
        signal.id,
        snapshot.current_price,
        positionSize.position_size_native,
        positionSize.position_size_usd,
        scoreResult.final_score,
        positionSize.confidence,
        positionSize.kelly_fraction,
        snapshot.liquidity_usd,
        snapshot.top10_holders,
        snapshot.slippage_bps_1sol,
        scoreResult.breakdown.tg_accel || 0,
        JSON.stringify(snapshot.risk_wallets || []),
        executionResult.trade_id,
        executionResult.tx_hash || null
      );

      console.log('   ✅ Position recorded in database');

    } catch (error) {
      console.error('❌ Record position error:', error.message);
    }
  }

  /**
   * Mark signal as processed
   */
  markSignalProcessed(signalId) {
    try {
      this.db.prepare(`
        UPDATE telegram_signals
        SET processed = 1
        WHERE id = ?
      `).run(signalId);
    } catch (error) {
      console.error('❌ Mark processed error:', error.message);
    }
  }

  /**
   * Stop the system
   */
  async stop() {
    console.log('\n⏹️  Stopping Sentiment Arbitrage System...\n');

    this.isRunning = false;

    if (this.signalInterval) {
      clearInterval(this.signalInterval);
    }

    this.telegramService.stop();
    this.positionMonitor.stop();

    console.log('✅ System stopped\n');
    this.printStats();
  }

  /**
   * Print system statistics
   */
  printStats() {
    console.log('━'.repeat(80));
    console.log('📊 SESSION STATISTICS');
    console.log('━'.repeat(80));
    console.log(`Signals Received:      ${this.stats.signals_received}`);
    console.log(`Hard Gate Passed:      ${this.stats.hard_gate_passed}`);
    console.log(`Scores Computed:       ${this.stats.soft_score_computed}`);
    console.log(`Buy Decisions:         ${this.stats.buy_decisions}`);
    console.log(`Greylist Decisions:    ${this.stats.greylist_decisions}`);
    console.log(`Reject Decisions:      ${this.stats.reject_decisions}`);
    console.log(`Executions Success:    ${this.stats.executions_success}`);
    console.log(`Executions Failed:     ${this.stats.executions_failed}`);
    console.log('━'.repeat(80) + '\n');
  }

  /**
   * Get system status
   */
  getStatus() {
    return {
      is_running: this.isRunning,
      mode: this.config.SHADOW_MODE ? 'shadow' : 'live',
      auto_buy_enabled: this.config.AUTO_BUY_ENABLED,
      stats: this.stats,
      telegram_status: this.telegramService.getStatus(),
      monitor_status: this.positionMonitor.getStatus()
    };
  }
}

// ==========================================
// PREMIUM CHANNEL MODE
// ==========================================

class PremiumChannelSystem {
  constructor() {
    this.config = this.loadConfig();
    this.db = new Database(this.config.DB_PATH);
    this.listener = new PremiumChannelListener(this.config);
    this.engine = new PremiumSignalEngine(this.config, this.db);

    // 实盘组件（SHADOW_MODE=false 时启用）
    this.jupiterExecutor = null;
    this.livePriceMonitor = null;
    this.livePositionMonitor = null;

    const isLive = process.env.SHADOW_MODE === 'false';

    console.log('\n' + '═'.repeat(80));
    console.log('💎 PREMIUM CHANNEL MODE');
    console.log('═'.repeat(80));
    console.log(`Mode: ${isLive ? '💰 LIVE' : '🎭 SHADOW'}`);
    if (isLive) {
      console.log(`执行器: Jupiter Swap`);
      console.log(`仓位: ${process.env.PREMIUM_POSITION_SOL || '0.12'} SOL`);
      console.log(`RPC: ${process.env.SOLANA_RPC_URL || 'mainnet-beta (default)'}`);
    }
    console.log(`Channel ID: ${process.env.PREMIUM_CHANNEL_ID || '3636518327'}`);
    console.log('═'.repeat(80) + '\n');
  }

  loadConfig() {
    return {
      DB_PATH: process.env.DB_PATH || './data/sentiment_arb.db',
      SHADOW_MODE: process.env.SHADOW_MODE !== 'false',
      AUTO_BUY_ENABLED: process.env.AUTO_BUY_ENABLED === 'true',
      total_capital_sol: process.env.TOTAL_CAPITAL_SOL || '10.0',
      hard_gate_thresholds: {
        SOL: {
          freeze_authority: 'DISABLED',
          mint_authority: 'DISABLED',
          lp_required: 'BURNED_OR_LOCKED',
          lp_lock_min_days: 30
        }
      },
      exit_gate_thresholds: {
        SOL: {
          min_liquidity_sol: 50,
          max_top10_percent: 30,
          max_wash_with_risk: 'MEDIUM'
        }
      },
      exit_gate_slippage: {
        test_sell_percentage: 20,
        sol_reject_threshold_pct: 5,
        sol_pass_threshold_pct: 2
      },
      cooldowns: {
        same_token_cooldown_minutes: 30,
        same_narrative_max_concurrent: 3
      },
      position_limits: {
        max_concurrent: parseInt(process.env.PREMIUM_MAX_POSITIONS || '5'),
        max_daily_trades: 50
      }
    };
  }

  async start() {
    const isLive = process.env.SHADOW_MODE === 'false';
    const usePriceV2 = process.env.USE_PRICE_MONITOR_V2 === 'true';  // 新增配置开关

    // 实盘模式：额外初始化 Jupiter Executor + LivePositionMonitor
    if (isLive) {
      try {
        this.jupiterExecutor = new JupiterUltraExecutor();
        this.jupiterExecutor.initialize();

        // 选择价格监控器版本
        if (usePriceV2 && process.env.JUPITER_API_KEY) {
          console.log('📡 [价格监控] 使用 V2 版本 (Jupiter Swap Quote)');
          this.livePriceMonitor = new LivePriceMonitorV2(this.jupiterExecutor);
        } else if (process.env.JUPITER_API_KEY) {
          console.log('📡 [价格监控] 使用 V1 版本 (Jupiter Price API)');
          this.livePriceMonitor = new LivePriceMonitor();
        }

        if (this.livePriceMonitor) {
          this.livePriceMonitor.start();
          this.engine.setLivePriceMonitor(this.livePriceMonitor);
        }

        this.livePositionMonitor = new LivePositionMonitor(this.livePriceMonitor, this.jupiterExecutor, this.engine.riskManager);
        await this.livePositionMonitor.start();

        // 注入到 engine
        this.engine.setLiveComponents(this.jupiterExecutor, this.livePositionMonitor);

        // 🔧 注册退出回调：退出后触发信号引擎冷却
        this.livePositionMonitor.onExit((symbol, tokenCA, pnl) => {
          if (this.engine && this.engine.markExitCooldown) {
            this.engine.markExitCooldown(symbol);
          }
        });

        const solBalance = await this.jupiterExecutor.getSolBalance();
        console.log(`💰 [实盘] 钱包余额: ${solBalance.toFixed(4)} SOL`);
      } catch (error) {
        console.error(`❌ [实盘] 初始化失败: ${error.message}`);
        console.log('⚠️  降级为 SHADOW 模式');
        this.jupiterExecutor = null;
        this.livePositionMonitor = null;
      }
    }

    // 初始化引擎
    await this.engine.initialize();

    // 注册信号回调
    this.listener.onSignal(async (signal) => {
      try {
        await this.engine.processSignal(signal);
      } catch (error) {
        console.error('❌ [Premium] 信号处理异常:', error.message);
      }
    });

    // 启动监听
    await this.listener.start();

    // Expose listener globally for API access (channel history)
    global.__telegramService = this.listener;

    // 把 Telegram client 传给 engine 用于 Buzz 搜索
    if (this.listener.client) {
      this.engine.setTelegramClient(this.listener.client);
    }

    console.log('\n✅ Premium Channel System 运行中...');
    console.log('   等待频道信号...\n');

    // 暴露给 dashboard-server 用于手动暂停/恢复交易
    global.__riskManager = this.engine.riskManager;
    if (this.jupiterExecutor) global.__executor = this.jupiterExecutor;

    // 启动 Dashboard Server（Zeabur 健康检查 + /premium 页面）
    startDashboardServer();
  }

  async stop() {
    await this.listener.stop();
    await this.engine.stop();
    if (this.livePositionMonitor) {
      this.livePositionMonitor.stop();
    }
    if (this.livePriceMonitor) {
      this.livePriceMonitor.stop();
    }
    this.db.close();
    console.log('⏹️  Premium Channel System 已停止');
  }
}

// ==========================================
// MAIN EXECUTION
// ==========================================

async function main() {
  const mode = process.argv.includes('--premium') || process.env.PREMIUM_MODE_ENABLED === 'true'
    ? 'premium'
    : 'default';

  const system = mode === 'premium'
    ? new PremiumChannelSystem()
    : new SentimentArbitrageSystem();

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\n\n🛑 Received SIGINT, shutting down gracefully...');
    await system.stop();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    console.log('\n\n🛑 Received SIGTERM, shutting down gracefully...');
    await system.stop();
    process.exit(0);
  });

  // Start system
  try {
    await system.start();
  } catch (error) {
    console.error('❌ Fatal error:', error);
    process.exit(1);
  }
}

// Run
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(error => {
    console.error('❌ Unhandled error:', error);
    process.exit(1);
  });
}

export { SentimentArbitrageSystem, PremiumChannelSystem };
