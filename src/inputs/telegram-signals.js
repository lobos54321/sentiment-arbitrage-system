/**
 * Telegram Signal Listener
 *
 * Monitors configured Telegram channels for token signals
 * Extracts token addresses (Solana + BSC)
 * Stores signals in database for processing
 */

import TelegramBot from 'node-telegram-bot-api';

export class TelegramService {
  constructor(config, db) {
    this.config = config;
    this.db = db;
    this.bot = null;
    this.isRunning = false;
    this.monitoredChannels = [];
  }

  /**
   * Start Telegram listener
   */
  async start() {
    try {
      const token = process.env.TELEGRAM_BOT_TOKEN;
      if (!token) {
        console.error('❌ TELEGRAM_BOT_TOKEN not found in .env');
        return;
      }

      // Initialize bot with polling
      this.bot = new TelegramBot(token, {
        polling: {
          interval: 1000,
          autoStart: false
        }
      });

      // Load monitored channels from database
      this.loadChannels();

      // Set up message handler
      this.bot.on('message', (msg) => this.handleMessage(msg));
      this.bot.on('channel_post', (msg) => this.handleMessage(msg));

      // Start polling
      await this.bot.startPolling();
      this.isRunning = true;

      console.log(`✅ Telegram listener started`);
      console.log(`   Monitoring ${this.monitoredChannels.length} channels`);

    } catch (error) {
      console.error('❌ Failed to start Telegram listener:', error.message);
      throw error;
    }
  }

  /**
   * Load monitored channels from database
   */
  loadChannels() {
    try {
      const channels = this.db.prepare(`
        SELECT * FROM telegram_channels
        WHERE active = 1
      `).all();

      this.monitoredChannels = channels.map(ch => ({
        id: ch.id,
        name: ch.channel_name,
        username: ch.channel_username,
        tier: ch.tier
      }));

      console.log(`📋 Loaded ${this.monitoredChannels.length} channels from database`);

    } catch (error) {
      console.error('❌ Failed to load channels:', error.message);
      this.monitoredChannels = [];
    }
  }

  /**
   * Handle incoming message
   */
  async handleMessage(msg) {
    try {
      const chatId = msg.chat.id;
      const chatTitle = msg.chat.title || msg.chat.username || 'Unknown';
      const messageText = msg.text || msg.caption || '';

      // Extract token addresses
      const tokens = this.extractTokenAddresses(messageText);

      if (tokens.length === 0) {
        return; // No token addresses found
      }

      // Save each token signal
      for (const token of tokens) {
        this.saveSignal({
          channel_id: chatId,
          channel_name: chatTitle,
          message_text: messageText,
          token_ca: token.address,
          chain: token.chain,
          timestamp: new Date(msg.date * 1000).toISOString()
        });
      }

    } catch (error) {
      console.error('❌ Handle message error:', error.message);
    }
  }

  /**
   * Extract token addresses from message text
   */
  extractTokenAddresses(text) {
    const tokens = [];

    // Solana address pattern (base58, 32-44 chars)
    const solanaPattern = /\b([1-9A-HJ-NP-Za-km-z]{32,44})\b/g;

    // BSC address pattern (0x + 40 hex chars)
    const bscPattern = /\b(0x[a-fA-F0-9]{40})\b/g;

    // Extract Solana addresses
    let match;
    while ((match = solanaPattern.exec(text)) !== null) {
      const address = match[1];
      // Filter out common false positives (all same char, etc)
      if (this.isValidSolanaAddress(address)) {
        tokens.push({
          address,
          chain: 'SOL'
        });
      }
    }

    // Extract BSC addresses
    while ((match = bscPattern.exec(text)) !== null) {
      const address = match[1];
      if (this.isValidBSCAddress(address)) {
        tokens.push({
          address,
          chain: 'BSC'
        });
      }
    }

    return tokens;
  }

  /**
   * Validate Solana address
   */
  isValidSolanaAddress(address) {
    // Length check
    if (address.length < 32 || address.length > 44) {
      return false;
    }

    // Not all same character
    if (/^(.)\1+$/.test(address)) {
      return false;
    }

    // Has variety of characters
    const uniqueChars = new Set(address.split(''));
    if (uniqueChars.size < 5) {
      return false;
    }

    return true;
  }

  /**
   * Validate BSC address
   */
  isValidBSCAddress(address) {
    // Must start with 0x and be 42 chars total
    if (!/^0x[a-fA-F0-9]{40}$/.test(address)) {
      return false;
    }

    // Not all zeros or all same
    if (/^0x(0+|(.)\2+)$/.test(address)) {
      return false;
    }

    return true;
  }

  /**
   * Save signal to database
   */
  saveSignal(signal) {
    try {
      // Check if already exists (deduplicate)
      const existing = this.db.prepare(`
        SELECT id FROM telegram_signals
        WHERE token_ca = ? AND channel_name = ?
        AND timestamp > datetime('now', '-5 minutes')
      `).get(signal.token_ca, signal.channel_name);

      if (existing) {
        return; // Already have this signal recently
      }

      // Insert new signal
      this.db.prepare(`
        INSERT INTO telegram_signals (
          channel_name, message_text, token_ca, chain, timestamp, processed
        ) VALUES (?, ?, ?, ?, ?, 0)
      `).run(
        signal.channel_name,
        signal.message_text,
        signal.token_ca,
        signal.chain,
        signal.timestamp
      );

      console.log(`\n🔔 NEW SIGNAL: ${signal.token_ca.substring(0, 8)}... (${signal.chain}) from ${signal.channel_name}`);

    } catch (error) {
      console.error('❌ Save signal error:', error.message);
    }
  }

  /**
   * Stop Telegram listener
   */
  stop() {
    if (this.bot && this.isRunning) {
      this.bot.stopPolling();
      this.isRunning = false;
      console.log('⏹️  Telegram listener stopped');
    }
  }

  /**
   * Get service status
   */
  getStatus() {
    return {
      is_running: this.isRunning,
      monitored_channels: this.monitoredChannels.length,
      channels: this.monitoredChannels.map(ch => ch.name)
    };
  }

  /**
   * Add channel to monitor
   */
  addChannel(channelInfo) {
    try {
      this.db.prepare(`
        INSERT OR REPLACE INTO telegram_channels (
          channel_name, channel_username, tier, active
        ) VALUES (?, ?, ?, 1)
      `).run(
        channelInfo.name,
        channelInfo.username || null,
        channelInfo.tier || 'B'
      );

      this.loadChannels(); // Reload
      console.log(`✅ Added channel: ${channelInfo.name}`);

    } catch (error) {
      console.error('❌ Add channel error:', error.message);
    }
  }

  /**
   * Remove channel from monitoring
   */
  removeChannel(channelName) {
    try {
      this.db.prepare(`
        UPDATE telegram_channels
        SET active = 0
        WHERE channel_name = ?
      `).run(channelName);

      this.loadChannels(); // Reload
      console.log(`✅ Removed channel: ${channelName}`);

    } catch (error) {
      console.error('❌ Remove channel error:', error.message);
    }
  }
}
