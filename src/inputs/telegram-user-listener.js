/**
 * Telegram User API Channel Listener
 *
 * Uses Telegram User API (MTProto) to monitor public channels
 * This is necessary because Bot API cannot read channel messages
 */

import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import { NewMessage } from 'telegram/events/index.js';

export class TelegramUserListener {
  constructor(config, db) {
    this.config = config;
    this.db = db;
    this.client = null;
    this.isRunning = false;
    this.monitoredChannels = [];
  }

  /**
   * Start User API listener
   */
  async start() {
    try {
      const apiId = parseInt(process.env.TELEGRAM_API_ID || '0');
      const apiHash = process.env.TELEGRAM_API_HASH || '';
      const sessionString = process.env.TELEGRAM_SESSION || '';

      if (!apiId || !apiHash || !sessionString) {
        console.error('❌ Missing Telegram User API credentials');
        console.error('   Please run: node scripts/authenticate-telegram.js');
        return;
      }

      // Load monitored channels
      this.loadChannels();

      // Initialize client with saved session
      const session = new StringSession(sessionString);
      this.client = new TelegramClient(session, apiId, apiHash, {
        connectionRetries: 5,
      });

      // Connect to Telegram
      await this.client.connect();
      console.log('✅ Connected to Telegram User API');

      // Subscribe to each monitored channel
      await this.subscribeToChannels();

      // Set up message handler
      this.client.addEventHandler(
        (event) => this.handleMessage(event),
        new NewMessage({})
      );

      this.isRunning = true;

      console.log(`✅ Telegram User listener started`);
      console.log(`   Monitoring ${this.monitoredChannels.length} channels`);

    } catch (error) {
      console.error('❌ Failed to start Telegram User listener:', error.message);
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
   * Subscribe to all monitored channels
   */
  async subscribeToChannels() {
    console.log('📡 Subscribing to channels...');

    for (const channel of this.monitoredChannels) {
      try {
        // Try to get the channel entity
        const entity = await this.client.getEntity(channel.username);
        console.log(`   ✅ Subscribed to ${channel.username}`);
      } catch (error) {
        console.error(`   ⚠️  Failed to subscribe to ${channel.username}: ${error.message}`);
        console.error(`      Make sure you have joined this channel in Telegram first!`);
      }
    }
  }

  /**
   * Handle incoming message
   */
  async handleMessage(event) {
    try {
      const message = event.message;

      // Get chat info
      const chat = await message.getChat();
      const chatUsername = chat.username ? `@${chat.username}` : null;

      // Check if this message is from a monitored channel
      const isMonitored = this.monitoredChannels.some(
        ch => ch.username === chatUsername
      );

      if (!isMonitored) {
        return; // Ignore messages from non-monitored channels
      }

      const messageText = message.text || '';
      const chatTitle = chat.title || chat.username || 'Unknown';

      // Extract token addresses
      const tokens = this.extractTokenAddresses(messageText);

      if (tokens.length === 0) {
        return; // No token addresses found
      }

      // Save each token signal
      for (const token of tokens) {
        this.saveSignal({
          channel_name: chatTitle,
          channel_username: chatUsername,
          message_text: messageText,
          token_ca: token.address,
          chain: token.chain,
          timestamp: new Date(message.date * 1000).toISOString()
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
      // Filter out common false positives
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
    if (/^(.)\\1+$/.test(address)) {
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
    if (/^0x(0+|(.)\\2+)$/.test(address)) {
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
   * Stop listener
   */
  async stop() {
    if (this.client && this.isRunning) {
      await this.client.disconnect();
      this.isRunning = false;
      console.log('⏹️  Telegram User listener stopped');
    }
  }

  /**
   * Fetch channel message history (for backtest analysis)
   * @param {number} limit - Number of messages to fetch (default 200)
   * @returns {Array} Array of {timestamp, text, tokens[]}
   */
  async getChannelHistory(limit = 200) {
    if (!this.client || !this.isRunning) {
      return { error: 'Telegram client not connected' };
    }

    const results = [];
    
    for (const channel of this.monitoredChannels) {
      try {
        const entity = await this.client.getEntity(channel.username);
        const messages = await this.client.getMessages(entity, { limit });
        
        for (const msg of messages) {
          if (!msg.text) continue;
          
          const tokens = this.extractTokenAddresses(msg.text);
          if (tokens.length === 0) continue;
          
          results.push({
            channel: channel.name || channel.username,
            timestamp: new Date(msg.date * 1000).toISOString(),
            text: msg.text.substring(0, 500),
            tokens: tokens.map(t => ({ address: t.address, chain: t.chain }))
          });
        }
        
        console.log(`📜 Fetched ${messages.length} messages from ${channel.username}, ${results.length} with tokens`);
      } catch (error) {
        console.error(`❌ Failed to fetch history from ${channel.username}: ${error.message}`);
      }
    }
    
    return { 
      count: results.length,
      channels: this.monitoredChannels.map(ch => ch.username),
      messages: results 
    };
  }

  /**
   * Get service status
   */
  getStatus() {
    return {
      is_running: this.isRunning,
      monitored_channels: this.monitoredChannels.length,
      channels: this.monitoredChannels.map(ch => ch.username)
    };
  }
}
