/**
 * Telegram Signal Parser
 *
 * Parses signals from MomentumTrackerCN and similar aggregator channels
 * Extracts: chain, token_ca, symbol, promoted_channels[], timestamps
 */

import TelegramBot from 'node-telegram-bot-api';

export class TelegramSignalParser {
  constructor(config, db) {
    this.config = config;
    this.db = db;
    this.bot = new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: true });
    this.monitoredChannels = this.loadMonitoredChannels();
  }

  loadMonitoredChannels() {
    const stmt = this.db.prepare(`
      SELECT tme_link, channel_name, current_tier, status
      FROM channel_performance
      WHERE status IN ('ACTIVE', 'WATCH')
    `);
    return stmt.all();
  }

  start() {
    console.log('🤖 Starting Telegram signal parser...');

    // Listen to channel posts
    this.bot.on('channel_post', async (msg) => {
      await this.handleChannelPost(msg);
    });

    // Listen to forwarded messages (if monitoring groups)
    this.bot.on('message', async (msg) => {
      if (msg.forward_from_chat) {
        await this.handleChannelPost(msg);
      }
    });

    console.log(`✅ Monitoring ${this.monitoredChannels.length} channels`);
  }

  async handleChannelPost(msg) {
    try {
      const text = msg.text || msg.caption || '';
      if (!text) return;

      // Check if this is from a monitored channel
      const channelId = msg.chat.id || msg.forward_from_chat?.id;
      const isMonitored = this.monitoredChannels.some(ch => {
        // You need to map tme_link to actual chat_id
        // This is simplified - in production, you'd maintain a mapping
        return true; // Temporarily accept all for parsing demo
      });

      // Parse signal
      const signal = this.parseSignalMessage(text, msg);

      if (signal && signal.token_ca) {
        console.log('📊 New signal detected:', signal.symbol || signal.token_ca);
        await this.processSignal(signal, msg);
      }
    } catch (error) {
      console.error('Error handling channel post:', error);
    }
  }

  parseSignalMessage(text, msg) {
    const signal = {
      raw_message: text,
      timestamp: msg.date * 1000, // Convert to ms
      message_id: msg.message_id,
      chat_id: msg.chat.id
    };

    // Extract chain
    const chainMatch = text.match(/(?:Chain|链)[:：]\s*(SOL|Solana|BSC|BNB)/i);
    if (chainMatch) {
      const chain = chainMatch[1].toUpperCase();
      signal.chain = chain === 'SOLANA' ? 'SOL' : chain === 'BNB' ? 'BSC' : chain;
    } else {
      // Try to infer from CA format
      const caMatch = text.match(/\b([A-HJ-NP-Za-km-z1-9]{32,44})\b/);
      if (caMatch) {
        const ca = caMatch[1];
        // SOL addresses are typically 32-44 chars base58
        // BSC addresses are 0x + 40 hex chars
        signal.chain = ca.startsWith('0x') ? 'BSC' : 'SOL';
      }
    }

    // Extract token CA (contract address)
    const patterns = {
      SOL: /(?:CA|Contract|Address|地址)[:：]?\s*([A-HJ-NP-Za-km-z1-9]{32,44})/i,
      BSC: /(?:CA|Contract|Address|地址)[:：]?\s*(0x[a-fA-F0-9]{40})/i
    };

    for (const [chain, pattern] of Object.entries(patterns)) {
      const match = text.match(pattern);
      if (match) {
        signal.token_ca = match[1];
        if (!signal.chain) signal.chain = chain;
        break;
      }
    }

    // If still no CA, try generic base58/hex patterns
    if (!signal.token_ca) {
      const base58Match = text.match(/\b([A-HJ-NP-Za-km-z1-9]{32,44})\b/);
      const hexMatch = text.match(/\b(0x[a-fA-F0-9]{40})\b/);

      if (base58Match && !hexMatch) {
        signal.token_ca = base58Match[1];
        signal.chain = signal.chain || 'SOL';
      } else if (hexMatch) {
        signal.token_ca = hexMatch[1];
        signal.chain = signal.chain || 'BSC';
      }
    }

    // Extract symbol
    const symbolMatch = text.match(/(?:Symbol|Token|代币)[:：]?\s*\$?([A-Z][A-Z0-9]{1,10})/i);
    if (symbolMatch) {
      signal.symbol = symbolMatch[1].toUpperCase();
    }

    // Extract market cap if mentioned
    const mcMatch = text.match(/(?:MC|Market\s*Cap|市值)[:：]?\s*\$?([\d.]+)\s*([KMB])?/i);
    if (mcMatch) {
      let mc = parseFloat(mcMatch[1]);
      const unit = mcMatch[2]?.toUpperCase();
      if (unit === 'K') mc *= 1000;
      else if (unit === 'M') mc *= 1_000_000;
      else if (unit === 'B') mc *= 1_000_000_000;
      signal.mc_at_signal = mc;
    }

    // Extract promoted channels
    signal.promoted_channels = this.extractPromotedChannels(text);

    // Calculate N_total
    signal.N_total = signal.promoted_channels.length;

    return signal;
  }

  extractPromotedChannels(text) {
    const channels = [];
    const channelPattern = /(?:@|t\.me\/|https?:\/\/t\.me\/)([a-zA-Z0-9_]+)/g;

    let match;
    while ((match = channelPattern.exec(text)) !== null) {
      const username = match[1];
      const tme_link = `t.me/${username}`;

      // Check if already in list
      if (!channels.find(ch => ch.tme_link === tme_link)) {
        // Look up tier from database
        const channelInfo = this.db.prepare(`
          SELECT channel_name, current_tier
          FROM channel_performance
          WHERE tme_link = ?
        `).get(tme_link);

        channels.push({
          name: channelInfo?.channel_name || username,
          tme_link: tme_link,
          timestamp: Date.now(),
          tier: channelInfo?.current_tier || 'C' // Default to C if unknown
        });
      }
    }

    return channels;
  }

  async processSignal(signal, msg) {
    // Check if already processed (deduplication)
    const existing = this.db.prepare(`
      SELECT token_ca FROM tokens WHERE token_ca = ?
    `).get(signal.token_ca);

    const isNew = !existing;

    // Insert or update token
    this.db.prepare(`
      INSERT INTO tokens (token_ca, chain, symbol, first_seen_at, mc_at_signal)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(token_ca) DO UPDATE SET
        symbol = COALESCE(symbol, excluded.symbol),
        mc_at_signal = COALESCE(mc_at_signal, excluded.mc_at_signal)
    `).run(
      signal.token_ca,
      signal.chain,
      signal.symbol,
      signal.timestamp,
      signal.mc_at_signal
    );

    // Insert social snapshot
    const derivedMetrics = this.calculateDerivedMetrics(signal);

    this.db.prepare(`
      INSERT INTO social_snapshots (
        token_ca,
        observed_at,
        promoted_channels,
        tg_t0,
        tg_time_lag,
        tg_ch_5m,
        tg_ch_15m,
        tg_ch_60m,
        tg_velocity,
        tg_accel,
        tg_clusters_15m,
        N_total
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      signal.token_ca,
      signal.timestamp,
      JSON.stringify(signal.promoted_channels),
      derivedMetrics.tg_t0,
      derivedMetrics.tg_time_lag,
      derivedMetrics.tg_ch_5m,
      derivedMetrics.tg_ch_15m,
      derivedMetrics.tg_ch_60m,
      derivedMetrics.tg_velocity,
      derivedMetrics.tg_accel,
      derivedMetrics.tg_clusters_15m,
      signal.N_total
    );

    // Emit signal for processing pipeline
    if (isNew) {
      this.emit('new_signal', {
        ...signal,
        ...derivedMetrics
      });
    } else {
      this.emit('signal_update', {
        ...signal,
        ...derivedMetrics
      });
    }

    return signal;
  }

  calculateDerivedMetrics(signal) {
    const now = Date.now();

    // Get historical snapshots for this token
    const snapshots = this.db.prepare(`
      SELECT observed_at, promoted_channels, tg_ch_15m
      FROM social_snapshots
      WHERE token_ca = ?
      ORDER BY observed_at DESC
      LIMIT 10
    `).all(signal.token_ca);

    // tg_t0: earliest mention time
    let tg_t0 = signal.timestamp;
    if (snapshots.length > 0) {
      const allChannels = [];
      for (const snap of snapshots) {
        const channels = JSON.parse(snap.promoted_channels || '[]');
        allChannels.push(...channels);
      }
      if (allChannels.length > 0) {
        tg_t0 = Math.min(...allChannels.map(ch => ch.timestamp));
      }
    }

    // tg_time_lag: minutes from t0 to now
    const tg_time_lag = Math.floor((now - tg_t0) / (1000 * 60));

    // tg_ch_Xm: count unique channels in time window
    const allChannels = signal.promoted_channels;
    const historicalChannels = snapshots.flatMap(s =>
      JSON.parse(s.promoted_channels || '[]')
    );

    const uniqueChannels = new Map();
    [...allChannels, ...historicalChannels].forEach(ch => {
      uniqueChannels.set(ch.tme_link, ch);
    });

    const getChannelsInWindow = (windowMinutes) => {
      const cutoff = now - windowMinutes * 60 * 1000;
      return Array.from(uniqueChannels.values()).filter(
        ch => ch.timestamp >= cutoff
      ).length;
    };

    const tg_ch_5m = getChannelsInWindow(5);
    const tg_ch_15m = getChannelsInWindow(15);
    const tg_ch_60m = getChannelsInWindow(60);

    // tg_velocity: channels per minute
    const tg_velocity = tg_time_lag > 0 ? tg_ch_15m / 15 : 0;

    // tg_accel: change in velocity
    let tg_accel = 0;
    if (snapshots.length >= 2) {
      const prevVelocity = snapshots[1].tg_ch_15m / 15;
      tg_accel = tg_velocity - prevVelocity;
    }

    // tg_clusters_15m: count independent clusters (simplified)
    const tg_clusters_15m = this.estimateClusters(
      Array.from(uniqueChannels.values()).filter(
        ch => ch.timestamp >= now - 15 * 60 * 1000
      )
    );

    return {
      tg_t0,
      tg_time_lag,
      tg_ch_5m,
      tg_ch_15m,
      tg_ch_60m,
      tg_velocity,
      tg_accel,
      tg_clusters_15m
    };
  }

  estimateClusters(channels) {
    if (channels.length === 0) return 0;

    // Group by tier (simple heuristic)
    const tiers = new Set(channels.map(ch => ch.tier));

    // Also check for time clustering (synchronized posting)
    const timestamps = channels.map(ch => ch.timestamp).sort((a, b) => a - b);
    let clusters = 1;

    for (let i = 1; i < timestamps.length; i++) {
      const gap = timestamps[i] - timestamps[i - 1];
      if (gap > 2 * 60 * 1000) {
        // Gap > 2 minutes = new cluster
        clusters++;
      }
    }

    return Math.max(tiers.size, clusters);
  }

  // Event emitter methods
  emit(event, data) {
    if (!this.handlers) this.handlers = {};
    if (!this.handlers[event]) return;
    this.handlers[event].forEach(handler => handler(data));
  }

  on(event, handler) {
    if (!this.handlers) this.handlers = {};
    if (!this.handlers[event]) this.handlers[event] = [];
    this.handlers[event].push(handler);
  }
}

export default TelegramSignalParser;
