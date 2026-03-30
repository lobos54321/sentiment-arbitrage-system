/**
 * Premium Channel Listener (Egeye AI Gems 100X Vip)
 *
 * Uses Telegram User API (MTProto) to monitor a private paid signal channel.
 * Parses "🔥 New Trending" messages for token signals, ignores ATH alerts.
 * No database dependency - emits signals via callback.
 */

import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import { NewMessage } from 'telegram/events/index.js';
import { Api } from 'telegram';
import https from 'https';
import http from 'http';
import fs from 'fs';
import path from 'path';

const DEFAULT_CHANNEL_ID = 3636518327;
const DEDUP_WINDOW_MS = 5 * 60 * 1000; // 5 minutes

// Solana base58 address pattern
const SOL_ADDRESS_RE = /\b([1-9A-HJ-NP-Za-km-z]{32,44})\b/g;

// Known false-positive patterns (URLs, common words that match base58)
const FALSE_POSITIVE_RE = /^(https?|discord|telegram|dexscreener|twitter|solscan)/i;

const WATCHDOG_INTERVAL_MS = 2 * 60 * 1000;   // check every 2 minutes
const WATCHDOG_DEAD_THRESHOLD_MS = 5 * 60 * 1000; // treat as dead if no ping for 5 min
// Path to persist channel access_hash so GetDialogs is never needed after first run
const CHANNEL_CACHE_PATH = process.env.CHANNEL_CACHE_PATH ||
  path.join(process.env.DATA_DIR || '/app/data', 'tg-channel-cache.json');

export class PremiumChannelListener {
  constructor(config = {}) {
    this.config = config;
    this.client = null;
    this.isRunning = false;
    this.channelEntity = null;
    this.signalCallbacks = [];
    this.recentTokens = new Map(); // token_ca -> timestamp for dedup
    this._watchdogTimer = null;
    this._lastPingTs = Date.now();
    this._reconnecting = false;
    // Webhook URLs for real-time signal forwarding (comma-separated in env)
    this.webhookUrls = (process.env.SIGNAL_WEBHOOK_URLS || '')
      .split(',')
      .map(u => u.trim())
      .filter(u => u.length > 0);
    if (this.webhookUrls.length > 0) {
      console.log(`🔗 Signal webhook configured: ${this.webhookUrls.length} endpoint(s)`);
    }
  }

  /**
   * Register a signal callback
   */
  onSignal(callback) {
    this.signalCallbacks.push(callback);
  }

  /**
   * Start listener - connect, find channel, register handler
   */
  async start() {
    try {
      const apiId = parseInt(process.env.TELEGRAM_API_ID || '0');
      const apiHash = process.env.TELEGRAM_API_HASH || '';
      const sessionString = process.env.TELEGRAM_SESSION || '';

      if (!apiId || !apiHash || !sessionString) {
        console.error('❌ Missing Telegram User API credentials');
        console.error('   Please run: node scripts/authenticate-telegram.js');
        this.isRunning = false;
        return false;
      }

      const channelId = parseInt(process.env.PREMIUM_CHANNEL_ID || String(DEFAULT_CHANNEL_ID));

      const session = new StringSession(sessionString);
      this.client = new TelegramClient(session, apiId, apiHash, {
        connectionRetries: 5,
      });

      await this.client.connect();
      console.log('✅ Connected to Telegram User API (Premium)');

      // Resolution priority:
      //   1. In-memory entity (reconnect within same process)
      //   2. Persistent disk cache (survives container restarts)
      //   3. GetDialogs API call (first-ever start only)
      if (!this.channelEntity) {
        this.channelEntity = this._loadChannelCache(channelId);
        if (this.channelEntity) {
          console.log(`✅ Loaded channel entity from cache: ${channelId}`);
        }
      }

      if (!this.channelEntity) {
        this.channelEntity = await this._findChannelById(channelId);
        if (this.channelEntity) {
          console.log(`✅ Found premium channel: ${this.channelEntity.title || channelId}`);
          this._saveChannelCache(channelId, this.channelEntity);
        } else {
          // Telegram server issues — register handler now so we catch messages once
          // the server recovers, and keep retrying GetDialogs in the background.
          console.warn(`⚠️ Could not resolve channel ${channelId} right now (Telegram server issues)`);
          console.warn('   Messages will be received but channel-filter is relaxed until resolved.');
          console.warn('   Background retry every 2 min until channel entity is found...');
          this.client.addEventHandler(
            (event) => this._handleMessage(event),
            new NewMessage({})
          );
          this.isRunning = true;
          this._lastPingTs = Date.now();
          this._startWatchdog();
          this._scheduleChannelRetry(channelId);
          return true;
        }
      } else {
        console.log(`✅ Reusing cached channel entity: ${this.channelEntity.title || channelId}`);
      }

      // Register message handler
      this.client.addEventHandler(
        (event) => this._handleMessage(event),
        new NewMessage({})
      );

      this.isRunning = true;
      this._lastPingTs = Date.now();
      this._startWatchdog();
      console.log('✅ Premium channel listener started');
      return true;

    } catch (error) {
      this.isRunning = false;
      console.error('❌ Failed to start premium channel listener:', error.message);
      return false;
    }
  }

  /**
   * Background retry: keep calling GetDialogs every 2 min until we get the entity.
   * Once resolved, saves cache — _handleMessage will then filter correctly.
   */
  _scheduleChannelRetry(channelId) {
    const RETRY_MS = 2 * 60 * 1000;
    const attempt = async () => {
      if (this.channelEntity || !this.isRunning) return; // already resolved or stopped
      console.log(`🔄 [ChannelRetry] Attempting GetDialogs for channel ${channelId}...`);
      try {
        const entity = await this._findChannelById(channelId);
        if (entity) {
          this.channelEntity = entity;
          this._saveChannelCache(channelId, entity);
          console.log(`✅ [ChannelRetry] Channel entity resolved: ${entity.title || channelId}`);
          return; // done
        }
      } catch (err) {
        console.warn(`⚠️ [ChannelRetry] Error: ${err.message}`);
      }
      // Still not resolved — try again after delay
      setTimeout(attempt, RETRY_MS);
    };
    setTimeout(attempt, RETRY_MS);
  }

  /**
   * Start watchdog timer — detects dead Telegram connection and reconnects
   */
  _startWatchdog() {
    if (this._watchdogTimer) clearInterval(this._watchdogTimer);
    this._watchdogTimer = setInterval(() => this._watchdogTick(), WATCHDOG_INTERVAL_MS);
  }

  async _watchdogTick() {
    if (this._reconnecting) return;

    const connected = this.client?.connected === true;
    const stale = Date.now() - this._lastPingTs > WATCHDOG_DEAD_THRESHOLD_MS;

    if (connected) {
      // Send a lightweight ping to confirm the server is reachable
      try {
        await this.client.invoke(new Api.Ping({ pingId: BigInt(Date.now()) }));
        this._lastPingTs = Date.now();
      } catch (err) {
        console.warn(`⚠️ [Watchdog] Telegram ping failed: ${err.message} — triggering reconnect`);
        await this._doReconnect();
      }
    } else if (!connected || stale) {
      console.warn(`⚠️ [Watchdog] Telegram connection dead (connected=${connected}, stale=${stale}) — triggering reconnect`);
      await this._doReconnect();
    }
  }

  async _doReconnect() {
    if (this._reconnecting) return;
    this._reconnecting = true;
    console.log('🔄 [Watchdog] Reconnecting Telegram client...');
    // Preserve cached channelEntity so we don't need getDialogs on reconnect
    const cachedEntity = this.channelEntity;
    try {
      if (this.client) {
        try { await this.client.disconnect(); } catch (_) {}
      }
      this.client = null;
      this.isRunning = false;
      // Restore cached entity before start() so _findChannelById is skipped
      this.channelEntity = cachedEntity;

      // Re-run full start sequence
      const ok = await this.start();
      if (ok) {
        console.log('✅ [Watchdog] Telegram reconnect successful');
      } else {
        console.error('❌ [Watchdog] Telegram reconnect failed — will retry next cycle');
      }
    } catch (err) {
      console.error(`❌ [Watchdog] Reconnect error: ${err.message}`);
    } finally {
      this._reconnecting = false;
    }
  }

  /**
   * Find channel entity by numeric ID using getDialogs.
   * Retries with exponential backoff on server errors.
   */
  async _findChannelById(channelId) {
    const delays = [0, 8000, 20000, 40000]; // 0s, 8s, 20s, 40s
    for (let attempt = 0; attempt < delays.length; attempt++) {
      if (delays[attempt] > 0) {
        console.log(`🔄 [FindChannel] Retry ${attempt}/${delays.length - 1} in ${delays[attempt] / 1000}s...`);
        await new Promise(r => setTimeout(r, delays[attempt]));
      }
      try {
        const dialogs = await this.client.getDialogs({ limit: 200 });
        for (const dialog of dialogs) {
          const entity = dialog.entity;
          if (!entity) continue;
          const entityId = entity.id?.value !== undefined
            ? Number(entity.id.value)
            : Number(entity.id);
          if (entityId === channelId) return entity;
        }
        return null; // connected fine but channel not found
      } catch (error) {
        // Match telegram.js internal retry exhaustion, -500, TIMEOUT, etc.
        const isServerError = error.code === -500 ||
          /TIMEOUT|workers|unsuccessful/i.test(error.message);
        console.error(`❌ Error finding channel (attempt ${attempt + 1}): ${error.message}`);
        if (!isServerError || attempt === delays.length - 1) return null;
        // otherwise loop to retry
      }
    }
    return null;
  }

  /** Persist channel entity's access_hash to disk for future restarts */
  _saveChannelCache(channelId, entity) {
    try {
      const accessHash = entity.accessHash?.value !== undefined
        ? String(entity.accessHash.value)
        : String(entity.accessHash || '0');
      const data = { channelId, accessHash, title: entity.title || '', savedAt: Date.now() };
      fs.mkdirSync(path.dirname(CHANNEL_CACHE_PATH), { recursive: true });
      fs.writeFileSync(CHANNEL_CACHE_PATH, JSON.stringify(data), 'utf8');
      console.log(`💾 Channel entity cached to ${CHANNEL_CACHE_PATH}`);
    } catch (err) {
      console.warn(`⚠️ Could not save channel cache: ${err.message}`);
    }
  }

  /** Load persisted channel entity; returns reconstructed InputPeerChannel or null */
  _loadChannelCache(channelId) {
    try {
      if (!fs.existsSync(CHANNEL_CACHE_PATH)) return null;
      const data = JSON.parse(fs.readFileSync(CHANNEL_CACHE_PATH, 'utf8'));
      if (data.channelId !== channelId) return null;
      return new Api.Channel({
        id: BigInt(channelId),
        accessHash: BigInt(data.accessHash),
        title: data.title || '',
        megagroup: false,
      });
    } catch (err) {
      console.warn(`⚠️ Could not load channel cache: ${err.message}`);
      return null;
    }
  }

  /**
   * Handle incoming messages
   */
  async _handleMessage(event) {
    try {
      this._lastPingTs = Date.now(); // any message = connection is alive
      const message = event.message;
      if (!message || !message.peerId) return;

      // Check if message is from our premium channel
      const peerId = message.peerId;
      const msgChannelId = peerId.channelId?.value !== undefined
        ? Number(peerId.channelId.value)
        : Number(peerId.channelId || 0);

      if (this.channelEntity) {
        const targetId = this.channelEntity.id?.value !== undefined
          ? Number(this.channelEntity.id.value)
          : Number(this.channelEntity.id);
        if (msgChannelId !== targetId) return;
      } else {
        // Entity not yet resolved — only pass through obvious signal messages
        const text0 = message.text || message.message || '';
        if (!text0.includes('New Trending') && !text0.includes('ATH')) return;
      }

      const text = message.text || message.message || '';
      if (!text) return;

      // Process 🔥 New Trending OR 📈 ATH messages
      if (text.includes('🔥') && text.includes('New Trending')) {
        console.log(`🔥 [NOT_ATH原文] ${text.substring(0, 500)}`);
        const signal = this._parseSignal(text);
        if (!signal) return;
        const superIdx = signal.indices?.super_index?.value ?? signal.indices?.super_index ?? 0;
        console.log(`🔥 [NOT_ATH解析] $${signal.symbol||'?'} MC=${signal.market_cap} super=${superIdx}`);
        if (this._isDuplicate('NT_' + signal.token_ca)) return;
        this._emitSignal(signal);
      } else if (text.includes('📈') && text.includes('ATH')) {
        console.log(`📈 [ATH原文] ${text.substring(0, 200)}`);
        const signal = this._parseATHSignal(text);
        if (!signal) {
          console.log(`⚠️ [ATH] 解析失败: ${text.substring(0, 100)}`);
          return;
        }
        console.log(`📈 [ATH解析] $${signal.symbol} gain=${signal.gain_pct}% MC=${signal.market_cap_from}→${signal.market_cap} is_ath=${signal.is_ath}`);
        // ATH 用独立 key，不被同 CA 的 New Trending 信号误拦截
        if (this._isDuplicate('ATH_' + signal.token_ca)) return;
        this._emitSignal(signal);
      }

    } catch (error) {
      console.error('❌ Premium message handler error:', error.message);
    }
  }

  /**
   * Parse a 🔥 New Trending message into a signal object
   */
  _parseSignal(text) {
    const normalized = text
      .replace(/\*\*/g, '')
      .replace(/[\u200B-\u200D\uFEFF]/g, '')
      .replace(/\r/g, '');

    // Extract symbol: 📍 SYMBOL：$xxx
    const symbolMatch = normalized.match(/📍\s*SYMBOL\s*[：:]\s*\$?([^\s\n]+)/i);
    const symbol = symbolMatch ? symbolMatch[1].trim() : null;

    // Extract market cap: 🏦 MC: 21.83K
    const mcMatch = normalized.match(/🏦\s*MC\s*[：:]\s*\$?([\d,.]+)\s*([KMBkmb])?/i);
    const market_cap = mcMatch ? this._parseNumber(mcMatch[1], mcMatch[2]) : 0;

    // Extract holders: 👥 Holders: 132
    const holdersMatch = normalized.match(/👥\s*Holders\s*[：:]\s*([\d,]+)/i);
    const holders = holdersMatch ? parseInt(holdersMatch[1].replace(/,/g, ''), 10) : 0;

    // Extract volume 24h: 💰 Vol24H: $27.08K
    const volMatch = normalized.match(/💰\s*Vol24H\s*[：:]\s*\$?([\d,.]+)\s*([KMBkmb])?/i);
    const volume_24h = volMatch ? this._parseNumber(volMatch[1], volMatch[2]) : 0;

    // Extract top10 percentage: 📊 Top10: 22.85%
    const top10Match = normalized.match(/📊\s*Top10\s*[：:]\s*([\d.]+)%/i);
    const top10_pct = top10Match ? parseFloat(top10Match[1]) : 0;

    // Extract freeze authority / mint authority flags
    const freeze_ok = /freezeAuthority\s*[：:]\s*✅/i.test(normalized);
    const mint_ok = /NoMint\s*[：:]\s*✅/i.test(normalized) || /mintAuthorityDisabled\s*[：:]\s*✅/i.test(normalized);

    // Extract age: 🕒 Age: 4M
    const ageMatch = normalized.match(/🕒\s*Age\s*[：:]\s*(\S+)/i);
    const age = ageMatch ? ageMatch[1] : '';

    // Extract Solana contract address
    const token_ca = this._extractSolAddress(text);
    if (!token_ca) return null;

    // Extract Egeye AI Index data (7 indices)
    const indices = this._parseIndices(text);

    return {
      token_ca,
      chain: 'SOL',
      symbol: symbol || 'UNKNOWN',
      market_cap,
      holders,
      volume_24h,
      top10_pct,
      freeze_ok,
      mint_ok,
      age,
      indices,  // { super_index, ai_index, trade_index, security_index, address_index, viral_index, media_index }
      description: text,
      channel: 'Egeye AI Gems 100X Vip',
      timestamp: Date.now(),
      source: 'premium_channel',
    };
  }

  /**
   * Parse 📈 ATH message
   * Format: "📈New ATH $KIRBY is up **51%** 📈" or "📈New ATH $PMPR is up **15.43X** 📈"
   */
  _parseATHSignal(text) {
    // Extract symbol and gain: $SYMBOL is up **XX%** or **X.XXX**
    const athMatch = text.match(/\$(\S+)\s+is\s+up\s+\*{0,2}([\d.]+)(%|X)\*{0,2}/i);
    const symbol = athMatch ? athMatch[1] : null;
    let gainPct = 0;
    if (athMatch) {
      if (athMatch[3].toUpperCase() === 'X') {
        gainPct = (parseFloat(athMatch[2]) - 1) * 100; // 2.15X = +115%
      } else {
        gainPct = parseFloat(athMatch[2]); // 51% = +51%
      }
    }

    // Extract MC range: $12.86K —> $23.62K
    const mcMatch = text.match(/\$([\d,.]+)\s*([KMBkmb])?\s*[—\-]+>\s*\$([\d,.]+)\s*([KMBkmb])?/);
    const mcFrom = mcMatch ? this._parseNumber(mcMatch[1], mcMatch[2]) : 0;
    const mcTo = mcMatch ? this._parseNumber(mcMatch[3], mcMatch[4]) : 0;

    // Extract CA
    const token_ca = this._extractSolAddress(text);
    if (!token_ca) return null;

    // Extract Egeye AI Index data
    const indices = this._parseIndices(text);

    if (!mcTo || mcTo === 0) {
      console.log(`⚠️ [ATH] $${symbol || 'UNKNOWN'} mcTo=0, 使用 mcFrom=${mcFrom} 作为估算`);
    }
    const mc = mcTo || mcFrom || 0;

    return {
      token_ca,
      chain: 'SOL',
      symbol: symbol || 'UNKNOWN',
      market_cap: mc,
      market_cap_from: mcFrom,
      gain_pct: gainPct,
      is_ath: true,
      holders: 0,
      volume_24h: 0,
      top10_pct: 0,
      freeze_ok: null,
      mint_ok: null,
      age: '',
      indices,
      description: text,
      channel: 'Egeye AI Gems 100X Vip',
      timestamp: Date.now(),
      source: 'premium_channel_ath',
    };
  }

  /**
   * Parse Egeye AI Index data from signal text
   *
   * ATH format:      ✡ Super Index：(signal)116🔮 --> (current)124🔮 🔺6%
   *   → { signal: 116, current: 124, growth: 6.9 }
   *
   * Trending format: ✡ Super Index： 128🔮
   *   → { value: 128 }
   */
  _parseIndices(text) {
    const normalized = String(text || '')
      .replace(/\*\*/g, '')
      .replace(/[\u200B-\u200D\uFEFF]/g, '')
      .replace(/\r/g, '');
    const indices = {};
    const indexNames = [
      ['super_index',     'Super Index'],
      ['ai_index',        'AI Index'],
      ['trade_index',     'Trade Index'],
      ['security_index',  'Security Index'],
      ['address_index',   'Address Index'],
      ['sentiment_index', 'Sentiment Index'],
      ['viral_index',     'Viral Index'],
      ['media_index',     'Media Index'],
    ];

    for (const [key, label] of indexNames) {
      const escaped = label.replace(/\s+/g, '\\s+');

      // ATH delta format: Label：(signal)116🔮 --> (current)124🔮
      const reDelta = new RegExp(escaped + '[：:]\\s*[\\(（]signal[\\)）]\\s*x?(\\d+).*?[\\(（]current[\\)）]\\s*x?(\\d+)', 'i');
      const mDelta = normalized.match(reDelta);
      if (mDelta) {
        const signalVal = parseInt(mDelta[1]);
        const currentVal = parseInt(mDelta[2]);
        indices[key] = {
          signal: signalVal,
          current: currentVal,
          growth: signalVal > 0 ? ((currentVal - signalVal) / signalVal * 100) : (currentVal > 0 ? 999 : 0),
        };
        continue;
      }

      // Trending single-value format: Label： 128🔮 or Label： 0 (no (signal)/(current), emoji optional)
      const reSingle = new RegExp(escaped + '[：:]\\s*(\\d+)\\s*(?:🔮)?', 'i');
      const mSingle = normalized.match(reSingle);
      if (mSingle) {
        indices[key] = { value: parseInt(mSingle[1]) };
        continue;
      }

      // NOT_ATH format: Label： ✡ x 82  or  Label：✡ x 82 (x separator, no emoji suffix)
      // Pattern: "✡ x NUMBER" or "✡x NUMBER" after the label
      const reXSep = new RegExp(escaped + '[：:]\\s*✡\\s*x\\s*(\\d+)', 'i');
      const mXSep = normalized.match(reXSep);
      if (mXSep) {
        indices[key] = { value: parseInt(mXSep[1]) };
      }
    }

    return Object.keys(indices).length > 0 ? indices : null;
  }

  /**
   * Extract a Solana base58 address from text, filtering false positives
   */
  _extractSolAddress(text) {
    const matches = text.match(SOL_ADDRESS_RE);
    if (!matches) return null;

    for (const match of matches) {
      // Skip short matches (likely not addresses)
      if (match.length < 32) continue;
      // Skip known false positives
      if (FALSE_POSITIVE_RE.test(match)) continue;
      // Skip if it looks like a URL fragment
      if (match.includes('.')) continue;
      return match;
    }
    return null;
  }

  /**
   * Parse number with K/M/B suffix
   */
  _parseNumber(numStr, suffix) {
    const num = parseFloat(numStr.replace(/,/g, ''));
    if (!suffix) return num;
    const multipliers = { k: 1e3, m: 1e6, b: 1e9 };
    return num * (multipliers[suffix.toLowerCase()] || 1);
  }

  /**
   * Check if token was seen within dedup window
   */
  _isDuplicate(tokenCa) {
    const now = Date.now();

    // Clean expired entries
    for (const [ca, ts] of this.recentTokens) {
      if (now - ts > DEDUP_WINDOW_MS) {
        this.recentTokens.delete(ca);
      }
    }

    if (this.recentTokens.has(tokenCa)) return true;

    this.recentTokens.set(tokenCa, now);
    return false;
  }

  /**
   * Emit signal to all registered callbacks
   */
  _emitSignal(signal) {
    console.log(`\n🔔 PREMIUM SIGNAL: $${signal.symbol} (${signal.token_ca.substring(0, 8)}...) MC: $${signal.market_cap}`);

    for (const cb of this.signalCallbacks) {
      try {
        cb(signal);
      } catch (error) {
        console.error('❌ Signal callback error:', error.message);
      }
    }

    // Forward to webhook endpoints (non-blocking)
    this._forwardToWebhooks(signal);

    // Broadcast to SSE clients (non-blocking)
    if (global.__sseClients && global.__sseClients.size > 0) {
      const sseData = JSON.stringify({ event: 'signal', timestamp: new Date().toISOString(), signal });
      for (const client of global.__sseClients) {
        try { client.write(`data: ${sseData}\n\n`); } catch (e) { /* client gone */ }
      }
    }
  }

  /**
   * Forward signal to all configured webhook URLs (fire-and-forget)
   */
  _forwardToWebhooks(signal) {
    if (this.webhookUrls.length === 0) return;

    const payload = JSON.stringify({
      event: 'signal',
      timestamp: new Date().toISOString(),
      signal: signal,
    });

    for (const url of this.webhookUrls) {
      try {
        const parsedUrl = new URL(url);
        const transport = parsedUrl.protocol === 'https:' ? https : http;
        const options = {
          hostname: parsedUrl.hostname,
          port: parsedUrl.port || (parsedUrl.protocol === 'https:' ? 443 : 80),
          path: parsedUrl.pathname + parsedUrl.search,
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(payload),
            'X-Signal-Source': 'sentiment-arbitrage',
          },
          timeout: 5000,
        };

        const req = transport.request(options, (res) => {
          if (res.statusCode >= 400) {
            console.warn(`⚠️ Webhook ${parsedUrl.hostname} returned ${res.statusCode}`);
          }
          res.resume(); // consume response
        });

        req.on('error', (err) => {
          console.warn(`⚠️ Webhook ${parsedUrl.hostname} error: ${err.message}`);
        });
        req.on('timeout', () => {
          req.destroy();
        });

        req.write(payload);
        req.end();
      } catch (err) {
        console.warn(`⚠️ Webhook URL error (${url}): ${err.message}`);
      }
    }
  }

  /**
   * Stop listener
   */
  async stop() {
    if (this._watchdogTimer) {
      clearInterval(this._watchdogTimer);
      this._watchdogTimer = null;
    }
    if (this.client && this.isRunning) {
      await this.client.disconnect();
      this.isRunning = false;
      console.log('⏹️  Premium channel listener stopped');
    }
  }

  /**
   * Fetch channel message history for backtest analysis
   */
  async getChannelHistory(limit = 500) {
    if (!this.client || !this.isRunning || !this.channelEntity) {
      return { error: 'Premium channel listener not connected' };
    }

    try {
      const messages = await this.client.getMessages(this.channelEntity, { limit });
      const results = [];

      for (const msg of messages) {
        const text = msg.text || msg.message || '';
        if (!text) continue;

        const token_ca = this._extractSolAddress(text);
        let signal = null;
        
        // Parse New Trending signals
        if (text.includes('🔥') && text.includes('New Trending')) {
          signal = this._parseSignal(text);
          if (signal) signal.type = 'NEW_TRENDING';
        }
        // Parse ATH signals
        else if (text.includes('📈') && text.includes('ATH')) {
          signal = this._parseATHSignal(text);
          if (signal) signal.type = 'ATH';
        }
        // Other messages with token addresses
        else if (token_ca) {
          signal = {
            token_ca,
            chain: 'SOL',
            symbol: 'UNKNOWN',
            type: 'OTHER',
          };
        }

        if (!signal) continue;

        results.push({
          timestamp: new Date(msg.date * 1000).toISOString(),
          type: signal.type || 'UNKNOWN',
          symbol: signal.symbol || 'UNKNOWN',
          token_ca: signal.token_ca,
          market_cap: signal.market_cap || 0,
          market_cap_from: signal.market_cap_from || 0,
          gain_pct: signal.gain_pct || 0,
          is_ath: signal.is_ath || false,
          indices: signal.indices || null,
          text_preview: text.substring(0, 300),
        });
      }

      console.log(`📜 Fetched ${messages.length} messages, ${results.length} with tokens`);
      
      return {
        count: results.length,
        total_messages: messages.length,
        channel: 'Egeye AI Gems 100X Vip',
        signals: results,
      };
    } catch (error) {
      console.error(`❌ Failed to fetch channel history: ${error.message}`);
      return { error: error.message };
    }
  }

  /**
   * Get service status
   */
  getStatus() {
    return {
      is_running: this.isRunning,
      channel: 'Egeye AI Gems 100X Vip',
      channel_found: !!this.channelEntity,
      dedup_cache_size: this.recentTokens.size,
      callbacks_registered: this.signalCallbacks.length,
    };
  }
}
