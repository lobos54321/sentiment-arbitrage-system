/**
 * Premium Channel Listener (Egeye AI Gems 100X Vip)
 *
 * Uses Telegram User API (MTProto) to monitor a private paid signal channel.
 * Parses "🔥 New Trending" and "📈 ATH" messages for token signals.
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
import {
  compareMessageIds,
  mergeGapStaging,
  normalizeTelegramMessage,
  readWatermarkState,
  writeWatermarkState,
} from './premium-telegram-watermark.js';

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

function envEnabled(name, defaultValue = true) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || raw === '') return defaultValue;
  return !['0', 'false', 'no', 'off'].includes(String(raw).trim().toLowerCase());
}

function boundedInteger(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

export class PremiumChannelListener {
  constructor(config = {}) {
    this.config = config;
    this.captureOnly = config.captureOnly === true;
    this.captureOnlyReason = config.captureOnlyReason || null;
    this.client = null;
    this.isRunning = false;
    this.channelEntity = null;
    this.signalCallbacks = [];
    this.recentTokens = new Map(); // token_ca -> timestamp for dedup
    this._watchdogTimer = null;
    this._watchdogBootstrapTimer = null;
    this._lastPingTs = Date.now();
    this._reconnecting = false;
    const dataDir = process.env.DATA_DIR || '/app/data';
    const channelId = parseInt(process.env.PREMIUM_CHANNEL_ID || String(DEFAULT_CHANNEL_ID));
    this._watermarkEnabled = config.watermarkEnabled ?? envEnabled('TELEGRAM_WATERMARK_WATCHDOG_ENABLED', true);
    this._watermarkStatePath = config.watermarkStatePath
      || process.env.TELEGRAM_INGESTION_WATERMARK_PATH
      || path.join(dataDir, 'recovery', 'telegram-ingestion-watermark.json');
    this._gapStagingPath = config.gapStagingPath
      || process.env.TELEGRAM_GAP_STAGING_PATH
      || path.join(dataDir, 'recovery', 'telegram-gap-staging.json');
    this._watermarkHistoryLimit = boundedInteger(
      config.watermarkHistoryLimit ?? process.env.TELEGRAM_WATERMARK_HISTORY_LIMIT,
      100,
      10,
      500,
    );
    this._watermarkLagThreshold = boundedInteger(
      config.watermarkLagThreshold ?? process.env.TELEGRAM_WATERMARK_LAG_CHECKS,
      2,
      1,
      10,
    );
    this._watermarkCheckInFlight = false;
    this._watermarkState = readWatermarkState(this._watermarkStatePath, channelId);
    // Webhook URLs for real-time signal forwarding (comma-separated in env)
    this.webhookUrls = (this.captureOnly ? '' : (process.env.SIGNAL_WEBHOOK_URLS || ''))
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

      if (this.captureOnly && this._watermarkEnabled) {
        this._persistWatermark({
          status: 'capture_only_starting',
          capture_only: true,
          degraded_reason: this.captureOnlyReason,
          execution_allowed: false,
          emitted_to_signal_callbacks: false,
          written_to_premium_signals: false,
          last_error: null,
        });
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
          await this._primeUpdateStream();
          this.isRunning = true;
          this._lastPingTs = Date.now();
          this._startWatchdog();
          this._recordListenerStarted();
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
      await this._primeUpdateStream();

      this.isRunning = true;
      this._lastPingTs = Date.now();
      this._startWatchdog();
      this._recordListenerStarted();
      console.log('✅ Premium channel listener started');
      return true;

    } catch (error) {
      this.isRunning = false;
      if (this.captureOnly && this._watermarkEnabled) {
        this._recordWatermarkError(error, 'capture_only_start_failed');
      }
      console.error('❌ Failed to start premium channel listener:', error.message);
      return false;
    }
  }

  _recordListenerStarted() {
    if (!this._watermarkEnabled) return;
    this._persistWatermark({
      status: this.captureOnly ? 'capture_only_listening' : 'listener_started',
      capture_only: this.captureOnly,
      degraded_reason: this.captureOnly ? this.captureOnlyReason : null,
      execution_allowed: this.captureOnly ? false : null,
      emitted_to_signal_callbacks: this.captureOnly ? false : null,
      written_to_premium_signals: this.captureOnly ? false : null,
      last_error: null,
    });
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

  async _primeUpdateStream() {
    try {
      await this.client.getMe(true);
      await this.client.invoke(new Api.updates.GetState());
      this._lastPingTs = Date.now();
      console.log('✅ Telegram update stream primed');
    } catch (error) {
      console.warn(`⚠️ Telegram update stream prime failed: ${error.message}`);
    }
  }

  /**
   * Start watchdog timer — detects dead Telegram connection and reconnects
   */
  _startWatchdog() {
    if (this._watchdogTimer) clearInterval(this._watchdogTimer);
    if (this._watchdogBootstrapTimer) clearTimeout(this._watchdogBootstrapTimer);
    this._watchdogTimer = setInterval(() => this._watchdogTick(), WATCHDOG_INTERVAL_MS);
    this._watchdogBootstrapTimer = setTimeout(() => this._watchdogTick(), 10000);
    this._watchdogBootstrapTimer.unref?.();
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
        return;
      }
    } else if (!connected || stale) {
      console.warn(`⚠️ [Watchdog] Telegram connection dead (connected=${connected}, stale=${stale}) — triggering reconnect`);
      await this._doReconnect();
      return;
    }

    if (!this._watermarkEnabled || !this.channelEntity) return;
    try {
      const result = await this._checkMessageWatermark();
      if (result?.reconnect) {
        console.warn(
          `⚠️ [Watchdog] Telegram history advanced to message ${result.latest_message_id} `
          + `without a live event; staged=${result.staged_count}, reconnecting`,
        );
        await this._doReconnect();
      }
    } catch (error) {
      this._recordWatermarkError(error);
      console.warn(`⚠️ [Watchdog] Telegram watermark check failed: ${error.message}`);
    }
  }

  _persistWatermark(patch = {}) {
    this._watermarkState = writeWatermarkState(this._watermarkStatePath, {
      ...this._watermarkState,
      ...patch,
    });
    return this._watermarkState;
  }

  _recordWatermarkError(error, status = 'watermark_check_error') {
    try {
      this._persistWatermark({
        status,
        last_error: error?.message || String(error),
      });
    } catch {}
  }

  _recordLiveMessage(message) {
    if (!this._watermarkEnabled) return;
    const channelId = this._watermarkState.channel_id || String(DEFAULT_CHANNEL_ID);
    const row = normalizeTelegramMessage(message, channelId);
    if (!row) return;

    let capturePatch = null;
    if (this.captureOnly && row.signal_kind) {
      const staging = mergeGapStaging(this._gapStagingPath, [row], {
        channel_id: channelId,
        reason: 'runtime_degraded_capture_only',
      });
      const lastStagedMessageId = compareMessageIds(
        row.message_id,
        this._watermarkState.last_staged_message_id,
      ) >= 0
        ? row.message_id
        : this._watermarkState.last_staged_message_id;
      capturePatch = {
        status: 'capture_only_live_signal_staged',
        last_staged_message_id: lastStagedMessageId,
        last_staged_at: new Date().toISOString(),
        staged_row_count: staging.row_count,
      };
    }

    if (compareMessageIds(row.message_id, this._watermarkState.last_live_message_id) < 0) {
      if (capturePatch) {
        this._persistWatermark({
          ...capturePatch,
          status: 'capture_only_out_of_order_signal_staged',
        });
      }
      return;
    }

    const patch = {
      status: 'live_event_seen',
      last_live_message_id: row.message_id,
      last_live_message_ts: row.message_ts,
      last_error: null,
      ...(capturePatch || {}),
    };
    if (
      this._watermarkState.pending_history_message_id
      && compareMessageIds(row.message_id, this._watermarkState.pending_history_message_id) >= 0
    ) {
      patch.pending_history_message_id = null;
      patch.pending_lag_checks = 0;
      patch.status = this.captureOnly
        ? 'capture_only_live_signal_staged'
        : 'live_stream_healthy';
    }
    try {
      this._persistWatermark(patch);
    } catch (error) {
      console.warn(`⚠️ [Watchdog] Could not persist live Telegram watermark: ${error.message}`);
    }
  }

  async _checkMessageWatermark() {
    if (this._watermarkCheckInFlight) return { skipped: 'check_in_flight' };
    if (!this.client || !this.channelEntity) return { skipped: 'listener_not_ready' };
    this._watermarkCheckInFlight = true;
    try {
      const messages = await this.client.getMessages(this.channelEntity, {
        limit: this._watermarkHistoryLimit,
      });
      const rows = messages
        .map((message) => normalizeTelegramMessage(message, this._watermarkState.channel_id))
        .filter(Boolean)
        .sort((left, right) => compareMessageIds(right.message_id, left.message_id));
      if (rows.length === 0) return { skipped: 'history_empty' };

      const latest = rows[0];
      const previousHistoryId = this._watermarkState.last_history_message_id;
      if (!previousHistoryId) {
        this._persistWatermark({
          status: 'history_baseline_initialized',
          last_history_message_id: latest.message_id,
          last_history_message_ts: latest.message_ts,
          pending_history_message_id: null,
          pending_lag_checks: 0,
          last_error: null,
        });
        return { baseline_initialized: true, latest_message_id: latest.message_id };
      }

      const liveCaughtUp = compareMessageIds(
        this._watermarkState.last_live_message_id,
        latest.message_id,
      ) >= 0;
      if (liveCaughtUp) {
        this._persistWatermark({
          status: 'live_stream_healthy',
          last_history_message_id: latest.message_id,
          last_history_message_ts: latest.message_ts,
          pending_history_message_id: null,
          pending_lag_checks: 0,
          last_error: null,
        });
        return { healthy: true, latest_message_id: latest.message_id };
      }

      const historyAdvanced = compareMessageIds(latest.message_id, previousHistoryId) > 0;
      const samePending = compareMessageIds(
        latest.message_id,
        this._watermarkState.pending_history_message_id,
      ) === 0;
      if (!historyAdvanced && !samePending) {
        return { healthy: null, latest_message_id: latest.message_id };
      }

      const pendingLagChecks = samePending
        ? Number(this._watermarkState.pending_lag_checks || 0) + 1
        : 1;
      this._persistWatermark({
        status: 'live_stream_lag_observed',
        last_history_message_id: latest.message_id,
        last_history_message_ts: latest.message_ts,
        pending_history_message_id: latest.message_id,
        pending_lag_checks: pendingLagChecks,
        last_error: null,
      });
      if (pendingLagChecks < this._watermarkLagThreshold) {
        return {
          lag_observed: true,
          lag_checks: pendingLagChecks,
          latest_message_id: latest.message_id,
        };
      }

      const lowerBound = compareMessageIds(
        this._watermarkState.last_live_message_id,
        this._watermarkState.last_staged_message_id,
      ) >= 0
        ? this._watermarkState.last_live_message_id
        : this._watermarkState.last_staged_message_id;
      const gapRows = rows.filter((row) => (
        row.signal_kind
        && compareMessageIds(row.message_id, lowerBound) > 0
      ));
      const staging = mergeGapStaging(this._gapStagingPath, gapRows, {
        channel_id: this._watermarkState.channel_id,
        reason: 'live_stream_watermark_gap',
      });
      this._persistWatermark({
        status: 'gap_staged_reconnect_pending',
        pending_history_message_id: null,
        pending_lag_checks: 0,
        last_staged_message_id: latest.message_id,
        last_staged_at: new Date().toISOString(),
        reconnect_count: Number(this._watermarkState.reconnect_count || 0) + 1,
        last_error: null,
      });
      return {
        reconnect: true,
        latest_message_id: latest.message_id,
        staged_count: staging.row_count,
      };
    } finally {
      this._watermarkCheckInFlight = false;
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
      const sourceMessageTs = message.date ? Number(message.date) * 1000 : null;
      this._recordLiveMessage(message);

      // Process 🔥 New Trending OR 📈 ATH messages
      if (text.includes('🔥') && text.includes('New Trending')) {
        console.log(`🔥 [NOT_ATH原文] ${text.substring(0, 500)}`);
        const signal = this._parseSignal(text);
        if (!signal) return;
        signal.source_message_ts = sourceMessageTs;
        signal.signal_source = signal.signal_source || signal.source || 'premium_channel';
        signal.source_event_id = signal.source_event_id
          || ['premium_channel', signal.token_ca, sourceMessageTs || signal.receive_ts || signal.timestamp, 'NEW_TRENDING'].join(':');
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
        signal.source_message_ts = sourceMessageTs;
        signal.signal_source = signal.signal_source || signal.source || 'premium_channel_ath';
        signal.source_event_id = signal.source_event_id
          || ['premium_channel_ath', signal.token_ca, sourceMessageTs || signal.receive_ts || signal.timestamp, 'ATH'].join(':');
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

    // Extract symbol from the `SYMBOL：$xxx` field. The channel has used different
    // leading emojis over time (📍 historically, 🪙 currently), so the field label
    // is the anchor, not the emoji. Fallback: the bold `**name**` header preceding
    // "New Trending" in the raw (pre-normalization) text.
    const symbolMatch = normalized.match(/SYMBOL\s*[：:]\s*\$?([^\s\n]+)/i);
    let symbol = symbolMatch ? symbolMatch[1].trim() : null;
    if (!symbol) {
      const headerMatch = text.match(/\*\*([^*\n]{1,32})\*\*\s*New\s+Trending/i);
      symbol = headerMatch ? headerMatch[1].trim() : null;
    }

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

    const receivedTs = Date.now();
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
      raw_message: text,
      channel: 'Egeye AI Gems 100X Vip',
      timestamp: receivedTs,
      source_message_ts: null,
      receive_ts: receivedTs,
      signal_type: 'NEW_TRENDING',
      is_ath: false,
      parse_status: 'parsed',
      parse_missing_fields: [
        symbol ? null : 'symbol',
        market_cap > 0 ? null : 'market_cap',
        holders > 0 ? null : 'holders',
        top10_pct > 0 ? null : 'top10_pct'
      ].filter(Boolean),
      source: 'premium_channel',
    };
  }

  /**
   * Parse 📈 ATH message
   * Format: "📈New ATH $KIRBY is up **51%** 📈",
   * "📈New ATH $PMPR is up **15.43X** 📈", or "ATH $Mike 2.6X".
   */
  _parseATHSignal(text) {
    const normalized = text
      .replace(/[\u200B-\u200D\uFEFF]/g, '')
      .replace(/\r/g, '');

    // Extract symbol and gain: $SYMBOL is up **XX%**, $SYMBOL is up **X.XXX**,
    // or compact premium-channel form: ATH $SYMBOL X.XX.
    const athMatch = normalized.match(/(?:New\s+)?ATH\s+\$([^\s`*]+)(?:\s+(?:is\s+)?up)?\s+\*{0,2}([\d.]+)\s*(%|X)\*{0,2}/i)
      || normalized.match(/\$([^\s`*]+)\s+(?:is\s+)?up\s+\*{0,2}([\d.]+)\s*(%|X)\*{0,2}/i);
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
    const mcMatch = normalized.match(/\$([\d,.]+)\s*([KMBkmb])?\s*[—\-]+>\s*\$([\d,.]+)\s*([KMBkmb])?/);
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

    const receivedTs = Date.now();
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
      raw_message: text,
      channel: 'Egeye AI Gems 100X Vip',
      timestamp: receivedTs,
      source_message_ts: null,
      receive_ts: receivedTs,
      signal_type: 'ATH',
      parse_status: 'parsed',
      parse_missing_fields: [
        symbol ? null : 'symbol',
        mc > 0 ? null : 'market_cap'
      ].filter(Boolean),
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

      // ATH delta format:
      //   Label：(signal)116🔮 --> (current)124🔮
      //   Label：(signal)87 --> 244 🔺180%
      const reDelta = new RegExp(
        escaped
        + '[：:]\\s*[\\(（]signal[\\)）]\\s*x?(\\d+)\\s*(?:🔮)?\\s*'
        + '(?:-->|->|→|—>)\\s*(?:[\\(（]current[\\)）]\\s*)?x?(\\d+)',
        'i'
      );
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
    if (this.captureOnly) {
      console.log(`📥 [TelegramCaptureOnly] staged $${signal.symbol || 'UNKNOWN'}; execution disabled`);
      return;
    }

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
    if (this._watchdogBootstrapTimer) {
      clearTimeout(this._watchdogBootstrapTimer);
      this._watchdogBootstrapTimer = null;
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
      capture_only: this.captureOnly,
      watermark_enabled: this._watermarkEnabled,
      watermark_status: this._watermarkState?.status || null,
      last_live_message_id: this._watermarkState?.last_live_message_id || null,
      last_history_message_id: this._watermarkState?.last_history_message_id || null,
    };
  }
}

export async function startPremiumTelegramCaptureOnlyDegraded(error, options = {}) {
  if (error?.code !== 'KLINE_DB_UNHEALTHY') {
    return { handled: false, started: false, listener: null };
  }
  const ListenerClass = options.ListenerClass || PremiumChannelListener;
  const listener = new ListenerClass({
    watermarkEnabled: true,
    captureOnly: true,
    captureOnlyReason: 'kline_db_unhealthy',
  });
  const started = await listener.start();
  return { handled: true, started, listener };
}
