import { createClient } from 'redis';
import { fetchWithRetry } from '../utils/fetch-with-retry.js';
import { RateLimiter } from '../utils/rate-limiter.js';

function envBool(name, defaultValue = false) {
  const value = process.env[name];
  if (value == null || value === '') return defaultValue;
  return value === 'true';
}

function nowMs() {
  return Date.now();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export const MARKET_DATA_REASON = Object.freeze({
  RATE_LIMITED: 'RATE_LIMITED',
  COOLDOWN_ACTIVE: 'COOLDOWN_ACTIVE',
  UPSTREAM_UNAVAILABLE: 'UPSTREAM_UNAVAILABLE',
  NO_ROUTE: 'NO_ROUTE',
  NO_POOL: 'NO_POOL',
  UNKNOWN_DATA: 'UNKNOWN_DATA',
  TOKEN_NOT_TRADABLE: 'TOKEN_NOT_TRADABLE',
  MISSING_INPUTS: 'MISSING_INPUTS'
});

export function normalizeMarketDataReason({ error = null, status = null, rateLimited = false } = {}) {
  const rawError = error == null ? '' : String(error).trim();
  const normalizedError = rawError.toLowerCase();

  if (normalizedError === 'cooldown_active' || normalizedError === MARKET_DATA_REASON.COOLDOWN_ACTIVE.toLowerCase()) {
    return MARKET_DATA_REASON.COOLDOWN_ACTIVE;
  }
  if (rateLimited || status === 429 || normalizedError === 'rate_limited' || normalizedError === 'rate_limited_429' || /429/.test(normalizedError)) {
    return MARKET_DATA_REASON.RATE_LIMITED;
  }
  if (normalizedError === 'no_route' || /no route|could not find any route/.test(normalizedError)) {
    return MARKET_DATA_REASON.NO_ROUTE;
  }
  if (normalizedError === 'no_pool' || normalizedError === 'no_pair') {
    return MARKET_DATA_REASON.NO_POOL;
  }
  if (normalizedError === 'unknown_data' || normalizedError === 'null_response' || normalizedError === 'no_bars' || normalizedError === 'no_ohlcv') {
    return MARKET_DATA_REASON.UNKNOWN_DATA;
  }
  if (normalizedError === 'token_not_tradable') {
    return MARKET_DATA_REASON.TOKEN_NOT_TRADABLE;
  }
  if (normalizedError === 'missing_token' || normalizedError === 'missing_quote_inputs') {
    return MARKET_DATA_REASON.MISSING_INPUTS;
  }
  if (rawError) {
    return MARKET_DATA_REASON.UPSTREAM_UNAVAILABLE;
  }
  return null;
}

export class SharedMarketRuntime {
  constructor(options = {}) {
    this.namespace = options.namespace || 'market-data';
    this.redisUrl = options.redisUrl || process.env.REDIS_URL || 'redis://127.0.0.1:6379';
    this.redisEnabled = options.redisEnabled ?? (process.env.REDIS_ENABLED !== 'false');
    this.sharedRedisEnabled = options.sharedRedisEnabled ?? envBool('MARKET_DATA_SHARED_REDIS_CACHE', false);
    this.localCache = new Map();
    this.inflight = new Map();
    this.limiters = new Map();
    this.cooldowns = new Map();
    this.singleFlightPollMs = options.singleFlightPollMs || 75;
    this.singleFlightLockMs = options.singleFlightLockMs || 10000;
    this.singleFlightWaitMs = options.singleFlightWaitMs || 12000;
    this.redisClient = null;
    this.redisConnectPromise = null;
    this.redisFailed = false;
  }

  getLimiter(provider, { requestsPerSecond = 1, burstCapacity = 1 } = {}) {
    const key = `${provider}:${requestsPerSecond}:${burstCapacity}`;
    if (!this.limiters.has(key)) {
      this.limiters.set(key, new RateLimiter(requestsPerSecond, burstCapacity));
    }
    return this.limiters.get(key);
  }

  async throttle(provider, limiterConfig = {}) {
    const limiter = this.getLimiter(provider, limiterConfig);
    await limiter.throttle();
  }

  getCooldownRemaining(provider) {
    return Math.max(0, (this.cooldowns.get(provider) || 0) - nowMs());
  }

  isCoolingDown(provider) {
    return this.getCooldownRemaining(provider) > 0;
  }

  setCooldown(provider, durationMs) {
    if (!(durationMs > 0)) return 0;
    const expiresAt = nowMs() + durationMs;
    this.cooldowns.set(provider, expiresAt);
    return expiresAt;
  }

  getLocalCache(key) {
    const entry = this.localCache.get(key);
    if (!entry) return null;
    if (entry.expiresAt <= nowMs()) {
      this.localCache.delete(key);
      return null;
    }
    return entry.value;
  }

  setLocalCache(key, value, ttlMs) {
    if (!(ttlMs > 0)) return;
    this.localCache.set(key, {
      value,
      expiresAt: nowMs() + ttlMs
    });
  }

  async runSingleFlight(key, producer, options = {}) {
    if (this.inflight.has(key)) {
      return this.inflight.get(key);
    }

    const promise = Promise.resolve()
      .then(async () => {
        if (options.distributed !== false) {
          return this.runDistributedSingleFlight(key, producer, options);
        }
        return producer();
      })
      .finally(() => {
        this.inflight.delete(key);
      });

    this.inflight.set(key, promise);
    return promise;
  }

  async runDistributedSingleFlight(key, producer, options = {}) {
    const {
      cacheKey = null,
      cacheReader = cacheKey ? (() => this.getCache(cacheKey)) : null,
      lockMs = this.singleFlightLockMs,
      waitMs = this.singleFlightWaitMs,
      pollMs = this.singleFlightPollMs,
      onFollowerTimeout = null
    } = options;

    const client = await this.#ensureRedisClient();
    if (!client?.isOpen) {
      return producer();
    }

    const lockKey = this.#redisKey(`singleflight:${key}:lock`);
    const token = `${process.pid}:${nowMs()}:${Math.random().toString(36).slice(2)}`;

    try {
      const leader = await client.set(lockKey, token, { PX: lockMs, NX: true });
      if (leader === 'OK') {
        try {
          return await producer();
        } finally {
          await this.#releaseDistributedSingleFlightLock(lockKey, token);
        }
      }
    } catch {
      this.redisFailed = true;
      return producer();
    }

    const startedAt = nowMs();
    while (nowMs() - startedAt < waitMs) {
      if (typeof cacheReader === 'function') {
        const shared = await cacheReader();
        if (shared) {
          return { ...shared, cacheHit: true, sharedFlightFollower: true };
        }
      }
      await sleep(pollMs);
    }

    if (typeof onFollowerTimeout === 'function') {
      const timedOut = await onFollowerTimeout();
      if (timedOut) return timedOut;
    }

    return producer();
  }

  async getCache(key) {
    const local = this.getLocalCache(key);
    if (local) return local;

    if (!this.sharedRedisEnabled) {
      return null;
    }

    const client = await this.#ensureRedisClient();
    if (!client?.isOpen) {
      return null;
    }

    try {
      const raw = await client.get(this.#redisKey(key));
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (parsed?.expiresAt && parsed.expiresAt <= nowMs()) {
        return null;
      }
      if (parsed?.value) {
        const remainingTtl = parsed.expiresAt ? Math.max(250, parsed.expiresAt - nowMs()) : 0;
        this.setLocalCache(key, parsed.value, remainingTtl);
        return parsed.value;
      }
      return null;
    } catch {
      this.redisFailed = true;
      return null;
    }
  }

  async setCache(key, value, ttlMs) {
    if (!(ttlMs > 0)) return;
    this.setLocalCache(key, value, ttlMs);

    if (!this.sharedRedisEnabled) {
      return;
    }

    const client = await this.#ensureRedisClient();
    if (!client?.isOpen) {
      return;
    }

    try {
      await client.set(
        this.#redisKey(key),
        JSON.stringify({ value, expiresAt: nowMs() + ttlMs }),
        { PX: ttlMs }
      );
    } catch {
      this.redisFailed = true;
    }
  }

  async getSharedCooldown(provider) {
    const localRemaining = this.getCooldownRemaining(provider);
    if (localRemaining > 0) {
      return localRemaining;
    }

    const client = await this.#ensureRedisClient();
    if (!client?.isOpen) {
      return 0;
    }

    try {
      const raw = await client.get(this.#redisKey(`cooldown:${provider}`));
      if (!raw) return 0;
      const parsed = JSON.parse(raw);
      const expiresAt = Number(parsed?.expiresAt || 0);
      const remainingMs = Math.max(0, expiresAt - nowMs());
      if (remainingMs > 0) {
        this.setCooldown(provider, remainingMs);
      }
      return remainingMs;
    } catch {
      this.redisFailed = true;
      return 0;
    }
  }

  async setSharedCooldown(provider, durationMs) {
    const expiresAt = this.setCooldown(provider, durationMs);
    if (!(durationMs > 0) || !expiresAt) {
      return 0;
    }

    const client = await this.#ensureRedisClient();
    if (!client?.isOpen) {
      return durationMs;
    }

    try {
      await client.set(
        this.#redisKey(`cooldown:${provider}`),
        JSON.stringify({ provider, expiresAt }),
        { PX: durationMs }
      );
    } catch {
      this.redisFailed = true;
    }
    return Math.max(0, expiresAt - nowMs());
  }

  async fetchJson(url, options = {}) {
    const {
      provider = 'external',
      source = provider.toUpperCase(),
      requestKey = `${provider}:${url}`,
      cacheTtlMs = 0,
      cooldownMs = 0,
      limiter,
      timeout = 15000,
      maxRetries = 2,
      initialDelay = 1000,
      maxDelay = 20000,
      headers = { accept: 'application/json' },
      silent = true,
      useSingleFlight = true,
      force = false,
      retryOn403 = true
    } = options;

    if (!force && cacheTtlMs > 0) {
      const cached = await this.getCache(requestKey);
      if (cached) {
        return { ...cached, cacheHit: true };
      }
    }

    const execute = async () => {
      const cooldownRemainingMs = await this.getSharedCooldown(provider);
      if (cooldownRemainingMs > 0) {
        return {
          ok: false,
          provider,
          error: 'cooldown_active',
          reason: MARKET_DATA_REASON.COOLDOWN_ACTIVE,
          status: 429,
          rateLimited: true,
          fetchedAt: nowMs(),
          cooldownRemainingMs,
          cacheHit: false
        };
      }

      if (limiter) {
        await this.throttle(provider, limiter);
      }

      const response = await fetchWithRetry(url, {
        source,
        timeout,
        maxRetries,
        initialDelay,
        maxDelay,
        silent,
        headers,
        retryOn403
      });

      const rateLimited = Boolean(response?.status === 429 || /429/.test(String(response?.error || '')) || response?.error === 'cooldown_active');
      const reason = normalizeMarketDataReason({
        error: response?.error || null,
        status: response?.status || null,
        rateLimited
      });
      const sharedCooldownRemainingMs = rateLimited && cooldownMs > 0
        ? await this.setSharedCooldown(provider, cooldownMs)
        : 0;

      const normalized = {
        ok: !response?.error,
        provider,
        data: response?.error ? null : response,
        error: response?.error || null,
        reason,
        status: response?.status || null,
        rateLimited,
        fetchedAt: nowMs(),
        cooldownRemainingMs: sharedCooldownRemainingMs,
        cacheHit: false
      };

      if (normalized.ok && cacheTtlMs > 0) {
        await this.setCache(requestKey, normalized, cacheTtlMs);
      }

      return normalized;
    };

    return useSingleFlight
      ? this.runSingleFlight(`fetch:${requestKey}`, execute, {
        distributed: true,
        cacheKey: !force && cacheTtlMs > 0 ? requestKey : null
      })
      : execute();
  }

  async close() {
    const client = this.redisClient;
    this.redisClient = null;
    this.redisConnectPromise = null;
    if (!client?.isOpen) return;
    try {
      await client.quit();
    } catch {}
  }

  async #releaseDistributedSingleFlightLock(lockKey, token) {
    const client = this.redisClient;
    if (!client?.isOpen) {
      return;
    }
    try {
      const current = await client.get(lockKey);
      if (current === token) {
        await client.del(lockKey);
      }
    } catch {
      this.redisFailed = true;
    }
  }

  #redisKey(key) {
    return `${this.namespace}:${key}`;
  }

  async #ensureRedisClient() {
    if (!this.redisEnabled || !this.sharedRedisEnabled || this.redisFailed) {
      return null;
    }

    if (this.redisClient?.isOpen) {
      return this.redisClient;
    }

    if (!this.redisClient) {
      this.redisClient = createClient({ url: this.redisUrl });
      this.redisClient.on('error', () => {
        this.redisFailed = true;
      });
    }

    if (!this.redisConnectPromise) {
      this.redisConnectPromise = this.redisClient.connect()
        .then(() => {
          this.redisFailed = false;
          return this.redisClient;
        })
        .catch(() => {
          this.redisFailed = true;
          return null;
        })
        .finally(() => {
          this.redisConnectPromise = null;
        });
    }

    return this.redisConnectPromise;
  }
}

export function isMarketDataFlagEnabled(name, defaultValue = false) {
  return envBool(name, defaultValue);
}

export function isMarketDataUnifiedRolloutEnabled(defaultValue = true) {
  return envBool('MARKET_DATA_UNIFIED_ROLLOUT', defaultValue);
}

export function isMarketDataProcessEnabled(processFlagName, defaultValue = true) {
  return isMarketDataUnifiedRolloutEnabled(defaultValue) && envBool(processFlagName, defaultValue);
}

export function applyMarketDataProcessOverride(processFlagName, defaultValue = true) {
  const enabled = isMarketDataProcessEnabled(processFlagName, defaultValue);
  if (!enabled) {
    process.env.MARKET_DATA_SHARED_POOL_RESOLUTION = 'false';
    process.env.MARKET_DATA_SHARED_OHLCV = 'false';
    process.env.MARKET_DATA_SHARED_QUOTES = 'false';
    process.env.MARKET_DATA_SHARED_REDIS_CACHE = 'false';
  }
  return enabled;
}

export default SharedMarketRuntime;
