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
    if (!(durationMs > 0)) return;
    this.cooldowns.set(provider, nowMs() + durationMs);
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

  async runSingleFlight(key, producer) {
    if (this.inflight.has(key)) {
      return this.inflight.get(key);
    }

    const promise = Promise.resolve()
      .then(producer)
      .finally(() => {
        this.inflight.delete(key);
      });

    this.inflight.set(key, promise);
    return promise;
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
      if (this.isCoolingDown(provider)) {
        return {
          ok: false,
          provider,
          error: 'cooldown_active',
          rateLimited: true,
          fetchedAt: nowMs(),
          cooldownRemainingMs: this.getCooldownRemaining(provider)
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
      if (rateLimited && cooldownMs > 0) {
        this.setCooldown(provider, cooldownMs);
      }

      const normalized = {
        ok: !response?.error,
        provider,
        data: response?.error ? null : response,
        error: response?.error || null,
        status: response?.status || null,
        rateLimited,
        fetchedAt: nowMs(),
        cacheHit: false
      };

      if (normalized.ok && cacheTtlMs > 0) {
        await this.setCache(requestKey, normalized, cacheTtlMs);
      }

      return normalized;
    };

    return useSingleFlight
      ? this.runSingleFlight(`fetch:${requestKey}`, execute)
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

export default SharedMarketRuntime;
