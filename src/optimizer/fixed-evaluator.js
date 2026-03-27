import fs from 'fs';
import path from 'path';
import Database from 'better-sqlite3';
import autonomyConfig from '../config/autonomy-config.js';
import { PaperStrategyRegistry } from '../config/paper-strategy-registry.js';
import signalDatabase from '../database/signal-database.js';
import { MarketDataBackfillService } from '../market-data/market-data-backfill-service.js';
import { fetchWithRetry } from '../utils/fetch-with-retry.js';
import { RateLimiter } from '../utils/rate-limiter.js';

function median(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function percentile(values, p) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.max(0, Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length)));
  return sorted[index];
}

function maxDrawdown(values) {
  let peak = 0;
  let worst = 0;
  let equity = 0;
  for (const value of values) {
    equity += value;
    peak = Math.max(peak, equity);
    worst = Math.min(worst, equity - peak);
  }
  return Math.abs(worst);
}

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

export class FixedEvaluator {
  constructor(config = autonomyConfig) {
    this.config = config;
    this.registry = new PaperStrategyRegistry();
    this.exportPath = this.config.exports.filePath;
    this.sourceDb = new Database(this.config.dbPath);
    this.klineDb = new Database(this.config.evaluator.klineCacheDbPath);
    this.readOnlyKlineDbs = this.#openReadOnlyKlineDbs();
    this.marketDataBackfill = new MarketDataBackfillService(this.config);
    this.geckoLimiter = new RateLimiter(0.2, 1);
    this.dexLimiter = new RateLimiter(0.75, 1);
    this.geckoCooldownUntil = 0;
    this.#initKlineTables();
  }

  loadDataset() {
    if (fs.existsSync(this.exportPath)) {
      const parsed = JSON.parse(fs.readFileSync(this.exportPath, 'utf8'));
      const premiumSignals = parsed.tables?.premium_signals?.rows || parsed.premium_signals || [];
      if (premiumSignals.length) {
        return premiumSignals;
      }
    }

    return this.#loadLocalDataset();
  }

  async #throttledFetchJson(url, { provider, timeout = 20000 } = {}) {
    const isDex = provider === 'dexscreener';
    const limiter = isDex ? this.dexLimiter : this.geckoLimiter;

    if (!isDex && this.geckoCooldownUntil > Date.now()) {
      await new Promise((resolve) => setTimeout(resolve, this.geckoCooldownUntil - Date.now()));
    }

    await limiter.throttle();

    const response = await fetchWithRetry(url, {
      source: isDex ? 'DEXSCREENER' : 'GECKOTERMINAL',
      timeout,
      maxRetries: isDex ? 3 : 2,
      initialDelay: isDex ? 1200 : 2500,
      maxDelay: isDex ? 8000 : 20000,
      silent: true,
      headers: { accept: 'application/json' }
    });

    if (response?.error) {
      if (!isDex && /HTTP 429/.test(response.error)) {
        this.geckoCooldownUntil = Date.now() + 30000;
      }
      throw new Error(response.error);
    }

    if (!isDex) {
      this.geckoCooldownUntil = 0;
    }

    return response;
  }

  async fetchTokenPool(ca) {
    try {
      const url = `https://api.geckoterminal.com/api/v2/networks/solana/tokens/${ca}/pools?page=1`;
      const response = await this.#throttledFetchJson(url, { provider: 'geckoterminal', timeout: 15000 });
      const pool = response?.data?.[0];
      const poolId = pool?.id || null;
      const poolAddress = pool?.attributes?.address || (poolId?.startsWith('solana_') ? poolId.replace(/^solana_/, '') : poolId);
      return { poolId, poolAddress };
    } catch (error) {
      return { poolId: null, poolAddress: null, error: error.message };
    }
  }

  async fetchDexScreenerPair(tokenCa) {
    try {
      const dsUrl = `https://api.dexscreener.com/latest/dex/tokens/${tokenCa}`;
      const response = await this.#throttledFetchJson(dsUrl, { provider: 'dexscreener', timeout: 15000 });
      const pairs = (response?.pairs || []).filter((pair) => pair.chainId === 'solana');
      const pair = pairs.sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0))[0] || null;
      return pair;
    } catch (error) {
      return { error: error.message };
    }
  }

  async fetchGeckoBars(poolId, bars) {
    const url = `https://api.geckoterminal.com/api/v2/networks/solana/pools/${poolId}/ohlcv/minute?aggregate=1&limit=${bars}&token=base`;
    const response = await this.#throttledFetchJson(url, { provider: 'geckoterminal', timeout: 20000 });
    const list = response?.data?.attributes?.ohlcv_list || [];
    return list.map(([timestamp, open, high, low, close, volume]) => ({ timestamp, open, high, low, close, volume }));
  }

  async fetchDexBackfilledBars(tokenCa, entryTsSec, bars) {
    const pair = await this.fetchDexScreenerPair(tokenCa);
    if (!pair?.pairAddress) {
      return { provider: null, poolId: pair?.pairAddress || null, bars: [], error: pair?.error || 'no_pair' };
    }

    const windows = [entryTsSec + 600, entryTsSec + 3600];
    const byTs = new Map();
    let geckoError = null;

    for (const windowEnd of windows) {
      try {
        const url = `https://api.geckoterminal.com/api/v2/networks/solana/pools/${pair.pairAddress}/ohlcv/minute?aggregate=1&limit=${Math.min(200, bars)}&before_timestamp=${windowEnd}&token=base`;
        const response = await this.#throttledFetchJson(url, { provider: 'geckoterminal', timeout: 20000 });
        const list = response?.data?.attributes?.ohlcv_list || [];
        for (const row of list) {
          const ts = Number(row[0]);
          if (!byTs.has(ts)) {
            byTs.set(ts, {
              timestamp: ts,
              open: Number(row[1]),
              high: Number(row[2]),
              low: Number(row[3]),
              close: Number(row[4]),
              volume: Number(row[5])
            });
          }
        }
      } catch (error) {
        geckoError = error.message;
      }
    }

    return {
      provider: byTs.size ? 'dexscreener+geckoterminal' : null,
      poolId: pair.pairAddress,
      bars: [...byTs.values()].sort((a, b) => a.timestamp - b.timestamp).slice(-bars),
      error: byTs.size ? null : geckoError || 'no_ohlcv'
    };
  }

  async fetchKlines({ tokenCa, signalTsSec = Math.floor(Date.now() / 1000), poolId, bars = this.config.evaluator.maxHistoricalBars }) {
    let resolvedPoolId = poolId;
    let resolvedPoolAddress = null;
    const out = { provider: null, poolId: null, bars: [], error: null };

    if (!resolvedPoolId) {
      const poolResult = await this.fetchTokenPool(tokenCa);
      resolvedPoolId = poolResult.poolId || poolResult.poolAddress;
      resolvedPoolAddress = poolResult.poolAddress;
      if (poolResult.error) {
        out.error = `token-pools:${poolResult.error}`;
      }
    }

    if (resolvedPoolId) {
      const normalizedPoolId = resolvedPoolId.startsWith('solana_') ? resolvedPoolId : `solana_${resolvedPoolId}`;
      try {
        const geckoBars = await this.fetchGeckoBars(normalizedPoolId, bars);
        if (geckoBars.length) {
          out.provider = 'geckoterminal';
          out.poolId = normalizedPoolId;
          out.bars = geckoBars;
          return out;
        }
        out.error = out.error || 'geckoterminal:no_data';
      } catch (error) {
        out.error = `geckoterminal:${error.message}`;
      }

      if (resolvedPoolAddress && resolvedPoolAddress !== resolvedPoolId) {
        try {
          const geckoBars = await this.fetchGeckoBars(`solana_${resolvedPoolAddress}`, bars);
          if (geckoBars.length) {
            out.provider = 'geckoterminal';
            out.poolId = `solana_${resolvedPoolAddress}`;
            out.bars = geckoBars;
            return out;
          }
        } catch (error) {
          out.error = `geckoterminal-address:${error.message}`;
        }
      }
    }

    const dexFallback = await this.fetchDexBackfilledBars(tokenCa, signalTsSec, bars);
    if (dexFallback.bars.length) {
      return dexFallback;
    }

    return {
      provider: null,
      poolId: dexFallback.poolId || resolvedPoolId || null,
      bars: [],
      error: dexFallback.error || out.error || 'no_ohlcv'
    };
  }

  #tableExists(db, tableName) {
    const row = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = ?").get(tableName);
    return !!row;
  }

  #loadLocalDataset() {
    if (!this.#tableExists(this.sourceDb, 'premium_signals')) {
      return [];
    }

    const premiumSignals = this.sourceDb.prepare(`
      SELECT id, token_ca, symbol, market_cap, description, timestamp, hard_gate_status, ai_action, ai_confidence, ai_narrative_tier
      FROM premium_signals
      ORDER BY timestamp DESC
      LIMIT 1000
    `).all();

    const outcomesByToken = this.#tableExists(this.sourceDb, 'signal_outcomes')
      ? new Map(this.sourceDb.prepare(`
          SELECT token_ca, pnl_percent, max_gain_percent, max_drawdown_percent,
                 CASE
                   WHEN entry_time IS NOT NULL AND exit_time IS NOT NULL AND exit_time > entry_time
                   THEN (exit_time - entry_time) / 60.0
                   ELSE NULL
                 END as holding_minutes,
                 exit_reason
          FROM signal_outcomes
          ORDER BY rowid DESC
        `).all().map((row) => [row.token_ca, row]))
      : new Map();

    const shadowByToken = this.#tableExists(this.sourceDb, 'shadow_pnl')
      ? new Map(this.sourceDb.prepare(`
          SELECT token_ca, exit_pnl, high_pnl, (closed_at - entry_time) / 60000.0 as holding_minutes
          FROM shadow_pnl
          WHERE closed = 1
          ORDER BY rowid DESC
        `).all().map((row) => [row.token_ca, row]))
      : new Map();

    return premiumSignals.map((signal) => {
      const outcome = outcomesByToken.get(signal.token_ca) || shadowByToken.get(signal.token_ca) || null;
      return {
        ...signal,
        final_pnl: outcome?.pnl_percent ?? outcome?.exit_pnl ?? null,
        max_pnl: outcome?.max_gain_percent ?? outcome?.high_pnl ?? null,
        max_drawdown_percent: outcome?.max_drawdown_percent ?? outcome?.low_pnl ?? null,
        hold_duration_minutes: outcome?.holding_minutes ?? null,
        exit_reason: outcome?.exit_reason ?? null
      };
    });
  }

  #openReadOnlyKlineDbs() {
    const candidates = this.config.evaluator.klineCacheCandidates || [];
    const opened = [];
    for (const filePath of candidates) {
      if (!filePath || path.resolve(filePath) === path.resolve(this.config.evaluator.klineCacheDbPath)) {
        continue;
      }
      if (!fs.existsSync(filePath)) {
        continue;
      }
      try {
        opened.push({ path: filePath, db: new Database(filePath, { readonly: true }) });
      } catch {
        // ignore read-only cache open failures
      }
    }
    return opened;
  }

  #initKlineTables() {
    this.klineDb.exec(`
      CREATE TABLE IF NOT EXISTS kline_1m (
        token_ca TEXT NOT NULL,
        pool_address TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        PRIMARY KEY (token_ca, timestamp)
      );
      CREATE TABLE IF NOT EXISTS pool_mapping (
        token_ca TEXT PRIMARY KEY,
        pool_address TEXT,
        fetched_at INTEGER NOT NULL
      );
    `);
    try { this.klineDb.exec(`ALTER TABLE kline_1m ADD COLUMN provider TEXT DEFAULT 'geckoterminal'`); } catch {}
    try { this.klineDb.exec(`ALTER TABLE kline_1m ADD COLUMN fetched_at INTEGER DEFAULT (strftime('%s','now'))`); } catch {}
    try { this.klineDb.exec(`ALTER TABLE pool_mapping ADD COLUMN provider TEXT`); } catch {}
  }

  getCachedKlines(tokenCa, signalTsSec, lookaheadBars = this.config.evaluator.maxHistoricalBars) {
    const endTs = signalTsSec + (lookaheadBars * 60);
    const query = `
      SELECT timestamp, open, high, low, close, volume, pool_address, provider
      FROM kline_1m
      WHERE token_ca = ?
        AND timestamp >= ?
        AND timestamp <= ?
      ORDER BY timestamp ASC
    `;

    let rows = this.klineDb.prepare(query).all(tokenCa, signalTsSec, endTs);
    if (rows.length) {
      return rows;
    }

    for (const source of this.readOnlyKlineDbs) {
      try {
        rows = source.db.prepare(query).all(tokenCa, signalTsSec, endTs);
        if (rows.length) {
          return rows;
        }
      } catch {
        // ignore broken backup caches
      }
    }

    return [];
  }

  async backfillKlinesForSignal(signal, options = {}) {
    const tokenCa = signal.token_ca || signal.tokenCa;
    const signalTsSec = Math.floor((signal.timestamp || Date.now()) / 1000);
    const cacheOnly = options.cacheOnly === true || process.env.AUTONOMY_CACHE_ONLY_EVAL === 'true';
    const lookaheadBars = options.lookaheadBars || this.config.evaluator.maxHistoricalBars;
    let cached = this.getCachedKlines(tokenCa, signalTsSec, lookaheadBars);
    if (cached.length) {
      return { provider: 'cache', poolId: cached[0]?.pool_address || null, bars: cached, error: null };
    }

    if (cacheOnly) {
      return { provider: null, poolId: null, bars: [], error: 'cache_only_miss' };
    }

    const heliusResult = await this.marketDataBackfill.backfillWindow({
      tokenCa,
      signalTsSec,
      startTs: signalTsSec,
      endTs: signalTsSec + (lookaheadBars * 60),
      minBars: Math.max(2, Math.min(lookaheadBars, 5))
    });
    if (heliusResult.bars.length) {
      cached = this.getCachedKlines(tokenCa, signalTsSec, lookaheadBars);
      return {
        provider: heliusResult.provider || 'helius',
        poolId: heliusResult.poolAddress || null,
        bars: cached,
        error: heliusResult.error || null,
        metrics: {
          signaturesFetched: heliusResult.signaturesFetched,
          transactionsFetched: heliusResult.transactionsFetched,
          tradesInserted: heliusResult.tradesInserted,
          barsWritten: heliusResult.barsWritten,
          cacheHit: heliusResult.cacheHit
        }
      };
    }

    const fetched = await this.fetchKlines({ tokenCa, signalTsSec, bars: lookaheadBars });
    if (fetched.bars.length) {
      const hasProviderColumn = !!this.klineDb.prepare("SELECT 1 FROM pragma_table_info('kline_1m') WHERE name = 'provider'").get();
      const hasFetchedAtColumn = !!this.klineDb.prepare("SELECT 1 FROM pragma_table_info('kline_1m') WHERE name = 'fetched_at'").get();
      const hasPoolProviderColumn = !!this.klineDb.prepare("SELECT 1 FROM pragma_table_info('pool_mapping') WHERE name = 'provider'").get();

      const insertSql = hasProviderColumn && hasFetchedAtColumn
        ? `INSERT OR REPLACE INTO kline_1m (token_ca, pool_address, timestamp, open, high, low, close, volume, provider, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        : `INSERT OR REPLACE INTO kline_1m (token_ca, pool_address, timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
      const stmt = this.klineDb.prepare(insertSql);
      const tx = this.klineDb.transaction((bars) => {
        for (const bar of bars) {
          if (hasProviderColumn && hasFetchedAtColumn) {
            stmt.run(tokenCa, fetched.poolId || '', bar.timestamp, bar.open, bar.high, bar.low, bar.close, bar.volume, fetched.provider || 'unknown', Math.floor(Date.now() / 1000));
          } else {
            stmt.run(tokenCa, fetched.poolId || '', bar.timestamp, bar.open, bar.high, bar.low, bar.close, bar.volume);
          }
        }
      });
      tx(fetched.bars);
      if (fetched.poolId) {
        if (hasPoolProviderColumn) {
          this.klineDb.prepare(`INSERT OR REPLACE INTO pool_mapping (token_ca, pool_address, provider, fetched_at) VALUES (?, ?, ?, ?)`).run(tokenCa, fetched.poolId, fetched.provider || 'unknown', Math.floor(Date.now() / 1000));
        } else {
          this.klineDb.prepare(`INSERT OR REPLACE INTO pool_mapping (token_ca, pool_address, fetched_at) VALUES (?, ?, ?)`).run(tokenCa, fetched.poolId, Math.floor(Date.now() / 1000));
        }
      }
    }

    cached = this.getCachedKlines(tokenCa, signalTsSec, lookaheadBars);
    return {
      provider: fetched.provider,
      poolId: fetched.poolId,
      bars: cached,
      error: heliusResult.error || fetched.error || null,
      diagnostics: {
        heliusError: heliusResult.error || null,
        fallbackError: fetched.error || null,
        heliusProvider: heliusResult.provider || null,
        fallbackProvider: fetched.provider || null
      }
    };
  }

  async evaluateCandidate(candidate, baseline = this.registry.getBaseline(), options = {}) {
    const coveredOnly = options.coveredOnly === true;
    const datasetLimit = Number(options.datasetLimit || process.env.AUTONOMY_EVAL_DATASET_LIMIT || 0);
    const fullDataset = this.loadDataset();
    const dataset = datasetLimit > 0 ? fullDataset.slice(0, datasetLimit) : fullDataset;
    const candidateResults = [];
    const baselineResults = [];
    const progressEvery = Math.max(1, Number(process.env.AUTONOMY_EVAL_PROGRESS_EVERY || 25));
    let sourceMentions = 0;
    let coveredDatasetSize = 0;
    let uncoveredDatasetSize = 0;
    let evaluatedDatasetSize = 0;
    let baselineEligibleBuyCount = 0;
    let candidateEligibleBuyCount = 0;
    let baselineCoveredBuyCount = 0;
    let candidateCoveredBuyCount = 0;
    let baselineUncoveredBuyCount = 0;
    let candidateUncoveredBuyCount = 0;
    const coverageErrorBreakdown = {};
    const heliusErrorBreakdown = {};
    const fallbackErrorBreakdown = {};

    for (let index = 0; index < dataset.length; index += 1) {
      const signal = dataset[index];
      if ((index + 1) % progressEvery === 0 || index === 0 || index === dataset.length - 1) {
        console.log(`[eval] processing ${index + 1}/${dataset.length} ${signal?.symbol || signal?.token_ca || signal?.tokenCa || 'unknown'}`);
      }

      const normalizedSignal = {
        ...signal,
        token_ca: signal.token_ca || signal.tokenCa,
        is_ath: signal.description?.includes('ATH') || signal.is_ath === 1 || signal.is_ath === true,
        ai_confidence: signal.ai_confidence || 0,
        indices: signal.indices || this.#extractIndices(signal.description)
      };

      const klineData = await this.backfillKlinesForSignal(normalizedSignal, options);
      const hasCoverage = (klineData.bars?.length || 0) >= 2;

      if (hasCoverage) {
        coveredDatasetSize += 1;
      } else {
        uncoveredDatasetSize += 1;
        const errorKey = String(klineData.error || 'unknown');
        coverageErrorBreakdown[errorKey] = (coverageErrorBreakdown[errorKey] || 0) + 1;
        const heliusErrorKey = String(klineData.diagnostics?.heliusError || 'none');
        heliusErrorBreakdown[heliusErrorKey] = (heliusErrorBreakdown[heliusErrorKey] || 0) + 1;
        const fallbackErrorKey = String(klineData.diagnostics?.fallbackError || 'none');
        fallbackErrorBreakdown[fallbackErrorKey] = (fallbackErrorBreakdown[fallbackErrorKey] || 0) + 1;
      }

      if (coveredOnly && !hasCoverage) {
        continue;
      }

      evaluatedDatasetSize += 1;
      const baselineDecision = this.registry.evaluateSignal(normalizedSignal, baseline);
      const candidateDecision = this.registry.evaluateSignal(normalizedSignal, candidate);

      if (baselineDecision.action === 'BUY') {
        baselineEligibleBuyCount += 1;
        if (hasCoverage) {
          baselineCoveredBuyCount += 1;
          baselineResults.push(this.#simulateTradeOutcome(normalizedSignal, baselineDecision, baseline.strategyConfig, klineData.bars));
        } else {
          baselineUncoveredBuyCount += 1;
        }
      }
      if (candidateDecision.action === 'BUY') {
        candidateEligibleBuyCount += 1;
        if (hasCoverage) {
          candidateCoveredBuyCount += 1;
          candidateResults.push(this.#simulateTradeOutcome(normalizedSignal, candidateDecision, candidate.strategyConfig, klineData.bars));
        } else {
          candidateUncoveredBuyCount += 1;
        }
      }

      const mentions = signalDatabase.getUniqueChannelCount(normalizedSignal.token_ca || '', 120);
      if (mentions > 0) sourceMentions += 1;
    }

    const baselineMetrics = this.#summarize(baselineResults, sourceMentions, evaluatedDatasetSize);
    const candidateMetrics = this.#summarize(candidateResults, sourceMentions, evaluatedDatasetSize);
    candidateMetrics.comparisonToBaseline = Number((candidateMetrics.expectancy - baselineMetrics.expectancy).toFixed(4));

    return {
      datasetSize: dataset.length,
      evaluatedDatasetSize,
      mode: { coveredOnly },
      coverage: {
        coveredDatasetSize,
        uncoveredDatasetSize,
        baselineEligibleBuyCount,
        candidateEligibleBuyCount,
        baselineCoveredBuyCount,
        candidateCoveredBuyCount,
        baselineUncoveredBuyCount,
        candidateUncoveredBuyCount,
        baselineSimulatedSampleSize: baselineMetrics.sampleSize,
        candidateSimulatedSampleSize: candidateMetrics.sampleSize,
        errorBreakdown: coverageErrorBreakdown,
        heliusErrorBreakdown,
        fallbackErrorBreakdown
      },
      baselineMetrics,
      candidateMetrics
    };
  }

  #simulateTradeOutcome(signal, decision, strategyConfig, bars = []) {
    const stopLossPct = Math.abs(toNumber(strategyConfig.paperExitRules?.stopLossPct, 35)) / 100;
    const takeProfits = (strategyConfig.paperExitRules?.takeProfitPct || [50, 100, 200])
      .map((value) => Math.abs(toNumber(value, 0)) / 100)
      .filter((value) => value > 0)
      .sort((a, b) => a - b);
    const timeoutBars = Math.max(1, Math.min(bars.length || 1, toNumber(strategyConfig.paperExitRules?.timeoutMinutes, 30)));

    if (bars.length < 2) {
      const pnl = Number(signal.trade_result || signal.final_pnl || signal.exit_pnl || 0);
      return {
        pnl,
        holdMinutes: toNumber(signal.hold_duration_minutes, timeoutBars),
        isGold: pnl >= 100,
        falsePositive: pnl < -20,
        exitReason: 'fallback',
        peakPnl: pnl,
        strategyId: decision.strategyId
      };
    }

    const entryBar = bars[0];
    const entryPrice = toNumber(entryBar.close, toNumber(entryBar.open, 0));
    if (entryPrice <= 0) {
      return {
        pnl: 0,
        holdMinutes: 0,
        isGold: false,
        falsePositive: false,
        exitReason: 'invalid_entry',
        peakPnl: 0,
        strategyId: decision.strategyId
      };
    }

    let bestPnl = -Infinity;
    let exitPnl = 0;
    let exitReason = 'timeout';
    let holdMinutes = 0;
    let realizedStages = 0;

    for (let i = 1; i < Math.min(bars.length, timeoutBars + 1); i += 1) {
      const bar = bars[i];
      const highPnl = (toNumber(bar.high, entryPrice) - entryPrice) / entryPrice;
      const lowPnl = (toNumber(bar.low, entryPrice) - entryPrice) / entryPrice;
      const closePnl = (toNumber(bar.close, entryPrice) - entryPrice) / entryPrice;
      holdMinutes = Math.max(1, Math.round((toNumber(bar.timestamp, 0) - toNumber(entryBar.timestamp, 0)) / 60));
      bestPnl = Math.max(bestPnl, highPnl, closePnl);

      if (lowPnl <= -stopLossPct) {
        exitPnl = -stopLossPct * 100;
        exitReason = 'stop_loss';
        break;
      }

      while (realizedStages < takeProfits.length && highPnl >= takeProfits[realizedStages]) {
        realizedStages += 1;
      }

      if (realizedStages === takeProfits.length && takeProfits.length > 0) {
        exitPnl = takeProfits[takeProfits.length - 1] * 100;
        exitReason = 'take_profit';
        break;
      }

      exitPnl = closePnl * 100;
    }

    if (bestPnl === -Infinity) {
      bestPnl = exitPnl / 100;
    }

    return {
      pnl: Number(exitPnl.toFixed(4)),
      holdMinutes,
      isGold: bestPnl >= 1,
      falsePositive: exitPnl < -20,
      exitReason,
      peakPnl: Number((bestPnl * 100).toFixed(4)),
      strategyId: decision.strategyId
    };
  }

  #extractIndices(description = '') {
    const pick = (name) => {
      const re = new RegExp(`${name}[^0-9]*([0-9]+(?:\\.[0-9]+)?)`, 'i');
      const match = description.match(re);
      return match ? Number(match[1]) : 0;
    };

    return {
      super_index: { current: pick('super') },
      trade_index: { current: pick('trade'), signal: 0 },
      address_index: { current: pick('addr') },
      security_index: { current: pick('sec') }
    };
  }

  #summarize(results, sourceMentions, datasetSize) {
    const pnls = results.map((item) => item.pnl);
    const wins = pnls.filter((value) => value > 0);
    const losses = pnls.filter((value) => value <= 0);
    const grossProfit = wins.reduce((sum, value) => sum + value, 0);
    const grossLoss = Math.abs(losses.reduce((sum, value) => sum + value, 0));
    const falsePositives = results.filter((item) => item.falsePositive).length;
    const missedGold = Math.max(0, datasetSize - results.filter((item) => item.isGold).length);
    const total = results.length;

    return {
      sampleSize: total,
      winRate: total ? wins.length / total : 0,
      avgPnl: total ? pnls.reduce((sum, value) => sum + value, 0) / total : 0,
      medianPnl: median(pnls),
      expectancy: total ? pnls.reduce((sum, value) => sum + value, 0) / total / 100 : 0,
      profitFactor: grossLoss > 0 ? grossProfit / grossLoss : grossProfit > 0 ? 99 : 0,
      maxDrawdown: maxDrawdown(pnls),
      tailLoss95: Math.abs(percentile(pnls, 5)),
      falsePositiveRate: total ? falsePositives / total : 0,
      missedGoldRate: datasetSize ? missedGold / datasetSize : 0,
      sourceDiversity: datasetSize ? sourceMentions / datasetSize : 0,
      holdingTimeMedian: median(results.map((item) => item.holdMinutes))
    };
  }

  close() {
    try { this.sourceDb.close(); } catch {}
    try { this.marketDataBackfill.close(); } catch {}
    try { this.klineDb.close(); } catch {}
    for (const source of this.readOnlyKlineDbs) {
      try { source.db.close(); } catch {}
    }
  }
}

export default FixedEvaluator;
