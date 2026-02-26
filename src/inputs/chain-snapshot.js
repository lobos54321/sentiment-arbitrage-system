/**
 * Chain Snapshot Aggregator
 *
 * Unified interface for getting on-chain snapshots
 * Routes to appropriate chain-specific module
 * Handles caching and error recovery
 */

import SolanaSnapshotService from './chain-snapshot-sol.js';
import BSCSnapshotService from './chain-snapshot-bsc.js';

export class ChainSnapshotAggregator {
  constructor(config, db) {
    this.config = config;
    this.db = db;

    // Initialize chain-specific services
    this.solService = new SolanaSnapshotService(config);
    this.bscService = new BSCSnapshotService(config);

    // Cache to avoid duplicate requests within short time
    this.cache = new Map();
    this.cacheT TL = 60 * 1000; // 60 seconds
  }

  /**
   * Main entry: Get snapshot for any chain
   *
   * @param {string} chain - 'SOL' or 'BSC'
   * @param {string} tokenCA - Token contract address
   * @param {number} plannedPosition - Position size (for slippage test)
   * @returns {Promise<Object>} Standardized snapshot data
   */
  async getSnapshot(chain, tokenCA, plannedPosition = null) {
    console.log(`🔍 [Aggregator] Getting snapshot: ${chain}/${tokenCA}`);

    // Check cache
    const cacheKey = `${chain}:${tokenCA}:${plannedPosition}`;
    const cached = this.getCached(cacheKey);
    if (cached) {
      console.log('✅ [Aggregator] Using cached snapshot');
      return cached;
    }

    try {
      let snapshot;

      if (chain === 'SOL') {
        snapshot = await this.solService.getSnapshot(tokenCA, plannedPosition);
      } else if (chain === 'BSC') {
        snapshot = await this.bscService.getSnapshot(tokenCA, plannedPosition);
      } else {
        throw new Error(`Unsupported chain: ${chain}`);
      }

      // Add chain to result
      snapshot.chain = chain;
      snapshot.token_ca = tokenCA;

      // Cache result
      this.setCache(cacheKey, snapshot);

      // Persist to database
      await this.persistSnapshot(snapshot);

      return snapshot;
    } catch (error) {
      console.error(`❌ [Aggregator] Snapshot error for ${chain}/${tokenCA}:`, error);

      // Return Unknown snapshot
      return {
        chain,
        token_ca: tokenCA,
        ...this.getUnknownSnapshot(chain),
        error: error.message
      };
    }
  }

  /**
   * Persist snapshot to database (gates table)
   */
  async persistSnapshot(snapshot) {
    try {
      const stmt = this.db.prepare(`
        INSERT INTO gates (
          token_ca,
          evaluated_at,
          hard_status,
          hard_reasons,
          freeze_authority,
          mint_authority,
          lp_status,
          honeypot,
          tax_buy,
          tax_sell,
          tax_mutable,
          owner_type,
          dangerous_functions,
          exit_status,
          exit_reasons,
          top10_percent,
          liquidity,
          liquidity_unit,
          slippage_sell_20pct,
          wash_flag,
          key_risk_wallets,
          vol_24h_usd,
          sell_constraints_flag
        ) VALUES (
          ?, ?, 'PENDING', NULL,
          ?, ?, ?, ?, ?, ?, ?, ?,
          ?, 'PENDING', NULL,
          ?, ?, ?, ?, ?, ?, ?, ?
        )
      `);

      stmt.run(
        snapshot.token_ca,
        snapshot.snapshot_time,
        // SOL fields (or null for BSC)
        snapshot.freeze_authority || null,
        snapshot.mint_authority || null,
        snapshot.lp_status || null,
        // BSC fields (or null for SOL)
        snapshot.honeypot || null,
        snapshot.tax_buy || null,
        snapshot.tax_sell || null,
        snapshot.tax_mutable ? 1 : 0,
        snapshot.owner_type || null,
        snapshot.dangerous_functions ? JSON.stringify(snapshot.dangerous_functions) : null,
        // Common fields
        snapshot.top10_percent || null,
        snapshot.liquidity || null,
        snapshot.liquidity_unit || null,
        snapshot.slippage_sell_20pct || null,
        snapshot.wash_flag || null,
        snapshot.key_risk_wallets ? JSON.stringify(snapshot.key_risk_wallets) : null,
        snapshot.vol_24h_usd || null,
        snapshot.sell_constraints_flag ? 1 : 0
      );

      console.log('✅ [Aggregator] Snapshot persisted to database');
    } catch (error) {
      console.error('❌ [Aggregator] Failed to persist snapshot:', error.message);
    }
  }

  /**
   * Get cached snapshot
   */
  getCached(key) {
    const cached = this.cache.get(key);
    if (!cached) return null;

    const age = Date.now() - cached.timestamp;
    if (age > this.cacheTTL) {
      this.cache.delete(key);
      return null;
    }

    return cached.data;
  }

  /**
   * Set cache
   */
  setCache(key, data) {
    this.cache.set(key, {
      data,
      timestamp: Date.now()
    });

    // Clean old cache entries periodically
    if (this.cache.size > 1000) {
      this.cleanCache();
    }
  }

  /**
   * Clean expired cache entries
   */
  cleanCache() {
    const now = Date.now();
    for (const [key, value] of this.cache.entries()) {
      if (now - value.timestamp > this.cacheTTL) {
        this.cache.delete(key);
      }
    }
  }

  /**
   * Get Unknown snapshot template for a chain
   */
  getUnknownSnapshot(chain) {
    if (chain === 'SOL') {
      return {
        freeze_authority: 'Unknown',
        mint_authority: 'Unknown',
        lp_status: 'Unknown',
        liquidity: null,
        liquidity_unit: 'SOL',
        top10_percent: null,
        slippage_sell_20pct: null,
        wash_flag: 'Unknown',
        key_risk_wallets: [],
        snapshot_time: Date.now(),
        data_source: 'Failed'
      };
    } else if (chain === 'BSC') {
      return {
        honeypot: 'Unknown',
        tax_buy: null,
        tax_sell: null,
        tax_mutable: null,
        owner_type: 'Unknown',
        dangerous_functions: [],
        lp_lock: null,
        liquidity: null,
        liquidity_unit: 'BNB',
        vol_24h_usd: null,
        top10_percent: null,
        sell_constraints_flag: null,
        snapshot_time: Date.now(),
        data_source: 'Failed'
      };
    }

    return {};
  }

  /**
   * Batch get snapshots for multiple tokens
   */
  async getBatchSnapshots(requests) {
    console.log(`📦 [Aggregator] Batch fetching ${requests.length} snapshots`);

    const results = await Promise.allSettled(
      requests.map(req => this.getSnapshot(req.chain, req.tokenCA, req.plannedPosition))
    );

    return results.map((result, i) => ({
      request: requests[i],
      status: result.status,
      data: result.status === 'fulfilled' ? result.value : null,
      error: result.status === 'rejected' ? result.reason : null
    }));
  }

  /**
   * Get latest snapshot from database
   */
  getLatestSnapshotFromDB(tokenCA) {
    const stmt = this.db.prepare(`
      SELECT * FROM gates
      WHERE token_ca = ?
      ORDER BY evaluated_at DESC
      LIMIT 1
    `);

    return stmt.get(tokenCA);
  }

  /**
   * Check if snapshot needs refresh
   */
  needsRefresh(tokenCA, maxAgeMinutes = 5) {
    const latest = this.getLatestSnapshotFromDB(tokenCA);
    if (!latest) return true;

    const ageMinutes = (Date.now() - latest.evaluated_at) / (1000 * 60);
    return ageMinutes > maxAgeMinutes;
  }
}

export default ChainSnapshotAggregator;
