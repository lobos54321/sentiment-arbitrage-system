/**
 * Permanent Blacklist Service
 *
 * Manages permanent blacklisting of tokens that trigger critical Exit Gate conditions
 *
 * Blacklist Triggers:
 * - Liquidity collapse (> 50% drop from initial)
 * - Key Risk Wallet mass exit (> 80% holdings sold)
 * - Rug pull detection (ownership transfer + liquidity drain)
 * - Confirmed Honeypot (buy success but sell fails with high gas)
 *
 * Unlike temporary cooldown (30min), permanent blacklist is永久生效
 */

export class PermanentBlacklistService {
  constructor(db) {
    this.db = db;
    this.initDatabase();
  }

  /**
   * Initialize permanent_blacklist table
   */
  initDatabase() {
    try {
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS permanent_blacklist (
          token_ca TEXT PRIMARY KEY,
          chain TEXT NOT NULL,
          blacklist_reason TEXT NOT NULL,
          blacklist_timestamp INTEGER NOT NULL,
          initial_liquidity REAL,
          final_liquidity REAL,
          exit_tx_hash TEXT,
          deployer_address TEXT,
          additional_data TEXT
        )
      `);

      console.log('✅ [Permanent Blacklist] Database initialized');
    } catch (error) {
      console.error('❌ [Permanent Blacklist] Failed to init database:', error.message);
    }
  }

  /**
   * Check if token is permanently blacklisted
   *
   * @param {string} tokenCA - Token contract address
   * @param {string} chain - Chain identifier (SOL/BSC/ETH)
   * @returns {Object|null} Blacklist record if exists, null otherwise
   */
  isBlacklisted(tokenCA, chain) {
    try {
      const stmt = this.db.prepare(`
        SELECT * FROM permanent_blacklist
        WHERE token_ca = ? AND chain = ?
      `);

      const record = stmt.get(tokenCA, chain);
      return record || null;
    } catch (error) {
      console.error('❌ [Permanent Blacklist] Check failed:', error.message);
      return null;
    }
  }

  /**
   * Add token to permanent blacklist
   *
   * @param {Object} params - Blacklist parameters
   * @param {string} params.token_ca - Token contract address
   * @param {string} params.chain - Chain identifier
   * @param {string} params.reason - Blacklist reason
   * @param {number} params.initial_liquidity - Initial liquidity USD
   * @param {number} params.final_liquidity - Final liquidity USD
   * @param {string} params.exit_tx_hash - Exit transaction hash (optional)
   * @param {string} params.deployer_address - Deployer address (optional)
   * @param {Object} params.additional_data - Additional metadata (optional)
   */
  addToBlacklist({
    token_ca,
    chain,
    reason,
    initial_liquidity = null,
    final_liquidity = null,
    exit_tx_hash = null,
    deployer_address = null,
    additional_data = null
  }) {
    try {
      const stmt = this.db.prepare(`
        INSERT OR REPLACE INTO permanent_blacklist (
          token_ca,
          chain,
          blacklist_reason,
          blacklist_timestamp,
          initial_liquidity,
          final_liquidity,
          exit_tx_hash,
          deployer_address,
          additional_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);

      stmt.run(
        token_ca,
        chain,
        reason,
        Date.now(),
        initial_liquidity,
        final_liquidity,
        exit_tx_hash,
        deployer_address,
        additional_data ? JSON.stringify(additional_data) : null
      );

      console.log(`🚫 [Permanent Blacklist] Added: ${chain}/${token_ca}`);
      console.log(`   Reason: ${reason}`);
      if (initial_liquidity !== null && final_liquidity !== null) {
        const dropPct = ((initial_liquidity - final_liquidity) / initial_liquidity * 100).toFixed(1);
        console.log(`   Liquidity drop: $${initial_liquidity.toFixed(0)} → $${final_liquidity.toFixed(0)} (-${dropPct}%)`);
      }

      return true;
    } catch (error) {
      console.error('❌ [Permanent Blacklist] Add failed:', error.message);
      return false;
    }
  }

  /**
   * Remove token from blacklist (manual override only)
   *
   * @param {string} tokenCA - Token contract address
   * @param {string} chain - Chain identifier
   */
  removeFromBlacklist(tokenCA, chain) {
    try {
      const stmt = this.db.prepare(`
        DELETE FROM permanent_blacklist
        WHERE token_ca = ? AND chain = ?
      `);

      stmt.run(tokenCA, chain);
      console.log(`✅ [Permanent Blacklist] Removed: ${chain}/${tokenCA}`);
      return true;
    } catch (error) {
      console.error('❌ [Permanent Blacklist] Remove failed:', error.message);
      return false;
    }
  }

  /**
   * Get all blacklisted tokens
   *
   * @param {string} chain - Optional chain filter
   * @returns {Array} Array of blacklist records
   */
  getAllBlacklisted(chain = null) {
    try {
      let stmt;
      if (chain) {
        stmt = this.db.prepare('SELECT * FROM permanent_blacklist WHERE chain = ? ORDER BY blacklist_timestamp DESC');
        return stmt.all(chain);
      } else {
        stmt = this.db.prepare('SELECT * FROM permanent_blacklist ORDER BY blacklist_timestamp DESC');
        return stmt.all();
      }
    } catch (error) {
      console.error('❌ [Permanent Blacklist] Query failed:', error.message);
      return [];
    }
  }

  /**
   * Get blacklist statistics
   *
   * @returns {Object} Blacklist stats by chain and reason
   */
  getStats() {
    try {
      const total = this.db.prepare('SELECT COUNT(*) as count FROM permanent_blacklist').get();

      const byChain = this.db.prepare(`
        SELECT chain, COUNT(*) as count
        FROM permanent_blacklist
        GROUP BY chain
      `).all();

      const byReason = this.db.prepare(`
        SELECT blacklist_reason, COUNT(*) as count
        FROM permanent_blacklist
        GROUP BY blacklist_reason
        ORDER BY count DESC
      `).all();

      return {
        total: total.count,
        by_chain: byChain,
        by_reason: byReason
      };
    } catch (error) {
      console.error('❌ [Permanent Blacklist] Stats failed:', error.message);
      return { total: 0, by_chain: [], by_reason: [] };
    }
  }

  /**
   * Check if Exit Gate condition should trigger permanent blacklist
   *
   * @param {Object} exitGateResult - Exit gate evaluation result
   * @param {Object} snapshot - Current chain snapshot
   * @param {Object} initialSnapshot - Initial snapshot (from database)
   * @returns {Object|null} Blacklist params if should blacklist, null otherwise
   */
  shouldBlacklistFromExitGate(exitGateResult, snapshot, initialSnapshot) {
    // Check for critical Exit Gate failures
    const criticalReasons = [
      'LIQUIDITY_COLLAPSE',
      'KEY_RISK_WALLET_EXIT',
      'RUG_PULL_DETECTED',
      'HONEYPOT_CONFIRMED'
    ];

    // Extract failure type from exit gate result
    const failureType = this.detectCriticalFailure(exitGateResult, snapshot, initialSnapshot);

    if (failureType && criticalReasons.includes(failureType)) {
      return {
        token_ca: snapshot.token_ca,
        chain: snapshot.chain,
        reason: failureType,
        initial_liquidity: initialSnapshot?.liquidity_usd || snapshot.initial_liquidity,
        final_liquidity: snapshot.liquidity_usd,
        deployer_address: snapshot.deployer_address || null
      };
    }

    return null;
  }

  /**
   * Detect critical failure type from exit gate result
   *
   * @private
   */
  detectCriticalFailure(exitGateResult, snapshot, initialSnapshot) {
    // 1. Liquidity Collapse (> 50% drop)
    const initialLiquidity = initialSnapshot?.liquidity_usd || snapshot.initial_liquidity;
    if (initialLiquidity && snapshot.liquidity_usd) {
      const dropPct = (initialLiquidity - snapshot.liquidity_usd) / initialLiquidity;
      if (dropPct > 0.5) {
        return 'LIQUIDITY_COLLAPSE';
      }
    }

    // 2. Key Risk Wallet Exit (> 80% holdings sold)
    // This would be detected in Exit Gate's risk wallet monitoring
    if (exitGateResult.reasons?.some(r => r.includes('Key Risk Wallet') && r.includes('exit'))) {
      return 'KEY_RISK_WALLET_EXIT';
    }

    // 3. Rug Pull (ownership transfer + liquidity drain)
    if (exitGateResult.reasons?.some(r => r.includes('ownership') && r.includes('transfer'))) {
      if (initialLiquidity && snapshot.liquidity_usd < initialLiquidity * 0.3) {
        return 'RUG_PULL_DETECTED';
      }
    }

    // 4. Honeypot Confirmed (buy succeeds but sell fails with high gas)
    if (exitGateResult.reasons?.some(r => r.includes('Honeypot') || r.includes('gas'))) {
      return 'HONEYPOT_CONFIRMED';
    }

    return null;
  }
}

export default PermanentBlacklistService;
