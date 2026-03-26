import Database from 'better-sqlite3';

export class KlineRepository {
  hasColumn(tableName, columnName) {
    return this.db.prepare(`PRAGMA table_info(${tableName})`).all().some((row) => row.name === columnName);
  }

  constructor(dbPath) {
    this.dbPath = dbPath;
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.initSchema();
    const hasFetchedAt = this.hasColumn('kline_1m', 'fetched_at');
    this.insertBarStmt = hasFetchedAt
      ? this.db.prepare(`
          INSERT OR REPLACE INTO kline_1m (token_ca, pool_address, timestamp, open, high, low, close, volume, provider, fetched_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `)
      : this.db.prepare(`
          INSERT OR REPLACE INTO kline_1m (token_ca, pool_address, timestamp, open, high, low, close, volume, provider)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
    this.insertTradeStmt = this.db.prepare(`
      INSERT OR IGNORE INTO helius_trades (signature, slot, block_time, token_ca, pool_address, price, base_amount, quote_amount, volume, side, source, ingested_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    this.upsertPoolStmt = this.db.prepare(`
      INSERT INTO pool_mapping (token_ca, pool_address, provider, fetched_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(token_ca) DO UPDATE SET
        pool_address = excluded.pool_address,
        provider = excluded.provider,
        fetched_at = excluded.fetched_at
    `);
    this.upsertCursorStmt = this.db.prepare(`
      INSERT INTO history_backfill_cursor (pool_address, token_ca, oldest_signature_seen, newest_signature_seen, oldest_block_time, newest_block_time, last_backfill_at, status, error)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(pool_address) DO UPDATE SET
        token_ca = excluded.token_ca,
        oldest_signature_seen = COALESCE(excluded.oldest_signature_seen, history_backfill_cursor.oldest_signature_seen),
        newest_signature_seen = COALESCE(excluded.newest_signature_seen, history_backfill_cursor.newest_signature_seen),
        oldest_block_time = COALESCE(excluded.oldest_block_time, history_backfill_cursor.oldest_block_time),
        newest_block_time = COALESCE(excluded.newest_block_time, history_backfill_cursor.newest_block_time),
        last_backfill_at = excluded.last_backfill_at,
        status = excluded.status,
        error = excluded.error
    `);
  }

  initSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS kline_1m (
        token_ca TEXT NOT NULL,
        pool_address TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        provider TEXT DEFAULT 'geckoterminal',
        fetched_at INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY (token_ca, timestamp)
      );
      CREATE INDEX IF NOT EXISTS idx_kline_1m_lookup ON kline_1m(token_ca, timestamp);
      CREATE TABLE IF NOT EXISTS pool_mapping (
        token_ca TEXT PRIMARY KEY,
        pool_address TEXT,
        provider TEXT,
        fetched_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS helius_trades (
        signature TEXT PRIMARY KEY,
        slot INTEGER,
        block_time INTEGER,
        token_ca TEXT NOT NULL,
        pool_address TEXT NOT NULL,
        price REAL NOT NULL,
        base_amount REAL,
        quote_amount REAL,
        volume REAL,
        side TEXT,
        source TEXT DEFAULT 'helius',
        ingested_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_helius_trades_pool_time ON helius_trades(pool_address, block_time);
      CREATE INDEX IF NOT EXISTS idx_helius_trades_token_time ON helius_trades(token_ca, block_time);
      CREATE TABLE IF NOT EXISTS history_backfill_cursor (
        pool_address TEXT PRIMARY KEY,
        token_ca TEXT,
        oldest_signature_seen TEXT,
        newest_signature_seen TEXT,
        oldest_block_time INTEGER,
        newest_block_time INTEGER,
        last_backfill_at INTEGER,
        status TEXT,
        error TEXT
      );
    `);

    try { this.db.exec(`ALTER TABLE kline_1m ADD COLUMN provider TEXT DEFAULT 'geckoterminal'`); } catch {}
    try { this.db.exec(`ALTER TABLE kline_1m ADD COLUMN fetched_at INTEGER`); } catch {}
    try { this.db.exec(`ALTER TABLE pool_mapping ADD COLUMN provider TEXT`); } catch {}
    try { this.db.exec(`ALTER TABLE pool_mapping ADD COLUMN fetched_at INTEGER`); } catch {}
  }

  getPoolMapping(tokenCa) {
    return this.db.prepare(`SELECT token_ca, pool_address, provider, fetched_at FROM pool_mapping WHERE token_ca = ?`).get(tokenCa) || null;
  }

  getLatestCursorPoolHint(tokenCa) {
    return this.db.prepare(`
      SELECT token_ca, pool_address, status, error, newest_block_time, oldest_block_time, last_backfill_at
      FROM history_backfill_cursor
      WHERE token_ca = ?
        AND pool_address IS NOT NULL
        AND TRIM(pool_address) != ''
      ORDER BY COALESCE(newest_block_time, last_backfill_at, oldest_block_time, 0) DESC, COALESCE(last_backfill_at, 0) DESC
      LIMIT 1
    `).get(tokenCa) || null;
  }

  getLikelyTradePoolHint(tokenCa) {
    return this.db.prepare(`
      SELECT token_ca, pool_address, COUNT(*) AS trade_count, MAX(block_time) AS latest_block_time
      FROM helius_trades
      WHERE token_ca = ?
        AND pool_address IS NOT NULL
        AND TRIM(pool_address) != ''
      GROUP BY token_ca, pool_address
      ORDER BY COALESCE(latest_block_time, 0) DESC, trade_count DESC, pool_address ASC
      LIMIT 1
    `).get(tokenCa) || null;
  }

  getLatestKlinePoolHint(tokenCa) {
    return this.db.prepare(`
      SELECT token_ca, pool_address, provider, COUNT(*) AS bar_count, MAX(timestamp) AS latest_timestamp
      FROM kline_1m
      WHERE token_ca = ?
        AND pool_address IS NOT NULL
        AND TRIM(pool_address) != ''
      GROUP BY token_ca, pool_address, provider
      ORDER BY COALESCE(latest_timestamp, 0) DESC, bar_count DESC, pool_address ASC
      LIMIT 1
    `).get(tokenCa) || null;
  }

  upsertPoolMapping(tokenCa, poolAddress, provider = 'unknown') {
    this.upsertPoolStmt.run(tokenCa, poolAddress, provider, Math.floor(Date.now() / 1000));
  }

  getBars(tokenCa, startTs, endTs) {
    return this.db.prepare(`
      SELECT timestamp, open, high, low, close, volume, provider, pool_address
      FROM kline_1m
      WHERE token_ca = ? AND timestamp >= ? AND timestamp <= ?
      ORDER BY timestamp ASC
    `).all(tokenCa, startTs, endTs);
  }

  getBarsBefore(tokenCa, signalTsSec, limit) {
    return this.db.prepare(`
      SELECT timestamp, open, high, low, close, volume, provider, pool_address
      FROM kline_1m
      WHERE token_ca = ? AND timestamp < ?
      ORDER BY timestamp DESC
      LIMIT ?
    `).all(tokenCa, signalTsSec, limit);
  }

  upsertBars(tokenCa, poolAddress, bars = [], provider = 'helius') {
    if (!bars.length) return 0;
    const now = Math.floor(Date.now() / 1000);
    const tx = this.db.transaction((items) => {
      for (const bar of items) {
        if (this.hasColumn('kline_1m', 'fetched_at')) {
          this.insertBarStmt.run(
            tokenCa,
            poolAddress || '',
            Number(bar.timestamp),
            Number(bar.open),
            Number(bar.high),
            Number(bar.low),
            Number(bar.close),
            Number(bar.volume || 0),
            provider,
            now
          );
        } else {
          this.insertBarStmt.run(
            tokenCa,
            poolAddress || '',
            Number(bar.timestamp),
            Number(bar.open),
            Number(bar.high),
            Number(bar.low),
            Number(bar.close),
            Number(bar.volume || 0),
            provider
          );
        }
      }
    });
    tx(bars);
    return bars.length;
  }

  upsertTrades(trades = []) {
    if (!trades.length) return 0;
    const now = Math.floor(Date.now() / 1000);
    let inserted = 0;
    const tx = this.db.transaction((items) => {
      for (const trade of items) {
        const info = this.insertTradeStmt.run(
          trade.signature,
          trade.slot || 0,
          trade.blockTime,
          trade.tokenCa,
          trade.poolAddress,
          trade.price,
          trade.baseAmount || 0,
          trade.quoteAmount || 0,
          trade.volume || 0,
          trade.side || null,
          trade.source || 'helius',
          now
        );
        inserted += info.changes || 0;
      }
    });
    tx(trades);
    return inserted;
  }

  getTrades(tokenCa, startTs, endTs, poolAddress = null) {
    if (poolAddress) {
      return this.db.prepare(`
        SELECT signature, slot, block_time, token_ca, pool_address, price, base_amount, quote_amount, volume, side, source
        FROM helius_trades
        WHERE token_ca = ? AND pool_address = ? AND block_time >= ? AND block_time <= ?
        ORDER BY block_time ASC
      `).all(tokenCa, poolAddress, startTs, endTs);
    }

    return this.db.prepare(`
      SELECT signature, slot, block_time, token_ca, pool_address, price, base_amount, quote_amount, volume, side, source
      FROM helius_trades
      WHERE token_ca = ? AND block_time >= ? AND block_time <= ?
      ORDER BY block_time ASC
    `).all(tokenCa, startTs, endTs);
  }

  getCursor(poolAddress) {
    return this.db.prepare(`SELECT * FROM history_backfill_cursor WHERE pool_address = ?`).get(poolAddress) || null;
  }

  listRecentCursors(limit = 20) {
    return this.db.prepare(`
      SELECT pool_address, token_ca, oldest_signature_seen, newest_signature_seen, oldest_block_time, newest_block_time, last_backfill_at, status, error
      FROM history_backfill_cursor
      ORDER BY COALESCE(last_backfill_at, 0) DESC
      LIMIT ?
    `).all(limit);
  }

  updateCursor(cursor) {
    this.upsertCursorStmt.run(
      cursor.poolAddress,
      cursor.tokenCa || null,
      cursor.oldestSignatureSeen || null,
      cursor.newestSignatureSeen || null,
      cursor.oldestBlockTime || null,
      cursor.newestBlockTime || null,
      Math.floor(Date.now() / 1000),
      cursor.status || 'ok',
      cursor.error || null
    );
  }

  getStats() {
    const barCount = this.db.prepare(`SELECT COUNT(*) AS c FROM kline_1m`).get().c;
    const heliusBarCount = this.db.prepare(`SELECT COUNT(*) AS c FROM kline_1m WHERE provider = 'helius'`).get().c;
    const tradeCount = this.db.prepare(`SELECT COUNT(*) AS c FROM helius_trades`).get().c;
    return { barCount, heliusBarCount, tradeCount };
  }

  close() {
    try { this.db.close(); } catch {}
  }
}

export default KlineRepository;
