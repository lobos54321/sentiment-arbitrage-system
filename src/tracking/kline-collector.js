/**
 * K 线收集器
 *
 * 监听 LivePriceMonitor 的 price-update 事件，
 * 聚合成 1 分钟 OHLCV K 线存入 SQLite。
 *
 * 用途：为回测提供完整连续的历史价格数据。
 */

import Database from 'better-sqlite3';
import path from 'path';

export class KlineCollector {
  constructor(options = {}) {
    const dbPath = options.dbPath || path.join(process.cwd(), 'data', 'kline_cache.db');
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');

    // 内存中的当前分钟 tick 聚合 Map<tokenCA, {open, high, low, close, volume, minute}>
    this.currentBars = new Map();

    // 每分钟 flush 定时器
    this.flushInterval = null;
    this.flushIntervalMs = 60_000;

    // 统计
    this.stats = { ticks: 0, bars_written: 0, tokens_active: 0 };

    this._initDB();
  }

  _initDB() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS kline_1m (
        token_ca TEXT NOT NULL,
        pool_address TEXT NOT NULL DEFAULT '',
        timestamp INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL DEFAULT 0,
        PRIMARY KEY (token_ca, timestamp)
      )
    `);
    this.db.exec(`CREATE INDEX IF NOT EXISTS idx_kline_ts ON kline_1m(token_ca, timestamp)`);

    this._insertStmt = this.db.prepare(`
      INSERT OR REPLACE INTO kline_1m (token_ca, pool_address, timestamp, open, high, low, close, volume)
      VALUES (?, '', ?, ?, ?, ?, ?, ?)
    `);
  }

  /**
   * 绑定到 LivePriceMonitor，监听价格事件
   */
  attach(priceMonitor) {
    priceMonitor.on('price-update', (data) => this._onTick(data));
    this.start();
    console.log('📊 [KlineCollector] 已绑定到价格监控器，开始收集 K 线');
  }

  start() {
    if (this.flushInterval) return;
    this.flushInterval = setInterval(() => this._flushBars(), this.flushIntervalMs);
  }

  stop() {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
      this.flushInterval = null;
    }
    this._flushBars(); // 最后一次 flush
    this.db.close();
    console.log(`📊 [KlineCollector] 已停止 | 共写入 ${this.stats.bars_written} 根 K 线`);
  }

  _onTick(data) {
    const { tokenCA, usdPrice, price: solPrice, timestamp } = data;
    // 优先用 USD 价格，没有则用 SOL 价格
    const tickPrice = usdPrice || solPrice;
    if (!tokenCA || !tickPrice || tickPrice <= 0) return;

    this.stats.ticks++;

    // 当前分钟的起始时间戳(秒)
    const minuteTs = Math.floor((timestamp || Date.now()) / 60000) * 60;
    const price = tickPrice;

    const bar = this.currentBars.get(tokenCA);

    if (!bar || bar.minute !== minuteTs) {
      // 新的一分钟 — 先把旧 bar 写入(如果有)
      if (bar) {
        this._writeBar(tokenCA, bar);
      }
      // 开始新 bar
      this.currentBars.set(tokenCA, {
        minute: minuteTs,
        open: price,
        high: price,
        low: price,
        close: price,
        volume: 0,  // TODO: 从 DexScreener Trades API 获取交易量
      });
    } else {
      // 更新当前 bar
      bar.high = Math.max(bar.high, price);
      bar.low = Math.min(bar.low, price);
      bar.close = price;
    }
  }

  _writeBar(tokenCA, bar) {
    try {
      this._insertStmt.run(tokenCA, bar.minute, bar.open, bar.high, bar.low, bar.close, bar.volume || 0);
      this.stats.bars_written++;
    } catch (e) {
      // 静默忽略重复写入
    }
  }

  _flushBars() {
    let flushed = 0;
    const now = Math.floor(Date.now() / 60000) * 60;

    for (const [tokenCA, bar] of this.currentBars.entries()) {
      // 只 flush 已过去的分钟
      if (bar.minute < now) {
        this._writeBar(tokenCA, bar);
        this.currentBars.delete(tokenCA);
        flushed++;
      }
    }

    this.stats.tokens_active = this.currentBars.size;

    if (flushed > 0 || this.stats.bars_written % 100 === 0) {
      console.log(`📊 [KlineCollector] flush ${flushed} bars | 总计 ${this.stats.bars_written} | 活跃 ${this.stats.tokens_active} tokens | ticks ${this.stats.ticks}`);
    }
  }

  getStats() {
    return { ...this.stats };
  }
}
