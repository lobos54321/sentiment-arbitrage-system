/**
 * Live Position Monitor
 *
 * 事件驱动仓位监控，监听 LivePriceMonitor 的 price-update 事件
 * 每次价格更新立即评估退出条件（~1.5 秒响应）
 *
 * 退出逻辑（基于时间，适配 1.5 秒轮询）：
 * - STOP_LOSS(-20%)：全卖
 * - FAST_STOP（45 秒内，从未涨过 & < -5%）：全卖
 * - MID_STOP（< -12% & 从未涨过 +10%）：全卖
 * - +50% 分批止盈：卖 80% token，留 20%
 * - MOON_STOP（剩余 20% 回撤到 peak*35%，最低保 +25%）：全卖剩余
 * - TRAIL_STOP（未触发分批，peak >= 15%，回撤到 peak*65-70%）：全卖
 */

import Database from 'better-sqlite3';

export class LivePositionMonitor {
  constructor(priceMonitor, executor) {
    this.priceMonitor = priceMonitor;
    this.executor = executor;

    // 持仓 Map<tokenCA, position>
    this.positions = new Map();

    // 防抖 Map<tokenCA, timestamp> — 3 秒内不重复触发
    this.sellDebounce = new Map();
    this.debouncMs = 3000;

    // 持久化
    const dbPath = process.env.DB_PATH || './data/sentiment_arb.db';
    this.db = new Database(dbPath);
    this._initDB();

    // 绑定事件
    this._onPriceUpdate = this._onPriceUpdate.bind(this);

    console.log('🎯 [LivePositionMonitor] 初始化');
  }

  _initDB() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS live_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_ca TEXT NOT NULL,
        symbol TEXT,
        entry_price REAL,
        entry_mc REAL,
        entry_sol REAL,
        token_amount REAL,
        token_decimals INTEGER DEFAULT 0,
        sold_80_pct INTEGER DEFAULT 0,
        remaining_pct REAL DEFAULT 100,
        locked_pnl REAL DEFAULT 0,
        high_pnl REAL DEFAULT 0,
        low_pnl REAL DEFAULT 0,
        exit_pnl REAL,
        exit_reason TEXT,
        status TEXT DEFAULT 'open',
        entry_time INTEGER,
        closed_at INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);
    this.db.exec(`CREATE INDEX IF NOT EXISTS idx_live_pos_status ON live_positions(status)`);
  }

  /**
   * 启动监控 — 监听价格事件
   */
  start() {
    this.priceMonitor.on('price-update', this._onPriceUpdate);

    // 恢复未关闭的持仓
    this._restorePositions();

    console.log(`✅ [LivePositionMonitor] 启动 | 持仓: ${this.positions.size}`);
  }

  /**
   * 停止
   */
  stop() {
    this.priceMonitor.removeListener('price-update', this._onPriceUpdate);
    this._printReport();
    console.log('⏹️  [LivePositionMonitor] 已停止');
  }

  /**
   * 注册新持仓（买入后调用）
   */
  addPosition(tokenCA, symbol, entryPrice, entryMC, entrySol, tokenAmount, tokenDecimals) {
    const position = {
      tokenCA,
      symbol: symbol || 'UNKNOWN',
      entryPrice,
      entryMC,
      entrySol,
      tokenAmount,        // raw amount（含 decimals）
      tokenDecimals,
      sold80: false,
      remainingPct: 100,
      lockedPnl: 0,
      highPnl: 0,
      lowPnl: 0,
      lastPnl: 0,
      entryTime: Date.now(),
      closed: false,
      exitReason: null
    };

    this.positions.set(tokenCA, position);

    // 注册到价格监控
    this.priceMonitor.addToken(tokenCA);

    // 持久化
    try {
      this.db.prepare(`
        INSERT INTO live_positions (token_ca, symbol, entry_price, entry_mc, entry_sol, token_amount, token_decimals, entry_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(tokenCA, position.symbol, entryPrice, entryMC, entrySol, tokenAmount, tokenDecimals, position.entryTime);
    } catch (e) {
      console.warn(`⚠️  [LivePositionMonitor] DB 写入失败: ${e.message}`);
    }

    console.log(`🎯 [LivePositionMonitor] 新持仓: $${position.symbol} | ${entrySol} SOL | ${tokenAmount} tokens`);
  }

  /**
   * 价格更新事件处理
   */
  async _onPriceUpdate(event) {
    const { tokenCA, price, mc } = event;
    const pos = this.positions.get(tokenCA);
    if (!pos || pos.closed) return;

    // 计算 PnL
    let pnl;
    if (pos.entryPrice > 0 && price > 0) {
      pnl = ((price - pos.entryPrice) / pos.entryPrice) * 100;
    } else if (pos.entryMC > 0 && mc > 0) {
      pnl = ((mc - pos.entryMC) / pos.entryMC) * 100;
    } else {
      return; // 无法计算
    }

    pos.lastPnl = pnl;
    if (pnl > pos.highPnl) pos.highPnl = pnl;
    if (pnl < pos.lowPnl) pos.lowPnl = pnl;

    const holdTimeMs = Date.now() - pos.entryTime;
    const holdTimeSec = holdTimeMs / 1000;

    // ==================== 退出条件评估 ====================

    // 1. STOP_LOSS: -20% 全卖
    if (pnl <= -20) {
      await this._triggerExit(pos, 'STOP_LOSS', 100);
      return;
    }

    // 2. FAST_STOP: 买入后 45 秒内，从未涨过(highPnl<=0) 且当前 < -5%
    if (holdTimeSec <= 45 && pos.highPnl <= 0 && pnl < -5) {
      await this._triggerExit(pos, 'FAST_STOP', 100);
      return;
    }

    // 3. MID_STOP: PnL < -12% & 从未涨过 +10%
    if (pnl < -12 && pos.highPnl < 10) {
      await this._triggerExit(pos, 'MID_STOP', 100);
      return;
    }

    // 4. 分批止盈: +50% 卖 80%，留 20%
    if (!pos.sold80 && pnl >= 50) {
      await this._triggerPartialSell(pos, 80);
      return;
    }

    // 5. MOON_STOP: 已卖 80%，剩余 20% 回撤到 peak*35%，最低保 +25%
    if (pos.sold80 && pos.highPnl >= 15) {
      const moonExit = pos.highPnl * 0.35;
      const exitLine = Math.max(moonExit, 25);
      if (pnl < exitLine) {
        await this._triggerExit(pos, `MOON_STOP(peak+${pos.highPnl.toFixed(0)}%)`, 100);
        return;
      }
    }

    // 6. TRAIL_STOP: 未触发分批止盈，peak >= 15%，回撤到 peak*65-70%
    if (!pos.sold80 && pos.highPnl >= 15) {
      const keepRatio = pos.highPnl >= 50 ? 0.70 : 0.65;
      const trailExit = pos.highPnl * keepRatio;
      const exitLine = Math.max(trailExit, 10);
      if (pnl < exitLine) {
        await this._triggerExit(pos, `TRAIL_STOP(peak+${pos.highPnl.toFixed(0)}%)`, 100);
        return;
      }
    }
  }

  /**
   * 触发全部卖出
   */
  async _triggerExit(pos, reason, sellPct) {
    // 防抖
    const lastSell = this.sellDebounce.get(pos.tokenCA);
    if (lastSell && (Date.now() - lastSell) < this.debouncMs) return;
    this.sellDebounce.set(pos.tokenCA, Date.now());

    pos.closed = true;
    pos.exitReason = reason;

    // 计算最终 PnL
    let finalPnl = pos.lastPnl;
    if (pos.sold80) {
      finalPnl = pos.lockedPnl + pos.lastPnl * (pos.remainingPct / 100);
    }

    const icon = finalPnl > 0 ? '🟢' : '🔴';
    console.log(`\n${icon} [EXIT] $${pos.symbol} | ${reason} | PnL: ${finalPnl >= 0 ? '+' : ''}${finalPnl.toFixed(1)}% | 最高: +${pos.highPnl.toFixed(1)}%`);

    // 执行卖出
    try {
      const sellAmount = pos.sold80
        ? Math.floor(pos.tokenAmount * (pos.remainingPct / 100))
        : pos.tokenAmount;

      if (sellAmount > 0) {
        const result = await this.executor.sell(pos.tokenCA, sellAmount);
        console.log(`   TX: ${result.txHash} | 收到: ${result.amountOut.toFixed(6)} SOL`);

        // 记录亏损
        if (result.amountOut < pos.entrySol * (pos.remainingPct / 100)) {
          const loss = pos.entrySol * (pos.remainingPct / 100) - result.amountOut;
          this.executor.recordLoss(loss);
        }
      }
    } catch (error) {
      console.error(`   ❌ 卖出失败: ${error.message}`);
    }

    // 持久化
    this._closePosition(pos, finalPnl);

    // 从价格监控移除
    this.priceMonitor.removeToken(pos.tokenCA);
  }

  /**
   * 分批卖出（卖 80%，留 20%）
   */
  async _triggerPartialSell(pos, sellPct) {
    // 防抖
    const lastSell = this.sellDebounce.get(pos.tokenCA);
    if (lastSell && (Date.now() - lastSell) < this.debouncMs) return;
    this.sellDebounce.set(pos.tokenCA, Date.now());

    const sellAmount = Math.floor(pos.tokenAmount * (sellPct / 100));
    const remainPct = 100 - sellPct;

    console.log(`\n💰 [分批止盈] $${pos.symbol} +${pos.lastPnl.toFixed(0)}% → 卖${sellPct}%，留${remainPct}%追金狗`);

    try {
      const result = await this.executor.sell(pos.tokenCA, sellAmount);
      console.log(`   TX: ${result.txHash} | 收到: ${result.amountOut.toFixed(6)} SOL`);

      pos.sold80 = true;
      pos.remainingPct = remainPct;
      pos.lockedPnl = pos.lastPnl * (sellPct / 100);

      // 更新 DB
      try {
        this.db.prepare(`
          UPDATE live_positions SET sold_80_pct=1, remaining_pct=?, locked_pnl=?
          WHERE token_ca=? AND status='open'
        `).run(remainPct, pos.lockedPnl, pos.tokenCA);
      } catch (e) { /* ignore */ }
    } catch (error) {
      console.error(`   ❌ 分批卖出失败: ${error.message}`);
    }
  }

  /**
   * 关闭持仓（DB 更新）
   */
  _closePosition(pos, finalPnl) {
    try {
      this.db.prepare(`
        UPDATE live_positions
        SET exit_pnl=?, exit_reason=?, high_pnl=?, low_pnl=?, status='closed', closed_at=?
        WHERE token_ca=? AND status='open'
      `).run(finalPnl, pos.exitReason, pos.highPnl, pos.lowPnl, Date.now(), pos.tokenCA);
    } catch (e) {
      console.warn(`⚠️  [LivePositionMonitor] DB 更新失败: ${e.message}`);
    }
  }

  /**
   * 从 DB 恢复未关闭的持仓
   */
  _restorePositions() {
    try {
      const rows = this.db.prepare(`SELECT * FROM live_positions WHERE status='open'`).all();
      for (const row of rows) {
        this.positions.set(row.token_ca, {
          tokenCA: row.token_ca,
          symbol: row.symbol,
          entryPrice: row.entry_price,
          entryMC: row.entry_mc,
          entrySol: row.entry_sol,
          tokenAmount: row.token_amount,
          tokenDecimals: row.token_decimals,
          sold80: !!row.sold_80_pct,
          remainingPct: row.remaining_pct || 100,
          lockedPnl: row.locked_pnl || 0,
          highPnl: row.high_pnl || 0,
          lowPnl: row.low_pnl || 0,
          lastPnl: 0,
          entryTime: row.entry_time,
          closed: false,
          exitReason: null
        });
        this.priceMonitor.addToken(row.token_ca);
      }
      if (rows.length > 0) {
        console.log(`🔄 [LivePositionMonitor] 恢复 ${rows.length} 个持仓`);
      }
    } catch (e) {
      console.warn(`⚠️  [LivePositionMonitor] 恢复持仓失败: ${e.message}`);
    }
  }

  /**
   * 打印报告
   */
  _printReport() {
    const all = [...this.positions.values()];
    if (all.length === 0) return;

    console.log('\n' + '═'.repeat(60));
    console.log('📊 [LivePositionMonitor] 最终报告');
    console.log('═'.repeat(60));

    for (const p of all) {
      const icon = (p.lastPnl || 0) > 0 ? '🟢' : '🔴';
      const exit = p.exitReason ? ` [${p.exitReason}]` : ' [OPEN]';
      console.log(`${icon} $${p.symbol.padEnd(12)} PnL:${p.lastPnl !== null ? (p.lastPnl >= 0 ? '+' : '') + p.lastPnl.toFixed(1) + '%' : 'N/A'} 最高:+${p.highPnl.toFixed(1)}% 最低:${p.lowPnl.toFixed(1)}%${exit}`);
    }

    const closed = all.filter(p => p.closed);
    const winners = closed.filter(p => (p.exitReason?.includes('MOON') || p.exitReason?.includes('TRAIL') || p.lastPnl > 0));
    console.log(`\n总: ${all.length} | 已关: ${closed.length} | 胜: ${winners.length}`);
    console.log('═'.repeat(60));
  }

  /**
   * 获取状态
   */
  getStatus() {
    const open = [...this.positions.values()].filter(p => !p.closed);
    return {
      open_positions: open.length,
      total_tracked: this.positions.size,
      positions: open.map(p => ({
        symbol: p.symbol,
        pnl: p.lastPnl?.toFixed(1),
        highPnl: p.highPnl?.toFixed(1),
        sold80: p.sold80,
        holdTime: ((Date.now() - p.entryTime) / 1000).toFixed(0) + 's'
      }))
    };
  }
}

export default LivePositionMonitor;
