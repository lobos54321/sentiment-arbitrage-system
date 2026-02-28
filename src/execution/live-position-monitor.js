/**
 * Live Position Monitor
 *
 * 事件驱动仓位监控，监听 LivePriceMonitor 的 price-update 事件
 * 每次价格更新立即评估退出条件（~1.5 秒响应）
 *
 * 退出逻辑（策略C - 渐进式分批止盈）：
 * - STOP_LOSS(-20%)：全卖
 * - FAST_STOP（45 秒内，从未涨过 & < -5%）：全卖
 * - MID_STOP（< -12% & 从未涨过 +10%）：全卖
 * - TP1: +50% 卖30% | TP2: +100% 卖20% | TP3: +200% 卖20% | 剩30% trailing
 * - MOON_STOP（剩余仓位回撤到 moonHighPnl*55%，最低保 +25%）：全卖剩余
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
      // 策略C: 渐进式分批止盈
      tp1: false,           // +50% 卖30%
      tp2: false,           // +100% 卖20%
      tp3: false,           // +200% 卖20%
      soldPct: 0,           // 已卖出百分比
      remainingPct: 100,
      lockedPnl: 0,
      moonHighPnl: 0,       // 分批后剩余仓位的独立峰值
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
    // 注意：已分批止盈的仓位(tp1=true)不用普通止损，用 MOON_STOP

    // 1. STOP_LOSS: -20% 全卖（仅未分批的仓位）
    if (pnl <= -20 && !pos.tp1) {
      await this._triggerExit(pos, 'STOP_LOSS', 100);
      return;
    }

    // 2. FAST_STOP: 买入后 45 秒内，从未涨过(highPnl<=0) 且当前 < -5%（仅未分批的仓位）
    if (!pos.tp1 && holdTimeSec <= 45 && pos.highPnl <= 0 && pnl < -5) {
      await this._triggerExit(pos, 'FAST_STOP', 100);
      return;
    }

    // 3. MID_STOP: PnL < -12% & 从未涨过 +10%（仅未分批的仓位）
    if (!pos.tp1 && pnl < -12 && pos.highPnl < 10) {
      await this._triggerExit(pos, 'MID_STOP', 100);
      return;
    }

    // ==================== 策略C: 渐进式分批止盈 ====================
    // +50%: 卖30% | +100%: 卖20% | +200%: 卖20% | 剩30% trailing

    // TP1: +50% 卖30%
    if (!pos.tp1 && pnl >= 50) {
      await this._triggerPartialSell(pos, 30, 'TP1');
      pos.tp1 = true;
      pos.moonHighPnl = pnl;
      return;
    }

    // TP2: +100% 卖20%（需要先触发TP1）
    if (pos.tp1 && !pos.tp2 && pnl >= 100) {
      await this._triggerPartialSell(pos, 20, 'TP2');
      pos.tp2 = true;
      return;
    }

    // TP3: +200% 卖20%（需要先触发TP2）
    if (pos.tp2 && !pos.tp3 && pnl >= 200) {
      await this._triggerPartialSell(pos, 20, 'TP3');
      pos.tp3 = true;
      return;
    }

    // 更新剩余仓位的独立峰值
    if (pos.tp1 && pnl > pos.moonHighPnl) {
      pos.moonHighPnl = pnl;
    }

    // 5. MOON_STOP: 已分批止盈，剩余仓位回撤到 moonHighPnl*55%，最低保 +25%
    if (pos.tp1 && pos.highPnl >= 15) {
      const moonExit = pos.moonHighPnl * 0.55;
      const exitLine = Math.max(moonExit, 25);
      if (pnl < exitLine) {
        await this._triggerExit(pos, `MOON_STOP(peak+${pos.highPnl.toFixed(0)}%)`, 100);
        return;
      }
    }

    // 6. TRAIL_STOP: 未触发分批止盈，peak >= 15%，回撤到 peak*65-70%
    if (!pos.tp1 && pos.highPnl >= 15) {
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

    // 计算最终 PnL（策略C：已锁定 + 剩余仓位当前PnL）
    let finalPnl = pos.lastPnl;
    if (pos.tp1) {
      finalPnl = pos.lockedPnl + pos.lastPnl * (pos.remainingPct / 100);
    }

    const icon = finalPnl > 0 ? '🟢' : '🔴';
    console.log(`\n${icon} [EXIT] $${pos.symbol} | ${reason} | PnL: ${finalPnl >= 0 ? '+' : ''}${finalPnl.toFixed(1)}% | 最高: +${pos.highPnl.toFixed(1)}%`);

    // 执行卖出
    try {
      const sellAmount = pos.tp1
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
   * 分批卖出（策略C: TP1卖30%, TP2卖20%, TP3卖20%）
   */
  async _triggerPartialSell(pos, sellPct, label) {
    // 防抖
    const lastSell = this.sellDebounce.get(pos.tokenCA);
    if (lastSell && (Date.now() - lastSell) < this.debouncMs) return;
    this.sellDebounce.set(pos.tokenCA, Date.now());

    // 计算卖出数量
    const sellAmount = Math.floor(pos.tokenAmount * (sellPct / 100));
    const newRemaining = pos.remainingPct - sellPct;

    console.log(`\n💰 [${label}] $${pos.symbol} +${pos.lastPnl.toFixed(0)}% → 卖${sellPct}%，留${newRemaining}%`);

    try {
      const result = await this.executor.sell(pos.tokenCA, sellAmount);
      console.log(`   TX: ${result.txHash} | 收到: ${result.amountOut.toFixed(6)} SOL`);

      pos.soldPct += sellPct;
      pos.remainingPct = newRemaining;
      pos.lockedPnl += pos.lastPnl * (sellPct / 100);

      // 更新 DB
      try {
        this.db.prepare(`
          UPDATE live_positions SET sold_80_pct=1, remaining_pct=?, locked_pnl=?
          WHERE token_ca=? AND status='open'
        `).run(newRemaining, pos.lockedPnl, pos.tokenCA);
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
          // 策略C: 兼容恢复（从旧的sold80字段推断tp1状态）
          tp1: !!row.sold_80_pct,
          tp2: row.remaining_pct <= 50,
          tp3: row.remaining_pct <= 30,
          soldPct: 100 - (row.remaining_pct || 100),
          remainingPct: row.remaining_pct || 100,
          lockedPnl: row.locked_pnl || 0,
          moonHighPnl: row.high_pnl || 0,
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
