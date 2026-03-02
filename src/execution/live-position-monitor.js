/**
 * Live Position Monitor
 *
 * 事件驱动仓位监控，监听 LivePriceMonitor 的 price-update 事件
 * 每次价格更新立即评估退出条件（~1.5 秒响应）
 *
 * 退出逻辑（策略D - 激进锁定利润版）：
 * - STOP_LOSS(-20%)：全卖
 * - FAST_STOP（45 秒内，从未涨过 & < -5%）：全卖
 * - MID_STOP（< -12% & 从未涨过 +10%）：全卖
 * - TP1: +50% 卖50% | TP2: +100% 卖30% | 剩20% trailing
 * - MOON_STOP（剩余仓位回撤到 moonHighPnl*80%，最低保 +50%）：全卖剩余
 * - TRAIL_STOP（未触发分批，peak >= 15%，回撤到 peak*80%）：全卖
 *
 * 实盘优化（2026-03-02 v2）：
 * - 分批止盈: 30%/20%/20%/30% → 50%/30%/20%（更早锁定更多利润）
 * - MOON_STOP: 70%→80%，最低保40%→50%（流动性窗口短）
 * - TRAIL_STOP: 75-80%→80%，最低保12%→15%
 * - 紧急卖出模式，每30秒钱包扫描
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

    // 🔧 BUG FIX: 重试计数器 Map<tokenCA, { count, pauseUntil }>
    this.retryCounter = new Map();
    this.maxRetries = 5;              // 最大重试 5 次
    this.retryPauseMs = 60000;        // 滑点错误后暂停 1 分钟
    this.maxWalletScanRetries = 3;    // 钱包扫描最大重试次数

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
        total_sol_received REAL DEFAULT 0,
        exit_pnl REAL,
        exit_reason TEXT,
        status TEXT DEFAULT 'open',
        entry_time INTEGER,
        closed_at INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);
    this.db.exec(`CREATE INDEX IF NOT EXISTS idx_live_pos_status ON live_positions(status)`);

    // 迁移：给旧表添加 total_sol_received 字段
    try {
      this.db.exec(`ALTER TABLE live_positions ADD COLUMN total_sol_received REAL DEFAULT 0`);
    } catch (e) { /* 字段已存在 */ }

    // 🔧 BUG FIX: 添加 scan_retry_count 字段
    try {
      this.db.exec(`ALTER TABLE live_positions ADD COLUMN scan_retry_count INTEGER DEFAULT 0`);
    } catch (e) { /* 字段已存在 */ }
  }

  /**
   * 启动监控 — 监听价格事件
   */
  start() {
    this.priceMonitor.on('price-update', this._onPriceUpdate);

    // 恢复未关闭的持仓
    this._restorePositions();

    // 启动钱包扫描（每60秒检查滞留token）
    this._startWalletScanner();

    console.log(`✅ [LivePositionMonitor] 启动 | 持仓: ${this.positions.size}`);
  }

  /**
   * 停止
   */
  stop() {
    this.priceMonitor.removeListener('price-update', this._onPriceUpdate);
    if (this.walletScanInterval) {
      clearInterval(this.walletScanInterval);
      this.walletScanInterval = null;
    }
    this._printReport();
    console.log('⏹️  [LivePositionMonitor] 已停止');
  }

  /**
   * 启动钱包扫描器 — 定期检查滞留 token 并重试卖出
   */
  _startWalletScanner() {
    this.walletScanInterval = setInterval(async () => {
      await this._scanAndRetrySell();
    }, 30000); // 每30秒扫描一次（从60秒缩短）

    // 首次启动时立即扫描一次
    setTimeout(() => this._scanAndRetrySell(), 5000);
  }

  /**
   * 扫描钱包中的滞留 token 并尝试卖出
   * 🔧 BUG FIX: 添加重试次数限制
   */
  async _scanAndRetrySell() {
    try {
      // 查找状态为 closed 但 total_sol_received = 0 的记录（卖出失败）
      // 🔧 BUG FIX: 添加 scan_retry_count 过滤，避免无限重试
      const failedSells = this.db.prepare(`
        SELECT * FROM live_positions
        WHERE status = 'closed' AND (total_sol_received = 0 OR total_sol_received IS NULL)
        AND closed_at > ?
        AND (scan_retry_count IS NULL OR scan_retry_count < ?)
        ORDER BY closed_at DESC
        LIMIT 5
      `).all(Date.now() - 24 * 60 * 60 * 1000, this.maxWalletScanRetries);

      if (failedSells.length === 0) return;

      // 🔧 BUG FIX: 检查 SOL 余额是否足够
      const solBalance = await this.executor.getSolBalance();
      if (solBalance < 0.02) {
        console.log(`\n🔍 [钱包扫描] SOL 余额不足 (${solBalance.toFixed(4)})，跳过扫描`);
        return;
      }

      console.log(`\n🔍 [钱包扫描] 发现 ${failedSells.length} 笔卖出失败的交易，尝试重新卖出...`);

      for (const row of failedSells) {
        try {
          // 🔧 BUG FIX: 更新扫描重试次数
          const currentRetry = (row.scan_retry_count || 0) + 1;
          this.db.prepare(`
            UPDATE live_positions SET scan_retry_count = ? WHERE id = ?
          `).run(currentRetry, row.id);

          // 查询实际钱包余额
          const balance = await this.executor.getTokenBalance(row.token_ca);
          if (balance.amount <= 0) {
            // token 已经不在钱包里了（可能手动卖了或转走了）
            console.log(`   ✓ $${row.symbol} 已不在钱包中，移除记录`);
            // 设置 total_sol_received = -1 标记为"手动处理"，避免重复检查
            this.db.prepare(`
              UPDATE live_positions SET total_sol_received = -1, exit_reason = ? WHERE id = ?
            `).run(`${row.exit_reason}(MANUAL_SOLD)`, row.id);
            continue;
          }

          console.log(`   🚨 紧急卖出 $${row.symbol} | ${balance.uiAmount} tokens | 重试: ${currentRetry}/${this.maxWalletScanRetries}`);

          // 使用紧急卖出模式
          const result = await this.executor.emergencySell(row.token_ca, balance.amount);

          if (result.success) {
            // 再次检查余额确认卖出成功
            await new Promise(r => setTimeout(r, 2000));
            const balanceAfter = await this.executor.getTokenBalance(row.token_ca);

            if (balanceAfter.amount <= 0) {
              // 获取实际收到的 SOL（通过查询 SOL 余额变化估算）
              const solReceived = result.soldAmount ? result.soldAmount * (row.entry_sol / row.token_amount) : 0.001;  // 估算值

              // 更新数据库
              this.db.prepare(`
                UPDATE live_positions
                SET total_sol_received = ?, exit_reason = ?
                WHERE id = ?
              `).run(solReceived, `${row.exit_reason}(EMERGENCY_SOLD)`, row.id);

              console.log(`   ✅ 紧急卖出成功: $${row.symbol}`);
            }
          } else {
            console.log(`   ❌ 紧急卖出失败: $${row.symbol} | ${result.reason || 'unknown'}`);
            // 🔧 BUG FIX: 如果达到最大重试，标记为放弃
            if (currentRetry >= this.maxWalletScanRetries) {
              console.log(`   🚫 达到最大扫描重试次数，放弃 $${row.symbol}`);
              this.db.prepare(`
                UPDATE live_positions SET total_sol_received = -2, exit_reason = ? WHERE id = ?
              `).run(`${row.exit_reason}(RETRY_LIMIT)`, row.id);
            }
          }
        } catch (error) {
          console.log(`   ❌ 重试失败: $${row.symbol} | ${error.message}`);
        }

        // 每笔之间等待2秒（增加间隔）
        await new Promise(r => setTimeout(r, 2000));
      }
    } catch (error) {
      console.error(`⚠️  [钱包扫描] 异常: ${error.message}`);
    }
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
      // 策略D: 渐进式分批止盈
      tp1: false,           // +50% 卖50%
      tp2: false,           // +100% 卖30%
      tp3: false,           // 保留字段
      soldPct: 0,           // 已卖出百分比
      remainingPct: 100,
      lockedPnl: 0,
      moonHighPnl: 0,       // 分批后剩余仓位的独立峰值
      highPnl: 0,
      lowPnl: 0,
      lastPnl: 0,
      totalSolReceived: 0,  // 累计收到的 SOL（用于计算真实 PnL）
      entryTime: Date.now(),
      closed: false,
      exitInProgress: false,  // 防止并发卖出
      partialSellInProgress: false,  // 防止并发分批卖出
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

    // 如果正在退出中，跳过评估
    if (pos.exitInProgress || pos.partialSellInProgress) return;

    // 🔧 BUG FIX: 检查是否在暂停期内
    const retryInfo = this.retryCounter.get(tokenCA) || { count: 0, pauseUntil: 0 };
    if (retryInfo.pauseUntil > Date.now()) {
      return;  // 仍在暂停期，跳过
    }

    // 🔧 BUG FIX: 检查是否超过最大重试次数
    if (retryInfo.count >= this.maxRetries) {
      if (!pos.retryLimitReached) {
        console.log(`🚫 [重试上限] $${pos.symbol} 已达最大重试次数 ${this.maxRetries}，停止重试`);
        pos.retryLimitReached = true;
      }
      pos.pendingSell = false;
      return;
    }

    // 如果有待卖出的仓位，尝试重新卖出
    if (pos.pendingSell) {
      console.log(`🔄 [重试卖出] $${pos.symbol} | 原因: ${pos.pendingSellReason} | 重试: ${retryInfo.count + 1}/${this.maxRetries}`);
      pos.pendingSell = false;
      await this._triggerExit(pos, pos.pendingSellReason || 'PENDING_SELL', 100);
      return;
    }

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

    // ==================== 策略D: 激进锁定利润版 ====================
    // +50%: 卖50% | +100%: 卖30% | 剩20% trailing

    // TP1: +50% 卖50%
    if (!pos.tp1 && pnl >= 50) {
      const success = await this._triggerPartialSell(pos, 50, 'TP1');
      if (success) {
        pos.tp1 = true;
        pos.moonHighPnl = pnl;
      }
      return;
    }

    // TP2: +100% 卖30%（需要先触发TP1）
    if (pos.tp1 && !pos.tp2 && pnl >= 100) {
      const success = await this._triggerPartialSell(pos, 30, 'TP2');
      if (success) {
        pos.tp2 = true;
      }
      // TP2后剩余20%，不再有TP3
      return;
    }

    // 更新剩余仓位的独立峰值
    if (pos.tp1 && pnl > pos.moonHighPnl) {
      pos.moonHighPnl = pnl;
    }

    // 5. MOON_STOP: 已分批止盈，剩余仓位回撤到 moonHighPnl*80%，最低保 +50%
    // 实盘数据：流动性窗口极短，必须更早出场
    if (pos.tp1 && pos.highPnl >= 15) {
      const moonExit = pos.moonHighPnl * 0.80;  // 从70%提高到80%
      const exitLine = Math.max(moonExit, 50);   // 最低保从40%提高到50%
      if (pnl < exitLine) {
        await this._triggerExit(pos, `MOON_STOP(peak+${pos.highPnl.toFixed(0)}%)`, 100);
        return;
      }
    }

    // 6. TRAIL_STOP: 未触发分批止盈，peak >= 15%，回撤到 peak*80%
    // 实盘数据：流动性窗口极短，必须更早出场
    if (!pos.tp1 && pos.highPnl >= 15) {
      const trailExit = pos.highPnl * 0.80;  // 统一用80%
      const exitLine = Math.max(trailExit, 15);  // 最低保从12%提高到15%
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
    // 1. 检查是否已在退出中（防止并发）
    if (pos.exitInProgress || pos.closed) {
      return;
    }

    // 2. 立即标记退出中（在任何异步操作之前）
    pos.exitInProgress = true;

    // 3. 防抖（额外保护，增加到 10 秒）
    const lastSell = this.sellDebounce.get(pos.tokenCA);
    if (lastSell && (Date.now() - lastSell) < 10000) {
      pos.exitInProgress = false;
      return;
    }
    this.sellDebounce.set(pos.tokenCA, Date.now());

    pos.exitReason = reason;

    // 计算最终 PnL（策略D：已锁定 + 剩余仓位当前PnL）
    let finalPnl = pos.lastPnl;
    if (pos.tp1) {
      finalPnl = pos.lockedPnl + pos.lastPnl * (pos.remainingPct / 100);
    }

    const icon = finalPnl > 0 ? '🟢' : '🔴';
    console.log(`\n${icon} [EXIT] $${pos.symbol} | ${reason} | PnL: ${finalPnl >= 0 ? '+' : ''}${finalPnl.toFixed(1)}% | 最高: +${pos.highPnl.toFixed(1)}%`);

    // 执行卖出
    let solReceived = 0;
    let sellSuccess = false;
    try {
      // 先检查余额，避免无效卖出
      const balance = await this.executor.getTokenBalance(pos.tokenCA);
      if (balance.amount <= 0) {
        console.log(`   ⚠️  余额为0，可能已被卖出`);
        pos.closed = true;
        pos.exitInProgress = false;
        this._closePosition(pos, finalPnl);
        this.priceMonitor.removeToken(pos.tokenCA);
        return;
      }

      const sellAmount = pos.tp1
        ? Math.floor(pos.tokenAmount * (pos.remainingPct / 100))
        : pos.tokenAmount;

      if (sellAmount > 0) {
        const result = await this.executor.sell(pos.tokenCA, sellAmount);

        if (!result.success) {
          throw new Error(result.error || 'Sell failed');
        }

        solReceived = result.amountOut || 0;
        sellSuccess = true;
        console.log(`   TX: ${result.txHash} | 收到: ${solReceived.toFixed(6)} SOL`);

        // 累加实际收到的 SOL
        pos.totalSolReceived += solReceived;

        // 记录亏损
        if (result.amountOut < pos.entrySol * (pos.remainingPct / 100)) {
          const loss = pos.entrySol * (pos.remainingPct / 100) - result.amountOut;
          this.executor.recordLoss(loss);
        }
      }
    } catch (error) {
      console.error(`   ❌ 卖出失败: ${error.message}`);

      // 🔧 BUG FIX: 更新重试计数器
      const retryInfo = this.retryCounter.get(pos.tokenCA) || { count: 0, pauseUntil: 0 };
      retryInfo.count += 1;

      // 🔧 BUG FIX: 检测滑点错误，暂停更长时间
      const isSlippageError = error.message.includes('6025') ||
                              error.message.includes('6024') ||
                              error.message.includes('Slippage') ||
                              error.message.includes('滑点');

      if (isSlippageError) {
        console.log(`   ⚠️  滑点错误，暂停 ${this.retryPauseMs / 1000} 秒后重试`);
        retryInfo.pauseUntil = Date.now() + this.retryPauseMs;
      }

      // 🔧 BUG FIX: 检查是否达到最大重试次数
      if (retryInfo.count >= this.maxRetries) {
        console.log(`   🚫 达到最大重试次数 ${this.maxRetries}，停止重试 $${pos.symbol}`);
        pos.pendingSell = false;
        pos.retryLimitReached = true;
        this.retryCounter.set(pos.tokenCA, retryInfo);
        pos.exitInProgress = false;
        return;
      }

      this.retryCounter.set(pos.tokenCA, retryInfo);

      // 卖出失败，保持仓位 open，下次价格更新时重试
      console.log(`   ⚠️  保持仓位 open，等待重试 (${retryInfo.count}/${this.maxRetries})...`);
      pos.closed = false;
      pos.exitReason = null;
      pos.exitInProgress = false;  // 重置标记，允许重试
      pos.pendingSell = true;  // 标记待卖出
      pos.pendingSellReason = reason;
      return;  // 不关闭仓位
    }

    // 卖出成功，关闭仓位
    pos.closed = true;
    pos.exitInProgress = false;

    // 计算真实 PnL（基于实际 SOL 进出）
    const realPnl = pos.entrySol > 0 ? ((pos.totalSolReceived - pos.entrySol) / pos.entrySol) * 100 : finalPnl;
    console.log(`   💰 真实PnL: ${realPnl >= 0 ? '+' : ''}${realPnl.toFixed(1)}% (投入: ${pos.entrySol} SOL, 收回: ${pos.totalSolReceived.toFixed(4)} SOL)`);

    // 持久化（使用真实 PnL）
    this._closePosition(pos, realPnl);

    // 从价格监控移除
    this.priceMonitor.removeToken(pos.tokenCA);
  }

  /**
   * 分批卖出（策略D: TP1卖50%, TP2卖30%）
   * @returns {boolean} 是否卖出成功
   */
  async _triggerPartialSell(pos, sellPct, label) {
    // 1. 检查是否已在卖出中（防止并发）
    if (pos.partialSellInProgress || pos.exitInProgress || pos.closed) {
      return false;
    }

    // 2. 立即标记卖出中
    pos.partialSellInProgress = true;

    // 3. 防抖（增加到 10 秒）
    const lastSell = this.sellDebounce.get(pos.tokenCA);
    if (lastSell && (Date.now() - lastSell) < 10000) {
      pos.partialSellInProgress = false;
      return false;
    }
    this.sellDebounce.set(pos.tokenCA, Date.now());

    // 计算卖出数量
    const sellAmount = Math.floor(pos.tokenAmount * (sellPct / 100));
    const newRemaining = pos.remainingPct - sellPct;

    console.log(`\n💰 [${label}] $${pos.symbol} +${pos.lastPnl.toFixed(0)}% → 卖${sellPct}%，留${newRemaining}%`);

    try {
      // 先检查余额
      const balance = await this.executor.getTokenBalance(pos.tokenCA);
      if (balance.amount <= 0) {
        console.log(`   ⚠️  余额为0，跳过分批卖出`);
        pos.partialSellInProgress = false;
        return false;
      }

      const result = await this.executor.sell(pos.tokenCA, sellAmount);

      if (!result.success) {
        console.error(`   ❌ 分批卖出失败: ${result.error || 'Unknown error'}`);
        pos.partialSellInProgress = false;
        return false;
      }

      const solReceived = result.amountOut || 0;
      console.log(`   TX: ${result.txHash} | 收到: ${solReceived.toFixed(6)} SOL`);

      // 累加实际收到的 SOL
      pos.totalSolReceived += solReceived;

      pos.soldPct += sellPct;
      pos.remainingPct = newRemaining;
      pos.lockedPnl += pos.lastPnl * (sellPct / 100);

      // 更新 DB（包含累计收到的 SOL）
      try {
        this.db.prepare(`
          UPDATE live_positions SET sold_80_pct=1, remaining_pct=?, locked_pnl=?, total_sol_received=?
          WHERE token_ca=? AND status='open'
        `).run(newRemaining, pos.lockedPnl, pos.totalSolReceived, pos.tokenCA);
      } catch (e) { /* ignore */ }

      pos.partialSellInProgress = false;
      return true;
    } catch (error) {
      console.error(`   ❌ 分批卖出异常: ${error.message}`);
      pos.partialSellInProgress = false;
      return false;
    }
  }

  /**
   * 关闭持仓（DB 更新）
   */
  _closePosition(pos, finalPnl) {
    try {
      this.db.prepare(`
        UPDATE live_positions
        SET exit_pnl=?, exit_reason=?, high_pnl=?, low_pnl=?, total_sol_received=?, status='closed', closed_at=?
        WHERE token_ca=? AND status='open'
      `).run(finalPnl, pos.exitReason, pos.highPnl, pos.lowPnl, pos.totalSolReceived || 0, Date.now(), pos.tokenCA);
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
          // 策略D: 兼容恢复（从旧的sold80字段推断tp1状态）
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
          totalSolReceived: row.total_sol_received || 0,
          entryTime: row.entry_time,
          closed: false,
          exitInProgress: false,
          partialSellInProgress: false,
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
