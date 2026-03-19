/**
 * Live Position Monitor — v19
 *
 * 事件驱动仓位监控，监听 LivePriceMonitor 的 price-update 事件
 * 每次价格更新立即评估退出条件（~0.5 秒响应）
 *
 * v19 退出策略:
 * - ASYMMETRIC: 非对称收割 (SL-35%→TP1@50%卖60%→SL移至0%→TP2@100%/TP3@200%/TP4@500%卖5% + 5%Moonbag回撤35%平仓)
 * - NOT_ATH: NOT_ATH专用 (SL-15%→TP1@80%卖60%→SL移至0%→TP2@100%/TP3@200%/TP4@500%+ 8分死水/15分大限)
 * - TP_NOSL: TP+100% / 无SL / 4h超时 (保留向后兼容)
 * - TP_SL: TP+75% / SL-25% / 24h超时 (保留向后兼容)
 * - DynSL: 保留向后兼容
 *
 * v19 Bug修复:
 * - #19: highPnl/locked_pnl 创新高时立即持久化（之前每60次，崩溃时峰值丢失 → Moonbag退出点算错）
 * - #21: 钱包扫描器 _walletScanRunning 并发保护（之前可能双重 emergencySell）
 * - #22: lockedPnl 持久化到DB + 重启恢复（之前重启后 lockedPnl=0，总收益计算偏低）
 * - #23: _asyncCleanupResidual 清理残余后更新DB总收回金额（之前仅更新内存）
 */

import Database from 'better-sqlite3';

export class LivePositionMonitor {
  constructor(priceMonitor, executor, riskManager = null) {
    this.priceMonitor = priceMonitor;
    this.executor = executor;
    this.riskManager = riskManager;

    // 持仓 Map<tokenCA, position>
    this.positions = new Map();

    // 🔧 退出回调（通知信号引擎设置冷却）
    this.onExitCallbacks = [];

    // 防抖 Map<tokenCA, timestamp> — 3 秒内不重复触发
    this.sellDebounce = new Map();
    this.debouncMs = 3000;

    // 🔧 BUG FIX: 重试计数器 Map<tokenCA, { count, pauseUntil }>
    this.retryCounter = new Map();
    this.maxRetries = 5;              // 最大重试 5 次
    this.retryPauseMs = 15000;         // 滑点错误后暂停 15 秒
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

    // v12: 添加部署恢复关键字段（conviction/tp1/sold_pct/entry_tier）
    // 解决: server重启后conviction丢失→HC变NM, high_pnl=0→退出逻辑错误
    try { this.db.exec(`ALTER TABLE live_positions ADD COLUMN conviction TEXT DEFAULT 'NORMAL'`); } catch (e) { /* 已存在 */ }
    try { this.db.exec(`ALTER TABLE live_positions ADD COLUMN tp1_triggered INTEGER DEFAULT 0`); } catch (e) { /* 已存在 */ }
    try { this.db.exec(`ALTER TABLE live_positions ADD COLUMN sold_pct REAL DEFAULT 0`); } catch (e) { /* 已存在 */ }
    try { this.db.exec(`ALTER TABLE live_positions ADD COLUMN entry_tier TEXT DEFAULT ''`); } catch (e) { /* 已存在 */ }
    // v15: 添加出场策略字段
    try { this.db.exec(`ALTER TABLE live_positions ADD COLUMN exit_strategy TEXT DEFAULT 'ASYMMETRIC'`); } catch (e) { /* 已存在 */ }
    // v18: 添加 tp2/tp3/tp4/moonbag 状态字段（修复重启后重复触发 TP）
    try { this.db.exec(`ALTER TABLE live_positions ADD COLUMN tp2_triggered INTEGER DEFAULT 0`); } catch (e) { /* 已存在 */ }
    try { this.db.exec(`ALTER TABLE live_positions ADD COLUMN tp3_triggered INTEGER DEFAULT 0`); } catch (e) { /* 已存在 */ }
    try { this.db.exec(`ALTER TABLE live_positions ADD COLUMN tp4_triggered INTEGER DEFAULT 0`); } catch (e) { /* 已存在 */ }
    try { this.db.exec(`ALTER TABLE live_positions ADD COLUMN moonbag_active INTEGER DEFAULT 0`); } catch (e) { /* 已存在 */ }
    try { this.db.exec(`ALTER TABLE live_positions ADD COLUMN moonbag_high_pnl REAL DEFAULT 0`); } catch (e) { /* 已存在 */ }
  }

  /**
   * 启动监控 — 监听价格事件
   */
  async start() {
    this.priceMonitor.on('price-update', this._onPriceUpdate);

    // 修复损坏数据（清理重复 MANUAL_SOLD 标记、修正错误 exit_reason）
    this._fixCorruptedData();

    // 恢复未关闭的持仓（检查链上余额清理已卖出的）
    await this._restorePositions();

    // 启动钱包扫描（每60秒检查滞留token）
    this._startWalletScanner();

    // 每30秒打印持仓状态（调试用）
    this._statusInterval = setInterval(() => this._printLiveStatus(), 30000);

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
   * 🔧 注册退出回调（用于通知信号引擎设置冷却）
   */
  onExit(callback) {
    this.onExitCallbacks.push(callback);
  }

  /**
   * 启动钱包扫描器 — 定期检查滞留 token 并重试卖出
   */
  _startWalletScanner() {
    this._walletScanRunning = false; // 并发保护标志
    this.walletScanInterval = setInterval(async () => {
      if (this._walletScanRunning) return; // 防止上次扫描未结束时重入
      this._walletScanRunning = true;
      try {
        await this._scanAndRetrySell();
      } finally {
        this._walletScanRunning = false;
      }
    }, 30000); // 每30秒扫描一次

    // 首次启动时立即扫描一次
    setTimeout(() => this._scanAndRetrySell(), 5000);
  }

  /**
   * 卖出后异步清理残余 token（等链上确认后检查余额，不阻塞主流程）
   */
  _asyncCleanupResidual(pos) {
    new Promise(async (resolve) => {
      try {
        await new Promise(r => setTimeout(r, 3000)); // 等链上确认
        const postSellBalance = await this.executor.getTokenBalance(pos.tokenCA);
        if (postSellBalance.amount > 0) {
          console.log(`   ⚠️  卖出后仍有残余 ${postSellBalance.amount} tokens，尝试清理...`);
          try {
            const cleanupResult = await this.executor.sell(pos.tokenCA, postSellBalance.amount);
            if (cleanupResult.success) {
              const cleanupSol = cleanupResult.amountOut || 0;
              pos.totalSolReceived += cleanupSol;
              // 即使仓位已关闭也更新DB，确保实际收回金额被记录
              try {
                this.db.prepare(`UPDATE live_positions SET total_sol_received=? WHERE token_ca=?`)
                  .run(pos.totalSolReceived, pos.tokenCA);
              } catch (_) {}
              console.log(`   ✅ 残余清理成功: +${cleanupSol.toFixed(6)} SOL`);
            }
          } catch (cleanErr) {
            console.log(`   ⚠️  残余清理失败: ${cleanErr.message} (${postSellBalance.amount} tokens留在钱包)`);
          }
        }
      } catch (_) {}
      resolve();
    });
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
            console.log(`   ✓ $${row.symbol} 已不在钱包中，标记为手动处理`);
            // 不覆盖 total_sol_received（可能有部分卖出记录），只追加一次标记
            const newReason = row.exit_reason && row.exit_reason.includes('MANUAL_SOLD')
              ? row.exit_reason  // 已标记过，不重复追加
              : `${row.exit_reason || 'UNKNOWN'}(MANUAL_SOLD)`;
            this.db.prepare(`
              UPDATE live_positions SET exit_reason = ?, scan_retry_count = ? WHERE id = ?
            `).run(newReason, this.maxWalletScanRetries, row.id);  // 设到最大重试次数，防止再扫
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
  addPosition(tokenCA, symbol, entryPrice, entryMC, entrySol, tokenAmount, tokenDecimals, conviction = 'NORMAL', exitStrategy = 'ASYMMETRIC') {
    const position = {
      tokenCA,
      symbol: symbol || 'UNKNOWN',
      entryPrice,
      entryMC,
      entrySol,
      tokenAmount,        // raw amount（含 decimals）
      tokenDecimals,
      highPnl: 0,
      lowPnl: 0,
      lastPnl: 0,
      totalSolReceived: 0,  // 累计收到的 SOL（用于计算真实 PnL）
      entryTime: Date.now(),
      closed: false,
      exitInProgress: false,  // 防止并发卖出
      exitReason: null,
      conviction,             // v8: 'NORMAL' or 'HIGH' — 高信念模式
      dynFloorBelowCount: 0,  // v9: DYN_FLOOR确认计数（需连续2次才触发）
      exitStrategy,           // v16: 'TP_SL' (主力), 向后兼容: 'DYNSL_20_40_60', 'DYNSL_25_50_75'

      // 新策略字段
      tp1: false,           // TP1是否已触发
      tp1Time: null,        // TP1触发时间（用于40min超时）
      breakeven: false,     // 是否已触发保本（highPnl曾≥35%）
      // tp1ZonePeak removed — v19 uses fixed 50% TP1
      tp2: false,           // TP2是否已触发
      tp3: false,           // TP3是否已触发
      tp4: false,           // TP4是否已触发
      soldPct: 0,           // 已卖出百分比
      lockedPnl: 0,         // 已锁定的利润
      moonMode: false,      // 是否进入月球模式
      moonHighPnl: 0,       // 月球模式的最高PnL
      moonbag: false,       // 是否进入Moonbag模式(TP4后5%仓位)
      moonbagHighPnl: 0,    // Moonbag最高PnL
      partialSellInProgress: false  // 是否正在分批卖出
    };

    this.positions.set(tokenCA, position);

    // 注册到价格监控（传递 token 数量和 decimals 用于 Quote 查询）
    if (this.priceMonitor.addToken.length === 3) {
      // V2 版本：需要 tokenAmount 和 decimals
      this.priceMonitor.addToken(tokenCA, tokenAmount, tokenDecimals);
    } else {
      // V1 版本：只需要 tokenCA
      this.priceMonitor.addToken(tokenCA);
    }

    // 持久化（v15: 包含conviction和exit_strategy用于重启恢复）
    try {
      this.db.prepare(`
        INSERT INTO live_positions (token_ca, symbol, entry_price, entry_mc, entry_sol, token_amount, token_decimals, entry_time, conviction, exit_strategy)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(tokenCA, position.symbol, entryPrice, entryMC, entrySol, tokenAmount, tokenDecimals, position.entryTime, conviction, exitStrategy);
    } catch (e) {
      console.warn(`⚠️  [LivePositionMonitor] DB 写入失败: ${e.message}`);
    }

    console.log(`🎯 [LivePositionMonitor] 新持仓: $${position.symbol} | ${entrySol} SOL | ${tokenAmount} tokens | 出场:${exitStrategy}${conviction === 'HIGH' ? ' | 🔥高信念模式' : ''}`);
  }

  /**
   * v14: DCA加仓 — 向已有持仓追加资金
   * 更新入场价为加权平均, 增加token数量和SOL投入
   */
  addToPosition(tokenCA, addSol, addTokenAmount, addTokenDecimals, addEntryMC) {
    const pos = this.positions.get(tokenCA);
    if (!pos || pos.closed) {
      console.warn(`⚠️ [DCA] ${tokenCA.slice(0,8)} 无持仓或已关闭，无法加仓`);
      return false;
    }

    const oldSol = pos.entrySol;
    const oldMC = pos.entryMC;
    const oldTokenAmount = pos.tokenAmount;

    // 加权平均入场MC
    const newTotalSol = oldSol + addSol;
    const newEntryMC = (oldSol * oldMC + addSol * addEntryMC) / newTotalSol;

    // 更新持仓
    pos.entrySol = newTotalSol;
    pos.entryMC = newEntryMC;
    pos.tokenAmount = oldTokenAmount + addTokenAmount;

    // 重算入场价 (SOL per token)
    const totalActualTokens = pos.tokenAmount / Math.pow(10, pos.tokenDecimals);
    if (totalActualTokens > 0) {
      pos.entryPrice = newTotalSol / totalActualTokens;
    }

    // 标记为DCA加仓
    pos.isDCA = true;
    pos.dcaAddTime = Date.now();
    pos.dcaAddSol = addSol;

    // 持久化
    try {
      this.db.prepare(`
        UPDATE live_positions SET entry_price=?, entry_mc=?, entry_sol=?, token_amount=?
        WHERE token_ca=? AND status='open'
      `).run(pos.entryPrice, pos.entryMC, pos.entrySol, pos.tokenAmount, tokenCA);
    } catch (e) {
      console.warn(`⚠️ [DCA] DB 更新失败: ${e.message}`);
    }

    console.log(`📈 [DCA加仓] $${pos.symbol} | ${oldSol}→${newTotalSol} SOL | MC: $${(oldMC/1000).toFixed(1)}K→$${(newEntryMC/1000).toFixed(1)}K(avg) | tokens: ${oldTokenAmount}→${pos.tokenAmount}`);
    return true;
  }

  /**
   * 价格更新事件处理
   */
  async _onPriceUpdate(event) {
    const { tokenCA, price, mc } = event;
    let pos;
    try {
    pos = this.positions.get(tokenCA);
    if (!pos || pos.closed) return;

    // 如果正在退出中，跳过评估
    if (pos.exitInProgress || pos.partialSellInProgress) return;

    // 🔧 BUG FIX: 检查是否在暂停期内
    const retryInfo = this.retryCounter.get(tokenCA) || { count: 0, pauseUntil: 0 };
    if (retryInfo.pauseUntil > Date.now()) {
      return;  // 仍在暂停期，跳过
    }

    // 🔧 BUG FIX: 检查是否超过最大重试次数（每 5 分钟重置一次）
    if (retryInfo.count >= this.maxRetries) {
      const timeSinceLastRetry = Date.now() - (retryInfo.lastRetryTime || 0);

      // v4: 追踪累计重试，防止无限循环 (RUN=47x, IRAN=14x 问题)
      const totalRetries = pos._totalExitRetries || 0;
      if (totalRetries >= 15) {
        // 累计15次+重试，放弃主动卖出，交给钱包扫描器处理
        if (!pos._abandonedSell) {
          console.log(`🚫 [放弃卖出] $${pos.symbol} 累计重试 ${totalRetries} 次，交给钱包扫描器`);
          pos._abandonedSell = true;
        }
        return;
      }

      if (timeSinceLastRetry > 5 * 60 * 1000) {
        // 5 分钟后重置重试计数器，允许再次尝试
        console.log(`🔄 [重试重置] $${pos.symbol} 5 分钟已过，重置重试计数器`);
        retryInfo.count = 0;
        retryInfo.pauseUntil = 0;
        pos.retryLimitReached = false;
        pos.pendingSell = true;
        pos.pendingSellReason = pos.exitReason || 'RETRY_RESET';
        this.retryCounter.set(tokenCA, retryInfo);
      } else {
        if (!pos.retryLimitReached) {
          console.log(`🚫 [重试上限] $${pos.symbol} 已达最大重试次数 ${this.maxRetries}，等待 5 分钟后重置`);
          pos.retryLimitReached = true;
          retryInfo.lastRetryTime = Date.now();
          this.retryCounter.set(tokenCA, retryInfo);
        }
        pos.pendingSell = false;

        // 60 分钟强制清理：超过 60 分钟仍卖不掉的持仓，强制关闭
        const holdTimeMin = (Date.now() - pos.entryTime) / 60000;
        if (holdTimeMin > 60 && !pos._forceCleanupDone) {
          console.log(`🧹 [强制清理] $${pos.symbol} 持仓 ${holdTimeMin.toFixed(0)} 分钟仍无法卖出，尝试紧急清理`);
          pos._forceCleanupDone = true;
          // 触发一次紧急卖出
          pos.retryLimitReached = false;
          retryInfo.count = 0;
          this.retryCounter.set(tokenCA, retryInfo);
          pos.pendingSell = true;
          pos.pendingSellReason = 'FORCE_CLEANUP';
        }
        return;
      }
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

    // 🔧 首价保护：始终跳过第1次价格更新的EXIT评估
    // 首次价格常受AMM滑点/报价延迟影响，不可靠
    if (!pos._updateCount) pos._updateCount = 0;
    pos._updateCount++;

    if (pos._updateCount === 1) {
      // 第1次价格更新：只记录，不更新highPnl，不做EXIT评估
      if (Math.abs(pnl) > 20) {
        pos._suspiciousFirstPrice = true;
        console.log(`⚠️  [首价跳过] $${pos.symbol} 首次PnL:${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}% 偏差大，跳过EXIT`);
      } else {
        console.log(`⏸️  [首价等待] $${pos.symbol} 首次PnL:${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}%，等第2次确认后开始EXIT评估`);
      }
      return;  // 始终跳过第1次
    }
    // 第2次更新时，如果首价可疑(>20%)，也以第2次为基准
    if (pos._suspiciousFirstPrice && pos._updateCount === 2) {
      pos._suspiciousFirstPrice = false;
      console.log(`✅ [价格确认] $${pos.symbol} 第2次PnL:${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}%，以此为准`);
    }

    const prevHigh = pos.highPnl;
    if (pnl > pos.highPnl) pos.highPnl = pnl;
    if (pnl < pos.lowPnl) pos.lowPnl = pnl;

    // 每60次 OR 创新高时立即持久化（防止崩溃后丢失峰值，影响Moonbag退出阈值）
    const isNewHighForDB = pnl > prevHigh && pnl > 0;
    if (pos._updateCount % 60 === 0 || isNewHighForDB) {
      try {
        this.db.prepare(`
          UPDATE live_positions SET high_pnl=?, low_pnl=?, tp1_triggered=?, tp2_triggered=?, tp3_triggered=?, tp4_triggered=?, moonbag_active=?, moonbag_high_pnl=?, sold_pct=?, token_amount=?, locked_pnl=?
          WHERE token_ca=? AND status IN ('open', 'selling')
        `).run(pos.highPnl, pos.lowPnl, pos.tp1 ? 1 : 0, pos.tp2 ? 1 : 0, pos.tp3 ? 1 : 0, pos.tp4 ? 1 : 0, pos.moonbag ? 1 : 0, pos.moonbagHighPnl || 0, pos.soldPct || 0, pos.tokenAmount, pos.lockedPnl || 0, pos.tokenCA);
      } catch (e) { console.warn(`⚠️ [DB] $${pos.symbol} 定期持久化失败: ${e.message}`); }
    }

    const holdTimeMs = Date.now() - pos.entryTime;
    const holdTimeSec = holdTimeMs / 1000;

    // 前 10 次价格更新每次都打，之后每 20 次打一次
    const shouldLog = pos._updateCount <= 10 || pos._updateCount % 20 === 0;
    // highPnl 创新高时打
    const isNewHigh = pos.highPnl > prevHigh && pos.highPnl > 0;

    if (shouldLog || isNewHigh) {
      const tag = isNewHigh ? '🔺新高' : '';
      console.log(`📡 [价格#${pos._updateCount}] $${pos.symbol} 入:${pos.entryPrice.toExponential(3)} 现:${price.toExponential(3)} PnL:${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}% 峰:+${pos.highPnl.toFixed(1)}% 持:${holdTimeSec.toFixed(0)}s ${tag}`);
    }

    // ==================== v15 退出条件评估（按出场策略分流） ====================
    const holdTimeMin = holdTimeSec / 60;
    const strategy = pos.exitStrategy || 'ASYMMETRIC';

    if (strategy === 'ASYMMETRIC') {
      // ====== 精准掐头去尾 × 非对称收割 (v19) ======
      // Hard SL: -35% (TP1前) / 0%保本 (TP1后)
      // TP1: ≥50% → 卖60% → SL上移至0%
      // TP2: +100% → 卖15% | TP3: +200% → 卖15% | TP4: +500% → 卖5%
      // 剩余5% = 登月彩票Moonbag: 不主动止盈, 从最高点回撤35%才被动平仓
      // 15分死水: 未碰TP1 && peak<50% && -15%~+15% → 全平
      // 30分大限: 未碰TP1 → 全平

      const currentSL = pos.tp1 ? 0 : -35;
      const remainingPct = 100 - (pos.soldPct || 0);

      // 1. 止损检查 (Hard SL -35% 或 TP1后保本 0%)
      // Moonbag模式下不用保本SL, 用回撤止损
      if (!pos.moonbag && pnl <= currentSL) {
        if (pos.tp1) {
          console.log(`🛡️ [ASYM:保本SL] $${pos.symbol} PnL:${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}% ≤ 0% → 卖出全部剩余 (已锁TP1利润)`);
          await this._triggerExit(pos, `BREAKEVEN_SL(PnL${pnl.toFixed(0)}%,locked_TP1)`, 100);
        } else {
          console.log(`🛑 [ASYM:硬SL] $${pos.symbol} PnL:${pnl.toFixed(1)}% ≤ -35% → 止损全卖`);
          await this._triggerExit(pos, `HARD_SL_35(PnL${pnl.toFixed(0)}%)`, 100);
        }
        return;
      }

      // 2. TP1: ≥50% → 立即卖60%
      if (!pos.tp1 && pnl >= 50) {
        console.log(`🎯 [ASYM:TP1] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ +50% → 卖出60% (SL上移至0%保本)`);
        await this._triggerPartialSell(pos, 'TP1', 60, pnl);
        return;
      }

      // 3. TP2: +100% → 卖15%
      if (pos.tp1 && !pos.tp2 && pnl >= 100) {
        console.log(`🚀 [ASYM:TP2] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ +100% → 卖出15%`);
        await this._triggerPartialSell(pos, 'TP2', 15, pnl);
        return;
      }

      // 4. TP3: +200% → 卖15%
      if (pos.tp2 && !pos.tp3 && pnl >= 200) {
        console.log(`🌙 [ASYM:TP3] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ +200% → 卖出15%`);
        await this._triggerPartialSell(pos, 'TP3', 15, pnl);
        return;
      }

      // 5. TP4: +500% → 卖5% (只卖5%, 剩余5%进入Moonbag模式)
      if (pos.tp3 && !pos.tp4 && pnl >= 500) {
        console.log(`🌕 [ASYM:TP4] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ +500% → 卖出5% (剩余5%=登月彩票🎫)`);
        await this._triggerPartialSell(pos, 'TP4', 5, pnl);
        // 只有 TP4 卖出成功（pos.tp4=true）才进入 Moonbag 模式
        if (pos.tp4) {
          pos.moonbag = true;
          pos.moonbagHighPnl = pnl;
        }
        return;
      }

      // 6. Moonbag模式: TP4触发后, 剩余5%永不主动止盈, 从最高点回撤35%才被动平仓
      if (pos.moonbag) {
        // 更新moonbag最高PnL
        if (pnl > (pos.moonbagHighPnl || 0)) {
          pos.moonbagHighPnl = pnl;
        }
        const moonPeak = pos.moonbagHighPnl || pnl;
        const dropFromPeak = moonPeak - pnl;
        const dropPct = moonPeak > 0 ? (dropFromPeak / moonPeak) * 100 : 0;

        if (dropPct >= 35) {
          console.log(`🎫 [ASYM:Moonbag平仓] $${pos.symbol} 从峰值+${moonPeak.toFixed(0)}%回撤${dropPct.toFixed(0)}%≥35% → 卖出全部剩余`);
          await this._triggerExit(pos, `MOONBAG_EXIT(peak+${moonPeak.toFixed(0)}%,drop${dropPct.toFixed(0)}%,PnL+${pnl.toFixed(0)}%)`, 100);
          return;
        }
        // Moonbag不受时间限制，不受保本SL限制，只看回撤
        return;
      }

      // 7. 15分钟死水清理: 未碰TP1 && peak<50% && 当前价在-15%~+15% → 全平
      if (!pos.tp1 && holdTimeMin >= 15) {
        if (pos.highPnl < 50 && pnl >= -15 && pnl <= 15) {
          console.log(`💀 [ASYM:死水] $${pos.symbol} 15分钟无起色 peak:+${pos.highPnl.toFixed(1)}%<50% PnL:${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}% → 全平`);
          await this._triggerExit(pos, `DEAD_WATER_15M(PnL${pnl.toFixed(0)}%,peak+${pos.highPnl.toFixed(0)}%)`, 100);
          return;
        }
      }

      // 8. 30分钟大限: 未碰TP1 → 全平
      if (!pos.tp1 && holdTimeMin >= 30) {
        console.log(`⏰ [ASYM:30分大限] $${pos.symbol} 持仓${holdTimeMin.toFixed(0)}m≥30m 未触TP1 PnL:${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}% → 全平`);
        await this._triggerExit(pos, `TIMEOUT_30M(PnL${pnl.toFixed(0)}%)`, 100);
        return;
      }

      // v19: TP1+ 仓位 2h 绝对超时兜底（防止 TP4 卖出失败后永远卡死）
      if (pos.tp1 && !pos.closed && !pos.exitInProgress) {
        if (holdTimeMin > 120) {
          console.log(`⏰ [v19] $${pos.symbol} TP1+仓位持有 ${holdTimeMin.toFixed(0)}分 > 120分钟，强制退出`);
          pos.exitReason = 'TP1_PLUS_TIMEOUT_2H';
          await this._triggerExit(pos, `TP1_PLUS_TIMEOUT_2H(PnL${pnl.toFixed(0)}%,held${holdTimeMin.toFixed(0)}m)`, 100);
          return;
        }
      }

    } else if (strategy === 'NOT_ATH') {
      // ====== NOT_ATH 专用出场 (v20 BALANCED，与 shadow 完全对齐) ======
      // SL: 默认 -20%，highPnl 曾达 +35% 则上移至 0%（保本）
      // FAST_STOP: 前3次更新内 pnl < -5% 且从未涨过 → 立即出
      // TP1: +80% → 卖60%，剩40% SL移至成本
      // TP1后: pnl < 0 → 平剩余（成本止损）
      // DW: 10分钟无 TP1 → 全平
      // MH: 20分钟硬超时 → 全平
      // TP1超时: TP1后40分钟 → 平剩余

      // 1. 保本触发（永久记录，重启后从 highPnl 重算）
      if (!pos.breakeven && pos.highPnl >= 35) {
        pos.breakeven = true;
        console.log(`🔒 [NOT_ATH:保本] $${pos.symbol} 曾到+${pos.highPnl.toFixed(0)}% → SL上移至0%`);
      }
      const dynamicSL = pos.breakeven ? 0 : -20;

      // 2. FAST_STOP: 前3次价格更新内，从未涨过且已跌 -5%
      if (!pos.tp1 && !pos.breakeven && (pos._updateCount || 0) <= 3 && pos.highPnl <= 0 && pnl < -5) {
        console.log(`⚡ [NOT_ATH:FAST_STOP] $${pos.symbol} 入场即下跌 PnL:${pnl.toFixed(1)}% → 快速止损`);
        await this._triggerExit(pos, `NOT_ATH_FAST_STOP(PnL${pnl.toFixed(0)}%)`, 100);
        return;
      }

      // 3. 动态止损（TP1前）
      if (!pos.tp1 && pnl <= dynamicSL) {
        const label = pos.breakeven ? 'BREAKEVEN_SL' : 'HARD_SL_20';
        console.log(`🛑 [NOT_ATH:${label}] $${pos.symbol} PnL:${pnl.toFixed(1)}% ≤ ${dynamicSL}% → 全卖`);
        await this._triggerExit(pos, `NOT_ATH_${label}(PnL${pnl.toFixed(0)}%)`, 100);
        return;
      }

      // 4. TP1: +80% → 卖60%，剩余40% SL移至成本
      if (!pos.tp1 && pnl >= 80) {
        pos.tp1Time = Date.now();
        console.log(`🎯 [NOT_ATH:TP1] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ +80% → 卖出60%，SL→成本`);
        await this._triggerPartialSell(pos, 'TP1', 60, pnl);
        return;
      }

      // 5. TP1后成本止损（rawPnl < 0）
      if (pos.tp1 && !pos.moonbag && pnl < 0) {
        console.log(`🛡️ [NOT_ATH:TP1后成本SL] $${pos.symbol} PnL:${pnl.toFixed(1)}% < 0% → 平剩余仓位`);
        await this._triggerExit(pos, `NOT_ATH_COST_SL(PnL${pnl.toFixed(0)}%)`, 100);
        return;
      }

      // 6. TP2/TP3/TP4 连续止盈（超预期大涨时让利润跑）
      if (pos.tp1 && !pos.tp2 && pnl >= 100) {
        console.log(`🚀 [NOT_ATH:TP2] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ +100% → 卖出50%`);
        await this._triggerPartialSell(pos, 'TP2', 50, pnl);
        return;
      }
      if (pos.tp2 && !pos.tp3 && pnl >= 200) {
        console.log(`🌙 [NOT_ATH:TP3] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ +200% → 卖出50%`);
        await this._triggerPartialSell(pos, 'TP3', 50, pnl);
        return;
      }
      if (pos.tp3 && !pos.tp4 && pnl >= 500) {
        console.log(`🌕 [NOT_ATH:TP4] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ +500% → 卖出80%，进入Moonbag`);
        await this._triggerPartialSell(pos, 'TP4', 80, pnl);
        if (pos.tp4) { pos.moonbag = true; pos.moonbagHighPnl = pnl; }
        return;
      }

      // 7. Moonbag: 从最高点回撤35%平仓
      if (pos.moonbag) {
        if (pnl > (pos.moonbagHighPnl || 0)) pos.moonbagHighPnl = pnl;
        const moonPeak = pos.moonbagHighPnl || pnl;
        const dropPct = moonPeak > 0 ? ((moonPeak - pnl) / moonPeak) * 100 : 0;
        if (dropPct >= 35) {
          console.log(`🎫 [NOT_ATH:Moonbag] $${pos.symbol} 从+${moonPeak.toFixed(0)}%回撤${dropPct.toFixed(0)}% → 平仓`);
          await this._triggerExit(pos, `NOT_ATH_MOONBAG(peak+${moonPeak.toFixed(0)}%,PnL+${pnl.toFixed(0)}%)`, 100);
        }
        return;
      }

      // 8. TP1后40分钟超时 → 平剩余
      if (pos.tp1 && pos.tp1Time && (Date.now() - pos.tp1Time) >= 40 * 60 * 1000) {
        console.log(`⏰ [NOT_ATH:TP1超时40m] $${pos.symbol} TP1后已持仓40分钟 → 平剩余`);
        await this._triggerExit(pos, `NOT_ATH_TP1_TIMEOUT_40M(PnL${pnl.toFixed(0)}%)`, 100);
        return;
      }

      // 9. 死水超时: 10分钟无 TP1 → 全平
      if (!pos.tp1 && holdTimeMin >= 10) {
        console.log(`💧 [NOT_ATH:死水] $${pos.symbol} 10分钟未触TP1 PnL:${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}% → 全平`);
        await this._triggerExit(pos, `NOT_ATH_DEAD_WATER_10M(PnL${pnl.toFixed(0)}%)`, 100);
        return;
      }

      // 10. 20分钟硬超时
      if (holdTimeMin >= 20) {
        console.log(`⏰ [NOT_ATH:20分大限] $${pos.symbol} 持仓${holdTimeMin.toFixed(0)}m ≥ 20m → 强制全平`);
        await this._triggerExit(pos, `NOT_ATH_MAX_HOLD_20M(PnL${pnl.toFixed(0)}%)`, 100);
        return;
      }

    } else if (strategy === 'TP_NOSL') {
      // ====== v17.4: TP+100% / 无SL / 1h<30%快出 / 4h超时 ======
      // DB回测(5天): TP+100% ROI=+19.0% > TP+75% ROI=+12.2%
      // 无SL原因: MEME币73.5%的金狗经历>-50%最大回撤后才涨起来, SL-25%误杀41%的金狗
      // 1h快出: 持仓>1h且峰值<30% → 大概率不会涨了，及时止损

      if (pnl >= 100) {
        // 止盈: PnL ≥ +100% → 全卖
        console.log(`🎯 [v17.4:TP] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ +100% → 止盈全卖`);
        await this._triggerExit(pos, `TP_100(PnL+${pnl.toFixed(0)}%)`, 100);
        return;
      }

      // 1h快出: 持仓超过1小时，如果峰值<30%，大概率不会涨到TP → 尽早止损
      if (holdTimeMin >= 60 && pos.highPnl < 30) {
        console.log(`⚡ [v17.4:1h快出] $${pos.symbol} 持仓${holdTimeMin.toFixed(0)}m≥60m 峰值+${pos.highPnl.toFixed(1)}%<30% PnL:${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}% → 快速止损`);
        await this._triggerExit(pos, `QUICK_1H(peak+${pos.highPnl.toFixed(0)}%,PnL${pnl.toFixed(0)}%)`, 100);
        return;
      }

      // 无SL — 不止损, 只有超时退出

      // 时间停损: 4小时
      if (holdTimeMin >= 240) {
        console.log(`⏰ [v17.4:超时] $${pos.symbol} 持仓${(holdTimeMin/60).toFixed(1)}h ≥ 4h PnL:${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}% → 超时全卖`);
        await this._triggerExit(pos, `TIMEOUT_4H(PnL${pnl.toFixed(0)}%)`, 100);
        return;
      }

    } else if (strategy === 'TP_SL') {
      // ====== v17统一出场: TP+75% / SL-25% (简单止盈止损) ======
      // 回测依据: ATH#1 + MC$20-75K + Super(sig)≥80 + TP75/SL25 = 129笔, 42.6%WR, +2248%总PnL
      // TP = +75%, SL = -25%

      if (pnl >= 75) {
        // 止盈: PnL ≥ +75% → 全卖
        console.log(`🎯 [v17:TP] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ +75% → 止盈全卖`);
        await this._triggerExit(pos, `TP_75(PnL+${pnl.toFixed(0)}%)`, 100);
        return;
      }

      if (pnl <= -25) {
        // 止损: PnL ≤ -25% → 全卖
        console.log(`🛑 [v17:SL] $${pos.symbol} PnL:${pnl.toFixed(1)}% ≤ -25% → 止损全卖`);
        await this._triggerExit(pos, `SL_25(PnL${pnl.toFixed(0)}%)`, 100);
        return;
      }

      // 时间停损: 24小时
      if (holdTimeMin >= 1440) {
        console.log(`⏰ [v17:超时] $${pos.symbol} 持仓${(holdTimeMin/60).toFixed(1)}h ≥ 24h → 全卖`);
        await this._triggerExit(pos, 'TIMEOUT_24H', 100);
        return;
      }

    } else {
      // ====== 信号A出场: DynSL 动态止损 ======
      // 根据策略选择DynSL阶梯
      let dynslConfig;
      if (strategy === 'DYNSL_20_40_60') {
        // v15 信号A: 回测最优 DynSL 20/40/60
        dynslConfig = [
          { threshold: 20, floor: 0 },   // 峰值≥20%: 保本
          { threshold: 40, floor: 15 },   // 峰值≥40%: 止损+15%
          { threshold: 60, floor: 30 },   // 峰值≥60%: 止损+30%
        ];
      } else {
        // 默认 DYNSL_25_50_75 (旧策略)
        dynslConfig = [
          { threshold: 25, floor: 0 },
          { threshold: 50, floor: 20 },
          { threshold: 75, floor: 40 },
        ];
      }

      // 计算动态止损线
      const highPnl = pos.highPnl || 0;
      let dynamicSL = -25; // 默认止损
      for (const tier of dynslConfig) {
        if (highPnl >= tier.threshold) {
          dynamicSL = tier.floor;
        }
      }

      // 1. SL / 动态止损
      if (pnl <= dynamicSL) {
        const remainingPct = 100 - pos.soldPct;
        if (dynamicSL > -25) {
          console.log(`🛡️ [v15:DynSL] $${pos.symbol} PnL:${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}% < 止损线+${dynamicSL}% (峰值+${highPnl.toFixed(1)}%) → 卖出${remainingPct}%`);
          await this._triggerExit(pos, `DYN_SL(peak+${highPnl.toFixed(0)}%,floor+${dynamicSL}%)`, remainingPct);
        } else {
          await this._triggerExit(pos, 'STOP_LOSS', remainingPct);
        }
        return;
      }

      // 2. TP1: PnL ≥ +120% → 卖25%
      if (!pos.tp1 && pnl >= 120) {
        console.log(`🎯 [v15:TP1] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ 120% → 卖出25%锁利`);
        await this._triggerPartialSell(pos, 'TP1', 25, pnl);
        return;
      }

      // 3. TP2: PnL ≥ +300% → 卖30%
      if (pos.tp1 && !pos.tp2 && pnl >= 300) {
        console.log(`🚀 [v15:TP2] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ 300% → 卖出30%`);
        await this._triggerPartialSell(pos, 'TP2', 30, pnl);
        return;
      }

      // 4. Trailing Stop: TP2触发后，从峰值回撤25% → 卖剩余
      if (pos.tp2) {
        const dropFromPeak = pos.highPnl - pnl;
        const dropPct = pos.highPnl > 0 ? (dropFromPeak / pos.highPnl) * 100 : 0;
        if (dropPct >= 20 && dropPct < 25) {
          console.log(`⚠️  [接近TRAILING] $${pos.symbol} PnL:+${pnl.toFixed(1)}% peak:+${pos.highPnl.toFixed(1)}% 回撤:${dropPct.toFixed(0)}% (≥25%触发)`);
        }
        if (dropPct >= 25) {
          const remainingPct = 100 - pos.soldPct;
          await this._triggerExit(pos, `TRAILING(peak+${pos.highPnl.toFixed(0)}%,drop${dropPct.toFixed(0)}%)`, remainingPct);
          return;
        }
      }

      // 5. TP1保底: TP1已触发但未到TP2，跌回+15%以下 → 卖剩余
      if (pos.tp1 && !pos.tp2 && pnl < 15) {
        const remainingPct = 100 - pos.soldPct;
        console.log(`🛡️ [v15:TP1保底] $${pos.symbol} PnL:+${pnl.toFixed(1)}% < +15% → 卖剩余${remainingPct}%`);
        await this._triggerExit(pos, `TP1_FLOOR(PnL+${pnl.toFixed(0)}%<15%)`, remainingPct);
        return;
      }

      // 6. 时间停损: 24小时 → 全卖剩余
      if (holdTimeMin >= 1440) {
        const remainingPct = 100 - pos.soldPct;
        console.log(`⏰ [v16:超时] $${pos.symbol} 持仓${(holdTimeMin/60).toFixed(1)}h ≥ 24h → 全卖`);
        await this._triggerExit(pos, 'TIMEOUT_24H', remainingPct);
        return;
      }
    }
    } catch (error) {
      console.error(`🔴 [CRITICAL] _onPriceUpdate 异常: ${error.message}`, error.stack);
      if (pos) { pos.exitInProgress = false; pos.partialSellInProgress = false; }
    }
  }

  /**
   * 触发分批卖出
   */
  async _triggerPartialSell(pos, tpName, sellPct, currentPnl) {
    // 防止并发
    if (pos.partialSellInProgress || pos.exitInProgress || pos.closed) {
      return;
    }

    pos.partialSellInProgress = true;

    console.log(`\n🎯 [${tpName}] $${pos.symbol} | PnL: +${currentPnl.toFixed(1)}% | 卖出 ${sellPct}%`);

    try {
      // 计算卖出数量
      const sellAmount = Math.floor(pos.tokenAmount * (sellPct / 100));

      if (sellAmount <= 0) {
        console.log(`   ⚠️  卖出数量为0，跳过`);
        pos.partialSellInProgress = false;
        return;
      }

      // 执行卖出
      const result = await this.executor.sell(pos.tokenCA, sellAmount);

      if (!result.success) {
        throw new Error(result.error || 'Partial sell failed');
      }

      const solReceived = result.amountOut || 0;
      console.log(`   TX: ${result.txHash} | 收到: ${solReceived.toFixed(6)} SOL`);

      // 更新持仓状态
      // actualSoldFraction: 本次卖出占原始仓位的真实百分比
      // sellPct 是「当前余额的百分比」，需换算成原始仓位百分比
      const remainingBeforeSell = 100 - (pos.soldPct || 0);
      const actualSoldFraction = (sellPct / 100) * remainingBeforeSell;
      pos.tokenAmount -= sellAmount;
      pos.totalSolReceived += solReceived;
      pos.soldPct += actualSoldFraction;
      pos.lockedPnl += currentPnl * (actualSoldFraction / 100);
      pos[tpName.toLowerCase()] = true;

      // 🔧 v14.6 FIX: 异步同步链上真实余额（不阻塞主流程，下次卖出前会再次查链上余额）
      new Promise(async (resolve) => {
        try {
          await new Promise(r => setTimeout(r, 3000));
          const realBalance = await this.executor.getTokenBalance(pos.tokenCA);
          if (realBalance.amount > 0 && realBalance.amount !== pos.tokenAmount) {
            console.log(`   🔄 余额同步: 跟踪=${pos.tokenAmount} 链上=${realBalance.amount} → 使用链上值`);
            pos.tokenAmount = realBalance.amount;
          }
        } catch (_) {}
        resolve();
      });

      console.log(`   ✅ 已卖出 ${pos.soldPct}% | 剩余 ${100 - pos.soldPct}% | 锁定利润: +${pos.lockedPnl.toFixed(1)}%`);

      // 即时持久化 TP 状态到 DB，防止重启后重复触发
      try {
        this.db.prepare(`
          UPDATE live_positions SET tp1_triggered=?, tp2_triggered=?, tp3_triggered=?, tp4_triggered=?, moonbag_active=?, moonbag_high_pnl=?, sold_pct=?, token_amount=?, high_pnl=?, total_sol_received=?, locked_pnl=?
          WHERE token_ca=? AND status IN ('open', 'selling')
        `).run(pos.tp1 ? 1 : 0, pos.tp2 ? 1 : 0, pos.tp3 ? 1 : 0, pos.tp4 ? 1 : 0, pos.moonbag ? 1 : 0, pos.moonbagHighPnl || 0, pos.soldPct || 0, pos.tokenAmount, pos.highPnl, pos.totalSolReceived || 0, pos.lockedPnl || 0, pos.tokenCA);
      } catch (e) {
        console.warn(`   ⚠️ TP状态持久化失败: ${e.message}`);
      }

    } catch (error) {
      console.error(`   ❌ 分批卖出失败: ${error.message}`);
    } finally {
      pos.partialSellInProgress = false;
    }
  }

  /**
   * 触发全部卖出（或剩余部分）
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

    const finalPnl = pos.lastPnl;

    const icon = finalPnl > 0 ? '🟢' : '🔴';
    const sellPctStr = sellPct < 100 ? ` | 卖出剩余${sellPct}%` : '';
    console.log(`\n${icon} [EXIT] $${pos.symbol} | ${reason} | PnL: ${finalPnl >= 0 ? '+' : ''}${finalPnl.toFixed(1)}% | 最高: +${pos.highPnl.toFixed(1)}%${sellPctStr}`);

    // 标记中间状态，防止崩溃后重复卖出
    try {
      this.db.prepare(`UPDATE live_positions SET status='selling' WHERE token_ca=? AND status='open'`).run(pos.tokenCA);
    } catch(e) {}

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

      // 🔧 v14.6 FIX: 使用链上真实余额而非pos.tokenAmount（防止跟踪偏差导致残余）
      const actualBalance = balance.amount;
      if (actualBalance !== pos.tokenAmount) {
        console.log(`   ⚠️  余额修正: 跟踪=${pos.tokenAmount} 链上=${actualBalance} → 使用链上值`);
        pos.tokenAmount = actualBalance;
      }

      // 计算实际卖出数量（如果是部分卖出，使用剩余数量）
      const sellAmount = sellPct >= 100 ? actualBalance : Math.floor(actualBalance * (sellPct / 100));

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

        // 立即持久化 total_sol_received，防止重启时丢失
        try {
          this.db.prepare(`UPDATE live_positions SET total_sol_received=? WHERE token_ca=? AND status IN ('open','selling')`)
            .run(pos.totalSolReceived, pos.tokenCA);
        } catch (_) {}

        // 🔧 v14.6 FIX: 卖出后异步清理残余token（不阻塞主流程）
        this._asyncCleanupResidual(pos);

        // 如果有锁定利润，显示总收益
        if (pos.lockedPnl > 0) {
          const totalPnl = ((pos.totalSolReceived - pos.entrySol) / pos.entrySol * 100);
          console.log(`   💰 总收益: ${totalPnl >= 0 ? '+' : ''}${totalPnl.toFixed(1)}% | 锁定利润: +${pos.lockedPnl.toFixed(1)}%`);
        }

        // 记录亏损
        if (pos.totalSolReceived < pos.entrySol) {
          const loss = pos.entrySol - pos.totalSolReceived;
          this.executor.recordLoss(loss);
        }
      }
    } catch (error) {
      console.error(`   ❌ 卖出失败: ${error.message}`);

      // v4: 追踪累计总重试次数（跨cycle）
      pos._totalExitRetries = (pos._totalExitRetries || 0) + 1;

      // 🔧 BUG FIX: 更新重试计数器
      const retryInfo = this.retryCounter.get(pos.tokenCA) || { count: 0, pauseUntil: 0, lastRetryTime: 0 };
      retryInfo.count += 1;
      retryInfo.lastRetryTime = Date.now();

      // 🔧 指数退避: 第1次 15s, 第2次 30s, 第3次 60s, 第4次 120s (上限120s)
      const backoffMs = Math.min(this.retryPauseMs * Math.pow(2, retryInfo.count - 1), 120000);
      const backoffSec = (backoffMs / 1000).toFixed(0);

      // 🔧 BUG FIX: 检测滑点错误，暂停更长时间
      const isSlippageError = error.message.includes('6025') ||
                              error.message.includes('6024') ||
                              error.message.includes('Slippage') ||
                              error.message.includes('滑点');

      if (isSlippageError) {
        console.log(`   ⚠️  滑点错误，暂停 ${backoffSec}s 后重试 (第${retryInfo.count}次)`);
        retryInfo.pauseUntil = Date.now() + backoffMs;
      } else {
        // 非滑点错误也加退避，防止刷爆 API
        const nonSlipBackoff = Math.min(5000 * retryInfo.count, 30000);
        retryInfo.pauseUntil = Date.now() + nonSlipBackoff;
      }

      // 🔧 BUG FIX: 检查是否达到最大重试次数
      if (retryInfo.count >= this.maxRetries) {
        console.log(`   🚫 达到最大重试次数 ${this.maxRetries}，停止重试 $${pos.symbol}`);
        
        // 🔧 v14.6 FIX: 尝试用链上余额做最后一次紧急卖出
        try {
          const emergBalance = await this.executor.getTokenBalance(pos.tokenCA);
          if (emergBalance.amount > 0) {
            console.log(`   🚨 钱包仍有 ${emergBalance.amount} tokens，尝试紧急清理...`);
            const emergResult = await this.executor.emergencySell(pos.tokenCA, emergBalance.amount);
            if (emergResult.success) {
              console.log(`   ✅ 紧急清理成功`);
            } else {
              console.log(`   ❌ 紧急清理失败，${emergBalance.amount} tokens留在钱包，需手动处理`);
            }
          }
        } catch (emergErr) {
          console.log(`   ❌ 紧急清理异常: ${emergErr.message}`);
        }
        
        // 无论清理是否成功，都关闭仓位防止卡死
        pos.closed = true;
        pos.exitInProgress = false;
        pos.pendingSell = false;
        pos.retryLimitReached = true;
        this._closePosition(pos, finalPnl);
        this.priceMonitor.removeToken(pos.tokenCA);
        this.retryCounter.set(pos.tokenCA, retryInfo);
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
   * 关闭持仓（DB 更新）
   */
  _closePosition(pos, finalPnl) {
    try {
      this.db.prepare(`
        UPDATE live_positions
        SET exit_pnl=?, exit_reason=?, high_pnl=?, low_pnl=?, total_sol_received=?, status='closed', closed_at=?
        WHERE token_ca=? AND status IN ('open', 'selling')
      `).run(finalPnl, pos.exitReason, pos.highPnl, pos.lowPnl, pos.totalSolReceived || 0, Date.now(), pos.tokenCA);
    } catch (e) {
      console.warn(`⚠️  [LivePositionMonitor] DB 更新失败: ${e.message}`);
    }

    // 🔧 BUG FIX: 从内存Map中删除持仓，释放持仓槽位
    this.positions.delete(pos.tokenCA);

    // 通知 RiskManager 交易结果
    if (this.riskManager) {
      const isWin = finalPnl > 0;
      this.riskManager.recordTradeResult(isWin);
    }

    // 清理防抖和重试计数器，防止内存泄漏
    this.sellDebounce.delete(pos.tokenCA);
    this.retryCounter.delete(pos.tokenCA);

    // 🔧 通知退出回调（触发信号引擎冷却）
    for (const cb of this.onExitCallbacks) {
      try { cb(pos.symbol, pos.tokenCA, finalPnl); } catch (_) {}
    }
  }

  /**
   * 修复损坏数据：清理重复 MANUAL_SOLD 标记等
   */
  _fixCorruptedData() {
    try {
      // 1. 清理重复的 (MANUAL_SOLD) 标记
      const dupes = this.db.prepare(`
        SELECT id, exit_reason FROM live_positions
        WHERE exit_reason LIKE '%(MANUAL_SOLD)%(MANUAL_SOLD)%'
      `).all();
      for (const row of dupes) {
        // 只保留一个 (MANUAL_SOLD)
        const cleaned = row.exit_reason.replace(/\(MANUAL_SOLD\)/g, '').trim() + '(MANUAL_SOLD)';
        this.db.prepare(`UPDATE live_positions SET exit_reason = ? WHERE id = ?`).run(cleaned, row.id);
        console.log(`🔧 [数据修复] 清理重复标记: ${row.exit_reason} → ${cleaned}`);
      }

      // 2. 对 total_sol_received=0 但 status='closed' 的记录，标记 exit_pnl=-100（无法追踪）
      // 不覆盖已有的 exit_pnl
      const zeroRecv = this.db.prepare(`
        SELECT id, symbol, entry_sol, exit_pnl FROM live_positions
        WHERE status = 'closed' AND (total_sol_received = 0 OR total_sol_received IS NULL) AND exit_pnl IS NULL
      `).all();
      for (const row of zeroRecv) {
        this.db.prepare(`UPDATE live_positions SET exit_pnl = -100, total_sol_received = -1 WHERE id = ?`).run(row.id);
        console.log(`🔧 [数据修复] $${row.symbol} 无收回记录，标记 PnL=-100% (投入: ${row.entry_sol} SOL)`);
      }
    } catch (e) {
      console.warn(`⚠️ [数据修复] ${e.message}`);
    }
  }

  /**
   * 从 DB 恢复未关闭的持仓
   */
  async _restorePositions() {
    try {
      const rows = this.db.prepare(`SELECT * FROM live_positions WHERE status IN ('open', 'selling')`).all();
      if (rows.length === 0) return;

      console.log(`🔄 [LivePositionMonitor] 发现 ${rows.length} 个未关闭持仓，验证链上余额...`);
      let restored = 0;
      let cleaned = 0;

      for (const row of rows) {
        // 先检查链上余额，如果为 0 说明已手动卖出（包括 status='selling' 时崩溃后实际已卖出的情况）
        if (this.executor) {
          try {
            const balance = await this.executor.getTokenBalance(row.token_ca);
            if (balance.amount <= 0) {
              const exitReason = row.status === 'selling' ? 'SELL_COMPLETED_BEFORE_CRASH' : 'MANUAL_SELL';
              const solReceived = row.total_sol_received || 0;
              // 用已记录的 total_sol_received 计算真实 PnL
              const exitPnl = (solReceived > 0 && row.entry_sol > 0)
                ? ((solReceived - row.entry_sol) / row.entry_sol * 100)
                : null;
              console.log(`🧹 [清理] $${row.symbol} (${row.token_ca.substring(0, 8)}...) 链上余额为 0，标记已关闭 (${exitReason}) | 收回: ${solReceived.toFixed(4)} SOL${exitPnl !== null ? ' PnL:' + exitPnl.toFixed(1) + '%' : ''}`);
              this.db.prepare(`UPDATE live_positions SET status='closed', exit_reason=?, closed_at=?, exit_pnl=? WHERE token_ca=? AND status IN ('open', 'selling')`)
                .run(exitReason, Date.now(), exitPnl, row.token_ca);
              cleaned++;
              continue;
            }
          } catch (e) {
            console.warn(`⚠️ [恢复] $${row.symbol} 余额查询失败: ${e.message}，仍然恢复`);
          }
        }
        // 如果 status='selling' 但余额不为0，恢复为 open 状态继续监控
        if (row.status === 'selling') {
          try {
            this.db.prepare(`UPDATE live_positions SET status='open' WHERE token_ca=? AND status='selling'`).run(row.token_ca);
          } catch(e) {}
        }

        this.positions.set(row.token_ca, {
          tokenCA: row.token_ca,
          symbol: row.symbol,
          entryPrice: row.entry_price,
          entryMC: row.entry_mc,
          entrySol: row.entry_sol,
          tokenAmount: row.token_amount,
          tokenDecimals: row.token_decimals,
          highPnl: row.high_pnl || 0,
          lowPnl: row.low_pnl || 0,
          lastPnl: 0,
          totalSolReceived: row.total_sol_received || 0,
          entryTime: row.entry_time,
          closed: false,
          exitInProgress: false,
          exitReason: null,
          // v12: 恢复关键交易状态（解决server重启后conviction/tp1丢失问题）
          conviction: row.conviction || 'NORMAL',
          tp1: row.tp1_triggered === 1,
          tp1Time: row.tp1_triggered === 1 ? Date.now() : null,  // 重启后从现在起重计40min超时
          breakeven: false,  // 每次价格更新时由 highPnl>=35 自动重置
          // v18: 恢复 tp2/tp3/tp4/moonbag 状态（修复重启后重复触发 TP）
          tp2: row.tp2_triggered === 1,
          tp3: row.tp3_triggered === 1,
          tp4: row.tp4_triggered === 1,
          moonbag: row.moonbag_active === 1,
          moonbagHighPnl: row.moonbag_high_pnl || 0,
          soldPct: row.sold_pct || 0,
          dynFloorBelowCount: 0,
          lockedPnl: row.locked_pnl || 0,  // 恢复已锁定利润（重启后总收益计算正确）
          partialSellInProgress: false,
          // v15: 恢复出场策略
          exitStrategy: row.exit_strategy || 'ASYMMETRIC'
        });
        // 注册到价格监控（V2 需要 tokenAmount 和 decimals）
        const tokenAmount = row.token_amount || row.tokenAmount;
        if (this.priceMonitor.addToken.length === 3) {
          if (!tokenAmount) {
            console.warn(`⚠️ [恢复] $${row.symbol} (${row.token_ca.substring(0, 8)}...) 缺少 tokenAmount，跳过价格监控`);
            continue;
          }
          this.priceMonitor.addToken(row.token_ca, tokenAmount, row.token_decimals || 6);
        } else {
          this.priceMonitor.addToken(row.token_ca);
        }
        restored++;
        const convLabel = (row.conviction || 'NORMAL') === 'HIGH' ? '🔥HC' : 'NM';
        const exitLabel = row.exit_strategy || 'DYNSL_25_50_75';
        console.log(`  ✅ $${row.symbol} | peak:+${(row.high_pnl||0).toFixed(1)}% | ${convLabel} | 出场:${exitLabel}${row.tp1_triggered ? ' | TP1已触发' : ''}`);
      }
      console.log(`🔄 [LivePositionMonitor] 恢复 ${restored} 个持仓，清理 ${cleaned} 个已卖出`);
    } catch (e) {
      console.warn(`⚠️  [LivePositionMonitor] 恢复持仓失败: ${e.message}`);
    }
  }

  /**
   * 每30秒打印持仓实时状态（调试）
   */
  _printLiveStatus() {
    const open = [...this.positions.values()].filter(p => !p.closed);
    if (open.length === 0) return;

    const lines = open.map(p => {
      const age = ((Date.now() - p.entryTime) / 60000).toFixed(0);
      const priceData = this.priceMonitor.priceCache?.get(p.tokenCA);
      const hasPrice = priceData && (Date.now() - priceData.timestamp) < 30000;
      const priceAge = hasPrice ? ((Date.now() - priceData.timestamp) / 1000).toFixed(0) + 's' : 'N/A';
      return `  $${p.symbol.padEnd(10)} PnL:${p.lastPnl !== null ? (p.lastPnl >= 0 ? '+' : '') + p.lastPnl.toFixed(1) + '%' : 'N/A'} 峰:+${p.highPnl.toFixed(1)}% 价格:${hasPrice ? 'OK(' + priceAge + ')' : '❌无'} ${age}min`;
    });
    console.log(`📊 [持仓状态] ${open.length} 个活跃:\n${lines.join('\n')}`);
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
