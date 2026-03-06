/**
 * Live Position Monitor — v10
 *
 * 事件驱动仓位监控，监听 LivePriceMonitor 的 price-update 事件
 * 每次价格更新立即评估退出条件（~0.5 秒响应）
 *
 * v10 退出策略（基于OHLCV生命周期分析）：
 *
 * Phase 1（≤10 分钟）：
 * - FAST_STOP: ≤90s, highPnl≤0, PnL<-20% → 全卖
 * - STOP_LOSS: PnL≤-20% → 全卖
 * - TP1 锁利: PnL≥50% → 卖40%，剩余60%进入超宽模式
 * - 超宽模式(TP1后): 只用WIDE_TRAIL(-40%from peak) + ABSOLUTE_FLOOR(+10%)
 * - Dynamic Floor (未TP1时): 渐进式保底
 * - MINI_TS: highPnl 15-35%, PnL跌幅>阈值 from peak → 卖
 * - MID_STOP: ≥60s, highPnl<10%, PnL<-15% → 全卖
 * - TIMEOUT: HC:20min, NM:10min
 *
 * Phase 2（>10 分钟，仅 highPnl≥5%）：
 * - MOON_STOP: TP1后保留比率的峰值
 * - PEAK_EXIT: PnL从峰值回撤>40% → 卖剩余
 * - Dynamic Floor 继续生效
 *
 * ATH重复信号：不卖出，考虑加仓
 */

import Database from 'better-sqlite3';

export class LivePositionMonitor {
  constructor(priceMonitor, executor) {
    this.priceMonitor = priceMonitor;
    this.executor = executor;

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
  async start() {
    this.priceMonitor.on('price-update', this._onPriceUpdate);

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
  addPosition(tokenCA, symbol, entryPrice, entryMC, entrySol, tokenAmount, tokenDecimals, conviction = 'NORMAL') {
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

      // 新策略字段
      tp1: false,           // TP1是否已触发
      tp2: false,           // TP2是否已触发
      tp3: false,           // TP3是否已触发
      tp4: false,           // TP4是否已触发
      soldPct: 0,           // 已卖出百分比
      lockedPnl: 0,         // 已锁定的利润
      moonMode: false,      // 是否进入月球模式
      moonHighPnl: 0,       // 月球模式的最高PnL
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

    // 持久化
    try {
      this.db.prepare(`
        INSERT INTO live_positions (token_ca, symbol, entry_price, entry_mc, entry_sol, token_amount, token_decimals, entry_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(tokenCA, position.symbol, entryPrice, entryMC, entrySol, tokenAmount, tokenDecimals, position.entryTime);
    } catch (e) {
      console.warn(`⚠️  [LivePositionMonitor] DB 写入失败: ${e.message}`);
    }

    console.log(`🎯 [LivePositionMonitor] 新持仓: $${position.symbol} | ${entrySol} SOL | ${tokenAmount} tokens${conviction === 'HIGH' ? ' | 🔥高信念模式' : ''}`);
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

    // 接近退出线时打警告（FINAL策略: PEAK_EXIT 回撤>40%）
    if (pos.highPnl >= 30) {
      const dropPct = pos.highPnl > 0 ? ((pos.highPnl - pnl) / pos.highPnl) * 100 : 0;
      if (dropPct > 30 && dropPct <= 40) {
        console.log(`⚠️  [接近PEAK_EXIT] $${pos.symbol} PnL:+${pnl.toFixed(1)}% peak:+${pos.highPnl.toFixed(1)}% 回撤:${dropPct.toFixed(0)}% (>40%触发)`);
      }
    }

    // ==================== v10 策略退出条件评估 ====================
    const holdTimeMin = holdTimeSec / 60;
    const isPhase1 = holdTimeMin <= 10;  // v10: Phase 1 延长到10分钟（OHLCV验证：meme coin主升浪5-30分钟）
    const isPhase2 = holdTimeMin > 10;   // Phase 2: >10 分钟

    // ========== Phase 1: ≤5 分钟 ==========
    if (isPhase1) {

      // FAST_STOP: ≤90s, highPnl≤0, PnL<-20% → 全卖（数据驱动：持仓<10s全亏，需给更多反弹时间）
      if (holdTimeSec <= 90 && pos.highPnl <= 0 && pnl < -20) {
        await this._triggerExit(pos, 'FAST_STOP', 100);
        return;
      }

      // STOP_LOSS: PnL ≤ -20% → 全卖（任何时间）
      if (pnl <= -20) {
        const remainingPct = 100 - pos.soldPct;
        await this._triggerExit(pos, 'STOP_LOSS', remainingPct);
        return;
      }

      // v10: Phase1 TP锁利 — PnL≥50%时卖出40%，剩余60%进入超宽模式
      // 数据驱动: hamster +10.4%退出但2.22x, 汉堡 -0.5%退出但5.53x
      // OHLCV验证: +50%锁利后,剩余60%用超宽trailing可以捕获更多涨幅
      if (!pos.tp1 && pnl >= 50) {
        console.log(`🎯 [v10:TP1锁利] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ 50% → 卖出40%锁利`);
        await this._triggerPartialSell(pos, 'TP1', 40, pnl);
        return;
      }

      // v10: TP1已触发 → 剩余60%使用超宽模式（跳过DYN_FLOOR/MINI_TS）
      if (pos.tp1) {
        // 绝对保底: 剩余仓位PnL低于+10%时全部卖出（确保剩余部分至少赚10%）
        if (pnl < 10) {
          const remainingPct = 100 - pos.soldPct;
          console.log(`🛡️ [v10:保底] $${pos.symbol} PnL:+${pnl.toFixed(1)}% < +10% → 保底卖出剩余${remainingPct}%`);
          await this._triggerExit(pos, `TP1_FLOOR(PnL+${pnl.toFixed(0)}%<10%)`, remainingPct);
          return;
        }

        // 超宽trailing: 随峰值收紧（峰值越高,保留比率越大）
        if (pos.highPnl >= 50) {
          let retainRatio;
          if (pos.highPnl >= 500) retainRatio = 0.75;       // 峰值500%+: 保留75%
          else if (pos.highPnl >= 200) retainRatio = 0.70;  // 峰值200%+: 保留70%
          else if (pos.highPnl >= 100) retainRatio = 0.65;  // 峰值100%+: 保留65%
          else retainRatio = 0.60;                          // 峰值50-100%: 保留60%

          const trailFloor = pos.highPnl * retainRatio;
          if (pnl < trailFloor) {
            // 需要连续2次确认（防止单次价格波动触发）
            pos._wideTrailBelowCount = (pos._wideTrailBelowCount || 0) + 1;
            if (pos._wideTrailBelowCount >= 2) {
              const remainingPct = 100 - pos.soldPct;
              await this._triggerExit(pos, `WIDE_TRAIL(peak+${pos.highPnl.toFixed(0)}%,floor+${trailFloor.toFixed(0)}%,retain${(retainRatio*100).toFixed(0)}%)`, remainingPct);
              return;
            }
          } else {
            pos._wideTrailBelowCount = 0;
          }
        }

        // TP1后继续持有（超宽模式，等待更高收益或TIMEOUT）
        return;
      }

      // === 以下为未触发TP1时的正常退出逻辑 ===

      // Dynamic Floor（渐进式保底：floor = peak * ratio）
      // v9: HIGH conviction使用更宽松的ratio + 需要连续2次确认才触发
      // v10: 保持v9的DYN_FLOOR逻辑（对未达+50%的仓位仍然有效）
      const isHigh = pos.conviction === 'HIGH';
      const minHoldForFloor = isHigh ? 45 : 0;  // HC需要至少45秒才开始DYN_FLOOR评估

      if (holdTimeSec < minHoldForFloor) {
        // HC最小持有期内：只做FAST_STOP/STOP_LOSS保护，不做DYN_FLOOR/MINI_TS
        // (FAST_STOP和STOP_LOSS已在上面处理)
        return;
      }

      if (pos.highPnl >= 50) {
        const floor = pos.highPnl * (isHigh ? 0.25 : 0.45);  // v9: HC 25%(was 30%), NORMAL 45%(was 50%)
        if (pnl < floor) {
          pos.dynFloorBelowCount = (pos.dynFloorBelowCount || 0) + 1;
          if (pos.dynFloorBelowCount >= 2) {  // v9: 需要连续2次确认
            const remainingPct = 100 - pos.soldPct;
            await this._triggerExit(pos, `DYN_FLOOR(peak+${pos.highPnl.toFixed(0)}%,floor+${floor.toFixed(0)}%${isHigh ? ',HC' : ''})`, remainingPct);
            return;
          }
        } else {
          pos.dynFloorBelowCount = 0;  // 回到floor以上,重置计数
        }
      } else if (pos.highPnl >= 30) {
        const floor = pos.highPnl * (isHigh ? 0.20 : 0.40);  // v9: HC 20%(was 25%), NORMAL 40%(was 45%)
        if (pnl < floor) {
          pos.dynFloorBelowCount = (pos.dynFloorBelowCount || 0) + 1;
          if (pos.dynFloorBelowCount >= 2) {
            const remainingPct = 100 - pos.soldPct;
            await this._triggerExit(pos, `DYN_FLOOR(peak+${pos.highPnl.toFixed(0)}%,floor+${floor.toFixed(0)}%${isHigh ? ',HC' : ''})`, remainingPct);
            return;
          }
        } else {
          pos.dynFloorBelowCount = 0;
        }
      } else if (pos.highPnl >= 15) {
        const floor = pos.highPnl * (isHigh ? 0.10 : 0.35);  // v9: HC 10%(was 20%), NORMAL 35%(was 40%)
        if (pnl < floor) {
          pos.dynFloorBelowCount = (pos.dynFloorBelowCount || 0) + 1;
          if (pos.dynFloorBelowCount >= 2) {
            const remainingPct = 100 - pos.soldPct;
            await this._triggerExit(pos, `DYN_FLOOR(peak+${pos.highPnl.toFixed(0)}%,floor+${floor.toFixed(0)}%${isHigh ? ',HC' : ''})`, remainingPct);
            return;
          }
        } else {
          pos.dynFloorBelowCount = 0;
        }
      } else if (pos.highPnl >= 10) {
        const floor = pos.highPnl * (isHigh ? 0.10 : 0.45);  // v9: HC 10%(was 20%), NORMAL 45%(was 50%)
        if (pnl < floor) {
          pos.dynFloorBelowCount = (pos.dynFloorBelowCount || 0) + 1;
          if (pos.dynFloorBelowCount >= 2) {
            const remainingPct = 100 - pos.soldPct;
            await this._triggerExit(pos, `DYN_FLOOR(peak+${pos.highPnl.toFixed(0)}%,floor+${floor.toFixed(0)}%${isHigh ? ',HC' : ''})`, remainingPct);
            return;
          }
        } else {
          pos.dynFloorBelowCount = 0;
        }
      }

      // MINI_TS: highPnl 15-35%, PnL从峰值回撤超过阈值 → 卖
      // v9: 放宽阈值让赢家跑得更远（hamster +10.4%退出但涨了2.22x）
      // v9: HIGH 70%(was 55%), NORMAL 50%(was 35%)
      if (pos.highPnl >= 15 && pos.highPnl <= 35) {
        const dropFromPeak = pos.highPnl - pnl;
        const dropPct = pos.highPnl > 0 ? (dropFromPeak / pos.highPnl) * 100 : 0;
        const dropThreshold = pos.conviction === 'HIGH' ? 70 : 50;
        if (dropPct > dropThreshold) {
          const remainingPct = 100 - pos.soldPct;
          await this._triggerExit(pos, `MINI_TS(peak+${pos.highPnl.toFixed(0)}%,drop${dropPct.toFixed(0)}%${pos.conviction === 'HIGH' ? ',HC' : ''})`, remainingPct);
          return;
        }
      }

      // MID_STOP: ≥60s, highPnl<10%, PnL<-15% → 全卖（数据驱动：30s-1min有83%WR，给更多时间）
      if (holdTimeSec >= 60 && pos.highPnl < 10 && pnl < -15) {
        const remainingPct = 100 - pos.soldPct;
        await this._triggerExit(pos, 'MID_STOP', remainingPct);
        return;
      }

      // TIMEOUT: v10: HC延长到20min, NORMAL延长到10min
      // OHLCV验证: meme coin主升浪5-30分钟, TOP10涨幅币持续1-8小时
      // v10: TP1已触发的不走这个TIMEOUT（在上面tp1逻辑中已处理）
      const timeoutSec = pos.conviction === 'HIGH' ? 1200 : 600;
      if (holdTimeSec > timeoutSec && pos.highPnl < 5 && pnl < 0) {
        const remainingPct = 100 - pos.soldPct;
        await this._triggerExit(pos, 'TIMEOUT', remainingPct);
        return;
      }

      // Phase 1 内其他情况 HOLD
      return;
    }

    // ========== Phase 2: >5 分钟（仅 highPnl ≥ 5% 才进入） ==========
    if (isPhase2) {
      // highPnl < 5% 的持仓在 Phase 1 TIMEOUT 中已处理
      // 如果到了 Phase 2 但 highPnl < 5%，也触发超时退出
      if (pos.highPnl < 5) {
        if (pnl < 0) {
          const remainingPct = 100 - pos.soldPct;
          await this._triggerExit(pos, 'TIMEOUT_P2', remainingPct);
          return;
        }
        // highPnl < 5% 但当前盈利，继续观察
        return;
      }

      // STOP_LOSS 在 Phase 2 仍然有效
      if (pnl <= -20) {
        const remainingPct = 100 - pos.soldPct;
        await this._triggerExit(pos, 'STOP_LOSS', remainingPct);
        return;
      }

      // v10: Phase2 TP1 — 如果Phase1没有触发过TP1（币涨得慢，Phase2才到+50%）
      if (!pos.tp1 && pnl >= 50) {
        console.log(`🎯 [v10:TP1-P2] $${pos.symbol} Phase2 PnL:+${pnl.toFixed(1)}% ≥ 50% → 卖出40%锁利`);
        await this._triggerPartialSell(pos, 'TP1', 40, pnl);
        return;
      }

      // MOON_STOP: TP1 已触发后，进入月球模式
      if (pos.tp1 && !pos.moonMode && pnl >= 100) {
        pos.moonMode = true;
        pos.moonHighPnl = pnl;
        console.log(`🌙 [MOON_MODE] $${pos.symbol} 进入月球模式 @ +${pnl.toFixed(0)}%`);
      }

      if (pos.moonMode) {
        pos.moonHighPnl = Math.max(pos.moonHighPnl, pnl);
        // v10: 月球模式保留比率随峰值增加（峰值越高保留越多）
        let retainRatio;
        if (pos.moonHighPnl >= 500) retainRatio = 0.75;
        else if (pos.moonHighPnl >= 200) retainRatio = 0.70;
        else retainRatio = 0.60;
        const moonFloor = pos.moonHighPnl * retainRatio;
        if (pnl < moonFloor) {
          const remainingPct = 100 - pos.soldPct;
          await this._triggerExit(pos, `MOON_STOP(peak+${pos.moonHighPnl.toFixed(0)}%,retain${(retainRatio*100).toFixed(0)}%)`, remainingPct);
          return;
        }
      }

      // v10: TP1后Phase2的绝对保底（同Phase1）
      if (pos.tp1 && pnl < 10) {
        const remainingPct = 100 - pos.soldPct;
        await this._triggerExit(pos, `TP1_FLOOR_P2(PnL+${pnl.toFixed(0)}%<10%)`, remainingPct);
        return;
      }

      // PEAK_EXIT: PnL从峰值回撤>40% → 卖剩余
      if (pos.highPnl >= 30) {
        const dropFromPeak = pos.highPnl - pnl;
        const dropPct = pos.highPnl > 0 ? (dropFromPeak / pos.highPnl) * 100 : 0;
        if (dropPct > 40) {
          const remainingPct = 100 - pos.soldPct;
          await this._triggerExit(pos, `PEAK_EXIT(peak+${pos.highPnl.toFixed(0)}%,drop${dropPct.toFixed(0)}%)`, remainingPct);
          return;
        }
      }

      // Dynamic Floor（Phase 2 渐进式保底，v4提升比率）
      if (pos.highPnl >= 50) {
        const floor = pos.highPnl * 0.50;
        if (pnl < floor) {
          const remainingPct = 100 - pos.soldPct;
          await this._triggerExit(pos, `DYN_FLOOR_P2(peak+${pos.highPnl.toFixed(0)}%,floor+${floor.toFixed(0)}%)`, remainingPct);
          return;
        }
      } else if (pos.highPnl >= 30) {
        const floor = pos.highPnl * 0.45;
        if (pnl < floor) {
          const remainingPct = 100 - pos.soldPct;
          await this._triggerExit(pos, `DYN_FLOOR_P2(peak+${pos.highPnl.toFixed(0)}%,floor+${floor.toFixed(0)}%)`, remainingPct);
          return;
        }
      } else if (pos.highPnl >= 15) {
        const floor = pos.highPnl * 0.40;
        if (pnl < floor) {
          const remainingPct = 100 - pos.soldPct;
          await this._triggerExit(pos, `DYN_FLOOR_P2(peak+${pos.highPnl.toFixed(0)}%,floor+${floor.toFixed(0)}%)`, remainingPct);
          return;
        }
      } else if (pos.highPnl >= 10) {
        const floor = pos.highPnl * 0.50;
        if (pnl < floor) {
          const remainingPct = 100 - pos.soldPct;
          await this._triggerExit(pos, `DYN_FLOOR_P2(peak+${pos.highPnl.toFixed(0)}%,floor+${floor.toFixed(0)}%)`, remainingPct);
          return;
        }
      }
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
      pos.tokenAmount -= sellAmount;
      pos.totalSolReceived += solReceived;
      pos.soldPct += sellPct;
      pos.lockedPnl += currentPnl * (sellPct / 100);
      pos[tpName.toLowerCase()] = true;

      console.log(`   ✅ 已卖出 ${pos.soldPct}% | 剩余 ${100 - pos.soldPct}% | 锁定利润: +${pos.lockedPnl.toFixed(1)}%`);

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

      // 计算实际卖出数量（如果是部分卖出，使用剩余数量）
      const sellAmount = sellPct >= 100 ? pos.tokenAmount : Math.floor(pos.tokenAmount * (sellPct / 100));

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

    // 🔧 通知退出回调（触发信号引擎冷却）
    for (const cb of this.onExitCallbacks) {
      try { cb(pos.symbol, pos.tokenCA, finalPnl); } catch (_) {}
    }
  }

  /**
   * 从 DB 恢复未关闭的持仓
   */
  async _restorePositions() {
    try {
      const rows = this.db.prepare(`SELECT * FROM live_positions WHERE status='open'`).all();
      if (rows.length === 0) return;

      console.log(`🔄 [LivePositionMonitor] 发现 ${rows.length} 个未关闭持仓，验证链上余额...`);
      let restored = 0;
      let cleaned = 0;

      for (const row of rows) {
        // 先检查链上余额，如果为 0 说明已手动卖出
        if (this.executor) {
          try {
            const balance = await this.executor.getTokenBalance(row.token_ca);
            if (balance.amount <= 0) {
              console.log(`🧹 [清理] $${row.symbol} (${row.token_ca.substring(0, 8)}...) 链上余额为 0，标记已关闭`);
              this.db.prepare(`UPDATE live_positions SET status='closed', exit_reason='MANUAL_SELL', closed_at=? WHERE token_ca=? AND status='open'`)
                .run(Date.now(), row.token_ca);
              cleaned++;
              continue;
            }
          } catch (e) {
            console.warn(`⚠️ [恢复] $${row.symbol} 余额查询失败: ${e.message}，仍然恢复`);
          }
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
          exitReason: null
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
