/**
 * Shadow PnL Tracker
 *
 * 追踪 shadow 买入的代币实时 PnL
 * 每隔一段时间检查价格，记录最高/最低/当前 PnL
 */

import axios from 'axios';
import Database from 'better-sqlite3';
import path from 'path';

export class ShadowPnlTracker {
  constructor() {
    this.positions = new Map();
    this.interval = null;
    this.checkIntervalMs = 1.5 * 1000; // 1.5 秒（与实盘一致）
    this.livePriceMonitor = null; // 注入 LivePriceMonitor

    // Shadow模式固定交易损耗（滑点+手续费，买卖合计约7%）
    this.tradingCostPct = 7;

    // 持久化到 SQLite
    const dbPath = process.env.DB_PATH || './data/sentiment_arb.db';
    this.db = new Database(dbPath);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS shadow_pnl (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_ca TEXT NOT NULL,
        symbol TEXT,
        score INTEGER,
        entry_mc REAL,
        entry_time INTEGER,
        exit_pnl REAL,
        high_pnl REAL,
        low_pnl REAL,
        exit_reason TEXT,
        closed INTEGER DEFAULT 0,
        closed_at INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);
    this.db.exec(`CREATE INDEX IF NOT EXISTS idx_shadow_pnl_closed ON shadow_pnl(closed)`);
  }

  /**
   * 记录一个 shadow 买入
   */
  addPosition(tokenCA, symbol, entryMC, score) {
    // 防止重复购买同一 token
    if (this.hasOpenPosition(tokenCA)) {
      console.log(`⏭️ [Shadow Tracker] $${symbol} 已有未平仓持仓，跳过`);
      return false;
    }

    this.positions.set(tokenCA, {
      symbol,
      entryMC,
      entryTime: Date.now(),
      score,
      highPnl: 0,
      lowPnl: 0,
      lastPnl: null,
      currentMC: entryMC,
      checks: 0,
      closed: false,
      exitReason: null
    });

    // 持久化
    try {
      this.db.prepare(`
        INSERT INTO shadow_pnl (token_ca, symbol, score, entry_mc, entry_time, high_pnl, low_pnl, closed)
        VALUES (?, ?, ?, ?, ?, 0, 0, 0)
      `).run(tokenCA, symbol, score, entryMC, Date.now());
    } catch (e) { /* ignore */ }

    console.log(`📝 [Shadow Tracker] 记录: $${symbol} | 入场MC: $${(entryMC / 1000).toFixed(1)}K | 评分: ${score}`);
    return true;
  }

  /**
   * 检查是否有未平仓持仓
   */
  hasOpenPosition(tokenCA) {
    const pos = this.positions.get(tokenCA);
    return pos && !pos.closed;
  }

  /**
   * 启动定时检查
   */
  start() {
    if (this.interval) return;
    this.interval = setInterval(() => this.checkAll(), this.checkIntervalMs);
    console.log(`✅ [Shadow Tracker] 启动 | 检查间隔: ${this.checkIntervalMs / 1000}s`);
  }

  /**
   * 设置 LivePriceMonitor
   */
  setLivePriceMonitor(monitor) {
    this.livePriceMonitor = monitor;
    console.log('✅ [Shadow Tracker] LivePriceMonitor 已注入');
  }

  /**
   * 检查所有持仓
   */
  async checkAll() {
    const open = [...this.positions.entries()].filter(([, p]) => !p.closed);
    if (open.length === 0) return;

    console.log(`\n📊 [Shadow Tracker] 检查 ${open.length} 个持仓...`);

    // 优先用 LivePriceMonitor（Jupiter 实时价格），fallback 到 DexScreener
    const mcMap = new Map();
    const cas = open.map(([ca]) => ca);

    if (this.livePriceMonitor) {
      // 从 LivePriceMonitor 缓存获取价格
      for (const ca of cas) {
        const cached = this.livePriceMonitor.priceCache.get(ca);
        if (cached && cached.mc && (Date.now() - cached.timestamp) < 15000) {
          mcMap.set(ca, { mc: cached.mc, liq: cached.liquidity || 0, source: cached.source });
        }
      }
    }

    // 没有从 LivePriceMonitor 拿到的，fallback 到 DexScreener
    const missing = cas.filter(ca => !mcMap.has(ca));
    if (missing.length > 0) {
      const batchSize = 30;
      for (let i = 0; i < missing.length; i += batchSize) {
        const batch = missing.slice(i, i + batchSize);
        try {
          const res = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${batch.join(',')}`, { timeout: 10000 });
          const pairs = res.data?.pairs || [];
          for (const pair of pairs) {
            if (pair.baseToken?.address && pair.marketCap) {
              const addr = pair.baseToken.address;
              if (!mcMap.has(addr) || (pair.liquidity?.usd || 0) > (mcMap.get(addr).liq || 0)) {
                mcMap.set(addr, { mc: pair.marketCap, liq: pair.liquidity?.usd || 0, source: 'dexscreener' });
              }
            }
          }
        } catch (e) {
          console.log(`  ⚠️ DexScreener fallback 查询失败: ${e.message}`);
        }
      }
    }

    let winners = 0, losers = 0, totalPnl = 0;

    for (const [ca, pos] of open) {
      const priceData = mcMap.get(ca);

      if (!priceData) {
        pos.lastPnl = -100;
        pos.lowPnl = -100;
        pos.closed = true;
        pos.exitReason = 'DELISTED';
        console.log(`  ⚫ $${pos.symbol} 已下架 → -100%`);
        // 持久化
        try {
          this.db.prepare(`
            UPDATE shadow_pnl SET exit_pnl=?, high_pnl=?, low_pnl=?, exit_reason=?, closed=1, closed_at=?
            WHERE token_ca=? AND closed=0
          `).run(pos.lastPnl, pos.highPnl, pos.lowPnl, pos.exitReason, Date.now(), ca);
        } catch (e) { /* ignore */ }
        totalPnl += pos.lastPnl;
        losers++;
        continue;
      }

      const currentMC = priceData.mc;

      // 第一次检查：用实时价格修正入场 MC（DexScreener 数据有延迟）
      if (pos.checks === 0 && currentMC > 0) {
        const drift = pos.entryMC > 0 ? Math.abs((currentMC - pos.entryMC) / pos.entryMC * 100) : 0;
        if (drift > 20) {
          console.log(`  🔄 $${pos.symbol} 入场MC修正: $${(pos.entryMC/1000).toFixed(1)}K → $${(currentMC/1000).toFixed(1)}K (偏差${drift.toFixed(0)}%)`);
          pos.entryMC = currentMC;
          // 更新数据库
          try {
            this.db.prepare(`UPDATE shadow_pnl SET entry_mc=? WHERE token_ca=? AND closed=0`).run(currentMC, ca);
          } catch (e) { /* ignore */ }
        }
      }

      const rawPnl = pos.entryMC > 0 ? ((currentMC - pos.entryMC) / pos.entryMC) * 100 : 0;
      const pnl = rawPnl - this.tradingCostPct; // 含交易损耗，用于显示和最终收益

      pos.currentMC = currentMC;
      pos.lastPnl = pnl;
      pos.checks++;
      if (rawPnl > pos.highPnl) pos.highPnl = rawPnl; // 用原始PnL追踪峰值
      if (pnl < pos.lowPnl) pos.lowPnl = pnl;

      // 止损判断全部用 rawPnl（不含交易损耗），避免入场就触发止损
      // 注意：已分批止盈的仓位不用普通止损，用 MOON_STOP

      // 模拟止损 -20%（原始跌20%，含损耗实际-27%）
      if (rawPnl <= -20 && !pos.closed && !pos.tp1) {
        pos.closed = true;
        pos.exitReason = 'STOP_LOSS';
        pos.lastPnl = pnl;
      }

      // 快速止损：前 3 次检查（~15s），从未涨过且原始 < -5%，直接出
      if (!pos.closed && !pos.tp1 && pos.checks <= 3 && pos.highPnl <= 0 && rawPnl < -5) {
        pos.closed = true;
        pos.exitReason = 'FAST_STOP';
        pos.lastPnl = pnl;
      }

      // 中速止损：任何时候原始 < -12% 且从未涨过 +10%，直接出
      // 已分批止盈的仓位不用这个，用 MOON_STOP
      if (!pos.closed && !pos.tp1 && rawPnl < -12 && pos.highPnl < 10) {
        pos.closed = true;
        pos.exitReason = 'MID_STOP';
        pos.lastPnl = pnl;
      }

      // 渐进式分批止盈（策略C）：
      // +50%: 卖30% | +100%: 卖20% | +200%: 卖20% | 剩30% trailing
      // 回测76笔: 总PnL +968% vs 当前+753%, 胜率57.9%
      // 用 rawPnl 判断触发，锁定利润用 pnl（含损耗）
      if (!pos.closed) {
        if (!pos.tp1 && rawPnl >= 50) {
          pos.tp1 = true;
          pos.soldPct = 30;
          pos.remainingPct = 70;
          pos.lockedPnl = pnl * 0.30;
          pos.moonHighPnl = rawPnl;
          console.log(`  💰 $${pos.symbol} +${rawPnl.toFixed(0)}% → TP1卖30%锁利，留70%`);
        }
        if (!pos.tp2 && pos.tp1 && rawPnl >= 100) {
          pos.tp2 = true;
          pos.soldPct = 50;
          pos.remainingPct = 50;
          pos.lockedPnl += pnl * 0.20;
          console.log(`  💰 $${pos.symbol} +${rawPnl.toFixed(0)}% → TP2卖20%锁利，留50%`);
        }
        if (!pos.tp3 && pos.tp2 && rawPnl >= 200) {
          pos.tp3 = true;
          pos.soldPct = 70;
          pos.remainingPct = 30;
          pos.lockedPnl += pnl * 0.20;
          console.log(`  💰 $${pos.symbol} +${rawPnl.toFixed(0)}% → TP3卖20%锁利，留30%`);
        }
      }

      // 更新剩余仓位的独立峰值（用rawPnl）
      if (pos.tp1 && !pos.closed && rawPnl > (pos.moonHighPnl || 0)) {
        pos.moonHighPnl = rawPnl;
      }

      // 移动止盈（用rawPnl判断）
      if (!pos.closed && pos.highPnl >= 15) {
        if (pos.tp1) {
          // 已分批止盈，剩余仓位用移动止盈
          // 回撤到剩余仓位峰值的 55% 才出
          const moonExit = pos.moonHighPnl * 0.55;
          const minMoonExit = 25;
          const exitLine = Math.max(moonExit, minMoonExit);
          if (rawPnl < exitLine) {
            pos.closed = true;
            const finalPnl = pos.lockedPnl + pnl * (pos.remainingPct / 100);
            pos.exitReason = `MOON_STOP(peak+${pos.highPnl.toFixed(0)}%)`;
            pos.lastPnl = finalPnl;
          }
        } else {
          // 未触发分批止盈，用原来的移动止盈
          const keepRatio = pos.highPnl >= 50 ? 0.70 : 0.65;
          const trailExit = pos.highPnl * keepRatio;
          const minExit = 10;
          const exitLine = Math.max(trailExit, minExit);
          if (rawPnl < exitLine) {
            pos.closed = true;
            pos.exitReason = `TRAIL_STOP(peak+${pos.highPnl.toFixed(0)}%)`;
            pos.lastPnl = Math.max(pnl, minExit - this.tradingCostPct);
          }
        }
      }

      const displayPnl = pos.closed ? pos.lastPnl : pnl;
      const icon = displayPnl > 0 ? '🟢' : displayPnl > -20 ? '🟡' : '🔴';
      const exitTag = pos.closed ? ` [${pos.exitReason}]` : '';
      const soldTag = pos.tp1 && !pos.closed ? ` (${pos.remainingPct}%仓)` : '';
      console.log(`  ${icon} $${pos.symbol.padEnd(12)} PnL: ${displayPnl >= 0 ? '+' : ''}${displayPnl.toFixed(1)}%${soldTag} | 最高: +${pos.highPnl.toFixed(1)}% | 最低: ${pos.lowPnl.toFixed(1)}% | MC: $${(currentMC / 1000).toFixed(1)}K${exitTag}`);

      // 持久化
      if (pos.closed) {
        try {
          this.db.prepare(`
            UPDATE shadow_pnl SET exit_pnl=?, high_pnl=?, low_pnl=?, exit_reason=?, closed=1, closed_at=?
            WHERE token_ca=? AND closed=0
          `).run(pos.lastPnl, pos.highPnl, pos.lowPnl, pos.exitReason, Date.now(), ca);
        } catch (e) { /* ignore */ }
      } else {
        try {
          this.db.prepare(`
            UPDATE shadow_pnl SET high_pnl=?, low_pnl=? WHERE token_ca=? AND closed=0
          `).run(pos.highPnl, pos.lowPnl, ca);
        } catch (e) { /* ignore */ }
      }

      totalPnl += pos.closed ? pos.lastPnl : pnl;
      if (pnl > 0) winners++;
      else losers++;
    }

    // 汇总
    const allPositions = [...this.positions.values()];
    const closedPositions = allPositions.filter(p => p.closed);
    const openPositions = allPositions.filter(p => !p.closed);

    console.log(`\n  ─── Shadow 汇总 ───`);
    console.log(`  总持仓: ${allPositions.length} | 开放: ${openPositions.length} | 已关闭: ${closedPositions.length}`);
    console.log(`  本轮: ${winners}W / ${losers}L | 胜率: ${open.length > 0 ? (winners / open.length * 100).toFixed(0) : 0}%`);

    if (closedPositions.length > 0) {
      const closedPnl = closedPositions.reduce((s, p) => s + p.lastPnl, 0);
      const closedWinners = closedPositions.filter(p => p.lastPnl > 0);
      const closedWinRate = (closedWinners.length / closedPositions.length * 100).toFixed(0);
      console.log(`  已关闭胜率: ${closedWinRate}% (${closedWinners.length}W / ${closedPositions.length - closedWinners.length}L) | 总PnL: ${closedPnl >= 0 ? '+' : ''}${closedPnl.toFixed(1)}%`);
    }
  }

  /**
   * 停止
   */
  stop() {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
    this.printFinalReport();
  }

  /**
   * 最终报告
   */
  printFinalReport() {
    const all = [...this.positions.values()];
    if (all.length === 0) return;

    console.log('\n' + '═'.repeat(60));
    console.log('📊 [Shadow Tracker] 最终报告');
    console.log('═'.repeat(60));

    for (const p of all) {
      const icon = (p.lastPnl || 0) > 0 ? '🟢' : '🔴';
      const exit = p.exitReason ? ` [${p.exitReason}]` : ' [OPEN]';
      console.log(`${icon} $${p.symbol.padEnd(12)} 评分:${String(p.score).padEnd(4)} PnL:${p.lastPnl !== null ? (p.lastPnl >= 0 ? '+' : '') + p.lastPnl.toFixed(1) + '%' : 'N/A'} 最高:+${p.highPnl.toFixed(1)}% 最低:${p.lowPnl.toFixed(1)}%${exit}`);
    }

    const withPnl = all.filter(p => p.lastPnl !== null);
    const winners = withPnl.filter(p => p.lastPnl > 0);
    const avgPnl = withPnl.length > 0 ? (withPnl.reduce((s, p) => s + p.lastPnl, 0) / withPnl.length).toFixed(1) : 0;
    console.log(`\n胜率: ${withPnl.length > 0 ? (winners.length / withPnl.length * 100).toFixed(0) : 0}% | 平均PnL: ${avgPnl}% | 总交易: ${withPnl.length}`);
    console.log('═'.repeat(60));
  }
}

export default ShadowPnlTracker;
