/**
 * 回测当前 PEAK_EXIT 策略
 *
 * 策略规则（策略E）：
 * - 涨幅 30-50%:  回撤 10% → 全卖（最低保 +20%）
 * - 涨幅 50-100%: 回撤 15% → 全卖（最低保 +30%）
 * - 涨幅 >100%:   回撤 20% → 全卖（最低保 +50%）
 * - 止损: -20%
 */

import Database from 'better-sqlite3';

const db = new Database('./data/sentiment_arb.db');

console.log('🔄 ========== PEAK_EXIT 策略回测 ==========\n');

// 获取所有虚拟盘交易数据
const allTrades = db.prepare(`
  SELECT
    id,
    symbol,
    high_pnl,
    exit_pnl,
    exit_reason,
    entry_mc,
    (closed_at - entry_time)/1000/60 as hold_minutes
  FROM shadow_pnl
  WHERE high_pnl IS NOT NULL
  ORDER BY id
`).all();

console.log(`📊 总交易数: ${allTrades.length}\n`);

// 模拟 PEAK_EXIT 策略
function simulatePeakExit(highPnl) {
  // 止损
  if (highPnl <= -20) {
    return { exitPnl: -20, reason: 'STOP_LOSS' };
  }

  // 未达到 30% 不触发 PEAK_EXIT
  if (highPnl < 30) {
    // 假设最终以当前 PnL 退出（实际可能更低）
    return { exitPnl: Math.max(highPnl * 0.5, -20), reason: 'TIMEOUT_OR_OTHER' };
  }

  // PEAK_EXIT 策略
  let retainRatio, minPnl;

  if (highPnl >= 100) {
    retainRatio = 0.80;  // 20% 回撤
    minPnl = 50;         // 最低保 +50%
  } else if (highPnl >= 50) {
    retainRatio = 0.85;  // 15% 回撤
    minPnl = 30;         // 最低保 +30%
  } else {
    retainRatio = 0.90;  // 10% 回撤
    minPnl = 20;         // 最低保 +20%
  }

  const exitLine = Math.max(highPnl * retainRatio, minPnl);

  return {
    exitPnl: exitLine,
    reason: `PEAK_EXIT(peak+${highPnl.toFixed(0)}%)`,
    retainRatio,
    minPnl
  };
}

// 回测所有交易
const results = allTrades.map(trade => {
  const simulated = simulatePeakExit(trade.high_pnl);

  return {
    symbol: trade.symbol,
    highPnl: trade.high_pnl,
    actualExitPnl: trade.exit_pnl,
    simulatedExitPnl: simulated.exitPnl,
    actualReason: trade.exit_reason,
    simulatedReason: simulated.reason,
    loss: trade.high_pnl - simulated.exitPnl,
    lossPct: ((trade.high_pnl - simulated.exitPnl) / trade.high_pnl * 100),
    entryMc: trade.entry_mc
  };
});

// 统计结果
const stats = {
  total: results.length,
  profitable: results.filter(r => r.simulatedExitPnl > 0).length,
  unprofitable: results.filter(r => r.simulatedExitPnl <= 0).length,
  avgHighPnl: results.reduce((sum, r) => sum + r.highPnl, 0) / results.length,
  avgSimulatedExit: results.reduce((sum, r) => sum + r.simulatedExitPnl, 0) / results.length,
  avgLoss: results.reduce((sum, r) => sum + r.loss, 0) / results.length,
  avgLossPct: results.reduce((sum, r) => sum + r.lossPct, 0) / results.length,
  totalPnl: results.reduce((sum, r) => sum + r.simulatedExitPnl, 0)
};

// 按峰值区间统计
const rangeStats = [
  { name: '0-30%', min: 0, max: 30 },
  { name: '30-50%', min: 30, max: 50 },
  { name: '50-100%', min: 50, max: 100 },
  { name: '100-200%', min: 100, max: 200 },
  { name: '>200%', min: 200, max: Infinity }
].map(range => {
  const trades = results.filter(r => r.highPnl >= range.min && r.highPnl < range.max);
  return {
    range: range.name,
    count: trades.length,
    avgHighPnl: trades.length > 0 ? trades.reduce((sum, r) => sum + r.highPnl, 0) / trades.length : 0,
    avgSimulatedExit: trades.length > 0 ? trades.reduce((sum, r) => sum + r.simulatedExitPnl, 0) / trades.length : 0,
    avgLoss: trades.length > 0 ? trades.reduce((sum, r) => sum + r.loss, 0) / trades.length : 0,
    avgLossPct: trades.length > 0 ? trades.reduce((sum, r) => sum + r.lossPct, 0) / trades.length : 0
  };
});

console.log('📈 整体统计:\n');
console.log(`总交易数: ${stats.total}`);
console.log(`盈利交易: ${stats.profitable} (${(stats.profitable/stats.total*100).toFixed(1)}%)`);
console.log(`亏损交易: ${stats.unprofitable} (${(stats.unprofitable/stats.total*100).toFixed(1)}%)`);
console.log(`平均峰值: +${stats.avgHighPnl.toFixed(2)}%`);
console.log(`平均退出: ${stats.avgSimulatedExit >= 0 ? '+' : ''}${stats.avgSimulatedExit.toFixed(2)}%`);
console.log(`平均损失: ${stats.avgLoss.toFixed(2)}% (${stats.avgLossPct.toFixed(1)}%)`);
console.log(`累计 PnL: ${stats.totalPnl >= 0 ? '+' : ''}${stats.totalPnl.toFixed(2)}%`);
console.log(`期望值: ${(stats.totalPnl / stats.total).toFixed(2)}%\n`);

console.log('📊 按峰值区间统计:\n');
rangeStats.forEach(s => {
  if (s.count > 0) {
    console.log(`${s.range}:`);
    console.log(`  交易数: ${s.count} (${(s.count/stats.total*100).toFixed(1)}%)`);
    console.log(`  平均峰值: +${s.avgHighPnl.toFixed(1)}%`);
    console.log(`  平均退出: ${s.avgSimulatedExit >= 0 ? '+' : ''}${s.avgSimulatedExit.toFixed(1)}%`);
    console.log(`  平均损失: ${s.avgLoss.toFixed(1)}% (${s.avgLossPct.toFixed(1)}%)`);
    console.log('');
  }
});

// 最差的交易（损失最大）
console.log('💔 损失最大的交易（Top 20）:\n');
const worstTrades = results
  .filter(r => r.highPnl >= 50)
  .sort((a, b) => b.loss - a.loss)
  .slice(0, 20);

worstTrades.forEach((t, i) => {
  console.log(`${i+1}. $${t.symbol}: 峰值 +${t.highPnl.toFixed(1)}% → 退出 +${t.simulatedExitPnl.toFixed(1)}% | 损失 ${t.loss.toFixed(1)}% (${t.lossPct.toFixed(1)}%) | MC: $${(t.entryMc/1000).toFixed(1)}K`);
});

// 对比 TAKE_PROFIT_50 策略
console.log('\n\n🆚 ========== 策略对比 ==========\n');

// TAKE_PROFIT_50 回测
const tp50Results = results.map(r => {
  let exitPnl;
  if (r.highPnl >= 50) {
    exitPnl = 50;  // 达到 50% 立即卖出
  } else if (r.highPnl <= -20) {
    exitPnl = -20;  // 止损
  } else {
    exitPnl = r.highPnl * 0.5;  // 未达到目标，假设以峰值的 50% 退出
  }
  return exitPnl;
});

const tp50Stats = {
  profitable: tp50Results.filter(r => r > 0).length,
  avgExit: tp50Results.reduce((sum, r) => sum + r, 0) / tp50Results.length,
  totalPnl: tp50Results.reduce((sum, r) => sum + r, 0),
  expectedValue: tp50Results.reduce((sum, r) => sum + r, 0) / tp50Results.length
};

// TAKE_PROFIT_30 回测
const tp30Results = results.map(r => {
  let exitPnl;
  if (r.highPnl >= 30) {
    exitPnl = 30;
  } else if (r.highPnl <= -15) {
    exitPnl = -15;
  } else {
    exitPnl = r.highPnl * 0.5;
  }
  return exitPnl;
});

const tp30Stats = {
  profitable: tp30Results.filter(r => r > 0).length,
  avgExit: tp30Results.reduce((sum, r) => sum + r, 0) / tp30Results.length,
  totalPnl: tp30Results.reduce((sum, r) => sum + r, 0),
  expectedValue: tp30Results.reduce((sum, r) => sum + r, 0) / tp30Results.length
};

console.log('策略 A: PEAK_EXIT (当前策略)');
console.log(`  胜率: ${(stats.profitable/stats.total*100).toFixed(1)}%`);
console.log(`  平均退出: ${stats.avgSimulatedExit >= 0 ? '+' : ''}${stats.avgSimulatedExit.toFixed(2)}%`);
console.log(`  期望值: ${(stats.totalPnl / stats.total).toFixed(2)}%`);
console.log(`  累计 PnL: ${stats.totalPnl >= 0 ? '+' : ''}${stats.totalPnl.toFixed(2)}%\n`);

console.log('策略 B: TAKE_PROFIT_50');
console.log(`  胜率: ${(tp50Stats.profitable/results.length*100).toFixed(1)}%`);
console.log(`  平均退出: ${tp50Stats.avgExit >= 0 ? '+' : ''}${tp50Stats.avgExit.toFixed(2)}%`);
console.log(`  期望值: ${tp50Stats.expectedValue.toFixed(2)}%`);
console.log(`  累计 PnL: ${tp50Stats.totalPnl >= 0 ? '+' : ''}${tp50Stats.totalPnl.toFixed(2)}%\n`);

console.log('策略 C: TAKE_PROFIT_30');
console.log(`  胜率: ${(tp30Stats.profitable/results.length*100).toFixed(1)}%`);
console.log(`  平均退出: ${tp30Stats.avgExit >= 0 ? '+' : ''}${tp30Stats.avgExit.toFixed(2)}%`);
console.log(`  期望值: ${tp30Stats.expectedValue.toFixed(2)}%`);
console.log(`  累计 PnL: ${tp30Stats.totalPnl >= 0 ? '+' : ''}${tp30Stats.totalPnl.toFixed(2)}%\n`);

// 性能对比
const comparison = [
  { name: 'PEAK_EXIT', value: stats.totalPnl / stats.total },
  { name: 'TAKE_PROFIT_50', value: tp50Stats.expectedValue },
  { name: 'TAKE_PROFIT_30', value: tp30Stats.expectedValue }
].sort((a, b) => b.value - a.value);

console.log('🏆 策略排名（按期望值）:\n');
comparison.forEach((c, i) => {
  const medal = i === 0 ? '🥇' : i === 1 ? '🥈' : '🥉';
  console.log(`${medal} ${i+1}. ${c.name}: ${c.value >= 0 ? '+' : ''}${c.value.toFixed(2)}%`);
});

console.log('\n✅ 回测完成！');

db.close();
