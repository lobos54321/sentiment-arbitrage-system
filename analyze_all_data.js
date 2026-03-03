/**
 * 完整数据分析脚本
 * 分析虚拟盘和实盘的所有交易数据
 */

import Database from 'better-sqlite3';
import fs from 'fs';
import axios from 'axios';

const db = new Database('./data/sentiment_arb.db');

console.log('📊 ========== 数据源分析 ==========\n');

// 1. 虚拟盘数据分析
console.log('🔍 虚拟盘 (Shadow Trading) 数据分析:\n');

const shadowStats = db.prepare(`
  SELECT
    COUNT(*) as total,
    COUNT(CASE WHEN high_pnl >= 50 THEN 1 END) as over_50,
    COUNT(CASE WHEN high_pnl >= 100 THEN 1 END) as over_100,
    COUNT(CASE WHEN high_pnl >= 200 THEN 1 END) as over_200,
    COUNT(CASE WHEN high_pnl >= 500 THEN 1 END) as over_500,
    COUNT(CASE WHEN high_pnl >= 1000 THEN 1 END) as over_1000,
    MAX(high_pnl) as max_pnl,
    AVG(high_pnl) as avg_peak,
    AVG(exit_pnl) as avg_exit,
    AVG(high_pnl - COALESCE(exit_pnl, 0)) as avg_loss
  FROM shadow_pnl
  WHERE high_pnl IS NOT NULL
`).get();

console.log('总交易数:', shadowStats.total);
console.log('峰值 >50%:', shadowStats.over_50, `(${(shadowStats.over_50/shadowStats.total*100).toFixed(1)}%)`);
console.log('峰值 >100%:', shadowStats.over_100, `(${(shadowStats.over_100/shadowStats.total*100).toFixed(1)}%)`);
console.log('峰值 >200%:', shadowStats.over_200, `(${(shadowStats.over_200/shadowStats.total*100).toFixed(1)}%)`);
console.log('峰值 >500%:', shadowStats.over_500, `(${(shadowStats.over_500/shadowStats.total*100).toFixed(1)}%)`);
console.log('峰值 >1000%:', shadowStats.over_1000, `(${(shadowStats.over_1000/shadowStats.total*100).toFixed(1)}%)`);
console.log('最高峰值:', shadowStats.max_pnl.toFixed(2) + '%');
console.log('平均峰值:', shadowStats.avg_peak.toFixed(2) + '%');
console.log('平均退出:', shadowStats.avg_exit.toFixed(2) + '%');
console.log('平均损失:', shadowStats.avg_loss.toFixed(2) + '%');

// 2. 超高收益交易详情
console.log('\n🚀 峰值 >500% 的交易:\n');
const highPeakTrades = db.prepare(`
  SELECT
    symbol,
    high_pnl,
    exit_pnl,
    entry_mc,
    exit_reason,
    datetime(entry_time/1000, 'unixepoch') as entry_time,
    datetime(closed_at/1000, 'unixepoch') as exit_time,
    (closed_at - entry_time)/1000/60 as hold_minutes
  FROM shadow_pnl
  WHERE high_pnl >= 500
  ORDER BY high_pnl DESC
`).all();

highPeakTrades.forEach(t => {
  console.log(`$${t.symbol}: 峰值 +${t.high_pnl.toFixed(1)}% → 退出 +${t.exit_pnl?.toFixed(1) || 'N/A'}% | MC: $${(t.entry_mc/1000).toFixed(1)}K | 持仓: ${t.hold_minutes?.toFixed(0) || 'N/A'}分钟 | ${t.exit_reason}`);
});

// 3. 200-500% 交易
console.log('\n📈 峰值 200-500% 的交易:\n');
const mediumPeakTrades = db.prepare(`
  SELECT
    symbol,
    high_pnl,
    exit_pnl,
    entry_mc,
    exit_reason,
    (closed_at - entry_time)/1000/60 as hold_minutes
  FROM shadow_pnl
  WHERE high_pnl >= 200 AND high_pnl < 500
  ORDER BY high_pnl DESC
`).all();

mediumPeakTrades.forEach(t => {
  console.log(`$${t.symbol}: 峰值 +${t.high_pnl.toFixed(1)}% → 退出 +${t.exit_pnl?.toFixed(1) || 'N/A'}% | MC: $${(t.entry_mc/1000).toFixed(1)}K | 持仓: ${t.hold_minutes?.toFixed(0) || 'N/A'}分钟 | ${t.exit_reason}`);
});

// 4. 100-200% 交易
console.log('\n💰 峰值 100-200% 的交易:\n');
const goodPeakTrades = db.prepare(`
  SELECT
    symbol,
    high_pnl,
    exit_pnl,
    entry_mc,
    exit_reason,
    (closed_at - entry_time)/1000/60 as hold_minutes
  FROM shadow_pnl
  WHERE high_pnl >= 100 AND high_pnl < 200
  ORDER BY high_pnl DESC
  LIMIT 20
`).all();

goodPeakTrades.forEach(t => {
  console.log(`$${t.symbol}: 峰值 +${t.high_pnl.toFixed(1)}% → 退出 +${t.exit_pnl?.toFixed(1) || 'N/A'}% | MC: $${(t.entry_mc/1000).toFixed(1)}K | 持仓: ${t.hold_minutes?.toFixed(0) || 'N/A'}分钟 | ${t.exit_reason}`);
});

// 5. 退出策略分析
console.log('\n📊 退出策略效果分析:\n');
const exitStrategyStats = db.prepare(`
  SELECT
    exit_reason,
    COUNT(*) as count,
    AVG(high_pnl) as avg_peak,
    AVG(exit_pnl) as avg_exit,
    AVG(high_pnl - exit_pnl) as avg_loss,
    AVG((high_pnl - exit_pnl) / high_pnl * 100) as loss_pct
  FROM shadow_pnl
  WHERE exit_pnl IS NOT NULL AND high_pnl > 0
  GROUP BY exit_reason
  ORDER BY count DESC
`).all();

exitStrategyStats.forEach(s => {
  console.log(`${s.exit_reason}:`);
  console.log(`  交易数: ${s.count}`);
  console.log(`  平均峰值: +${s.avg_peak.toFixed(1)}%`);
  console.log(`  平均退出: +${s.avg_exit.toFixed(1)}%`);
  console.log(`  平均损失: ${s.avg_loss.toFixed(1)}% (${s.loss_pct.toFixed(1)}%)`);
  console.log('');
});

// 6. MC 区间分析
console.log('\n💎 入场 MC 区间分析:\n');
const mcRangeStats = db.prepare(`
  SELECT
    CASE
      WHEN entry_mc < 5000 THEN '<5K'
      WHEN entry_mc < 10000 THEN '5-10K'
      WHEN entry_mc < 20000 THEN '10-20K'
      WHEN entry_mc < 50000 THEN '20-50K'
      ELSE '>50K'
    END as mc_range,
    COUNT(*) as count,
    AVG(high_pnl) as avg_peak,
    AVG(exit_pnl) as avg_exit,
    COUNT(CASE WHEN high_pnl >= 100 THEN 1 END) as over_100_count,
    COUNT(CASE WHEN high_pnl >= 200 THEN 1 END) as over_200_count
  FROM shadow_pnl
  WHERE entry_mc IS NOT NULL AND high_pnl IS NOT NULL
  GROUP BY mc_range
  ORDER BY
    CASE mc_range
      WHEN '<5K' THEN 1
      WHEN '5-10K' THEN 2
      WHEN '10-20K' THEN 3
      WHEN '20-50K' THEN 4
      ELSE 5
    END
`).all();

mcRangeStats.forEach(s => {
  console.log(`MC ${s.mc_range}:`);
  console.log(`  交易数: ${s.count}`);
  console.log(`  平均峰值: +${s.avg_peak.toFixed(1)}%`);
  console.log(`  平均退出: +${s.avg_exit.toFixed(1)}%`);
  console.log(`  >100%: ${s.over_100_count} (${(s.over_100_count/s.count*100).toFixed(1)}%)`);
  console.log(`  >200%: ${s.over_200_count} (${(s.over_200_count/s.count*100).toFixed(1)}%)`);
  console.log('');
});

// 7. 持仓时间分析
console.log('\n⏱️  持仓时间分析:\n');
const holdTimeStats = db.prepare(`
  SELECT
    CASE
      WHEN (closed_at - entry_time)/1000/60 < 5 THEN '<5分钟'
      WHEN (closed_at - entry_time)/1000/60 < 15 THEN '5-15分钟'
      WHEN (closed_at - entry_time)/1000/60 < 30 THEN '15-30分钟'
      WHEN (closed_at - entry_time)/1000/60 < 60 THEN '30-60分钟'
      WHEN (closed_at - entry_time)/1000/60 < 120 THEN '1-2小时'
      ELSE '>2小时'
    END as time_range,
    COUNT(*) as count,
    AVG(high_pnl) as avg_peak,
    AVG(exit_pnl) as avg_exit,
    COUNT(CASE WHEN high_pnl >= 100 THEN 1 END) as over_100_count
  FROM shadow_pnl
  WHERE closed_at IS NOT NULL AND entry_time IS NOT NULL AND high_pnl IS NOT NULL
  GROUP BY time_range
  ORDER BY
    CASE time_range
      WHEN '<5分钟' THEN 1
      WHEN '5-15分钟' THEN 2
      WHEN '15-30分钟' THEN 3
      WHEN '30-60分钟' THEN 4
      WHEN '1-2小时' THEN 5
      ELSE 6
    END
`).all();

holdTimeStats.forEach(s => {
  console.log(`持仓 ${s.time_range}:`);
  console.log(`  交易数: ${s.count}`);
  console.log(`  平均峰值: +${s.avg_peak.toFixed(1)}%`);
  console.log(`  平均退出: +${s.avg_exit.toFixed(1)}%`);
  console.log(`  >100%: ${s.over_100_count} (${(s.over_100_count/s.count*100).toFixed(1)}%)`);
  console.log('');
});

// 8. 最优策略推荐
console.log('\n🎯 ========== 策略优化建议 ==========\n');

// 分析最成功的交易特征
const successfulTrades = db.prepare(`
  SELECT
    symbol,
    high_pnl,
    exit_pnl,
    entry_mc,
    exit_reason,
    (closed_at - entry_time)/1000/60 as hold_minutes,
    (exit_pnl / high_pnl * 100) as capture_rate
  FROM shadow_pnl
  WHERE exit_pnl IS NOT NULL AND high_pnl >= 100
  ORDER BY exit_pnl DESC
  LIMIT 30
`).all();

console.log('💎 最成功的交易（退出 PnL 最高）:\n');
successfulTrades.forEach((t, i) => {
  console.log(`${i+1}. $${t.symbol}: 峰值 +${t.high_pnl.toFixed(1)}% → 退出 +${t.exit_pnl.toFixed(1)}% (捕获 ${t.capture_rate.toFixed(1)}%) | MC: $${(t.entry_mc/1000).toFixed(1)}K | 持仓: ${t.hold_minutes.toFixed(0)}分钟 | ${t.exit_reason}`);
});

// 计算最优捕获率
const captureRateStats = db.prepare(`
  SELECT
    AVG(exit_pnl / high_pnl * 100) as avg_capture_rate,
    MIN(exit_pnl / high_pnl * 100) as min_capture_rate,
    MAX(exit_pnl / high_pnl * 100) as max_capture_rate
  FROM shadow_pnl
  WHERE exit_pnl IS NOT NULL AND high_pnl > 0 AND exit_pnl > 0
`).get();

console.log('\n📊 峰值捕获率统计:');
console.log(`  平均捕获率: ${captureRateStats.avg_capture_rate.toFixed(1)}%`);
console.log(`  最低捕获率: ${captureRateStats.min_capture_rate.toFixed(1)}%`);
console.log(`  最高捕获率: ${captureRateStats.max_capture_rate.toFixed(1)}%`);

console.log('\n✅ 分析完成！');

db.close();
