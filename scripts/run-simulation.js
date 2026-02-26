/**
 * 模拟交易系统
 * 
 * 使用历史数据或实时数据模拟交易，验证策略效果
 * 
 * 运行方式：
 *   node scripts/run-simulation.js
 */

import Database from 'better-sqlite3';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

const dbPath = process.env.DB_PATH || './data/sentiment_arb.db';
const db = new Database(join(projectRoot, dbPath));

// 策略参数
const STRATEGY = {
  POSITION_SIZE_USD: 100, // 每笔 100u
  STOP_LOSS: -0.50, // -50% 止损
  BREAKEVEN_TRIGGER: 0.50, // +50% 保本
  BREAKEVEN_SELL_PERCENT: 0.60, // 卖 60%
};

// 模拟结果
const results = {
  total_trades: 0,
  wins: 0,
  losses: 0,
  breakeven_triggered: 0,
  stop_loss_triggered: 0,
  total_invested: 0,
  total_returned: 0,
  profit_position_outcomes: [],
  best_trade: null,
  worst_trade: null,
};

/**
 * 运行模拟
 */
async function runSimulation() {
  console.log('\n' + '═'.repeat(70));
  console.log('🎮 模拟交易系统 - 策略验证');
  console.log('═'.repeat(70));
  console.log('\n📋 策略参数:');
  console.log(`   每笔仓位: ${STRATEGY.POSITION_SIZE_USD}u`);
  console.log(`   止损: ${STRATEGY.STOP_LOSS * 100}%`);
  console.log(`   保本触发: +${STRATEGY.BREAKEVEN_TRIGGER * 100}%`);
  console.log(`   保本卖出: ${STRATEGY.BREAKEVEN_SELL_PERCENT * 100}%`);
  console.log('');

  // 获取所有已完成追踪的信号
  const trackedSignals = db.prepare(`
    SELECT 
      t.*,
      s.channel_name
    FROM shadow_price_tracking t
    LEFT JOIN telegram_signals s ON t.token_ca = s.token_ca AND t.chain = s.chain
    WHERE t.status = 'completed'
    ORDER BY t.created_at ASC
  `).all();

  if (trackedSignals.length === 0) {
    console.log('⚠️  没有已完成追踪的数据，请先运行 Shadow 模式收集数据');
    return;
  }

  console.log(`📊 找到 ${trackedSignals.length} 个已追踪信号\n`);
  console.log('─'.repeat(70));

  for (const signal of trackedSignals) {
    simulateTrade(signal);
  }

  // 输出结果
  printResults();
}

/**
 * 模拟单笔交易
 */
function simulateTrade(signal) {
  results.total_trades++;
  results.total_invested += STRATEGY.POSITION_SIZE_USD;

  const entryPrice = signal.entry_price || 0;
  const maxPnl = signal.max_pnl || 0;
  const pnl5m = signal.pnl_5m || 0;
  const pnl15m = signal.pnl_15m || 0;
  const minPnl = Math.min(pnl5m, pnl15m, 0);

  const symbol = signal.token_ca?.substring(0, 8) || 'Unknown';

  // 模拟逻辑
  let outcome = {
    symbol,
    chain: signal.chain,
    entry_price: entryPrice,
    max_pnl: maxPnl,
    min_pnl: minPnl,
    final_pnl: pnl15m,
    action: '',
    returned: 0,
    profit: 0,
    profit_position_pnl: null,
  };

  // 场景1: 先跌到止损
  if (minPnl <= STRATEGY.STOP_LOSS * 100) {
    outcome.action = 'STOP_LOSS';
    outcome.returned = STRATEGY.POSITION_SIZE_USD * (1 + STRATEGY.STOP_LOSS);
    outcome.profit = outcome.returned - STRATEGY.POSITION_SIZE_USD;
    results.stop_loss_triggered++;
    results.losses++;
  }
  // 场景2: 涨到保本线
  else if (maxPnl >= STRATEGY.BREAKEVEN_TRIGGER * 100) {
    results.breakeven_triggered++;

    // 保本卖出
    const breakevenValue = STRATEGY.POSITION_SIZE_USD * (1 + STRATEGY.BREAKEVEN_TRIGGER);
    const sellValue = breakevenValue * STRATEGY.BREAKEVEN_SELL_PERCENT;
    const remainingValue = breakevenValue * (1 - STRATEGY.BREAKEVEN_SELL_PERCENT);

    // 剩余利润仓的最终表现（假设持有到 15 分钟）
    // 利润仓的收益 = 从保本点到最终价格的变化
    const profitPositionReturn = remainingValue * (1 + (pnl15m - STRATEGY.BREAKEVEN_TRIGGER * 100) / 100);

    outcome.action = 'BREAKEVEN';
    outcome.returned = sellValue + Math.max(profitPositionReturn, 0); // 利润仓最差归零
    outcome.profit = outcome.returned - STRATEGY.POSITION_SIZE_USD;
    outcome.profit_position_pnl = ((profitPositionReturn / remainingValue) - 1) * 100;

    results.profit_position_outcomes.push(outcome.profit_position_pnl);

    if (outcome.profit >= 0) {
      results.wins++;
    } else {
      results.losses++;
    }
  }
  // 场景3: 既没止损也没保本
  else {
    // 按 15 分钟后的价格结算
    outcome.action = 'TIMEOUT';
    outcome.returned = STRATEGY.POSITION_SIZE_USD * (1 + pnl15m / 100);
    outcome.profit = outcome.returned - STRATEGY.POSITION_SIZE_USD;

    if (outcome.profit >= 0) {
      results.wins++;
    } else {
      results.losses++;
    }
  }

  results.total_returned += outcome.returned;

  // 记录最佳和最差
  if (!results.best_trade || outcome.profit > results.best_trade.profit) {
    results.best_trade = outcome;
  }
  if (!results.worst_trade || outcome.profit < results.worst_trade.profit) {
    results.worst_trade = outcome;
  }

  // 打印单笔结果
  const profitEmoji = outcome.profit >= 0 ? '✅' : '❌';
  const actionEmoji = outcome.action === 'BREAKEVEN' ? '💰' : outcome.action === 'STOP_LOSS' ? '🛑' : '⏰';
  console.log(`${profitEmoji} ${actionEmoji} [${signal.chain}] ${symbol} | Max: +${maxPnl.toFixed(0)}% | Final: ${pnl15m >= 0 ? '+' : ''}${pnl15m.toFixed(0)}% | ${outcome.action} | P/L: ${outcome.profit >= 0 ? '+' : ''}${outcome.profit.toFixed(1)}u`);
}

/**
 * 打印结果
 */
function printResults() {
  console.log('\n' + '═'.repeat(70));
  console.log('📊 模拟结果汇总');
  console.log('═'.repeat(70));

  const totalProfit = results.total_returned - results.total_invested;
  const roi = (totalProfit / results.total_invested) * 100;
  const winRate = (results.wins / results.total_trades) * 100;

  console.log(`
┌─────────────────────────────────────────────────────────────────────┐
│ 交易统计                                                            │
├─────────────────────────────────────────────────────────────────────┤
│ 总交易数: ${String(results.total_trades).padStart(10)}                                          │
│ 盈利次数: ${String(results.wins).padStart(10)}                                          │
│ 亏损次数: ${String(results.losses).padStart(10)}                                          │
│ 胜率:     ${(winRate.toFixed(1) + '%').padStart(10)}                                          │
├─────────────────────────────────────────────────────────────────────┤
│ 策略触发                                                            │
├─────────────────────────────────────────────────────────────────────┤
│ 保本触发: ${String(results.breakeven_triggered).padStart(10)}                                          │
│ 止损触发: ${String(results.stop_loss_triggered).padStart(10)}                                          │
├─────────────────────────────────────────────────────────────────────┤
│ 资金统计                                                            │
├─────────────────────────────────────────────────────────────────────┤
│ 总投入:   ${(results.total_invested.toFixed(0) + 'u').padStart(10)}                                          │
│ 总回收:   ${(results.total_returned.toFixed(0) + 'u').padStart(10)}                                          │
│ 净收益:   ${((totalProfit >= 0 ? '+' : '') + totalProfit.toFixed(0) + 'u').padStart(10)}                                          │
│ ROI:      ${((roi >= 0 ? '+' : '') + roi.toFixed(1) + '%').padStart(10)}                                          │
└─────────────────────────────────────────────────────────────────────┘
`);

  // 利润仓表现
  if (results.profit_position_outcomes.length > 0) {
    const avgProfitPnl = results.profit_position_outcomes.reduce((a, b) => a + b, 0) / results.profit_position_outcomes.length;
    const bestProfitPnl = Math.max(...results.profit_position_outcomes);
    const worstProfitPnl = Math.min(...results.profit_position_outcomes);

    console.log('📈 利润仓表现（保本后剩余 40%）:');
    console.log(`   平均收益: ${avgProfitPnl >= 0 ? '+' : ''}${avgProfitPnl.toFixed(1)}%`);
    console.log(`   最佳表现: +${bestProfitPnl.toFixed(1)}%`);
    console.log(`   最差表现: ${worstProfitPnl.toFixed(1)}%`);
    console.log('');
  }

  // 最佳/最差交易
  if (results.best_trade) {
    console.log(`🏆 最佳交易: [${results.best_trade.chain}] ${results.best_trade.symbol}`);
    console.log(`   Max PnL: +${results.best_trade.max_pnl.toFixed(0)}% | 收益: +${results.best_trade.profit.toFixed(1)}u`);
  }
  if (results.worst_trade) {
    console.log(`💀 最差交易: [${results.worst_trade.chain}] ${results.worst_trade.symbol}`);
    console.log(`   Min PnL: ${results.worst_trade.min_pnl.toFixed(0)}% | 亏损: ${results.worst_trade.profit.toFixed(1)}u`);
  }

  console.log('\n' + '═'.repeat(70));
}

// 运行模拟
runSimulation().catch(error => {
  console.error('❌ 模拟失败:', error.message);
  process.exit(1);
});
