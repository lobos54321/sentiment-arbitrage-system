/**
 * 策略C回测脚本
 * 
 * 基于 latest-log-analysis.md 的 10 笔交易数据
 * 应用策略C（渐进式分批止盈）规则
 */

// 今天的交易数据（从 latest-log-analysis.md 提取）
const trades = [
  { symbol: '$4', buySol: 0.080, sellSol: 0.1027, realPnl: 28.4, peakPnl: 37, exitReason: 'PEAK_EXIT', holdMin: 2.9, subsequentATH: 308 },
  { symbol: '$CHINAGUY', buySol: 0.100, sellSol: 0.0862, realPnl: -14.0, peakPnl: 0, exitReason: 'MID_STOP', holdMin: 0.18, subsequentATH: null },
  { symbol: '$BONG', buySol: 0.080, sellSol: 0.0708, realPnl: -11.5, peakPnl: 0, exitReason: 'FAST_STOP', holdMin: 1.2, subsequentATH: 81 },
  { symbol: '$ANTS', buySol: 0.080, sellSol: 0.0708, realPnl: -11.5, peakPnl: 0, exitReason: 'MID_STOP', holdMin: 0.1, subsequentATH: 1341 },
  { symbol: '$BILL', buySol: 0.060, sellSol: 0.0512, realPnl: -14.7, peakPnl: 4, exitReason: 'MID_STOP', holdMin: 0.63, subsequentATH: 260 },
  { symbol: '$Jiang', buySol: 0.080, sellSol: 0.0611, realPnl: -23.6, peakPnl: 4, exitReason: 'STOP_LOSS', holdMin: 3.2, subsequentATH: null },
  { symbol: '$Cuzco', buySol: 0.080, sellSol: 0.1013, realPnl: 26.6, peakPnl: 31, exitReason: 'PEAK_EXIT', holdMin: 3.5, subsequentATH: 266 },
  { symbol: '$Belicoin', buySol: 0.060, sellSol: 0.0542, realPnl: -9.7, peakPnl: 7, exitReason: 'MID_STOP', holdMin: 0.67, subsequentATH: 302 },
  { symbol: '$PUMP', buySol: 0.080, sellSol: 0.0949, realPnl: 18.6, peakPnl: 33, exitReason: 'PEAK_EXIT', holdMin: 0.52, subsequentATH: 54 },
  { symbol: '$PARADISE', buySol: 0.100, sellSol: 0.1635, realPnl: 63.5, peakPnl: 85, exitReason: 'PEAK_EXIT', holdMin: 0.47, subsequentATH: 341 }
];

// 策略C参数
const STRATEGY_C = {
  STOP_LOSS: -20,
  FAST_STOP_THRESHOLD: -5,
  MID_STOP_THRESHOLD: -12,
  MID_STOP_HIGH_PNL: 10,
  TP1_TRIGGER: 50,
  TP1_SELL_PCT: 30,
  TP2_TRIGGER: 100,
  TP2_SELL_PCT: 20,
  TP3_TRIGGER: 200,
  TP3_SELL_PCT: 20,
  MOON_STOP_RETAIN: 0.55,
  MOON_STOP_MIN: 25,
  TRAIL_STOP_RETAIN_HIGH: 0.70,
  TRAIL_STOP_RETAIN_LOW: 0.65,
  TRAIL_STOP_THRESHOLD: 50,
  TRAIL_STOP_MIN: 10,
  TRADING_COST_PCT: 7
};

function applyStrategyC(trade) {
  const { symbol, buySol, peakPnl, realPnl, exitReason, holdMin } = trade;
  
  // 原始 PnL（不含交易损耗）
  const rawPeakPnl = peakPnl;
  const rawCurrentPnl = realPnl + STRATEGY_C.TRADING_COST_PCT;
  
  let result = {
    symbol,
    buySol,
    originalExit: exitReason,
    originalPnl: realPnl,
    strategyC: {}
  };
  
  // 1. 止损检查
  if (rawPeakPnl <= 0 && rawCurrentPnl <= STRATEGY_C.STOP_LOSS) {
    result.strategyC.exit = 'STOP_LOSS';
    result.strategyC.pnl = STRATEGY_C.STOP_LOSS - STRATEGY_C.TRADING_COST_PCT;
    result.strategyC.solReceived = buySol * (1 + result.strategyC.pnl / 100);
    return result;
  }
  
  // 2. FAST_STOP（前15秒，从未涨过）
  if (holdMin < 0.25 && rawPeakPnl <= 0 && rawCurrentPnl < STRATEGY_C.FAST_STOP_THRESHOLD) {
    result.strategyC.exit = 'FAST_STOP';
    result.strategyC.pnl = rawCurrentPnl - STRATEGY_C.TRADING_COST_PCT;
    result.strategyC.solReceived = buySol * (1 + result.strategyC.pnl / 100);
    return result;
  }
  
  // 3. MID_STOP（从未涨过10%，当前<-12%）
  if (rawCurrentPnl < STRATEGY_C.MID_STOP_THRESHOLD && rawPeakPnl < STRATEGY_C.MID_STOP_HIGH_PNL) {
    result.strategyC.exit = 'MID_STOP';
    result.strategyC.pnl = rawCurrentPnl - STRATEGY_C.TRADING_COST_PCT;
    result.strategyC.solReceived = buySol * (1 + result.strategyC.pnl / 100);
    return result;
  }
  
  // 4. 分批止盈逻辑
  let tp1 = false, tp2 = false, tp3 = false;
  let soldPct = 0, remainingPct = 100;
  let lockedPnl = 0;
  let totalSolReceived = 0;
  
  // 用含损耗的 PnL 计算锁定利润
  const pnlWithCost = rawPeakPnl - STRATEGY_C.TRADING_COST_PCT;
  
  if (rawPeakPnl >= STRATEGY_C.TP1_TRIGGER) {
    tp1 = true;
    soldPct = STRATEGY_C.TP1_SELL_PCT;
    remainingPct = 100 - soldPct;
    lockedPnl = pnlWithCost * (soldPct / 100);
    totalSolReceived += buySol * (1 + pnlWithCost / 100) * (soldPct / 100);
  }
  
  if (rawPeakPnl >= STRATEGY_C.TP2_TRIGGER) {
    tp2 = true;
    const additionalSell = STRATEGY_C.TP2_SELL_PCT;
    soldPct += additionalSell;
    remainingPct = 100 - soldPct;
    lockedPnl += pnlWithCost * (additionalSell / 100);
    totalSolReceived += buySol * (1 + pnlWithCost / 100) * (additionalSell / 100);
  }
  
  if (rawPeakPnl >= STRATEGY_C.TP3_TRIGGER) {
    tp3 = true;
    const additionalSell = STRATEGY_C.TP3_SELL_PCT;
    soldPct += additionalSell;
    remainingPct = 100 - soldPct;
    lockedPnl += pnlWithCost * (additionalSell / 100);
    totalSolReceived += buySol * (1 + pnlWithCost / 100) * (additionalSell / 100);
  }
  
  // 5. 剩余仓位退出
  if (tp1) {
    // MOON_STOP: 回撤到峰值55%
    const exitLine = Math.max(rawPeakPnl * STRATEGY_C.MOON_STOP_RETAIN, STRATEGY_C.MOON_STOP_MIN);
    const finalPnl = lockedPnl + (rawCurrentPnl - STRATEGY_C.TRADING_COST_PCT) * (remainingPct / 100);
    totalSolReceived += buySol * (1 + (rawCurrentPnl - STRATEGY_C.TRADING_COST_PCT) / 100) * (remainingPct / 100);
    
    result.strategyC.exit = `MOON_STOP(peak+${rawPeakPnl.toFixed(0)}%)`;
    result.strategyC.pnl = finalPnl;
    result.strategyC.solReceived = totalSolReceived;
    result.strategyC.tp1 = tp1;
    result.strategyC.tp2 = tp2;
    result.strategyC.tp3 = tp3;
    result.strategyC.soldPct = soldPct;
    result.strategyC.lockedPnl = lockedPnl;
  } else {
    // TRAIL_STOP: 未触发分批止盈
    const keepRatio = rawPeakPnl >= STRATEGY_C.TRAIL_STOP_THRESHOLD 
      ? STRATEGY_C.TRAIL_STOP_RETAIN_HIGH 
      : STRATEGY_C.TRAIL_STOP_RETAIN_LOW;
    const exitLine = Math.max(rawPeakPnl * keepRatio, STRATEGY_C.TRAIL_STOP_MIN);
    const finalPnl = Math.max(rawCurrentPnl - STRATEGY_C.TRADING_COST_PCT, STRATEGY_C.TRAIL_STOP_MIN - STRATEGY_C.TRADING_COST_PCT);
    
    result.strategyC.exit = `TRAIL_STOP(peak+${rawPeakPnl.toFixed(0)}%)`;
    result.strategyC.pnl = finalPnl;
    result.strategyC.solReceived = buySol * (1 + finalPnl / 100);
  }
  
  return result;
}

// 执行回测
console.log('📊 策略C回测 - 基于今天10笔交易\n');
console.log('═'.repeat(100));

let totalInvested = 0;
let totalReceivedOriginal = 0;
let totalReceivedStrategyC = 0;
let winnersOriginal = 0;
let winnersStrategyC = 0;

const results = trades.map(trade => {
  const result = applyStrategyC(trade);
  totalInvested += trade.buySol;
  totalReceivedOriginal += trade.sellSol;
  totalReceivedStrategyC += result.strategyC.solReceived;
  
  if (trade.realPnl > 0) winnersOriginal++;
  if (result.strategyC.pnl > 0) winnersStrategyC++;
  
  return result;
});

// 打印详细结果
results.forEach((r, i) => {
  const diff = r.strategyC.pnl - r.originalPnl;
  const diffIcon = diff > 0 ? '📈' : diff < 0 ? '📉' : '➡️';
  
  console.log(`${i + 1}. ${r.symbol.padEnd(12)} | 投入: ${r.buySol.toFixed(3)} SOL`);
  console.log(`   原策略: ${r.originalExit.padEnd(20)} PnL: ${r.originalPnl >= 0 ? '+' : ''}${r.originalPnl.toFixed(1)}%`);
  console.log(`   策略C:  ${r.strategyC.exit.padEnd(20)} PnL: ${r.strategyC.pnl >= 0 ? '+' : ''}${r.strategyC.pnl.toFixed(1)}% ${diffIcon} ${diff >= 0 ? '+' : ''}${diff.toFixed(1)}%`);
  if (r.strategyC.tp1) {
    console.log(`   分批: TP1=${r.strategyC.tp1} TP2=${r.strategyC.tp2} TP3=${r.strategyC.tp3} | 已卖${r.strategyC.soldPct}% 锁利${r.strategyC.lockedPnl.toFixed(1)}%`);
  }
  console.log('');
});

console.log('═'.repeat(100));
console.log('\n📈 汇总对比\n');

const originalTotalPnl = ((totalReceivedOriginal - totalInvested) / totalInvested * 100);
const strategyCTotalPnl = ((totalReceivedStrategyC - totalInvested) / totalInvested * 100);
const winRateOriginal = (winnersOriginal / trades.length * 100);
const winRateStrategyC = (winnersStrategyC / trades.length * 100);

console.log('| 指标 | 原策略E | 策略C | 差异 |');
console.log('|------|---------|-------|------|');
console.log(`| 投入 | ${totalInvested.toFixed(3)} SOL | ${totalInvested.toFixed(3)} SOL | - |`);
console.log(`| 收回 | ${totalReceivedOriginal.toFixed(3)} SOL | ${totalReceivedStrategyC.toFixed(3)} SOL | ${(totalReceivedStrategyC - totalReceivedOriginal >= 0 ? '+' : '')}${(totalReceivedStrategyC - totalReceivedOriginal).toFixed(3)} SOL |`);
console.log(`| 总PnL | ${originalTotalPnl >= 0 ? '+' : ''}${originalTotalPnl.toFixed(1)}% | ${strategyCTotalPnl >= 0 ? '+' : ''}${strategyCTotalPnl.toFixed(1)}% | ${(strategyCTotalPnl - originalTotalPnl >= 0 ? '+' : '')}${(strategyCTotalPnl - originalTotalPnl).toFixed(1)}% |`);
console.log(`| 胜率 | ${winRateOriginal.toFixed(0)}% (${winnersOriginal}W/${trades.length - winnersOriginal}L) | ${winRateStrategyC.toFixed(0)}% (${winnersStrategyC}W/${trades.length - winnersStrategyC}L) | ${(winRateStrategyC - winRateOriginal >= 0 ? '+' : '')}${(winRateStrategyC - winRateOriginal).toFixed(0)}% |`);

console.log('\n💡 关键发现：\n');

// 分析哪些交易受益于策略C
const improved = results.filter(r => r.strategyC.pnl > r.originalPnl);
const worsened = results.filter(r => r.strategyC.pnl < r.originalPnl);

if (improved.length > 0) {
  console.log(`✅ 策略C改善了 ${improved.length} 笔交易：`);
  improved.forEach(r => {
    const diff = r.strategyC.pnl - r.originalPnl;
    console.log(`   ${r.symbol}: ${r.originalPnl.toFixed(1)}% → ${r.strategyC.pnl.toFixed(1)}% (+${diff.toFixed(1)}%)`);
  });
  console.log('');
}

if (worsened.length > 0) {
  console.log(`❌ 策略C恶化了 ${worsened.length} 笔交易：`);
  worsened.forEach(r => {
    const diff = r.strategyC.pnl - r.originalPnl;
    console.log(`   ${r.symbol}: ${r.originalPnl.toFixed(1)}% → ${r.strategyC.pnl.toFixed(1)}% (${diff.toFixed(1)}%)`);
  });
}
