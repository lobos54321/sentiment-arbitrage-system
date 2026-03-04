/**
 * 策略C完整回测 - 基于真实日志
 * 
 * 解析日志中的价格轨迹，逐tick模拟策略C的决策
 */

import { execSync } from 'child_process';
import fs from 'fs';

// 策略C参数
const STRATEGY_C = {
  STOP_LOSS: -20,
  FAST_STOP_CHECKS: 3,  // 前3次检查（约4.5秒）
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
  TRADING_COST_PCT: 7,
  GRACE_PERIOD_SEC: 60  // 新增：60秒 grace period
};

// 解析日志文件
function parseLogFile(logPath) {
  console.log(`\n📂 解析日志: ${logPath.split('/').pop()}`);
  
  try {
    const content = execSync(`gunzip -c "${logPath}"`).toString();
    const lines = content.split('\n');
    
    const trades = [];
    let currentTrade = null;
    
    for (const line of lines) {
      // 新持仓
      const newPosMatch = line.match(/新持仓: \$(\S+) \| ([\d.]+) SOL \| ([\d]+) tokens/);
      if (newPosMatch) {
        if (currentTrade && !currentTrade.exitReason) {
          // 上一笔未关闭，标记为未完成
          currentTrade.incomplete = true;
          trades.push(currentTrade);
        }
        
        currentTrade = {
          symbol: newPosMatch[1],
          buySol: parseFloat(newPosMatch[2]),
          tokenAmount: parseInt(newPosMatch[3]),
          priceUpdates: [],
          exitReason: null,
          exitPnl: null,
          sellSol: null
        };
        continue;
      }
      
      // 价格更新
      const priceMatch = line.match(/价格#(\d+)\] \$(\S+) 入:([\d.e-]+) 现:([\d.e-]+) PnL:([+-]?[\d.]+)% 峰:\+?([\d.]+)% 持:(\d+)s/);
      if (priceMatch && currentTrade) {
        const updateNum = parseInt(priceMatch[1]);
        const symbol = priceMatch[2];
        const entryPrice = parseFloat(priceMatch[3]);
        const currentPrice = parseFloat(priceMatch[4]);
        const pnl = parseFloat(priceMatch[5]);
        const highPnl = parseFloat(priceMatch[6]);
        const holdSec = parseInt(priceMatch[7]);
        
        if (symbol === currentTrade.symbol) {
          currentTrade.priceUpdates.push({
            updateNum,
            entryPrice,
            currentPrice,
            pnl,
            highPnl,
            holdSec
          });
        }
        continue;
      }
      
      // 退出
      const exitMatch = line.match(/\[EXIT\] \$(\S+) \| ([^|]+) \| PnL: ([+-]?[\d.]+)% \| 最高: \+?([\d.]+)%/);
      if (exitMatch && currentTrade) {
        const symbol = exitMatch[1];
        if (symbol === currentTrade.symbol) {
          currentTrade.exitReason = exitMatch[2].trim();
          currentTrade.exitPnl = parseFloat(exitMatch[3]);
          currentTrade.peakPnl = parseFloat(exitMatch[4]);
        }
        continue;
      }
      
      // 卖出成功
      const sellMatch = line.match(/卖出成功:.*实际到账: ([\d.]+) SOL/);
      if (sellMatch && currentTrade && currentTrade.exitReason && !currentTrade.sellSol) {
        currentTrade.sellSol = parseFloat(sellMatch[1]);
        trades.push(currentTrade);
        currentTrade = null;
      }
    }
    
    // 处理最后一笔未完成的交易
    if (currentTrade) {
      currentTrade.incomplete = true;
      trades.push(currentTrade);
    }
    
    console.log(`   ✓ 提取 ${trades.length} 笔交易`);
    return trades;
    
  } catch (error) {
    console.error(`   ✗ 解析失败: ${error.message}`);
    return [];
  }
}

// 策略C模拟
function simulateStrategyC(trade) {
  if (!trade.priceUpdates || trade.priceUpdates.length === 0) {
    return null;
  }
  
  let tp1 = false, tp2 = false, tp3 = false;
  let soldPct = 0, remainingPct = 100;
  let lockedPnl = 0;
  let totalSolReceived = 0;
  let highPnl = 0;
  let exitTick = null;
  let exitReason = null;
  
  for (let i = 0; i < trade.priceUpdates.length; i++) {
    const tick = trade.priceUpdates[i];
    const rawPnl = tick.pnl;
    const rawHighPnl = tick.highPnl;
    
    if (rawHighPnl > highPnl) highPnl = rawHighPnl;
    
    // 1. STOP_LOSS -20%（需要过 grace period）
    if (tick.holdSec >= STRATEGY_C.GRACE_PERIOD_SEC && rawPnl <= STRATEGY_C.STOP_LOSS) {
      exitTick = tick;
      exitReason = 'STOP_LOSS';
      break;
    }
    
    // 2. FAST_STOP（前3次检查，从未涨过，<-5%）
    if (i < STRATEGY_C.FAST_STOP_CHECKS && highPnl <= 0 && rawPnl < STRATEGY_C.FAST_STOP_THRESHOLD) {
      exitTick = tick;
      exitReason = 'FAST_STOP';
      break;
    }
    
    // 3. MID_STOP（过 grace period，<-12%，从未涨过10%）
    if (tick.holdSec >= STRATEGY_C.GRACE_PERIOD_SEC && rawPnl < STRATEGY_C.MID_STOP_THRESHOLD && highPnl < STRATEGY_C.MID_STOP_HIGH_PNL) {
      exitTick = tick;
      exitReason = 'MID_STOP';
      break;
    }
    
    // 4. 分批止盈触发
    const pnlWithCost = rawHighPnl - STRATEGY_C.TRADING_COST_PCT;
    
    if (!tp1 && rawHighPnl >= STRATEGY_C.TP1_TRIGGER) {
      tp1 = true;
      soldPct = STRATEGY_C.TP1_SELL_PCT;
      remainingPct = 100 - soldPct;
      lockedPnl = pnlWithCost * (soldPct / 100);
      totalSolReceived += trade.buySol * (1 + pnlWithCost / 100) * (soldPct / 100);
    }
    
    if (!tp2 && rawHighPnl >= STRATEGY_C.TP2_TRIGGER) {
      tp2 = true;
      const additionalSell = STRATEGY_C.TP2_SELL_PCT;
      soldPct += additionalSell;
      remainingPct = 100 - soldPct;
      lockedPnl += pnlWithCost * (additionalSell / 100);
      totalSolReceived += trade.buySol * (1 + pnlWithCost / 100) * (additionalSell / 100);
    }
    
    if (!tp3 && rawHighPnl >= STRATEGY_C.TP3_TRIGGER) {
      tp3 = true;
      const additionalSell = STRATEGY_C.TP3_SELL_PCT;
      soldPct += additionalSell;
      remainingPct = 100 - soldPct;
      lockedPnl += pnlWithCost * (additionalSell / 100);
      totalSolReceived += trade.buySol * (1 + pnlWithCost / 100) * (additionalSell / 100);
    }
    
    // 5. 移动止盈检查
    if (highPnl >= 15) {
      if (tp1) {
        // MOON_STOP: 回撤到峰值55%
        const exitLine = Math.max(highPnl * STRATEGY_C.MOON_STOP_RETAIN, STRATEGY_C.MOON_STOP_MIN);
        if (rawPnl < exitLine) {
          exitTick = tick;
          exitReason = `MOON_STOP(peak+${highPnl.toFixed(0)}%)`;
          break;
        }
      } else {
        // TRAIL_STOP
        const keepRatio = highPnl >= STRATEGY_C.TRAIL_STOP_THRESHOLD 
          ? STRATEGY_C.TRAIL_STOP_RETAIN_HIGH 
          : STRATEGY_C.TRAIL_STOP_RETAIN_LOW;
        const exitLine = Math.max(highPnl * keepRatio, STRATEGY_C.TRAIL_STOP_MIN);
        if (rawPnl < exitLine) {
          exitTick = tick;
          exitReason = `TRAIL_STOP(peak+${highPnl.toFixed(0)}%)`;
          break;
        }
      }
    }
  }
  
  // 如果没有触发退出，用最后一个tick
  if (!exitTick) {
    exitTick = trade.priceUpdates[trade.priceUpdates.length - 1];
    exitReason = 'NO_EXIT_TRIGGERED';
  }
  
  // 计算最终PnL
  const rawExitPnl = exitTick.pnl;
  let finalPnl;
  
  if (tp1) {
    finalPnl = lockedPnl + (rawExitPnl - STRATEGY_C.TRADING_COST_PCT) * (remainingPct / 100);
    totalSolReceived += trade.buySol * (1 + (rawExitPnl - STRATEGY_C.TRADING_COST_PCT) / 100) * (remainingPct / 100);
  } else {
    finalPnl = Math.max(rawExitPnl - STRATEGY_C.TRADING_COST_PCT, STRATEGY_C.TRAIL_STOP_MIN - STRATEGY_C.TRADING_COST_PCT);
    totalSolReceived = trade.buySol * (1 + finalPnl / 100);
  }
  
  return {
    exitReason,
    exitPnl: finalPnl,
    peakPnl: highPnl,
    exitTick: exitTick.updateNum,
    totalTicks: trade.priceUpdates.length,
    tp1, tp2, tp3,
    soldPct,
    lockedPnl,
    solReceived: totalSolReceived
  };
}

// 主函数
function main() {
  const logFiles = [
    '/Users/boliu/Downloads/runtime-log-20260304-000000-db8d895e-d49a-46c5-89ec-2287d4f49062.log.gz',
    '/Users/boliu/Downloads/runtime-log-20260304-000000-32f70f39-fff3-4c52-8ef2-1b69f16f95fe.log.gz',
    '/Users/boliu/Downloads/runtime-log-20260303-000000-5f2da453-ad89-4a2a-a6d8-002350500b7c.log.gz',
    '/Users/boliu/Downloads/runtime-log-20260303-000000-2e3c9ace-dfad-4a90-b594-5c3d3af0c389.log.gz'
  ];
  
  console.log('📊 策略C完整回测 - 基于真实日志价格轨迹\n');
  console.log('═'.repeat(100));
  
  let allTrades = [];
  for (const logFile of logFiles) {
    if (fs.existsSync(logFile)) {
      const trades = parseLogFile(logFile);
      allTrades = allTrades.concat(trades);
    }
  }
  
  console.log(`\n✓ 总共提取 ${allTrades.length} 笔交易\n`);
  console.log('═'.repeat(100));
  
  // 过滤掉不完整的交易
  const completeTrades = allTrades.filter(t => !t.incomplete && t.sellSol && t.priceUpdates.length > 0);
  console.log(`\n✓ 完整交易: ${completeTrades.length} 笔\n`);
  
  let totalInvestedOriginal = 0;
  let totalReceivedOriginal = 0;
  let totalReceivedStrategyC = 0;
  let winnersOriginal = 0;
  let winnersStrategyC = 0;
  
  const results = [];
  
  for (const trade of completeTrades) {
    const strategyC = simulateStrategyC(trade);
    if (!strategyC) continue;
    
    totalInvestedOriginal += trade.buySol;
    totalReceivedOriginal += trade.sellSol;
    totalReceivedStrategyC += strategyC.solReceived;
    
    if (trade.exitPnl > 0) winnersOriginal++;
    if (strategyC.exitPnl > 0) winnersStrategyC++;
    
    results.push({
      trade,
      strategyC
    });
  }
  
  // 打印详细结果
  console.log('\n📋 逐笔对比\n');
  results.forEach((r, i) => {
    const t = r.trade;
    const s = r.strategyC;
    const diff = s.exitPnl - t.exitPnl;
    const diffIcon = diff > 5 ? '📈' : diff < -5 ? '📉' : '➡️';
    
    console.log(`${(i + 1).toString().padStart(3)}. ${t.symbol.padEnd(12)} | 投入: ${t.buySol.toFixed(3)} SOL | Ticks: ${t.priceUpdates.length}`);
    console.log(`     原策略E: ${t.exitReason.padEnd(25)} PnL: ${t.exitPnl >= 0 ? '+' : ''}${t.exitPnl.toFixed(1)}% | 收回: ${t.sellSol.toFixed(4)} SOL`);
    console.log(`     策略C:   ${s.exitReason.padEnd(25)} PnL: ${s.exitPnl >= 0 ? '+' : ''}${s.exitPnl.toFixed(1)}% | 收回: ${s.solReceived.toFixed(4)} SOL ${diffIcon} ${diff >= 0 ? '+' : ''}${diff.toFixed(1)}%`);
    if (s.tp1) {
      console.log(`     分批: TP1=${s.tp1} TP2=${s.tp2} TP3=${s.tp3} | 已卖${s.soldPct}% 锁利${s.lockedPnl.toFixed(1)}% | 退出@tick${s.exitTick}/${s.totalTicks}`);
    }
    console.log('');
  });
  
  console.log('═'.repeat(100));
  console.log('\n📈 汇总对比\n');
  
  const originalTotalPnl = ((totalReceivedOriginal - totalInvestedOriginal) / totalInvestedOriginal * 100);
  const strategyCTotalPnl = ((totalReceivedStrategyC - totalInvestedOriginal) / totalInvestedOriginal * 100);
  const winRateOriginal = (winnersOriginal / results.length * 100);
  const winRateStrategyC = (winnersStrategyC / results.length * 100);
  
  console.log('| 指标 | 原策略E | 策略C | 差异 |');
  console.log('|------|---------|-------|------|');
  console.log(`| 交易数 | ${results.length} | ${results.length} | - |`);
  console.log(`| 投入 | ${totalInvestedOriginal.toFixed(3)} SOL | ${totalInvestedOriginal.toFixed(3)} SOL | - |`);
  console.log(`| 收回 | ${totalReceivedOriginal.toFixed(3)} SOL | ${totalReceivedStrategyC.toFixed(3)} SOL | ${(totalReceivedStrategyC - totalReceivedOriginal >= 0 ? '+' : '')}${(totalReceivedStrategyC - totalReceivedOriginal).toFixed(3)} SOL |`);
  console.log(`| 总PnL | ${originalTotalPnl >= 0 ? '+' : ''}${originalTotalPnl.toFixed(1)}% | ${strategyCTotalPnl >= 0 ? '+' : ''}${strategyCTotalPnl.toFixed(1)}% | ${(strategyCTotalPnl - originalTotalPnl >= 0 ? '+' : '')}${(strategyCTotalPnl - originalTotalPnl).toFixed(1)}% |`);
  console.log(`| 胜率 | ${winRateOriginal.toFixed(0)}% (${winnersOriginal}W/${results.length - winnersOriginal}L) | ${winRateStrategyC.toFixed(0)}% (${winnersStrategyC}W/${results.length - winnersStrategyC}L) | ${(winRateStrategyC - winRateOriginal >= 0 ? '+' : '')}${(winRateStrategyC - winRateOriginal).toFixed(0)}% |`);
  
  // 分析改善和恶化的交易
  const improved = results.filter(r => r.strategyC.exitPnl - r.trade.exitPnl > 1);
  const worsened = results.filter(r => r.strategyC.exitPnl - r.trade.exitPnl < -1);
  
  console.log('\n💡 关键发现：\n');
  
  if (improved.length > 0) {
    console.log(`✅ 策略C改善了 ${improved.length} 笔交易 (${(improved.length / results.length * 100).toFixed(0)}%)：`);
    improved.slice(0, 10).forEach(r => {
      const diff = r.strategyC.exitPnl - r.trade.exitPnl;
      console.log(`   ${r.trade.symbol}: ${r.trade.exitPnl.toFixed(1)}% → ${r.strategyC.exitPnl.toFixed(1)}% (+${diff.toFixed(1)}%) [${r.trade.exitReason} → ${r.strategyC.exitReason}]`);
    });
    if (improved.length > 10) {
      console.log(`   ... 还有 ${improved.length - 10} 笔`);
    }
    console.log('');
  }
  
  if (worsened.length > 0) {
    console.log(`❌ 策略C恶化了 ${worsened.length} 笔交易 (${(worsened.length / results.length * 100).toFixed(0)}%)：`);
    worsened.slice(0, 10).forEach(r => {
      const diff = r.strategyC.exitPnl - r.trade.exitPnl;
      console.log(`   ${r.trade.symbol}: ${r.trade.exitPnl.toFixed(1)}% → ${r.strategyC.exitPnl.toFixed(1)}% (${diff.toFixed(1)}%) [${r.trade.exitReason} → ${r.strategyC.exitReason}]`);
    });
    if (worsened.length > 10) {
      console.log(`   ... 还有 ${worsened.length - 10} 笔`);
    }
  }
}

main();
