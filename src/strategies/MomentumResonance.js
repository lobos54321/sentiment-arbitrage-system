/**
 * Momentum Resonance (狙击者策略)
 * 
 * 此模块完全脱离于现有业务主流程解耦，用于独立测试和运作。
 * 它包含了：
 * 1. 进场策略 (Entry Sniper Sandbox)：延迟进入，观察回撤形态
 * 2. 离场策略 (Exit Sniper Asymmetric)：非对称离场，多段止盈+极限回撤
 */

/**
 * ----------------------------------------------------
 * 第一部分：进场过滤与动能共振探测沙箱 (Sandbox Entry)
 * 可以挂载或替代 Signal Engine 的 `_executeNotAth`
 * ----------------------------------------------------
 */
async function executeSniperWatchlistEntry(ca, signal, systemContext) {
  const symbol = signal.symbol || ca.substring(0, 8);
  const maxSandboxMinutes = 3;

  // systemContext 提供挂载点引擎现有的工具函数，比如 _checkKline, saveSignalRecord, livePositionMonitor 等
  const { livePositionMonitor, shadowMode, shadowTracker, stats, _backfillPrebuyKlines, _waitForFreshLocalKlines, _checkKline, saveSignalRecord } = systemContext;

  // 0. ATH 历史强制拉黑机制
  const { signalHistory } = systemContext;
  const isATH = signal.is_ath === true;
  const sigHistory = signalHistory ? signalHistory.get(ca) : null;
  const prevAthCount = sigHistory ? (sigHistory.athCount || 0) : 0;

  if (isATH) {
    if (signalHistory) {
      signalHistory.set(ca, { ...sigHistory, athCount: prevAthCount + 1, lastSeen: Date.now() });
      if (systemContext._saveAthCounts) systemContext._saveAthCounts();
    }
    console.log(`⏭️ [SNIPER_WATCHLIST] $${symbol} 触发 ATH (硬黑名单) → 拦截`);
    return { action: 'SKIP', reason: 'block_ath_entirely' };
  }

  if (prevAthCount > 0) {
    console.log(`⏭️ [SNIPER_WATCHLIST] $${symbol} 曾经触发过 ATH (硬黑名单) →拦截`);
    return { action: 'SKIP', reason: 'block_ever_ath' };
  }

  // 1. 持仓检查
  if (livePositionMonitor?.positions?.has(ca)) {
    console.log(`⏭️ [SNIPER_WATCHLIST] $${symbol} 已持仓 → 跳过`);
    return { action: 'SKIP', reason: 'already_holding' };
  }
  if (shadowMode && shadowTracker?.hasOpenPosition(ca)) {
    console.log(`⏭️ [SNIPER_WATCHLIST] $${symbol} Shadow已有未平仓持仓，跳过`);
    return { action: 'SKIP', reason: 'already_in_position' };
  }

  // 2. Super Index >= 70 过滤（动能不足则跳过）
  const signalIndices = signal.indices || {};
  const superIndex = signalIndices?.super_index?.current
    || signalIndices?.super_index?.value
    || signalIndices?.super_index
    || 0;
  if (superIndex > 0 && superIndex < 70) {
    console.log(`⏭️ [SNIPER_WATCHLIST] $${symbol} Super=${superIndex} < 70 → 动能不足，跳过`);
    saveSignalRecord(signal, 'SUPER_INDEX_TOO_LOW', null, false);
    return { action: 'SKIP', reason: 'super_index_too_low' };
  }

  // 3. 硬过滤：判断 MC 和 Vol
  const mc = signal.market_cap || 0;
  const vol = signal.volume_24h || 0;
  if (mc > 0 && vol > 0 && (mc < 16000 || vol < 38000)) {
    console.log(`⏭️ [SNIPER_WATCHLIST] $${symbol} MC=$${(mc/1000).toFixed(1)}K Vol=$${(vol/1000).toFixed(1)}K 未达16K/38K门槛 → 跳过`);
    saveSignalRecord(signal, 'MC_VOL_FILTER_REJECTED', null, false);
    return { action: 'SKIP', reason: 'mc_vol_filter' };
  }

  console.log(`🧭 [SNIPER_WATCHLIST] $${symbol} 进入动能共振沙箱，将进行最多 ${maxSandboxMinutes} 分钟的滞后探测...`);

  let finalDecision = 'BLOCK';
  let finalReason = 'init';

  // 2. 核心异步循环 (3分钟观察探测)
  for (let attempt = 1; attempt <= maxSandboxMinutes; attempt++) {
    const signalTsSec = Math.floor(Date.now() / 1000); 

    try {
      const backfill = await _backfillPrebuyKlines(ca, signalTsSec, 5);
      if (backfill?.enough) {
        await _waitForFreshLocalKlines(ca, 4, 1200);
      }

      const klineCheck = await _checkKline(ca, { isATH: false, signalTsSec });
      const structuralFailures = new Set(['not_red_bar', 'support_break', 'high_vol', 'high_vol_calm', 'inactive']);
      const gateDecision = klineCheck?.gateStatus || (klineCheck?.passed ? 'PASS' : 'UNKNOWN_DATA');
      const blockedStructural = gateDecision === 'BLOCK' && structuralFailures.has(klineCheck?.reason);

      finalDecision = gateDecision;
      finalReason = klineCheck?.reason || 'unknown_data';

      // 当出现健康的缩量红K回撤形态，直接突破执行
      if (gateDecision === 'PASS') {
        console.log(`🎯 [SNIPER_WATCHLIST] $${symbol} 沙箱命中！符合缩量回调形态 (尝试 ${attempt}/${maxSandboxMinutes})`);
        if(stats) stats.not_ath_prebuy_kline_pass++;
        break; 
      }

      // 如果只是不满足 K 线形态且没有出现严重错误，倒数挂起 60 秒
      if (blockedStructural && attempt < maxSandboxMinutes) {
        console.log(`⏳ [SNIPER_WATCHLIST] $${symbol} 形态未满足 (${klineCheck?.reason})，继续等待 60s... (尝试 ${attempt}/${maxSandboxMinutes})`);
        await new Promise(resolve => setTimeout(resolve, 60000));
      } else {
        break; // 出错或次数耗尽直接终止探测
      }
    } catch (error) {
      console.warn(`⚠️ [SNIPER_WATCHLIST] $${symbol} 探测异常尝试 ${attempt}: ${error.message}`);
      finalDecision = 'ERROR';
      finalReason = error.message;
      break; 
    }
  }

  if (finalDecision !== 'PASS') {
    if(stats) stats.not_ath_prebuy_kline_block++;
    console.log(`🚫 [SNIPER_WATCHLIST] $${symbol} 沙箱超时或被拒绝 (${finalReason}) → 结束追踪`);
    saveSignalRecord(signal, `SNIPER_SANDBOX_REJECT_${finalDecision}`, null, false);
    return { action: 'SKIP', reason: finalReason };
  }

  // 3. 通关返回：指定强执行出路
  return { action: 'READY_TO_BUY', exitStrategy: 'SNIPER_RESONANCE', tradeConviction: 'HIGH' };
}

/**
 * ----------------------------------------------------
 * 第二部分：离场分流策略 (Sniper Exit Monitor)
 * 可以在 PositionMonitor._onPriceUpdate 的策略切分块调用
 * ----------------------------------------------------
 */
async function evaluateSniperExit(pos, pnl, holdTimeMin, systemContext) {
  // systemContext: 包含执行器触发器 _triggerExit / _triggerPartialSell 等
  const { _triggerExit, _triggerPartialSell } = systemContext;
  
  const remainingPct = 100 - (pos.soldPct || 0);

  // 1. 止损检查: Hard SL -10% (TP1触发前生效)
  if (!pos.tp1 && pnl <= -10) {
    console.log(`🛑 [SNIPER:硬SL] $${pos.symbol} PnL:${pnl.toFixed(1)}% ≤ -10% → 止损全卖`);
    await _triggerExit(pos, `HARD_SL_10(PnL${pnl.toFixed(0)}%)`, 100);
    return true; 
  }

  // 2. TP1: 利润摸到 +10% → 抛售 40%
  if (!pos.tp1 && pnl >= 10) {
    console.log(`🎯 [SNIPER:TP1] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ +10% → 落袋卖出 40%`);
    await _triggerPartialSell(pos, 'TP1', 40, pnl);
    return true;
  }

  // 3. TP2: 利润飙到 +20% → 抛售 30%
  if (pos.tp1 && !pos.tp2 && pnl >= 20) {
    console.log(`🚀 [SNIPER:TP2] $${pos.symbol} PnL:+${pnl.toFixed(1)}% ≥ +20% → 乘胜卖出 30%`);
    await _triggerPartialSell(pos, 'TP2', 30, pnl);
    
    // 借用现有属性激活追踪止损模式
    pos.moonbag = true; 
    pos.moonbagHighPnl = pnl;
    return true;
  }

  // 4. Trailing Stop 追踪机制 (回撤 5% 或跌破初始成本即平仓)
  if ((pos.tp2 || pos.moonbag) && remainingPct > 0) {
    if (pnl > (pos.moonbagHighPnl || 0)) {
      pos.moonbagHighPnl = pnl;
    }
    const peak = pos.moonbagHighPnl || pnl;
    const dropPct = peak > 0 ? ((peak - pnl) / peak) * 100 : 0;

    // 当利润从最高点回撤 >= 5% ，或整体跌回成本线及以下，撤离保留火种
    if (dropPct >= 5 || pnl <= 0) {
      console.log(`🎫 [SNIPER:追踪止损] $${pos.symbol} 峰值+${peak.toFixed(0)}%，大幅回撤${dropPct.toFixed(0)}% 或破成本线 → 清仓`);
      await _triggerExit(pos, `TRAILING_STOP(peak+${peak.toFixed(0)}%,drop${dropPct.toFixed(0)}%,PnL${pnl.toFixed(0)}%)`, remainingPct);
      return true;
    }
  }

  // 5. 死水与超时拦截
  if (!pos.tp1 && holdTimeMin >= 15 && pos.highPnl < 10) {
    console.log(`💀 [SNIPER:死水扫描] $${pos.symbol} 15分钟无突破 PnL:${pnl.toFixed(1)}% → 判断死水全平`);
    await _triggerExit(pos, `DEAD_WATER_15M(PnL${pnl.toFixed(0)}%)`, 100);
    return true;
  }
  
  if (holdTimeMin >= 30 && !pos.closed && !pos.exitInProgress) {
    console.log(`⏰ [SNIPER:超时兜底] $${pos.symbol} 超过 30 分钟未处理完毕，强制全平释放流动性`);
    await _triggerExit(pos, `TIMEOUT_30M(PnL${pnl.toFixed(0)}%)`, 100);
    return true;
  }

  return false; // 未触发任何交易操作
}

export {
  executeSniperWatchlistEntry,
  evaluateSniperExit
};
