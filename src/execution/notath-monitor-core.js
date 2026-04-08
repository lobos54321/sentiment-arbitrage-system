function toNumber(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function toNullableNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

export function hydrateMonitorState(rawState = {}, fallback = {}) {
  const entryTime = Math.max(
    0,
    Math.trunc(toNumber(rawState.entryTime ?? fallback.entryTime, Date.now()))
  );

  return {
    tokenCA: rawState.tokenCA || fallback.tokenCA || null,
    symbol: rawState.symbol || fallback.symbol || 'UNKNOWN',
    entryPrice: toNumber(rawState.entryPrice ?? fallback.entryPrice, 0),
    entryMC: toNumber(rawState.entryMC ?? fallback.entryMC, 0),
    entrySol: toNumber(rawState.entrySol ?? fallback.entrySol, 0),
    tokenAmount: Math.max(0, Math.trunc(toNumber(rawState.tokenAmount ?? fallback.tokenAmount, 0))),
    tokenDecimals: Math.max(0, Math.trunc(toNumber(rawState.tokenDecimals ?? fallback.tokenDecimals, 0))),
    highPnl: toNumber(rawState.highPnl ?? rawState.peakPnl, 0),
    lowPnl: toNumber(rawState.lowPnl, 0),
    lastPnl: toNumber(rawState.lastPnl, 0),
    totalSolReceived: Math.max(0, toNumber(rawState.totalSolReceived, 0)),
    entryTime,
    closed: Boolean(rawState.closed),
    exitReason: rawState.exitReason || null,
    conviction: rawState.conviction || fallback.conviction || 'NORMAL',
    exitStrategy: rawState.exitStrategy || fallback.exitStrategy || 'NOT_ATH',
    tp1: Boolean(rawState.tp1),
    tp1Time: toNullableNumber(rawState.tp1Time),
    breakeven: Boolean(rawState.breakeven ?? rawState.trailingActive),
    tp2: Boolean(rawState.tp2),
    tp3: Boolean(rawState.tp3),
    tp4: Boolean(rawState.tp4),
    soldPct: toNumber(rawState.soldPct, 0),
    lockedPnl: toNumber(rawState.lockedPnl, 0),
    moonbag: Boolean(rawState.moonbag),
    moonbagHighPnl: toNumber(rawState.moonbagHighPnl, 0),
    partialSellInProgress: Boolean(rawState.partialSellInProgress),
    exitInProgress: Boolean(rawState.exitInProgress),
    pendingSell: Boolean(rawState.pendingSell),
    pendingSellReason: rawState.pendingSellReason || null,
    retryLimitReached: Boolean(rawState.retryLimitReached),
    _updateCount: Math.max(0, Math.trunc(toNumber(rawState._updateCount, 0))),
    _suspiciousFirstPrice: Boolean(rawState._suspiciousFirstPrice),
  };
}

export function advanceMonitorStateWithPrice(rawState = {}, event = {}) {
  const state = hydrateMonitorState(rawState);
  const price = toNullableNumber(event.price);
  const mc = toNullableNumber(event.mc);
  const timestamp = Math.max(0, Math.trunc(toNumber(event.timestamp, Date.now())));

  let pnl = null;
  if (state.entryPrice > 0 && price != null && price > 0) {
    pnl = ((price - state.entryPrice) / state.entryPrice) * 100;
  } else if (state.entryMC > 0 && mc != null && mc > 0) {
    pnl = ((mc - state.entryMC) / state.entryMC) * 100;
  } else {
    return {
      valid: false,
      state,
      reason: 'no_pricing_basis',
      price,
      mc,
      timestamp,
    };
  }

  state.lastPnl = pnl;
  state._updateCount = Math.max(0, Math.trunc(toNumber(state._updateCount, 0))) + 1;

  const holdTimeMin = Math.max(0, (timestamp - state.entryTime) / 60000);

  if (state._updateCount === 1) {
    if (Math.abs(pnl) > 20) {
      state._suspiciousFirstPrice = true;
    }
    return {
      valid: true,
      skip: true,
      skipReason: 'first_price_wait',
      state,
      pnl,
      holdTimeMin,
      timestamp,
      price,
      mc,
      prevHigh: state.highPnl,
    };
  }

  if (state._suspiciousFirstPrice && state._updateCount === 2) {
    state._suspiciousFirstPrice = false;
  }

  const prevHigh = state.highPnl;
  if (pnl > state.highPnl) state.highPnl = pnl;
  if (pnl < state.lowPnl) state.lowPnl = pnl;

  return {
    valid: true,
    skip: false,
    state,
    pnl,
    holdTimeMin,
    timestamp,
    price,
    mc,
    prevHigh,
  };
}

export function decideNotAthAction(state, { pnl, holdTimeMin, now = Date.now() } = {}) {
  if (!state || typeof pnl !== 'number' || !Number.isFinite(pnl)) {
    return { type: 'hold', reason: 'invalid_pnl' };
  }

  if (!state.breakeven && state.highPnl >= 35) {
    state.breakeven = true;
  }
  const dynamicSL = state.breakeven ? 0 : -15;

  if (!state.tp1 && !state.breakeven && (state._updateCount || 0) <= 5 && state.highPnl <= 0 && pnl < -15) {
    return {
      type: 'exit',
      reason: `NOT_ATH_FAST_STOP(PnL${pnl.toFixed(0)}%)`,
      logLine: `⚡ [NOT_ATH:FAST_STOP] $${state.symbol} 入场即下跌 PnL:${pnl.toFixed(1)}% → 快速止损`,
    };
  }

  if (!state.tp1 && pnl <= dynamicSL) {
    const label = state.breakeven ? 'BREAKEVEN_SL' : 'HARD_SL_15';
    return {
      type: 'watch_reentry',
      reason: `NOT_ATH_${label}(PnL${pnl.toFixed(0)}%)`,
      logLine: `🛑 [NOT_ATH:${label}] $${state.symbol} PnL:${pnl.toFixed(1)}% ≤ ${dynamicSL}% → 切换至观察模式等待反弹`,
    };
  }

  if (!state.tp1 && pnl >= 80) {
    state.tp1Time = now;
    return {
      type: 'partial_sell',
      tpName: 'TP1',
      sellPct: 60,
      reason: `NOT_ATH_TP1(PnL+${pnl.toFixed(0)}%)`,
      logLine: `🎯 [NOT_ATH:TP1] $${state.symbol} PnL:+${pnl.toFixed(1)}% ≥ +80% → 卖出60%，SL→成本`,
    };
  }

  if (state.tp1 && !state.moonbag && pnl < 0) {
    return {
      type: 'exit',
      reason: `NOT_ATH_COST_SL(PnL${pnl.toFixed(0)}%)`,
      logLine: `🛡️ [NOT_ATH:TP1后成本SL] $${state.symbol} PnL:${pnl.toFixed(1)}% < 0% → 平剩余仓位`,
    };
  }

  if (state.tp1 && !state.tp2 && pnl >= 100) {
    return {
      type: 'partial_sell',
      tpName: 'TP2',
      sellPct: 50,
      reason: `NOT_ATH_TP2(PnL+${pnl.toFixed(0)}%)`,
      logLine: `🚀 [NOT_ATH:TP2] $${state.symbol} PnL:+${pnl.toFixed(1)}% ≥ +100% → 卖出50%`,
    };
  }

  if (state.tp2 && !state.tp3 && pnl >= 200) {
    return {
      type: 'partial_sell',
      tpName: 'TP3',
      sellPct: 50,
      reason: `NOT_ATH_TP3(PnL+${pnl.toFixed(0)}%)`,
      logLine: `🌙 [NOT_ATH:TP3] $${state.symbol} PnL:+${pnl.toFixed(1)}% ≥ +200% → 卖出50%`,
    };
  }

  if (state.tp3 && !state.tp4 && pnl >= 500) {
    return {
      type: 'partial_sell',
      tpName: 'TP4',
      sellPct: 80,
      reason: `NOT_ATH_TP4(PnL+${pnl.toFixed(0)}%)`,
      logLine: `🌕 [NOT_ATH:TP4] $${state.symbol} PnL:+${pnl.toFixed(1)}% ≥ +500% → 卖出80%，进入Moonbag`,
    };
  }

  if (state.moonbag) {
    if (pnl > (state.moonbagHighPnl || 0)) {
      state.moonbagHighPnl = pnl;
    }
    const moonPeak = state.moonbagHighPnl || pnl;
    const dropPct = moonPeak > 0 ? ((moonPeak - pnl) / moonPeak) * 100 : 0;

    // 动态回撤逻辑：利润越高，容忍度越低
    let dynamicDropThreshold = 15;
    if (moonPeak > 100) dynamicDropThreshold = 10;
    if (moonPeak > 300) dynamicDropThreshold = 7;

    if (dropPct >= dynamicDropThreshold) {
      return {
        type: 'exit',
        reason: `NOT_ATH_MOONBAG(peak+${moonPeak.toFixed(0)}%,PnL+${pnl.toFixed(0)}%)`,
        logLine: `🎫 [NOT_ATH:Moonbag] $${state.symbol} 从+${moonPeak.toFixed(0)}%回撤${dropPct.toFixed(0)}% (阈值:${dynamicDropThreshold}%) → 平仓`,
      };
    }
    return { type: 'hold', reason: 'moonbag_hold' };
  }

  if (state.tp1 && state.tp1Time && (now - state.tp1Time) >= 40 * 60 * 1000) {
    return {
      type: 'exit',
      reason: `NOT_ATH_TP1_TIMEOUT_40M(PnL${pnl.toFixed(0)}%)`,
      logLine: `⏰ [NOT_ATH:TP1超时40m] $${state.symbol} TP1后已持仓40分钟 → 平剩余`,
    };
  }

  if (!state.tp1 && holdTimeMin >= 10) {
    return {
      type: 'exit',
      reason: `NOT_ATH_DEAD_WATER_10M(PnL${pnl.toFixed(0)}%)`,
      logLine: `💧 [NOT_ATH:死水] $${state.symbol} 10分钟未触TP1 PnL:${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}% → 全平`,
    };
  }

  if (holdTimeMin >= 20) {
    return {
      type: 'exit',
      reason: `NOT_ATH_MAX_HOLD_20M(PnL${pnl.toFixed(0)}%)`,
      logLine: `⏰ [NOT_ATH:20分大限] $${state.symbol} 持仓${holdTimeMin.toFixed(0)}m ≥ 20m → 强制全平`,
    };
  }

  return { type: 'hold', reason: 'hold' };
}

export function applyPartialSellToState(state, { tpName, sellPct, sellAmount, solReceived, currentPnl }) {
  const remainingBeforeSell = 100 - (state.soldPct || 0);
  const actualSoldFraction = (sellPct / 100) * remainingBeforeSell;
  state.tokenAmount = Math.max(0, Math.trunc(state.tokenAmount - sellAmount));
  state.totalSolReceived = Math.max(0, toNumber(state.totalSolReceived, 0) + toNumber(solReceived, 0));
  state.soldPct = toNumber(state.soldPct, 0) + actualSoldFraction;
  state.lockedPnl = toNumber(state.lockedPnl, 0) + (toNumber(currentPnl, 0) * (actualSoldFraction / 100));
  state[tpName.toLowerCase()] = true;
  if (tpName === 'TP4') {
    state.moonbag = true;
    state.moonbagHighPnl = Math.max(toNumber(state.moonbagHighPnl, 0), toNumber(currentPnl, 0));
  }
  return state;
}

export function applyExitToState(state, { solReceived }) {
  state.totalSolReceived = Math.max(0, toNumber(state.totalSolReceived, 0) + toNumber(solReceived, 0));
  state.closed = true;
  return state;
}

export function classifyLifecycleReason(rawReason, { tp1 = false, finalPnlPct = 0 } = {}) {
  const reason = String(rawReason || '');
  if (reason.includes('FAST_STOP') || reason.includes('HARD_SL_20')) {
    return 'sl';
  }
  if (reason.includes('MOONBAG') || reason.includes('TP1_TIMEOUT') || reason.includes('COST_SL')) {
    return finalPnlPct > 0 || tp1 ? 'trail' : 'timeout';
  }
  if (reason.includes('DEAD_WATER') || reason.includes('MAX_HOLD')) {
    return 'timeout';
  }
  if (finalPnlPct > 0 || tp1) {
    return 'trail';
  }
  return 'timeout';
}
