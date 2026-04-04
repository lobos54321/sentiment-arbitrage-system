/**
 * Legacy/stale path note:
 * Canonical paper active path is scripts/paper_trade_monitor.py -> scripts/execution_bridge.js
 * -> src/execution/paper-live-position-monitor.js.
 * Keep this file out of new fixes unless the canonical path is explicitly being migrated.
 */
import {
  hydrateMonitorState,
  advanceMonitorStateWithPrice,
  decideNotAthAction,
  applyPartialSellToState,
  applyExitToState,
  classifyLifecycleReason,
} from './notath-monitor-core.js';

function toNumber(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function normalizeTimestampMs(value, fallback = Date.now()) {
  const numeric = toNumber(value, fallback);
  if (numeric <= 0) return Math.trunc(fallback);
  return numeric > 1e12 ? Math.trunc(numeric) : Math.trunc(numeric * 1000);
}

function normalizeTimestampSec(value, fallback = Math.floor(Date.now() / 1000)) {
  const numeric = toNumber(value, fallback);
  if (numeric <= 0) return Math.trunc(fallback);
  return numeric > 1e12 ? Math.trunc(numeric / 1000) : Math.trunc(numeric);
}

function buildMonitorState(position = {}) {
  return hydrateMonitorState(position.monitorState || {}, {
    tokenCA: position.tokenCA,
    symbol: position.symbol,
    entryPrice: toNumber(position.entryPrice, 0),
    entryMC: toNumber(position.entryMC, 0),
    entrySol: toNumber(position.positionSizeSol ?? position.entrySol, 0),
    tokenAmount: Math.max(0, Math.trunc(toNumber(position.tokenAmountRaw ?? position.tokenAmount, 0))),
    tokenDecimals: Math.max(0, Math.trunc(toNumber(position.tokenDecimals, 0))),
    entryTime: normalizeTimestampMs(position.entryTs),
    exitStrategy: 'NOT_ATH',
  });
}

function buildSellOptions(position = {}, sellAmount = 0) {
  const tokenDecimals = Math.max(0, Math.trunc(toNumber(position.tokenDecimals, 0)));
  const divisor = tokenDecimals > 0 ? Math.pow(10, tokenDecimals) : 1;
  return {
    stage: position.strategyStage,
    strategyId: position.strategyId,
    lifecycleId: position.lifecycleId,
    inputAmount: sellAmount > 0 ? sellAmount / divisor : null,
  };
}

function finalizeUpdatedState(state, position = {}, quoteTsSec = 0) {
  const entryTsSec = normalizeTimestampSec(position.entryTs);
  const normalizedQuoteTsSec = normalizeTimestampSec(quoteTsSec, entryTsSec);
  return {
    ...state,
    barsHeld: Math.max(0, Math.floor(Math.max(0, normalizedQuoteTsSec - entryTsSec) / 60) + 1),
    lastMarkTs: normalizedQuoteTsSec,
  };
}

export async function evaluatePaperPositionExit({ position = {}, mark = {}, executor = null } = {}) {
  const state = buildMonitorState(position);
  const quoteTsMs = normalizeTimestampMs(mark.quoteTsSec ?? mark.timestamp ?? Date.now());
  const quoteTsSec = normalizeTimestampSec(quoteTsMs);

  const advanced = advanceMonitorStateWithPrice(state, {
    price: mark.currentPrice,
    mc: mark.currentMc,
    timestamp: quoteTsMs,
  });

  if (!advanced.valid) {
    return {
      ok: false,
      action: 'invalid',
      failureReason: advanced.reason || 'invalid_mark',
      updatedState: finalizeUpdatedState(state, position, quoteTsSec),
    };
  }

  const updatedState = finalizeUpdatedState(advanced.state, position, quoteTsSec);
  const triggerPnlPct = toNumber(advanced.pnl, 0);
  const triggerPnl = triggerPnlPct / 100;

  if (advanced.skip) {
    return {
      ok: true,
      action: 'hold',
      skip: true,
      skipReason: advanced.skipReason || 'first_price_wait',
      triggerPnl,
      triggerPnlPct,
      holdTimeMin: advanced.holdTimeMin,
      updatedState,
      shouldExit: false,
    };
  }

  const decision = decideNotAthAction(updatedState, {
    pnl: triggerPnlPct,
    holdTimeMin: advanced.holdTimeMin,
    now: quoteTsMs,
  });

  if (!decision || decision.type === 'hold') {
    return {
      ok: true,
      action: 'hold',
      triggerPnl,
      triggerPnlPct,
      holdTimeMin: advanced.holdTimeMin,
      updatedState,
      shouldExit: false,
    };
  }

  const currentTokenAmount = Math.max(0, Math.trunc(toNumber(updatedState.tokenAmount, 0)));
  const sellAmount = decision.type === 'partial_sell'
    ? Math.floor(currentTokenAmount * (toNumber(decision.sellPct, 0) / 100))
    : currentTokenAmount;

  if (!(sellAmount > 0)) {
    return {
      ok: true,
      action: 'hold',
      skip: true,
      skipReason: 'zero_sell_amount',
      triggerPnl,
      triggerPnlPct,
      holdTimeMin: advanced.holdTimeMin,
      updatedState,
      shouldExit: false,
    };
  }

  if (!executor) {
    return {
      ok: false,
      action: 'invalid',
      failureReason: 'missing_executor',
      updatedState,
      triggerPnl,
      triggerPnlPct,
    };
  }

  const execution = await executor.simulateSell(
    position.tokenCA,
    sellAmount,
    buildSellOptions(position, sellAmount)
  );

  if (!execution?.success) {
    return {
      ok: true,
      action: 'execution_failed',
      triggerPnl,
      triggerPnlPct,
      holdTimeMin: advanced.holdTimeMin,
      updatedState,
      shouldExit: false,
      decisionType: decision.type,
      actionReason: decision.reason,
      execution: execution || { success: false, failureReason: 'sell_execution_failed' },
    };
  }

  const solReceived = toNumber(execution.quotedOutAmount, 0);
  const executionResult = {
    ...execution,
    sellAmount,
    decisionType: decision.type,
    actionReason: decision.reason,
  };

  if (decision.type === 'partial_sell') {
    applyPartialSellToState(updatedState, {
      tpName: decision.tpName,
      sellPct: toNumber(decision.sellPct, 0),
      sellAmount,
      solReceived,
      currentPnl: triggerPnlPct,
    });
    return {
      ok: true,
      action: 'partial_sell',
      triggerPnl,
      triggerPnlPct,
      holdTimeMin: advanced.holdTimeMin,
      updatedState,
      shouldExit: false,
      tpName: decision.tpName,
      sellPct: toNumber(decision.sellPct, 0),
      actionReason: decision.reason,
      execution: executionResult,
    };
  }

  applyExitToState(updatedState, { solReceived });
  const realizedPnl = updatedState.entrySol > 0
    ? (updatedState.totalSolReceived - updatedState.entrySol) / updatedState.entrySol
    : triggerPnl;
  const lifecycleReason = classifyLifecycleReason(decision.reason, {
    tp1: updatedState.tp1,
    finalPnlPct: realizedPnl * 100,
  });

  return {
    ok: true,
    action: 'exit',
    shouldExit: true,
    triggerPnl,
    triggerPnlPct,
    holdTimeMin: advanced.holdTimeMin,
    updatedState,
    lifecycleReason,
    exitReason: lifecycleReason,
    actionReason: decision.reason,
    realizedPnl,
    execution: executionResult,
  };
}

export default evaluatePaperPositionExit;
