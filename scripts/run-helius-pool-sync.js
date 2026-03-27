#!/usr/bin/env node
import Database from 'better-sqlite3';
import autonomyConfig from '../src/config/autonomy-config.js';
import { MarketDataBackfillService } from '../src/market-data/market-data-backfill-service.js';

const db = new Database(autonomyConfig.dbPath);
const service = new MarketDataBackfillService(autonomyConfig);

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

function uniqByToken(rows = []) {
  const byToken = new Map();
  for (const row of rows) {
    const tokenCa = row?.token_ca;
    if (!tokenCa) continue;
    const existing = byToken.get(tokenCa);
    if (!existing || Number(row.priorityScore || 0) > Number(existing.priorityScore || 0)) {
      byToken.set(tokenCa, row);
    }
  }
  return [...byToken.values()];
}

function safeQuery(sql, params = []) {
  try {
    return db.prepare(sql).all(...params);
  } catch {
    return [];
  }
}

function classifySignal(signal) {
  const hardGateStatus = String(signal.hard_gate_status || '').toUpperCase();
  const aiAction = String(signal.ai_action || signal.status || '').toUpperCase();
  const isOpenTrade = aiAction === 'OPEN' || String(signal.status || '').toUpperCase() === 'OPEN';
  const isPass = hardGateStatus === 'PASS';
  const isGreylist = hardGateStatus.startsWith('GREYLIST');
  return { isOpenTrade, isPass, isGreylist };
}

function scoreSignal(signal, recentCursorMap) {
  const tsSec = Math.floor(Number(signal.timestamp || Date.now()) / 1000);
  const ageMinutes = Math.max(0, (nowSec() - tsSec) / 60);
  const { isOpenTrade, isPass, isGreylist } = classifySignal(signal);
  let priorityScore = Math.max(0, 200 - ageMinutes);
  if (isOpenTrade) priorityScore += autonomyConfig.helius.stage3PriorityOpenTradeBonus;
  if (isPass) priorityScore += autonomyConfig.helius.stage3PriorityPassBonus;
  if (isGreylist) priorityScore += autonomyConfig.helius.stage3PriorityGreylistBonus;
  const cursor = recentCursorMap.get(signal.token_ca);
  if (cursor?.last_backfill_at && (nowSec() - Number(cursor.last_backfill_at)) < 30 * 60) {
    priorityScore -= autonomyConfig.helius.stage3PriorityRecentCursorPenalty;
  }
  return priorityScore;
}

function loadOpenTrades(limit = autonomyConfig.helius.openTradeLimit) {
  const params = [limit];

  try {
    return db.prepare(`
      SELECT token_ca, symbol, timestamp, status, pool_address
      FROM trades
      WHERE token_ca IS NOT NULL AND status = 'OPEN'
      ORDER BY timestamp DESC
      LIMIT ?
    `).all(...params);
  } catch (error) {
    if (!/no such column:\s*pool_address/i.test(String(error?.message || ''))) {
      return [];
    }
  }

  return safeQuery(`
    SELECT token_ca, symbol, timestamp, status
    FROM trades
    WHERE token_ca IS NOT NULL AND status = 'OPEN'
    ORDER BY timestamp DESC
    LIMIT ?
  `, params);
}

function loadOpenTradesMissingPool(limit = 1000000) {
  const params = [limit];

  try {
    return db.prepare(`
      SELECT token_ca, symbol, timestamp, status, pool_address
      FROM trades
      WHERE token_ca IS NOT NULL
        AND status = 'OPEN'
        AND (pool_address IS NULL OR TRIM(pool_address) = '')
      ORDER BY timestamp DESC
      LIMIT ?
    `).all(...params);
  } catch (error) {
    if (!/no such column:\s*pool_address/i.test(String(error?.message || ''))) {
      return [];
    }
  }

  return safeQuery(`
    SELECT token_ca, symbol, timestamp, status
    FROM trades
    WHERE token_ca IS NOT NULL AND status = 'OPEN'
    ORDER BY timestamp DESC
    LIMIT ?
  `, params);
}

function loadTrackedSignals() {
  const signalLimit = autonomyConfig.helius.trackedSignalLimit;
  const lookbackTs = nowSec() - (autonomyConfig.helius.trackedSignalLookbackHours * 3600);
  const recentCursorRows = service.listRecentCursors(autonomyConfig.helius.incrementalMaxPoolsPerRun * 2);
  const recentCursorMap = new Map(recentCursorRows.map((row) => [row.token_ca, row]));

  const premiumSignals = safeQuery(`
    SELECT token_ca, symbol, timestamp, hard_gate_status, ai_action
    FROM premium_signals
    WHERE token_ca IS NOT NULL AND timestamp >= ?
    ORDER BY timestamp DESC
    LIMIT ?
  `, [lookbackTs * 1000, signalLimit]).map((row) => ({
    ...row,
    source: 'premium_signal',
    priorityScore: 0
  }));

  const openTrades = loadOpenTrades().map((row) => ({
    ...row,
    hard_gate_status: 'OPEN_TRADE',
    ai_action: 'OPEN',
    source: 'open_trade',
    priorityScore: 0
  }));

  const cursorSignals = recentCursorRows.map((row) => ({
    token_ca: row.token_ca,
    symbol: null,
    timestamp: (row.newest_block_time || row.last_backfill_at || nowSec()) * 1000,
    hard_gate_status: row.status || 'CURSOR',
    ai_action: 'TRACK',
    source: 'cursor',
    priorityScore: 0,
    cursor: row
  }));

  const combined = [...openTrades, ...premiumSignals, ...cursorSignals].map((row) => ({
    ...row,
    priorityScore: scoreSignal(row, recentCursorMap)
  }));

  return uniqByToken(combined)
    .sort((a, b) => Number(b.priorityScore || 0) - Number(a.priorityScore || 0));
}

function lookupKnownTradePoolAddress(tokenCa) {
  if (!tokenCa) return null;

  try {
    const row = db.prepare(`
      SELECT pool_address
      FROM trades
      WHERE token_ca = ?
        AND pool_address IS NOT NULL
        AND TRIM(pool_address) != ''
      ORDER BY updated_at DESC, created_at DESC, timestamp DESC
      LIMIT 1
    `).get(tokenCa);
    return row?.pool_address || null;
  } catch (error) {
    if (/no such column:\s*pool_address/i.test(String(error?.message || ''))) {
      return null;
    }
    throw error;
  }
}

function persistOpenTradePoolAddress(tokenCa, poolAddress) {
  if (!tokenCa || !poolAddress) return 0;

  try {
    return db.prepare(`
      UPDATE trades
      SET pool_address = ?
      WHERE status = 'OPEN'
        AND token_ca = ?
        AND (pool_address IS NULL OR TRIM(pool_address) = '')
    `).run(poolAddress, tokenCa).changes;
  } catch (error) {
    if (/no such column:\s*pool_address/i.test(String(error?.message || ''))) {
      return 0;
    }
    throw error;
  }
}

function deriveWindow(signal) {
  const signalTsSec = Math.floor(Number(signal.timestamp || Date.now()) / 1000);
  const overlap = autonomyConfig.helius.incrementalOverlapMinutes * 60;
  const stage3Start = signalTsSec + (autonomyConfig.helius.stage3SignalWindowMinutes * 60);
  const stage3End = signalTsSec + (autonomyConfig.helius.stage3LongContinuationMinutes * 60);
  const continuationEnd = Math.max(signalTsSec + (autonomyConfig.helius.incrementalWindowMinutes * 60), stage3End);
  return {
    signalTsSec,
    startTs: Math.max(0, signalTsSec - overlap),
    endTs: continuationEnd,
    stage3Start,
    stage3End
  };
}

function createResolverSummary() {
  return {
    memory: 0,
    pool_mapping: 0,
    cursor: 0,
    helius_trades: 0,
    kline_1m: 0,
    geckoterminal: 0,
    dexscreener: 0,
    input: 0,
    unresolved: 0,
    other: 0
  };
}

function incrementResolverSource(summary, provider) {
  if (!provider) {
    summary.unresolved += 1;
    return;
  }

  if (Object.prototype.hasOwnProperty.call(summary, provider)) {
    summary[provider] += 1;
    return;
  }

  summary.other += 1;
}

function summarizeReadiness(results = []) {
  const processedOpenTrades = results.filter((item) => item.source === 'open_trade').length;
  const openTradeNoPool = results.filter((item) => item.source === 'open_trade' && item.error === 'no_pool').length;
  const processedCandidates = results.length;
  const unresolvedNoPool = results.filter((item) => item.error === 'no_pool').length;

  const openTradeNoPoolRate = processedOpenTrades ? openTradeNoPool / processedOpenTrades : 0;
  const unresolvedNoPoolRate = processedCandidates ? unresolvedNoPool / processedCandidates : 0;

  return {
    syncCoverage: {
      processedOpenTrades,
      openTradeNoPool,
      openTradeNoPoolRate,
      threshold: 0.05,
      ready: processedOpenTrades > 0 ? openTradeNoPoolRate <= 0.05 : false
    },
    candidateCoverage: {
      processedCandidates,
      unresolvedNoPool,
      unresolvedNoPoolRate,
      threshold: 0.1,
      ready: processedCandidates > 0 ? unresolvedNoPoolRate <= 0.1 : false
    },
    nextStep: 'Run `HELIUS_API_KEY=... node scripts/run-paper-eval-pipeline.js --covered-only` and confirm covered dataset ratio >= 0.80 plus baseline covered BUY count >= 30 before resuming the prior Stage 3 backtest.'
  };
}

async function runOpenTradePoolBackfill() {
  const openTrades = uniqByToken(loadOpenTradesMissingPool()).map((row) => ({
    ...row,
    hard_gate_status: 'OPEN_TRADE',
    ai_action: 'OPEN',
    source: 'open_trade'
  }));

  const summary = {
    startedAt: new Date().toISOString(),
    mode: 'open_trade_pool_backfill',
    totalOpenTradeTokens: openTrades.length,
    processed: 0,
    backfilledTrades: 0,
    resolvedPools: 0,
    unresolvedNoPool: 0,
    resolverSources: createResolverSummary(),
    results: []
  };

  for (const signal of openTrades) {
    const tokenCa = signal.token_ca;
    const { signalTsSec, startTs, endTs } = deriveWindow(signal);
    const knownPoolAddress = signal.pool_address || lookupKnownTradePoolAddress(tokenCa) || null;
    const result = await service.backfillWindow({
      tokenCa,
      signalTsSec,
      startTs,
      endTs,
      minBars: 1,
      poolAddress: knownPoolAddress
    });

    const poolAddressBackfilled = result.poolAddress
      ? persistOpenTradePoolAddress(tokenCa, result.poolAddress)
      : 0;

    summary.processed += 1;
    summary.backfilledTrades += Number(poolAddressBackfilled || 0);
    summary.resolvedPools += result.poolAddress ? 1 : 0;
    if (result.error === 'no_pool') {
      summary.unresolvedNoPool += 1;
    }
    incrementResolverSource(summary.resolverSources, result.poolProvider || null);

    summary.results.push({
      tokenCa,
      symbol: signal.symbol || null,
      provider: result.provider,
      poolProvider: result.poolProvider || null,
      poolAddress: result.poolAddress || null,
      poolAddressBackfilled,
      signaturesFetched: result.signaturesFetched || 0,
      transactionsFetched: result.transactionsFetched || 0,
      tradesInserted: result.tradesInserted || 0,
      barsWritten: result.barsWritten || 0,
      error: result.error || null
    });
  }

  summary.completedAt = new Date().toISOString();
  console.log(JSON.stringify(summary, null, 2));
}

async function main() {
  if (!process.env.HELIUS_API_KEY) {
    throw new Error('HELIUS_API_KEY is required');
  }

  if (process.argv.includes('--backfill-open-trade-pools')) {
    await runOpenTradePoolBackfill();
    return;
  }

  const trackedSignals = loadTrackedSignals().slice(0, autonomyConfig.helius.incrementalMaxPoolsPerRun);
  const summary = {
    startedAt: new Date().toISOString(),
    trackedCandidates: trackedSignals.length,
    processed: 0,
    heliusWins: 0,
    fallbackNeeded: 0,
    totalTradesInserted: 0,
    totalBarsWritten: 0,
    resolverSources: createResolverSummary(),
    results: []
  };

  for (const signal of trackedSignals) {
    const tokenCa = signal.token_ca;
    const { signalTsSec, startTs, endTs, stage3Start, stage3End } = deriveWindow(signal);
    const resolvedPoolAddress = signal.pool_address || lookupKnownTradePoolAddress(tokenCa) || null;
    const result = await service.backfillWindow({
      tokenCa,
      signalTsSec,
      startTs,
      endTs,
      minBars: 3,
      poolAddress: resolvedPoolAddress
    });

    const poolAddressBackfilled = signal.source === 'open_trade' && !signal.pool_address && result.poolAddress
      ? persistOpenTradePoolAddress(tokenCa, result.poolAddress)
      : 0;

    summary.processed += 1;
    summary.totalTradesInserted += Number(result.tradesInserted || 0);
    summary.totalBarsWritten += Number(result.barsWritten || 0);
    incrementResolverSource(summary.resolverSources, result.poolProvider || null);
    if (result.provider === 'helius') {
      summary.heliusWins += 1;
    } else {
      summary.fallbackNeeded += 1;
    }

    summary.results.push({
      tokenCa,
      symbol: signal.symbol || null,
      hardGateStatus: signal.hard_gate_status || null,
      source: signal.source || null,
      priorityScore: signal.priorityScore || 0,
      signalTsSec,
      startTs,
      endTs,
      stage3Start,
      stage3End,
      provider: result.provider,
      poolProvider: result.poolProvider || null,
      poolAddress: result.poolAddress || null,
      bars: result.bars?.length || 0,
      signaturesFetched: result.signaturesFetched || 0,
      transactionsFetched: result.transactionsFetched || 0,
      tradesInserted: result.tradesInserted || 0,
      barsWritten: result.barsWritten || 0,
      poolAddressBackfilled,
      error: result.error || null
    });
  }

  summary.repositoryStats = service.repository.getStats();
  summary.readiness = summarizeReadiness(summary.results);
  summary.completedAt = new Date().toISOString();
  console.log(JSON.stringify(summary, null, 2));
}

main()
  .catch((error) => {
    console.error(error.stack || error.message);
    process.exit(1);
  })
  .finally(() => {
    try { service.close(); } catch {}
    try { db.close(); } catch {}
  });
