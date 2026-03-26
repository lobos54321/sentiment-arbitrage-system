#!/usr/bin/env node
import Database from 'better-sqlite3';
import autonomyConfig from '../src/config/autonomy-config.js';
import { MarketDataBackfillService } from '../src/market-data/market-data-backfill-service.js';

const db = new Database(autonomyConfig.dbPath, { readonly: true });
const service = new MarketDataBackfillService(autonomyConfig);

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

function uniqByToken(rows = []) {
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const tokenCa = row?.token_ca;
    if (!tokenCa || seen.has(tokenCa)) continue;
    seen.add(tokenCa);
    out.push(row);
  }
  return out;
}

function safeQuery(sql, params = []) {
  try {
    return db.prepare(sql).all(...params);
  } catch {
    return [];
  }
}

function loadTrackedSignals() {
  const signalLimit = autonomyConfig.helius.trackedSignalLimit;
  const lookbackTs = nowSec() - (autonomyConfig.helius.trackedSignalLookbackHours * 3600);
  const premiumSignals = safeQuery(`
    SELECT token_ca, symbol, timestamp, hard_gate_status, ai_action
    FROM premium_signals
    WHERE token_ca IS NOT NULL AND timestamp >= ?
    ORDER BY timestamp DESC
    LIMIT ?
  `, [lookbackTs * 1000, signalLimit]);
  const openTrades = safeQuery(`
    SELECT token_ca, symbol, timestamp, status
    FROM trades
    WHERE token_ca IS NOT NULL AND status = 'OPEN'
    ORDER BY timestamp DESC
    LIMIT ?
  `, [autonomyConfig.helius.openTradeLimit]);
  const recentCursorRows = service.listRecentCursors(autonomyConfig.helius.incrementalMaxPoolsPerRun);
  const cursorSignals = recentCursorRows.map((row) => ({
    token_ca: row.token_ca,
    symbol: null,
    timestamp: (row.newest_block_time || row.last_backfill_at || nowSec()) * 1000,
    hard_gate_status: row.status || 'CURSOR',
    ai_action: 'TRACK'
  }));

  return uniqByToken([...openTrades, ...premiumSignals, ...cursorSignals]);
}

function deriveWindow(signal) {
  const signalTsSec = Math.floor(Number(signal.timestamp || Date.now()) / 1000);
  const incrementalWindow = autonomyConfig.helius.incrementalWindowMinutes * 60;
  const overlap = autonomyConfig.helius.incrementalOverlapMinutes * 60;
  return {
    signalTsSec,
    startTs: Math.max(0, signalTsSec - overlap),
    endTs: signalTsSec + incrementalWindow
  };
}

async function main() {
  if (!process.env.HELIUS_API_KEY) {
    throw new Error('HELIUS_API_KEY is required');
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
    results: []
  };

  for (const signal of trackedSignals) {
    const tokenCa = signal.token_ca;
    const { signalTsSec, startTs, endTs } = deriveWindow(signal);
    const result = await service.backfillWindow({
      tokenCa,
      signalTsSec,
      startTs,
      endTs,
      minBars: 3
    });

    summary.processed += 1;
    summary.totalTradesInserted += Number(result.tradesInserted || 0);
    summary.totalBarsWritten += Number(result.barsWritten || 0);
    if (result.provider === 'helius') {
      summary.heliusWins += 1;
    } else {
      summary.fallbackNeeded += 1;
    }

    summary.results.push({
      tokenCa,
      symbol: signal.symbol || null,
      hardGateStatus: signal.hard_gate_status || null,
      signalTsSec,
      startTs,
      endTs,
      provider: result.provider,
      poolAddress: result.poolAddress || null,
      bars: result.bars?.length || 0,
      signaturesFetched: result.signaturesFetched || 0,
      transactionsFetched: result.transactionsFetched || 0,
      tradesInserted: result.tradesInserted || 0,
      barsWritten: result.barsWritten || 0,
      error: result.error || null
    });
  }

  summary.repositoryStats = service.repository.getStats();
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
