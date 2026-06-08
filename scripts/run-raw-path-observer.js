#!/usr/bin/env node
import { dirname, isAbsolute, join } from 'path';
import { fileURLToPath } from 'url';

import Database from 'better-sqlite3';

import autonomyConfig from '../src/config/autonomy-config.js';
import {
  aggregateSwapsToRawPriceBars,
  ensureRawPathObserverSchema,
  normalizeBondingCurveTransactions,
  normalizeRawPathBar,
} from '../src/analytics/raw-path-observer.js';
import { MarketDataBackfillService } from '../src/market-data/market-data-backfill-service.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

function envInt(name, defaultValue, minValue, maxValue) {
  const raw = Number.parseInt(String(process.env[name] ?? defaultValue), 10);
  const value = Number.isFinite(raw) ? raw : defaultValue;
  return Math.max(minValue, Math.min(maxValue, value));
}

function resolvePath(raw) {
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function openSqlite(dbPath, options) {
  const db = options === undefined ? new Database(dbPath) : new Database(dbPath, options);
  try { db.pragma('mmap_size = 0'); } catch {}
  return db;
}

function getTableColumns(database, tableName) {
  return new Set(database.prepare(`PRAGMA table_info(${tableName})`).all().map((row) => row.name));
}

function tableExists(database, tableName) {
  return database.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(tableName) != null;
}

function normalizeSignalTs(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

function loadPremiumSignals(db, { lookbackHours, limit }) {
  const tables = new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
  if (!tables.has('premium_signals')) return [];
  const cols = getTableColumns(db, 'premium_signals');
  if (!cols.has('token_ca')) return [];
  const timestampExpr = cols.has('timestamp')
    ? "CASE WHEN timestamp > 1000000000000 THEN CAST(timestamp / 1000 AS INTEGER) ELSE CAST(timestamp AS INTEGER) END"
    : '0';
  const sinceTs = nowSec() - lookbackHours * 3600;
  return db.prepare(`
    SELECT
      ${cols.has('id') ? 'id' : 'NULL AS id'},
      token_ca,
      ${cols.has('symbol') ? 'symbol' : 'NULL AS symbol'},
      ${cols.has('timestamp') ? 'timestamp' : 'NULL AS timestamp'},
      ${timestampExpr} AS timestamp_sec,
      ${cols.has('hard_gate_status') ? 'hard_gate_status' : 'NULL AS hard_gate_status'},
      ${cols.has('ai_action') ? 'ai_action' : 'NULL AS ai_action'}
    FROM premium_signals
    WHERE token_ca IS NOT NULL
      AND ${timestampExpr} >= @since
    ORDER BY ${timestampExpr} DESC, ${cols.has('id') ? 'id' : timestampExpr} DESC
    LIMIT @limit
  `).all({ since: sinceTs, limit });
}

function selectUniqueSignals(signals, limit) {
  const byToken = new Map();
  for (const row of signals || []) {
    const tokenCa = String(row.token_ca || '').trim();
    const signalTs = normalizeSignalTs(row.timestamp_sec ?? row.timestamp);
    if (!tokenCa || signalTs == null) continue;
    const existing = byToken.get(tokenCa);
    if (!existing || signalTs > normalizeSignalTs(existing.timestamp_sec ?? existing.timestamp)) {
      byToken.set(tokenCa, { ...row, signal_ts_sec: signalTs });
    }
  }
  return [...byToken.values()].slice(0, limit);
}

function countBarsInSqlite(database, tableName, tokenCa, startTs, endTs) {
  if (!database || !tokenCa || !Number.isFinite(startTs) || !Number.isFinite(endTs)) {
    return { count: 0, baseline_count: 0, available: false };
  }
  if (!tableExists(database, tableName)) {
    return { count: 0, baseline_count: 0, available: false };
  }
  const cols = getTableColumns(database, tableName);
  if (!cols.has('token_ca')) {
    return { count: 0, baseline_count: 0, available: false };
  }
  const timestampColumn = ['timestamp', 'timestamp_sec', 'ts', 'sample_ts'].find((name) => cols.has(name));
  if (!timestampColumn) {
    return { count: 0, baseline_count: 0, available: false };
  }
  const baselineEndTs = startTs + 300;
  const row = database.prepare(`
    SELECT
      COUNT(*) AS count,
      SUM(CASE WHEN ${timestampColumn} >= @startTs AND ${timestampColumn} <= @baselineEndTs THEN 1 ELSE 0 END) AS baseline_count
    FROM ${tableName}
    WHERE token_ca = @tokenCa
      AND ${timestampColumn} >= @startTs
      AND ${timestampColumn} <= @endTs
  `).get({ tokenCa, startTs, endTs, baselineEndTs });
  return {
    count: Number(row?.count || 0),
    baseline_count: Number(row?.baseline_count || 0),
    available: true,
  };
}

function countRepositoryBars(service, tokenCa, startTs, endTs) {
  try {
    const rows = service?.getBars?.(tokenCa, startTs, endTs) || [];
    return {
      count: rows.length,
      baseline_count: rows.filter((row) => Number(row.timestamp) >= startTs && Number(row.timestamp) <= startTs + 300).length,
      available: true,
    };
  } catch {
    return { count: 0, baseline_count: 0, available: false };
  }
}

function rankSignalsForBackfill(signals, { signalDb, rawDb, service, now, horizonSec }) {
  return (signals || []).map((signal) => {
    const signalTsSec = Number(signal.signal_ts_sec ?? normalizeSignalTs(signal.timestamp_sec ?? signal.timestamp));
    const endTs = Math.min(now, signalTsSec + horizonSec);
    const tokenCa = String(signal.token_ca || '').trim();
    const raw = countBarsInSqlite(rawDb, 'raw_price_bars_1m', tokenCa, signalTsSec, endTs);
    const legacy = countBarsInSqlite(signalDb, 'kline_1m', tokenCa, signalTsSec, endTs);
    const cache = countRepositoryBars(service, tokenCa, signalTsSec, endTs);
    const pathCount = raw.count + legacy.count + cache.count;
    const baselineCount = raw.baseline_count + legacy.baseline_count + cache.baseline_count;
    const matured = Number.isFinite(signalTsSec) && now >= signalTsSec + horizonSec;
    let priority = 90;
    let priorityReason = 'already_path_covered';
    if (!pathCount) {
      priority = matured ? 0 : 2;
      priorityReason = 'no_raw_legacy_or_cache_path';
    } else if (!baselineCount) {
      priority = matured ? 1 : 3;
      priorityReason = 'path_exists_but_baseline_missing';
    } else if (!raw.count && !cache.count) {
      priority = matured ? 4 : 6;
      priorityReason = 'legacy_signal_kline_only';
    } else if (!raw.count) {
      priority = matured ? 5 : 7;
      priorityReason = 'cache_or_legacy_only_no_raw_path';
    } else if (raw.count < 10) {
      priority = matured ? 8 : 10;
      priorityReason = 'raw_path_sparse';
    }
    return {
      ...signal,
      signal_ts_sec: signalTsSec,
      raw_path_selection_priority: priority,
      raw_path_selection_reason: priorityReason,
      raw_path_existing_bars: raw.count,
      legacy_kline_existing_bars: legacy.count,
      cache_existing_bars: cache.count,
      existing_path_bars: pathCount,
      existing_baseline_bars: baselineCount,
      matured_for_raw_path: matured,
    };
  }).sort((a, b) => (
    Number(a.raw_path_selection_priority) - Number(b.raw_path_selection_priority)
    || (b.matured_for_raw_path ? 1 : 0) - (a.matured_for_raw_path ? 1 : 0)
    || Number(a.signal_ts_sec || 0) - Number(b.signal_ts_sec || 0)
  ));
}

function rawDbPath() {
  return resolvePath(process.env.RAW_SIGNAL_OUTCOMES_DB || './data/raw_signal_outcomes.db');
}

function upsertRawPriceBars(db, bars) {
  if (!bars?.length) return 0;
  ensureRawPathObserverSchema(db);
  const stmt = db.prepare(`
    INSERT INTO raw_price_bars_1m (
      token_ca, pool_address, timestamp, open, high, low, close, volume,
      provider, source_kind, source_family, price_unit,
      trade_count, first_trade_ts, last_trade_ts, fetched_at, payload_json, updated_at
    ) VALUES (
      @token_ca, @pool_address, @timestamp, @open, @high, @low, @close, @volume,
      @provider, @source_kind, @source_family, @price_unit,
      @trade_count, @first_trade_ts, @last_trade_ts, @fetched_at, @payload_json, @updated_at
    )
    ON CONFLICT(token_ca, pool_address, timestamp, provider, source_kind, price_unit) DO UPDATE SET
      open = excluded.open,
      high = excluded.high,
      low = excluded.low,
      close = excluded.close,
      volume = excluded.volume,
      source_family = excluded.source_family,
      trade_count = excluded.trade_count,
      first_trade_ts = excluded.first_trade_ts,
      last_trade_ts = excluded.last_trade_ts,
      fetched_at = excluded.fetched_at,
      payload_json = excluded.payload_json,
      updated_at = excluded.updated_at
  `);
  const ts = nowSec();
  let written = 0;
  const tx = db.transaction((items) => {
    for (const item of items) {
      const row = normalizeRawPathBar(item, { fetched_at: ts });
      if (!row.token_ca || !row.pool_address || row.timestamp == null) continue;
      if (row.open == null || row.high == null || row.low == null || row.close == null) continue;
      stmt.run({
        ...row,
        volume: row.volume ?? 0,
        source_family: row.source_family || null,
        price_unit: row.price_unit || 'native',
        trade_count: row.trade_count ?? null,
        first_trade_ts: row.first_trade_ts ?? null,
        last_trade_ts: row.last_trade_ts ?? null,
        fetched_at: row.fetched_at ?? ts,
        payload_json: row.payload_json || null,
        updated_at: ts,
      });
      written += 1;
    }
  });
  tx(bars);
  return written;
}

function barsToRawPathRows({ tokenCa, poolAddress, provider, bars }) {
  return (bars || []).map((bar) => {
    const rowProvider = String(bar.provider || provider || '').toLowerCase();
    const sourceKind = rowProvider.includes('helius') ? 'amm_pool' : 'indexed_ohlcv';
    return {
      token_ca: tokenCa,
      pool_address: bar.pool_address || poolAddress || '',
      timestamp: bar.timestamp,
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
      volume: bar.volume || 0,
      provider: sourceKind === 'amm_pool' ? 'helius_amm_pool' : (bar.provider || provider || 'indexed_ohlcv'),
      source_kind: sourceKind,
      source_family: sourceKind === 'amm_pool' ? 'onchain_swap' : 'third_party_kline',
      price_unit: 'native',
      fetched_at: nowSec(),
    };
  });
}

function looksLikePumpFunMint(tokenCa) {
  return String(tokenCa || '').trim().toLowerCase().endsWith('pump');
}

async function backfillBondingCurveWindow(service, { tokenCa, signalTsSec, endTs, maxPages = 3, pageSize = 100 }) {
  if (!looksLikePumpFunMint(tokenCa) || !service?.heliusClient?.isEnabled?.()) {
    return {
      provider: null,
      poolAddress: null,
      bars: [],
      signaturesFetched: 0,
      transactionsFetched: 0,
      rawTrades: 0,
      error: looksLikePumpFunMint(tokenCa) ? 'helius_disabled' : 'not_pump_fun_mint',
    };
  }

  let before = null;
  let signaturesFetched = 0;
  let transactionsFetched = 0;
  let oldestBlockTime = null;
  const transactions = [];
  for (let page = 0; page < maxPages; page += 1) {
    const pageResult = await service.heliusClient.fetchHistoryPage(tokenCa, {
      before,
      limit: pageSize,
    });
    const signatures = pageResult.signatures || [];
    if (!signatures.length) break;
    signaturesFetched += signatures.length;
    transactionsFetched += pageResult.transactions?.length || 0;
    transactions.push(...(pageResult.transactions || []));
    before = signatures[signatures.length - 1]?.signature || null;
    for (const sig of signatures) {
      if (sig.blockTime) {
        oldestBlockTime = oldestBlockTime == null ? sig.blockTime : Math.min(oldestBlockTime, sig.blockTime);
      }
    }
    if (oldestBlockTime != null && oldestBlockTime <= signalTsSec) break;
  }

  const trades = normalizeBondingCurveTransactions(transactions, { tokenCa })
    .filter((trade) => trade.blockTime >= signalTsSec && trade.blockTime <= endTs);
  const bars = aggregateSwapsToRawPriceBars(trades, {
    token_ca: tokenCa,
    source_kind: 'bonding_curve',
    provider: 'helius_bonding_curve',
    price_unit: 'native',
  }).filter((bar) => bar.timestamp >= signalTsSec && bar.timestamp <= endTs);
  const error = bars.length ? null : (trades.length ? 'no_bonding_curve_bars' : 'no_bonding_curve_trades');
  return {
    provider: bars.length ? 'helius_bonding_curve' : null,
    poolAddress: `bonding_curve:${tokenCa}`,
    bars,
    signaturesFetched,
    transactionsFetched,
    rawTrades: trades.length,
    error,
  };
}

async function main() {
  if (!process.env.HELIUS_API_KEY && !process.env.HELIUS_RPC_URL) {
    throw new Error('HELIUS_API_KEY or HELIUS_RPC_URL is required for raw path backfill');
  }

  const signalDbPath = resolvePath(process.env.DB_PATH || autonomyConfig.dbPath || './data/sentiment_arb.db');
  const lookbackHours = envInt('RAW_PATH_OBSERVER_LOOKBACK_HOURS', 24, 1, 168);
  const signalLimit = envInt('RAW_PATH_OBSERVER_SIGNAL_LIMIT', 5000, 1, 50000);
  const maxSignalsPerRun = envInt('RAW_PATH_OBSERVER_MAX_SIGNALS_PER_RUN', 25, 1, 500);
  const horizonSec = envInt('RAW_PATH_OBSERVER_HORIZON_SEC', 7200, 300, 24 * 3600);
  const now = nowSec();

  const signalDb = openSqlite(signalDbPath, { readonly: true, fileMustExist: true });
  const rawDb = openSqlite(rawDbPath());
  ensureRawPathObserverSchema(rawDb);
  const service = new MarketDataBackfillService(autonomyConfig);

  const summary = {
    schema_version: 'raw_path_observer_backfill.v1',
    started_at: new Date().toISOString(),
    observe_only: true,
    signal_db_path: signalDbPath,
    raw_db_path: rawDbPath(),
    lookback_hours: lookbackHours,
    signal_limit: signalLimit,
    max_signals_per_run: maxSignalsPerRun,
    horizon_sec: horizonSec,
    loaded_signals: 0,
    selection_strategy: 'missing_raw_path_first',
    selection_priority_breakdown: {},
    processed: 0,
    bars_written_to_kline_cache: 0,
    bars_written_to_raw_db: 0,
    resolved_pools: 0,
    bonding_curve_backfills: 0,
    no_pool: 0,
    errors: 0,
    results: [],
    note: 'Observe-only raw path backfill. AMM pools use PoolResolver/Helius history; pump.fun pre-graduation mints fall back to mint-address Helius enhanced transactions and are written as source_kind=bonding_curve.',
  };

  try {
    const signals = loadPremiumSignals(signalDb, { lookbackHours, limit: signalLimit });
    summary.loaded_signals = signals.length;
    const rankedSignals = rankSignalsForBackfill(selectUniqueSignals(signals, signalLimit), {
      signalDb,
      rawDb,
      service,
      now,
      horizonSec,
    });
    summary.selection_priority_breakdown = rankedSignals.reduce((out, row) => {
      const key = row.raw_path_selection_reason || 'unknown';
      out[key] = (out[key] || 0) + 1;
      return out;
    }, {});
    const selected = rankedSignals.slice(0, maxSignalsPerRun);
    for (const signal of selected) {
      const tokenCa = String(signal.token_ca || '').trim();
      const signalTsSec = Number(signal.signal_ts_sec);
      const endTs = Math.min(now, signalTsSec + horizonSec);
      if (!tokenCa || !Number.isFinite(signalTsSec) || endTs <= signalTsSec) continue;
      const result = await service.backfillWindow({
        tokenCa,
        signalTsSec,
        startTs: signalTsSec,
        endTs,
        minBars: 1,
      });
      let rawRows = barsToRawPathRows({
        tokenCa,
        poolAddress: result.poolAddress || null,
        provider: result.provider || result.poolProvider || null,
        bars: result.bars || [],
      });
      let bondingResult = null;
      if (!rawRows.length && looksLikePumpFunMint(tokenCa) && (!result.poolAddress || result.error === 'no_pool')) {
        bondingResult = await backfillBondingCurveWindow(service, {
          tokenCa,
          signalTsSec,
          endTs,
          maxPages: envInt('RAW_PATH_OBSERVER_BONDING_MAX_PAGES', 3, 1, 10),
          pageSize: envInt('RAW_PATH_OBSERVER_BONDING_PAGE_SIZE', 100, 10, 1000),
        });
        rawRows = bondingResult.bars || [];
        if (rawRows.length) summary.bonding_curve_backfills += 1;
      }
      const rawRowsWritten = upsertRawPriceBars(rawDb, rawRows);
      summary.processed += 1;
      summary.bars_written_to_kline_cache += Number(result.barsWritten || 0);
      summary.bars_written_to_raw_db += rawRowsWritten;
      if (result.poolAddress) summary.resolved_pools += 1;
      if (result.error === 'no_pool' && !rawRows.length) summary.no_pool += 1;
      if (result.error && result.error !== 'no_helius_bars' && result.error !== 'no_helius_trades' && result.error !== 'no_pool') summary.errors += 1;
      summary.results.push({
        token_ca: tokenCa,
        symbol: signal.symbol || null,
        signal_ts_sec: signalTsSec,
        end_ts: endTs,
        selection_priority: signal.raw_path_selection_priority,
        selection_reason: signal.raw_path_selection_reason,
        existing_path_bars: signal.existing_path_bars,
        existing_baseline_bars: signal.existing_baseline_bars,
        raw_path_existing_bars: signal.raw_path_existing_bars,
        legacy_kline_existing_bars: signal.legacy_kline_existing_bars,
        cache_existing_bars: signal.cache_existing_bars,
        provider: result.provider || null,
        pool_provider: result.poolProvider || null,
        pool_address: result.poolAddress || null,
        bars_available: result.bars?.length || 0,
        bars_written_to_kline_cache: result.barsWritten || 0,
        bars_written_to_raw_db: rawRowsWritten,
        signatures_fetched: result.signaturesFetched || 0,
        transactions_fetched: result.transactionsFetched || 0,
        trades_inserted: result.tradesInserted || 0,
        bonding_curve: bondingResult ? {
          provider: bondingResult.provider,
          pool_address: bondingResult.poolAddress,
          bars: bondingResult.bars?.length || 0,
          signatures_fetched: bondingResult.signaturesFetched || 0,
          transactions_fetched: bondingResult.transactionsFetched || 0,
          raw_trades: bondingResult.rawTrades || 0,
          error: bondingResult.error || null,
        } : null,
        cache_hit: Boolean(result.cacheHit),
        error: result.error || null,
      });
    }
    summary.completed_at = new Date().toISOString();
    console.log(JSON.stringify(summary, null, 2));
  } finally {
    try { service.close(); } catch {}
    try { signalDb.close(); } catch {}
    try { rawDb.close(); } catch {}
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exit(1);
  });
}

export {
  countBarsInSqlite,
  countRepositoryBars,
  rankSignalsForBackfill,
  selectUniqueSignals,
};
