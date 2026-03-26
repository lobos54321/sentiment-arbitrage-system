#!/usr/bin/env node
import Database from 'better-sqlite3';
import autonomyConfig from '../src/config/autonomy-config.js';

const dbPaths = autonomyConfig.evaluator.klineCacheCandidates;
const report = [];

function hasColumn(db, tableName, columnName) {
  return db.prepare(`PRAGMA table_info(${tableName})`).all().some((row) => row.name === columnName);
}

for (const dbPath of dbPaths) {
  const db = new Database(dbPath, { readonly: true });
  const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").all().map((row) => row.name);
  const klineHasProvider = tables.includes('kline_1m') ? hasColumn(db, 'kline_1m', 'provider') : false;
  const bars = tables.includes('kline_1m') ? db.prepare('SELECT COUNT(*) as c FROM kline_1m').get().c : 0;
  const tokens = tables.includes('kline_1m') ? db.prepare('SELECT COUNT(*) as c FROM (SELECT DISTINCT token_ca FROM kline_1m)').get().c : 0;
  const pools = tables.includes('pool_mapping') ? db.prepare('SELECT COUNT(*) as c FROM pool_mapping').get().c : 0;
  const providerBreakdown = tables.includes('kline_1m')
    ? (klineHasProvider
        ? db.prepare('SELECT provider, COUNT(*) as count FROM kline_1m GROUP BY provider ORDER BY count DESC').all()
        : [{ provider: 'legacy', count: bars }])
    : [];
  const heliusTrades = tables.includes('helius_trades') ? db.prepare('SELECT COUNT(*) as c FROM helius_trades').get().c : 0;
  const heliusTradeTokens = tables.includes('helius_trades') ? db.prepare('SELECT COUNT(DISTINCT token_ca) as c FROM helius_trades').get().c : 0;
  const cursors = tables.includes('history_backfill_cursor') ? db.prepare('SELECT COUNT(*) as c FROM history_backfill_cursor').get().c : 0;
  const samples = tables.includes('kline_1m')
    ? (klineHasProvider
        ? db.prepare('SELECT token_ca, provider, MIN(timestamp) as min_ts, MAX(timestamp) as max_ts, COUNT(*) as bars FROM kline_1m GROUP BY token_ca, provider ORDER BY bars DESC LIMIT 5').all()
        : db.prepare("SELECT token_ca, 'legacy' as provider, MIN(timestamp) as min_ts, MAX(timestamp) as max_ts, COUNT(*) as bars FROM kline_1m GROUP BY token_ca ORDER BY bars DESC LIMIT 5").all())
    : [];

  report.push({ dbPath, tables, bars, tokens, pools, providerBreakdown, heliusTrades, heliusTradeTokens, cursors, samples });
}

console.log(JSON.stringify(report, null, 2));
