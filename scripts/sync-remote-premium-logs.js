#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import http from 'http';
import https from 'https';
import Database from 'better-sqlite3';
import autonomyConfig from '../src/config/autonomy-config.js';

const BASE_URL = process.env.ZEABUR_URL || autonomyConfig.exports.zeaburBaseUrl;
const TOKEN = process.env.DASHBOARD_TOKEN || '';
const DB_PATH = autonomyConfig.dbPath;
const statePath = path.join(autonomyConfig.dataDir, 'sync-remote-premium-logs-state.json');
const parserVersion = 'remote-premium-importer-v1';
const PAGE_LIMIT = Math.max(1, parseInt(process.env.AUTONOMY_REMOTE_PREMIUM_PAGE_LIMIT || '500', 10));

function readJson(filePath, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

function writeJson(filePath, payload) {
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2));
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    const target = new URL(url);
    const client = target.protocol === 'http:' ? http : https;
    const request = client.get(target, {
      timeout: 30000,
      headers: {
        'User-Agent': 'autonomy-remote-premium-sync/1.0',
        'Accept': 'application/json'
      }
    }, (response) => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', (chunk) => {
        body += chunk;
      });
      response.on('end', () => {
        if (response.statusCode && response.statusCode >= 400) {
          reject(new Error(`Remote export failed with status ${response.statusCode}`));
          return;
        }
        try {
          resolve({ body, json: JSON.parse(body) });
        } catch (error) {
          reject(new Error(`Remote export returned invalid JSON: ${error.message}`));
        }
      });
    });
    request.on('error', reject);
    request.on('timeout', () => {
      request.destroy(new Error('Remote export request timed out'));
    });
  });
}

function ensurePremiumSignalsSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS premium_signals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      token_ca TEXT NOT NULL,
      symbol TEXT,
      market_cap REAL,
      holders INTEGER,
      volume_24h REAL,
      top10_pct REAL,
      age TEXT,
      description TEXT,
      raw_message TEXT,
      timestamp INTEGER NOT NULL,
      source_message_ts INTEGER,
      receive_ts INTEGER,
      signal_type TEXT,
      is_ath INTEGER DEFAULT 0,
      parse_status TEXT,
      parse_missing_fields TEXT,
      hard_gate_status TEXT,
      gate_result TEXT,
      ai_action TEXT,
      ai_confidence INTEGER,
      ai_narrative_tier TEXT,
      executed INTEGER DEFAULT 0,
      trade_result TEXT,
      downstream_trade_id INTEGER,
      downstream_lifecycle_id TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);
  const addColumn = (sql) => {
    try { db.exec(sql); } catch {}
  };
  addColumn(`ALTER TABLE premium_signals ADD COLUMN raw_message TEXT`);
  addColumn(`ALTER TABLE premium_signals ADD COLUMN source_message_ts INTEGER`);
  addColumn(`ALTER TABLE premium_signals ADD COLUMN receive_ts INTEGER`);
  addColumn(`ALTER TABLE premium_signals ADD COLUMN signal_type TEXT`);
  addColumn(`ALTER TABLE premium_signals ADD COLUMN is_ath INTEGER DEFAULT 0`);
  addColumn(`ALTER TABLE premium_signals ADD COLUMN parse_status TEXT`);
  addColumn(`ALTER TABLE premium_signals ADD COLUMN parse_missing_fields TEXT`);
  addColumn(`ALTER TABLE premium_signals ADD COLUMN gate_result TEXT`);
  addColumn(`ALTER TABLE premium_signals ADD COLUMN downstream_trade_id INTEGER`);
  addColumn(`ALTER TABLE premium_signals ADD COLUMN downstream_lifecycle_id TEXT`);
  addColumn(`ALTER TABLE premium_signals ADD COLUMN remote_signal_id INTEGER`);
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_premium_signals_remote_signal_id ON premium_signals(remote_signal_id) WHERE remote_signal_id IS NOT NULL`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_premium_signals_token_ts_type ON premium_signals(token_ca, timestamp, signal_type)`);
}

function normalizeJsonField(value, fallback = null) {
  if (value == null || value === '') return fallback;
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}

function normalizeInt(value, fallback = null) {
  if (value == null || value === '') return fallback;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? Math.trunc(numeric) : fallback;
}

function normalizeFloat(value, fallback = null) {
  if (value == null || value === '') return fallback;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function normalizeSignalType(row) {
  if (row.signal_type) return String(row.signal_type).toUpperCase();
  if (row.is_ath === 1 || row.is_ath === true) return 'ATH';
  return 'NEW_TRENDING';
}

function normalizeRow(row) {
  const signalType = normalizeSignalType(row);
  const timestamp = normalizeInt(row.timestamp, normalizeInt(row.source_message_ts, normalizeInt(row.receive_ts, Date.now())));
  const sourceMessageTs = normalizeInt(row.source_message_ts, timestamp);
  const receiveTs = normalizeInt(row.receive_ts, timestamp);
  const parseMissingFields = Array.isArray(row.parse_missing_fields)
    ? JSON.stringify(row.parse_missing_fields)
    : normalizeJsonField(row.parse_missing_fields, null);
  const gateResult = normalizeJsonField(row.gate_result, null);

  return {
    remoteSignalId: normalizeInt(row.id, null),
    tokenCa: row.token_ca || row.tokenCa || null,
    symbol: row.symbol || null,
    marketCap: normalizeFloat(row.market_cap, null),
    holders: normalizeInt(row.holders, null),
    volume24h: normalizeFloat(row.volume_24h, null),
    top10Pct: normalizeFloat(row.top10_pct, null),
    age: row.age || null,
    description: row.description || null,
    rawMessage: row.raw_message || row.description || null,
    timestamp,
    sourceMessageTs,
    receiveTs,
    signalType,
    isAth: row.is_ath === 1 || row.is_ath === true ? 1 : 0,
    parseStatus: row.parse_status || (parseMissingFields ? 'partial' : 'parsed'),
    parseMissingFields,
    hardGateStatus: row.hard_gate_status || null,
    gateResult,
    aiAction: row.ai_action || null,
    aiConfidence: normalizeInt(row.ai_confidence, null),
    aiNarrativeTier: row.ai_narrative_tier || null,
    executed: row.executed === 1 || row.executed === true ? 1 : 0,
    tradeResult: row.trade_result || null,
    downstreamTradeId: normalizeInt(row.downstream_trade_id, null),
    downstreamLifecycleId: row.downstream_lifecycle_id || null,
    createdAt: row.created_at || null
  };
}

function rowsDiffer(localRow, incoming) {
  const fields = [
    ['token_ca', incoming.tokenCa],
    ['symbol', incoming.symbol],
    ['market_cap', incoming.marketCap],
    ['holders', incoming.holders],
    ['volume_24h', incoming.volume24h],
    ['top10_pct', incoming.top10Pct],
    ['age', incoming.age],
    ['description', incoming.description],
    ['raw_message', incoming.rawMessage],
    ['timestamp', incoming.timestamp],
    ['source_message_ts', incoming.sourceMessageTs],
    ['receive_ts', incoming.receiveTs],
    ['signal_type', incoming.signalType],
    ['is_ath', incoming.isAth],
    ['parse_status', incoming.parseStatus],
    ['parse_missing_fields', incoming.parseMissingFields],
    ['hard_gate_status', incoming.hardGateStatus],
    ['gate_result', incoming.gateResult],
    ['ai_action', incoming.aiAction],
    ['ai_confidence', incoming.aiConfidence],
    ['ai_narrative_tier', incoming.aiNarrativeTier],
    ['executed', incoming.executed],
    ['trade_result', incoming.tradeResult],
    ['downstream_trade_id', incoming.downstreamTradeId],
    ['downstream_lifecycle_id', incoming.downstreamLifecycleId],
    ['remote_signal_id', incoming.remoteSignalId]
  ];
  return fields.some(([key, value]) => (localRow[key] ?? null) !== (value ?? null));
}

async function main() {
  if (!TOKEN) {
    throw new Error('DASHBOARD_TOKEN is required');
  }

  const previous = readJson(statePath, {}) || {};
  const lastRemoteId = normalizeInt(previous.lastRemoteId, 0) || 0;
  let beforeId = null;
  let downloadedBytes = 0;
  let parsedSignals = 0;
  let inserted = 0;
  let updated = 0;
  let skipped = 0;
  let latest = previous.latest || null;
  let latestExportedAt = previous.lastExportedAt || null;
  let maxSeenRemoteId = lastRemoteId;
  let pageCount = 0;

  const db = new Database(DB_PATH);
  ensurePremiumSignalsSchema(db);

  const selectByRemoteId = db.prepare(`SELECT * FROM premium_signals WHERE remote_signal_id = ? LIMIT 1`);
  const selectByNaturalKey = db.prepare(`
    SELECT * FROM premium_signals
    WHERE token_ca = ?
      AND timestamp = ?
      AND signal_type = ?
      AND (
        (? IS NOT NULL AND source_message_ts = ?)
        OR (? IS NULL AND source_message_ts IS NULL)
      )
    ORDER BY id DESC
    LIMIT 1
  `);
  const insertStmt = db.prepare(`
    INSERT INTO premium_signals (
      token_ca, symbol, market_cap, holders, volume_24h, top10_pct,
      age, description, raw_message, timestamp, source_message_ts, receive_ts,
      signal_type, is_ath, parse_status, parse_missing_fields,
      hard_gate_status, gate_result, ai_action, ai_confidence, ai_narrative_tier,
      executed, trade_result, downstream_trade_id, downstream_lifecycle_id, remote_signal_id,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
  `);
  const updateStmt = db.prepare(`
    UPDATE premium_signals
    SET token_ca = ?,
        symbol = ?,
        market_cap = ?,
        holders = ?,
        volume_24h = ?,
        top10_pct = ?,
        age = ?,
        description = ?,
        raw_message = ?,
        timestamp = ?,
        source_message_ts = ?,
        receive_ts = ?,
        signal_type = ?,
        is_ath = ?,
        parse_status = ?,
        parse_missing_fields = ?,
        hard_gate_status = ?,
        gate_result = ?,
        ai_action = ?,
        ai_confidence = ?,
        ai_narrative_tier = ?,
        executed = ?,
        trade_result = ?,
        downstream_trade_id = ?,
        downstream_lifecycle_id = ?,
        remote_signal_id = COALESCE(remote_signal_id, ?)
    WHERE id = ?
  `);

  const applyRow = db.transaction((incoming) => {
    if (!incoming.tokenCa || !incoming.timestamp) {
      skipped += 1;
      return;
    }

    const existing = incoming.remoteSignalId != null
      ? (selectByRemoteId.get(incoming.remoteSignalId) || selectByNaturalKey.get(incoming.tokenCa, incoming.timestamp, incoming.signalType, incoming.sourceMessageTs, incoming.sourceMessageTs, incoming.sourceMessageTs))
      : selectByNaturalKey.get(incoming.tokenCa, incoming.timestamp, incoming.signalType, incoming.sourceMessageTs, incoming.sourceMessageTs, incoming.sourceMessageTs);

    if (!existing) {
      insertStmt.run(
        incoming.tokenCa,
        incoming.symbol,
        incoming.marketCap,
        incoming.holders,
        incoming.volume24h,
        incoming.top10Pct,
        incoming.age,
        incoming.description,
        incoming.rawMessage,
        incoming.timestamp,
        incoming.sourceMessageTs,
        incoming.receiveTs,
        incoming.signalType,
        incoming.isAth,
        incoming.parseStatus,
        incoming.parseMissingFields,
        incoming.hardGateStatus,
        incoming.gateResult,
        incoming.aiAction,
        incoming.aiConfidence,
        incoming.aiNarrativeTier,
        incoming.executed,
        incoming.tradeResult,
        incoming.downstreamTradeId,
        incoming.downstreamLifecycleId,
        incoming.remoteSignalId,
        incoming.createdAt
      );
      inserted += 1;
      return;
    }

    if (!rowsDiffer(existing, incoming)) {
      skipped += 1;
      return;
    }

    updateStmt.run(
      incoming.tokenCa,
      incoming.symbol,
      incoming.marketCap,
      incoming.holders,
      incoming.volume24h,
      incoming.top10Pct,
      incoming.age,
      incoming.description,
      incoming.rawMessage,
      incoming.timestamp,
      incoming.sourceMessageTs,
      incoming.receiveTs,
      incoming.signalType,
      incoming.isAth,
      incoming.parseStatus,
      incoming.parseMissingFields,
      incoming.hardGateStatus,
      incoming.gateResult,
      incoming.aiAction,
      incoming.aiConfidence,
      incoming.aiNarrativeTier,
      incoming.executed,
      incoming.tradeResult,
      incoming.downstreamTradeId,
      incoming.downstreamLifecycleId,
      incoming.remoteSignalId,
      existing.id
    );
    updated += 1;
  });

  while (true) {
    const url = new URL('/api/export', BASE_URL);
    url.searchParams.set('token', TOKEN);
    url.searchParams.set('limit', String(PAGE_LIMIT));
    if (beforeId != null) {
      url.searchParams.set('before_id', String(beforeId));
    }

    const { body, json } = await fetchJson(url.toString());
    downloadedBytes += Buffer.byteLength(body, 'utf8');
    pageCount += 1;
    latestExportedAt = json?.exported_at || latestExportedAt;

    const rows = json?.tables?.premium_signals?.rows || [];
    if (!rows.length) break;

    parsedSignals += rows.length;
    const normalizedRows = rows
      .map((row) => normalizeRow(row))
      .filter((row) => row.remoteSignalId != null || (row.tokenCa && row.timestamp));

    if (normalizedRows.length) {
      const latestRow = normalizedRows.reduce((best, row) => {
        if (!best) return row;
        if ((row.remoteSignalId ?? 0) > (best.remoteSignalId ?? 0)) return row;
        if ((row.remoteSignalId ?? 0) === (best.remoteSignalId ?? 0) && (row.timestamp ?? 0) > (best.timestamp ?? 0)) return row;
        return best;
      }, latest || null);

      latest = latestRow ? {
        lastRemoteId: latestRow.remoteSignalId ?? null,
        lastTimestamp: latestRow.timestamp ?? null,
        lastTokenCa: latestRow.tokenCa ?? null,
        signalType: latestRow.signalType ?? null
      } : latest;
    }

    let reachedWatermark = false;
    for (const row of normalizedRows) {
      if (row.remoteSignalId != null) {
        maxSeenRemoteId = Math.max(maxSeenRemoteId, row.remoteSignalId);
      }
      if (lastRemoteId > 0 && row.remoteSignalId != null && row.remoteSignalId <= lastRemoteId) {
        reachedWatermark = true;
        continue;
      }
      applyRow(row);
    }

    const oldestRemoteId = normalizedRows.reduce((min, row) => {
      if (row.remoteSignalId == null) return min;
      if (min == null) return row.remoteSignalId;
      return row.remoteSignalId < min ? row.remoteSignalId : min;
    }, null);

    if (lastRemoteId > 0 && reachedWatermark) break;
    if (oldestRemoteId == null || normalizedRows.length < PAGE_LIMIT || oldestRemoteId <= 1) break;
    beforeId = oldestRemoteId;
  }

  db.close();

  latest = latest ? {
    ...latest,
    lastRemoteId: maxSeenRemoteId || latest.lastRemoteId || null
  } : (maxSeenRemoteId ? {
    lastRemoteId: maxSeenRemoteId,
    lastTimestamp: null,
    lastTokenCa: null,
    signalType: null
  } : null);

  const nextState = {
    parserVersion,
    baseUrl: BASE_URL,
    lastRemoteId: maxSeenRemoteId,
    lastExportedAt: latestExportedAt,
    updatedAt: new Date().toISOString(),
    latest,
    pageLimit: PAGE_LIMIT,
    pagesFetched: pageCount
  };
  writeJson(statePath, nextState);

  console.log(JSON.stringify({
    parserVersion,
    downloadedBytes,
    parsedSignals,
    inserted,
    updated,
    skipped,
    latest,
    dbPath: DB_PATH,
    statePath,
    source: 'remote-dashboard-export'
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
