#!/usr/bin/env node
/**
 * Shadow-only pump.fun launch observer.
 *
 * This script never writes premium_signals, candidate observations, paper
 * trades, or any production decision table. It only persists normalized
 * pump.fun launch events into an isolated SQLite DB for P8 side-by-side source
 * comparison.
 */

import fs from 'fs';
import os from 'os';
import { createHash } from 'crypto';
import { dirname, isAbsolute, join } from 'path';
import { fileURLToPath } from 'url';

import Database from 'better-sqlite3';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

const SCHEMA_VERSION = 'pump_fun_shadow_observer.v1';
const SIGNAL_SOURCE = 'pump_fun_shadow';
const SOURCE_COMPONENT = 'pump_fun_launch_stream';
const SIGNAL_TYPE = 'PUMP_FUN_LAUNCH';

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

function utcNow() {
  return new Date().toISOString();
}

function resolvePath(raw) {
  if (!raw) return raw;
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    db: process.env.PUMP_FUN_SHADOW_DB || '/app/data/pump_fun_shadow_signals.db',
    out: process.env.PUMP_FUN_SHADOW_SUMMARY || '/app/data/agent_runs/latest/pump_fun_shadow_observer_summary.json',
    inputJson: process.env.PUMP_FUN_SHADOW_INPUT_JSON || '',
    sourceUrl: process.env.PUMP_FUN_SHADOW_SOURCE_URL || '',
    websocketUrl: process.env.PUMP_FUN_SHADOW_WEBSOCKET_URL || '',
    subscribeJson: process.env.PUMP_FUN_SHADOW_SUBSCRIBE_JSON || '{"method":"subscribeNewToken"}',
    durationSec: Number.parseInt(process.env.PUMP_FUN_SHADOW_DURATION_SEC || '60', 10),
    limit: Number.parseInt(process.env.PUMP_FUN_SHADOW_LIMIT || '1000', 10),
    provider: process.env.PUMP_FUN_SHADOW_PROVIDER || 'pump_fun',
    selfTest: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = () => argv[++i];
    if (arg === '--db') args.db = next();
    else if (arg === '--out') args.out = next();
    else if (arg === '--input-json') args.inputJson = next();
    else if (arg === '--source-url') args.sourceUrl = next();
    else if (arg === '--websocket-url') args.websocketUrl = next();
    else if (arg === '--subscribe-json') args.subscribeJson = next();
    else if (arg === '--duration-sec') args.durationSec = Number.parseInt(next(), 10);
    else if (arg === '--limit') args.limit = Number.parseInt(next(), 10);
    else if (arg === '--provider') args.provider = next();
    else if (arg === '--self-test') args.selfTest = true;
    else if (arg === '--help' || arg === '-h') {
      printHelp();
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }
  args.db = resolvePath(args.db);
  args.out = resolvePath(args.out);
  args.inputJson = args.inputJson ? resolvePath(args.inputJson) : '';
  args.durationSec = Math.max(1, Math.min(Number.isFinite(args.durationSec) ? args.durationSec : 60, 3600));
  args.limit = Math.max(1, Math.min(Number.isFinite(args.limit) ? args.limit : 1000, 100000));
  return args;
}

function printHelp() {
  console.log(`
Usage:
  node scripts/pump_fun_shadow_observer.js [options]

Options:
  --db PATH               Shadow SQLite DB, default /app/data/pump_fun_shadow_signals.db
  --out PATH              Summary JSON artifact
  --input-json PATH       Read a bounded local JSON fixture or export
  --source-url URL        Poll an HTTP JSON endpoint once per loop
  --websocket-url URL     Connect to a websocket stream for duration-sec
  --subscribe-json JSON   Optional websocket subscribe payload
  --duration-sec N        Bounded live collection duration, default 60
  --limit N               Max events to ingest this run
  --provider NAME         Provider label, default pump_fun
  --self-test             Run isolated self-test
`);
}

function ensureSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS pump_fun_shadow_signals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_event_id TEXT NOT NULL,
      mint TEXT NOT NULL,
      symbol TEXT,
      name TEXT,
      event_ts INTEGER,
      observed_at INTEGER NOT NULL,
      signal_source TEXT NOT NULL DEFAULT '${SIGNAL_SOURCE}',
      source_component TEXT NOT NULL DEFAULT '${SOURCE_COMPONENT}',
      signal_type TEXT NOT NULL DEFAULT '${SIGNAL_TYPE}',
      provider TEXT,
      creator TEXT,
      bonding_curve TEXT,
      signature TEXT,
      market_cap REAL,
      sol_amount REAL,
      initial_buy REAL,
      uri TEXT,
      raw_payload_json TEXT,
      created_at INTEGER NOT NULL,
      UNIQUE(source_event_id)
    );
    CREATE INDEX IF NOT EXISTS idx_pump_fun_shadow_mint_ts
      ON pump_fun_shadow_signals(mint, event_ts);
    CREATE INDEX IF NOT EXISTS idx_pump_fun_shadow_observed_at
      ON pump_fun_shadow_signals(observed_at);
    CREATE TABLE IF NOT EXISTS pump_fun_shadow_runs (
      run_id TEXT PRIMARY KEY,
      schema_version TEXT NOT NULL,
      started_at TEXT NOT NULL,
      finished_at TEXT,
      mode TEXT,
      provider TEXT,
      inserted_count INTEGER DEFAULT 0,
      duplicate_count INTEGER DEFAULT 0,
      status TEXT,
      summary_json TEXT
    );
  `);
}

function openDb(dbPath) {
  fs.mkdirSync(dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  try { db.pragma('journal_mode = WAL'); } catch {}
  try { db.pragma('mmap_size = 0'); } catch {}
  ensureSchema(db);
  return db;
}

function writeJson(path, payload) {
  fs.mkdirSync(dirname(path), { recursive: true });
  const tmp = `${path}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  fs.renameSync(tmp, path);
}

function asString(value) {
  if (value === undefined || value === null) return null;
  const out = String(value).trim();
  return out || null;
}

function asNumber(value) {
  if (value === undefined || value === null || value === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function normalizeTs(value) {
  const num = asNumber(value);
  if (num === null) return null;
  return num > 1_000_000_000_000 ? Math.floor(num / 1000) : Math.floor(num);
}

function firstPresent(payload, keys) {
  for (const key of keys) {
    if (payload && Object.prototype.hasOwnProperty.call(payload, key) && payload[key] !== undefined && payload[key] !== null && payload[key] !== '') {
      return payload[key];
    }
  }
  return null;
}

function stableEventId(payload, normalized) {
  const direct = firstPresent(payload, [
    'source_event_id',
    'event_id',
    'id',
    'signature',
    'txHash',
    'tx_hash',
    'transactionSignature',
  ]);
  if (direct) return String(direct);
  const seed = JSON.stringify({
    mint: normalized.mint,
    event_ts: normalized.event_ts,
    symbol: normalized.symbol,
    creator: normalized.creator,
    payload,
  });
  return createHash('sha256').update(seed).digest('hex').slice(0, 32);
}

export function normalizePumpFunEvent(payload, { provider = 'pump_fun', observedAt = nowSec() } = {}) {
  if (!payload || typeof payload !== 'object') return null;
  const mint = asString(firstPresent(payload, ['mint', 'token_ca', 'token', 'address', 'ca', 'contractAddress']));
  if (!mint) return null;
  const normalized = {
    source_event_id: null,
    mint,
    symbol: asString(firstPresent(payload, ['symbol', 'ticker'])),
    name: asString(firstPresent(payload, ['name', 'tokenName'])),
    event_ts: normalizeTs(firstPresent(payload, ['event_ts', 'timestamp', 'created_at', 'createdAt', 'time', 'blockTime'])),
    observed_at: observedAt,
    signal_source: SIGNAL_SOURCE,
    source_component: SOURCE_COMPONENT,
    signal_type: SIGNAL_TYPE,
    provider,
    creator: asString(firstPresent(payload, ['creator', 'traderPublicKey', 'user', 'deployer'])),
    bonding_curve: asString(firstPresent(payload, ['bondingCurveKey', 'bonding_curve', 'bondingCurve', 'pool'])),
    signature: asString(firstPresent(payload, ['signature', 'txHash', 'tx_hash', 'transactionSignature'])),
    market_cap: asNumber(firstPresent(payload, ['marketCapSol', 'market_cap', 'marketCap', 'usd_market_cap'])),
    sol_amount: asNumber(firstPresent(payload, ['solAmount', 'sol_amount', 'solInBondingCurve'])),
    initial_buy: asNumber(firstPresent(payload, ['initialBuy', 'initial_buy', 'vTokensInBondingCurve'])),
    uri: asString(firstPresent(payload, ['uri', 'metadataUri', 'url'])),
    raw_payload_json: JSON.stringify(payload),
  };
  normalized.source_event_id = stableEventId(payload, normalized);
  if (!normalized.event_ts) normalized.event_ts = observedAt;
  return normalized;
}

function extractEvents(payload) {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload;
  if (typeof payload !== 'object') return [];
  for (const key of ['events', 'data', 'results', 'tokens', 'items']) {
    if (Array.isArray(payload[key])) return payload[key];
  }
  return [payload];
}

function insertEvents(db, events, args) {
  const insert = db.prepare(`
    INSERT OR IGNORE INTO pump_fun_shadow_signals (
      source_event_id, mint, symbol, name, event_ts, observed_at,
      signal_source, source_component, signal_type, provider, creator,
      bonding_curve, signature, market_cap, sol_amount, initial_buy,
      uri, raw_payload_json, created_at
    ) VALUES (
      @source_event_id, @mint, @symbol, @name, @event_ts, @observed_at,
      @signal_source, @source_component, @signal_type, @provider, @creator,
      @bonding_curve, @signature, @market_cap, @sol_amount, @initial_buy,
      @uri, @raw_payload_json, @created_at
    )
  `);
  let seen = 0;
  let normalized = 0;
  let inserted = 0;
  let duplicates = 0;
  const examples = [];
  const tx = db.transaction((rows) => {
    for (const row of rows) {
      if (seen >= args.limit) break;
      seen += 1;
      const event = normalizePumpFunEvent(row, { provider: args.provider, observedAt: nowSec() });
      if (!event) continue;
      normalized += 1;
      event.created_at = nowSec();
      const info = insert.run(event);
      if (info.changes > 0) inserted += 1;
      else duplicates += 1;
      if (examples.length < 5) {
        examples.push({
          mint: event.mint,
          symbol: event.symbol,
          event_ts: event.event_ts,
          source_event_id: event.source_event_id,
        });
      }
    }
  });
  tx(events);
  return { seen, normalized, inserted, duplicates, examples };
}

function readInputJson(path) {
  const payload = JSON.parse(fs.readFileSync(path, 'utf8'));
  return extractEvents(payload);
}

async function fetchJsonEvents(url) {
  const response = await fetch(url, { headers: { 'User-Agent': 'sentiment-arbitrage-p8-shadow/1.0' } });
  if (!response.ok) throw new Error(`HTTP ${response.status} fetching ${url}`);
  return extractEvents(await response.json());
}

async function resolveWebSocketConstructor() {
  if (typeof globalThis.WebSocket === 'function') {
    return { WebSocketCtor: globalThis.WebSocket, backend: 'global_websocket' };
  }
  try {
    const mod = await import('ws');
    const WebSocketCtor = mod.WebSocket || mod.default;
    if (typeof WebSocketCtor === 'function') {
      return { WebSocketCtor, backend: 'ws_package' };
    }
  } catch (error) {
    return {
      WebSocketCtor: null,
      backend: 'unavailable',
      error_code: 'websocket_backend_unavailable',
      error: error.message,
    };
  }
  return {
    WebSocketCtor: null,
    backend: 'unavailable',
    error_code: 'websocket_backend_unavailable',
  };
}

function websocketOn(ws, eventName, handler) {
  if (typeof ws.addEventListener === 'function') {
    ws.addEventListener(eventName, handler);
  } else if (typeof ws.on === 'function') {
    ws.on(eventName, handler);
  }
}

async function collectWebSocketEvents(args) {
  const resolved = await resolveWebSocketConstructor();
  if (!resolved.WebSocketCtor) {
    return {
      status: 'P8_STREAM_BACKEND_UNAVAILABLE',
      backend: resolved.backend,
      error_code: resolved.error_code || 'websocket_backend_unavailable',
      error: resolved.error || null,
      events: [],
    };
  }
  const events = [];
  const started = Date.now();
  let opened = false;
  let closeReason = null;
  await new Promise((resolve) => {
    const ws = new resolved.WebSocketCtor(args.websocketUrl);
    const timeout = setTimeout(() => {
      closeReason = 'duration_elapsed';
      try { ws.close(); } catch {}
      resolve();
    }, args.durationSec * 1000);
    websocketOn(ws, 'open', () => {
      opened = true;
      if (args.subscribeJson) {
        try { ws.send(args.subscribeJson); } catch {}
      }
    });
    websocketOn(ws, 'message', (message) => {
      try {
        const data = message?.data !== undefined ? message.data : message;
        const raw = typeof data === 'string' ? data : Buffer.from(data).toString('utf8');
        const parsed = JSON.parse(raw);
        for (const event of extractEvents(parsed)) {
          if (events.length < args.limit) events.push(event);
        }
        if (events.length >= args.limit) {
          closeReason = 'limit_reached';
          clearTimeout(timeout);
          try { ws.close(); } catch {}
          resolve();
        }
      } catch {
        // Ignore malformed stream messages; this is a shadow source.
      }
    });
    websocketOn(ws, 'error', (event) => {
      closeReason = 'websocket_error';
      clearTimeout(timeout);
      resolve(event);
    });
    websocketOn(ws, 'close', () => {
      clearTimeout(timeout);
      resolve();
    });
  });
  return {
    status: 'P8_STREAM_COLLECTED',
    backend: resolved.backend,
    opened,
    elapsed_sec: Math.round((Date.now() - started) / 1000),
    close_reason: closeReason,
    events,
  };
}

function dbCounts(db) {
  const row = db.prepare(`
    SELECT
      COUNT(*) AS signal_rows,
      COUNT(DISTINCT mint) AS unique_tokens,
      MIN(event_ts) AS min_event_ts,
      MAX(event_ts) AS max_event_ts,
      MAX(observed_at) AS latest_observed_at
    FROM pump_fun_shadow_signals
  `).get();
  return {
    signal_rows: Number(row?.signal_rows || 0),
    unique_tokens: Number(row?.unique_tokens || 0),
    min_event_ts: row?.min_event_ts == null ? null : Number(row.min_event_ts),
    max_event_ts: row?.max_event_ts == null ? null : Number(row.max_event_ts),
    latest_observed_at: row?.latest_observed_at == null ? null : Number(row.latest_observed_at),
  };
}

function writeRunRow(db, runId, summary) {
  db.prepare(`
    INSERT OR REPLACE INTO pump_fun_shadow_runs (
      run_id, schema_version, started_at, finished_at, mode, provider,
      inserted_count, duplicate_count, status, summary_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    runId,
    SCHEMA_VERSION,
    summary.started_at,
    summary.finished_at,
    summary.mode,
    summary.provider,
    summary.inserted_count,
    summary.duplicate_count,
    summary.status,
    JSON.stringify(summary)
  );
}

async function run(args) {
  const startedAt = utcNow();
  const db = openDb(args.db);
  const runId = `pumpfun_${startedAt.replace(/[-:]/g, '').replace(/\..+/, 'Z')}`;
  let mode = 'none';
  let sourceStatus = 'P8_SOURCE_NOT_CONFIGURED';
  let collection = { seen: 0, normalized: 0, inserted: 0, duplicates: 0, examples: [] };
  const notes = [];
  try {
    if (args.inputJson) {
      mode = 'input_json';
      const events = readInputJson(args.inputJson);
      collection = insertEvents(db, events, args);
      sourceStatus = 'P8_INPUT_JSON_INGESTED';
    } else if (args.websocketUrl) {
      mode = 'websocket';
      const wsResult = await collectWebSocketEvents(args);
      collection = insertEvents(db, wsResult.events || [], args);
      sourceStatus = wsResult.status;
      notes.push({
        websocket_backend: wsResult.backend ?? null,
        websocket_opened: wsResult.opened ?? null,
        close_reason: wsResult.close_reason ?? null,
        elapsed_sec: wsResult.elapsed_sec ?? null,
        error_code: wsResult.error_code ?? null,
        error: wsResult.error ?? null,
      });
    } else if (args.sourceUrl) {
      mode = 'http_poll';
      const deadline = Date.now() + args.durationSec * 1000;
      do {
        const events = await fetchJsonEvents(args.sourceUrl);
        const partial = insertEvents(db, events, args);
        collection.seen += partial.seen;
        collection.normalized += partial.normalized;
        collection.inserted += partial.inserted;
        collection.duplicates += partial.duplicates;
        collection.examples.push(...partial.examples);
        if (collection.seen >= args.limit) break;
        await new Promise((resolve) => setTimeout(resolve, 1000));
      } while (Date.now() < deadline);
      collection.examples = collection.examples.slice(0, 5);
      sourceStatus = 'P8_HTTP_POLL_INGESTED';
    } else {
      notes.push('Set PUMP_FUN_SHADOW_SOURCE_URL or PUMP_FUN_SHADOW_WEBSOCKET_URL, or pass --input-json for a bounded import.');
    }
  } catch (error) {
    sourceStatus = 'P8_SHADOW_OBSERVER_ERROR';
    notes.push({ error: error.message });
  }
  const counts = dbCounts(db);
  const summary = {
    schema_version: SCHEMA_VERSION,
    generated_at: utcNow(),
    started_at: startedAt,
    finished_at: utcNow(),
    run_id: runId,
    mode,
    provider: args.provider,
    status: sourceStatus,
    db_path: args.db,
    signal_source: SIGNAL_SOURCE,
    source_component: SOURCE_COMPONENT,
    signal_type: SIGNAL_TYPE,
    inserted_count: collection.inserted,
    duplicate_count: collection.duplicates,
    normalized_count: collection.normalized,
    seen_count: collection.seen,
    examples: collection.examples,
    cumulative: counts,
    production_impact: 'zero_shadow_only',
    promotion_allowed: false,
    strategy_change_allowed: false,
    automatic_runtime_change_allowed: false,
    paper_enablement_allowed: false,
    guardrails: {
      writes_premium_signals: false,
      writes_candidate_observations: false,
      writes_paper_trades: false,
      changes_entry_policy: false,
      changes_gates: false,
      changes_executor: false,
      changes_risk: false,
    },
    notes,
  };
  writeRunRow(db, runId, summary);
  writeJson(args.out, summary);
  db.close();
  return summary;
}

async function selfTest() {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'pump-fun-shadow-'));
  const inputPath = join(dir, 'events.json');
  const dbPath = join(dir, 'pump.db');
  const outPath = join(dir, 'summary.json');
  fs.writeFileSync(inputPath, JSON.stringify({
    events: [
      { mint: 'Pump111111111111111111111111111111111111111', symbol: 'PUMP', name: 'Pump Test', signature: 'sig-1', marketCapSol: 12, timestamp: 1_800_000_000 },
      { mint: 'Pump111111111111111111111111111111111111111', symbol: 'PUMP', name: 'Pump Test', signature: 'sig-1', marketCapSol: 12, timestamp: 1_800_000_000 },
      { mint: 'Shared1111111111111111111111111111111111111', symbol: 'SHR', name: 'Shared Test', signature: 'sig-2', timestamp: 1_800_000_010 },
      { nope: true },
    ],
  }), 'utf8');
  const summary = await run({
    db: dbPath,
    out: outPath,
    inputJson: inputPath,
    sourceUrl: '',
    websocketUrl: '',
    subscribeJson: '',
    durationSec: 1,
    limit: 100,
    provider: 'self_test',
  });
  if (summary.inserted_count !== 2) throw new Error(`expected 2 inserted, got ${summary.inserted_count}`);
  if (summary.duplicate_count !== 1) throw new Error(`expected 1 duplicate, got ${summary.duplicate_count}`);
  const payload = JSON.parse(fs.readFileSync(outPath, 'utf8'));
  if (payload.production_impact !== 'zero_shadow_only') throw new Error('guardrail summary missing');
  console.log(JSON.stringify({ ok: true, summary_path: outPath, db_path: dbPath }, null, 2));
}

const args = parseArgs();
if (args.selfTest) {
  selfTest().catch((error) => {
    console.error(error);
    process.exit(1);
  });
} else {
  run(args).then((summary) => {
    console.log(JSON.stringify(summary, null, 2));
  }).catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
