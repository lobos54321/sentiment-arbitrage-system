#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { execFileSync } from 'child_process';
import { createRequire } from 'module';
import { pathToFileURL } from 'url';

const require = createRequire(import.meta.url);
let BetterSqlite = null;
try {
  BetterSqlite = require('better-sqlite3');
} catch {
  BetterSqlite = null;
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    paperDb: '',
    outDb: '',
    startTs: null,
    endTs: null,
    cohortDogs: '',
    cohortDuds: '',
    marginSec: 900,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--paper-db') { args.paperDb = next; i += 1; continue; }
    if (key === '--out-db') { args.outDb = next; i += 1; continue; }
    if (key === '--start-ts') { args.startTs = next; i += 1; continue; }
    if (key === '--end-ts') { args.endTs = next; i += 1; continue; }
    if (key === '--cohort-dogs') { args.cohortDogs = next; i += 1; continue; }
    if (key === '--cohort-duds') { args.cohortDuds = next; i += 1; continue; }
    if (key === '--margin-sec') { args.marginSec = Number(next); i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/export-paper-decision-subset.js --paper-db paper_trades.db --out-db paper_decision_subset.db --cohort-dogs dogs.json --cohort-duds duds.json',
    '  node scripts/export-paper-decision-subset.js --paper-db paper_trades.db --out-db paper_decision_subset.db --start-ts 1780739887 --end-ts 1781199369',
    '',
    'Exports the decision tables needed by v10 decision-anchor audits into a small read-only data-pack DB.',
    'The time window is widened by --margin-sec (default 900) to support bounded matching around signal_ts.',
  ].join('\n');
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (Number.isFinite(n)) return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return Math.floor(parsed / 1000);
  }
  return null;
}

function loadJsonRows(filePath) {
  if (!filePath) return [];
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed.results)) return parsed.results;
  if (Array.isArray(parsed.rows)) return parsed.rows;
  return [];
}

function deriveWindow(args) {
  const explicitStart = numeric(args.startTs);
  const explicitEnd = numeric(args.endTs);
  if (explicitStart != null && explicitEnd != null) return { start: explicitStart, end: explicitEnd, source: 'explicit' };

  const rows = [
    ...loadJsonRows(args.cohortDogs),
    ...loadJsonRows(args.cohortDuds),
  ];
  const ts = rows.map((row) => numeric(row.signal_ts)).filter((value) => value != null).sort((a, b) => a - b);
  if (!ts.length) throw new Error('Provide --start-ts/--end-ts or cohort files with signal_ts');
  return { start: ts[0], end: ts[ts.length - 1], source: 'cohort' };
}

function sqlLiteral(value) {
  return String(value).replaceAll("'", "''");
}

function runSql(dbPath, sql) {
  if (BetterSqlite) {
    const db = new BetterSqlite(dbPath);
    try {
      const trimmed = sql.trim();
      if (/^SELECT\s+1\s+FROM\s+sqlite_master/i.test(trimmed)) {
        const rows = db.prepare(trimmed).all();
        return rows.map((row) => String(Object.values(row)[0])).join('\n');
      }
      const pragmaMatch = trimmed.match(/^PRAGMA\s+table_info\(([^)]+)\);?$/i);
      if (pragmaMatch) {
        const rows = db.prepare(trimmed).all();
        return rows.map((row) => [
          row.cid,
          row.name,
          row.type,
          row.notnull,
          row.dflt_value,
          row.pk,
        ].join('|')).join('\n');
      }
      db.exec(sql);
      return '';
    } finally {
      db.close();
    }
  }
  return execFileSync('sqlite3', [dbPath, sql], {
    encoding: 'utf8',
    maxBuffer: 20 * 1024 * 1024,
  });
}

function runJson(dbPath, sql) {
  if (BetterSqlite) {
    const db = new BetterSqlite(dbPath, { readonly: true, fileMustExist: true });
    try {
      return db.prepare(sql).all();
    } finally {
      db.close();
    }
  }
  const out = execFileSync('sqlite3', ['-json', dbPath, sql], {
    encoding: 'utf8',
    maxBuffer: 20 * 1024 * 1024,
  }).trim();
  return out ? JSON.parse(out) : [];
}

function tableExists(dbPath, tableName) {
  const out = runSql(dbPath, `SELECT 1 FROM sqlite_master WHERE type='table' AND name='${sqlLiteral(tableName)}' LIMIT 1;`).trim();
  return out === '1';
}

function tableColumns(dbPath, tableName) {
  const out = runSql(dbPath, `PRAGMA table_info(${tableName});`);
  return new Set(out.split(/\r?\n/).filter(Boolean).map((line) => line.split('|')[1]));
}

function requireColumns(dbPath, tableName, cols) {
  if (!tableExists(dbPath, tableName)) throw new Error(`Missing table: ${tableName}`);
  const available = tableColumns(dbPath, tableName);
  const missing = cols.filter((col) => !available.has(col));
  if (missing.length) throw new Error(`Table ${tableName} missing columns: ${missing.join(', ')}`);
}

const A_CLASS_COLUMNS = [
  'id',
  'event_ts',
  'token_ca',
  'symbol',
  'lifecycle_id',
  'signal_ts',
  'opportunity_ts',
  'action',
  'would_action',
  'grade',
  'score',
  'reason',
  'hard_blockers_json',
  'soft_notes_json',
  'block_cause',
  'recoverability',
  'classification_reason',
  'blocker_classifications_json',
  'quote_available',
  'quote_executable',
  'quote_clean',
  'route_available',
  'quote_source',
  'quote_age_sec',
  'data_confidence',
  'provider_reason',
  'provider_hydrate_outcome',
  'evidence_status',
  'quote_failure_reason',
  'route_failure_reason',
  'liquidity_usd',
  'spread_pct',
  'expected_rr',
  'defined_risk_pct',
  'expected_upside_pct',
  'matrix_json',
];

const OPPORTUNITY_COLUMNS = [
  'id',
  'opportunity_key',
  'event_ts',
  'token_ca',
  'symbol',
  'lifecycle_id',
  'source_type',
  'source_component',
  'source_reason',
  'raw_signal_ts',
  'opportunity_ts',
  'quote_available',
  'quote_executable',
  'quote_clean',
  'route_available',
  'liquidity_usd',
  'spread_pct',
  'matrix_score',
  'expected_rr',
  'defined_risk_pct',
  'hard_blockers_json',
  'soft_notes_json',
  'would_enter_a_class',
  'did_enter',
  'linked_trade_id',
  'final_entry_decision_json',
  'quote_source',
  'quote_age_sec',
  'data_confidence',
  'provider_data_state',
  'provider_reason',
  'provider_attempts_json',
  'evidence_status',
  'quote_failure_reason',
  'block_cause',
  'recoverability',
  'classification_reason',
  'blocker_classifications_json',
  'hydrate_outcome',
  'hydrate_success',
  'path_sample_count',
];

const LEDGER_COLUMNS = [
  'trade_id',
  'token_ca',
  'symbol',
  'lifecycle_id',
  'entry_ts',
  'entry_size_sol',
  'entry_price',
  'entry_quote_source',
  'entry_route_available',
  'entry_quote_executable',
  'entry_spread_pct',
  'entry_liquidity_usd',
  'entry_data_confidence',
  'exit_ts',
  'exit_price',
  'exit_reason',
  'realized_pnl_sol',
  'realized_pnl_pct',
  'peak_quote_pnl_pct',
  'time_to_peak_sec',
  'time_held_sec',
  'loss_cap_breach',
  'loss_cap_pct',
];

function csv(cols) {
  return cols.join(', ');
}

function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.paperDb || !fs.existsSync(args.paperDb)) throw new Error('Provide an existing --paper-db');
  if (!args.outDb) throw new Error('Provide --out-db');

  requireColumns(args.paperDb, 'a_class_decision_events', A_CLASS_COLUMNS);
  requireColumns(args.paperDb, 'opportunity_events', OPPORTUNITY_COLUMNS);
  requireColumns(args.paperDb, 'canonical_trade_ledger', LEDGER_COLUMNS);

  const window = deriveWindow(args);
  const margin = Math.max(0, Number(args.marginSec || 0));
  const start = window.start - margin;
  const end = window.end + margin;
  fs.mkdirSync(path.dirname(path.resolve(args.outDb)), { recursive: true });
  if (fs.existsSync(args.outDb)) fs.rmSync(args.outDb);

  const script = `
ATTACH DATABASE '${sqlLiteral(args.outDb)}' AS subset;
PRAGMA subset.journal_mode=DELETE;
CREATE TABLE subset.a_class_decision_events AS
  SELECT ${csv(A_CLASS_COLUMNS)}
  FROM main.a_class_decision_events
  WHERE event_ts BETWEEN ${start} AND ${end};
CREATE TABLE subset.opportunity_events AS
  SELECT ${csv(OPPORTUNITY_COLUMNS)}
  FROM main.opportunity_events
  WHERE event_ts BETWEEN ${start} AND ${end};
CREATE TABLE subset.canonical_trade_ledger AS
  SELECT ${csv(LEDGER_COLUMNS)}
  FROM main.canonical_trade_ledger
  WHERE COALESCE(entry_ts, exit_ts) BETWEEN ${start} AND ${end};
CREATE INDEX subset.idx_a_class_token_event ON a_class_decision_events(token_ca, event_ts);
CREATE INDEX subset.idx_a_class_signal ON a_class_decision_events(signal_ts);
CREATE INDEX subset.idx_opportunity_token_event ON opportunity_events(token_ca, event_ts);
CREATE INDEX subset.idx_opportunity_signal ON opportunity_events(raw_signal_ts);
CREATE INDEX subset.idx_ledger_token_entry ON canonical_trade_ledger(token_ca, entry_ts);
CREATE TABLE subset.export_manifest (
  schema_version TEXT,
  generated_at TEXT,
  source_paper_db TEXT,
  source_window TEXT,
  start_ts REAL,
  end_ts REAL,
  margin_sec REAL
);
INSERT INTO subset.export_manifest VALUES (
  'paper_decision_subset.v1',
  datetime('now'),
  '${sqlLiteral(args.paperDb)}',
  '${sqlLiteral(window.source)}',
  ${window.start},
  ${window.end},
  ${margin}
);
DETACH DATABASE subset;
`;
  runSql(args.paperDb, script);

  const [aClass] = runJson(args.outDb, 'SELECT count(*) AS rows, min(event_ts) AS min_ts, max(event_ts) AS max_ts FROM a_class_decision_events;');
  const [opportunity] = runJson(args.outDb, 'SELECT count(*) AS rows, min(event_ts) AS min_ts, max(event_ts) AS max_ts FROM opportunity_events;');
  const [ledger] = runJson(args.outDb, 'SELECT count(*) AS rows, min(entry_ts) AS min_ts, max(entry_ts) AS max_ts FROM canonical_trade_ledger;');
  console.log(JSON.stringify({
    out_db: args.outDb,
    a_class_decision_events: aClass?.rows || 0,
    opportunity_events: opportunity?.rows || 0,
    canonical_trade_ledger: ledger?.rows || 0,
    start_ts: window.start,
    end_ts: window.end,
    margin_sec: margin,
    exported_table_ranges: {
      a_class_decision_events: aClass || { rows: 0, min_ts: null, max_ts: null },
      opportunity_events: opportunity || { rows: 0, min_ts: null, max_ts: null },
      canonical_trade_ledger: ledger || { rows: 0, min_ts: null, max_ts: null },
    },
  }, null, 2));
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main();
  } catch (error) {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  }
}
