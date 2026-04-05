#!/usr/bin/env node
import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

function readCliArg(name) {
  const argv = process.argv.slice(2);
  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    if (current === name) {
      return argv[index + 1] || null;
    }
    if (current.startsWith(`${name}=`)) {
      return current.slice(name.length + 1) || null;
    }
  }
  return null;
}

const sentimentDbPath = readCliArg('--sentiment-db')
  || process.env.SENTIMENT_DB
  || process.env.DB_PATH
  || join(projectRoot, 'data', 'sentiment_arb.db');
const paperDbPath = readCliArg('--db')
  || process.env.PAPER_DB
  || join(projectRoot, 'data', 'paper_trades.db');
const isDryRun = process.argv.includes('--dry-run');

function parseJson(value, fallback = null) {
  if (!value) return fallback;
  try { return JSON.parse(value); } catch { return fallback; }
}

function normalizeSignalType(row) {
  if (row.signal_type) return String(row.signal_type).toUpperCase();
  const text = String(row.description || '');
  if (text.includes('New Trending')) return 'NEW_TRENDING';
  if (/\bATH\b/i.test(text) && !/NOT_ATH/i.test(text)) return 'ATH';
  return 'UNKNOWN';
}

function inferParseMissingFields(row, signalType) {
  const missing = [];
  if (!row.symbol) missing.push('symbol');
  if (!(Number(row.market_cap) > 0)) missing.push('market_cap');
  if (signalType === 'NEW_TRENDING') {
    if (!(Number(row.holders) > 0)) missing.push('holders');
    if (!(Number(row.top10_pct) > 0)) missing.push('top10_pct');
  }
  return missing;
}

function inferParseStatus(row, signalType) {
  if (row.parse_status) return row.parse_status;
  const missing = inferParseMissingFields(row, signalType);
  if (signalType === 'UNKNOWN') return 'unknown';
  return missing.length ? 'partial' : 'parsed';
}

function inferPaperOutcome(row) {
  const exitReason = row.exit_reason ? String(row.exit_reason) : null;
  if (!exitReason) {
    if (row.last_exit_quote_failure) {
      return {
        strategyOutcome: 'blocked_by_infra',
        executionAvailability: 'unavailable',
        accountingOutcome: 'open',
        syntheticClose: 0,
      };
    }
    return {
      strategyOutcome: 'entered',
      executionAvailability: 'available',
      accountingOutcome: 'open',
      syntheticClose: 0,
    };
  }

  if (exitReason.startsWith('trapped_') || exitReason.startsWith('legacy_missing_')) {
    return {
      strategyOutcome: 'blocked_by_infra',
      executionAvailability: 'unavailable',
      accountingOutcome: 'closed_synthetic',
      syntheticClose: 1,
    };
  }

  return {
    strategyOutcome: exitReason,
    executionAvailability: 'available',
    accountingOutcome: 'closed_real',
    syntheticClose: 0,
  };
}

function buildSignalIndex(sentimentDb) {
  const rows = sentimentDb.prepare(`
    SELECT id, token_ca, timestamp, signal_type, is_ath, parse_status, hard_gate_status, description
    FROM premium_signals
    ORDER BY id ASC
  `).all();

  const byToken = new Map();
  for (const row of rows) {
    if (!byToken.has(row.token_ca)) byToken.set(row.token_ca, []);
    byToken.get(row.token_ca).push(row);
  }
  return { rows, byToken };
}

function bestSignalMatch(signalRows, tokenCa, signalTs) {
  const candidates = signalRows.get(tokenCa) || [];
  if (!candidates.length) return null;
  const ts = Number(signalTs || 0);
  const scored = candidates.map((row) => ({
    row,
    delta: Math.abs(Number(row.timestamp || 0) - ts),
  })).sort((a, b) => a.delta - b.delta || a.row.id - b.row.id);
  return scored[0]?.delta <= 10 * 60 * 1000 ? scored[0].row : null;
}

function main() {
  console.log('═'.repeat(72));
  console.log('Backfill premium/paper lineage');
  console.log('═'.repeat(72));
  console.log(`Sentiment DB: ${sentimentDbPath}`);
  console.log(`Paper DB: ${paperDbPath}`);
  console.log(`Mode: ${isDryRun ? 'DRY RUN' : 'LIVE'}`);
  console.log(`CLI sentiment-db: ${readCliArg('--sentiment-db') || '-'}`);
  console.log(`CLI db: ${readCliArg('--db') || '-'}`);
  console.log('');

  const sentimentDb = new Database(sentimentDbPath);
  const paperDb = new Database(paperDbPath);

  const premiumRows = sentimentDb.prepare(`
    SELECT id, token_ca, symbol, market_cap, holders, top10_pct, description, raw_message,
           timestamp, source_message_ts, receive_ts, signal_type, is_ath, parse_status,
           parse_missing_fields, hard_gate_status, gate_result, downstream_trade_id, downstream_lifecycle_id
    FROM premium_signals
    ORDER BY id ASC
  `).all();

  const updatePremium = sentimentDb.prepare(`
    UPDATE premium_signals
    SET raw_message = ?,
        source_message_ts = ?,
        receive_ts = ?,
        signal_type = ?,
        is_ath = ?,
        parse_status = ?,
        parse_missing_fields = ?,
        gate_result = ?,
        downstream_trade_id = COALESCE(downstream_trade_id, ?),
        downstream_lifecycle_id = COALESCE(downstream_lifecycle_id, ?)
    WHERE id = ?
  `);

  let premiumUpdated = 0;
  for (const row of premiumRows) {
    const signalType = normalizeSignalType(row);
    const parseMissingFields = row.parse_missing_fields || JSON.stringify(inferParseMissingFields(row, signalType));
    const parseStatus = inferParseStatus(row, signalType);
    const gateResult = row.gate_result || JSON.stringify({
      status: row.hard_gate_status || 'UNKNOWN',
      backfilled: true,
    });
    const rawMessage = row.raw_message || row.description || null;
    const sourceMessageTs = row.source_message_ts || row.timestamp || null;
    const receiveTs = row.receive_ts || row.timestamp || null;
    const isAth = row.is_ath != null ? Number(Boolean(row.is_ath)) : Number(signalType === 'ATH');

    if (!isDryRun) {
      updatePremium.run(
        rawMessage,
        sourceMessageTs,
        receiveTs,
        signalType,
        isAth,
        parseStatus,
        parseMissingFields,
        gateResult,
        null,
        null,
        row.id,
      );
    }
    premiumUpdated += 1;
  }

  const { byToken } = buildSignalIndex(sentimentDb);
  const paperRows = paperDb.prepare(`
    SELECT id, token_ca, signal_ts, exit_reason, last_exit_quote_failure,
           premium_signal_id, signal_type, strategy_outcome, execution_availability,
           accounting_outcome, synthetic_close
    FROM paper_trades
    ORDER BY id ASC
  `).all();

  const updatePaper = paperDb.prepare(`
    UPDATE paper_trades
    SET premium_signal_id = ?,
        signal_type = ?,
        strategy_outcome = ?,
        execution_availability = ?,
        accounting_outcome = ?,
        synthetic_close = ?
    WHERE id = ?
  `);

  let paperUpdated = 0;
  for (const row of paperRows) {
    const matchedSignal = row.premium_signal_id ? null : bestSignalMatch(byToken, row.token_ca, row.signal_ts);
    const signalType = row.signal_type || matchedSignal?.signal_type || normalizeSignalType({ signal_type: null, description: matchedSignal?.description || '' });
    const outcome = inferPaperOutcome(row);

    if (!isDryRun) {
      const nextStrategyOutcome = row.strategy_outcome || outcome.strategyOutcome;
      const nextExecutionAvailability = row.execution_availability || outcome.executionAvailability;
      const nextAccountingOutcome = row.accounting_outcome || outcome.accountingOutcome;
      const nextSyntheticClose = nextAccountingOutcome === 'closed_synthetic'
        ? 1
        : (row.synthetic_close != null ? row.synthetic_close : outcome.syntheticClose);
      updatePaper.run(
        row.premium_signal_id || matchedSignal?.id || null,
        signalType || null,
        nextStrategyOutcome,
        nextExecutionAvailability,
        nextAccountingOutcome,
        nextSyntheticClose,
        row.id,
      );
    }
    paperUpdated += 1;
  }

  if (!isDryRun) {
    const paperLinked = paperDb.prepare(`SELECT COUNT(*) AS c FROM paper_trades WHERE premium_signal_id IS NOT NULL`).get().c;
    const synthetic = paperDb.prepare(`SELECT COUNT(*) AS c FROM paper_trades WHERE synthetic_close = 1`).get().c;
    console.log(`premium_signals processed: ${premiumUpdated}`);
    console.log(`paper_trades processed: ${paperUpdated}`);
    console.log(`paper_trades linked: ${paperLinked}`);
    console.log(`synthetic closes marked: ${synthetic}`);
  } else {
    console.log(`Would process premium_signals: ${premiumUpdated}`);
    console.log(`Would process paper_trades: ${paperUpdated}`);
  }

  sentimentDb.close();
  paperDb.close();
}

main();
