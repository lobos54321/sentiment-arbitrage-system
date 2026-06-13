#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { parse as parseCsv } from 'csv-parse/sync';

import {
  aggregateMinuteBars,
  derivePumpfunBondingCurvePda,
  estimatePumpfunProgressPctFromRealTokenReserves,
  estimatePumpfunReservePriceSolFromReserves,
} from './run-helius-pumpfun-curve-decode-audit.js';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    worklist: '',
    trades: '',
    out: '',
    preSec: 900,
    postSec: 0,
    assumeCompleteWindow: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--worklist') { args.worklist = next; i += 1; continue; }
    if (key === '--trades') { args.trades = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--pre-sec') { args.preSec = Number(next); i += 1; continue; }
    if (key === '--post-sec') { args.postSec = Number(next); i += 1; continue; }
    if (key === '--assume-complete-window') { args.assumeCompleteWindow = true; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/build-v10-curve-feature-decode-from-trades.js --worklist sample.txt --trades pumpfun-trades.csv --out merged-decode.json --assume-complete-window',
    '',
    'Builds a decode-like JSON from exported pump.fun TradeEvent rows, avoiding raw RPC tail scans.',
    'The output is consumable by build-v10-curve-feature-table.js.',
    '',
    'Required trade fields (flexible aliases accepted):',
    '  token_ca/mint, block_time/timestamp, side/is_buy, user/trader, sol_amount,',
    '  token_amount, and either reserve_price_sol/price_sol or reserves/progress fields.',
    '',
    'Use --assume-complete-window only when the export query fully covers every worklist [signal-pre, signal+post] window.',
  ].join('\n');
}

function readRows(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  if (!raw.trim()) return [];
  if (filePath.endsWith('.csv')) {
    return parseCsv(raw, {
      columns: true,
      skip_empty_lines: true,
      bom: true,
      trim: true,
    });
  }
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed;
    if (Array.isArray(parsed.rows)) return parsed.rows;
    if (Array.isArray(parsed.trades)) return parsed.trades;
    if (Array.isArray(parsed.results)) return parsed.results;
    return [parsed];
  } catch {
    return raw.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
  }
}

function readWorklist(filePath) {
  return fs.readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'))
    .map((line) => {
      const [token, ts, label = ''] = line.split(/[|,\t]/).map((part) => part.trim());
      return {
        token_ca: token,
        anchor_ts: normalizeTs(ts),
        symbol: label,
      };
    })
    .filter((row) => row.token_ca && row.anchor_ts != null);
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeTs(value) {
  const n = numeric(value);
  if (n == null) return null;
  return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

function pick(row, keys) {
  for (const key of keys) {
    if (row[key] != null && row[key] !== '') return row[key];
  }
  return null;
}

function normalizeSide(row) {
  const rawSide = String(pick(row, ['side', 'trade_side', 'direction']) || '').toLowerCase();
  if (rawSide === 'buy' || rawSide === 'sell') return rawSide;
  const isBuy = pick(row, ['is_buy', 'isBuy']);
  if (String(isBuy).toLowerCase() === 'true' || Number(isBuy) === 1) return 'buy';
  if (String(isBuy).toLowerCase() === 'false' || Number(isBuy) === 0) return 'sell';
  return 'unknown';
}

function normalizeTrade(row) {
  const token = String(pick(row, ['token_ca', 'mint', 'token_mint', 'mint_address']) || '').trim();
  const blockTime = normalizeTs(pick(row, ['block_time', 'timestamp', 'blockTime', 'evt_block_time']));
  if (!token || blockTime == null) return null;
  const tokenAmount = numeric(pick(row, ['token_amount', 'tokenAmount', 'token_amount_ui']));
  const solAmount = numeric(pick(row, ['sol_amount', 'solAmount', 'sol_amount_sol', 'sol']));
  const realTokenReserves = numeric(pick(row, ['real_token_reserves', 'realTokenReserves']));
  const virtualTokenReserves = numeric(pick(row, ['virtual_token_reserves', 'virtualTokenReserves']));
  const virtualSolReserves = numeric(pick(row, ['virtual_sol_reserves', 'virtualSolReserves']));
  const reservePriceSol = numeric(pick(row, ['reserve_price_sol', 'reservePriceSol']))
    ?? (virtualSolReserves && virtualTokenReserves ? virtualSolReserves / virtualTokenReserves : null)
    ?? estimatePumpfunReservePriceSolFromReserves({ virtualTokenReserves, realTokenReserves });
  const priceSol = numeric(pick(row, ['price_sol', 'priceSol']))
    ?? (solAmount != null && tokenAmount > 0 ? solAmount / tokenAmount : null)
    ?? reservePriceSol;
  const progressPct = numeric(pick(row, ['progress_pct', 'progressPct']))
    ?? estimatePumpfunProgressPctFromRealTokenReserves(realTokenReserves);
  return {
    signature: String(pick(row, ['signature', 'tx_hash', 'tx_id']) || ''),
    slot: numeric(pick(row, ['slot'])),
    block_time: blockTime,
    token_ca: token,
    curve_pda: String(pick(row, ['curve_pda', 'bonding_curve', 'bonding_curve_account']) || ''),
    side: normalizeSide(row),
    user: String(pick(row, ['user', 'trader', 'wallet', 'buyer', 'seller']) || ''),
    token_amount: tokenAmount,
    sol_amount: solAmount ?? 0,
    price_sol: priceSol,
    reserve_price_sol: reservePriceSol,
    progress_pct: progressPct,
    virtual_sol_reserves: virtualSolReserves,
    virtual_token_reserves: virtualTokenReserves,
    real_token_reserves: realTokenReserves,
    progress_decode_status: progressPct == null
      ? 'exported_trade_event'
      : 'exported_trade_event_estimated_progress_v1',
    price_decode_status: 'exported_trade_event',
    price_feasible: true,
  };
}

function anchorKey(row = {}) {
  return `${row.token_ca || ''}:${row.anchor_ts || ''}`;
}

function selectBaselineTradeAtAnchor(trades = [], anchorTs) {
  const sorted = [...trades].sort((a, b) => a.block_time - b.block_time);
  let selected = null;
  for (const trade of sorted) {
    if (trade.block_time <= anchorTs) selected = trade;
    if (trade.block_time > anchorTs) break;
  }
  if (!selected) return null;
  return {
    baseline_price_sol_chain: selected.reserve_price_sol ?? selected.price_sol ?? null,
    baseline_source: 'export_last_pre_anchor_trade',
    baseline_trade_lag_sec: selected.block_time - anchorTs,
    baseline_post_anchor_biased: false,
    baseline_signature: selected.signature || null,
    baseline_trade_side: selected.side || null,
    baseline_progress_pct: selected.progress_pct ?? null,
    baseline_virtual_sol_reserves: selected.virtual_sol_reserves ?? null,
    baseline_virtual_token_reserves: selected.virtual_token_reserves ?? null,
    baseline_real_token_reserves: selected.real_token_reserves ?? null,
  };
}

function decodeAnchorFromTrades(anchor, tradesByToken, args) {
  const startTs = anchor.anchor_ts - args.preSec;
  const endTs = anchor.anchor_ts + args.postSec;
  const curve = derivePumpfunBondingCurvePda(anchor.token_ca);
  const trades = (tradesByToken.get(anchor.token_ca) || [])
    .filter((trade) => trade.block_time >= startTs && trade.block_time <= endTs)
    .sort((a, b) => a.block_time - b.block_time || String(a.signature || '').localeCompare(String(b.signature || '')));
  const bars = aggregateMinuteBars(trades);
  const baseline = selectBaselineTradeAtAnchor(trades, anchor.anchor_ts);
  return {
    ...anchor,
    status: 'ok',
    curve_pda: curve.pda,
    curve_bump: curve.bump,
    start_ts: startTs,
    end_ts: endTs,
    signatures_fetched: 0,
    transactions_fetched: 0,
    signatures_skipped_after_end: 0,
    signatures_skipped_before_start: 0,
    transactions_in_window: trades.length,
    baseline_scan_mode: 'exported_trade_events',
    baseline_last_pre_anchor_found: Boolean(baseline),
    baseline_first_post_anchor_found: null,
    oldest_block_time: trades[0]?.block_time ?? null,
    newest_block_time: trades.at(-1)?.block_time ?? null,
    history_reached_start: args.assumeCompleteWindow,
    oldest_lag_sec: trades[0] ? trades[0].block_time - anchor.anchor_ts : null,
    trades_n: trades.length,
    bars_n: bars.length,
    first_trade_lag_sec: trades[0] ? trades[0].block_time - anchor.anchor_ts : null,
    last_trade_lag_sec: trades.at(-1) ? trades.at(-1).block_time - anchor.anchor_ts : null,
    ...(baseline || {}),
    total_sol_volume: Number(trades.reduce((sum, trade) => sum + (Number(trade.sol_amount) || 0), 0).toFixed(9)),
    buy_count: trades.filter((trade) => trade.side === 'buy').length,
    sell_count: trades.filter((trade) => trade.side === 'sell').length,
    unique_buyers: new Set(trades.filter((trade) => trade.side === 'buy' && trade.user).map((trade) => trade.user)).size,
    unique_sellers: new Set(trades.filter((trade) => trade.side === 'sell' && trade.user).map((trade) => trade.user)).size,
    exact_trade_event_n: trades.length,
    transfer_heuristic_trade_n: 0,
    infeasible_transfer_price_n: 0,
    progress_decode_status: trades.some((trade) => trade.progress_pct != null)
      ? 'exported_trade_event_estimated_progress_v1'
      : 'exported_trade_event',
    bars,
    sample_trades: trades.slice(0, 5),
  };
}

function main() {
  const args = parseArgs();
  if (args.help || !args.worklist || !args.trades || !args.out) {
    console.log(usage());
    process.exit(args.help ? 0 : 1);
  }
  const worklist = readWorklist(args.worklist);
  const trades = readRows(args.trades).map(normalizeTrade).filter(Boolean);
  const tradesByToken = new Map();
  for (const trade of trades) {
    if (!tradesByToken.has(trade.token_ca)) tradesByToken.set(trade.token_ca, []);
    tradesByToken.get(trade.token_ca).push(trade);
  }
  const results = worklist.map((anchor) => decodeAnchorFromTrades(anchor, tradesByToken, args));
  const ok = results.filter((row) => row.status === 'ok');
  const report = {
    schema_version: 'v10_curve_feature_decode_from_exported_trades.v1',
    generated_at: new Date().toISOString(),
    inputs: {
      worklist: args.worklist,
      trades: args.trades,
      rows: worklist.length,
      exported_trades_n: trades.length,
      pre_sec: args.preSec,
      post_sec: args.postSec,
      assume_complete_window: args.assumeCompleteWindow,
    },
    summary: {
      ok_n: ok.length,
      anchors_with_trades_n: ok.filter((row) => row.trades_n > 0).length,
      anchors_with_bars_n: ok.filter((row) => row.bars_n > 0).length,
      history_reached_start_n: ok.filter((row) => row.history_reached_start === true).length,
      history_incomplete_n: ok.filter((row) => row.history_reached_start === false).length,
      total_curve_trades: ok.reduce((sum, row) => sum + Number(row.trades_n || 0), 0),
      exact_trade_event_n: ok.reduce((sum, row) => sum + Number(row.exact_trade_event_n || 0), 0),
      total_sol_volume: Number(ok.reduce((sum, row) => sum + Number(row.total_sol_volume || 0), 0).toFixed(9)),
    },
    results,
  };
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({
    out: args.out,
    summary: report.summary,
  }, null, 2));
}

main();
