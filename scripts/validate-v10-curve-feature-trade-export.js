#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { parse as parseCsv } from 'csv-parse/sync';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    windows: '',
    trades: '',
    out: '',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--windows') { args.windows = next; i += 1; continue; }
    if (key === '--trades') { args.trades = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/validate-v10-curve-feature-trade-export.js --windows signal_windows.csv --trades pumpfun-trades.csv --out validation.json',
    '',
    'Validates exported pump.fun TradeEvent rows before using --assume-complete-window.',
    'This checks schema, token/time joins, and per-window trade counts. It cannot prove completeness unless the export source/query guarantees it.',
  ].join('\n');
}

function readTable(filePath) {
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

function normalizeWindow(row) {
  return {
    window_id: String(row.window_id || ''),
    token_ca: String(row.token_ca || '').trim(),
    signal_ts: normalizeTs(row.signal_ts),
    window_start_ts: normalizeTs(row.window_start_ts),
    window_end_ts: normalizeTs(row.window_end_ts),
    label: String(row.label || ''),
  };
}

function normalizeTrade(row) {
  const token = String(pick(row, ['token_ca', 'mint', 'token_mint', 'mint_address']) || '').trim();
  const blockTime = normalizeTs(pick(row, ['block_time', 'timestamp', 'blockTime', 'evt_block_time']));
  return {
    token_ca: token,
    block_time: blockTime,
    side: pick(row, ['side', 'trade_side', 'direction', 'is_buy', 'isBuy']),
    user: pick(row, ['user', 'trader', 'wallet', 'buyer', 'seller']),
    sol_amount: numeric(pick(row, ['sol_amount', 'solAmount', 'sol_amount_sol', 'sol'])),
    token_amount: numeric(pick(row, ['token_amount', 'tokenAmount', 'token_amount_ui'])),
    reserve_price_sol: numeric(pick(row, ['reserve_price_sol', 'reservePriceSol', 'price_sol', 'priceSol'])),
    virtual_sol_reserves: numeric(pick(row, ['virtual_sol_reserves', 'virtualSolReserves'])),
    virtual_token_reserves: numeric(pick(row, ['virtual_token_reserves', 'virtualTokenReserves'])),
    real_token_reserves: numeric(pick(row, ['real_token_reserves', 'realTokenReserves'])),
    raw: row,
  };
}

function summarize(values) {
  const clean = values.map(Number).filter(Number.isFinite).sort((a, b) => a - b);
  if (!clean.length) return { n: 0, min: null, p50: null, p90: null, max: null };
  const at = (pct) => clean[Math.min(clean.length - 1, Math.floor((clean.length - 1) * pct))];
  return {
    n: clean.length,
    min: clean[0],
    p50: at(0.5),
    p90: at(0.9),
    max: clean[clean.length - 1],
  };
}

function main() {
  const args = parseArgs();
  if (args.help || !args.windows || !args.trades || !args.out) {
    console.log(usage());
    process.exit(args.help ? 0 : 1);
  }
  const windows = readTable(args.windows).map(normalizeWindow).filter((row) => row.token_ca && row.window_start_ts != null && row.window_end_ts != null);
  const trades = readTable(args.trades).map(normalizeTrade).filter((row) => row.token_ca && row.block_time != null);
  const tradeTokens = new Set(trades.map((row) => row.token_ca));
  const windowsByToken = new Map();
  for (const window of windows) {
    if (!windowsByToken.has(window.token_ca)) windowsByToken.set(window.token_ca, []);
    windowsByToken.get(window.token_ca).push(window);
  }
  const perWindow = windows.map((window) => ({
    ...window,
    trade_count: 0,
    buy_count: 0,
    sell_count: 0,
    sol_amount_sum: 0,
    has_price: false,
    has_progress: false,
    first_trade_ts: null,
    last_trade_ts: null,
  }));
  const perWindowById = new Map(perWindow.map((row) => [row.window_id, row]));
  const outOfWindowTrades = [];
  const missingRequired = {
    side: 0,
    sol_amount: 0,
    token_amount: 0,
    price_or_reserves: 0,
    progress_or_reserves: 0,
  };
  for (const trade of trades) {
    if (trade.side == null || trade.side === '') missingRequired.side += 1;
    if (trade.sol_amount == null) missingRequired.sol_amount += 1;
    if (trade.token_amount == null) missingRequired.token_amount += 1;
    if (trade.reserve_price_sol == null && !(trade.virtual_sol_reserves != null && trade.virtual_token_reserves != null) && trade.real_token_reserves == null) {
      missingRequired.price_or_reserves += 1;
    }
    if (trade.real_token_reserves == null) {
      missingRequired.progress_or_reserves += 1;
    }
    const matches = (windowsByToken.get(trade.token_ca) || []).filter((window) => (
      trade.block_time >= window.window_start_ts && trade.block_time <= window.window_end_ts
    ));
    if (!matches.length) {
      outOfWindowTrades.push(trade);
      continue;
    }
    for (const window of matches) {
      const row = perWindowById.get(window.window_id);
      row.trade_count += 1;
      const side = String(trade.side || '').toLowerCase();
      if (side === 'buy' || String(trade.side).toLowerCase() === 'true' || Number(trade.side) === 1) row.buy_count += 1;
      if (side === 'sell' || String(trade.side).toLowerCase() === 'false' || Number(trade.side) === 0) row.sell_count += 1;
      row.sol_amount_sum += Number(trade.sol_amount || 0);
      row.has_price = row.has_price || trade.reserve_price_sol != null || (trade.virtual_sol_reserves != null && trade.virtual_token_reserves != null) || trade.real_token_reserves != null;
      row.has_progress = row.has_progress || trade.real_token_reserves != null;
      row.first_trade_ts = row.first_trade_ts == null ? trade.block_time : Math.min(row.first_trade_ts, trade.block_time);
      row.last_trade_ts = row.last_trade_ts == null ? trade.block_time : Math.max(row.last_trade_ts, trade.block_time);
    }
  }
  for (const row of perWindow) {
    row.sol_amount_sum = Number(row.sol_amount_sum.toFixed(9));
  }
  const windowsWithTrades = perWindow.filter((row) => row.trade_count > 0);
  const report = {
    schema_version: 'v10_curve_feature_trade_export_validation.v1',
    generated_at: new Date().toISOString(),
    inputs: {
      windows: args.windows,
      trades: args.trades,
    },
    summary: {
      windows_n: windows.length,
      window_tokens_n: new Set(windows.map((row) => row.token_ca)).size,
      trades_n: trades.length,
      trade_tokens_n: tradeTokens.size,
      windows_with_trades_n: windowsWithTrades.length,
      windows_without_trades_n: perWindow.length - windowsWithTrades.length,
      out_of_window_trades_n: outOfWindowTrades.length,
      trade_count_by_window: summarize(perWindow.map((row) => row.trade_count)),
      sol_amount_by_window: summarize(perWindow.map((row) => row.sol_amount_sum)),
      missing_required_fields: missingRequired,
      warning: 'This validator checks schema and joins. It cannot prove no-trade windows are complete unless the export query/source guarantees complete window coverage.',
    },
    windows_without_trades: perWindow.filter((row) => row.trade_count === 0).slice(0, 200),
    out_of_window_trades_sample: outOfWindowTrades.slice(0, 50).map((row) => ({
      token_ca: row.token_ca,
      block_time: row.block_time,
    })),
    per_window: perWindow,
  };
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({
    out: args.out,
    summary: report.summary,
  }, null, 2));
}

main();
