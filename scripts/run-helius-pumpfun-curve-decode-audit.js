#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';
import { PublicKey } from '@solana/web3.js';

import { HeliusHistoryClient } from '../src/market-data/helius-history-client.js';

const DEFAULT_PUMPFUN_PROGRAM_ID = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    tokensFile: '',
    out: './data/audits/helius-pumpfun-curve/latest.json',
    preSec: 7200,
    postSec: 7200,
    limit: 280,
    maxPages: 20,
    pageSize: 100,
    dryRun: false,
    programId: process.env.PUMPFUN_PROGRAM_ID || DEFAULT_PUMPFUN_PROGRAM_ID,
    checkpointOut: '',
    resume: false,
    progressEvery: 1,
    stopOnRateLimit: true,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--tokens-file') { args.tokensFile = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--pre-sec') { args.preSec = Number(next); i += 1; continue; }
    if (key === '--post-sec') { args.postSec = Number(next); i += 1; continue; }
    if (key === '--limit') { args.limit = Number(next); i += 1; continue; }
    if (key === '--max-pages') { args.maxPages = Number(next); i += 1; continue; }
    if (key === '--page-size') { args.pageSize = Number(next); i += 1; continue; }
    if (key === '--program-id') { args.programId = next; i += 1; continue; }
    if (key === '--checkpoint-out') { args.checkpointOut = next; i += 1; continue; }
    if (key === '--resume') { args.resume = true; continue; }
    if (key === '--progress-every') { args.progressEvery = Number(next); i += 1; continue; }
    if (key === '--continue-on-rate-limit') { args.stopOnRateLimit = false; continue; }
    if (key === '--dry-run') { args.dryRun = true; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/run-helius-pumpfun-curve-decode-audit.js --tokens-file token|decision_ts lines [options]',
    '',
    'Options:',
    '  --pre-sec <n>      Seconds before anchor to fetch, default 7200',
    '  --post-sec <n>     Seconds after anchor to fetch, default 7200',
    '  --limit <n>        Max anchors, default 280',
    '  --max-pages <n>    Max signature pages per token, default 20',
    '  --page-size <n>    Signatures per page, default 100',
    '  --checkpoint-out <path>  JSONL checkpoint path, default <out>.jsonl',
    '  --resume          Reuse rows already present in checkpoint JSONL',
    '  --progress-every <n>  Print progress every n anchors, default 1',
    '  --continue-on-rate-limit  Keep processing after Helius 429/max-usage errors',
    '  --dry-run          Derive bonding curve PDA and write planned rows without Helius calls',
    '',
    'Requires HELIUS_API_KEY or HELIUS_RPC_URL. This is an offline readonly audit and does not write production DBs.',
  ].join('\n');
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

function loadAnchorsFromTokensFile(filePath, limit = 280) {
  const lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/);
  const out = [];
  const seen = new Set();
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const [token, ts, symbol = ''] = trimmed.split(/[|,\t]/).map((part) => part.trim());
    const anchorTs = normalizeTs(ts);
    if (!token || anchorTs == null) continue;
    const key = `${token}:${anchorTs}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ token_ca: token, anchor_ts: anchorTs, symbol });
    if (out.length >= limit) break;
  }
  return out;
}

function anchorKey(row = {}) {
  return `${row.token_ca || ''}:${row.anchor_ts || row.signal_ts || ''}`;
}

function readCheckpointRows(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return [];
  return fs.readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      try { return JSON.parse(line); } catch { return null; }
    })
    .filter(Boolean);
}

function appendCheckpointRow(filePath, row) {
  if (!filePath) return;
  fs.mkdirSync(path.dirname(path.resolve(filePath)), { recursive: true });
  fs.appendFileSync(filePath, `${JSON.stringify(row)}\n`);
}

function isRateLimitError(row = {}) {
  const text = String(row.error || '').toLowerCase();
  return row.status === 'error' && (
    text.includes('http 429')
    || text.includes('max usage reached')
    || text.includes('rate limit')
    || text.includes('too many requests')
  );
}

function derivePumpfunBondingCurvePda(mint, programId = DEFAULT_PUMPFUN_PROGRAM_ID) {
  const mintKey = new PublicKey(mint);
  const programKey = new PublicKey(programId);
  const [pda, bump] = PublicKey.findProgramAddressSync([
    Buffer.from('bonding-curve'),
    mintKey.toBuffer(),
  ], programKey);
  return { pda: pda.toBase58(), bump };
}

function pickTransferAmount(transfer) {
  return Math.abs(
    numeric(
      transfer?.tokenAmount
      ?? transfer?.rawTokenAmount?.uiAmount
      ?? transfer?.rawTokenAmount?.tokenAmount
      ?? transfer?.amount
    ) ?? 0
  );
}

function pickNativeLamports(nativeTransfers = [], curvePda = null) {
  const candidates = nativeTransfers
    .map((transfer) => ({
      transfer,
      amount: Math.abs(numeric(transfer?.amount) ?? 0),
      touchesCurve: curvePda
        ? transfer?.fromUserAccount === curvePda || transfer?.toUserAccount === curvePda
        : false,
    }))
    .filter((row) => row.amount > 0)
    .sort((a, b) => Number(b.touchesCurve) - Number(a.touchesCurve) || b.amount - a.amount);
  return candidates[0] || null;
}

function inferSideAndUser({ tokenTransfer, nativeTransfer, curvePda }) {
  const tokenFrom = tokenTransfer?.fromUserAccount || null;
  const tokenTo = tokenTransfer?.toUserAccount || null;
  const nativeFrom = nativeTransfer?.fromUserAccount || null;
  const nativeTo = nativeTransfer?.toUserAccount || null;
  if (tokenTo && nativeFrom && tokenTo === nativeFrom) {
    return { side: 'buy', user: tokenTo };
  }
  if (tokenFrom && nativeTo && tokenFrom === nativeTo) {
    return { side: 'sell', user: tokenFrom };
  }
  if (curvePda && tokenTo && tokenFrom === curvePda) {
    return { side: 'buy', user: tokenTo };
  }
  if (curvePda && tokenFrom && tokenTo === curvePda) {
    return { side: 'sell', user: tokenFrom };
  }
  return { side: 'unknown', user: tokenTo || tokenFrom || nativeFrom || nativeTo || null };
}

function normalizeCurveTransaction(tx = {}, { tokenCa, curvePda } = {}) {
  const signature = tx.signature || null;
  const blockTime = normalizeTs(tx.timestamp ?? tx.blockTime);
  if (!signature || blockTime == null || tx.transactionError || tx.meta?.err) return null;
  const tokenTransfers = Array.isArray(tx.tokenTransfers) ? tx.tokenTransfers : [];
  const nativeTransfers = Array.isArray(tx.nativeTransfers) ? tx.nativeTransfers : [];
  const tokenTransfer = tokenTransfers
    .filter((transfer) => transfer?.mint === tokenCa)
    .map((transfer) => ({ transfer, amount: pickTransferAmount(transfer) }))
    .filter((row) => row.amount > 0)
    .sort((a, b) => b.amount - a.amount)[0] || null;
  const native = pickNativeLamports(nativeTransfers, curvePda);
  if (!tokenTransfer || !native) return null;
  const tokenAmount = tokenTransfer.amount;
  const solAmount = native.amount / 1e9;
  const priceSol = tokenAmount > 0 ? solAmount / tokenAmount : null;
  if (!Number.isFinite(priceSol) || priceSol <= 0) return null;
  const sideUser = inferSideAndUser({
    tokenTransfer: tokenTransfer.transfer,
    nativeTransfer: native.transfer,
    curvePda,
  });
  return {
    signature,
    slot: numeric(tx.slot),
    block_time: blockTime,
    token_ca: tokenCa,
    curve_pda: curvePda,
    side: sideUser.side,
    user: sideUser.user,
    token_amount: tokenAmount,
    sol_amount: solAmount,
    price_sol: priceSol,
    progress_pct: null,
    virtual_sol_reserves: null,
    virtual_token_reserves: null,
    progress_decode_status: 'not_decoded_v1_transfer_heuristic',
  };
}

function aggregateMinuteBars(trades = []) {
  const buckets = new Map();
  for (const trade of trades) {
    const minuteTs = Math.floor(trade.block_time / 60) * 60;
    const existing = buckets.get(minuteTs);
    if (!existing) {
      buckets.set(minuteTs, {
        timestamp: minuteTs,
        open: trade.price_sol,
        high: trade.price_sol,
        low: trade.price_sol,
        close: trade.price_sol,
        sol_volume: trade.sol_amount,
        buy_count: trade.side === 'buy' ? 1 : 0,
        sell_count: trade.side === 'sell' ? 1 : 0,
        unknown_side_count: trade.side === 'unknown' ? 1 : 0,
        buy_sol_volume: trade.side === 'buy' ? trade.sol_amount : 0,
        sell_sol_volume: trade.side === 'sell' ? trade.sol_amount : 0,
        users: new Set(trade.user ? [trade.user] : []),
        buyers: new Set(trade.side === 'buy' && trade.user ? [trade.user] : []),
        sellers: new Set(trade.side === 'sell' && trade.user ? [trade.user] : []),
        first_trade_ts: trade.block_time,
        last_trade_ts: trade.block_time,
      });
    } else {
      existing.high = Math.max(existing.high, trade.price_sol);
      existing.low = Math.min(existing.low, trade.price_sol);
      existing.close = trade.price_sol;
      existing.sol_volume += trade.sol_amount;
      existing.buy_count += trade.side === 'buy' ? 1 : 0;
      existing.sell_count += trade.side === 'sell' ? 1 : 0;
      existing.unknown_side_count += trade.side === 'unknown' ? 1 : 0;
      existing.buy_sol_volume += trade.side === 'buy' ? trade.sol_amount : 0;
      existing.sell_sol_volume += trade.side === 'sell' ? trade.sol_amount : 0;
      if (trade.user) existing.users.add(trade.user);
      if (trade.side === 'buy' && trade.user) existing.buyers.add(trade.user);
      if (trade.side === 'sell' && trade.user) existing.sellers.add(trade.user);
      existing.last_trade_ts = Math.max(existing.last_trade_ts, trade.block_time);
    }
  }
  return [...buckets.values()].sort((a, b) => a.timestamp - b.timestamp).map((bar) => ({
    timestamp: bar.timestamp,
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
    sol_volume: Number(bar.sol_volume.toFixed(9)),
    buy_count: bar.buy_count,
    sell_count: bar.sell_count,
    unknown_side_count: bar.unknown_side_count,
    buy_sol_volume: Number(bar.buy_sol_volume.toFixed(9)),
    sell_sol_volume: Number(bar.sell_sol_volume.toFixed(9)),
    net_buy_sol_volume: Number((bar.buy_sol_volume - bar.sell_sol_volume).toFixed(9)),
    unique_users: bar.users.size,
    unique_buyers: bar.buyers.size,
    unique_sellers: bar.sellers.size,
    first_trade_ts: bar.first_trade_ts,
    last_trade_ts: bar.last_trade_ts,
    progress_pct: null,
    progress_decode_status: 'not_decoded_v1_transfer_heuristic',
  }));
}

async function fetchTransactionsForAnchor(client, { curvePda, startTs, endTs, maxPages, pageSize }) {
  let before = null;
  let signaturesFetched = 0;
  let transactionsFetched = 0;
  let oldestBlockTime = null;
  const transactions = [];
  for (let page = 0; page < maxPages; page += 1) {
    const pageResult = await client.fetchHistoryPage(curvePda, { before, limit: pageSize });
    const signatures = pageResult.signatures || [];
    if (!signatures.length) break;
    signaturesFetched += signatures.length;
    transactionsFetched += pageResult.transactions?.length || 0;
    transactions.push(...(pageResult.transactions || []));
    before = signatures[signatures.length - 1]?.signature || null;
    for (const sig of signatures) {
      if (sig.blockTime != null) {
        oldestBlockTime = oldestBlockTime == null ? sig.blockTime : Math.min(oldestBlockTime, sig.blockTime);
      }
    }
    if (oldestBlockTime != null && oldestBlockTime <= startTs) break;
  }
  return {
    signaturesFetched,
    transactionsFetched,
    transactions: transactions.filter((tx) => {
      const ts = normalizeTs(tx.timestamp ?? tx.blockTime);
      return ts != null && ts >= startTs && ts <= endTs;
    }),
  };
}

async function decodeAnchor(client, anchor, args) {
  let curve;
  try {
    curve = derivePumpfunBondingCurvePda(anchor.token_ca, args.programId);
  } catch (error) {
    return {
      ...anchor,
      status: 'error',
      error: `pda_derive_failed:${String(error?.message || error).slice(0, 120)}`,
    };
  }
  const startTs = anchor.anchor_ts - args.preSec;
  const endTs = anchor.anchor_ts + args.postSec;
  if (args.dryRun) {
    return {
      ...anchor,
      status: 'planned',
      curve_pda: curve.pda,
      curve_bump: curve.bump,
      start_ts: startTs,
      end_ts: endTs,
    };
  }
  try {
    const fetched = await fetchTransactionsForAnchor(client, {
      curvePda: curve.pda,
      startTs,
      endTs,
      maxPages: args.maxPages,
      pageSize: args.pageSize,
    });
    const trades = fetched.transactions
      .map((tx) => normalizeCurveTransaction(tx, { tokenCa: anchor.token_ca, curvePda: curve.pda }))
      .filter(Boolean)
      .sort((a, b) => a.block_time - b.block_time);
    const bars = aggregateMinuteBars(trades);
    return {
      ...anchor,
      status: 'ok',
      curve_pda: curve.pda,
      curve_bump: curve.bump,
      start_ts: startTs,
      end_ts: endTs,
      signatures_fetched: fetched.signaturesFetched,
      transactions_fetched: fetched.transactionsFetched,
      transactions_in_window: fetched.transactions.length,
      trades_n: trades.length,
      bars_n: bars.length,
      first_trade_lag_sec: trades[0] ? trades[0].block_time - anchor.anchor_ts : null,
      last_trade_lag_sec: trades.at(-1) ? trades.at(-1).block_time - anchor.anchor_ts : null,
      total_sol_volume: Number(trades.reduce((sum, trade) => sum + trade.sol_amount, 0).toFixed(9)),
      buy_count: trades.filter((trade) => trade.side === 'buy').length,
      sell_count: trades.filter((trade) => trade.side === 'sell').length,
      unique_buyers: new Set(trades.filter((trade) => trade.side === 'buy' && trade.user).map((trade) => trade.user)).size,
      unique_sellers: new Set(trades.filter((trade) => trade.side === 'sell' && trade.user).map((trade) => trade.user)).size,
      progress_decode_status: 'not_decoded_v1_transfer_heuristic',
      bars,
      sample_trades: trades.slice(0, 5),
    };
  } catch (error) {
    return {
      ...anchor,
      status: 'error',
      curve_pda: curve.pda,
      curve_bump: curve.bump,
      start_ts: startTs,
      end_ts: endTs,
      error: String(error?.message || error).replace(/api-key=[^&\s]+/g, 'api-key=<redacted>').slice(0, 500),
    };
  }
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.tokensFile) throw new Error('Provide --tokens-file');
  const anchors = loadAnchorsFromTokensFile(args.tokensFile, args.limit);
  const checkpointOut = args.checkpointOut || `${args.out}.jsonl`;
  if (checkpointOut && !args.resume) {
    try { fs.unlinkSync(checkpointOut); } catch {}
  }
  const checkpointRows = args.resume ? readCheckpointRows(checkpointOut) : [];
  const byKey = new Map(checkpointRows.map((row) => [anchorKey(row), row]));
  const client = new HeliusHistoryClient({
    apiKey: process.env.HELIUS_API_KEY || '',
    rpcUrl: process.env.HELIUS_RPC_URL || undefined,
    pageSize: args.pageSize,
  });
  if (!args.dryRun && !client.isEnabled()) throw new Error('HELIUS_API_KEY or HELIUS_RPC_URL is required');
  const results = [...checkpointRows];
  let processed = 0;
  for (const anchor of anchors) {
    const key = anchorKey(anchor);
    if (byKey.has(key)) {
      processed += 1;
      if (args.progressEvery > 0 && processed % args.progressEvery === 0) {
        console.error(`[helius-pumpfun] ${processed}/${anchors.length} resume_hit token=${anchor.token_ca.slice(-8)}`);
      }
      continue;
    }
    const row = await decodeAnchor(client, anchor, args);
    results.push(row);
    byKey.set(key, row);
    appendCheckpointRow(checkpointOut, row);
    processed += 1;
    if (args.progressEvery > 0 && processed % args.progressEvery === 0) {
      console.error(`[helius-pumpfun] ${processed}/${anchors.length} status=${row.status} token=${anchor.token_ca.slice(-8)} trades=${row.trades_n ?? 0} bars=${row.bars_n ?? 0}`);
    }
    if (args.stopOnRateLimit && isRateLimitError(row)) {
      console.error(`[helius-pumpfun] stopping_after_rate_limit token=${anchor.token_ca.slice(-8)} error=${String(row.error || '').slice(0, 160)}`);
      break;
    }
  }
  const ok = results.filter((row) => row.status === 'ok');
  const report = {
    schema_version: 'helius_pumpfun_curve_decode_audit.v1',
    generated_at: new Date().toISOString(),
    inputs: {
      tokens_file: args.tokensFile,
      anchors_n: anchors.length,
      pre_sec: args.preSec,
      post_sec: args.postSec,
      max_pages: args.maxPages,
      page_size: args.pageSize,
      program_id: args.programId,
      dry_run: args.dryRun,
      checkpoint_out: checkpointOut,
      resume: args.resume,
      stop_on_rate_limit: args.stopOnRateLimit,
    },
    summary: {
      ok_n: ok.length,
      error_n: results.filter((row) => row.status === 'error').length,
      planned_n: results.filter((row) => row.status === 'planned').length,
      anchors_with_trades_n: ok.filter((row) => row.trades_n > 0).length,
      anchors_with_bars_n: ok.filter((row) => row.bars_n > 0).length,
      rate_limit_error_n: results.filter(isRateLimitError).length,
      total_signatures_fetched: ok.reduce((sum, row) => sum + Number(row.signatures_fetched || 0), 0),
      total_transactions_fetched: ok.reduce((sum, row) => sum + Number(row.transactions_fetched || 0), 0),
      total_curve_trades: ok.reduce((sum, row) => sum + Number(row.trades_n || 0), 0),
      total_sol_volume: Number(ok.reduce((sum, row) => sum + Number(row.total_sol_volume || 0), 0).toFixed(9)),
      progress_decode_status: 'not_decoded_v1_transfer_heuristic',
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

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}

export {
  aggregateMinuteBars,
  derivePumpfunBondingCurvePda,
  inferSideAndUser,
  normalizeCurveTransaction,
};
