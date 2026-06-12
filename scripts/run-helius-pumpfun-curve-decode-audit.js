#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';
import { pathToFileURL } from 'url';
import { PublicKey } from '@solana/web3.js';

import { HeliusHistoryClient } from '../src/market-data/helius-history-client.js';
import { fetchWithRetry } from '../src/utils/fetch-with-retry.js';

const DEFAULT_PUMPFUN_PROGRAM_ID = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const PUMPFUN_TOKEN_DECIMALS = 6;
const PUMPFUN_INITIAL_REAL_TOKEN_RESERVES = Number(process.env.PUMPFUN_INITIAL_REAL_TOKEN_RESERVES || 793_100_000);
const TRADE_EVENT_DISCRIMINATOR = createHash('sha256').update('event:TradeEvent').digest().subarray(0, 8);

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    tokensFile: '',
    out: './data/audits/helius-pumpfun-curve/latest.json',
    preSec: 7200,
    postSec: 7200,
    limit: 0,
    maxPages: 20,
    pageSize: 100,
    dryRun: false,
    programId: process.env.PUMPFUN_PROGRAM_ID || DEFAULT_PUMPFUN_PROGRAM_ID,
    checkpointOut: '',
    transactionsJson: '',
    rpcUrl: process.env.SOLANA_RPC_URL || process.env.ALCHEMY_RPC_URL || '',
    rpcUrlFile: '',
    rpcMode: 'auto',
    rpcTxDelayMs: 0,
    anchorDelayMs: 0,
    perTokenTimeoutMs: 0,
    maxFeasiblePriceSol: 0,
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
    if (key === '--transactions-json') { args.transactionsJson = next; i += 1; continue; }
    if (key === '--rpc-url') { args.rpcUrl = next; i += 1; continue; }
    if (key === '--rpc-url-file') { args.rpcUrlFile = next; i += 1; continue; }
    if (key === '--rpc-mode') { args.rpcMode = next; i += 1; continue; }
    if (key === '--rpc-tx-delay-ms') { args.rpcTxDelayMs = Number(next); i += 1; continue; }
    if (key === '--anchor-delay-ms') { args.anchorDelayMs = Number(next); i += 1; continue; }
    if (key === '--per-token-timeout-ms') { args.perTokenTimeoutMs = Number(next); i += 1; continue; }
    if (key === '--max-feasible-price-sol') { args.maxFeasiblePriceSol = Number(next); i += 1; continue; }
    if (key === '--resume') { args.resume = true; continue; }
    if (key === '--progress-every') { args.progressEvery = Number(next); i += 1; continue; }
    if (key === '--continue-on-rate-limit') { args.stopOnRateLimit = false; continue; }
    if (key === '--dry-run') { args.dryRun = true; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function readSecretFile(filePath) {
  if (!filePath) return '';
  const raw = fs.readFileSync(filePath, 'utf8').replace(/\r?\n/g, '').trim();
  if (!raw) return '';
  if (raw.includes('=')) return raw.slice(raw.indexOf('=') + 1).trim();
  return raw;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/run-helius-pumpfun-curve-decode-audit.js --tokens-file token|decision_ts lines [options]',
    '',
    'Options:',
    '  --pre-sec <n>      Seconds before anchor to fetch, default 7200',
    '  --post-sec <n>     Seconds after anchor to fetch, default 7200',
    '  --limit <n>        Max anchors, default 0 (all rows)',
    '  --max-pages <n>    Max signature pages per token, default 20',
    '  --page-size <n>    Signatures per page, default 100',
    '  --checkpoint-out <path>  JSONL checkpoint path, default <out>.jsonl',
    '  --transactions-json <path>  Decode an exported raw/enhanced transaction JSON instead of fetching',
    '  --rpc-url <url>    Generic Solana RPC URL for raw getTransaction mode (Alchemy/Helius/etc.)',
    '  --rpc-url-file <path>  Read generic Solana RPC URL from a local secret file without exposing it in ps',
    '  --rpc-mode <auto|raw|enhanced>  History fetch mode, default auto',
    '  --rpc-tx-delay-ms <n>  Delay between raw getTransaction calls, default 0',
    '  --anchor-delay-ms <n>  Delay after each newly processed anchor, default 0',
    '  --per-token-timeout-ms <n>  Wall-clock timeout per anchor, default 0 (disabled)',
    '  --max-feasible-price-sol <n>  Optional heuristic price feasibility ceiling for transfer-derived prices',
    '  --resume          Reuse rows already present in checkpoint JSONL',
    '  --progress-every <n>  Print progress every n anchors, default 1',
    '  --continue-on-rate-limit  Keep processing after Helius 429/max-usage errors',
    '  --dry-run          Derive bonding curve PDA and write planned rows without Helius calls',
    '',
    'Requires HELIUS_API_KEY/HELIUS_RPC_URL, --rpc-url, --rpc-url-file, or --transactions-json. This is an offline readonly audit and does not write production DBs.',
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

function sleep(ms) {
  return ms > 0 ? new Promise((resolve) => setTimeout(resolve, ms)) : Promise.resolve();
}

export function loadAnchorsFromTokensFile(filePath, limit = 0) {
  const lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/);
  const out = [];
  const seen = new Set();
  const maxRows = Number(limit || 0);
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
    if (maxRows > 0 && out.length >= maxRows) break;
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

function readU64LE(buffer, offset) {
  if (offset + 8 > buffer.length) return null;
  return buffer.readBigUInt64LE(offset);
}

function readI64LE(buffer, offset) {
  if (offset + 8 > buffer.length) return null;
  return buffer.readBigInt64LE(offset);
}

function readPubkey(buffer, offset) {
  if (offset + 32 > buffer.length) return null;
  return new PublicKey(buffer.subarray(offset, offset + 32)).toBase58();
}

function u64ToNumber(value) {
  if (value == null) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function estimatePumpfunProgressPctFromRealTokenReserves(realTokenReserves) {
  const real = numeric(realTokenReserves);
  const initial = numeric(PUMPFUN_INITIAL_REAL_TOKEN_RESERVES);
  if (real == null || initial == null || initial <= 0) return null;
  const pct = 100 - ((real * 100) / initial);
  return Math.max(0, Math.min(100, pct));
}

function decodePumpfunTradeEventPayload(payload) {
  const buffer = Buffer.isBuffer(payload) ? payload : Buffer.from(payload || []);
  if (buffer.length < 8 + 32 + 8 + 8 + 1 + 32 + 8 + 8 + 8) return null;
  if (!buffer.subarray(0, 8).equals(TRADE_EVENT_DISCRIMINATOR)) return null;
  let offset = 8;
  const mint = readPubkey(buffer, offset); offset += 32;
  const solAmountRaw = readU64LE(buffer, offset); offset += 8;
  const tokenAmountRaw = readU64LE(buffer, offset); offset += 8;
  const isBuy = Boolean(buffer[offset]); offset += 1;
  const user = readPubkey(buffer, offset); offset += 32;
  const timestampRaw = readI64LE(buffer, offset); offset += 8;
  const virtualSolRaw = readU64LE(buffer, offset); offset += 8;
  const virtualTokenRaw = readU64LE(buffer, offset); offset += 8;
  const realSolRaw = readU64LE(buffer, offset); offset += 8;
  const realTokenRaw = readU64LE(buffer, offset); offset += 8;

  let feeRecipient = null;
  let feeBasisPointsRaw = null;
  let feeRaw = null;
  let creator = null;
  let creatorFeeBasisPointsRaw = null;
  let creatorFeeRaw = null;
  if (offset + 32 <= buffer.length) {
    feeRecipient = readPubkey(buffer, offset); offset += 32;
  }
  if (offset + 8 <= buffer.length) {
    feeBasisPointsRaw = readU64LE(buffer, offset); offset += 8;
  }
  if (offset + 8 <= buffer.length) {
    feeRaw = readU64LE(buffer, offset); offset += 8;
  }
  if (offset + 32 <= buffer.length) {
    creator = readPubkey(buffer, offset); offset += 32;
  }
  if (offset + 8 <= buffer.length) {
    creatorFeeBasisPointsRaw = readU64LE(buffer, offset); offset += 8;
  }
  if (offset + 8 <= buffer.length) {
    creatorFeeRaw = readU64LE(buffer, offset); offset += 8;
  }

  const solLamports = u64ToNumber(solAmountRaw);
  const tokenRaw = u64ToNumber(tokenAmountRaw);
  const tokenAmount = tokenRaw == null ? null : tokenRaw / (10 ** PUMPFUN_TOKEN_DECIMALS);
  const solAmount = solLamports == null ? null : solLamports / 1e9;
  const virtualSolReserves = u64ToNumber(virtualSolRaw) == null ? null : u64ToNumber(virtualSolRaw) / 1e9;
  const virtualTokenReserves = u64ToNumber(virtualTokenRaw) == null ? null : u64ToNumber(virtualTokenRaw) / (10 ** PUMPFUN_TOKEN_DECIMALS);
  const realTokenReserves = u64ToNumber(realTokenRaw) == null ? null : u64ToNumber(realTokenRaw) / (10 ** PUMPFUN_TOKEN_DECIMALS);
  const reservePriceSol = virtualSolReserves && virtualTokenReserves ? virtualSolReserves / virtualTokenReserves : null;
  return {
    mint,
    sol_amount: solAmount,
    token_amount: tokenAmount,
    price_sol: solAmount != null && tokenAmount > 0 ? solAmount / tokenAmount : null,
    side: isBuy ? 'buy' : 'sell',
    user,
    timestamp: timestampRaw == null ? null : Number(timestampRaw),
    virtual_sol_reserves: virtualSolReserves,
    virtual_token_reserves: virtualTokenReserves,
    real_sol_reserves: u64ToNumber(realSolRaw) == null ? null : u64ToNumber(realSolRaw) / 1e9,
    real_token_reserves: realTokenReserves,
    reserve_price_sol: reservePriceSol,
    progress_pct: estimatePumpfunProgressPctFromRealTokenReserves(realTokenReserves),
    fee_recipient: feeRecipient,
    fee_basis_points: u64ToNumber(feeBasisPointsRaw),
    fee_sol: u64ToNumber(feeRaw) == null ? null : u64ToNumber(feeRaw) / 1e9,
    creator,
    creator_fee_basis_points: u64ToNumber(creatorFeeBasisPointsRaw),
    creator_fee_sol: u64ToNumber(creatorFeeRaw) == null ? null : u64ToNumber(creatorFeeRaw) / 1e9,
    raw_event_bytes: buffer.length,
  };
}

function decodePumpfunTradeEventsFromLogs(logMessages = [], { tokenCa } = {}) {
  const events = [];
  for (const line of Array.isArray(logMessages) ? logMessages : []) {
    const match = String(line).match(/Program data:\s*([A-Za-z0-9+/=]+)/);
    if (!match) continue;
    let payload;
    try {
      payload = Buffer.from(match[1], 'base64');
    } catch {
      continue;
    }
    const event = decodePumpfunTradeEventPayload(payload);
    if (!event) continue;
    if (tokenCa && event.mint !== tokenCa) continue;
    events.push(event);
  }
  return events;
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

function normalizeRawRpcTransaction(tx = {}) {
  const result = tx.result || tx;
  if (!result) return null;
  return {
    signature: result.transaction?.signatures?.[0] || result.signature || tx.signature || null,
    timestamp: result.blockTime ?? result.timestamp ?? tx.blockTime ?? tx.timestamp,
    blockTime: result.blockTime ?? result.timestamp ?? tx.blockTime ?? tx.timestamp,
    slot: result.slot ?? tx.slot,
    meta: result.meta || tx.meta || {},
    transactionError: result.meta?.err || tx.transactionError || null,
    tokenTransfers: result.tokenTransfers || tx.tokenTransfers || [],
    nativeTransfers: result.nativeTransfers || tx.nativeTransfers || [],
  };
}

function normalizeCurveTransaction(tx = {}, { tokenCa, curvePda, maxFeasiblePriceSol = 0 } = {}) {
  const rawTx = normalizeRawRpcTransaction(tx) || tx;
  const signature = rawTx.signature || null;
  const blockTime = normalizeTs(rawTx.timestamp ?? rawTx.blockTime);
  const exactEvents = decodePumpfunTradeEventsFromLogs(rawTx.meta?.logMessages || rawTx.logMessages || [], { tokenCa });
  if (signature && exactEvents.length) {
    const event = exactEvents[0];
    const eventBlockTime = normalizeTs(event.timestamp) ?? blockTime;
    if (eventBlockTime == null || rawTx.transactionError || rawTx.meta?.err) return null;
    return {
      signature,
      slot: numeric(rawTx.slot),
      block_time: eventBlockTime,
      token_ca: tokenCa,
      curve_pda: curvePda,
      side: event.side,
      user: event.user,
      token_amount: event.token_amount,
      sol_amount: event.sol_amount,
      price_sol: event.price_sol,
      reserve_price_sol: event.reserve_price_sol,
      progress_pct: event.progress_pct,
      virtual_sol_reserves: event.virtual_sol_reserves,
      virtual_token_reserves: event.virtual_token_reserves,
      real_sol_reserves: event.real_sol_reserves,
      real_token_reserves: event.real_token_reserves,
      progress_decode_status: event.progress_pct == null
        ? 'exact_trade_event_reserves_decoded'
        : 'exact_trade_event_reserves_decoded_estimated_progress_v1',
      price_decode_status: 'exact_trade_event',
      price_feasible: true,
      raw_event_bytes: event.raw_event_bytes,
      fee_sol: event.fee_sol,
      creator_fee_sol: event.creator_fee_sol,
    };
  }
  if (!signature || blockTime == null || rawTx.transactionError || rawTx.meta?.err) return null;
  const tokenTransfers = Array.isArray(rawTx.tokenTransfers) ? rawTx.tokenTransfers : [];
  const nativeTransfers = Array.isArray(rawTx.nativeTransfers) ? rawTx.nativeTransfers : [];
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
    slot: numeric(rawTx.slot),
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
    price_decode_status: 'transfer_heuristic',
    price_feasible: maxFeasiblePriceSol > 0 ? priceSol <= maxFeasiblePriceSol : null,
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
        exact_trade_count: trade.price_decode_status === 'exact_trade_event' ? 1 : 0,
        heuristic_trade_count: trade.price_decode_status !== 'exact_trade_event' ? 1 : 0,
        last_virtual_sol_reserves: trade.virtual_sol_reserves ?? null,
        last_virtual_token_reserves: trade.virtual_token_reserves ?? null,
        last_reserve_price_sol: trade.reserve_price_sol ?? null,
        last_progress_pct: trade.progress_pct ?? null,
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
      existing.exact_trade_count += trade.price_decode_status === 'exact_trade_event' ? 1 : 0;
      existing.heuristic_trade_count += trade.price_decode_status !== 'exact_trade_event' ? 1 : 0;
      if (trade.virtual_sol_reserves != null) existing.last_virtual_sol_reserves = trade.virtual_sol_reserves;
      if (trade.virtual_token_reserves != null) existing.last_virtual_token_reserves = trade.virtual_token_reserves;
      if (trade.reserve_price_sol != null) existing.last_reserve_price_sol = trade.reserve_price_sol;
      if (trade.progress_pct != null) existing.last_progress_pct = trade.progress_pct;
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
    exact_trade_count: bar.exact_trade_count,
    heuristic_trade_count: bar.heuristic_trade_count,
    last_virtual_sol_reserves: bar.last_virtual_sol_reserves,
    last_virtual_token_reserves: bar.last_virtual_token_reserves,
    last_reserve_price_sol: bar.last_reserve_price_sol,
    progress_pct: bar.last_progress_pct ?? null,
    progress_decode_status: bar.exact_trade_count > 0
      ? (bar.last_progress_pct == null
          ? 'exact_trade_event_reserves_decoded'
          : 'exact_trade_event_reserves_decoded_estimated_progress_v1')
      : 'not_decoded_v1_transfer_heuristic',
  }));
}

export function selectBaselineTradeAtAnchor(trades = [], anchorTs) {
  const anchor = numeric(anchorTs);
  if (anchor == null) return null;
  const sorted = [...trades]
    .filter((trade) => trade && numeric(trade.block_time) != null)
    .sort((a, b) => a.block_time - b.block_time);
  let selected = null;
  for (const trade of sorted) {
    if (trade.block_time <= anchor) selected = trade;
    if (trade.block_time > anchor) break;
  }
  const source = selected ? 'chain_truth_last_pre_anchor_trade' : 'chain_truth_first_post_anchor_trade';
  if (!selected) selected = sorted.find((trade) => trade.block_time > anchor) || null;
  if (!selected) return null;
  return {
    baseline_price_sol_chain: selected.reserve_price_sol ?? selected.price_sol ?? null,
    baseline_source: source,
    baseline_trade_lag_sec: selected.block_time - anchor,
    baseline_post_anchor_biased: source === 'chain_truth_first_post_anchor_trade',
    baseline_signature: selected.signature ?? null,
    baseline_trade_side: selected.side ?? null,
    baseline_progress_pct: selected.progress_pct ?? null,
    baseline_virtual_sol_reserves: selected.virtual_sol_reserves ?? null,
    baseline_virtual_token_reserves: selected.virtual_token_reserves ?? null,
    baseline_real_sol_reserves: selected.real_sol_reserves ?? null,
    baseline_real_token_reserves: selected.real_token_reserves ?? null,
  };
}

class RawRpcHistoryClient {
  constructor(config = {}) {
    this.rpcUrl = config.rpcUrl || '';
    this.pageSize = Number(config.pageSize || 100);
    this.rpcTxDelayMs = Number(config.rpcTxDelayMs || 0);
  }

  isEnabled() {
    return Boolean(this.rpcUrl);
  }

  async #postRpc(method, params, { signal } = {}) {
    const response = await fetchWithRetry(this.rpcUrl, {
      source: 'SOLANA_RPC',
      method: 'POST',
      timeout: 30000,
      maxRetries: 3,
      initialDelay: 1500,
      headers: { 'content-type': 'application/json' },
      body: { jsonrpc: '2.0', id: method, method, params },
      silent: true,
      signal,
    });
    if (response?.error) throw new Error(response.error?.message || JSON.stringify(response.error));
    if (response?.result === undefined) throw new Error(`rpc_invalid_${method}`);
    return response.result;
  }

  async getSignaturesForAddress(address, { before, limit = this.pageSize, signal } = {}) {
    const params = [address, {
      limit,
      ...(before ? { before } : {}),
    }];
    return this.#postRpc('getSignaturesForAddress', params, { signal });
  }

  async getTransaction(signature, { signal } = {}) {
    await sleep(this.rpcTxDelayMs);
    const result = await this.#postRpc('getTransaction', [
      signature,
      {
        encoding: 'jsonParsed',
        commitment: 'confirmed',
        maxSupportedTransactionVersion: 0,
      },
    ], { signal });
    return result ? { ...result, signature } : null;
  }

  async fetchHistoryPage(address, options = {}) {
    const signatures = await this.getSignaturesForAddress(address, options);
    if (!signatures.length) return { signatures: [], transactions: [] };
    const filtered = filterSignaturesForWindow(signatures, {
      startTs: options.startTs,
      endTs: options.endTs,
    });
    const transactions = [];
    for (const sig of filtered.fetchable) {
      if (!sig?.signature) continue;
      const tx = await this.getTransaction(sig.signature, { signal: options.signal });
      if (tx) transactions.push(tx);
    }
    return {
      signatures,
      transactions,
      signaturesSkippedAfterEnd: filtered.skippedAfterEnd,
      signaturesSkippedBeforeStart: filtered.skippedBeforeStart,
      historyReachedStart: filtered.reachedStart,
    };
  }
}

export function filterSignaturesForWindow(signatures = [], { startTs, endTs } = {}) {
  const start = numeric(startTs);
  const end = numeric(endTs);
  const fetchable = [];
  let skippedAfterEnd = 0;
  let skippedBeforeStart = 0;
  let reachedStart = false;
  for (let i = 0; i < signatures.length; i += 1) {
    const sig = signatures[i];
    const blockTime = normalizeTs(sig?.blockTime);
    if (blockTime != null && end != null && blockTime > end) {
      skippedAfterEnd += 1;
      continue;
    }
    if (blockTime != null && start != null && blockTime < start) {
      skippedBeforeStart += signatures.length - i;
      reachedStart = true;
      break;
    }
    fetchable.push(sig);
  }
  return {
    fetchable,
    skippedAfterEnd,
    skippedBeforeStart,
    reachedStart,
  };
}

function loadTransactionsJson(filePath) {
  if (!filePath) return [];
  const raw = fs.readFileSync(filePath, 'utf8').trim();
  if (!raw) return [];
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return raw.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
  }
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed.transactions)) return parsed.transactions;
  if (parsed.result) return [parsed.result];
  if (Array.isArray(parsed.results)) {
    return parsed.results.flatMap((row) => (
      Array.isArray(row.transactions) ? row.transactions
        : Array.isArray(row.raw_transactions) ? row.raw_transactions
          : row.result ? [row.result]
            : []
    ));
  }
  return [parsed];
}

async function fetchTransactionsForAnchor(client, { curvePda, startTs, endTs, maxPages, pageSize, signal }) {
  let before = null;
  let signaturesFetched = 0;
  let transactionsFetched = 0;
  let signaturesSkippedAfterEnd = 0;
  let signaturesSkippedBeforeStart = 0;
  let oldestBlockTime = null;
  const transactions = [];
  for (let page = 0; page < maxPages; page += 1) {
    const pageResult = await client.fetchHistoryPage(curvePda, { before, limit: pageSize, startTs, endTs, signal });
    const signatures = pageResult.signatures || [];
    if (!signatures.length) break;
    signaturesFetched += signatures.length;
    transactionsFetched += pageResult.transactions?.length || 0;
    signaturesSkippedAfterEnd += Number(pageResult.signaturesSkippedAfterEnd || 0);
    signaturesSkippedBeforeStart += Number(pageResult.signaturesSkippedBeforeStart || 0);
    transactions.push(...(pageResult.transactions || []));
    before = signatures[signatures.length - 1]?.signature || null;
    for (const sig of signatures) {
      if (sig.blockTime != null) {
        oldestBlockTime = oldestBlockTime == null ? sig.blockTime : Math.min(oldestBlockTime, sig.blockTime);
      }
    }
    if (pageResult.historyReachedStart || (oldestBlockTime != null && oldestBlockTime <= startTs)) break;
  }
  return {
    signaturesFetched,
    transactionsFetched,
    signaturesSkippedAfterEnd,
    signaturesSkippedBeforeStart,
    oldestBlockTime,
    newestBlockTime: null,
    historyReachedStart: oldestBlockTime != null ? oldestBlockTime <= startTs : false,
    transactions: transactions.filter((tx) => {
      const ts = normalizeTs(tx.timestamp ?? tx.blockTime);
      return ts != null && ts >= startTs && ts <= endTs;
    }),
  };
}

async function decodeAnchor(client, anchor, args, transactionsPool = null, signal = null) {
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
    const fetched = transactionsPool
      ? {
        signaturesFetched: 0,
        transactionsFetched: transactionsPool.length,
        oldestBlockTime: null,
        newestBlockTime: null,
        historyReachedStart: null,
        transactions: transactionsPool.filter((tx) => {
          const rawTx = normalizeRawRpcTransaction(tx) || tx;
          const ts = normalizeTs(rawTx.timestamp ?? rawTx.blockTime);
          return ts != null && ts >= startTs && ts <= endTs;
        }),
      }
      : await fetchTransactionsForAnchor(client, {
        curvePda: curve.pda,
        startTs,
        endTs,
        maxPages: args.maxPages,
        pageSize: args.pageSize,
        signal,
      });
    const trades = fetched.transactions
      .map((tx) => normalizeCurveTransaction(tx, {
        tokenCa: anchor.token_ca,
        curvePda: curve.pda,
        maxFeasiblePriceSol: args.maxFeasiblePriceSol,
      }))
      .filter(Boolean)
      .sort((a, b) => a.block_time - b.block_time);
    const bars = aggregateMinuteBars(trades);
    const baselineAtAnchor = selectBaselineTradeAtAnchor(trades, anchor.anchor_ts);
    return {
      ...anchor,
      status: 'ok',
      curve_pda: curve.pda,
      curve_bump: curve.bump,
      start_ts: startTs,
      end_ts: endTs,
      signatures_fetched: fetched.signaturesFetched,
      transactions_fetched: fetched.transactionsFetched,
      signatures_skipped_after_end: fetched.signaturesSkippedAfterEnd,
      signatures_skipped_before_start: fetched.signaturesSkippedBeforeStart,
      transactions_in_window: fetched.transactions.length,
      oldest_block_time: fetched.oldestBlockTime,
      history_reached_start: fetched.historyReachedStart,
      oldest_lag_sec: fetched.oldestBlockTime == null ? null : fetched.oldestBlockTime - anchor.anchor_ts,
      trades_n: trades.length,
      bars_n: bars.length,
      first_trade_lag_sec: trades[0] ? trades[0].block_time - anchor.anchor_ts : null,
      last_trade_lag_sec: trades.at(-1) ? trades.at(-1).block_time - anchor.anchor_ts : null,
      ...(baselineAtAnchor || {}),
      total_sol_volume: Number(trades.reduce((sum, trade) => sum + trade.sol_amount, 0).toFixed(9)),
      buy_count: trades.filter((trade) => trade.side === 'buy').length,
      sell_count: trades.filter((trade) => trade.side === 'sell').length,
      unique_buyers: new Set(trades.filter((trade) => trade.side === 'buy' && trade.user).map((trade) => trade.user)).size,
      unique_sellers: new Set(trades.filter((trade) => trade.side === 'sell' && trade.user).map((trade) => trade.user)).size,
      exact_trade_event_n: trades.filter((trade) => trade.price_decode_status === 'exact_trade_event').length,
      transfer_heuristic_trade_n: trades.filter((trade) => trade.price_decode_status !== 'exact_trade_event').length,
      infeasible_transfer_price_n: trades.filter((trade) => trade.price_feasible === false).length,
      progress_decode_status: trades.some((trade) => trade.progress_decode_status === 'exact_trade_event_reserves_decoded_estimated_progress_v1')
        ? 'exact_trade_event_reserves_decoded_estimated_progress_v1'
        : (trades.some((trade) => trade.progress_decode_status === 'exact_trade_event_reserves_decoded')
            ? 'exact_trade_event_reserves_decoded'
            : 'not_decoded_v1_transfer_heuristic'),
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

function timeoutRowForAnchor(anchor, args, timeoutMs) {
  let curve = {};
  try {
    curve = derivePumpfunBondingCurvePda(anchor.token_ca, args.programId);
  } catch {}
  return {
    ...anchor,
    status: 'error',
    error: `per_token_timeout_ms:${timeoutMs}`,
    curve_pda: curve.pda || null,
    curve_bump: curve.bump ?? null,
    start_ts: anchor.anchor_ts - args.preSec,
    end_ts: anchor.anchor_ts + args.postSec,
    coverage_incomplete: true,
    timeout_ms: timeoutMs,
    history_reached_start: false,
  };
}

async function decodeAnchorWithTimeout(client, anchor, args, transactionsPool = null) {
  const timeoutMs = Number(args.perTokenTimeoutMs || 0);
  if (!timeoutMs || timeoutMs <= 0 || args.dryRun || transactionsPool) {
    return decodeAnchor(client, anchor, args, transactionsPool);
  }
  const controller = new AbortController();
  let timeoutId;
  try {
    const timeoutPromise = new Promise((resolve) => {
      timeoutId = setTimeout(() => {
        controller.abort();
        resolve(timeoutRowForAnchor(anchor, args, timeoutMs));
      }, timeoutMs);
    });
    return await Promise.race([
      decodeAnchor(client, anchor, args, transactionsPool, controller.signal),
      timeoutPromise,
    ]);
  } finally {
    clearTimeout(timeoutId);
  }
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.tokensFile) throw new Error('Provide --tokens-file');
  if (args.rpcUrlFile && !args.rpcUrl) {
    args.rpcUrl = readSecretFile(args.rpcUrlFile);
  }
  const anchors = loadAnchorsFromTokensFile(args.tokensFile, args.limit);
  const checkpointOut = args.checkpointOut || `${args.out}.jsonl`;
  if (checkpointOut && !args.resume) {
    try { fs.unlinkSync(checkpointOut); } catch {}
  }
  const checkpointRows = args.resume ? readCheckpointRows(checkpointOut) : [];
  const byKey = new Map(checkpointRows.map((row) => [anchorKey(row), row]));
  const transactionsPool = args.transactionsJson ? loadTransactionsJson(args.transactionsJson) : null;
  const useRawRpc = args.rpcMode === 'raw' || (args.rpcMode === 'auto' && args.rpcUrl);
  const client = useRawRpc
    ? new RawRpcHistoryClient({ rpcUrl: args.rpcUrl, pageSize: args.pageSize, rpcTxDelayMs: args.rpcTxDelayMs })
    : new HeliusHistoryClient({
      apiKey: process.env.HELIUS_API_KEY || '',
      rpcUrl: process.env.HELIUS_RPC_URL || undefined,
      pageSize: args.pageSize,
    });
  if (!args.dryRun && !transactionsPool && !client.isEnabled()) throw new Error('HELIUS_API_KEY, HELIUS_RPC_URL, --rpc-url, --rpc-url-file, or --transactions-json is required');
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
    const row = await decodeAnchorWithTimeout(client, anchor, args, transactionsPool);
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
    await sleep(Number(args.anchorDelayMs || 0));
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
      transactions_json: args.transactionsJson || null,
      rpc_mode: transactionsPool ? 'transactions_json' : (useRawRpc ? 'raw_rpc' : 'helius_enhanced'),
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
      per_token_timeout_n: results.filter((row) => String(row.error || '').startsWith('per_token_timeout_ms:')).length,
      total_signatures_fetched: ok.reduce((sum, row) => sum + Number(row.signatures_fetched || 0), 0),
      total_transactions_fetched: ok.reduce((sum, row) => sum + Number(row.transactions_fetched || 0), 0),
      total_signatures_skipped_after_end: ok.reduce((sum, row) => sum + Number(row.signatures_skipped_after_end || 0), 0),
      total_signatures_skipped_before_start: ok.reduce((sum, row) => sum + Number(row.signatures_skipped_before_start || 0), 0),
      total_curve_trades: ok.reduce((sum, row) => sum + Number(row.trades_n || 0), 0),
      exact_trade_event_n: ok.reduce((sum, row) => sum + Number(row.exact_trade_event_n || 0), 0),
      transfer_heuristic_trade_n: ok.reduce((sum, row) => sum + Number(row.transfer_heuristic_trade_n || 0), 0),
      infeasible_transfer_price_n: ok.reduce((sum, row) => sum + Number(row.infeasible_transfer_price_n || 0), 0),
      history_reached_start_n: ok.filter((row) => row.history_reached_start === true).length,
      history_incomplete_n: ok.filter((row) => row.history_reached_start === false).length,
      total_sol_volume: Number(ok.reduce((sum, row) => sum + Number(row.total_sol_volume || 0), 0).toFixed(9)),
      progress_decode_status: ok.some((row) => row.progress_decode_status === 'exact_trade_event_reserves_decoded')
        ? 'exact_trade_event_reserves_decoded'
        : 'not_decoded_v1_transfer_heuristic',
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
  decodePumpfunTradeEventPayload,
  decodePumpfunTradeEventsFromLogs,
  derivePumpfunBondingCurvePda,
  inferSideAndUser,
  normalizeCurveTransaction,
  normalizeRawRpcTransaction,
};
