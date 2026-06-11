import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { PublicKey } from '@solana/web3.js';

import {
  aggregateMinuteBars,
  decodePumpfunTradeEventPayload,
  decodePumpfunTradeEventsFromLogs,
  derivePumpfunBondingCurvePda,
  estimatePumpfunProgressPctFromRealTokenReserves,
  filterSignaturesForWindow,
  inferSideAndUser,
  normalizeCurveTransaction,
  selectBaselineTradeAtAnchor,
} from '../scripts/run-helius-pumpfun-curve-decode-audit.js';

const tokenCa = '6eEQtGNoQ7VaPFy3iUZBxNHU5LfvYZRep6umdbfpump';
const user = '11111111111111111111111111111112';

function u64(value) {
  const buf = Buffer.alloc(8);
  buf.writeBigUInt64LE(BigInt(value));
  return buf;
}

function i64(value) {
  const buf = Buffer.alloc(8);
  buf.writeBigInt64LE(BigInt(value));
  return buf;
}

function pubkey(value) {
  return new PublicKey(value).toBuffer();
}

function tradeEventPayload({
  mint = tokenCa,
  solLamports = 1_000_000_000,
  tokenRaw = 2_000_000_000,
  isBuy = true,
  userKey = user,
  timestamp = 1_000,
  virtualSolLamports = 31_000_000_000,
  virtualTokenRaw = 1_073_000_000_000_000,
  realSolLamports = 1_000_000_000,
  realTokenRaw = 793_100_000_000_000,
} = {}) {
  const discriminator = createHash('sha256').update('event:TradeEvent').digest().subarray(0, 8);
  return Buffer.concat([
    discriminator,
    pubkey(mint),
    u64(solLamports),
    u64(tokenRaw),
    Buffer.from([isBuy ? 1 : 0]),
    pubkey(userKey),
    i64(timestamp),
    u64(virtualSolLamports),
    u64(virtualTokenRaw),
    u64(realSolLamports),
    u64(realTokenRaw),
    pubkey(userKey),
    u64(100),
    u64(10_000),
    pubkey(userKey),
    u64(50),
    u64(5_000),
  ]);
}

test('derives deterministic pump.fun bonding curve PDA', () => {
  const first = derivePumpfunBondingCurvePda(tokenCa);
  const second = derivePumpfunBondingCurvePda(tokenCa);

  assert.equal(first.pda, second.pda);
  assert.equal(typeof first.bump, 'number');
  assert.equal(first.pda.length > 30, true);
});

test('infers buy side when token recipient paid native SOL', () => {
  const side = inferSideAndUser({
    tokenTransfer: { toUserAccount: 'buyer-wallet', fromUserAccount: 'curve' },
    nativeTransfer: { fromUserAccount: 'buyer-wallet', toUserAccount: 'curve' },
    curvePda: 'curve',
  });

  assert.deepEqual(side, { side: 'buy', user: 'buyer-wallet' });
});

test('normalizes Helius enhanced transaction into curve trade', () => {
  const trade = normalizeCurveTransaction({
    signature: 'sig-1',
    timestamp: 1_000,
    slot: 5,
    tokenTransfers: [
      { mint: tokenCa, tokenAmount: 2_000, fromUserAccount: 'curve-pda', toUserAccount: 'buyer' },
    ],
    nativeTransfers: [
      { amount: 1_000_000_000, fromUserAccount: 'buyer', toUserAccount: 'curve-pda' },
    ],
  }, { tokenCa, curvePda: 'curve-pda' });

  assert.equal(trade.side, 'buy');
  assert.equal(trade.user, 'buyer');
  assert.equal(trade.sol_amount, 1);
  assert.equal(trade.token_amount, 2_000);
  assert.equal(trade.price_sol, 0.0005);
  assert.equal(trade.progress_decode_status, 'not_decoded_v1_transfer_heuristic');
});

test('decodes exact pump.fun TradeEvent payload', () => {
  const event = decodePumpfunTradeEventPayload(tradeEventPayload());

  assert.equal(event.mint, tokenCa);
  assert.equal(event.side, 'buy');
  assert.equal(event.user, user);
  assert.equal(event.sol_amount, 1);
  assert.equal(event.token_amount, 2_000);
  assert.equal(event.price_sol, 0.0005);
  assert.equal(event.virtual_sol_reserves, 31);
  assert.equal(event.virtual_token_reserves, 1_073_000_000);
  assert.equal(event.progress_pct, 0);
  assert.equal(event.fee_basis_points, 100);
});

test('estimates pump.fun progress from decoded real token reserves', () => {
  assert.equal(estimatePumpfunProgressPctFromRealTokenReserves(793_100_000), 0);
  assert.equal(estimatePumpfunProgressPctFromRealTokenReserves(0), 100);
  assert.equal(Math.round(estimatePumpfunProgressPctFromRealTokenReserves(396_550_000)), 50);
});

test('filters signature pages by blockTime before fetching transactions', () => {
  const filtered = filterSignaturesForWindow([
    { signature: 'new-1', blockTime: 1_300 },
    { signature: 'new-2', blockTime: 1_250 },
    { signature: 'in-1', blockTime: 1_180 },
    { signature: 'unknown-time' },
    { signature: 'in-2', blockTime: 1_050 },
    { signature: 'old-1', blockTime: 990 },
    { signature: 'old-2', blockTime: 800 },
  ], { startTs: 1_000, endTs: 1_200 });

  assert.deepEqual(filtered.fetchable.map((row) => row.signature), ['in-1', 'unknown-time', 'in-2']);
  assert.equal(filtered.skippedAfterEnd, 2);
  assert.equal(filtered.skippedBeforeStart, 2);
  assert.equal(filtered.reachedStart, true);
});

test('does not stop on future signatures when scanning toward a target window', () => {
  const filtered = filterSignaturesForWindow([
    { signature: 'future', blockTime: 2_000 },
    { signature: 'inside', blockTime: 1_100 },
  ], { startTs: 1_000, endTs: 1_200 });

  assert.deepEqual(filtered.fetchable.map((row) => row.signature), ['inside']);
  assert.equal(filtered.skippedAfterEnd, 1);
  assert.equal(filtered.reachedStart, false);
});

test('extracts exact pump.fun TradeEvent from raw transaction logs', () => {
  const encoded = tradeEventPayload({ solLamports: 2_000_000_000, tokenRaw: 4_000_000_000 }).toString('base64');
  const events = decodePumpfunTradeEventsFromLogs([
    'Program log: Instruction: Buy',
    `Program data: ${encoded}`,
  ], { tokenCa });

  assert.equal(events.length, 1);
  assert.equal(events[0].sol_amount, 2);
  assert.equal(events[0].token_amount, 4_000);
});

test('prefers exact TradeEvent over transfer heuristic when raw logs are available', () => {
  const encoded = tradeEventPayload({ solLamports: 3_000_000_000, tokenRaw: 6_000_000_000 }).toString('base64');
  const trade = normalizeCurveTransaction({
    transaction: { signatures: ['sig-exact'] },
    blockTime: 999,
    slot: 8,
    meta: {
      logMessages: [`Program data: ${encoded}`],
    },
    tokenTransfers: [
      { mint: tokenCa, tokenAmount: 1, fromUserAccount: 'curve-pda', toUserAccount: 'buyer' },
    ],
    nativeTransfers: [
      { amount: 1, fromUserAccount: 'buyer', toUserAccount: 'curve-pda' },
    ],
  }, { tokenCa, curvePda: 'curve-pda' });

  assert.equal(trade.signature, 'sig-exact');
  assert.equal(trade.block_time, 1_000);
  assert.equal(trade.sol_amount, 3);
  assert.equal(trade.token_amount, 6_000);
  assert.equal(trade.price_decode_status, 'exact_trade_event');
  assert.equal(trade.progress_pct, 0);
  assert.equal(trade.progress_decode_status, 'exact_trade_event_reserves_decoded_estimated_progress_v1');
});

test('aggregates curve trades to 1m bars with buy/sell/user metrics', () => {
  const bars = aggregateMinuteBars([
    {
      block_time: 1_000,
      price_sol: 0.1,
      sol_amount: 1,
      side: 'buy',
      user: 'a',
    },
    {
      block_time: 1_010,
      price_sol: 0.2,
      sol_amount: 0.5,
      side: 'sell',
      user: 'b',
    },
  ]);

  assert.equal(bars.length, 1);
  assert.equal(bars[0].open, 0.1);
  assert.equal(bars[0].high, 0.2);
  assert.equal(bars[0].close, 0.2);
  assert.equal(bars[0].sol_volume, 1.5);
  assert.equal(bars[0].buy_count, 1);
  assert.equal(bars[0].sell_count, 1);
  assert.equal(bars[0].unique_buyers, 1);
  assert.equal(bars[0].unique_sellers, 1);
});

test('selects the last pre-anchor trade as chain baseline', () => {
  const baseline = selectBaselineTradeAtAnchor([
    { block_time: 990, reserve_price_sol: 0.1, signature: 'old', progress_pct: 10 },
    { block_time: 999, reserve_price_sol: 0.2, signature: 'pre', progress_pct: 20 },
    { block_time: 1005, reserve_price_sol: 0.3, signature: 'post', progress_pct: 30 },
  ], 1000);

  assert.equal(baseline.baseline_price_sol_chain, 0.2);
  assert.equal(baseline.baseline_source, 'chain_truth_last_pre_anchor_trade');
  assert.equal(baseline.baseline_trade_lag_sec, -1);
  assert.equal(baseline.baseline_post_anchor_biased, false);
  assert.equal(baseline.baseline_progress_pct, 20);
});

test('marks first post-anchor baseline as biased when no pre-anchor trade exists', () => {
  const baseline = selectBaselineTradeAtAnchor([
    { block_time: 1005, price_sol: 0.3, signature: 'post', progress_pct: 30 },
  ], 1000);

  assert.equal(baseline.baseline_price_sol_chain, 0.3);
  assert.equal(baseline.baseline_source, 'chain_truth_first_post_anchor_trade');
  assert.equal(baseline.baseline_trade_lag_sec, 5);
  assert.equal(baseline.baseline_post_anchor_biased, true);
});
