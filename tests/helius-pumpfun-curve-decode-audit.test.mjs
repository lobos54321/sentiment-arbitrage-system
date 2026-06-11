import assert from 'node:assert/strict';
import test from 'node:test';

import {
  aggregateMinuteBars,
  derivePumpfunBondingCurvePda,
  inferSideAndUser,
  normalizeCurveTransaction,
} from '../scripts/run-helius-pumpfun-curve-decode-audit.js';

const tokenCa = '6eEQtGNoQ7VaPFy3iUZBxNHU5LfvYZRep6umdbfpump';

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
