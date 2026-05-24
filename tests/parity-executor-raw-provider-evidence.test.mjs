import assert from 'node:assert/strict';
import { test } from 'node:test';

import { ParityExecutor } from '../src/execution/parity-executor.js';
import { JupiterUltraExecutor } from '../src/execution/jupiter-ultra-executor.js';

const rawOrder = {
  requestId: 'provider-request-123',
  outAmount: '2000000',
  routePlan: [{ swapInfo: { ammKey: 'pool-a' } }],
  slippageBps: 50,
};

test('paper simulateBuy preserves providerResponse while hiding internal raw order field', async () => {
  const executor = new ParityExecutor({
    mode: 'paper',
    executor: {
      async getPublicBuyQuote() {
        return {
          success: true,
          side: 'buy',
          requestId: rawOrder.requestId,
          quotedOutAmount: 2,
          quotedOutAmountRaw: rawOrder.outAmount,
          effectivePrice: 0.0005,
          quoteTs: 1_700_000_000_000,
          inputAmount: 0.001,
          inputAmountRaw: '1000000',
          inputMint: 'So11111111111111111111111111111111111111112',
          outputMint: 'TokenRaw',
          inputDecimals: 9,
          outputDecimals: 6,
          _rawOrder: rawOrder,
        };
      },
    },
  });

  const result = await executor.simulateBuy('TokenRaw', 0.001, { applyPaperPenalty: false });

  assert.equal(result.mode, 'paper');
  assert.equal(result.requestId, rawOrder.requestId);
  assert.equal(result.provider, 'jupiter_ultra');
  assert.equal(result.endpoint, '/ultra/v1/order');
  assert.deepEqual(result.providerResponse, rawOrder);
  assert.equal(Object.hasOwn(result, '_rawOrder'), false);
});

test('paper public buy quotes use Jupiter Ultra order so provider request id is capturable', async () => {
  const executor = new JupiterUltraExecutor();
  let publicOrderCalled = false;

  executor._getPublicOrder = async (inputMint, outputMint, amount) => {
    publicOrderCalled = true;
    assert.equal(inputMint, 'So11111111111111111111111111111111111111112');
    assert.equal(outputMint, 'TokenRaw');
    assert.equal(String(amount), '1000000');
    return rawOrder;
  };
  executor._getLiteSwapQuote = async () => {
    throw new Error('lite quote path should not be used for paper raw provider evidence');
  };
  executor._resolveMintDecimals = async (mint) => (
    mint === 'So11111111111111111111111111111111111111112' ? 9 : 6
  );

  const result = await executor.getPublicBuyQuote('TokenRaw', 0.001);

  assert.equal(publicOrderCalled, true);
  assert.equal(result.success, true);
  assert.equal(result.requestId, rawOrder.requestId);
  assert.equal(result._rawOrder, rawOrder);
});
