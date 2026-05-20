import test from 'node:test';
import assert from 'node:assert/strict';

import { SharedMarketRuntime } from '../src/market-data/shared-market-runtime.js';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test('shared market runtime coalesces identical in-flight requests in one process', async () => {
  const runtime = new SharedMarketRuntime({
    namespace: 'test-singleflight',
    redisEnabled: false,
    sharedRedisEnabled: false,
  });
  let producerCalls = 0;

  const producer = async () => {
    producerCalls += 1;
    await sleep(25);
    return { ok: true, value: producerCalls };
  };

  const [first, second, third] = await Promise.all([
    runtime.runSingleFlight('quote:TokenA:SOL:1000', producer),
    runtime.runSingleFlight('quote:TokenA:SOL:1000', producer),
    runtime.runSingleFlight('quote:TokenA:SOL:1000', producer),
  ]);

  assert.equal(producerCalls, 1);
  assert.deepEqual(first, { ok: true, value: 1 });
  assert.deepEqual(second, first);
  assert.deepEqual(third, first);
});

