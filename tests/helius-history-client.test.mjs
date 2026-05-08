import test from 'node:test';
import assert from 'node:assert/strict';

import { HeliusHistoryClient } from '../src/market-data/helius-history-client.js';

test('Helius client uses RPC URL api-key as the effective enhanced API key', () => {
  const client = new HeliusHistoryClient({
    apiKey: 'stale-key',
    rpcUrl: 'https://mainnet.helius-rpc.com/?api-key=fresh-key',
  });

  assert.equal(client.apiKey, 'fresh-key');
  assert.equal(client.isEnabled(), true);
});
