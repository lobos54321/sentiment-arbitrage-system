import test from 'node:test';
import assert from 'node:assert/strict';

import { summarizePremiumSignalGateHealth } from '../src/web/data-source-health-utils.js';

test('premium signal gate health surfaces rate limits and provider auth failures', () => {
  const health = summarizePremiumSignalGateHealth([
    {
      id: 26242,
      symbol: 'ILHAMR',
      token_ca: 'TokenA',
      timestamp: 1777891568281,
      gate_result: JSON.stringify({
        status: 'NOT_ATH_PREBUY_KLINE_UNKNOWN_DATA_BLOCKED',
        gateDecision: 'UNKNOWN_DATA',
        gateReason: 'backfill_rate_limited',
        unknownDataBlocked: true,
        observability: {
          failClosedApplied: true,
          providerDataState: 'backfill_rate_limited',
        },
        backfill: { provider: 'geckoterminal', reason: 'RATE_LIMITED' },
      }),
    },
    {
      id: 26241,
      symbol: 'Analyze',
      token_ca: 'TokenB',
      timestamp: 1777891198955,
      gate_result: JSON.stringify({
        status: 'NOT_ATH_PREBUY_KLINE_UNKNOWN_DATA_BLOCKED',
        gateDecision: 'UNKNOWN_DATA',
        gateReason: 'provider_rate_limited',
        unknownDataBlocked: true,
        observability: {
          failClosedApplied: true,
          providerDataState: 'provider_rate_limited',
        },
        backfill: {
          provider: 'geckoterminal',
          reason: 'HTTP 401: {"error":{"code":-32401,"message":"Invalid API key"}}',
        },
      }),
    },
    {
      id: 26240,
      symbol: 'CACHE',
      token_ca: 'TokenC',
      timestamp: 1777891199000,
      gate_result: JSON.stringify({
        status: 'PASS',
        gateDecision: 'PASS',
        observability: {
          providerDataState: 'scored',
        },
        backfill: {
          provider: 'local_cache',
          reason: 'LOCAL_CACHE',
        },
      }),
    },
  ]);

  assert.equal(health.status, 'degraded');
  assert.equal(health.counters.sampled_n, 3);
  assert.equal(health.counters.unknown_data_blocked_n, 2);
  assert.equal(health.counters.unknown_data_blocked_recent_n, 2);
  assert.equal(health.counters.fail_closed_n, 2);
  assert.equal(health.counters.rate_limited_n, 2);
  assert.equal(health.counters.rate_limited_recent_n, 2);
  assert.equal(health.counters.local_cache_scored_recent_n, 1);
  assert.equal(health.counters.invalid_api_key_n, 1);
  assert.equal(health.counters.by_provider.geckoterminal, 2);
  assert.equal(health.counters.by_provider.local_cache, 1);
  assert.equal(health.samples[0].timestamp_sec, 1777891568);
  assert.ok(health.warn_reasons.includes('premium_signal_provider_auth_failed'));
});
