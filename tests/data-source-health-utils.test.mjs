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
        dataConfidence: 'none',
        finalDataSource: 'shared_market_data',
        providerAttempts: [
          { provider: 'helius', ok: false, reason: 'helius_disabled' },
          { provider: 'geckoterminal', ok: false, reason: 'RATE_LIMITED', rateLimited: true },
        ],
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
  assert.equal(health.counters.by_data_confidence.none, 1);
  assert.equal(health.counters.provider_attempts['geckoterminal:RATE_LIMITED'], 1);
  assert.equal(health.samples[0].timestamp_sec, 1777891568);
  assert.ok(health.warn_reasons.includes('premium_signal_provider_auth_failed'));
});

test('premium signal gate health warns on isolated rate limit with fallback success', () => {
  const health = summarizePremiumSignalGateHealth([
    {
      id: 1,
      symbol: 'FALLBACK',
      token_ca: 'TokenFallback',
      timestamp: 1777891568,
      gate_result: JSON.stringify({
        status: 'PASS',
        gateDecision: 'PASS',
        gateReason: 'pass',
        provider: 'gmgn',
        finalDataSource: 'gmgn',
        dataConfidence: 'gmgn_kline',
        providerAttempts: [
          { provider: 'geckoterminal', ok: false, reason: 'RATE_LIMITED', rateLimited: true },
          { provider: 'gmgn', ok: true, reason: null },
        ],
        observability: {
          providerDataState: 'scored',
        },
      }),
    },
    {
      id: 2,
      symbol: 'CACHE',
      token_ca: 'TokenCache',
      timestamp: 1777891570,
      gate_result: JSON.stringify({
        status: 'PASS',
        gateDecision: 'PASS',
        provider: 'local_cache',
        finalDataSource: 'local_cache',
        dataConfidence: 'full_kline',
        observability: {
          providerDataState: 'scored',
        },
      }),
    },
    {
      id: 3,
      symbol: 'CACHE2',
      token_ca: 'TokenCache2',
      timestamp: 1777891580,
      gate_result: JSON.stringify({
        status: 'PASS',
        gateDecision: 'PASS',
        provider: 'local_cache',
        finalDataSource: 'local_cache',
        dataConfidence: 'full_kline',
        observability: {
          providerDataState: 'scored',
        },
      }),
    },
    {
      id: 4,
      symbol: 'CACHE3',
      token_ca: 'TokenCache3',
      timestamp: 1777891590,
      gate_result: JSON.stringify({
        status: 'PASS',
        gateDecision: 'PASS',
        provider: 'local_cache',
        finalDataSource: 'local_cache',
        dataConfidence: 'full_kline',
        observability: { providerDataState: 'scored' },
      }),
    },
    {
      id: 5,
      symbol: 'CACHE4',
      token_ca: 'TokenCache4',
      timestamp: 1777891600,
      gate_result: JSON.stringify({
        status: 'PASS',
        gateDecision: 'PASS',
        provider: 'local_cache',
        finalDataSource: 'local_cache',
        dataConfidence: 'full_kline',
        observability: { providerDataState: 'scored' },
      }),
    },
    {
      id: 6,
      symbol: 'CACHE5',
      token_ca: 'TokenCache5',
      timestamp: 1777891610,
      gate_result: JSON.stringify({
        status: 'PASS',
        gateDecision: 'PASS',
        provider: 'local_cache',
        finalDataSource: 'local_cache',
        dataConfidence: 'full_kline',
        observability: { providerDataState: 'scored' },
      }),
    },
  ]);

  assert.equal(health.status, 'warn');
  assert.equal(health.counters.rate_limited_n, 1);
  assert.equal(health.counters.unknown_data_blocked_n, 0);
  assert.equal(health.counters.fallback_success_n, 1);
  assert.equal(health.counters.by_final_data_source.gmgn, 1);
  assert.ok(health.warn_reasons.includes('premium_signal_provider_rate_limited'));
});
