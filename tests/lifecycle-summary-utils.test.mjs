import test from 'node:test';
import assert from 'node:assert/strict';

import {
  applyFinalBlocker,
  chooseFinalBlocker,
  finalBlockerFromEvent,
  finalBlockerFromMissed,
  finalBlockerFromTrade,
} from '../src/web/lifecycle-summary-utils.js';

test('canonical final blocker prefers hard block over earlier wait', () => {
  const wait = finalBlockerFromEvent({
    id: 1,
    event_ts: 100,
    component: 'matrix_evaluator',
    event_type: 'matrix_eval',
    decision: 'wait',
    reason: 'trend_not_ready',
  });
  const block = finalBlockerFromEvent({
    id: 2,
    event_ts: 90,
    component: 'entry_readiness_policy',
    event_type: 'preflight',
    decision: 'reject',
    reason: 'stale_ath_requires_fresh_high',
  });

  assert.equal(chooseFinalBlocker(wait, block).reason, 'stale_ath_requires_fresh_high');
});

test('trade outcome overrides blocker as lifecycle final state', () => {
  const blocked = finalBlockerFromMissed({
    component: 'lotto_entry_gate',
    decision: 'reject',
    reject_reason: 'low_liquidity',
    signal_ts: 100,
  });
  const entered = finalBlockerFromTrade({
    id: 7,
    entry_ts: 120,
    exit_ts: null,
    exit_reason: null,
  });

  assert.equal(chooseFinalBlocker(blocked, entered).status, 'entered');
  assert.equal(chooseFinalBlocker(blocked, entered).is_blocker, false);
});

test('applyFinalBlocker exposes one final_blocker key', () => {
  const summary = applyFinalBlocker({
    final_blocker: finalBlockerFromEvent({
      id: 3,
      event_ts: 110,
      component: 'execution_api',
      event_type: 'entry_quote',
      decision: 'fail',
      reason: 'no_route',
      data_source: 'jupiter_quote',
    }),
  });

  assert.equal(summary.final_status, 'blocked');
  assert.equal(summary.final_component, 'execution_api');
  assert.equal(summary.final_reason, 'no_route');
  assert.equal(summary.final_blocker.stage, 'execution_quote');
  assert.equal(summary.final_blocker_key, 'execution_quote:execution_api:no_route');
});
