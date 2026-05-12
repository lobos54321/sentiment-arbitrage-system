import test from 'node:test';
import assert from 'node:assert/strict';

import { resolvePremiumPaperOnlyMode } from '../src/engines/premium-mode.js';

test('premium channel defaults to paper-only unless live execution is explicit', () => {
  assert.equal(
    resolvePremiumPaperOnlyMode({}, { SHADOW_MODE: 'false' }),
    true
  );
});

test('premium live execution opt-in disables paper-only mode', () => {
  assert.equal(
    resolvePremiumPaperOnlyMode({}, {
      SHADOW_MODE: 'false',
      PREMIUM_LIVE_EXECUTION_ENABLED: 'true',
    }),
    false
  );
});

test('premium paper-only mode can be forced by config', () => {
  assert.equal(
    resolvePremiumPaperOnlyMode({ PAPER_ONLY_MODE: true }, {
      SHADOW_MODE: 'false',
      PREMIUM_LIVE_EXECUTION_ENABLED: 'true',
    }),
    true
  );
});
