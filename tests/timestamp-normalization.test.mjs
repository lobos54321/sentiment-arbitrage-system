import test from 'node:test';
import assert from 'node:assert/strict';

import { normalizeUnixTimestampSec } from '../src/utils/time-normalization.js';

test('normalizes unix timestamps to seconds', () => {
  assert.equal(normalizeUnixTimestampSec(1777791798466), 1777791798);
  assert.equal(normalizeUnixTimestampSec(1777791798), 1777791798);
  assert.equal(normalizeUnixTimestampSec(null, 1777791000123), 1777791000);
  assert.equal(normalizeUnixTimestampSec(undefined, 1777791000), 1777791000);
});
