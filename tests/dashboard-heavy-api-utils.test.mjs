import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  boundedIntParam,
  boundedWindowedSinceTs,
} from '../src/web/dashboard-server.js';

test('boundedIntParam clamps oversized live query parameters', () => {
  const url = new URL('https://example.test/api?event_limit=40000&limit=999');

  assert.equal(boundedIntParam(url, 'event_limit', 3000, 100, 8000), 8000);
  assert.equal(boundedIntParam(url, 'limit', 50, 1, 120), 120);
});

test('boundedWindowedSinceTs clamps hours for live heavy endpoints', () => {
  const url = new URL('https://example.test/api?hours=24');
  const since = boundedWindowedSinceTs(url, 1, 2, { nowSec: 10_000 });

  assert.equal(since, 10_000 - 2 * 3600);
});
