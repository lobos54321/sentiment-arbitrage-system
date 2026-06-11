import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildTouchSubset,
  chooseClosest,
} from '../scripts/filter-gmgn-touch-by-clean-pack.js';

test('chooseClosest picks nearest ok touch row for the clean anchor', () => {
  const match = chooseClosest(
    { token_ca: 'DOG', signal_ts: 1000 },
    [
      { token_ca: 'DOG', signal_ts: 800, bars: 1 },
      { token_ca: 'DOG', signal_ts: 1030, bars: 2 },
      { token_ca: 'OTHER', signal_ts: 1000, bars: 99 },
    ],
    { maxDeltaSec: 300 },
  );

  assert.equal(match.row.bars, 2);
  assert.equal(match.delta_sec, 30);
});

test('buildTouchSubset reports missing anchors outside the max delta', () => {
  const report = buildTouchSubset({
    anchors: [
      { token_ca: 'DOG', signal_ts: 1000, tier: 'gold' },
      { token_ca: 'MISS', signal_ts: 2000, tier: 'silver' },
    ],
    touchRows: [
      { token_ca: 'DOG', signal_ts: 1010, bars: 1 },
      { token_ca: 'MISS', signal_ts: 3000, bars: 1 },
    ],
    maxDeltaSec: 60,
  });

  assert.equal(report.summary.matched_n, 1);
  assert.equal(report.summary.missing_n, 1);
  assert.equal(report.results[0].clean_anchor_tier, 'gold');
});

