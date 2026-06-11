import assert from 'node:assert/strict';
import test from 'node:test';

import { buildPack } from '../scripts/build-clean-rawdog-pack.js';

test('summarizes polluted and no-bars rows as quarantine layer', () => {
  const pack = buildPack({
    schema_version: 'raw_dog_label_cleaning.v1',
    rows: [
      {
        token_ca: 'DOG',
        signal_ts: 100,
        tier: 'gold',
        label_status: 'clean',
      },
      {
        token_ca: 'DUD',
        signal_ts: 110,
        tier: 'bronze',
        label_status: 'clean',
      },
      {
        token_ca: 'BAD',
        signal_ts: 120,
        tier: 'gold',
        label_status: 'quarantine',
        label_cleaning_reason: 'label_unit_corrupt',
      },
      {
        token_ca: 'NOBARS',
        signal_ts: 130,
        tier: 'silver',
        label_status: 'quarantine',
        label_cleaning_reason: 'no_native_bars',
      },
    ],
  });

  assert.equal(pack.summary.clean_dog_unique_n, 1);
  assert.equal(pack.summary.clean_dud_unique_n, 1);
  assert.equal(pack.summary.quarantine_unique_n, 2);
  assert.equal(pack.summary.polluted_rows_n, 1);
  assert.equal(pack.summary.no_bars_rows_n, 1);
});

