import assert from 'node:assert/strict';
import fs from 'fs';
import os from 'os';
import path from 'path';
import test from 'node:test';

import {
  mergeWorklist,
  readRows,
} from '../scripts/build-chain-truth-worklist.js';

test('readRows forces quarantine cohort even when third column is a symbol', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chain-truth-worklist-'));
  const file = path.join(dir, 'quarantine.txt');
  fs.writeFileSync(file, 'BADpump|1000|LMEOW⚡️\n');

  const rows = readRows(file, { source: 'quarantine' });

  assert.equal(rows.length, 1);
  assert.equal(rows[0].cohort, 'quarantine');
  assert.equal(rows[0].chain_truth_need, 'label_quarantine_adjudication');
});

test('mergeWorklist includes quarantine-only tokens and prefers quarantine overlap', () => {
  const rows = mergeWorklist({
    targetedRows: [
      { token_ca: 'DOG', anchor_ts: 1000, cohort: 'dog', chain_truth_need: 'dark_peak', visibility_stage: 'dark' },
      { token_ca: 'OVERLAP', anchor_ts: 1100, cohort: 'dog', chain_truth_need: 'dark_peak', visibility_stage: 'dark' },
    ],
    quarantineRows: [
      { token_ca: 'OVERLAP', anchor_ts: 1100, cohort: 'quarantine', chain_truth_need: 'label_quarantine_adjudication', visibility_stage: 'unknown' },
      { token_ca: 'ONLYQ', anchor_ts: 1200, cohort: 'quarantine', chain_truth_need: 'label_quarantine_adjudication', visibility_stage: 'unknown' },
    ],
  });

  assert.equal(rows.length, 3);
  assert.equal(rows.find((row) => row.token_ca === 'OVERLAP').cohort, 'quarantine');
  assert.equal(rows.some((row) => row.token_ca === 'ONLYQ'), true);
});

