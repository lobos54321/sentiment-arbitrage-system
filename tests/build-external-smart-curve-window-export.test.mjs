import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { joinExternal, REQUIRED, FINAL_REASON } from '../scripts/build-external-smart-curve-window-export.js';

const START = 900; const END = 1100;
const cohortKeys = new Set(['T|1000']);
const cohortTokens = new Set(['T', 'T2']);

test('REQUIRED + FINAL_REASON cover both external modules', () => {
  assert.deepEqual(Object.keys(REQUIRED).sort(), ['curve_pumpfun', 'smart_money']);
  assert.ok(FINAL_REASON.smart_money.startsWith('FINAL:'));
  assert.ok(FINAL_REASON.curve_pumpfun.startsWith('FINAL:'));
});

test('no export path => FINAL missing reason, nothing joined', () => {
  const r = joinExternal('smart_money', undefined, cohortKeys, cohortTokens, START, END);
  assert.equal(r.status, 'EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON');
  assert.equal(r.joined.length, 0);
  assert.ok(r.missing_reason.startsWith('FINAL:'));
});

test('same-window HIGH + required fields => joined/covered; cross-window & token-only LOW => unjoined', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'extsmoke-'));
  const p = path.join(dir, 'sm.jsonl');
  const full = (over) => ({ smart_wallet_buy_count: 5, smart_wallet_sell_count: 1, smart_wallet_net_sol: 4.2, smart_wallet_unique_n: 3, wallet_signal_score: 0.8, wallet_signal_direction: 'buy', ...over });
  fs.writeFileSync(p, [
    full({ token_ca: 'T', signal_ts: 1000, evidence_ts: 1000 }),       // HIGH same-window, all fields -> joined
    full({ token_ca: 'T', signal_ts: 1000, evidence_ts: 5000 }),       // cross-window -> unjoined
    full({ token_ca: 'T2', signal_ts: 9999, evidence_ts: 1000 }),      // token-only LOW -> unjoined
    { token_ca: 'T', signal_ts: 1000, evidence_ts: 1000, smart_wallet_buy_count: 5 }, // missing fields -> unjoined
  ].map((x) => JSON.stringify(x)).join('\n') + '\n');

  const r = joinExternal('smart_money', p, cohortKeys, cohortTokens, START, END);
  assert.equal(r.status, 'EXTERNAL_EVIDENCE_JOINED');
  assert.equal(r.joined.length, 1);
  assert.equal(r.joined[0].join_confidence, 'HIGH');
  assert.equal(r.joined[0].same_window_valid, true);
  assert.equal(r.unjoined.length, 3);
  assert.ok(r.sha);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('empty export => EXTERNAL_EMPTY_BUT_VALID', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'extsmoke-'));
  const p = path.join(dir, 'cv.jsonl');
  fs.writeFileSync(p, '');
  const r = joinExternal('curve_pumpfun', p, cohortKeys, cohortTokens, START, END);
  assert.equal(r.status, 'EXTERNAL_EMPTY_BUT_VALID');
  assert.equal(r.missing_reason, null);
  fs.rmSync(dir, { recursive: true, force: true });
});
