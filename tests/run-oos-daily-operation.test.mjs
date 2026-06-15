import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { selectOosCohort } from '../scripts/run-oos-daily-operation.js';

const ORCH = new URL('../scripts/run-oos-daily-operation.js', import.meta.url).pathname;
const dog = (tok, ts) => ({ token_ca: tok, signal_ts: ts, label: 'dog', return_domain: 'native_sol', tier: 'gold' });
const dud = (tok, ts) => ({ token_ca: tok, signal_ts: ts, label: 'dud', return_domain: 'native_sol', tier: 'bronze' });

test('selectOosCohort: window filter excludes pre-window signals', () => {
  const r = selectOosCohort([dog('T1', 1000), dog('T2', 5000)], [], { oosWindowStart: 2000, trainingTokens: new Set(), cumulativeKeys: new Set() });
  assert.deepEqual(r.oosDogs.map((x) => x.token_ca), ['T2']);
  assert.equal(r.stat.date_excluded, 1);
});

test('selectOosCohort: training-token exclusion', () => {
  const r = selectOosCohort([dog('TRAIN', 5000), dog('FRESH', 5000)], [], { oosWindowStart: 2000, trainingTokens: new Set(['TRAIN']), cumulativeKeys: new Set() });
  assert.deepEqual(r.oosDogs.map((x) => x.token_ca), ['FRESH']);
  assert.equal(r.stat.training_excluded, 1);
});

test('selectOosCohort: already-cumulative exclusion (incremental day-N)', () => {
  const r = selectOosCohort([dog('OLD', 5000), dog('NEW', 6000)], [], { oosWindowStart: 2000, trainingTokens: new Set(), cumulativeKeys: new Set(['OLD|5000']) });
  assert.deepEqual(r.oosDogs.map((x) => x.token_ca), ['NEW']);
  assert.equal(r.stat.already_in_cumulative, 1);
});

test('selectOosCohort: (token,signal_ts) dedup within selection', () => {
  const r = selectOosCohort([dog('T1', 5000), dog('T1', 5000), dog('T1', 6000)], [], { oosWindowStart: 2000, trainingTokens: new Set(), cumulativeKeys: new Set() });
  assert.equal(r.oosDogs.length, 2); // (T1,5000) once + (T1,6000)
  assert.equal(r.stat.signal_dedup_removed, 1);
});

test('selectOosCohort: dud handled symmetrically', () => {
  const r = selectOosCohort([], [dud('U1', 5000), dud('U2', 1000), dud('U3', 5000)], { oosWindowStart: 2000, trainingTokens: new Set(), cumulativeKeys: new Set(['U3|5000']) });
  assert.deepEqual(r.oosDuds.map((x) => x.token_ca), ['U1']); // U2 pre-window, U3 already-cumulative
});

// ---- fail-closed CLI probes ----
function expectFail(args, reFragment) {
  let failed = false; let stderr = '';
  try { execFileSync(process.execPath, [ORCH, ...args], { stdio: 'pipe' }); }
  catch (e) { failed = true; stderr = String(e.stderr || ''); }
  assert.ok(failed, `expected non-zero exit for: ${args.join(' ')}`);
  if (reFragment) assert.ok(stderr.includes(reFragment), `stderr should mention "${reFragment}", got: ${stderr.slice(0, 200)}`);
}

test('fail-closed: --reveal-sealed-auc refused', () => {
  expectFail(['--pack-id', 'p', '--reveal-sealed-auc', '/x'], 'reveal-sealed-auc');
});
test('fail-closed: --look-point refused', () => {
  expectFail(['--pack-id', 'p', '--look-point', '50'], 'look-point');
});
test('fail-closed: missing --production-commit (no --force-smoke)', () => {
  expectFail(['--pack-id', 'p', '--snapshot', '/nope.db', '--cumulative-dir', '/c', '--out-dir', '/tmp/nope-out', '--training-tokens', '/tt'], 'production-commit required');
});
test('fail-closed: output path exists (no overwrite)', () => {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'orch-'));
  expectFail(['--pack-id', 'p', '--snapshot', '/nope.db', '--cumulative-dir', '/c', '--out-dir', d, '--training-tokens', '/tt', '--production-commit', 'abc'], 'output path exists');
});
test('fail-closed: missing snapshot', () => {
  expectFail(['--pack-id', 'p', '--snapshot', '/definitely/missing.db', '--cumulative-dir', '/c', '--out-dir', '/tmp/orch-nope2', '--training-tokens', '/tt', '--production-commit', 'abc'], 'snapshot not found');
});
test('fail-closed: unknown arg', () => {
  expectFail(['--pack-id', 'p', '--frobnicate', '1'], 'unknown arg');
});
