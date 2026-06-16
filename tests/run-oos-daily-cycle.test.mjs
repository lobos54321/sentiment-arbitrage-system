import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';

const RUNNER = new URL('../scripts/run-oos-daily-cycle.js', import.meta.url).pathname;

// These probes hit the arg loop BEFORE any network / precondition / pull, so they
// are deterministic and offline: the runner must refuse AUC-revealing flags and
// unknown args, fail-closed, with a non-zero exit.
function expectFail(args, reFragment) {
  let failed = false; let stderr = '';
  try { execFileSync(process.execPath, [RUNNER, ...args], { stdio: 'pipe' }); }
  catch (e) { failed = true; stderr = String(e.stderr || ''); }
  assert.ok(failed, `expected non-zero exit for: ${args.join(' ')}`);
  if (reFragment) assert.ok(stderr.includes(reFragment), `stderr should mention "${reFragment}", got: ${stderr.slice(0, 200)}`);
}

test('fail-closed: --look-point refused (AUC stays sealed)', () => {
  expectFail(['--look-point'], 'look-point');
});

test('fail-closed: --reveal-sealed-auc refused', () => {
  expectFail(['--reveal-sealed-auc'], 'reveal-sealed-auc');
});

test('fail-closed: unknown arg refused', () => {
  expectFail(['--frobnicate'], 'unknown arg');
});

test('--help prints usage and exits 0 (no network)', () => {
  const out = execFileSync(process.execPath, [RUNNER, '--help'], { encoding: 'utf8' });
  assert.ok(out.includes('deterministic daily OOS accumulation'), 'help should describe the runner');
  assert.ok(out.includes('AUC is never read'), 'help should state the AUC guarantee');
});
