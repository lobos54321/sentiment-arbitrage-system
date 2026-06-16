import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';

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

test('snapshot curl keeps token out of argv and has an outer watchdog timeout', () => {
  const src = fs.readFileSync(RUNNER, 'utf8');
  assert.ok(src.includes("execFileSync('curl', ['--config', curlConfigPath]"), 'curl must use a config file, not secret-bearing argv');
  assert.ok(!src.includes("'-H', `Authorization: Bearer ${token}`"), 'token-bearing header must not be passed on argv');
  assert.ok(src.includes('timeout: (PULL_TIMEOUT_S + 30) * 1000'), 'execFileSync must have a watchdog timeout around curl');
});
