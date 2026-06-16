import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';

const RUNNER = new URL('../scripts/run-oos-daily-cycle.js', import.meta.url).pathname;
const DUNE_EXPORT = new URL('../scripts/run-dune-sql-export.py', import.meta.url).pathname;

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
  assert.ok(src.includes("spawn('curl', ['--config', curlConfigPath]"), 'curl must use a config file, not secret-bearing argv');
  assert.ok(!src.includes("'-H', `Authorization: Bearer ${token}`"), 'token-bearing header must not be passed on argv');
  assert.ok(src.includes('PULL_WALL_TIMEOUT_MS'), 'curl must have an explicit wall-clock watchdog');
  assert.ok(src.includes('PULL_STALL_TIMEOUT_MS'), 'curl must have an explicit stalled-download watchdog');
  assert.ok(src.includes("child.kill('SIGKILL')"), 'watchdog must forcibly kill stalled curl');
});

test('Dune export is chunked rather than one monolithic query', () => {
  const src = fs.readFileSync(RUNNER, 'utf8');
  assert.ok(src.includes('DUNE_CHUNK_SIZE'), 'runner must have a configurable Dune chunk size');
  assert.ok(src.includes('DUNE_CHUNK_MAX_SPAN_S'), 'runner must bound Dune chunks by time span, not only row count');
  assert.ok(src.includes('DUNE_CHUNK_MIN_SPAN_S'), 'runner must be able to split a failed single window into smaller time slices');
  assert.ok(src.includes('exportDuneInChunks'), 'runner must export Dune windows in chunks');
  assert.ok(src.includes('chunkSignalWindows'), 'runner must use a chunking helper for signal windows');
  assert.ok(src.includes('splitFailedDuneRows'), 'runner must recursively split failed Dune chunks');
  assert.ok(src.includes('isDuneSplittableQueryFailure'), 'runner must only split query resource/time failures, not billing/auth errors');
  assert.ok(src.includes('FAILED_TYPE_EXECUTION_TIMEOUT'), 'Dune execution timeouts should be split and retried at finer granularity');
  assert.ok(src.includes('captureOutput: true'), 'Dune export output must be captured so split logic can inspect timeout/resource errors');
  assert.ok(src.includes('query_start_ts') && src.includes('query_end_ts'), 'runner must preserve original windows while narrowing query slices');
  assert.ok(src.includes('chunked:') && src.includes('chunks,'), 'combined Dune manifest must preserve chunk provenance');
  assert.ok(!src.includes("runStage('DUNE', PYTHON, [DUNE_EXPORT, '--sql', path.join(duneDir, 'oos.sql')"), 'runner must not execute the whole daily SQL as one Dune query');
});

test('Dune SQL template filters by query slice while exporting original window fields', () => {
  const template = fs.readFileSync(new URL('../scripts/oos-dune-trade-export.template.sql', import.meta.url), 'utf8');
  assert.ok(template.includes('query_start_ts') && template.includes('query_end_ts'), 'template must accept query slice bounds');
  assert.ok(template.includes('min(query_start_ts)') && template.includes('max(query_end_ts)'), 'source scan must be bounded by query slice bounds');
  assert.ok(template.includes('t.block_time BETWEEN w.query_start_ts AND w.query_end_ts'), 'join must use query slice bounds');
  assert.ok(template.includes('w.window_start_ts') && template.includes('w.window_end_ts'), 'output must retain original window bounds');
});

test('Dune exporter retries transient API/network failures but not HTTP failures', () => {
  const src = fs.readFileSync(DUNE_EXPORT, 'utf8');
  assert.ok(src.includes('TRANSIENT_RETRIES'), 'Dune exporter should have a bounded transient retry budget');
  assert.ok(src.includes('urllib.error.URLError'), 'Dune exporter should classify URL/DNS failures as transient');
  assert.ok(src.includes('retrying in'), 'Dune exporter should log transient retries');
  assert.ok(src.includes('except urllib.error.HTTPError'), 'Dune HTTP failures should stay fail-closed');
  assert.ok(src.indexOf('except urllib.error.HTTPError') < src.indexOf('except Exception as exc'), 'HTTP errors must be handled before generic transient retries');
});
