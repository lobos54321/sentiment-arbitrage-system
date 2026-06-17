import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync, spawnSync } from 'node:child_process';

import {
  buildCanaryWindows,
  hasForbiddenKey,
} from '../scripts/run-curve-observability-canary.js';

const SCRIPT = new URL('../scripts/run-curve-observability-canary.js', import.meta.url).pathname;
const NOW = 2_000_000_000;
const pump = '111111111111111111111111111111111111pump';

function tmpdir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'curve-canary-'));
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function writeJsonl(filePath, rows) {
  fs.writeFileSync(filePath, rows.map((r) => JSON.stringify(r)).join('\n') + '\n');
}

function baseSignal(over = {}) {
  return { token_ca: pump, signal_ts: NOW - 10_000, label: 'dog', return_domain: 'sol_curve', unique_buyers: 999, ...over };
}

test('selection is label-stripped, deterministic, pump-only, and chronological by token/signal', () => {
  const rows = [
    baseSignal({ token_ca: 'notpumpx', signal_ts: NOW - 10_000 }),
    baseSignal({ token_ca: `${'2'.repeat(36)}pump`, signal_ts: NOW - 100 }), // too recent
    baseSignal({ token_ca: `${'3'.repeat(36)}pump`, signal_ts: NOW - 10_000 }),
    baseSignal({ token_ca: `${'3'.repeat(36)}pump`, signal_ts: NOW - 10_000, label: 'dud' }),
    baseSignal({ token_ca: `${'1'.repeat(36)}pump`, signal_ts: NOW - 20_000 }),
  ];
  const { windows, stats } = buildCanaryWindows(rows.map((r) => ({ token_ca: r.token_ca, signal_ts: r.signal_ts })), { nowTs: NOW, limit: 10 });
  assert.deepEqual(windows.map((w) => w.token_ca), [`${'1'.repeat(36)}pump`, `${'3'.repeat(36)}pump`]);
  assert.equal(stats.dedup_removed, 1);
  assert.equal(stats.non_pump_excluded, 1);
  assert.equal(stats.too_recent_excluded, 1);
  assert.equal(Object.keys(windows[0]).includes('label'), false);
});

test('dune canary accepts complete label-bearing trade export but emits no forbidden fields', () => {
  const dir = tmpdir();
  const rowsPath = path.join(dir, 'signals.json');
  const tradesPath = path.join(dir, 'trades.jsonl');
  const outDir = path.join(dir, 'out');
  writeJson(rowsPath, [baseSignal()]);
  writeJsonl(tradesPath, [
    { token_ca: pump, block_time: NOW - 10_000 - 10, label: 'dog', return_domain: 'sol_curve', side: 'buy', user: 'wallet1', sol_amount: 1, token_amount: 100, real_token_reserves: 700000000 },
    { token_ca: pump, block_time: NOW - 10_000, effective_tier: 'gold', side: 'sell', user: 'wallet2', sol_amount: 0.5, token_amount: 50, virtual_sol_reserves: 40, virtual_token_reserves: 900000000 },
  ]);
  execFileSync(process.execPath, [SCRIPT,
    '--provider', 'dune', '--rows', rowsPath, '--trades', tradesPath,
    '--dune-assume-complete-window', '--out-dir', outDir, '--now-ts', String(NOW), '--limit', '1',
  ], { cwd: '/Users/boliu/sas-research' });
  const outRows = fs.readFileSync(path.join(outDir, 'canary_observability.jsonl'), 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  const summary = JSON.parse(fs.readFileSync(path.join(outDir, 'canary-summary.json'), 'utf8'));
  assert.equal(outRows.length, 1);
  assert.equal(outRows[0].in_window_trade_count, 2);
  assert.equal(outRows[0].history_reached_start, true);
  assert.equal(outRows[0].has_wallet, true);
  assert.equal(outRows[0].has_progress, true);
  assert.equal(summary.verdict, 'PASS');
  assert.equal(summary.provider_summary.usable_curve_window_rate, 1);
  assert.equal(hasForbiddenKey(outRows), null);
  assert.equal(hasForbiddenKey(summary), null);
  const sql = fs.readFileSync(path.join(outDir, 'curve_observability_canary.sql'), 'utf8');
  assert.doesNotMatch(sql, /\blabel\b|\bdog\b|\bdud\b|\breturn_domain\b|\bauc\b/i);
});

test('complete empty windows cannot produce a hollow PASS', () => {
  const dir = tmpdir();
  const rowsPath = path.join(dir, 'signals.json');
  const tradesPath = path.join(dir, 'trades.jsonl');
  const outDir = path.join(dir, 'out');
  writeJson(rowsPath, [
    baseSignal({ token_ca: `${'1'.repeat(36)}pump`, signal_ts: NOW - 10_000 }),
    baseSignal({ token_ca: `${'2'.repeat(36)}pump`, signal_ts: NOW - 11_000 }),
    baseSignal({ token_ca: `${'3'.repeat(36)}pump`, signal_ts: NOW - 12_000 }),
  ]);
  writeJsonl(tradesPath, [
    { token_ca: `${'1'.repeat(36)}pump`, signal_ts: NOW - 10_000, block_time: NOW - 10_000, side: 'buy', user: 'w1', sol_amount: 1, real_token_reserves: 1 },
  ]);
  execFileSync(process.execPath, [SCRIPT,
    '--provider', 'dune', '--rows', rowsPath, '--trades', tradesPath,
    '--dune-assume-complete-window', '--out-dir', outDir, '--now-ts', String(NOW), '--limit', '3',
  ], { cwd: '/Users/boliu/sas-research' });
  const summary = JSON.parse(fs.readFileSync(path.join(outDir, 'canary-summary.json'), 'utf8'));
  assert.equal(summary.provider_summary.complete_rate, 1);
  assert.equal(summary.provider_summary.usable_curve_window_rate, 0.333333);
  assert.equal(summary.verdict, 'PARTIAL');
  assert.equal(summary.reason, 'usable_curve_window_rate_below_floor');
});

test('post-signal trade fails closed while block_time == signal_ts is allowed', () => {
  const dir = tmpdir();
  const rowsPath = path.join(dir, 'signals.json');
  const goodTrades = path.join(dir, 'good.jsonl');
  const badTrades = path.join(dir, 'bad.jsonl');
  writeJson(rowsPath, [baseSignal()]);
  writeJsonl(goodTrades, [{ token_ca: pump, block_time: NOW - 10_000, side: 'buy', user: 'w', sol_amount: 1, real_token_reserves: 1 }]);
  const good = spawnSync(process.execPath, [SCRIPT, '--provider', 'dune', '--rows', rowsPath, '--trades', goodTrades, '--dune-assume-complete-window', '--out-dir', path.join(dir, 'good'), '--now-ts', String(NOW), '--limit', '1'], { cwd: '/Users/boliu/sas-research', encoding: 'utf8' });
  assert.equal(good.status, 0, good.stderr);

  writeJsonl(badTrades, [{ token_ca: pump, block_time: NOW - 10_000 + 1, side: 'buy', user: 'w', sol_amount: 1, real_token_reserves: 1 }]);
  const bad = spawnSync(process.execPath, [SCRIPT, '--provider', 'dune', '--rows', rowsPath, '--trades', badTrades, '--dune-assume-complete-window', '--out-dir', path.join(dir, 'bad'), '--now-ts', String(NOW), '--limit', '1'], { cwd: '/Users/boliu/sas-research', encoding: 'utf8' });
  assert.notEqual(bad.status, 0);
  assert.match(bad.stderr, /out_of_window_trade_count/);
});

test('dune provider requires explicit complete-window attestation', () => {
  const dir = tmpdir();
  const rowsPath = path.join(dir, 'signals.json');
  const tradesPath = path.join(dir, 'trades.jsonl');
  writeJson(rowsPath, [baseSignal()]);
  writeJsonl(tradesPath, []);
  const res = spawnSync(process.execPath, [SCRIPT, '--provider', 'dune', '--rows', rowsPath, '--trades', tradesPath, '--out-dir', path.join(dir, 'out'), '--now-ts', String(NOW)], { cwd: '/Users/boliu/sas-research', encoding: 'utf8' });
  assert.notEqual(res.status, 0);
  assert.match(res.stderr, /dune-assume-complete-window/);
});

test('dune canary joins by window_id so repeated-token windows do not create false leakage', () => {
  const dir = tmpdir();
  const rowsPath = path.join(dir, 'signals.json');
  const tradesPath = path.join(dir, 'trades.jsonl');
  const outDir = path.join(dir, 'out');
  writeJson(rowsPath, [
    baseSignal({ token_ca: pump, signal_ts: NOW - 20_000 }),
    baseSignal({ token_ca: pump, signal_ts: NOW - 10_000 }),
  ]);
  writeJsonl(tradesPath, [
    { window_id: 'w00001', token_ca: pump, block_time: NOW - 20_000, side: 'buy', user: 'w1', sol_amount: 1, real_token_reserves: 1 },
    { window_id: 'w00002', token_ca: pump, block_time: NOW - 10_000, side: 'buy', user: 'w2', sol_amount: 1, real_token_reserves: 1 },
  ]);
  execFileSync(process.execPath, [SCRIPT,
    '--provider', 'dune', '--rows', rowsPath, '--trades', tradesPath,
    '--dune-assume-complete-window', '--out-dir', outDir, '--now-ts', String(NOW), '--limit', '2',
  ], { cwd: '/Users/boliu/sas-research' });
  const outRows = fs.readFileSync(path.join(outDir, 'canary_observability.jsonl'), 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.deepEqual(outRows.map((r) => r.out_of_window_trade_count), [0, 0]);
  assert.deepEqual(outRows.map((r) => r.in_window_trade_count), [1, 1]);
});

test('dune canary prefers token+signal_ts over stale external window_id', () => {
  const dir = tmpdir();
  const rowsPath = path.join(dir, 'signals.json');
  const tradesPath = path.join(dir, 'trades.jsonl');
  const outDir = path.join(dir, 'out');
  writeJson(rowsPath, [
    baseSignal({ token_ca: pump, signal_ts: NOW - 20_000 }),
    baseSignal({ token_ca: pump, signal_ts: NOW - 10_000 }),
  ]);
  writeJsonl(tradesPath, [
    // window_id belongs to an external export and is intentionally stale.
    { window_id: 'w99999', token_ca: pump, signal_ts: NOW - 20_000, block_time: NOW - 20_000, side: 'buy', user: 'w1', sol_amount: 1, real_token_reserves: 1 },
    { window_id: 'w99998', token_ca: pump, signal_ts: NOW - 10_000, block_time: NOW - 10_000, side: 'buy', user: 'w2', sol_amount: 1, real_token_reserves: 1 },
  ]);
  execFileSync(process.execPath, [SCRIPT,
    '--provider', 'dune', '--rows', rowsPath, '--trades', tradesPath,
    '--dune-assume-complete-window', '--out-dir', outDir, '--now-ts', String(NOW), '--limit', '2',
  ], { cwd: '/Users/boliu/sas-research' });
  const outRows = fs.readFileSync(path.join(outDir, 'canary_observability.jsonl'), 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.deepEqual(outRows.map((r) => r.out_of_window_trade_count), [0, 0]);
  assert.deepEqual(outRows.map((r) => r.in_window_trade_count), [1, 1]);
});

test('disabled providers report provider_disabled without fallback', () => {
  const dir = tmpdir();
  const rowsPath = path.join(dir, 'signals.json');
  writeJson(rowsPath, [baseSignal()]);
  const env = { ...process.env };
  delete env.HELIUS_API_KEY;
  delete env.HELIUS_RPC_URL;
  const res = spawnSync(process.execPath, [SCRIPT, '--provider', 'helius', '--rows', rowsPath, '--out-dir', path.join(dir, 'out'), '--now-ts', String(NOW)], { cwd: '/Users/boliu/sas-research', encoding: 'utf8', env });
  assert.equal(res.status, 0, res.stderr);
  const summary = JSON.parse(fs.readFileSync(path.join(dir, 'out/canary-summary.json'), 'utf8'));
  const outRows = fs.readFileSync(path.join(dir, 'out/canary_observability.jsonl'), 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.equal(summary.verdict, 'FAIL');
  assert.equal(summary.reason, 'provider_disabled');
  assert.equal(outRows[0].error, 'provider_disabled');
  assert.equal(outRows[0].provider_disabled_reason, 'missing_helius_api_key_or_rpc_url');
});
