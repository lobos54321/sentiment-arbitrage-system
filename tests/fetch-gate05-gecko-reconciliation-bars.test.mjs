import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import test from 'node:test';

import Database from 'better-sqlite3';

const SCRIPT = path.resolve('scripts/fetch-gate05-gecko-reconciliation-bars.py');
const ts = (iso) => Math.floor(Date.parse(iso) / 1000);

function tmpdir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'gate05-gecko-'));
}

function writePilotSignals(filePath) {
  const rows = [
    { token_ca: 'A'.repeat(32) + 'pump', signal_ts: ts('2026-06-06T00:00:00Z') },
    { token_ca: 'B'.repeat(32) + 'pump', signal_ts: ts('2026-06-06T00:10:00Z') },
    { token_ca: 'C'.repeat(32) + 'pump', signal_ts: ts('2026-06-06T00:20:00Z') },
    { token_ca: 'D'.repeat(32) + 'pump', signal_ts: ts('2026-06-06T00:30:00Z') },
  ];
  fs.writeFileSync(filePath, JSON.stringify(rows, null, 2) + '\n');
}

function makeObserverDb(filePath) {
  const db = new Database(filePath);
  db.exec(`
    CREATE TABLE raw_signal_outcomes (
      token_ca TEXT,
      signal_ts INTEGER,
      path_provider TEXT,
      path_source_kind TEXT,
      path_source_family TEXT,
      path_price_unit TEXT,
      path_pool_address TEXT,
      raw_primary_tier TEXT,
      observation_status TEXT
    );
  `);
  const insert = db.prepare(`
    INSERT INTO raw_signal_outcomes
      (token_ca, signal_ts, path_provider, path_source_kind, path_source_family,
       path_price_unit, path_pool_address, raw_primary_tier, observation_status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  insert.run('A'.repeat(32) + 'pump', ts('2026-06-06T00:00:00Z'), 'geckoterminal', 'indexed_ohlcv', 'third_party_kline', 'native', '', 'gold', 'matured');
  insert.run('B'.repeat(32) + 'pump', ts('2026-06-06T00:10:00Z'), 'local_cache', null, null, 'native', '', 'bronze', 'matured');
  insert.run('C'.repeat(32) + 'pump', ts('2026-06-06T00:20:00Z'), 'geckoterminal', 'bonding_curve', 'onchain_swap', 'native', '', 'silver', 'matured');
  insert.run('D'.repeat(32) + 'pump', ts('2026-06-06T00:30:00Z'), 'geckoterminal', 'indexed_ohlcv', 'third_party_kline', 'usd_per_token', '', 'sub25', 'matured');
  db.close();
}

test('dry-run selects only geckoterminal indexed native observer windows and writes an empty bars file', () => {
  const dir = tmpdir();
  const pilot = path.join(dir, 'pilot-signals.json');
  const observer = path.join(dir, 'observer.db');
  const out = path.join(dir, 'gecko-bars.jsonl');
  const manifestPath = path.join(dir, 'manifest.json');
  const windowsCsv = path.join(dir, 'windows.csv');
  writePilotSignals(pilot);
  makeObserverDb(observer);

  const stdout = execFileSync('python3', [
    SCRIPT,
    '--pilot-signals', pilot,
    '--observer-db', observer,
    '--out-jsonl', out,
    '--manifest', manifestPath,
    '--windows-csv', windowsCsv,
    '--dry-run',
  ], { encoding: 'utf8' });

  const result = JSON.parse(stdout);
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  assert.equal(result.selected_geckoterminal_windows, 1);
  assert.equal(manifest.selection.selected_geckoterminal_windows, 1);
  assert.equal(manifest.selection.selection_counts.geckoterminal_indexed_ohlcv_native, 1);
  assert.equal(manifest.selection.selection_counts['observer_provider_not_geckoterminal:local_cache'], 1);
  assert.equal(manifest.selection.selection_counts['observer_source_kind_not_indexed_ohlcv:bonding_curve'], 1);
  assert.equal(manifest.selection.selection_counts['observer_price_unit_not_native:usd_per_token'], 1);
  assert.equal(manifest.contract.gecko_ohlcv_param, 'token=base');
  assert.deepEqual(manifest.contract.not_used, ['currency=usd', 'dune_curve_bars']);
  assert.equal(fs.readFileSync(out, 'utf8'), '');
  const csv = fs.readFileSync(windowsCsv, 'utf8');
  assert.match(csv, /^token_ca,signal_ts,window_start_ts,window_end_ts/m);
  assert.match(csv, /A{32}pump/);
  assert.doesNotMatch(csv, /B{32}pump/);
});
