import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import test from 'node:test';

import Database from 'better-sqlite3';

import {
  computeFinalVerdict,
  reconciliationVerdict,
  selectPilotSample,
  stageVerdict,
} from '../scripts/run-gate05-backfill-pilot.js';

const SCRIPT = path.resolve('scripts/run-gate05-backfill-pilot.js');
const DAY = 86400;
const ts = (iso) => Math.floor(Date.parse(iso) / 1000);

function tmpdir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'gate05-pilot-'));
}

function makePremiumDb(filePath) {
  const db = new Database(filePath);
  db.exec(`
    CREATE TABLE premium_signals (
      id INTEGER PRIMARY KEY,
      token_ca TEXT NOT NULL,
      symbol TEXT,
      timestamp INTEGER NOT NULL,
      signal_type TEXT,
      is_ath INTEGER,
      narrative_score REAL,
      ai_narrative_tier TEXT,
      raw_message TEXT,
      source_message_ts INTEGER,
      receive_ts INTEGER,
      signal_source TEXT,
      source_event_id TEXT,
      market_cap REAL,
      volume_24h REAL,
      age TEXT
    );
  `);
  const insert = db.prepare(`
    INSERT INTO premium_signals
      (token_ca, symbol, timestamp, signal_type, is_ath, narrative_score, ai_narrative_tier,
       raw_message, source_message_ts, receive_ts, signal_source, source_event_id, market_cap, volume_24h, age)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  insert.run('A'.repeat(32) + 'pump', 'AAA', ts('2026-06-06T00:00:00Z'), 'ATH', 1, 0, null, 'msg-a', ts('2026-06-06T00:00:00Z'), ts('2026-06-06T00:00:01Z'), 'premium_channel', 'e1', 40_000, 1000, '1h');
  insert.run('B'.repeat(32) + 'pump', 'BBB', ts('2026-06-06T00:10:00Z'), 'NEW_TRENDING', 0, 7, 'CONFIRMED', 'msg-b', ts('2026-06-06T00:10:00Z'), ts('2026-06-06T00:10:01Z'), 'premium_channel', 'e2', 200_000, 2000, '2h');
  insert.run('C'.repeat(32) + 'pump', 'CCC', ts('2026-05-01T00:00:00Z'), 'ATH', 1, 0, null, 'msg-c', ts('2026-05-01T00:00:00Z'), ts('2026-05-01T00:00:01Z'), 'premium_channel', 'e3', 300_000, 3000, '3h');
  db.close();
}

function makeObserverDb(filePath) {
  const db = new Database(filePath);
  db.exec(`
    CREATE TABLE raw_signal_outcomes (
      token_ca TEXT,
      signal_ts INTEGER,
      raw_primary_tier TEXT,
      raw_sustained_tier TEXT,
      max_sustained_peak_pct REAL,
      baseline_price REAL,
      baseline_ts INTEGER,
      baseline_lag_sec REAL,
      baseline_price_unit TEXT,
      baseline_confidence TEXT,
      peak_120m_pct REAL,
      time_to_sustained_peak_sec INTEGER,
      path_source_kind TEXT,
      path_source_family TEXT,
      path_provider TEXT,
      coverage_reason TEXT,
      kline_covered INTEGER,
      same_source_path INTEGER,
      sustained_evaluable INTEGER,
      observation_status TEXT,
      outlier_flag INTEGER
    );
  `);
  const insert = db.prepare(`
    INSERT INTO raw_signal_outcomes
      (token_ca, signal_ts, raw_primary_tier, raw_sustained_tier, max_sustained_peak_pct,
       baseline_price, baseline_ts, baseline_lag_sec, baseline_price_unit, baseline_confidence,
       peak_120m_pct, time_to_sustained_peak_sec, path_source_kind, path_source_family, path_provider,
       coverage_reason, kline_covered, same_source_path, sustained_evaluable, observation_status, outlier_flag)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  insert.run('A'.repeat(32) + 'pump', ts('2026-06-06T00:00:00Z'), 'gold', 'gold', 120, 1, ts('2026-06-06T00:00:00Z'), 0, 'native', 'high', 120, 60, 'bonding_curve', 'onchain_swap', 'dune_test', 'covered', 1, 1, 1, 'matured', 0);
  insert.run('B'.repeat(32) + 'pump', ts('2026-06-06T00:10:00Z'), 'sub25', 'sub25', 10, 1, ts('2026-06-06T00:10:00Z'), 0, 'native', 'high', 10, 60, 'bonding_curve', 'onchain_swap', 'dune_test', 'covered', 1, 1, 1, 'matured', 0);
  db.close();
}

function writeBars(filePath) {
  const a = 'A'.repeat(32) + 'pump';
  const b = 'B'.repeat(32) + 'pump';
  const rows = [
    { token_ca: a, timestamp: ts('2026-06-06T00:00:00Z'), open: 1, high: 1, low: 1, close: 1, volume: 100, provider: 'dune_test', source_kind: 'bonding_curve', source_family: 'onchain_swap', pool_address: `bonding_curve:${a}`, price_unit: 'native' },
    { token_ca: a, timestamp: ts('2026-06-06T00:01:00Z'), open: 1, high: 2.2, low: 1, close: 1.8, volume: 1000, provider: 'dune_test', source_kind: 'bonding_curve', source_family: 'onchain_swap', pool_address: `bonding_curve:${a}`, price_unit: 'native' },
    { token_ca: a, timestamp: ts('2026-06-06T00:02:00Z'), open: 1.8, high: 2.0, low: 1.7, close: 1.75, volume: 1000, provider: 'dune_test', source_kind: 'bonding_curve', source_family: 'onchain_swap', pool_address: `bonding_curve:${a}`, price_unit: 'native' },
    { token_ca: b, timestamp: ts('2026-06-06T00:10:00Z'), open: 1, high: 1, low: 1, close: 1, volume: 100, provider: 'dune_test', source_kind: 'bonding_curve', source_family: 'onchain_swap', pool_address: `bonding_curve:${b}`, price_unit: 'native' },
    { token_ca: b, timestamp: ts('2026-06-06T00:11:00Z'), open: 1, high: 1.1, low: 1, close: 1.05, volume: 100, provider: 'dune_test', source_kind: 'bonding_curve', source_family: 'onchain_swap', pool_address: `bonding_curve:${b}`, price_unit: 'native' },
    { token_ca: b, timestamp: ts('2026-06-06T00:12:00Z'), open: 1.05, high: 1.05, low: 1, close: 1.02, volume: 100, provider: 'dune_test', source_kind: 'bonding_curve', source_family: 'onchain_swap', pool_address: `bonding_curve:${b}`, price_unit: 'native' },
  ];
  fs.writeFileSync(filePath, rows.map((r) => JSON.stringify(r)).join('\n') + '\n');
}

test('selectPilotSample prioritizes overlap and reports non-candidate stratification fields', () => {
  const rows = [
    { token_ca: 'a', signal_ts: ts('2026-06-06T00:00:00Z'), month: '2026-06', day: '2026-06-06', signal_type: 'ATH' },
    { token_ca: 'b', signal_ts: ts('2026-06-06T00:01:00Z'), month: '2026-06', day: '2026-06-06', signal_type: 'NEW_TRENDING' },
    { token_ca: 'c', signal_ts: ts('2026-05-01T00:00:00Z'), month: '2026-05', day: '2026-05-01', signal_type: 'ATH', raw_message_present: true },
  ];
  const observerByKey = new Map([[`${rows[0].token_ca}|${rows[0].signal_ts}`, {}]]);
  const out = selectPilotSample(rows, observerByKey, { limit: 2 });
  assert.equal(out.rows.length, 2);
  assert.equal(out.rows[0].pilot_source, 'overlap_reconciliation');
  assert.equal(out.stats.overlap_selected, 1);
  assert.ok(!out.stats.stratification_fields.includes('signal_type'));
  assert.ok(out.stats.forbidden_stratification_fields.includes('signal_type'));
});

test('verdict thresholds are locked', () => {
  assert.equal(reconciliationVerdict(0.91), 'PASS');
  assert.equal(reconciliationVerdict(0.85), 'PARTIAL');
  assert.equal(reconciliationVerdict(0.79), 'NOT_FEASIBLE');
  assert.equal(stageVerdict(0.71), 'PASS');
  assert.equal(stageVerdict(0.55), 'PARTIAL');
  assert.equal(stageVerdict(0.49), 'NOT_FEASIBLE');
  assert.equal(computeFinalVerdict({ reconciliation: { verdict: 'PASS' }, stage: { verdict: 'PASS' }, costOk: true }), 'HISTORICAL_BACKFILL_FEASIBLE');
  assert.equal(computeFinalVerdict({ reconciliation: { verdict: 'PARTIAL' }, stage: { verdict: 'PASS' }, costOk: true }), 'HISTORICAL_BACKFILL_PARTIAL');
  assert.equal(computeFinalVerdict({ reconciliation: { verdict: 'PASS' }, stage: { verdict: 'NOT_FEASIBLE' }, costOk: true }), 'HISTORICAL_BACKFILL_NOT_FEASIBLE');
});

test('prepare and evaluate run end to end with observer reconciliation and no edge metrics', () => {
  const dir = tmpdir();
  const premiumDb = path.join(dir, 'premium.db');
  const observerDb = path.join(dir, 'observer.db');
  const prepareDir = path.join(dir, 'prepare');
  const evalDir = path.join(dir, 'eval');
  const bars = path.join(dir, 'bars.jsonl');
  makePremiumDb(premiumDb);
  makeObserverDb(observerDb);
  writeBars(bars);

  execFileSync(process.execPath, [
    SCRIPT,
    '--mode', 'prepare',
    '--premium-db', premiumDb,
    '--observer-db', observerDb,
    '--out-dir', prepareDir,
    '--limit', '2',
  ], { stdio: 'pipe' });

  const burned = fs.readFileSync(path.join(prepareDir, 'burned_keys.txt'), 'utf8').trim().split('\n');
  assert.equal(burned.length, 2);
  const prep = JSON.parse(fs.readFileSync(path.join(prepareDir, 'prepare-manifest.json'), 'utf8'));
  assert.equal(prep.sample.overlap_selected, 2);
  assert.ok(!JSON.stringify(prep).includes('signal_type_dog_rate'));

  execFileSync(process.execPath, [
    SCRIPT,
    '--mode', 'evaluate',
    '--observer-db', observerDb,
    '--pilot-signals', path.join(prepareDir, 'pilot-signals.json'),
    '--bars-jsonl', bars,
    '--out-dir', evalDir,
    '--cost-credits', '1',
  ], { stdio: 'pipe' });

  const summary = JSON.parse(fs.readFileSync(path.join(evalDir, 'pilot-evaluation-summary.json'), 'utf8'));
  assert.equal(summary.reconciliation.overlap_compared_n, 2);
  assert.equal(summary.reconciliation.dog_dud_agreement_rate, 1);
  assert.equal(summary.stage_resolution.stage_resolved_rate, 1);
  assert.equal(summary.verdict, 'HISTORICAL_BACKFILL_FEASIBLE');
  assert.equal(Object.prototype.hasOwnProperty.call(summary, 'auc'), false);
  assert.ok(!JSON.stringify(summary).includes('signal_type_dog_rate'));
});

