import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { execFileSync, spawnSync } from 'node:child_process';
import {
  buildPack,
  normalizeSignalTs,
  classifyStage,
  assertNoForbiddenMetricKeys,
} from '../scripts/build-telegram-metadata-alignment-pack.js';

function makeRawDb(p) {
  const db = new Database(p);
  db.exec(`
    CREATE TABLE raw_signal_outcomes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      signal_id TEXT,
      token_ca TEXT,
      symbol TEXT,
      signal_ts INTEGER,
      signal_type TEXT,
      route TEXT,
      source TEXT,
      observation_status TEXT,
      baseline_price_unit TEXT,
      baseline_confidence TEXT,
      path_provider TEXT,
      path_price_unit TEXT,
      same_source_path INTEGER,
      kline_covered INTEGER,
      coverage_reason TEXT,
      provider TEXT,
      max_sustained_peak_pct REAL,
      time_to_sustained_peak_sec INTEGER,
      raw_sustained_tier TEXT,
      raw_primary_tier TEXT,
      sustained_evaluable INTEGER,
      sustained_reason TEXT,
      outlier_flag INTEGER,
      created_at INTEGER,
      updated_at INTEGER,
      source_kind TEXT,
      source_family TEXT,
      path_source_kind TEXT,
      path_source_family TEXT
    );
  `);
  const insert = db.prepare(`
    INSERT INTO raw_signal_outcomes (
      signal_id, token_ca, symbol, signal_ts, signal_type, route, source,
      observation_status, baseline_price_unit, baseline_confidence, path_provider,
      path_price_unit, same_source_path, kline_covered, coverage_reason, provider,
      max_sustained_peak_pct, time_to_sustained_peak_sec, raw_sustained_tier,
      raw_primary_tier, sustained_evaluable, sustained_reason, outlier_flag,
      created_at, updated_at, source_kind, source_family, path_source_kind, path_source_family
    ) VALUES (
      @signal_id, @token_ca, @symbol, @signal_ts, @signal_type, @route, @source,
      @observation_status, @baseline_price_unit, @baseline_confidence, @path_provider,
      @path_price_unit, @same_source_path, @kline_covered, @coverage_reason, @provider,
      @max_sustained_peak_pct, @time_to_sustained_peak_sec, @raw_sustained_tier,
      @raw_primary_tier, @sustained_evaluable, @sustained_reason, @outlier_flag,
      @created_at, @updated_at, @source_kind, @source_family, @path_source_kind, @path_source_family
    )
  `);
  insert.run({
    signal_id: 'raw-old',
    token_ca: 'tok1',
    symbol: 'AAA',
    signal_ts: 1780001000,
    signal_type: 'ATH',
    route: 'ATH',
    source: 'premium_signals',
    observation_status: 'matured',
    baseline_price_unit: 'native',
    baseline_confidence: 'medium',
    path_provider: 'geckoterminal',
    path_price_unit: 'native',
    same_source_path: 1,
    kline_covered: 1,
    coverage_reason: 'covered',
    provider: 'geckoterminal',
    max_sustained_peak_pct: 42,
    time_to_sustained_peak_sec: 300,
    raw_sustained_tier: 'bronze',
    raw_primary_tier: 'bronze',
    sustained_evaluable: 1,
    sustained_reason: 'ok',
    outlier_flag: 0,
    created_at: 10,
    updated_at: 10,
    source_kind: 'indexed_ohlcv',
    source_family: 'third_party_kline',
    path_source_kind: 'indexed_ohlcv',
    path_source_family: 'third_party_kline',
  });
  insert.run({
    signal_id: 'raw-new',
    token_ca: 'tok1',
    symbol: 'AAA',
    signal_ts: 1780001000,
    signal_type: 'NEW_TRENDING',
    route: 'NEW_TRENDING',
    source: 'premium_signals',
    observation_status: 'matured',
    baseline_price_unit: 'native',
    baseline_confidence: 'high',
    path_provider: 'helius',
    path_price_unit: 'native',
    same_source_path: 1,
    kline_covered: 1,
    coverage_reason: 'covered',
    provider: 'helius',
    max_sustained_peak_pct: 210,
    time_to_sustained_peak_sec: 900,
    raw_sustained_tier: 'gold',
    raw_primary_tier: 'gold',
    sustained_evaluable: 1,
    sustained_reason: 'ok',
    outlier_flag: 0,
    created_at: 11,
    updated_at: 12,
    source_kind: 'bonding_curve',
    source_family: 'onchain_swap',
    path_source_kind: 'bonding_curve',
    path_source_family: 'onchain_swap',
  });
  insert.run({
    signal_id: 'raw2',
    token_ca: 'tok2',
    symbol: 'BBB',
    signal_ts: 1780002000,
    signal_type: 'ATH',
    route: 'ATH',
    source: 'premium_signals',
    observation_status: 'matured',
    baseline_price_unit: 'usd',
    baseline_confidence: 'low',
    path_provider: 'gmgn',
    path_price_unit: 'usd',
    same_source_path: 0,
    kline_covered: 1,
    coverage_reason: 'covered',
    provider: 'gmgn',
    max_sustained_peak_pct: 5,
    time_to_sustained_peak_sec: 60,
    raw_sustained_tier: 'sub25',
    raw_primary_tier: 'sub25',
    sustained_evaluable: 1,
    sustained_reason: 'ok',
    outlier_flag: 0,
    created_at: 20,
    updated_at: 20,
    source_kind: 'indexed_ohlcv',
    source_family: 'third_party_kline',
    path_source_kind: 'indexed_ohlcv',
    path_source_family: 'third_party_kline',
  });
  insert.run({
    signal_id: 'raw-unmatched',
    token_ca: 'tok3',
    symbol: 'CCC',
    signal_ts: 1780003000,
    signal_type: 'NEW_TRENDING',
    route: 'NEW_TRENDING',
    source: 'premium_signals',
    observation_status: 'matured',
    baseline_price_unit: 'native',
    baseline_confidence: 'high',
    path_provider: 'geckoterminal',
    path_price_unit: 'native',
    same_source_path: 1,
    kline_covered: 1,
    coverage_reason: 'covered',
    provider: 'geckoterminal',
    max_sustained_peak_pct: 30,
    time_to_sustained_peak_sec: 90,
    raw_sustained_tier: 'bronze',
    raw_primary_tier: 'bronze',
    sustained_evaluable: 1,
    sustained_reason: 'ok',
    outlier_flag: 0,
    created_at: 30,
    updated_at: 30,
    source_kind: 'indexed_ohlcv',
    source_family: 'third_party_kline',
    path_source_kind: 'indexed_ohlcv',
    path_source_family: 'third_party_kline',
  });
  db.close();
}

function makePremiumDb(p) {
  const db = new Database(p);
  db.exec(`
    CREATE TABLE premium_signals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      remote_signal_id INTEGER,
      token_ca TEXT,
      symbol TEXT,
      raw_message TEXT,
      timestamp INTEGER,
      source_message_ts INTEGER,
      receive_ts INTEGER,
      signal_type TEXT,
      is_ath INTEGER,
      ai_narrative_tier TEXT,
      source_event_id TEXT,
      signal_source TEXT,
      created_at TEXT,
      narrative_score REAL,
      narrative_confidence REAL,
      narrative_tags TEXT
    );
  `);
  const insert = db.prepare(`
    INSERT INTO premium_signals (
      remote_signal_id, token_ca, symbol, raw_message, timestamp,
      source_message_ts, receive_ts, signal_type, is_ath, ai_narrative_tier,
      source_event_id, signal_source, created_at, narrative_score,
      narrative_confidence, narrative_tags
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  insert.run(9, 'tok1', 'AAA', null, 1780001000 * 1000, 1780000999, 1780001000, 'ATH', 1, null, 'evt-old', 'premium_channel', 'x', null, null, null);
  insert.run(10, 'tok1', 'AAA', 'message', 1780001000 * 1000, 1780000999, 1780001000, 'NEW_TRENDING', 0, 'high', 'evt-new', 'premium_channel', 'x', 7.5, 0.8, 'tag');
  insert.run(11, 'tok2', 'BBB', '', 1780002000, 1780001999, 1780002000, 'ATH', 1, null, 'evt2', 'premium_ath', 'x', null, null, null);
  insert.run(12, 'premium-only', 'PO', 'message', 1780004000, 1780003999, 1780004000, 'NEW_TRENDING', 0, 'medium', 'evt4', 'premium_channel', 'x', 4, 0.5, null);
  db.close();
}

test('normalizes seconds and milliseconds timestamps', () => {
  assert.equal(normalizeSignalTs(1000), 1000);
  assert.equal(normalizeSignalTs(1000_000), 1000000);
  assert.equal(normalizeSignalTs(1_780_000_000_000), 1_780_000_000);
  assert.equal(normalizeSignalTs(null), null);
});

test('alignment pack exact-joins metadata to deduped raw outcomes', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'telegram-align-'));
  const rawDb = path.join(dir, 'raw.db');
  const premiumDb = path.join(dir, 'premium.db');
  makeRawDb(rawDb);
  makePremiumDb(premiumDb);

  const { report, rows } = buildPack({ rawDbPath: rawDb, premiumDbPath: premiumDb });
  assert.equal(report.alignment.aligned_rows, 2);
  assert.equal(report.raw_outcomes.unique_signal_keys, 3);
  assert.equal(report.raw_outcomes.duplicate_rows_removed, 1);
  assert.equal(report.premium_signals.duplicate_rows_removed, 1);
  assert.equal(report.alignment.raw_keys_without_premium_metadata, 1);
  assert.equal(report.alignment.premium_keys_without_raw_outcome, 1);

  const tok1 = rows.find((r) => r.token_ca === 'tok1');
  assert.equal(tok1.raw_signal_id, 'raw-new');
  assert.equal(tok1.premium_remote_signal_id, 10);
  assert.equal(tok1.metadata_signal_type, 'NEW_TRENDING');
  assert.equal(tok1.raw_message_present, true);
  assert.equal(tok1.stage_at_signal_proxy, 'curve_active');
  assert.equal(tok1.stage_resolved, true);
  assert.equal(tok1.raw_formal_eligible, true);
});

test('reports missing metadata coverage without discrimination metrics', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'telegram-align-'));
  const rawDb = path.join(dir, 'raw.db');
  const premiumDb = path.join(dir, 'premium.db');
  makeRawDb(rawDb);
  makePremiumDb(premiumDb);

  const { report } = buildPack({ rawDbPath: rawDb, premiumDbPath: premiumDb });
  assert.equal(report.metadata_coverage.narrative_score.present, 1);
  assert.equal(report.metadata_coverage.narrative_score.missing, 1);
  assert.equal(report.metadata_coverage.raw_message_present.present, 2);
  assert.equal(report.metric_leak_check.no_forbidden_metric_keys, true);
  assert.doesNotThrow(() => assertNoForbiddenMetricKeys(report));
});

test('classifies only defensible stage proxies', () => {
  assert.deepEqual(classifyStage({ path_source_kind: 'bonding_curve' }), {
    stage_at_signal_proxy: 'curve_active',
    stage_resolved: true,
    stage_reason: 'path_source_kind=bonding_curve',
  });
  assert.deepEqual(classifyStage({ path_source_kind: 'indexed_ohlcv', path_provider: 'geckoterminal' }), {
    stage_at_signal_proxy: 'indexed_or_unknown_stage',
    stage_resolved: false,
    stage_reason: 'price_track_present_stage_not_resolved',
  });
});

test('forbidden metric key guard is recursive', () => {
  assert.throws(() => assertNoForbiddenMetricKeys({ nested: { auc: 0.6 } }), /Forbidden metric keys/);
  assert.throws(() => assertNoForbiddenMetricKeys([{ x: { precision_at_k: 0.2 } }]), /Forbidden metric keys/);
});

test('CLI writes rows, report, and manifest', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'telegram-align-'));
  const rawDb = path.join(dir, 'raw.db');
  const premiumDb = path.join(dir, 'premium.db');
  const outDir = path.join(dir, 'out');
  makeRawDb(rawDb);
  makePremiumDb(premiumDb);
  const stdout = execFileSync(process.execPath, [
    'scripts/build-telegram-metadata-alignment-pack.js',
    '--raw-db', rawDb,
    '--premium-db', premiumDb,
    '--out-dir', outDir,
  ], { cwd: '/Users/boliu/sas-research', encoding: 'utf8' });
  const res = JSON.parse(stdout);
  assert.equal(res.ok, true);
  assert.equal(res.aligned_rows, 2);
  assert.equal(fs.existsSync(path.join(outDir, 'aligned-telegram-metadata-outcomes.jsonl')), true);
  assert.equal(fs.existsSync(path.join(outDir, 'alignment-qa-report.json')), true);
  assert.equal(fs.existsSync(path.join(outDir, 'manifest.json')), true);
});

test('CLI fails closed on missing premium DB', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'telegram-align-'));
  const rawDb = path.join(dir, 'raw.db');
  makeRawDb(rawDb);
  const res = spawnSync(process.execPath, [
    'scripts/build-telegram-metadata-alignment-pack.js',
    '--raw-db', rawDb,
    '--premium-db', path.join(dir, 'missing.db'),
    '--out-dir', path.join(dir, 'out'),
  ], { cwd: '/Users/boliu/sas-research', encoding: 'utf8' });
  assert.notEqual(res.status, 0);
  assert.match(res.stderr, /FAIL_CLOSED/);
});
