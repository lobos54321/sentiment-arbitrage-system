import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';

import {
  buildRawDogDecisionAudit,
} from '../src/analytics/raw-dog-decision-audit.js';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

const rawRow = (overrides = {}) => ({
  signal_id: overrides.signal_id ?? 1,
  token_ca: overrides.token_ca ?? 'DOG',
  symbol: overrides.symbol ?? 'DOG',
  lifecycle_id: overrides.lifecycle_id ?? `life-${overrides.token_ca ?? 'DOG'}`,
  signal_ts: overrides.signal_ts ?? 1000,
  raw_primary_tier: overrides.raw_primary_tier ?? 'gold',
  max_sustained_peak_pct: overrides.max_sustained_peak_pct ?? 120,
  max_wick_peak_pct: overrides.max_wick_peak_pct ?? 130,
  time_to_sustained_peak_sec: overrides.time_to_sustained_peak_sec ?? 300,
  entry_bar_volume: overrides.entry_bar_volume ?? 20_000,
  early_5m_volume: overrides.early_5m_volume ?? 45_000,
  early_15m_volume: overrides.early_15m_volume ?? 90_000,
  ...overrides,
});

const decision = (overrides = {}) => ({
  id: overrides.id ?? 1,
  source_kind: overrides.source_kind ?? 'opportunity_events',
  token_ca: overrides.token_ca ?? 'DOG',
  lifecycle_id: overrides.lifecycle_id ?? `life-${overrides.token_ca ?? 'DOG'}`,
  event_ts: overrides.event_ts ?? 1010,
  action: overrides.action ?? 'BLOCK',
  quote_available: overrides.quote_available ?? 1,
  quote_executable: overrides.quote_executable ?? 1,
  quote_clean: overrides.quote_clean ?? 1,
  route_available: overrides.route_available ?? 1,
  block_cause: overrides.block_cause ?? 'UNKNOWN',
  source_component: overrides.source_component ?? 'matrix',
  source_reason: overrides.source_reason ?? 'matrix_score_below_threshold',
  hard_blockers_json: overrides.hard_blockers_json ?? '[]',
  expected_rr: overrides.expected_rr ?? 2.4,
  score: overrides.score ?? 64,
  would_enter_a_class: overrides.would_enter_a_class ?? 0,
  did_enter: overrides.did_enter ?? 0,
  ...overrides,
});

test('audits only same-bucket duds against quote-clean no-would-enter raw dogs', () => {
  const dog = rawRow({ token_ca: 'DOG', lifecycle_id: 'life-dog', entry_bar_volume: 40_000 });
  const sameBucketDud = rawRow({
    token_ca: 'DUD_A',
    lifecycle_id: 'life-dud-a',
    raw_primary_tier: 'none',
    max_sustained_peak_pct: 12,
    entry_bar_volume: 5_000,
  });
  const wouldEnterDud = rawRow({
    token_ca: 'DUD_B',
    lifecycle_id: 'life-dud-b',
    raw_primary_tier: 'none',
    max_sustained_peak_pct: 5,
    entry_bar_volume: 50_000,
  });
  const notQuoteCleanDud = rawRow({
    token_ca: 'DUD_C',
    lifecycle_id: 'life-dud-c',
    raw_primary_tier: 'none',
    max_sustained_peak_pct: 8,
    entry_bar_volume: 60_000,
  });
  const report = buildRawDogDecisionAudit({
    rawDogs: [dog],
    dudCandidates: [sameBucketDud, wouldEnterDud, notQuoteCleanDud],
    decisionRecords: [
      decision({
        token_ca: 'DOG',
        lifecycle_id: 'life-dog',
        hard_blockers_json: '["scout_quality_volume_low"]',
      }),
      decision({ token_ca: 'DUD_A', lifecycle_id: 'life-dud-a' }),
      decision({
        token_ca: 'DUD_B',
        lifecycle_id: 'life-dud-b',
        action: 'WOULD_ENTER',
        would_enter_a_class: 1,
      }),
      decision({
        token_ca: 'DUD_C',
        lifecycle_id: 'life-dud-c',
        quote_clean: 0,
        block_cause: 'MARKET',
      }),
    ],
  });

  const audit = report.quote_clean_no_would_enter_audit;
  assert.equal(audit.raw_dogs_n, 1);
  assert.equal(audit.comparison_duds_n, 1);
  assert.equal(audit.dud_summary.examples[0].token_ca, 'DUD_A');
  assert.equal(audit.entry_volume.raw_dogs.entry_bar_volume.median, 40_000);
  assert.equal(audit.entry_volume.comparison_duds.entry_bar_volume.median, 5_000);
  assert.equal(audit.raw_dog_gate_reasons.gate_reason_counts.scout_quality_volume_low, 1);
  assert.equal(audit.raw_dog_gate_reasons.gate_reason_counts['matrix:matrix_score_below_threshold'], 1);
  assert.equal(audit.raw_dog_gate_reasons.score_bands.matrix_score_below_70, 1);
  assert.equal(report.interpretation.do_not_change_strategy, true);
});

test('CLI writes a static audit report from readonly SQLite fixtures', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'raw-dog-audit-'));
  const rawDbPath = path.join(tmp, 'raw_signal_outcomes.db');
  const paperDbPath = path.join(tmp, 'paper_trades.db');
  const outDir = path.join(tmp, 'out');
  const now = Math.floor(Date.now() / 1000);
  const signalTs = now - 3600;

  const rawDb = new Database(rawDbPath);
  rawDb.exec(`
    CREATE TABLE raw_signal_outcomes (
      signal_id TEXT,
      symbol TEXT,
      token_ca TEXT,
      lifecycle_id TEXT,
      signal_ts INTEGER,
      raw_primary_tier TEXT,
      max_sustained_peak_pct REAL,
      max_wick_peak_pct REAL,
      time_to_sustained_peak_sec INTEGER,
      baseline_confidence TEXT,
      coverage_reason TEXT,
      did_enter INTEGER,
      held_to_silver INTEGER,
      held_to_gold INTEGER,
      raw_dog_entered INTEGER,
      raw_dog_realized INTEGER,
      exit_reason TEXT,
      observation_status TEXT,
      kline_covered INTEGER,
      same_source_path INTEGER,
      outlier_flag INTEGER,
      sustained_evaluable INTEGER
    );
    CREATE TABLE raw_price_bars_1m (
      token_ca TEXT,
      timestamp INTEGER,
      volume REAL
    );
  `);
  const insertRaw = rawDb.prepare(`
    INSERT INTO raw_signal_outcomes (
      signal_id, symbol, token_ca, lifecycle_id, signal_ts, raw_primary_tier,
      max_sustained_peak_pct, max_wick_peak_pct, time_to_sustained_peak_sec,
      baseline_confidence, coverage_reason, did_enter, held_to_silver, held_to_gold,
      raw_dog_entered, raw_dog_realized, exit_reason,
      observation_status, kline_covered, same_source_path, outlier_flag, sustained_evaluable
    ) VALUES (
      @signal_id, @symbol, @token_ca, @lifecycle_id, @signal_ts, @raw_primary_tier,
      @max_sustained_peak_pct, @max_wick_peak_pct, @time_to_sustained_peak_sec,
      'high', 'covered', 0, 0, 0, 0, 0, NULL,
      'matured', 1, 1, 0, 1
    )
  `);
  insertRaw.run(rawRow({
    signal_id: 'sig-dog',
    token_ca: 'DOG',
    lifecycle_id: 'life-dog',
    signal_ts: signalTs,
    raw_primary_tier: 'gold',
    max_sustained_peak_pct: 120,
  }));
  insertRaw.run(rawRow({
    signal_id: 'sig-dud',
    token_ca: 'DUD',
    lifecycle_id: 'life-dud',
    signal_ts: signalTs,
    raw_primary_tier: 'none',
    max_sustained_peak_pct: 9,
  }));
  rawDb.prepare('INSERT INTO raw_price_bars_1m (token_ca, timestamp, volume) VALUES (?, ?, ?)').run('DOG', signalTs + 60, 30_000);
  rawDb.prepare('INSERT INTO raw_price_bars_1m (token_ca, timestamp, volume) VALUES (?, ?, ?)').run('DUD', signalTs + 60, 4_000);
  rawDb.close();

  const paperDb = new Database(paperDbPath);
  paperDb.exec(`
    CREATE TABLE opportunity_events (
      id INTEGER PRIMARY KEY,
      event_ts INTEGER,
      token_ca TEXT,
      symbol TEXT,
      lifecycle_id TEXT,
      source_component TEXT,
      source_reason TEXT,
      quote_failure_reason TEXT,
      hard_blockers_json TEXT,
      expected_rr REAL,
      matrix_score REAL,
      block_cause TEXT,
      recoverability TEXT,
      quote_available INTEGER,
      quote_executable INTEGER,
      quote_clean INTEGER,
      route_available INTEGER,
      evidence_status TEXT,
      provider_reason TEXT,
      route_failure_reason TEXT,
      would_enter_a_class INTEGER,
      did_enter INTEGER
    );
  `);
  const insertDecision = paperDb.prepare(`
    INSERT INTO opportunity_events (
      id, event_ts, token_ca, symbol, lifecycle_id, source_component, source_reason,
      quote_failure_reason, hard_blockers_json, expected_rr, matrix_score,
      block_cause, recoverability, quote_available, quote_executable, quote_clean,
      route_available, evidence_status, provider_reason, route_failure_reason,
      would_enter_a_class, did_enter
    ) VALUES (
      @id, @event_ts, @token_ca, @symbol, @lifecycle_id, @source_component, @source_reason,
      NULL, @hard_blockers_json, @expected_rr, @matrix_score,
      'UNKNOWN', NULL, 1, 1, 1, 1, 'quote_clean', NULL, NULL, 0, 0
    )
  `);
  insertDecision.run({
    id: 1,
    event_ts: signalTs + 90,
    token_ca: 'DOG',
    symbol: 'DOG',
    lifecycle_id: 'life-dog',
    source_component: 'matrix',
    source_reason: 'matrix_score_below_threshold',
    hard_blockers_json: '["scout_quality_volume_low"]',
    expected_rr: 2.6,
    matrix_score: 64,
  });
  insertDecision.run({
    id: 2,
    event_ts: signalTs + 90,
    token_ca: 'DUD',
    symbol: 'DUD',
    lifecycle_id: 'life-dud',
    source_component: 'matrix',
    source_reason: 'matrix_score_below_threshold',
    hard_blockers_json: '[]',
    expected_rr: 2.2,
    matrix_score: 62,
  });
  paperDb.close();

  execFileSync(process.execPath, [
    'scripts/run-raw-dog-decision-audit.js',
    '--hours', '24',
    '--max-duds', '20',
    '--raw-db', rawDbPath,
    '--paper-db', paperDbPath,
    '--out-dir', outDir,
  ], {
    cwd: repoRoot,
    stdio: 'pipe',
  });

  const report = JSON.parse(fs.readFileSync(path.join(outDir, 'latest.json'), 'utf8'));
  assert.equal(report.status, 'ok');
  assert.equal(report.quote_clean_no_would_enter_audit.raw_dogs_n, 1);
  assert.equal(report.quote_clean_no_would_enter_audit.comparison_duds_n, 1);
  assert.equal(report.quote_clean_no_would_enter_audit.dog_rows[0].matched_by, 'lifecycle_id');
  assert.equal(report.quote_clean_no_would_enter_audit.entry_volume.raw_dogs.entry_bar_volume.median, 30_000);
  assert.ok(fs.existsSync(path.join(outDir, 'latest.md')));
});
