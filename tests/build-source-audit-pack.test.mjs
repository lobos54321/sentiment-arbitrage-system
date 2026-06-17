import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { execFileSync, spawnSync } from 'node:child_process';
import { buildPack, signalRowsFromFeatures, distribution } from '../scripts/build-source-audit-pack.js';

function makeRawDb(p) {
  const db = new Database(p);
  db.exec(`
    CREATE TABLE raw_signal_outcomes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      signal_id TEXT,
      token_ca TEXT,
      signal_ts INTEGER,
      source TEXT,
      source_family TEXT,
      source_kind TEXT,
      path_source_family TEXT,
      path_source_kind TEXT,
      provider TEXT,
      baseline_provider TEXT,
      path_provider TEXT,
      signal_type TEXT,
      route TEXT,
      coverage_reason TEXT,
      observation_status TEXT,
      kline_covered INTEGER,
      created_at INTEGER
    );
  `);
  const insert = db.prepare(`
    INSERT INTO raw_signal_outcomes (
      signal_id, token_ca, signal_ts, source, source_family, source_kind,
      path_source_family, path_source_kind, provider, baseline_provider, path_provider,
      signal_type, route, coverage_reason, observation_status, kline_covered, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  insert.run('1', 'tok1', 100, 'premium_signals', 'third_party_kline', 'indexed_ohlcv', 'third_party_kline', 'indexed_ohlcv', 'gecko', 'gecko', 'gecko', 'NEW_TRENDING', 'NEW_TRENDING', 'covered', 'matured', 1, 10);
  insert.run('dup', 'tok1', 100, 'premium_signals', 'third_party_kline', 'indexed_ohlcv', 'third_party_kline', 'indexed_ohlcv', 'gecko', 'gecko', 'gecko', 'NEW_TRENDING', 'NEW_TRENDING', 'covered', 'matured', 1, 11);
  insert.run('2', 'tok2', 200, 'premium_signals', 'third_party_kline', 'indexed_ohlcv', 'third_party_kline', 'indexed_ohlcv', 'gmgn', 'gmgn', 'gmgn', 'NEW_TRENDING', 'NEW_TRENDING', 'covered', 'matured', 1, 20);
  db.close();
}

function makeScoreDb(p) {
  const db = new Database(p);
  db.exec('CREATE TABLE score_details (token_ca TEXT, source_score REAL, total_score REAL);');
  db.close();
}

function makePaperDb(p) {
  const db = new Database(p);
  db.exec('CREATE TABLE opportunity_events (token_ca TEXT, event_ts REAL, source_strength_score REAL);');
  db.prepare('INSERT INTO opportunity_events VALUES (?, ?, ?)').run('other', 1, null);
  db.close();
}

test('source audit pack reports degeneracy without outcome metrics', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'source-audit-'));
  const rawDb = path.join(dir, 'raw.db');
  const scoreDb = path.join(dir, 'score.db');
  const paperDb = path.join(dir, 'paper.db');
  const features = path.join(dir, 'features.jsonl');
  makeRawDb(rawDb);
  makeScoreDb(scoreDb);
  makePaperDb(paperDb);
  fs.writeFileSync(features, [
    JSON.stringify({ token_ca: 'tok1', signal_ts: 100, label: 'dog' }),
    JSON.stringify({ token_ca: 'tok2', signal_ts: 200, label: 'dud' }),
  ].join('\n') + '\n');

  const report = buildPack({ rawDbPath: rawDb, featuresPath: features, paperDbPath: paperDb, scoreDbs: [scoreDb] });
  assert.equal(report.verdict, 'SOURCE_AXIS_NULL_FOR_CURRENT_COHORT');
  assert.equal(report.cohort.signal_rows, 2);
  assert.equal(report.cohort.joined_signals, 2);
  assert.equal(report.cohort.raw_rows_for_signal_keys, 3);
  assert.equal(report.cohort.raw_rows_collapsed_by_signal_key, 1);
  assert.equal(report.source_axis_preconditions.origin_has_variance, false);
  assert.equal(report.source_axis_preconditions.family_kind_has_variance, false);
  assert.equal(report.component_score_coverage.score_details[0].rows_total, 0);

  const text = JSON.stringify(report).toLowerCase();
  for (const forbidden of ['lift', 'auc', 'precision', 'recall', 'cramers_v', 'mutual_info', 'chi2', 'separation', 'p_dog', 'p_dud']) {
    assert.equal(text.includes(forbidden), false, forbidden);
  }
});

test('source audit CLI writes manifest and refuses forbidden output terms', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'source-audit-'));
  const rawDb = path.join(dir, 'raw.db');
  const scoreDb = path.join(dir, 'score.db');
  const paperDb = path.join(dir, 'paper.db');
  const features = path.join(dir, 'features.jsonl');
  const outDir = path.join(dir, 'out');
  makeRawDb(rawDb);
  makeScoreDb(scoreDb);
  makePaperDb(paperDb);
  fs.writeFileSync(features, `${JSON.stringify({ token_ca: 'tok1', signal_ts: 100 })}\n`);

  const stdout = execFileSync(process.execPath, [
    'scripts/build-source-audit-pack.js',
    '--raw-db', rawDb,
    '--features', features,
    '--paper-db', paperDb,
    '--score-db', scoreDb,
    '--out-dir', outDir,
  ], { cwd: '/Users/boliu/sas-research', encoding: 'utf8' });
  const res = JSON.parse(stdout);
  assert.equal(res.ok, true);
  const manifest = JSON.parse(fs.readFileSync(path.join(outDir, 'manifest.json'), 'utf8'));
  assert.equal(manifest.verdict, 'SOURCE_AXIS_NULL_FOR_CURRENT_COHORT');
});

test('helper functions dedupe signals and produce stable distributions', () => {
  const rows = signalRowsFromFeatures([
    { token_ca: 'b', signal_ts: 2, label: 'dog' },
    { token_ca: 'a', signal_ts: 1, label: 'dud' },
    { token_ca: 'a', signal_ts: 1, label: 'dog' },
  ]);
  assert.deepEqual(rows.map((r) => `${r.token_ca}|${r.signal_ts}`), ['a|1', 'b|2']);
  assert.deepEqual(distribution([{ x: 'b' }, { x: 'a' }, { x: 'b' }, { x: null }], 'x'), [
    { value: 'b', count: 2 },
    { value: '<NULL>', count: 1 },
    { value: 'a', count: 1 },
  ]);
});

test('CLI fails closed on missing raw DB', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'source-audit-'));
  const features = path.join(dir, 'features.jsonl');
  fs.writeFileSync(features, `${JSON.stringify({ token_ca: 'tok1', signal_ts: 100 })}\n`);
  const res = spawnSync(process.execPath, [
    'scripts/build-source-audit-pack.js',
    '--raw-db', path.join(dir, 'missing.db'),
    '--features', features,
    '--out-dir', path.join(dir, 'out'),
  ], { cwd: '/Users/boliu/sas-research', encoding: 'utf8' });
  assert.notEqual(res.status, 0);
  assert.match(res.stderr, /FAIL_CLOSED/);
});
