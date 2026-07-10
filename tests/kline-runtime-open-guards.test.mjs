import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import Database from 'better-sqlite3';

import { getAutonomyConfig } from '../src/config/autonomy-config.js';
import { KlineDatabaseHealthError } from '../src/market-data/sqlite-file-health.js';
import { FixedEvaluator } from '../src/optimizer/fixed-evaluator.js';
import { KlineCollector } from '../src/tracking/kline-collector.js';


function createSourceDb(path) {
  const db = new Database(path);
  db.exec('CREATE TABLE premium_signals (id INTEGER PRIMARY KEY, token_ca TEXT)');
  db.close();
}


test('active kline collector refuses a missing database without creating it', () => {
  const root = mkdtempSync(join(tmpdir(), 'kline-runtime-'));
  try {
    const klinePath = join(root, 'missing.db');
    assert.throws(
      () => new KlineCollector({
        dbPath: klinePath,
        healthArtifactPath: join(root, 'collector-health.json'),
      }),
      (error) => error instanceof KlineDatabaseHealthError
        && error.details.classification === 'MISSING',
    );
    assert.equal(existsSync(klinePath), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});


test('fixed evaluator reaches the guarded repository before any kline direct open', () => {
  const root = mkdtempSync(join(tmpdir(), 'kline-runtime-'));
  try {
    const sourcePath = join(root, 'signals.db');
    const klinePath = join(root, 'missing.db');
    createSourceDb(sourcePath);
    const config = getAutonomyConfig({
      dbPath: sourcePath,
      exports: { filePath: join(root, 'export.json') },
      evaluator: {
        klineCacheDbPath: klinePath,
        klineCacheCandidates: [],
      },
    });
    assert.throws(
      () => new FixedEvaluator(config),
      (error) => error instanceof KlineDatabaseHealthError
        && error.details.classification === 'MISSING',
    );
    assert.equal(existsSync(klinePath), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});


test('collector opens a healthy existing database and preserves quick_check', () => {
  const root = mkdtempSync(join(tmpdir(), 'kline-runtime-'));
  try {
    const klinePath = join(root, 'healthy.db');
    const seed = new Database(klinePath);
    seed.exec('CREATE TABLE seed_evidence (id INTEGER PRIMARY KEY)');
    seed.close();
    const collector = new KlineCollector({
      dbPath: klinePath,
      healthArtifactPath: join(root, 'collector-health.json'),
    });
    collector.stop();
    const check = new Database(klinePath, { readonly: true, fileMustExist: true });
    assert.equal(check.pragma('quick_check', { simple: true }), 'ok');
    check.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
