import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import Database from 'better-sqlite3';

import { KlineRepository } from '../src/market-data/kline-repository.js';
import { KlineDatabaseHealthError, SQLITE_HEADER } from '../src/market-data/sqlite-file-health.js';


test('repository refuses a missing database instead of creating it', () => {
  const root = mkdtempSync(join(tmpdir(), 'kline-repository-'));
  try {
    const path = join(root, 'missing.db');
    const artifact = join(root, 'missing-health.json');
    assert.throws(
      () => new KlineRepository(path, { healthArtifactPath: artifact }),
      (error) => error instanceof KlineDatabaseHealthError && error.details.classification === 'MISSING',
    );
    assert.equal(readFileSync(artifact, 'utf8').includes('FAIL_CLOSED_NO_DATABASE_OPEN'), true);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('repository converts valid-header corruption into a structured fail-closed error', () => {
  const root = mkdtempSync(join(tmpdir(), 'kline-repository-'));
  try {
    const path = join(root, 'malformed.db');
    const artifact = join(root, 'malformed-health.json');
    writeFileSync(path, Buffer.concat([SQLITE_HEADER, Buffer.from('not-a-database')]));
    const before = readFileSync(path);
    assert.throws(
      () => new KlineRepository(path, { healthArtifactPath: artifact }),
      (error) => error instanceof KlineDatabaseHealthError
        && error.details.classification === 'MALFORMED'
        && error.details.mutation_performed === false,
    );
    assert.deepEqual(readFileSync(path), before);
    const report = JSON.parse(readFileSync(artifact, 'utf8'));
    assert.equal(report.action, 'FAIL_CLOSED_BEFORE_WRITE');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});


test('repository opens a healthy SQLite file and initializes only its expected schema', () => {
  const root = mkdtempSync(join(tmpdir(), 'kline-repository-'));
  try {
    const path = join(root, 'healthy.db');
    const seed = new Database(path);
    seed.exec('CREATE TABLE seed_evidence (id INTEGER PRIMARY KEY)');
    seed.close();
    const repository = new KlineRepository(path, { healthArtifactPath: join(root, 'unused.json') });
    assert.deepEqual(repository.getStats(), { barCount: 0, heliusBarCount: 0, tradeCount: 0 });
    repository.close();
    const check = new Database(path, { readonly: true, fileMustExist: true });
    assert.equal(check.pragma('quick_check', { simple: true }), 'ok');
    assert.equal(
      check.prepare("SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name='seed_evidence'").get().count,
      1,
    );
    check.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
