import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import {
  assertSqliteHeaderReadySync,
  inspectSqliteHeaderSync,
  KlineDatabaseHealthError,
  SQLITE_HEADER,
} from '../src/market-data/sqlite-file-health.js';

test('missing SQLite path fails closed without creating it', () => {
  const root = mkdtempSync(join(tmpdir(), 'sqlite-health-'));
  try {
    const path = join(root, 'missing.db');
    const artifact = join(root, 'missing-health.json');
    assert.throws(
      () => assertSqliteHeaderReadySync(path, { healthArtifactPath: artifact }),
      (error) => error instanceof KlineDatabaseHealthError && error.details.classification === 'MISSING',
    );
    assert.equal(inspectSqliteHeaderSync(path).classification, 'MISSING');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('zero-header SQLite path fails closed and remains unchanged', () => {
  const root = mkdtempSync(join(tmpdir(), 'sqlite-health-'));
  try {
    const path = join(root, 'zero.db');
    const artifact = join(root, 'health.json');
    const original = Buffer.alloc(4096);
    writeFileSync(path, original);
    assert.throws(
      () => assertSqliteHeaderReadySync(path, { healthArtifactPath: artifact }),
      (error) => error instanceof KlineDatabaseHealthError && error.exitCode === 78,
    );
    assert.deepEqual(readFileSync(path), original);
    const report = JSON.parse(readFileSync(artifact, 'utf8'));
    assert.equal(report.classification, 'INVALID_HEADER');
    assert.equal(report.mutation_performed, false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('valid SQLite header passes header preflight', () => {
  const root = mkdtempSync(join(tmpdir(), 'sqlite-health-'));
  try {
    const path = join(root, 'header.db');
    writeFileSync(path, Buffer.concat([SQLITE_HEADER, Buffer.alloc(128)]));
    const result = assertSqliteHeaderReadySync(path, { healthArtifactPath: join(root, 'unused.json') });
    assert.equal(result.classification, 'HEADER_VALID');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
