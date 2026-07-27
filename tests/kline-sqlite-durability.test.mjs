import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import Database from 'better-sqlite3';

import {
  acquireKlineWriterLockSync,
  configureKlineSqliteSync,
  releaseKlineWriterLockSync,
  withKlineWriterLockSync,
} from '../src/market-data/kline-sqlite-durability.js';
import { KlineRepository } from '../src/market-data/kline-repository.js';
import { KlineCollector } from '../src/tracking/kline-collector.js';

function createHealthyDb(path) {
  const db = new Database(path);
  db.exec('CREATE TABLE seed_evidence (id INTEGER PRIMARY KEY)');
  db.close();
}

function waitForChild(child) {
  return new Promise((resolve) => {
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => { stdout += chunk.toString(); });
    child.stderr.on('data', (chunk) => { stderr += chunk.toString(); });
    child.on('close', (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
}

test('durability config replaces WAL with DELETE and enforces FULL sync', () => {
  const root = mkdtempSync(join(tmpdir(), 'kline-durability-'));
  try {
    const path = join(root, 'kline.db');
    const seed = new Database(path);
    seed.pragma('journal_mode = WAL');
    seed.exec('CREATE TABLE evidence (id INTEGER PRIMARY KEY, value TEXT)');
    seed.prepare('INSERT INTO evidence (value) VALUES (?)').run('committed-before-conversion');
    seed.close();

    const db = new Database(path);
    const settings = configureKlineSqliteSync(db);
    assert.deepEqual(settings, {
      journal_mode: 'delete',
      synchronous: 'FULL',
      mmap_size: 0,
      busy_timeout_ms: 30_000,
    });
    assert.equal(db.prepare('SELECT value FROM evidence').get().value, 'committed-before-conversion');
    assert.equal(db.pragma('quick_check', { simple: true }), 'ok');
    db.close();
    assert.equal(existsSync(`${path}-wal`), false);
    assert.equal(existsSync(`${path}-shm`), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('repository and collector open healthy databases with NFS-safe settings', () => {
  const root = mkdtempSync(join(tmpdir(), 'kline-durability-'));
  try {
    for (const kind of ['repository', 'collector']) {
      const path = join(root, `${kind}.db`);
      const lockPath = join(root, `${kind}.lock`);
      createHealthyDb(path);
      const instance = kind === 'repository'
        ? new KlineRepository(path, { writerLockPath: lockPath })
        : new KlineCollector({ dbPath: path, writerLockPath: lockPath });
      assert.equal(instance.db.pragma('journal_mode', { simple: true }), 'delete');
      assert.equal(instance.db.pragma('synchronous', { simple: true }), 2);
      assert.equal(instance.db.pragma('mmap_size', { simple: true }), 0);
      if (kind === 'collector') instance.stop();
      else instance.close();
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('active Node writers honor the shared lock-path environment override', () => {
  const root = mkdtempSync(join(tmpdir(), 'kline-durability-env-'));
  const previous = process.env.KLINE_SQLITE_WRITER_LOCK_FILE;
  try {
    const path = join(root, 'kline.db');
    const lockPath = join(root, 'shared.lock');
    createHealthyDb(path);
    process.env.KLINE_SQLITE_WRITER_LOCK_FILE = lockPath;
    const repository = new KlineRepository(path);
    assert.equal(repository.writerLockPath, lockPath);
    repository.close();
  } finally {
    if (previous === undefined) delete process.env.KLINE_SQLITE_WRITER_LOCK_FILE;
    else process.env.KLINE_SQLITE_WRITER_LOCK_FILE = previous;
    rmSync(root, { recursive: true, force: true });
  }
});

test('writer lock times out rather than allowing concurrent ownership', () => {
  const root = mkdtempSync(join(tmpdir(), 'kline-durability-'));
  const lockPath = join(root, 'writer.lock');
  try {
    const first = acquireKlineWriterLockSync({ lockPath, owner: 'first' });
    assert.throws(
      () => withKlineWriterLockSync('second', () => {}, {
        lockPath,
        timeoutMs: 20,
        pollMs: 5,
      }),
      /Timed out waiting for Kline SQLite writer lock/,
    );
    releaseKlineWriterLockSync(first);
    assert.equal(existsSync(lockPath), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('writer lock recovers a deterministic dead-owner lock', () => {
  const root = mkdtempSync(join(tmpdir(), 'kline-durability-'));
  const lockPath = join(root, 'writer.lock');
  try {
    writeFileSync(lockPath, JSON.stringify({
      schema_version: 'kline_sqlite_writer_lock.v1',
      token: 'dead-owner',
      pid: 2_147_483_647,
      owner: 'dead-test-owner',
    }));
    const result = withKlineWriterLockSync('replacement', () => 'acquired', {
      lockPath,
      timeoutMs: 100,
    });
    assert.equal(result, 'acquired');
    assert.equal(existsSync(lockPath), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('mixed Node and Python contenders serialize stale-lock cleanup', async () => {
  const root = mkdtempSync(join(tmpdir(), 'kline-durability-race-'));
  const lockPath = join(root, 'writer.lock');
  const cleanupPath = `${lockPath}.cleanup`;
  const sentinelPath = join(root, 'critical-section');
  const barrierPath = join(root, 'start');
  const moduleUrl = new URL('../src/market-data/kline-sqlite-durability.js', import.meta.url).href;
  const scriptsDir = new URL('../scripts/', import.meta.url).pathname;
  const pythonExecutable = process.env.KLINE_TEST_PYTHON || 'python3';
  const childEnv = {
    ...process.env,
    TEST_LOCK_PATH: lockPath,
    TEST_SENTINEL_PATH: sentinelPath,
    TEST_BARRIER_PATH: barrierPath,
    TEST_KLINE_MODULE_URL: moduleUrl,
    TEST_SCRIPTS_DIR: scriptsDir,
  };
  const nodeProgram = `
    import { closeSync, existsSync, openSync, unlinkSync } from 'node:fs';
    const { withKlineWriterLockSync } = await import(process.env.TEST_KLINE_MODULE_URL);
    while (!existsSync(process.env.TEST_BARRIER_PATH)) {
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 2);
    }
    withKlineWriterLockSync('node-race-contender', () => {
      const fd = openSync(process.env.TEST_SENTINEL_PATH, 'wx', 0o600);
      try { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10); }
      finally { closeSync(fd); unlinkSync(process.env.TEST_SENTINEL_PATH); }
    }, { lockPath: process.env.TEST_LOCK_PATH, timeoutMs: 3000, pollMs: 2 });
  `;
  const pythonProgram = `
import os
import sys
import time
sys.path.insert(0, os.environ["TEST_SCRIPTS_DIR"])
from kline_sqlite_durability import kline_single_writer
while not os.path.exists(os.environ["TEST_BARRIER_PATH"]):
    time.sleep(0.002)
with kline_single_writer("python-race-contender", lock_file=os.environ["TEST_LOCK_PATH"], timeout_sec=3):
    fd = os.open(os.environ["TEST_SENTINEL_PATH"], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        time.sleep(0.01)
    finally:
        os.close(fd)
        os.unlink(os.environ["TEST_SENTINEL_PATH"])
  `;

  try {
    for (let round = 0; round < 5; round += 1) {
      writeFileSync(lockPath, JSON.stringify({
        schema_version: 'kline_sqlite_writer_lock.v1',
        token: `dead-race-owner-${round}`,
        pid: 2_147_483_647,
        owner: 'dead-race-owner',
      }));
      const children = [];
      for (let index = 0; index < 8; index += 1) {
        children.push(spawn(process.execPath, ['--input-type=module', '-e', nodeProgram], {
          cwd: process.cwd(),
          env: childEnv,
          stdio: ['ignore', 'pipe', 'pipe'],
        }));
        children.push(spawn(pythonExecutable, ['-c', pythonProgram], {
          cwd: process.cwd(),
          env: childEnv,
          stdio: ['ignore', 'pipe', 'pipe'],
        }));
      }
      const resultsPromise = Promise.all(children.map(waitForChild));
      writeFileSync(barrierPath, `start-${round}`);
      const results = await resultsPromise;
      assert.deepEqual(
        results.map(({ code }) => code),
        Array(results.length).fill(0),
        results.map(({ code, signal, stdout, stderr }) => ({ round, code, signal, stdout, stderr })),
      );
      assert.equal(existsSync(sentinelPath), false);
      assert.equal(existsSync(lockPath), false);
      assert.equal(existsSync(cleanupPath), false);
      rmSync(barrierPath, { force: true });
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('raw-path supervisor suppresses unhealthy-kline exit 78 retries', () => {
  const source = readFileSync(new URL('../src/index.js', import.meta.url), 'utf8');
  assert.match(source, /const retrySuppressed = code === 78;/);
  assert.match(source, /retry_suppression_reason: retrySuppressed \? 'kline_db_unhealthy' : null/);
  assert.match(
    source,
    /if \(retrySuppressed\) \{[\s\S]*retry suppressed until service restart because kline DB is unhealthy[\s\S]*return;[\s\S]*scheduleNext\(\);/,
  );
});
