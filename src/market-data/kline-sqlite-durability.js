import { randomUUID } from 'node:crypto';
import {
  closeSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

const DEFAULT_BUSY_TIMEOUT_MS = 30_000;
const DEFAULT_LOCK_TIMEOUT_MS = 30_000;
const DEFAULT_LOCK_POLL_MS = 25;
const INVALID_LOCK_GRACE_MS = 5_000;

function sleepSync(ms) {
  const buffer = new SharedArrayBuffer(4);
  Atomics.wait(new Int32Array(buffer), 0, 0, ms);
}

function processIsAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error?.code === 'EPERM';
  }
}

function readLockOwner(lockPath) {
  try {
    return JSON.parse(readFileSync(lockPath, 'utf8'));
  } catch {
    return null;
  }
}

function staleLockReason(lockPath, owner) {
  if (owner?.pid && !processIsAlive(Number(owner.pid))) {
    return 'owner_process_not_alive';
  }
  if (owner) return null;
  try {
    return Date.now() - statSync(lockPath).mtimeMs >= INVALID_LOCK_GRACE_MS
      ? 'invalid_owner_record'
      : null;
  } catch {
    return null;
  }
}

function sameLockOwner(expected, current) {
  if (expected?.token || current?.token) return expected?.token === current?.token;
  if (expected?.pid || current?.pid) {
    return Number(expected?.pid) === Number(current?.pid)
      && expected?.owner === current?.owner;
  }
  return expected === null && current === null;
}

function tryAcquireCleanupGate(lockPath) {
  const cleanupPath = `${lockPath}.cleanup`;
  const token = randomUUID();
  let fd;
  try {
    fd = openSync(cleanupPath, 'wx', 0o600);
    writeFileSync(fd, JSON.stringify({
      schema_version: 'kline_sqlite_cleanup_gate.v1',
      token,
      pid: process.pid,
      acquired_at: new Date().toISOString(),
    }));
    fsyncSync(fd);
    return { cleanupPath, fd, token };
  } catch (error) {
    if (fd !== undefined) {
      try { closeSync(fd); } catch {}
      try { unlinkSync(cleanupPath); } catch {}
    }
    if (error?.code === 'EEXIST') return null;
    throw error;
  }
}

function releaseCleanupGate(gate) {
  if (!gate) return;
  try {
    const owner = readLockOwner(gate.cleanupPath);
    if (owner?.token === gate.token) {
      try {
        unlinkSync(gate.cleanupPath);
      } catch (error) {
        if (error?.code !== 'ENOENT') throw error;
      }
    }
  } finally {
    try { closeSync(gate.fd); } catch {}
  }
}

function removeStaleLock(lockPath, observedOwner) {
  const gate = tryAcquireCleanupGate(lockPath);
  if (!gate) return false;
  try {
    const currentOwner = readLockOwner(lockPath);
    const reason = staleLockReason(lockPath, currentOwner);
    if (!reason || !sameLockOwner(observedOwner, currentOwner)) return false;

    const stalePath = `${lockPath}.stale-${Date.now()}-${process.pid}-${randomUUID()}`;
    try {
      renameSync(lockPath, stalePath);
    } catch (error) {
      if (error?.code === 'ENOENT') return false;
      throw error;
    }
    try {
      unlinkSync(stalePath);
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
    }
    console.warn(`[kline-sqlite] removed stale writer lock (${reason}): ${lockPath}`);
    return true;
  } finally {
    releaseCleanupGate(gate);
  }
}

export function defaultKlineWriterLockPath(dbPath) {
  void dbPath;
  return join(tmpdir(), 'kline_sqlite_single_writer.lock');
}

export function configureKlineSqliteSync(db, options = {}) {
  const requestedJournalMode = String(
    options.journalMode || process.env.KLINE_SQLITE_JOURNAL_MODE || 'DELETE',
  ).trim().toUpperCase();
  if (requestedJournalMode !== 'DELETE') {
    throw new Error(`Kline SQLite requires journal_mode=DELETE; received ${requestedJournalMode}`);
  }

  const busyTimeoutMs = Math.max(
    1,
    Number(options.busyTimeoutMs || process.env.KLINE_SQLITE_BUSY_TIMEOUT_MS || DEFAULT_BUSY_TIMEOUT_MS),
  );
  db.pragma(`busy_timeout = ${Math.floor(busyTimeoutMs)}`);
  db.pragma('mmap_size = 0');
  const journalMode = String(db.pragma('journal_mode = DELETE', { simple: true })).toLowerCase();
  db.pragma('synchronous = FULL');
  const synchronous = Number(db.pragma('synchronous', { simple: true }));
  const mmapSize = Number(db.pragma('mmap_size', { simple: true }));

  if (journalMode !== 'delete' || synchronous !== 2 || mmapSize !== 0) {
    throw new Error(
      `Unsafe Kline SQLite settings: journal_mode=${journalMode} synchronous=${synchronous} mmap_size=${mmapSize}`,
    );
  }
  return {
    journal_mode: journalMode,
    synchronous: 'FULL',
    mmap_size: mmapSize,
    busy_timeout_ms: Math.floor(busyTimeoutMs),
  };
}

export function acquireKlineWriterLockSync(options = {}) {
  const lockPath = options.lockPath
    || process.env.KLINE_SQLITE_WRITER_LOCK_FILE
    || defaultKlineWriterLockPath(options.dbPath || 'default-kline-db');
  const timeoutMs = Math.max(1, Number(options.timeoutMs || DEFAULT_LOCK_TIMEOUT_MS));
  const pollMs = Math.max(1, Number(options.pollMs || DEFAULT_LOCK_POLL_MS));
  const token = randomUUID();
  const deadline = Date.now() + timeoutMs;
  mkdirSync(dirname(lockPath), { recursive: true });

  while (true) {
    let fd;
    try {
      fd = openSync(lockPath, 'wx', 0o600);
      writeFileSync(fd, JSON.stringify({
        schema_version: 'kline_sqlite_writer_lock.v1',
        token,
        pid: process.pid,
        owner: options.owner || 'unknown',
        acquired_at: new Date().toISOString(),
      }));
      fsyncSync(fd);
      return { fd, lockPath, token };
    } catch (error) {
      if (fd !== undefined) {
        try { closeSync(fd); } catch {}
      }
      if (error?.code !== 'EEXIST') throw error;

      const owner = readLockOwner(lockPath);
      const staleReason = staleLockReason(lockPath, owner);
      if (staleReason && removeStaleLock(lockPath, owner)) {
        continue;
      }
      if (Date.now() >= deadline) {
        const activeOwner = owner
          ? `pid=${owner.pid ?? 'unknown'} owner=${owner.owner ?? 'unknown'}`
          : 'owner=initializing';
        throw new Error(`Timed out waiting for Kline SQLite writer lock ${lockPath} (${activeOwner})`);
      }
      sleepSync(Math.min(pollMs, Math.max(1, deadline - Date.now())));
    }
  }
}

export function releaseKlineWriterLockSync(lock) {
  if (!lock) return;
  try {
    const owner = readLockOwner(lock.lockPath);
    if (owner?.token === lock.token) {
      try {
        unlinkSync(lock.lockPath);
      } catch (error) {
        if (error?.code !== 'ENOENT') throw error;
      }
    }
  } finally {
    try { closeSync(lock.fd); } catch {}
  }
}

export function withKlineWriterLockSync(owner, callback, options = {}) {
  const lock = acquireKlineWriterLockSync({ ...options, owner });
  try {
    return callback();
  } finally {
    releaseKlineWriterLockSync(lock);
  }
}
