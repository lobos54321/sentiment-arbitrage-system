import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readSync,
  renameSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { basename, dirname, join, resolve } from 'node:path';

const SQLITE_HEADER = Buffer.from('SQLite format 3\0', 'binary');

export class KlineDatabaseHealthError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'KlineDatabaseHealthError';
    this.code = 'KLINE_DB_UNHEALTHY';
    this.exitCode = 78;
    this.details = details;
  }
}

export function inspectSqliteHeaderSync(dbPath) {
  const path = resolve(dbPath);
  if (!existsSync(path)) {
    return {
      path,
      exists: false,
      size_bytes: null,
      sqlite_header_valid: false,
      all_zero_first_64_bytes: null,
      first_64_bytes_hex: null,
      classification: 'MISSING',
    };
  }
  const stat = statSync(path);
  const fd = openSync(path, 'r');
  const buffer = Buffer.alloc(64);
  let bytesRead = 0;
  try {
    bytesRead = readSync(fd, buffer, 0, buffer.length, 0);
  } finally {
    closeSync(fd);
  }
  const first64 = buffer.subarray(0, bytesRead);
  const sqliteHeaderValid = bytesRead >= SQLITE_HEADER.length
    && first64.subarray(0, SQLITE_HEADER.length).equals(SQLITE_HEADER);
  return {
    path,
    exists: true,
    size_bytes: stat.size,
    inode: stat.ino,
    mtime_ms: stat.mtimeMs,
    sqlite_header_valid: sqliteHeaderValid,
    all_zero_first_64_bytes: bytesRead > 0 && first64.every((value) => value === 0),
    first_64_bytes_hex: first64.toString('hex'),
    classification: sqliteHeaderValid ? 'HEADER_VALID' : 'INVALID_HEADER',
  };
}

export function defaultKlineHealthArtifactPath(dbPath) {
  return join(dirname(resolve(dbPath)), 'recovery', 'kline_repository_health.json');
}

export function writeHealthArtifactSync(artifactPath, payload) {
  const target = resolve(artifactPath);
  mkdirSync(dirname(target), { recursive: true });
  const tempPath = join(dirname(target), `.${basename(target)}.${process.pid}.${Date.now()}.tmp`);
  const body = `${JSON.stringify(payload, null, 2)}\n`;
  const fd = openSync(tempPath, 'wx', 0o600);
  try {
    writeFileSync(fd, body, 'utf8');
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(tempPath, target);
  const directoryFd = openSync(dirname(target), 'r');
  try {
    fsyncSync(directoryFd);
  } finally {
    closeSync(directoryFd);
  }
  return target;
}

export function assertSqliteHeaderReadySync(dbPath, options = {}) {
  const inspection = inspectSqliteHeaderSync(dbPath);
  if (inspection.classification === 'HEADER_VALID') return inspection;
  const artifactPath = options.healthArtifactPath
    || process.env.KLINE_DB_HEALTH_ARTIFACT
    || defaultKlineHealthArtifactPath(dbPath);
  const artifact = {
    schema_version: 'kline_repository_health.v1',
    generated_at: new Date().toISOString(),
    classification: inspection.classification,
    action: 'FAIL_CLOSED_NO_DATABASE_OPEN',
    retry_suppression_recommended: true,
    mutation_performed: false,
    database: inspection,
  };
  try {
    writeHealthArtifactSync(artifactPath, artifact);
  } catch (error) {
    artifact.artifact_write_error = String(error?.message || error);
  }
  throw new KlineDatabaseHealthError(
    `Refusing to open unavailable or invalid SQLite file: ${inspection.path}`,
    { ...artifact, artifact_path: artifactPath },
  );
}

export function openExistingHealthySqliteSync(DatabaseClass, dbPath, options = {}) {
  const path = resolve(dbPath);
  const healthArtifactPath = options.healthArtifactPath
    || process.env.KLINE_DB_HEALTH_ARTIFACT
    || defaultKlineHealthArtifactPath(path);
  assertSqliteHeaderReadySync(path, { healthArtifactPath });
  let db;
  let quickCheck = null;
  try {
    db = new DatabaseClass(path, {
      ...(options.databaseOptions || {}),
      fileMustExist: true,
    });
    quickCheck = db.pragma('quick_check', { simple: true });
  } catch (error) {
    try { db?.close(); } catch {}
    const details = {
      schema_version: 'kline_repository_health.v1',
      generated_at: new Date().toISOString(),
      classification: 'MALFORMED',
      action: options.failureAction || 'FAIL_CLOSED_BEFORE_WRITE',
      database_path: path,
      quick_check: quickCheck,
      error: String(error?.message || error),
      mutation_performed: false,
    };
    try { writeHealthArtifactSync(healthArtifactPath, details); } catch {}
    throw new KlineDatabaseHealthError(`Kline database open/quick_check failed: ${path}`, details);
  }
  if (quickCheck !== 'ok') {
    try { db.close(); } catch {}
    const details = {
      schema_version: 'kline_repository_health.v1',
      generated_at: new Date().toISOString(),
      classification: 'MALFORMED',
      action: options.failureAction || 'FAIL_CLOSED_BEFORE_WRITE',
      database_path: path,
      quick_check: quickCheck,
      mutation_performed: false,
    };
    try { writeHealthArtifactSync(healthArtifactPath, details); } catch {}
    throw new KlineDatabaseHealthError(`Kline database quick_check failed: ${path}`, details);
  }
  return db;
}

export { SQLITE_HEADER };
