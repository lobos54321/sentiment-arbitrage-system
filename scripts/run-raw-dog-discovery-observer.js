#!/usr/bin/env node
import { dirname, isAbsolute, join } from 'path';
import { fileURLToPath } from 'url';

import Database from 'better-sqlite3';

import {
  buildRawDogDiscoveryApiPayloadFromRollingSummary,
  buildRawDogDiscoverySnapshot,
  readRawSignalOutcomeRollingSummary,
  writeRawDogDiscoveryApiSnapshot,
} from '../src/web/dashboard-server.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

function envInt(name, defaultValue, minValue, maxValue) {
  const raw = Number.parseInt(String(process.env[name] ?? defaultValue), 10);
  const value = Number.isFinite(raw) ? raw : defaultValue;
  return Math.max(minValue, Math.min(maxValue, value));
}

function resolvePath(raw) {
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function openReadonlySqlite(dbPath) {
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  try { db.pragma('mmap_size = 0'); } catch {}
  try { db.pragma('query_only = ON'); } catch {}
  return db;
}

function main() {
  const nowTs = Math.floor(Date.now() / 1000);
  const windowHours = envInt('RAW_DOG_DISCOVERY_OBSERVER_WINDOW_HOURS', 24, 1, 168);
  const limit = envInt('RAW_DOG_DISCOVERY_OBSERVER_LIMIT', 5000, 100, 50000);
  const horizonSec = envInt('RAW_DOG_DISCOVERY_OBSERVER_HORIZON_SEC', 7200, 300, 24 * 3600);
  const baselineMaxLagSec = envInt('RAW_DOG_DISCOVERY_OBSERVER_BASELINE_MAX_LAG_SEC', 300, 0, 3600);
  const coverageTargetPct = envInt('RAW_DOG_DISCOVERY_OBSERVER_COVERAGE_TARGET_PCT', 80, 0, 100);
  const signalDbPath = resolvePath(
    process.env.DB_PATH
      || process.env.SENTIMENT_DB
      || join('data', 'sentiment_arb.db'),
  );
  let signalDb;
  try {
    signalDb = openReadonlySqlite(signalDbPath);
    const snapshot = buildRawDogDiscoverySnapshot({
      signalDb,
      sinceTs: nowTs - windowHours * 3600,
      limit,
      nowTs,
      horizonSec,
      baselineMaxLagSec,
      coverageTargetPct,
      persist: true,
    });
    const apiSummary = readRawSignalOutcomeRollingSummary({
      hours: windowHours,
      limit: Math.min(limit, 500),
      coverageTargetPct,
    });
    const apiPayload = buildRawDogDiscoveryApiPayloadFromRollingSummary(apiSummary, {
      hours: windowHours,
      limit: Math.min(limit, 500),
      coverageTargetPct,
      source: 'raw_dog_discovery_worker_snapshot',
    });
    const materializedSnapshot = writeRawDogDiscoveryApiSnapshot(apiPayload);
    const summary = snapshot.report?.summary || null;
    const out = {
      schema_version: 'raw_dog_discovery_observer_run.v1',
      available: Boolean(snapshot.available),
      generated_at: snapshot.generated_at || new Date(nowTs * 1000).toISOString(),
      signal_db_path: signalDbPath,
      raw_db_path: snapshot.raw_db_path,
      filters: snapshot.filters,
      summary,
      materialized_snapshot: {
        path: materializedSnapshot.path,
        bytes: materializedSnapshot.bytes,
        available: Boolean(materializedSnapshot.payload?.available),
        source: materializedSnapshot.payload?.source || null,
      },
      diagnostics: {
        signals: snapshot.diagnostics?.signals || null,
        raw_path: {
          preferred_path_rows: snapshot.diagnostics?.raw_path?.preferred_path_rows ?? null,
          preferred_tokens: snapshot.diagnostics?.raw_path?.preferred_tokens ?? null,
          preferred_by_source_kind: snapshot.diagnostics?.raw_path?.preferred_by_source_kind || null,
          preferred_by_provider: snapshot.diagnostics?.raw_path?.preferred_by_provider || null,
          early_15m: snapshot.diagnostics?.raw_path?.early_15m || null,
          raw_db_error: snapshot.diagnostics?.raw_path?.raw_db_error || null,
        },
        paper: snapshot.diagnostics?.paper || null,
        decision_evidence: snapshot.diagnostics?.decision_evidence || null,
        raw_db: snapshot.diagnostics?.raw_db || null,
      },
      error: snapshot.error || snapshot.diagnostics?.raw_db?.error || null,
    };
    process.stdout.write(`${JSON.stringify(out, null, 2)}\n`);
    process.exit(snapshot.available && !out.error ? 0 : 2);
  } catch (error) {
    process.stderr.write(`[RAW_DOG_DISCOVERY_OBSERVER] failed: ${error?.stack || error?.message || String(error)}\n`);
    process.exit(1);
  } finally {
    try { if (signalDb) signalDb.close(); } catch {}
  }
}

main();
