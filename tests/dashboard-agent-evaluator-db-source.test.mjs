import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

import {
  evaluatorSnapshotProvenance,
  runEvaluatorSnapshotPreflight,
  runEvaluatorSnapshotPreflightAsync,
} from '../src/web/evaluator-snapshot-preflight.js';

const root = path.resolve(import.meta.dirname, '..');
const dashboard = fs.readFileSync(path.join(root, 'src/web/dashboard-server.js'), 'utf8');

test('scheduled AutoLoop fails closed unless a validated cross-DB snapshot exists', () => {
  assert.match(dashboard, /AGENT_CAPTURE_EVIDENCE_DB/);
  assert.match(dashboard, /runEvaluatorSnapshotPreflight/);
  assert.match(dashboard, /scripts', 'evaluator_db_contract\.py/);
  assert.match(dashboard, /blocked_evaluator_snapshot_required/);
  assert.match(dashboard, /'--signal-db', evaluatorDb\.evidence_databases\.signal/);
  assert.match(dashboard, /'--paper-db', evaluatorDb\.evidence_db/);
  assert.match(dashboard, /'--raw-db', evaluatorDb\.evidence_databases\.raw/);
  assert.match(dashboard, /'--kline-db', evaluatorDb\.evidence_databases\.kline/);
  assert.match(dashboard, /'--evidence-manifest', evaluatorDb\.evidence_manifest/);
  assert.match(dashboard, /'--evidence-lock-file', process\.env\.EVALUATOR_SNAPSHOT_LOCK_FILE/);
  assert.doesNotMatch(
    dashboard,
    /'scripts\/agent_capture_discovery_loop\.py',[\s\S]{0,160}'--paper-db', getPaperDbPath\(\)/,
  );
});

function acceptedPreflightPayload() {
  return {
    schema_version: 'evaluator_snapshot_bundle_contract.v1',
    accepted: true,
    blockers: [],
    snapshot_id: 'snapshot-1',
    snapshot_ts: 123,
    manifest_path: '/snapshot/snapshots/snapshot-1/manifest.json',
    manifest_sha256: 'a'.repeat(64),
    producer_status_path: '/snapshot/snapshot_status.json',
    producer_status_schema_version: 'cross_db_evaluator_snapshot_worker_status.v1',
    producer_status: {
      status: 'completed',
      accepted: true,
      last_accepted_snapshot: {
        snapshot_id: 'snapshot-1',
        manifest_sha256: 'a'.repeat(64),
      },
      promotion_allowed: false,
    },
    databases: {
      signal: '/snapshot/snapshots/snapshot-1/signal.db',
      paper: '/snapshot/snapshots/snapshot-1/paper_evidence.db',
      raw: '/snapshot/snapshots/snapshot-1/raw.db',
      kline: '/snapshot/snapshots/snapshot-1/kline.db',
    },
    promotion_allowed: false,
  };
}

function preflightOptions(runner) {
  return {
    pythonBin: 'python3',
    contractScript: '/repo/scripts/evaluator_db_contract.py',
    repoRoot: '/repo',
    dataDir: '/data',
    candidates: {
      signal: '/snapshot/signal.db',
      paper: '/snapshot/paper_evidence.db',
      raw: '/snapshot/raw.db',
      kline: '/snapshot/kline.db',
    },
    live: {
      signal: '/data/sentiment_arb.db',
      paper: '/data/paper_trades.db',
      raw: '/data/raw_signal_outcomes.db',
      kline: '/data/kline_cache.db',
    },
    manifestPath: '/snapshot/manifest.json',
    producerStatusPath: '/snapshot/snapshot_status.json',
    maxAgeSec: 28800,
    timeoutMs: 300000,
    runner,
  };
}

test('dashboard delegates acceptance to the authoritative Python contract', () => {
  let observedArgs = null;
  const status = runEvaluatorSnapshotPreflight(preflightOptions((_python, args) => {
    observedArgs = args;
    return JSON.stringify({
      schema_version: 'evaluator_snapshot_bundle_contract.v1',
      accepted: true,
      blockers: [],
      snapshot_id: 'snapshot-1',
      snapshot_ts: 123,
      manifest_path: '/snapshot/snapshots/snapshot-1/manifest.json',
      manifest_sha256: 'a'.repeat(64),
      producer_status_path: '/snapshot/snapshot_status.json',
      producer_status_schema_version: 'cross_db_evaluator_snapshot_worker_status.v1',
      producer_status: {
        status: 'completed',
        accepted: true,
        last_accepted_snapshot: {
          snapshot_id: 'snapshot-1',
          manifest_sha256: 'a'.repeat(64),
        },
        promotion_allowed: false,
      },
      databases: {
        signal: '/snapshot/snapshots/snapshot-1/signal.db',
        paper: '/snapshot/snapshots/snapshot-1/paper_evidence.db',
        raw: '/snapshot/snapshots/snapshot-1/raw.db',
        kline: '/snapshot/snapshots/snapshot-1/kline.db',
      },
      promotion_allowed: false,
    });
  }));

  assert.equal(status.accepted, true);
  assert.equal(status.promotion_allowed, false);
  assert.equal(status.evidence_db, '/snapshot/snapshots/snapshot-1/paper_evidence.db');
  assert.equal(status.evidence_manifest, '/snapshot/snapshots/snapshot-1/manifest.json');
  assert.ok(observedArgs.includes('--live-paper-db'));
  assert.ok(observedArgs.includes('/data/paper_trades.db'));
  assert.ok(observedArgs.includes('--manifest-path'));
  assert.ok(observedArgs.includes('--producer-status-path'));
  assert.ok(observedArgs.includes('/snapshot/snapshot_status.json'));
  const provenance = evaluatorSnapshotProvenance({
    ...status,
    verified_integrity: {
      paper: {
        sha256: 'c'.repeat(64),
        sha256_matches_manifest: true,
        quick_check: ['ok'],
      },
    },
  });
  assert.equal(provenance.schema_version, 'evaluator_snapshot_provenance.v1');
  assert.equal(provenance.accepted, true);
  assert.equal(provenance.snapshot_id, 'snapshot-1');
  assert.equal(provenance.manifest_sha256, 'a'.repeat(64));
  assert.equal(provenance.producer_status_path, '/snapshot/snapshot_status.json');
  assert.equal(provenance.producer_manifest_sha256, 'a'.repeat(64));
  assert.equal(provenance.databases.paper.sha256_matches_manifest, true);
  assert.deepEqual(provenance.databases.paper.quick_check, ['ok']);
  assert.equal(provenance.promotion_allowed, false);
});

test('dashboard authoritative preflight runs asynchronously without blocking the event loop', async () => {
  let releaseRunner;
  let runnerStarted = false;
  const pending = runEvaluatorSnapshotPreflightAsync(preflightOptions(async () => {
    runnerStarted = true;
    await new Promise((resolve) => {
      releaseRunner = resolve;
    });
    return JSON.stringify(acceptedPreflightPayload());
  }));

  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(runnerStarted, true);
  assert.equal(typeof releaseRunner, 'function');
  releaseRunner();
  const status = await pending;
  assert.equal(status.accepted, true);
  assert.equal(status.snapshot_id, 'snapshot-1');
  assert.equal(status.promotion_allowed, false);
});

test('dashboard asynchronous preflight classifies bounded timeout fail closed', async () => {
  const timeout = new Error('operation timed out');
  timeout.code = 'ETIMEDOUT';
  const status = await runEvaluatorSnapshotPreflightAsync(
    preflightOptions(async () => {
      throw timeout;
    }),
  );
  assert.equal(status.accepted, false);
  assert.deepEqual(status.blockers, [
    'evaluator_snapshot_authoritative_preflight_timeout',
  ]);
  assert.equal(status.promotion_allowed, false);
});

test('dashboard preflight fails closed on falsy or malformed authoritative output', () => {
  const nullStatus = runEvaluatorSnapshotPreflight(preflightOptions(() => 'null'));
  assert.equal(nullStatus.accepted, false);
  assert.deepEqual(nullStatus.blockers, [
    'evaluator_snapshot_authoritative_preflight_invalid_structure',
  ]);

  const malformedStatus = runEvaluatorSnapshotPreflight(preflightOptions(() => '{'));
  assert.equal(malformedStatus.accepted, false);
  assert.deepEqual(malformedStatus.blockers, [
    'evaluator_snapshot_authoritative_preflight_invalid_json',
  ]);

  const failureStatus = runEvaluatorSnapshotPreflight(preflightOptions(() => {
    throw new Error('python unavailable');
  }));
  assert.equal(failureStatus.accepted, false);
  assert.deepEqual(failureStatus.blockers, [
    'evaluator_snapshot_authoritative_preflight_failed',
  ]);

  const incompleteSuccess = runEvaluatorSnapshotPreflight(preflightOptions(() => JSON.stringify({
    accepted: true,
    blockers: [],
  })));
  assert.equal(incompleteSuccess.accepted, false);
  assert.deepEqual(incompleteSuccess.blockers, [
    'evaluator_snapshot_authoritative_preflight_invalid_contract',
  ]);

  const producerMismatch = runEvaluatorSnapshotPreflight(preflightOptions(() => JSON.stringify({
    schema_version: 'evaluator_snapshot_bundle_contract.v1',
    accepted: true,
    blockers: [],
    snapshot_id: 'snapshot-1',
    snapshot_ts: 123,
    manifest_path: '/snapshot/snapshots/snapshot-1/manifest.json',
    manifest_sha256: 'a'.repeat(64),
    producer_status_path: '/snapshot/snapshot_status.json',
    producer_status_schema_version: 'cross_db_evaluator_snapshot_worker_status.v1',
    producer_status: {
      last_accepted_snapshot: {
        snapshot_id: 'snapshot-1',
        manifest_sha256: 'b'.repeat(64),
      },
      promotion_allowed: false,
    },
    databases: {
      signal: '/snapshot/snapshots/snapshot-1/signal.db',
      paper: '/snapshot/snapshots/snapshot-1/paper_evidence.db',
      raw: '/snapshot/snapshots/snapshot-1/raw.db',
      kline: '/snapshot/snapshots/snapshot-1/kline.db',
    },
    promotion_allowed: false,
  })));
  assert.equal(producerMismatch.accepted, false);
  assert.deepEqual(producerMismatch.blockers, [
    'evaluator_snapshot_authoritative_preflight_invalid_contract',
  ]);

  const contradictorySuccess = runEvaluatorSnapshotPreflight(preflightOptions(() => JSON.stringify({
    schema_version: 'wrong-schema',
    accepted: true,
    blockers: ['active_paper_db_forbidden_for_evaluator'],
    snapshot_id: 'snapshot-1',
    snapshot_ts: 123,
    manifest_path: '/snapshot/manifest.json',
    manifest_sha256: 'b'.repeat(64),
    promotion_allowed: false,
  })));
  assert.equal(contradictorySuccess.accepted, false);
  assert.deepEqual(contradictorySuccess.blockers, [
    'evaluator_snapshot_authoritative_preflight_invalid_contract',
  ]);
});
