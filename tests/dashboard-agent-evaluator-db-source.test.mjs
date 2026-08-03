import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

import { runEvaluatorSnapshotPreflight } from '../src/web/evaluator-snapshot-preflight.js';

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
      manifest_path: '/snapshot/manifest.json',
      promotion_allowed: false,
    });
  }));

  assert.equal(status.accepted, true);
  assert.equal(status.promotion_allowed, false);
  assert.ok(observedArgs.includes('--live-paper-db'));
  assert.ok(observedArgs.includes('/data/paper_trades.db'));
  assert.ok(observedArgs.includes('--manifest-path'));
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

  const contradictorySuccess = runEvaluatorSnapshotPreflight(preflightOptions(() => JSON.stringify({
    schema_version: 'wrong-schema',
    accepted: true,
    blockers: ['active_paper_db_forbidden_for_evaluator'],
    snapshot_id: 'snapshot-1',
    snapshot_ts: 123,
    manifest_path: '/snapshot/manifest.json',
    promotion_allowed: false,
  })));
  assert.equal(contradictorySuccess.accepted, false);
  assert.deepEqual(contradictorySuccess.blockers, [
    'evaluator_snapshot_authoritative_preflight_invalid_contract',
  ]);
});
