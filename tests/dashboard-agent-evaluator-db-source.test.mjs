import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

const root = path.resolve(import.meta.dirname, '..');
const dashboard = fs.readFileSync(path.join(root, 'src/web/dashboard-server.js'), 'utf8');

test('scheduled AutoLoop fails closed unless a validated cross-DB snapshot exists', () => {
  assert.match(dashboard, /AGENT_CAPTURE_EVIDENCE_DB/);
  assert.match(dashboard, /cross_db_evaluator_snapshot\.v1/);
  assert.match(dashboard, /blocked_evaluator_snapshot_required/);
  assert.match(dashboard, /active_\$\{name\}_db_forbidden_for_evaluator/);
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
