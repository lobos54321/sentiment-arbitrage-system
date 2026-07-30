import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

const root = path.resolve(import.meta.dirname, '..');
const startup = fs.readFileSync(path.join(root, 'scripts/run_zeabur_services.sh'), 'utf8');
const dashboard = fs.readFileSync(path.join(root, 'src/web/dashboard-server.js'), 'utf8');
const pumpObserver = fs.readFileSync(path.join(root, 'scripts/pump_fun_shadow_observer.js'), 'utf8');
const pumpWorker = fs.readFileSync(path.join(root, 'scripts/run_pump_fun_shadow_worker.sh'), 'utf8');

test('Zeabur startup restores bounded research-only workers', () => {
  assert.match(startup, /RAW_DOG_DISCOVERY_OBSERVER_ENABLED.*:-true/);
  assert.match(startup, /RAW_PATH_OBSERVER_ENABLED.*:-true/);
  assert.match(startup, /RAW_PATH_OBSERVER_MAX_SIGNALS_PER_RUN.*:-10/);
  assert.match(startup, /AGENT_CAPTURE_DISCOVERY_SCHEDULER_ENABLED.*:-true/);
  assert.match(startup, /AGENT_CAPTURE_DISCOVERY_SCHEDULER_INTERVAL_SEC.*:-21600/);
  assert.match(startup, /AGENT_CAPTURE_MAX_SCAN_ROWS.*:-250000/);
  assert.match(startup, /AGENT_CAPTURE_RUN_HISTORY_LIMIT.*:-8/);
  assert.match(startup, /PUMP_FUN_SHADOW_WORKER_ENABLED.*:-true/);
  assert.match(startup, /PUMP_FUN_SHADOW_RETENTION_DAYS.*:-30/);
  assert.match(startup, /STRATEGY_MEMORY_ARTIFACT_DIR.*strategy-memory-seed/);
});

test('dashboard scheduler is guarded, observable, and reuses the read-only runner', () => {
  assert.match(dashboard, /AGENT_CAPTURE_DISCOVERY_SCHEDULER_ENABLED/);
  assert.match(dashboard, /startRawPathObserverScheduler\(\)/);
  assert.match(dashboard, /triggerAgentCaptureDiscoveryLoop\(url, \{ trigger: 'dashboard_scheduler' \}\)/);
  assert.match(dashboard, /agent_capture_scheduler: agentCaptureSchedulerStatus\(\)/);
  assert.match(dashboard, /startRawDogDiscoveryObserver\(\);\s+startAgentCaptureDiscoveryScheduler\(\);/);
  assert.match(dashboard, /promotion_allowed: false/);
  assert.match(dashboard, /strategy_change_allowed: false/);
  assert.match(dashboard, /paper_enablement_allowed: false/);
  assert.doesNotMatch(dashboard, /runtimeDataDir\(\)/);
  assert.match(dashboard, /detached: process\.platform !== 'win32'/);
  assert.match(dashboard, /signalProcessTree\(child\.pid, 'SIGTERM'\)/);
  assert.match(dashboard, /processGroupIsAlive\(child\.pid\)/);
  assert.match(dashboard, /finishAfterProcessGroupExit/);
  assert.match(dashboard, /processGroupManaged \? processGroupIsAlive\(status\.pid\) : pidAlive/);
  assert.match(dashboard, /agentCaptureLoopRunner\.run_id === runId/);
  const schedulerBlock = dashboard.slice(
    dashboard.indexOf('function startAgentCaptureDiscoveryScheduler()'),
    dashboard.indexOf('function buildAgentCaptureDiscoveryLatestSnapshot')
  );
  assert.equal(
    (schedulerBlock.match(/scheduleNext\(initialDelaySec\);/g) || []).length,
    1,
    'AutoLoop scheduler must create one initial timer'
  );
});

test('P8 remains isolated and prunes only its own expired shadow rows', () => {
  assert.match(pumpObserver, /DELETE FROM pump_fun_shadow_signals/);
  assert.match(pumpObserver, /DELETE FROM pump_fun_shadow_runs/);
  assert.match(pumpObserver, /production_impact: 'zero_shadow_only'/);
  assert.match(pumpObserver, /writes_premium_signals: false/);
  assert.match(pumpObserver, /writes_paper_trades: false/);
  assert.match(pumpObserver, /auto_vacuum = INCREMENTAL/);
  assert.match(pumpObserver, /incremental_vacuum/);
  assert.match(pumpObserver, /reusable_bytes/);
  assert.match(pumpObserver, /legacy_database_migrated/);
  assert.match(pumpObserver, /insufficient_free_space_for_bounded_vacuum/);
  assert.match(pumpWorker, /ACTIVE_CHILD_PGID/);
  assert.match(pumpWorker, /kill -TERM -- "-\$ACTIVE_CHILD_PGID"/);
  assert.match(pumpWorker, /kill -KILL -- "-\$ACTIVE_CHILD_PGID"/);
});

test('Strategy Memory seed contains the six bounded discovery artifacts', () => {
  const seed = path.join(root, 'docs/agents/strategy-memory-seed');
  const expected = [
    'strategy_memory_hypotheses.json',
    'strategy_memory_candidate_mapping.json',
    'strategy_memory_prioritized_queue.json',
    'filtered_winner_dossier_24h.json',
    'exit_policy_shadow_simulator_24h.json',
    'execution_delay_adjusted_replay_24h.json',
  ];
  for (const file of expected) {
    const payload = JSON.parse(fs.readFileSync(path.join(seed, file), 'utf8'));
    assert.equal(typeof payload, 'object');
    assert.notEqual(payload, null);
  }
});
