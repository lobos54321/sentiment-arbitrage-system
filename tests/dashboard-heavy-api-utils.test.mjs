import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import Database from 'better-sqlite3';
import {
  apiJsonHeaders,
  apiEnvelopePayloadForHash,
  auditSha256Hex,
  buildApiResponseErrorShape,
  buildV27ManualEvidenceApiResponse,
  buildDogCatchGoalProgress,
  buildV27KpiProofStatus,
  buildStorageHealthSnapshot,
  buildClosedLoopProbeSummary,
  buildClosedLoopMissedDogSummary,
  appendDashboardAuditEvent,
  buildDashboardAuditEvent,
  boundedIntParam,
  boundedWindowedSinceTs,
  dogCatchGoalFromLiveSnapshot,
  missedRecoverySummaryFromLiveSnapshot,
  readPaperFastLaneHealth,
  readV27DenominatorReadModelHealth,
  readV27ModeReadiness,
  LOG_REDACTION_PATTERN_SET,
  redactLogMessage,
  V27_API_RESPONSE_ENVELOPE_VERSION,
  resolveDashboardLogPath,
  resetPaperReportGateForTest,
  shouldUseMaterializedMissedRecoverySummary,
  tryBeginPaperReport,
  verifyDashboardAuditChain,
} from '../src/web/dashboard-server.js';

test('redactLogMessage masks dashboard secrets without hiding token addresses', () => {
  const redacted = redactLogMessage([
    'Authorization: Bearer unit-bearer-secret',
    'GET /api/logs?token=unit-query-secret',
    'dashboard_token=unit-dashboard-secret',
    '{"wallet_private_key":"unit-wallet-secret","token_ca":"So11111111111111111111111111111111111111112"}',
  ].join(' '));

  assert.equal(LOG_REDACTION_PATTERN_SET, 'v2.7.0.secret_pattern_set.dashboard_runtime.v1');
  assert.doesNotMatch(redacted, /unit-bearer-secret|unit-query-secret|unit-dashboard-secret|unit-wallet-secret/);
  assert.match(redacted, /Authorization: Bearer \[REDACTED\]/);
  assert.match(redacted, /\?token=\[REDACTED\]/);
  assert.match(redacted, /dashboard_token=\[REDACTED\]/);
  assert.match(redacted, /"wallet_private_key":"\[REDACTED\]"/);
  assert.match(redacted, /So11111111111111111111111111111111111111112/);
});

test('apiJsonHeaders defaults JSON responses to no-store', () => {
  assert.deepEqual(apiJsonHeaders(), {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  assert.equal(apiJsonHeaders('max-age=60')['Cache-Control'], 'max-age=60');
});

test('buildV27ManualEvidenceApiResponse preserves legacy schema and rejected error shape', () => {
  const accepted = buildV27ManualEvidenceApiResponse(
    'v2.7.0.manual_read_model_refresh.v1',
    { accepted: true, status: 'started' },
    { endpoint: '/api/paper/v27-read-model-refresh', generatedAt: '2026-05-25T00:00:00.000Z' },
  );

  assert.equal(accepted.generated_at, '2026-05-25T00:00:00.000Z');
  assert.equal(accepted.materialized, false);
  assert.equal(accepted.endpoint, '/api/paper/v27-read-model-refresh');
  assert.equal(accepted.envelope_version, V27_API_RESPONSE_ENVELOPE_VERSION);
  assert.equal(accepted.response_schema_version, 'v2.7.0.manual_read_model_refresh.v1');
  assert.equal(accepted.refresh_schema_version, 'v2.7.0.manual_read_model_refresh.v1');
  assert.equal(accepted.accepted, true);
  assert.equal(accepted.status, 'started');
  assert.deepEqual(accepted.error_shape, {
    has_error: false,
    accepted: true,
    error_field: null,
    error_code: null,
    status: 'started',
  });
  assert.match(accepted.payload_hash, /^[a-f0-9]{64}$/);
  assert.equal(accepted.payload_hash, auditSha256Hex(apiEnvelopePayloadForHash(accepted)));

  const rejected = buildV27ManualEvidenceApiResponse(
    'v2.7.0.manual_read_model_refresh.v1',
    { accepted: false, status: 'already_running' },
    { endpoint: '/api/paper/v27-read-model-refresh', generatedAt: '2026-05-25T00:00:01.000Z' },
  );

  assert.equal(rejected.error, 'already_running');
  assert.equal(rejected.error_code, 'already_running');
  assert.deepEqual(rejected.error_shape, {
    has_error: true,
    accepted: false,
    error_field: 'error',
    error_code: 'already_running',
    status: 'already_running',
  });
  assert.equal(rejected.payload_hash, auditSha256Hex(apiEnvelopePayloadForHash(rejected)));
  assert.deepEqual(buildApiResponseErrorShape({ accepted: false, status: 'manual_evidence_request_rejected' }), {
    has_error: true,
    accepted: false,
    error_field: null,
    error_code: null,
    status: 'manual_evidence_request_rejected',
  });
});

test('storage health reports db markers and disk snapshot without opening sqlite', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'storage-health-'));
  const paper = join(dir, 'paper_trades.db');
  fs.writeFileSync(paper, 'sqlite-placeholder');
  fs.writeFileSync(`${paper}.integrity_error`, 'malformed page');
  fs.writeFileSync(join(dir, 'preflight.log'), '[preflight] checkpoint failed');

  const snapshot = buildStorageHealthSnapshot({
    projectRoot: dir,
    dataDir: dir,
    includeFileStats: true,
    includePreflightTail: true,
    paperDbPath: paper,
    signalDbPath: join(dir, 'sentiment_arb.db'),
    klineDbPath: join(dir, 'kline_cache.db'),
    lifecycleDbPath: join(dir, 'lifecycle_tracks.db'),
  });

  assert.equal(snapshot.db_files.find((row) => row.label === 'paper_trades').exists, true);
  assert.match(snapshot.integrity_error, /malformed page/);
  assert.match(snapshot.preflight_tail, /checkpoint failed/);
});

test('dashboard audit events form a verifiable hash chain', () => {
  const first = buildDashboardAuditEvent({
    audit_event_id: 'audit-1',
    created_at: '2026-05-25T00:00:00.000Z',
    endpoint: '/api/pause-trading',
    method: 'POST',
    required_role: 'dashboard_admin',
    token_scope: 'dashboard:risk_mutation',
    danger_level: 'admin_mutation',
    action: 'pause_trading',
    payload: { hours: 4 },
  });
  const second = buildDashboardAuditEvent({
    audit_event_id: 'audit-2',
    created_at: '2026-05-25T00:01:00.000Z',
    endpoint: '/api/resume-trading',
    method: 'POST',
    required_role: 'dashboard_admin',
    token_scope: 'dashboard:risk_mutation',
    danger_level: 'admin_mutation',
    action: 'resume_trading',
    prev_audit_hash: first.audit_chain_hash,
  });

  assert.equal(first.prev_audit_hash, 'GENESIS');
  assert.match(first.audit_payload_hash, /^[a-f0-9]{64}$/);
  assert.match(first.audit_chain_hash, /^[a-f0-9]{64}$/);
  assert.deepEqual(verifyDashboardAuditChain([first, second]), {
    ok: true,
    event_count: 2,
    failures: [],
    last_audit_chain_hash: second.audit_chain_hash,
  });

  const tampered = { ...second, payload: { changed: true } };
  const tamperReport = verifyDashboardAuditChain([first, tampered]);
  assert.equal(tamperReport.ok, false);
  assert.equal(tamperReport.failures.some((row) => row.reason === 'audit_payload_hash_mismatch'), true);
});

test('dashboard audit append continues from previous chain hash', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'dashboard-audit-'));
  const auditLogPath = join(dir, 'audit.jsonl');
  const first = appendDashboardAuditEvent({
    audit_event_id: 'append-1',
    created_at: '2026-05-25T00:00:00.000Z',
    endpoint: '/api/paper/v27-read-model-refresh',
    method: 'POST',
    required_role: 'dashboard_operator',
    token_scope: 'v27:evidence_mutation',
    danger_level: 'operator_mutation',
    action: 'v27_read_model_refresh',
  }, { auditLogPath });
  const second = appendDashboardAuditEvent({
    audit_event_id: 'append-2',
    created_at: '2026-05-25T00:01:00.000Z',
    endpoint: '/api/paper/v27-mode-readiness',
    method: 'POST',
    required_role: 'dashboard_operator',
    token_scope: 'v27:evidence_mutation',
    danger_level: 'operator_mutation',
    action: 'v27_mode_readiness',
  }, { auditLogPath });

  assert.equal(second.prev_audit_hash, first.audit_chain_hash);
  const events = fs.readFileSync(auditLogPath, 'utf8').trim().split('\n').map((line) => JSON.parse(line));
  assert.equal(events.length, 2);
  assert.equal(verifyDashboardAuditChain(events).ok, true);
});

test('v27 read model health reports missing materialized snapshot as unsafe', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'v27-health-missing-'));
  const health = readV27DenominatorReadModelHealth({
    projectRoot: dir,
    healthPath: join(dir, 'data', 'v27_read_models', 'denominator_freshness.json'),
  });

  assert.equal(health.available, false);
  assert.equal(health.dashboard_safe, false);
  assert.deepEqual(health.blocking_reasons, ['v27_read_model_health_missing']);
  assert.equal(health.health.status, 'v27_read_model_health_missing');
});

test('paper fast lane health exposes public-safe missed rescue heartbeat', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'paper-fast-lane-health-'));
  const healthPath = join(dir, 'paper-fast-lane-health.json');
  fs.writeFileSync(healthPath, JSON.stringify({
    schema_version: 'v2.7.0.paper_fast_lane_health.v1',
    updated_at: '2026-05-28T23:00:00Z',
    paper_db_exists: true,
    missed_rescue: {
      last_scan_at: '2026-05-28T23:00:00Z',
      scan_count: 3,
      error_count: 0,
      last_result: {
        rows: 30,
        processed: 12,
        queued: 2,
        watch_only: 10,
        counterfactual_only: 0,
        deduped: 0,
        backlog_lookback_sec: 86400,
      },
      last_error: null,
    },
  }));

  const health = readPaperFastLaneHealth({ healthPath });

  assert.equal(health.available, true);
  assert.equal(health.status, 'ok');
  assert.equal(health.paper_db_exists, true);
  assert.equal(health.missed_rescue.scan_count, 3);
  assert.equal(health.missed_rescue.last_result.processed, 12);
  assert.equal(health.missed_rescue.last_error, null);
});

test('v27 read model health exposes materialized verifier result', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'v27-health-ready-'));
  const healthPath = join(dir, 'denominator_freshness.json');
  fs.writeFileSync(healthPath, JSON.stringify({
    refresh_schema_version: 'v2.7.0.read_model_refresh.v1',
    snapshot_id: 'v27denom_test',
    snapshot_hash: 'abc',
    projection_hash: 'def',
    read_model_seq: 7,
    event_log_latest_seq: 7,
    dashboard_safe: true,
    blocking_reasons: [],
    health: {
      dashboard_safe: true,
      normal_tiny_ready: true,
      highest_allowed_mode: 'normal_tiny',
      status: 'read_model_refresh_ok',
    },
    mode_readiness: {
      normal_tiny_ready: true,
      highest_allowed_mode: 'normal_tiny',
    },
    verifier_report: {
      snapshot_hash_ok: true,
      projection_hash_ok: true,
      spec_valid: true,
      read_model_fresh_enough: true,
      blocking_reasons: [],
    },
  }));

  const health = readV27DenominatorReadModelHealth({ healthPath });

  assert.equal(health.available, true);
  assert.equal(health.dashboard_safe, true);
  assert.equal(health.read_model_seq, 7);
  assert.equal(health.event_log_latest_seq, 7);
  assert.equal(health.health.status, 'read_model_refresh_ok');
  assert.equal(health.health.normal_tiny_ready, true);
  assert.equal(health.health.highest_allowed_mode, 'normal_tiny');
  assert.equal(health.verifier_report.spec_valid, true);
});

test('v27 read model health blocks unsafe projection statuses even if stale payload says safe', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'v27-health-unsafe-projection-'));
  const healthPath = join(dir, 'denominator_freshness.json');
  fs.writeFileSync(healthPath, JSON.stringify({
    refresh_schema_version: 'v2.7.0.read_model_refresh.v1',
    dashboard_safe: true,
    event_log_latest_seq: 0,
    projection_status: 'event_log_invalid',
    health: {
      dashboard_safe: true,
      normal_tiny_ready: false,
      status: 'read_model_refresh_ok',
    },
    verifier_report: {
      blocking_reasons: [],
      event_log_latest_seq: 0,
      projection_status: 'event_log_invalid',
    },
  }));

  const health = readV27DenominatorReadModelHealth({ healthPath });

  assert.equal(health.available, true);
  assert.equal(health.dashboard_safe, false);
  assert.deepEqual(health.blocking_reasons, ['projection_status_event_log_invalid', 'event_log_empty']);
  assert.equal(health.health.dashboard_safe, false);
});

test('v27 mode readiness exposes materialized matrix and missing state', () => {
  const missingDir = fs.mkdtempSync(join(os.tmpdir(), 'v27-mode-readiness-missing-'));
  const missing = readV27ModeReadiness({
    projectRoot: missingDir,
    modeReadinessPath: join(missingDir, 'data', 'v27_read_models', 'mode_readiness.json'),
  });
  assert.equal(missing.available, false);
  assert.deepEqual(missing.blocking_reasons, ['v27_mode_readiness_missing']);

  const readyDir = fs.mkdtempSync(join(os.tmpdir(), 'v27-mode-readiness-ready-'));
  const modeReadinessPath = join(readyDir, 'mode_readiness.json');
  fs.writeFileSync(modeReadinessPath, JSON.stringify({
    matrix_schema_version: 'v2.7.0.mode_readiness.v1',
    highest_allowed_mode: 'normal_tiny',
    health: {
      observe_only_ready: true,
      shadow_ready: true,
      ultra_tiny_ready: true,
      normal_tiny_ready: true,
      status: 'mode_readiness_evaluated',
    },
    read_model: {
      health: {
        dashboard_safe: true,
        normal_tiny_ready: false,
      },
    },
    basic_readiness: {
      blocking_contracts: [],
      health: {
        observe_only_foundation_ready: true,
        normal_tiny_ready: false,
      },
    },
    projection_consumer: {
      health: {
        shadow_consumer_ready: true,
        normal_tiny_ready: false,
      },
    },
    contract_statuses: {
      PaperModeSafetyBoundary: {
        status: 'pass',
        evidence: {
          runtime_evidence_present: true,
          live_private_key_present: false,
        },
      },
    },
  }));

  const readiness = readV27ModeReadiness({ modeReadinessPath });
  assert.equal(readiness.available, true);
  assert.equal(readiness.highest_allowed_mode, 'normal_tiny');
  assert.equal(readiness.read_model.health.normal_tiny_ready, true);
  assert.equal(readiness.basic_readiness.health.normal_tiny_ready, true);
  assert.equal(readiness.projection_consumer.health.normal_tiny_ready, true);
  assert.equal(readiness.read_model.health.normal_tiny_ready_source, 'mode_readiness_matrix');
  assert.equal(readiness.read_model.health.read_model_fresh, true);
  assert.equal(readiness.basic_readiness.health.basic_contracts_ready, true);
  assert.equal(readiness.projection_consumer.health.projection_consumer_ready, true);
  assert.equal(readiness.contract_statuses.PaperModeSafetyBoundary.evidence.runtime_evidence_present, true);
});

test('v27 KPI proof status separates token gate from KPI failure', () => {
  const proof = buildV27KpiProofStatus({
    generatedAt: '2026-05-25T00:30:00.000Z',
    nowMs: Date.parse('2026-05-25T00:30:00.000Z'),
    requestedHours: 24,
    dashboardTokenConfigured: false,
    paperDbExists: false,
    liveSnapshot: null,
    modeReadiness: {
      available: false,
      health: { normal_tiny_ready: false, status: 'v27_mode_readiness_missing' },
    },
    denominatorHealth: {
      available: false,
      dashboard_safe: false,
      health: { normal_tiny_ready: false, status: 'v27_read_model_health_missing' },
    },
  });

  assert.equal(proof.public_safe, true);
  assert.equal(proof.claim.verified, false);
  assert.equal(proof.claim.status, 'kpi_evidence_token_gated');
  assert.deepEqual(proof.claim.metrics, {
    clean_gold_silver_capture_rate: null,
    peak_win_rate: null,
    realized_roi: null,
    eligible_gold_silver_unique: null,
    captured_gold_silver_unique: null,
    missed_clean_gold_silver_unique: null,
    fills: null,
    closed: null,
  });
  assert.deepEqual(proof.claim.target_gaps, {
    clean_gold_silver_capture_rate: null,
    peak_win_rate: null,
    realized_roi: null,
  });
  assert.equal(proof.evidence_sources.protected_paper_endpoints.status, 'token_not_configured');
  assert.deepEqual(proof.evidence_sources.dog_catch_goal, {
    available: false,
    pass: false,
    blockers: [],
  });
  assert.ok(proof.blockers.includes('dashboard_token_missing_for_protected_kpi_evidence'));
  assert.ok(proof.blockers.includes('materialized_review_snapshot_missing'));
});

test('v27 KPI proof status verifies only fresh materialized KPI chain', () => {
  const proof = buildV27KpiProofStatus({
    generatedAt: '2026-05-25T00:20:00.000Z',
    nowMs: Date.parse('2026-05-25T00:20:00.000Z'),
    requestedHours: 24,
    maxSnapshotAgeMinutes: 30,
    dashboardTokenConfigured: true,
    paperDbExists: true,
    liveSnapshot: {
      snapshot_id: 'paper_live_24h_unit',
      generated_at: '2026-05-25T00:10:00.000Z',
      dog_catch_goal: {
        available: true,
        trades: {
          fills: 4,
          closed: 4,
          peak_win_rate: 0.75,
          realized_roi: 2.4,
          captured_gold_silver_unique: 3,
        },
        missed: {
          clean_gold_silver_unique: 1,
        },
        goal: {
          pass: true,
          blockers: [],
          eligible_gold_silver_unique: 4,
          captured_gold_silver_unique: 3,
          clean_gold_silver_capture_rate: 0.75,
        },
      },
    },
    modeReadiness: {
      available: true,
      highest_allowed_mode: 'normal_tiny',
      health: {
        normal_tiny_ready: true,
        status: 'mode_readiness_evaluated',
      },
    },
    denominatorHealth: {
      available: true,
      dashboard_safe: true,
      health: {
        normal_tiny_ready: true,
        status: 'read_model_refresh_ok',
      },
    },
  });

  assert.equal(proof.claim.verified, true);
  assert.equal(proof.claim.status, 'kpi_verified');
  assert.deepEqual(proof.claim.metrics, {
    clean_gold_silver_capture_rate: 0.75,
    peak_win_rate: 0.75,
    realized_roi: 2.4,
    eligible_gold_silver_unique: 4,
    captured_gold_silver_unique: 3,
    missed_clean_gold_silver_unique: 1,
    fills: 4,
    closed: 4,
  });
  assert.deepEqual(proof.claim.target_gaps, {
    clean_gold_silver_capture_rate: -0.15,
    peak_win_rate: -0.2,
    realized_roi: -0.4,
  });
  assert.equal(proof.evidence_sources.materialized_review_snapshot.fresh, true);
  assert.equal(proof.evidence_sources.materialized_review_snapshot.age_minutes, 10);
  assert.equal(proof.evidence_sources.dog_catch_goal.pass, true);
  assert.deepEqual(proof.blockers, []);
});

test('v27 KPI proof status exposes public-safe missed blocker attribution', () => {
  const proof = buildV27KpiProofStatus({
    generatedAt: '2026-05-25T00:20:00.000Z',
    nowMs: Date.parse('2026-05-25T00:20:00.000Z'),
    requestedHours: 24,
    maxSnapshotAgeMinutes: 30,
    dashboardTokenConfigured: true,
    paperDbExists: true,
    liveSnapshot: {
      snapshot_id: 'paper_live_24h_unit',
      generated_at: '2026-05-25T00:10:00.000Z',
      dog_catch_goal: {
        available: true,
        trades: {
          fills: 4,
          closed: 4,
          peak_win_rate: 0.25,
          realized_roi: -0.5,
          captured_gold_silver_unique: 1,
        },
        missed: {
          clean_gold_silver_unique: 4,
          clean_gold_unique: 1,
          clean_silver_unique: 3,
          by_blocker: [
            {
              route: 'paper_fast_lane',
              component: 'runtime_mode_gate',
              reject_reason: 'mode_readiness_missing',
              token_ca: 'DoNotExpose1111111111111111111111111111111111',
              gold_n: 1,
              silver_n: 2,
              unique_tokens: 3,
              max_pnl: 1.42,
            },
            {
              route: 'normal_tiny',
              component: 'quote_executor',
              reject_reason: 'quote_unavailable',
              gold_n: 0,
              silver_n: 1,
              unique_tokens: 1,
              max_pnl: 0.72,
            },
          ],
          reclaim_pipeline: [
            {
              route: 'LOTTO',
              component: 'discovery_tracking',
              reject_reason: 'tracking_ttl_expired',
              rescue_state: 'stale',
              fast_lane_status: 'watch_only',
              fast_lane_reason: 'clean_dog_reclaim_recovery_tradable_signal_stale_watch_only',
              entry_branch: 'tracking_ttl_reclaim_quote_clean_tiny_probe',
              entry_mode_hint: 'lotto_not_ath_reclaim_tiny_probe',
              token_ca: 'PipelineTokenMustNotLeak1111111111111111111111',
              gold_n: 0,
              silver_n: 2,
              unique_tokens: 2,
              max_pnl: 0.88,
            },
          ],
        },
        goal: {
          pass: false,
          blockers: [
            'clean_gold_silver_capture_rate_below_target',
            'peak_win_rate_below_target',
            'realized_roi_below_target',
          ],
          eligible_gold_silver_unique: 5,
          captured_gold_silver_unique: 1,
          clean_gold_silver_capture_rate: 0.2,
        },
      },
    },
    modeReadiness: {
      available: true,
      highest_allowed_mode: 'normal_tiny',
      health: {
        normal_tiny_ready: true,
        status: 'mode_readiness_evaluated',
      },
    },
    denominatorHealth: {
      available: true,
      dashboard_safe: true,
      health: {
        normal_tiny_ready: true,
        status: 'read_model_refresh_ok',
      },
    },
  });

  const attribution = proof.claim.failure_attribution;
  assert.equal(attribution.public_safe, true);
  assert.equal(attribution.current_capture_rate, 0.2);
  assert.equal(attribution.required_captured_gold_silver_unique, 3);
  assert.equal(attribution.additional_captures_needed_for_target, 2);
  assert.deepEqual(attribution.top_missed_blocker, {
    route: 'paper_fast_lane',
    component: 'runtime_mode_gate',
    reject_reason: 'mode_readiness_missing',
    clean_gold_silver_unique: 3,
    gold_n: 1,
    silver_n: 2,
    unique_tokens: 3,
    max_pnl: 1.42,
  });
  assert.deepEqual(attribution.top_reclaim_pipeline_gap, {
    route: 'LOTTO',
    component: 'discovery_tracking',
    reject_reason: 'tracking_ttl_expired',
    rescue_state: 'stale',
    fast_lane_status: 'watch_only',
    fast_lane_reason: 'clean_dog_reclaim_recovery_tradable_signal_stale_watch_only',
    entry_branch: 'tracking_ttl_reclaim_quote_clean_tiny_probe',
    entry_mode_hint: 'lotto_not_ath_reclaim_tiny_probe',
    clean_gold_silver_unique: 2,
    gold_n: 0,
    silver_n: 2,
    unique_tokens: 2,
    max_pnl: 0.88,
  });
  assert.equal(JSON.stringify(attribution).includes('DoNotExpose'), false);
  assert.equal(JSON.stringify(attribution).includes('PipelineTokenMustNotLeak'), false);
});

test('storage health includes v27 sidecar logs for mirror diagnosis', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'storage-health-v27-logs-'));
  fs.writeFileSync(join(dir, 'v27-paper-trade-source-label-mirror.log'), 'mirror failed');
  fs.writeFileSync(join(dir, 'v27-earliest-actionable-mirror.log'), 'earliest actionable failed');
  fs.writeFileSync(join(dir, 'v27-idempotency-contract-mirror.log'), 'idempotency failed');
  fs.writeFileSync(join(dir, 'v27-raw-provider-evidence-mirror.log'), 'raw provider failed');
  fs.writeFileSync(join(dir, 'v27-raw-provider-probe-evidence.log'), 'raw provider probe failed');
  fs.writeFileSync(join(dir, 'v27-randomness-control-mirror.log'), 'randomness failed');
  fs.writeFileSync(join(dir, 'v27-normal-tiny-ops-evidence.log'), 'ops evidence failed');
  fs.writeFileSync(join(dir, 'v27-execution-control-mirror.log'), 'execution control failed');
  fs.writeFileSync(join(dir, 'v27-paper-ledger-mirror.log'), 'paper ledger failed');
  fs.writeFileSync(join(dir, 'v27-recovery-control-mirror.log'), 'recovery control failed');

  const snapshot = buildStorageHealthSnapshot({
    projectRoot: dir,
    dataDir: dir,
    includeFileStats: true,
    paperDbPath: join(dir, 'paper_trades.db'),
    signalDbPath: join(dir, 'sentiment_arb.db'),
    klineDbPath: join(dir, 'kline_cache.db'),
    lifecycleDbPath: join(dir, 'lifecycle_tracks.db'),
  });

  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-paper-trade-source-label-mirror.log').exists, true);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-trade-outcome-mirror.log').exists, false);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-standardized-stop-mirror.log').exists, false);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-ex-ante-feasibility-mirror.log').exists, false);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-earliest-actionable-mirror.log').exists, true);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-realtime-clean-mirror.log').exists, false);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-quote-intent-binding-mirror.log').exists, false);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-raw-provider-evidence-mirror.log').exists, true);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-raw-provider-probe-evidence.log').exists, true);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-randomness-control-mirror.log').exists, true);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-normal-tiny-ops-evidence.log').exists, true);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-idempotency-contract-mirror.log').exists, true);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-execution-control-mirror.log').exists, true);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-paper-ledger-mirror.log').exists, true);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-recovery-control-mirror.log').exists, true);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-read-model-refresh.log').exists, false);
  assert.equal(snapshot.log_files.find((row) => row.label === 'v27-event-log-recovery.log').exists, false);
});

test('dashboard log resolver exposes v27 mirror sidecar logs', () => {
  const env = {
    V27_TRADE_OUTCOME_MIRROR_LOG: '/tmp/trade-outcome.log',
    V27_STANDARDIZED_STOP_MIRROR_LOG: '/tmp/standardized-stop.log',
    V27_EX_ANTE_FEASIBILITY_MIRROR_LOG: '/tmp/ex-ante.log',
    V27_EARLIEST_ACTIONABLE_MIRROR_LOG: '/tmp/earliest-actionable.log',
    V27_REALTIME_CLEAN_MIRROR_LOG: '/tmp/realtime-clean.log',
    V27_QUOTE_INTENT_BINDING_MIRROR_LOG: '/tmp/quote-intent-binding.log',
    V27_RAW_PROVIDER_EVIDENCE_MIRROR_LOG: '/tmp/raw-provider-evidence.log',
    V27_RAW_PROVIDER_PROBE_EVIDENCE_LOG: '/tmp/raw-provider-probe-evidence.log',
    V27_RANDOMNESS_CONTROL_MIRROR_LOG: '/tmp/randomness-control.log',
    V27_NORMAL_TINY_OPS_EVIDENCE_LOG: '/tmp/normal-tiny-ops-evidence.log',
    V27_IDEMPOTENCY_CONTRACT_MIRROR_LOG: '/tmp/idempotency-contract.log',
    V27_EXECUTION_CONTROL_MIRROR_LOG: '/tmp/execution-control.log',
    V27_PAPER_LEDGER_MIRROR_LOG: '/tmp/paper-ledger.log',
    V27_RECOVERY_CONTROL_MIRROR_LOG: '/tmp/recovery-control.log',
  };

  assert.equal(resolveDashboardLogPath('/api/logs/v27-trade-outcome-mirror', env), '/tmp/trade-outcome.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-standardized-stop-mirror', env), '/tmp/standardized-stop.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-ex-ante-feasibility-mirror', env), '/tmp/ex-ante.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-earliest-actionable-mirror', env), '/tmp/earliest-actionable.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-realtime-clean-mirror', env), '/tmp/realtime-clean.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-quote-intent-binding-mirror', env), '/tmp/quote-intent-binding.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-raw-provider-evidence-mirror', env), '/tmp/raw-provider-evidence.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-raw-provider-probe-evidence', env), '/tmp/raw-provider-probe-evidence.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-randomness-control-mirror', env), '/tmp/randomness-control.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-normal-tiny-ops-evidence', env), '/tmp/normal-tiny-ops-evidence.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-idempotency-contract-mirror', env), '/tmp/idempotency-contract.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-execution-control-mirror', env), '/tmp/execution-control.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-paper-ledger-mirror', env), '/tmp/paper-ledger.log');
  assert.equal(resolveDashboardLogPath('/api/logs/v27-recovery-control-mirror', env), '/tmp/recovery-control.log');
  assert.equal(resolveDashboardLogPath('/api/logs/not-registered', env), null);
});

test('boundedIntParam clamps oversized live query parameters', () => {
  const url = new URL('https://example.test/api?event_limit=40000&limit=999');

  assert.equal(boundedIntParam(url, 'event_limit', 3000, 100, 8000), 8000);
  assert.equal(boundedIntParam(url, 'limit', 50, 1, 120), 120);
});

test('boundedWindowedSinceTs clamps hours for live heavy endpoints', () => {
  const url = new URL('https://example.test/api?hours=24');
  const since = boundedWindowedSinceTs(url, 1, 2, { nowSec: 10_000 });

  assert.equal(since, 10_000 - 2 * 3600);
});

test('boundedWindowedSinceTs supports explicit 24h review windows', () => {
  const url = new URL('https://example.test/api?hours=24');
  const since = boundedWindowedSinceTs(url, 2, 24, { nowSec: 100_000 });

  assert.equal(since, 100_000 - 24 * 3600);
});

test('paper report gate rejects concurrent and cooldown requests', () => {
  resetPaperReportGateForTest();
  const first = tryBeginPaperReport('/api/paper/lifecycle-summary', 1000);
  const concurrent = tryBeginPaperReport('/api/paper/trade-replay', 1001);

  assert.equal(first.allowed, true);
  assert.equal(concurrent.allowed, false);
  assert.equal(concurrent.reason, 'paper_report_busy');

  first.release(2000);
  const cooldown = tryBeginPaperReport('/api/paper/trade-replay', 2001);

  assert.equal(cooldown.allowed, false);
  assert.equal(cooldown.reason, 'paper_report_cooldown');
});

test('missed recovery summary uses materialized snapshots for 2h default window', () => {
  assert.equal(shouldUseMaterializedMissedRecoverySummary(2, false), true);
  assert.equal(shouldUseMaterializedMissedRecoverySummary(8, false), true);
  assert.equal(shouldUseMaterializedMissedRecoverySummary(2, true), false);
  assert.equal(shouldUseMaterializedMissedRecoverySummary(1, false), false);
});

test('materialized missed recovery summary excludes stop-before-peak rows from clean dogs', () => {
  const summary = missedRecoverySummaryFromLiveSnapshot({
    snapshot_id: 'paper_live_2h_test',
    generated_at: '2026-05-21T00:00:00Z',
    window: { since_ts: 100, since_iso: '2026-05-21T00:00:00Z' },
    missed: {
      overall: {
        unique_tokens: 2,
        gold_unique: 1,
        quote_executable_unique: 2,
      },
      by_gate: [],
      top_dogs: [
        {
          token_ca: 'StopFirst',
          symbol: 'STOP',
          quote_exec: 1,
          tradable_missed: 1,
          would_stop_before_peak: 1,
          max_pnl: 10,
        },
        {
          token_ca: 'CleanDog',
          symbol: 'CLEAN',
          quote_exec: 1,
          tradable_missed: 1,
          would_stop_before_peak: 0,
          max_pnl: 2,
        },
      ],
    },
  }, { dbPath: '/tmp/paper.db', requestedHours: 2, limit: 10 });

  assert.deepEqual(
    summary.top_clean_quote_dogs.map((row) => row.token_ca),
    ['CleanDog']
  );
});

test('dog catch goal progress uses peak wins and clean missed dogs', () => {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE paper_trades (
      token_ca TEXT,
      entry_ts REAL,
      exit_ts REAL,
      pnl_pct REAL,
      trusted_peak_pnl REAL,
      position_size_sol REAL
    );
    CREATE TABLE paper_missed_signal_attribution (
      token_ca TEXT,
      signal_ts REAL,
      created_event_ts REAL,
      baseline_ts REAL,
      tradable_missed INTEGER,
      would_stop_before_peak INTEGER,
      executable_peak_pnl REAL
    );
  `);
  db.prepare(`
    INSERT INTO paper_trades (token_ca, entry_ts, exit_ts, pnl_pct, trusted_peak_pnl, position_size_sol)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run('caught-dog', 1001, 1010, 1.0, 0.7, 0.002);
  db.prepare(`
    INSERT INTO paper_trades (token_ca, entry_ts, exit_ts, pnl_pct, trusted_peak_pnl, position_size_sol)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run('small-loser', 1002, 1011, -0.1, 0.1, 0.002);
  db.prepare(`
    INSERT INTO paper_missed_signal_attribution (
      token_ca, signal_ts, created_event_ts, baseline_ts, tradable_missed,
      would_stop_before_peak, executable_peak_pnl
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('missed-dog', 1003, 1003, 1003, 1, 0, 0.6);
  db.prepare(`
    INSERT INTO paper_missed_signal_attribution (
      token_ca, signal_ts, created_event_ts, baseline_ts, tradable_missed,
      would_stop_before_peak, executable_peak_pnl
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run('stop-first', 1004, 1004, 1004, 1, 1, 2.0);

  const progress = buildDogCatchGoalProgress(
    db,
    new Set(['paper_trades', 'paper_missed_signal_attribution']),
    1000,
    { targetCatchRate: 0.60, targetWinRate: 0.55, targetRoi: 0.40 }
  );

  assert.equal(progress.trades.fills, 2);
  assert.equal(progress.trades.peak_wins, 1);
  assert.equal(progress.trades.captured_gold_silver_unique, 1);
  assert.equal(progress.missed.clean_gold_silver_unique, 1);
  assert.equal(progress.goal.eligible_gold_silver_unique, 2);
  assert.equal(progress.goal.clean_gold_silver_capture_rate, 0.5);
  assert.deepEqual(progress.goal.blockers, [
    'clean_gold_silver_capture_rate_below_target',
    'peak_win_rate_below_target',
  ]);
  db.close();
});

test('dog catch goal can be served from materialized live snapshot section', () => {
  const snapshot = {
    snapshot_id: 'paper_live_2h_test',
    generated_at: '2026-05-21T00:00:00Z',
    dog_catch_goal: {
      available: true,
      since_ts: 1000,
      trades: { fills: 1, peak_wins: 1, captured_gold_silver_unique: 1 },
      missed: {
        clean_gold_silver_unique: 2,
        clean_gold_unique: 1,
        clean_silver_unique: 1,
        by_blocker: [{ route: 'LOTTO', reject_reason: 'tracking_ttl_expired', gold_n: 1 }],
      },
      goal: {
        eligible_gold_silver_unique: 3,
        captured_gold_silver_unique: 1,
        clean_gold_silver_capture_rate: 1 / 3,
        pass: false,
        blockers: ['clean_gold_silver_capture_rate_below_target'],
      },
    },
  };

  const progress = dogCatchGoalFromLiveSnapshot(snapshot, {
    dbPath: '/tmp/paper.db',
    requestedHours: 2,
  });

  assert.equal(progress.materialized, true);
  assert.equal(progress.materialized_snapshot_id, 'paper_live_2h_test');
  assert.equal(progress.goal.eligible_gold_silver_unique, 3);
  assert.equal(progress.missed.by_blocker[0].reject_reason, 'tracking_ttl_expired');
});

test('closed loop missed dog summary ranks one blocker per token in SQL', () => {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE paper_missed_signal_attribution (
      token_ca TEXT,
      symbol TEXT,
      signal_id INTEGER,
      signal_ts REAL,
      route TEXT,
      component TEXT,
      reject_reason TEXT,
      tradability_status TEXT,
      tradability_reason TEXT,
      tradable_peak_pnl REAL,
      tradable_missed INTEGER,
      would_stop_before_peak INTEGER,
      max_pnl_recorded REAL,
      pnl_24h REAL,
      pnl_60m REAL,
      pnl_15m REAL,
      pnl_5m REAL,
      created_event_ts REAL,
      baseline_ts REAL
    );
  `);
  const insert = db.prepare(`
    INSERT INTO paper_missed_signal_attribution (
      token_ca, symbol, signal_id, signal_ts, route, component, reject_reason,
      tradability_status, tradability_reason, tradable_peak_pnl, tradable_missed,
      would_stop_before_peak, max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m,
      pnl_5m, created_event_ts, baseline_ts
    ) VALUES (
      @token_ca, @symbol, @signal_id, @signal_ts, @route, @component, @reject_reason,
      @tradability_status, @tradability_reason, @tradable_peak_pnl, @tradable_missed,
      @would_stop_before_peak, @max_pnl_recorded, @pnl_24h, @pnl_60m, @pnl_15m,
      @pnl_5m, @created_event_ts, @baseline_ts
    )
  `);
  insert.run({
    token_ca: 'token-a',
    symbol: 'A',
    signal_id: 1,
    signal_ts: 1001,
    route: 'ATH',
    component: 'matrix_evaluator',
    reject_reason: 'weak_matrix',
    tradability_status: 'tradable_reclaim',
    tradability_reason: 'older',
    tradable_peak_pnl: 0.3,
    tradable_missed: 1,
    would_stop_before_peak: 0,
    max_pnl_recorded: 0.3,
    pnl_24h: null,
    pnl_60m: null,
    pnl_15m: null,
    pnl_5m: null,
    created_event_ts: 1001,
    baseline_ts: 1001,
  });
  insert.run({
    token_ca: 'token-a',
    symbol: 'A',
    signal_id: 2,
    signal_ts: 1002,
    route: 'ATH',
    component: 'source_resonance_probe',
    reject_reason: 'scout_quality_buy_pressure_weak',
    tradability_status: 'tradable_reclaim',
    tradability_reason: 'best',
    tradable_peak_pnl: 1.2,
    tradable_missed: 1,
    would_stop_before_peak: 0,
    max_pnl_recorded: 1.2,
    pnl_24h: null,
    pnl_60m: null,
    pnl_15m: null,
    pnl_5m: null,
    created_event_ts: 1002,
    baseline_ts: 1002,
  });
  insert.run({
    token_ca: 'token-b',
    symbol: 'B',
    signal_id: 3,
    signal_ts: 1003,
    route: 'NOT_ATH',
    component: 'matrix_evaluator',
    reject_reason: 'matrices not yet aligned',
    tradability_status: 'stop_before_peak',
    tradability_reason: 'stopped',
    tradable_peak_pnl: 0.7,
    tradable_missed: 1,
    would_stop_before_peak: 1,
    max_pnl_recorded: 0.7,
    pnl_24h: null,
    pnl_60m: null,
    pnl_15m: null,
    pnl_5m: null,
    created_event_ts: 1003,
    baseline_ts: 1003,
  });
  insert.run({
    token_ca: 'token-c',
    symbol: 'C',
    signal_id: 4,
    signal_ts: 1004,
    route: 'LOTTO',
    component: 'discovery_tracking',
    reject_reason: 'tracking_ttl_expired',
    tradability_status: 'tradable_reclaim',
    tradability_reason: 'small',
    tradable_peak_pnl: 0.2,
    tradable_missed: 1,
    would_stop_before_peak: 0,
    max_pnl_recorded: 0.2,
    pnl_24h: null,
    pnl_60m: null,
    pnl_15m: null,
    pnl_5m: null,
    created_event_ts: 1004,
    baseline_ts: 1004,
  });
  insert.run({
    token_ca: 'token-mark-only',
    symbol: 'MARK',
    signal_id: 6,
    signal_ts: 1005,
    route: 'ATH',
    component: 'matrix_evaluator',
    reject_reason: 'mark_spike',
    tradability_status: 'tradable_reclaim',
    tradability_reason: 'mark_only',
    tradable_peak_pnl: null,
    tradable_missed: 1,
    would_stop_before_peak: 0,
    max_pnl_recorded: 1.3,
    pnl_24h: null,
    pnl_60m: null,
    pnl_15m: null,
    pnl_5m: null,
    created_event_ts: 1005,
    baseline_ts: 1005,
  });
  insert.run({
    token_ca: 'old-token',
    symbol: 'OLD',
    signal_id: 5,
    signal_ts: 900,
    route: 'ATH',
    component: 'matrix_evaluator',
    reject_reason: 'old',
    tradability_status: 'tradable_reclaim',
    tradability_reason: 'old',
    tradable_peak_pnl: 10,
    tradable_missed: 1,
    would_stop_before_peak: 0,
    max_pnl_recorded: 10,
    pnl_24h: null,
    pnl_60m: null,
    pnl_15m: null,
    pnl_5m: null,
    created_event_ts: 900,
    baseline_ts: 900,
  });

  const summary = buildClosedLoopMissedDogSummary(
    db,
    new Set(['paper_missed_signal_attribution']),
    1000,
    5,
    { includeDetails: true }
  );

  assert.equal(summary.available, true);
  assert.equal(summary.unique_tokens, 4);
  assert.equal(summary.quote_clean_unique, 3);
  assert.equal(summary.quote_clean_dog_unique, 1);
  assert.equal(summary.gold_unique, 1);
  assert.equal(summary.silver_unique, 1);
  assert.equal(summary.bronze_unique, 0);
  assert.equal(summary.mark_only_gold_unique, 1);
  assert.equal(summary.top_missed_dogs.length, 3);
  assert.equal(summary.top_missed_dogs[0].token_ca, 'token-a');
  assert.equal(summary.top_missed_dogs[0].final_blocker_key, 'ATH:source_resonance_probe:scout_quality_buy_pressure_weak');
  assert.equal(summary.top_missed_dogs[0].entry_mode_candidate, 'source_resonance_tiny_probe');
  assert.equal(summary.top_missed_dogs[1].token_ca, 'token-b');
  assert.equal(summary.top_missed_dogs[1].quote_clean, false);
  assert.equal(summary.top_missed_dogs[2].token_ca, 'token-mark-only');
  assert.equal(summary.top_missed_dogs[2].peak_trust_status, 'mark_only_peak_untrusted');
  assert.equal(summary.by_final_blocker[0].final_blocker_key, 'ATH:source_resonance_probe:scout_quality_buy_pressure_weak');
  assert.equal(summary.by_final_blocker[0].gold_unique, 1);

  const summaryOnly = buildClosedLoopMissedDogSummary(
    db,
    new Set(['paper_missed_signal_attribution']),
    1000,
    5,
    { includeDetails: false }
  );
  assert.equal(summaryOnly.unique_tokens, 4);
  assert.equal(summaryOnly.quote_clean_unique, 3);
  assert.equal(summaryOnly.quote_clean_dog_unique, 1);
  assert.equal(summaryOnly.gold_unique, 1);
  assert.equal(summaryOnly.silver_unique, 1);
  assert.equal(summaryOnly.mark_only_gold_unique, 1);
  assert.deepEqual(summaryOnly.top_missed_dogs, []);
  assert.deepEqual(summaryOnly.by_final_blocker, []);
  db.close();
});

test('closed loop missed dog summary excludes tokens already caught by paper trades', () => {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE paper_missed_signal_attribution (
      token_ca TEXT,
      symbol TEXT,
      signal_ts REAL,
      route TEXT,
      component TEXT,
      reject_reason TEXT,
      tradable_peak_pnl REAL,
      tradable_missed INTEGER,
      would_stop_before_peak INTEGER,
      max_pnl_recorded REAL,
      created_event_ts REAL,
      baseline_ts REAL
    );
    CREATE TABLE paper_trades (
      token_ca TEXT,
      entry_ts REAL
    );
  `);
  const insertMissed = db.prepare(`
    INSERT INTO paper_missed_signal_attribution (
      token_ca, symbol, signal_ts, route, component, reject_reason,
      tradable_peak_pnl, tradable_missed, would_stop_before_peak,
      max_pnl_recorded, created_event_ts, baseline_ts
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  insertMissed.run('caught-token', 'CAUGHT', 1001, 'ATH', 'source_resonance_probe', 'scout_quality_buy_pressure_weak', 1.2, 1, 0, 1.2, 1001, 1001);
  insertMissed.run('missed-token', 'MISSED', 1002, 'LOTTO', 'discovery_tracking', 'tracking_ttl_expired', 0.7, 1, 0, 0.7, 1002, 1002);
  db.prepare('INSERT INTO paper_trades (token_ca, entry_ts) VALUES (?, ?)').run('caught-token', 1005);

  const summary = buildClosedLoopMissedDogSummary(
    db,
    new Set(['paper_missed_signal_attribution', 'paper_trades']),
    1000,
    5,
    { includeDetails: true }
  );

  assert.equal(summary.unique_tokens, 1);
  assert.equal(summary.gold_unique, 0);
  assert.equal(summary.silver_unique, 1);
  assert.equal(summary.top_missed_dogs.length, 1);
  assert.equal(summary.top_missed_dogs[0].token_ca, 'missed-token');

  const summaryOnly = buildClosedLoopMissedDogSummary(
    db,
    new Set(['paper_missed_signal_attribution', 'paper_trades']),
    1000,
    5,
    { includeDetails: false }
  );
  assert.equal(summaryOnly.unique_tokens, 1);
  assert.equal(summaryOnly.silver_unique, 1);
  db.close();
});

test('closed loop probe summary uses recent trade window with exit fallback', () => {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE paper_decision_events (
      event_ts REAL,
      token_ca TEXT,
      component TEXT,
      event_type TEXT,
      decision TEXT,
      reason TEXT
    );
    CREATE TABLE paper_trades (
      entry_ts REAL,
      exit_ts REAL,
      entry_mode TEXT,
      token_ca TEXT,
      pnl_pct REAL,
      peak_pnl REAL
    );
  `);
  db.prepare(`
    INSERT INTO paper_decision_events (
      event_ts, token_ca, component, event_type, decision, reason
    ) VALUES (?, ?, ?, ?, ?, ?)
  `).run(1001, 'token-a', 'hard_gate_pass_probe', 'pending_entry', 'accept', 'armed');
  db.prepare(`
    INSERT INTO paper_decision_events (
      event_ts, token_ca, component, event_type, decision, reason
    ) VALUES (?, ?, ?, ?, ?, ?)
  `).run(1003, 'token-pre', 'pre_pass_resonance_probe', 'pending_entry', 'accept', 'armed');
  const insertTrade = db.prepare(`
    INSERT INTO paper_trades (
      entry_ts, exit_ts, entry_mode, token_ca, pnl_pct, peak_pnl
    ) VALUES (?, ?, ?, ?, ?, ?)
  `);
  insertTrade.run(1001, 1010, 'hard_gate_pass_tiny_probe', 'token-a', 0.2, 0.5);
  insertTrade.run(null, 1002, 'hard_gate_pass_tiny_probe', 'token-b', -0.1, 0.1);
  insertTrade.run(1003, 1009, 'pre_pass_resonance_tiny_probe', 'token-pre', 0.4, 0.6);
  insertTrade.run(900, 950, 'hard_gate_pass_tiny_probe', 'old-token', 4.0, 4.0);

  const summary = buildClosedLoopProbeSummary(
    db,
    new Set(['paper_decision_events', 'paper_trades']),
    1000
  );

  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.armed_events, 1);
  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.armed_unique, 1);
  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.fills, 2);
  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.fill_unique, 2);
  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.wins, 1);
  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.avg_pnl_pct, 5);
  assert.equal(summary.by_mode.hard_gate_pass_tiny_probe.max_peak_pnl_pct, 50);
  assert.equal(summary.by_mode.pre_pass_resonance_tiny_probe.armed_unique, 1);
  assert.equal(summary.by_mode.pre_pass_resonance_tiny_probe.fills, 1);
  assert.equal(summary.by_mode.pre_pass_resonance_tiny_probe.avg_pnl_pct, 40);
  db.close();
});
