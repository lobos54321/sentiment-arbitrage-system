import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import Database from 'better-sqlite3';
import {
  EVIDENCE_SCHEMA,
  isDecimalIdentifier,
  isEvidenceTimestamp,
  numericEvidenceRule,
  validateNumericEvidenceSchema,
  validateNumericEvidenceValue,
} from '../src/web/evaluator-evidence-schema.js';
import {
  apiJsonHeaders,
  aClassStatusFromLiveSnapshot,
  apiEnvelopePayloadForHash,
  auditSha256Hex,
  buildApiResponseErrorShape,
  buildV27ManualEvidenceApiResponse,
  buildDogCatchGoalProgress,
  buildIncidentArtifactSnapshot,
  buildNotAthReclaimFunnelReport,
  buildRolling24hGoalStatusFromLiveSnapshot,
  buildCounterfactualAiAuditFromP0,
  buildGoalControllerActions,
  buildAgentLatestStatus,
  buildMissedDogAiReviewFromP0,
  buildAClassBlockCauseBreakdown,
  summarizeAClassMatrixEvents,
  classifyAClassBlockCause,
  classifyAClassBlocker,
  buildV27KpiProofStatus,
  buildStorageHealthSnapshot,
  buildLottoQuoteGapAuditSummary,
  buildLottoQuoteGapWinnerJoinReport,
  latestActionableFastLaneQueueByToken,
  buildRawDogDiscoveryApiPayloadFromRollingSummary,
  dashboardEntrypointInfo,
  readRawDogDiscoveryApiSnapshot,
  resolveIncidentArtifactPath,
  writeRawDogDiscoveryApiSnapshot,
  buildClosedLoopProbeSummary,
  buildClosedLoopMissedDogSummary,
  appendDashboardAuditEvent,
  buildDashboardAuditEvent,
  boundedIntParam,
  boundedWindowedSinceTs,
  dogCatchGoalFromLiveSnapshot,
  livePaperQueryGuard,
  missedRecoverySummaryFromLiveSnapshot,
  readPaperDbRuntimeHealth,
  readPaperFastLaneHealth,
  readPaperReviewSnapshotHealth,
  readRuntimeFinalEvidenceHealth,
  readEvaluatorSnapshotWorkerHealth,
  parallelPaperStagePageClaimValid,
  PARALLEL_PAPER_STAGE_BULK_PAGE_MIN_BUDGET_BYTES,
  PARALLEL_PAPER_STAGE_CHUNK_TARGET_BYTES,
  PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION,
  PARALLEL_PAPER_STAGE_COMPRESSION,
  PARALLEL_PAPER_STAGE_STORAGE_CONTRACT_SHA256,
  JSON_NUMERIC_EVIDENCE_CONTRACT_SHA256,
  jsonNumericEvidenceTypesValid,
  sharedStageBudgetEvidenceSha256,
  sharedStageBudgetPlanSha256,
  readV27DenominatorReadModelHealth,
  readV27ModeReadiness,
  readV27ReadModelWorkerHealth,
  LOG_REDACTION_PATTERN_SET,
  redactLogMessage,
  V27_API_RESPONSE_ENVELOPE_VERSION,
  resolveDashboardLogPath,
  resetPaperReportGateForTest,
  shouldUseMaterializedMissedRecoverySummary,
  tryBeginPaperReport,
  verifyDashboardAuditChain,
} from '../src/web/dashboard-server.js';

test('parallel stage bulk-page claims use the production-calibrated 384 MiB floor', () => {
  const threshold = PARALLEL_PAPER_STAGE_BULK_PAGE_MIN_BUDGET_BYTES;

  assert.equal(threshold, 384 * 1024 ** 2);
  assert.equal(parallelPaperStagePageClaimValid(65536, 65536, threshold), true);
  assert.equal(parallelPaperStagePageClaimValid(65536, 65536, threshold - 4096), false);
  assert.equal(parallelPaperStagePageClaimValid(4096, 4096, threshold - 4096), true);
  assert.equal(parallelPaperStagePageClaimValid(8192, 8192, threshold), false);
  for (const grant of [523_489_280, 530_358_272, 518_160_384]) {
    assert.equal(parallelPaperStagePageClaimValid(65536, 65536, grant), true);
  }
});

test('parallel compressed-stage storage contract matches the Python golden hash', () => {
  assert.equal(
    PARALLEL_PAPER_STAGE_STORAGE_CONTRACT_SHA256,
    'abddfbfe3e94bea539b850bd05fe5d76b9f5517671f406ac13259e51952ac1bf',
  );
});

test('numeric evidence type contract matches the Python golden hash', () => {
  assert.equal(
    JSON_NUMERIC_EVIDENCE_CONTRACT_SHA256,
    'e111584ff5368a54ba03ad938ce7f136409a0dc5438c89694e54a13e0bf234f3',
  );
});

test('shared stage hashes match the Python cross-runtime golden vector', () => {
  const payload = {
    schema_version: 'shared_stage_budget.v2',
    allocation_mode: 'history_high_water_plus_advisory_source_demand',
    hash_canonicalization: 'json_sorted_float64_bits.v1',
    generated_at: 'x',
    capacity_sufficient: true,
    grants_sum_matches_total_cap: true,
    total_cap_bytes: 4096,
    total_granted_bytes: 4096,
    actual_total_bytes: 1024,
    unconsumed_bytes: 3072,
    all_targets_within_grant: true,
    targets: {
      t: {
        minimum_cap_bytes: 12288,
        average: 0.1,
        integral_float: 1.0,
        utilization_ratio: 0.25,
        actual_usage_bytes: 1024,
        sqlite_full_observed: true,
      },
    },
  };
  assert.equal(
    sharedStageBudgetPlanSha256(payload),
    '60c460889746f6e5b03d7c555796c6e98961be99f4ad4717b6e1d92c02d575fb',
  );
  payload.plan_sha256 = sharedStageBudgetPlanSha256(payload);
  assert.equal(
    sharedStageBudgetEvidenceSha256(payload),
    'ac61bf1db4807887f4640760b0e57a5ca0e0c8a2ca90e29b068742de55fa1b49',
  );
  for (const value of [2 ** 53, -(2 ** 53)]) {
    assert.throws(
      () => sharedStageBudgetPlanSha256({
        capacity_sufficient: true,
        grants_sum_matches_total_cap: true,
        total_cap_bytes: value,
        total_granted_bytes: value,
        targets: {},
      }),
      /safe integer/,
    );
  }
  for (const value of ['4096', false, null, 0.5, {}, []]) {
    assert.throws(
      () => sharedStageBudgetPlanSha256({
        capacity_sufficient: true,
        grants_sum_matches_total_cap: true,
        total_cap_bytes: value,
        total_granted_bytes: value,
        targets: {},
      }),
      /safe integers/,
    );
  }
  for (const value of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
    assert.throws(
      () => sharedStageBudgetPlanSha256({
        capacity_sufficient: true,
        grants_sum_matches_total_cap: true,
        total_cap_bytes: 4096,
        total_granted_bytes: 4096,
        diagnostic_ratio: value,
        targets: {},
      }),
      /non-finite/,
    );
  }
});

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

test('dashboardEntrypointInfo exposes process entrypoint without dumping env', () => {
  const info = dashboardEntrypointInfo();
  assert.equal(info.schema_version, 'dashboard_entrypoint.v1');
  assert.equal(info.entrypoint_file, process.argv[1] || null);
  assert.equal(info.argv1, process.argv[1] || null);
  assert.ok(Object.hasOwn(info, 'npm_lifecycle_event'));
  assert.ok(!Object.hasOwn(info, 'env'));
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
  fs.writeFileSync(join(dir, 'paper-db-retention.log'), '[retention] archived');

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
  assert.match(snapshot.retention_tail, /archived/);
});

test('paper db runtime health surfaces integrity marker without opening sqlite', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'paper-db-runtime-health-'));
  const paper = join(dir, 'paper_trades.db');
  fs.writeFileSync(paper, 'sqlite-placeholder');
  fs.writeFileSync(`${paper}.integrity_error`, 'context=watchlist_evaluation\nerror=database disk image is malformed\n');

  const health = readPaperDbRuntimeHealth({ paperDbPath: paper });

  assert.equal(health.available, true);
  assert.equal(health.status, 'paper_db_integrity_marker_present');
  assert.equal(health.integrity_marker.exists, true);
  assert.match(health.integrity_marker.text_preview, /watchlist_evaluation/);
  assert.match(health.integrity_marker.text_preview, /database disk image is malformed/);
});

test('paper db runtime health treats zero-byte live db as unavailable', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'paper-db-empty-health-'));
  const paper = join(dir, 'paper_trades.db');
  fs.writeFileSync(paper, '');

  const health = readPaperDbRuntimeHealth({ paperDbPath: paper });

  assert.equal(health.available, true);
  assert.equal(health.status, 'paper_db_empty');
  assert.equal(health.size_bytes, 0);
  assert.equal(health.reason, 'paper_trades_db_zero_bytes');
});

test('paper db runtime health detects invalid sqlite header without opening db', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'paper-db-invalid-health-'));
  const paper = join(dir, 'paper_trades.db');
  fs.writeFileSync(paper, 'not a sqlite database');

  const health = readPaperDbRuntimeHealth({ paperDbPath: paper });

  assert.equal(health.available, true);
  assert.equal(health.status, 'paper_db_invalid_sqlite_header');
  assert.equal(health.reason, 'paper_trades_db_header_not_sqlite');
});

test('runtime final evidence health reports missing and existing evidence log', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'runtime-final-evidence-health-'));
  const evidencePath = join(dir, 'runtime_final_evidence.jsonl');

  const missing = readRuntimeFinalEvidenceHealth({
    evidencePath,
    env: { RUNTIME_FINAL_EVIDENCE_LOG: evidencePath },
  });

  assert.equal(missing.available, false);
  assert.equal(missing.configured, true);
  assert.equal(missing.status, 'runtime_final_evidence_missing');
  assert.equal(missing.parent_exists, true);

  fs.writeFileSync(evidencePath, '{"module_group":"gmgn_policy"}\n');
  const existing = readRuntimeFinalEvidenceHealth({
    evidencePath,
    env: { RUNTIME_FINAL_EVIDENCE_LOG: evidencePath },
  });

  assert.equal(existing.available, true);
  assert.equal(existing.status, 'ok');
  assert.equal(existing.size_bytes > 0, true);
  assert.match(existing.mtime, /^\d{4}-\d{2}-\d{2}T/);
});

test('buildAgentLatestStatus reports compact motion trace coverage', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'agent-latest-status-'));
  const signalDbPath = join(dir, 'sentiment_arb.db');
  const db = new Database(signalDbPath);
  db.exec(`
    CREATE TABLE premium_signals (
      id INTEGER PRIMARY KEY,
      token_ca TEXT,
      timestamp INTEGER,
      indices_json TEXT,
      ath_stage TEXT,
      token_supply REAL,
      token_decimals INTEGER
    );
    CREATE TABLE token_motion_events (
      mint TEXT NOT NULL,
      signal_id INTEGER NOT NULL DEFAULT 0,
      lifecycle_id TEXT NOT NULL DEFAULT '',
      ts_ms INTEGER NOT NULL,
      domain TEXT NOT NULL,
      event_type TEXT NOT NULL,
      payload_json TEXT,
      created_at_ms INTEGER NOT NULL,
      PRIMARY KEY (mint, signal_id, ts_ms, domain, event_type)
    );
  `);
  db.prepare(`
    INSERT INTO premium_signals (id, token_ca, timestamp, indices_json, ath_stage, token_supply, token_decimals)
    VALUES (1, 'StatusToken', ?, '{"super_index":{"current":90}}', 'ATH1', 1000, 6)
  `).run(Math.floor(Date.now() / 1000));
  db.prepare(`
    INSERT INTO token_motion_events (mint, signal_id, lifecycle_id, ts_ms, domain, event_type, payload_json, created_at_ms)
    VALUES ('StatusToken', 1, '', ?, 'perceive', 'signal_received', '{}', ?)
  `).run(Date.now(), Date.now());
  db.close();

  const status = buildAgentLatestStatus({
    signalDbPath,
    hours: 24,
    nowMs: Date.now(),
  });

  assert.equal(status.schema_version, 'agent_latest_status.v1');
  assert.equal(status.guardrails.promotion_allowed, false);
  assert.equal(status.motion_trace.signal_coverage.available, true);
  assert.equal(status.motion_trace.signal_coverage.signal_rows, 1);
  assert.equal(status.motion_trace.signal_coverage.indices_json_present_rate, 1);
  assert.equal(status.motion_trace.event_coverage.available, true);
  assert.equal(status.motion_trace.event_coverage.total_events, 1);
});

test('incident artifact snapshot lists allowed evidence and blocks path escape', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'incident-artifacts-'));
  const backupDir = join(dir, 'backup', 'paper-db-family');
  const recoveryDir = join(dir, 'recovery');
  const evidenceDir = join(dir, 'paper_evidence_log');
  fs.mkdirSync(join(backupDir, 'paper_trades_20260602T150848Z'), { recursive: true });
  fs.mkdirSync(join(recoveryDir, 'paper_trades_corrupt_20260602T150849Z'), { recursive: true });
  fs.mkdirSync(evidenceDir, { recursive: true });
  fs.writeFileSync(
    join(backupDir, 'paper_trades_20260602T150848Z', 'manifest.json'),
    JSON.stringify({ note: 'startup backup' }),
  );
  fs.writeFileSync(
    join(recoveryDir, 'paper_trades_corrupt_20260602T150849Z', 'paper_trades.db.integrity_error'),
    'context=watchlist_evaluation\nerror=database disk image is malformed\n',
  );
  fs.writeFileSync(join(evidenceDir, 'paper-events-20260602.jsonl'), '{"event_type":"entry"}\n');

  const snapshot = buildIncidentArtifactSnapshot({
    backupDir,
    recoveryDir,
    evidenceDir,
    maxFiles: 20,
    includePreviews: true,
  });

  assert.equal(snapshot.error_code, undefined);
  assert.equal(snapshot.artifact_roots.backup.exists, true);
  assert.equal(snapshot.items.some((item) => item.scope === 'backup' && item.relative_path.endsWith('manifest.json')), true);
  const marker = snapshot.items.find((item) => item.relative_path.endsWith('paper_trades.db.integrity_error'));
  assert.match(marker.text_preview, /context=watchlist_evaluation/);
  assert.match(marker.download_path, /incident-artifact\/download/);

  const resolved = resolveIncidentArtifactPath(
    'recovery',
    'paper_trades_corrupt_20260602T150849Z/paper_trades.db.integrity_error',
    { backupDir, recoveryDir, evidenceDir },
  );
  assert.equal(resolved.ok, true);
  assert.equal(resolved.relative_path, 'paper_trades_corrupt_20260602T150849Z/paper_trades.db.integrity_error');

  const escaped = resolveIncidentArtifactPath('recovery', '../paper_trades.db', {
    backupDir,
    recoveryDir,
    evidenceDir,
  });
  assert.equal(escaped.ok, false);
  assert.equal(escaped.error_code, 'incident_artifact_path_outside_root');
});

test('lotto quote gap audit summary reports size curve actionability', () => {
  const rows = [
    {
      id: 2,
      event_ts: 1_780_000_120,
      token_ca: 'TokenB',
      symbol: 'DOGB',
      signal_ts: 1_780_000_100,
      reason: 'lotto_timing_negative_m5',
      decision: 'measured',
      payload_json: JSON.stringify({
        gate_decision: 'wait',
        entry_mode_candidate: 'gmgn_clean_lotto_fast_lane',
        intent_size_sol: 0.05,
        mark_price: 0.0001,
        quote_curve: [
          { size_key: '0.01', size_sol: 0.01, quote_executable: true, quote_gap_pct: 4, spread_pct: 4, latency_ms: 12 },
          { size_key: '0.05', size_sol: 0.05, quote_executable: true, quote_gap_pct: 18, spread_pct: 18, latency_ms: 13 },
          { size_key: '0.1', size_sol: 0.1, quote_executable: false, quote_reason: 'no_route' },
        ],
      }),
    },
    {
      id: 1,
      event_ts: 1_780_000_060,
      token_ca: 'TokenA',
      symbol: 'DOGA',
      signal_ts: 1_780_000_000,
      reason: 'lotto_fast_lane_ok',
      decision: 'measured',
      payload_json: JSON.stringify({
        gate_decision: 'allow',
        entry_mode_candidate: 'newborn_momentum_tiny_scout',
        mark_price: null,
        quote_curve: [
          { size_key: '0.01', size_sol: 0.01, quote_executable: true, quote_gap_pct: null, spread_pct: null, latency_ms: 9 },
          { size_key: '0.05', size_sol: 0.05, quote_executable: true, quote_gap_pct: null, spread_pct: null, latency_ms: 10 },
        ],
      }),
    },
  ];

  const report = buildLottoQuoteGapAuditSummary(rows, { recentLimit: 2 });

  assert.equal(report.audit_schema_version, 'v2.7.0.lotto_quote_gap_audit_summary.v1');
  assert.equal(report.summary.events, 2);
  assert.equal(report.summary.unique_tokens, 2);
  assert.equal(report.summary.executable_events, 2);
  assert.equal(report.summary.clean10_events, 1);
  assert.equal(report.summary.clean30_events, 1);
  assert.equal(report.summary.no_mark_price_events, 1);
  assert.equal(report.summary.best_gap_n, 1);
  assert.equal(report.summary.median_best_abs_quote_gap_pct, 4);
  assert.deepEqual(report.by_size.map((row) => row.size_key), ['0.01', '0.05', '0.1']);
  assert.equal(report.by_size[0].executable_rate_pct, 100);
  assert.equal(report.by_size[0].gap_n, 1);
  assert.equal(report.by_size[1].median_abs_quote_gap_pct, 18);
  assert.equal(report.by_size[2].executable_rate_pct, 0);
  assert.equal(report.by_reason[0].events, 1);
  assert.equal(report.recent_events[0].best_abs_quote_gap_pct, 4);
  assert.equal(report.recent_events[1].no_mark_price, true);
  assert.equal(report.recent_events[1].best_abs_quote_gap_pct, null);
  assert.equal(report.recent_events[1].quote_curve[0].quote_gap_pct, null);
});

test('lotto quote gap winner join report ties clean audit gaps to confirmed winners', () => {
  const auditRows = [
    {
      id: 30,
      event_ts: 1_780_000_180,
      token_ca: 'TokenUnjoined',
      symbol: 'DOGU',
      signal_ts: 1_780_000_140,
      reason: 'lotto_fast_lane_ok',
      payload_json: JSON.stringify({
        gate_decision: 'allow',
        entry_mode_candidate: 'newborn_momentum_tiny_scout',
        quote_curve: [
          { size_key: '0.01', size_sol: 0.01, quote_executable: true, quote_gap_pct: 6, spread_pct: 6, latency_ms: 8 },
        ],
      }),
    },
    {
      id: 20,
      event_ts: 1_780_000_120,
      token_ca: 'TokenBronze',
      symbol: 'DOGC',
      signal_ts: 1_780_000_090,
      reason: 'gmgn_clean_smart_money_boost',
      payload_json: JSON.stringify({
        gate_decision: 'allow',
        entry_mode_candidate: 'lotto_fast_lane',
        quote_curve: [
          { size_key: '0.01', size_sol: 0.01, quote_executable: true, quote_gap_pct: 15, spread_pct: 15, latency_ms: 9 },
          { size_key: '0.05', size_sol: 0.05, quote_executable: true, quote_gap_pct: 22, spread_pct: 22, latency_ms: 10 },
        ],
      }),
    },
    {
      id: 15,
      event_ts: 1_780_000_080,
      token_ca: 'TokenGold',
      symbol: 'DOGA',
      signal_ts: 1_779_999_970,
      reason: 'gmgn_clean_smart_money_boost',
      payload_json: JSON.stringify({
        gate_decision: 'allow',
        entry_mode_candidate: 'lotto_fast_lane',
        quote_curve: [
          { size_key: '0.01', size_sol: 0.01, quote_executable: true, quote_gap_pct: 6, spread_pct: 6, latency_ms: 8 },
          { size_key: '0.05', size_sol: 0.05, quote_executable: true, quote_gap_pct: 8, spread_pct: 8, latency_ms: 9 },
        ],
      }),
    },
    {
      id: 10,
      event_ts: 1_780_000_060,
      token_ca: 'TokenSilver',
      symbol: 'DOGB',
      signal_ts: 1_780_000_020,
      reason: 'lotto_liq_unknown_pumpfun_wait',
      payload_json: JSON.stringify({
        gate_decision: 'wait',
        entry_mode_candidate: 'gmgn_clean_lotto_fast_lane',
        quote_curve: [
          { size_key: '0.01', size_sol: 0.01, quote_executable: true, quote_gap_pct: 12, spread_pct: 12, latency_ms: 11 },
          { size_key: '0.05', size_sol: 0.05, quote_executable: true, quote_gap_pct: 18, spread_pct: 18, latency_ms: 12 },
        ],
      }),
    },
  ];

  const missedRows = [
    {
      id: 3,
      created_event_ts: 1_780_000_182,
      token_ca: 'TokenUnjoinedX',
      symbol: 'DOGU',
      signal_ts: 1_780_000_140,
      baseline_ts: 1_780_000_100,
      route: 'LOTTO',
      component: 'smart_entry',
      reject_reason: 'no_kline_low_volume',
      tradable_missed: 1,
      would_stop_before_peak: 0,
      tradable_peak_pnl: 0.95,
      quote_clean_peak_pnl: 0.9,
      executable_peak_pnl: 0.98,
      pnl_24h: 0.9,
    },
    {
      id: 2,
      created_event_ts: 1_780_000_122,
      token_ca: 'TokenBronze',
      symbol: 'DOGC',
      signal_ts: 1_780_000_092,
      baseline_ts: 1_780_000_060,
      route: 'LOTTO',
      component: 'smart_entry',
      reject_reason: 'no_kline_low_volume',
      tradable_missed: 1,
      would_stop_before_peak: 0,
      tradable_peak_pnl: 0.32,
      quote_clean_peak_pnl: 0.31,
      executable_peak_pnl: 0.33,
      pnl_24h: 0.31,
    },
    {
      id: 1,
      created_event_ts: 1_780_000_062,
      token_ca: 'TokenSilver',
      symbol: 'DOGB',
      signal_ts: 1_780_000_021,
      baseline_ts: 1_780_000_000,
      route: 'LOTTO',
      component: 'smart_entry',
      reject_reason: 'no_kline_low_volume',
      tradable_missed: 1,
      would_stop_before_peak: 0,
      tradable_peak_pnl: 0.7,
      quote_clean_peak_pnl: 0.69,
      executable_peak_pnl: 0.72,
      pnl_24h: 0.69,
    },
    {
      id: 0,
      created_event_ts: 1_780_000_022,
      token_ca: 'TokenGold',
      symbol: 'DOGA',
      signal_ts: 1_779_999_970,
      baseline_ts: 1_779_999_940,
      route: 'LOTTO',
      component: 'smart_entry',
      reject_reason: 'no_kline_low_volume',
      tradable_missed: 1,
      would_stop_before_peak: 0,
      tradable_peak_pnl: 1.2,
      quote_clean_peak_pnl: 1.1,
      executable_peak_pnl: 1.25,
      pnl_24h: 1.1,
    },
  ];

  const report = buildLottoQuoteGapWinnerJoinReport(auditRows, missedRows, {
    recentLimit: 2,
    topLimit: 3,
    maxJoinDeltaSec: 300,
    nowTs: 1_780_000_200,
    fastLaneRescueByMissedId: new Map([
      [0, {
        missed_attribution_id: 0,
        state: 'queued',
        last_status: 'queued',
        last_reason: 'tracking_ttl_reclaim_quote_clean_tiny_probe',
        entry_branch: 'tracking_ttl_reclaim_quote_clean_tiny_probe',
        entry_mode_hint: 'lotto_not_ath_reclaim_tiny_probe',
        blocker: 'tracking_ttl_expired',
        updated_at: 1_780_000_090,
      }],
      [1, {
        missed_attribution_id: 1,
        state: 'stale',
        last_status: 'watch_only',
        last_reason: 'ttl_rescue_tradable_signal_stale_watch_only',
        entry_branch: 'tracking_ttl_reclaim_quote_clean_tiny_probe',
        entry_mode_hint: 'lotto_not_ath_reclaim_tiny_probe',
        blocker: 'tracking_ttl_expired',
        updated_at: 1_780_000_070,
      }],
    ]),
    fastLaneQueueByToken: new Map([
      ['TokenGold', {
        token_ca: 'TokenGold',
        status: 'queued',
        source_type: 'ttl_final_reclaim_fast',
        entry_branch: 'tracking_ttl_reclaim_quote_clean_tiny_probe',
        entry_mode_hint: 'lotto_not_ath_reclaim_tiny_probe',
        updated_at: 1_780_000_091,
      }],
    ]),
  });

  assert.equal(report.audit_schema_version, 'v2.7.0.lotto_quote_gap_winner_join.v1');
  assert.equal(report.summary.audit_events, 4);
  assert.equal(report.summary.audit_unique_tokens, 4);
  assert.equal(report.summary.joined_events, 3);
  assert.equal(report.summary.joined_unique_tokens, 3);
  assert.equal(report.summary.join_coverage_pct, 75);
  assert.equal(report.summary.clean_tradable_joined_events, 3);
  assert.equal(report.summary.joined_medal_events, 3);
  assert.equal(report.summary.clean_medal_joined_events, 3);
  assert.equal(report.summary.gold_events, 1);
  assert.equal(report.summary.silver_events, 1);
  assert.equal(report.summary.bronze_events, 1);
  assert.equal(report.summary.joined_executable_events, 3);
  assert.equal(report.summary.joined_clean10_events, 1);
  assert.equal(report.summary.joined_clean30_events, 3);
  assert.equal(report.summary.median_best_abs_quote_gap_pct, 12);
  assert.equal(report.summary.p90_best_abs_quote_gap_pct, 15);
  assert.equal(report.by_tier[0].tier, 'gold');
  assert.equal(report.by_tier[0].unique_tokens, 1);
  assert.equal(report.by_tier[0].clean_medal_unique, 1);
  assert.equal(report.by_tier[0].median_best_abs_quote_gap_pct, 6);
  assert.equal(report.by_tier[1].tier, 'silver');
  assert.equal(report.by_tier[1].median_trusted_peak_pnl_pct, 72);
  assert.equal(report.by_tier[2].tier, 'bronze');
  assert.equal(report.by_tier[2].max_trusted_peak_pnl_pct, 33);
  assert.equal(report.by_blocker[0].reject_reason, 'no_kline_low_volume');
  assert.equal(report.by_blocker[0].events, 3);
  assert.equal(report.by_blocker[0].clean_medal_events, 3);
  assert.equal(report.by_blocker[0].clean_medal_unique, 3);
  assert.equal(report.by_blocker[0].silver_events, 1);
  assert.equal(report.by_blocker[0].bronze_events, 1);
  assert.equal(report.top_joined_winners[0].token_ca, 'TokenGold');
  assert.equal(report.top_joined_winners[0].trusted_peak_pnl_pct, 125);
  assert.equal(report.top_unique_joined_winners.length, 3);
  assert.equal(report.top_unique_joined_winners[0].token_ca, 'TokenGold');
  assert.equal(report.top_unique_joined_winners[0].fast_lane_rescue_seen, true);
  assert.equal(report.top_unique_joined_winners[0].fast_lane_rescue_state, 'queued');
  assert.equal(report.top_unique_joined_winners[0].fast_lane_entry_branch, 'tracking_ttl_reclaim_quote_clean_tiny_probe');
  assert.equal(report.top_unique_joined_winners[0].fast_lane_rescue_match_basis, 'missed_attribution_id');
  assert.equal(report.top_unique_joined_winners[0].fast_lane_rescue_scan_eligible, true);
  assert.equal(report.by_recovery_state[0].rescue_state, 'queued');
  assert.equal(report.by_recovery_state[0].clean_medal_unique, 1);
  assert.equal(
    report.by_recovery_state.some((row) => row.rescue_state === 'stale' && row.fast_lane_status === 'watch_only'),
    true
  );
  assert.equal(report.missed_rescue_scanner_coverage.summary.clean_medal_joined_events, 3);
  assert.equal(report.missed_rescue_scanner_coverage.summary.scanner_eligible_events, 3);
  assert.equal(report.missed_rescue_scanner_coverage.by_scan_gap[0].scan_gap_reason, 'scanner_eligible');
  assert.equal(report.unjoined_recent_audits[0].token_ca, 'TokenUnjoined');
  assert.equal(report.unjoined_recent_audits[0].best_abs_quote_gap_pct, 6);
});

test('lotto winner join can match token-only fast lane rescue state', () => {
  const report = buildLottoQuoteGapWinnerJoinReport([
    {
      id: 1,
      event_ts: 1_780_000_050,
      token_ca: 'TokenLegacyRescue',
      symbol: 'DOGLEG',
      signal_ts: 1_780_000_010,
      reason: 'lotto_quote_gap',
      payload_json: JSON.stringify({
        quote_curve: [
          { size_key: '0.01', quote_executable: true, quote_gap_pct: 4 },
        ],
      }),
    },
  ], [
    {
      id: 22,
      created_event_ts: 1_780_000_045,
      token_ca: 'TokenLegacyRescue',
      symbol: 'DOGLEG',
      signal_ts: 1_780_000_008,
      baseline_ts: 1_780_000_000,
      route: 'LOTTO',
      component: 'discovery_tracking',
      reject_reason: 'tracking_ttl_expired',
      tradable_missed: 1,
      would_stop_before_peak: 0,
      tradable_peak_pnl: 1.05,
      quote_clean_peak_pnl: 1.01,
      executable_peak_pnl: 1.1,
    },
  ], {
    topLimit: 1,
    maxJoinDeltaSec: 300,
    nowTs: 1_780_000_100,
    fastLaneRescueByToken: new Map([
      ['TokenLegacyRescue', {
        missed_attribution_id: null,
        token_ca: 'TokenLegacyRescue',
        state: 'queued',
        last_status: 'queued',
        last_reason: 'tracking_ttl_reclaim_quote_clean_tiny_probe',
        entry_branch: 'tracking_ttl_reclaim_quote_clean_tiny_probe',
        updated_at: 1_780_000_080,
      }],
    ]),
  });

  const top = report.top_unique_joined_winners[0];
  assert.equal(top.fast_lane_rescue_seen, true);
  assert.equal(top.fast_lane_rescue_match_basis, 'token_ca');
  assert.equal(top.fast_lane_rescue_state, 'queued');
  assert.equal(top.fast_lane_rescue_last_status, 'queued');
  assert.equal(top.fast_lane_rescue_scan_eligible, true);
  assert.equal(report.by_recovery_state[0].rescue_state, 'queued');
  assert.equal(report.missed_rescue_scanner_coverage.summary.rescue_seen_unique, 1);
});

test('fast lane queue attribution prefers actionable rows over newer watch observations', () => {
  const byToken = latestActionableFastLaneQueueByToken([
    {
      id: 12,
      token_ca: 'TokenQueueRank',
      status: 'watch_only',
      entry_branch: 'source_resonance_gmgn_fast',
      updated_at: 1_780_000_120,
    },
    {
      id: 10,
      token_ca: 'TokenQueueRank',
      status: 'queued',
      entry_branch: 'tracking_ttl_reclaim_quote_clean_tiny_probe',
      updated_at: 1_780_000_080,
    },
    {
      id: 22,
      token_ca: 'TokenEnteredRank',
      status: 'watch_only',
      entry_branch: 'source_resonance_gmgn_fast',
      updated_at: 1_780_000_130,
    },
    {
      id: 21,
      token_ca: 'TokenEnteredRank',
      status: 'entered',
      entry_branch: 'smart_entry_reclaim_quote_clean_tiny_probe',
      updated_at: 1_780_000_070,
    },
  ]);

  assert.equal(byToken.get('TokenQueueRank').status, 'queued');
  assert.equal(byToken.get('TokenQueueRank').entry_branch, 'tracking_ttl_reclaim_quote_clean_tiny_probe');
  assert.equal(byToken.get('TokenEnteredRank').status, 'entered');
  assert.equal(byToken.get('TokenEnteredRank').entry_branch, 'smart_entry_reclaim_quote_clean_tiny_probe');
});

test('lotto missed rescue scanner coverage allows smart momentum fading reclaim reasons', () => {
  const report = buildLottoQuoteGapWinnerJoinReport([
    {
      id: 1,
      event_ts: 1_780_000_080,
      token_ca: 'TokenMomentumDog',
      symbol: 'MDOG',
      signal_ts: 1_780_000_040,
      reason: 'lotto_quote_gap',
      payload_json: JSON.stringify({
        quote_curve: [
          { size_key: '0.01', quote_executable: true, quote_gap_pct: 7 },
        ],
      }),
    },
  ], [
    {
      id: 11,
      created_event_ts: 1_780_000_070,
      token_ca: 'TokenMomentumDog',
      symbol: 'MDOG',
      signal_ts: 1_780_000_040,
      baseline_ts: 1_780_000_020,
      route: 'LOTTO',
      component: 'smart_entry',
      reject_reason: 'momentum_fading',
      tradable_missed: 1,
      would_stop_before_peak: 0,
      tradable_peak_pnl: 0.42,
      quote_clean_peak_pnl: 0.4,
      executable_peak_pnl: 0.425,
      pnl_24h: 0.4,
    },
  ], {
    maxJoinDeltaSec: 300,
    nowTs: 1_780_000_100,
  });

  const top = report.top_unique_joined_winners[0];
  assert.equal(top.reject_reason, 'momentum_fading');
  assert.equal(top.fast_lane_rescue_scan_eligible, true);
  assert.equal(report.missed_rescue_scanner_coverage.summary.scanner_eligible_events, 1);
  assert.equal(report.missed_rescue_scanner_coverage.by_scan_gap[0].scan_gap_reason, 'scanner_eligible');
  assert.equal(report.missed_rescue_scanner_coverage.by_scan_gap[0].reject_reason_allowed, true);
});

test('lotto missed rescue scanner coverage allows exact missing market cap reclaim reason', () => {
  const report = buildLottoQuoteGapWinnerJoinReport([
    {
      id: 1,
      event_ts: 1_780_000_080,
      token_ca: 'TokenMissingMc',
      symbol: 'MMC',
      signal_ts: 1_780_000_040,
      reason: 'lotto_quote_gap',
      payload_json: JSON.stringify({
        quote_curve: [
          { size_key: '0.01', quote_executable: true, quote_gap_pct: 9 },
        ],
      }),
    },
  ], [
    {
      id: 12,
      created_event_ts: 1_780_000_070,
      token_ca: 'TokenMissingMc',
      symbol: 'MMC',
      signal_ts: 1_780_000_040,
      baseline_ts: 1_780_000_020,
      route: 'LOTTO',
      component: 'lotto_entry_gate',
      reject_reason: 'lotto_mc_0',
      tradable_missed: 1,
      would_stop_before_peak: 0,
      tradable_peak_pnl: 0.72,
      quote_clean_peak_pnl: 0.7,
      executable_peak_pnl: 0.728,
      pnl_24h: 0.7,
    },
  ], {
    maxJoinDeltaSec: 300,
    nowTs: 1_780_000_100,
  });

  const top = report.top_unique_joined_winners[0];
  assert.equal(top.reject_reason, 'lotto_mc_0');
  assert.equal(top.fast_lane_rescue_scan_eligible, true);
  assert.equal(report.missed_rescue_scanner_coverage.summary.scanner_eligible_events, 1);
  assert.equal(report.missed_rescue_scanner_coverage.by_scan_gap[0].reject_reason_allowed, true);
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
    worker_state: 'scanned',
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

  const health = readPaperFastLaneHealth({
    healthPath,
    nowMs: Date.parse('2026-05-28T23:05:00Z'),
    env: {
      SOURCE_SHADOW_WORKERS_ENABLED: 'true',
      PAPER_DB_WRITE_SIDECARS_ENABLED: 'true',
      PAPER_FAST_LANE_ENABLED: 'true',
    },
  });

  assert.equal(health.available, true);
  assert.equal(health.status, 'ok');
  assert.equal(health.required, true);
  assert.equal(health.paper_db_exists, true);
  assert.equal(health.worker_state, 'scanned');
  assert.equal(health.missed_rescue.scan_count, 3);
  assert.equal(health.missed_rescue.last_result.processed, 12);
  assert.equal(health.missed_rescue.last_error, null);
});

test('paper fast lane health fails loud when heartbeat is stale', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'paper-fast-lane-health-stale-'));
  const healthPath = join(dir, 'paper-fast-lane-health.json');
  fs.writeFileSync(healthPath, JSON.stringify({
    schema_version: 'v2.7.0.paper_fast_lane_health.v1',
    updated_at: '2026-05-28T23:00:00Z',
    paper_db_exists: true,
    worker_state: 'scanned',
    missed_rescue: {
      last_scan_at: '2026-05-28T23:00:00Z',
      scan_count: 3,
      error_count: 0,
      last_result: {},
      last_error: null,
    },
  }));

  const health = readPaperFastLaneHealth({
    healthPath,
    nowMs: Date.parse('2026-05-29T00:01:00Z'),
    maxAgeMinutes: 30,
    env: {
      SOURCE_SHADOW_WORKERS_ENABLED: 'true',
      PAPER_DB_WRITE_SIDECARS_ENABLED: 'true',
      PAPER_FAST_LANE_ENABLED: 'true',
    },
  });

  assert.equal(health.available, true);
  assert.equal(health.status, 'paper_fast_lane_health_stale_or_undated');
  assert.equal(health.required, true);
  assert.equal(health.fresh, false);
  assert.equal(health.age_minutes, 61);
  assert.equal(health.max_age_minutes, 30);
});

test('paper fast lane stale heartbeat can be non-required when source sidecars are disabled', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'paper-fast-lane-health-nonrequired-'));
  const healthPath = join(dir, 'paper-fast-lane-health.json');
  fs.writeFileSync(healthPath, JSON.stringify({
    schema_version: 'v2.7.0.paper_fast_lane_health.v1',
    updated_at: '2026-05-28T23:00:00Z',
    paper_db_exists: true,
    worker_state: 'scanned',
    missed_rescue: {
      last_scan_at: '2026-05-28T23:00:00Z',
      scan_count: 3,
      error_count: 0,
      last_result: {},
      last_error: null,
    },
  }));

  const health = readPaperFastLaneHealth({
    healthPath,
    nowMs: Date.parse('2026-05-29T00:01:00Z'),
    maxAgeMinutes: 30,
    env: {
      INDEX_RUNTIME_CHILD_SOURCE_SHADOW_WORKERS_ENABLED: 'false',
    },
  });

  assert.equal(health.available, true);
  assert.equal(health.status, 'paper_fast_lane_health_stale_or_undated');
  assert.equal(health.required, false);
  assert.equal(health.fresh, false);
});

test('paper review snapshot health fails loud when materialized snapshot is stale', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'paper-review-health-stale-'));
  const liveDir = join(dir, 'review-artifacts', 'live');
  fs.mkdirSync(liveDir, { recursive: true });
  fs.writeFileSync(join(liveDir, 'paper_review_24h.json'), JSON.stringify({
    snapshot_id: 'paper_live_24h_stale',
    generated_at: '2026-06-04T00:00:00.000Z',
  }));

  const previous = process.env.PAPER_REVIEW_LIVE_DIR;
  process.env.PAPER_REVIEW_LIVE_DIR = liveDir;
  try {
    const health = readPaperReviewSnapshotHealth({
      requestedHours: 24,
      nowMs: Date.parse('2026-06-04T01:00:00.000Z'),
      maxAgeMinutes: 30,
    });

    assert.equal(health.available, true);
    assert.equal(health.status, 'paper_review_snapshot_stale_or_undated');
    assert.equal(health.fresh, false);
    assert.equal(health.age_minutes, 60);
    assert.equal(health.snapshot_id, 'paper_live_24h_stale');
  } finally {
    if (previous === undefined) delete process.env.PAPER_REVIEW_LIVE_DIR;
    else process.env.PAPER_REVIEW_LIVE_DIR = previous;
  }
});

test('not ATH reclaim funnel summarizes Markov green through queue and trade outcomes', () => {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE paper_decision_events (
      id INTEGER PRIMARY KEY,
      event_ts REAL,
      signal_id INTEGER,
      token_ca TEXT,
      symbol TEXT,
      lifecycle_id TEXT,
      trade_id INTEGER,
      signal_ts INTEGER,
      strategy_stage TEXT,
      route TEXT,
      component TEXT,
      event_type TEXT,
      decision TEXT,
      reason TEXT,
      data_source TEXT,
      payload_json TEXT
    );
    CREATE TABLE paper_fast_entry_queue (
      id INTEGER PRIMARY KEY,
      created_at REAL,
      updated_at REAL,
      token_ca TEXT,
      symbol TEXT,
      source_type TEXT,
      entry_mode_hint TEXT,
      entry_branch TEXT,
      status TEXT,
      last_error TEXT,
      first_error TEXT,
      payload_json TEXT,
      market_session TEXT
    );
    CREATE TABLE paper_trades (
      id INTEGER PRIMARY KEY,
      symbol TEXT,
      token_ca TEXT,
      lifecycle_id TEXT,
      entry_ts REAL,
      exit_ts REAL,
      exit_reason TEXT,
      pnl_pct REAL,
      trusted_peak_pnl REAL,
      quote_peak_pnl REAL,
      mark_peak_pnl REAL,
      peak_trust_status TEXT,
      position_size_sol REAL,
      signal_route TEXT,
      entry_mode TEXT,
      entry_branch TEXT,
      replay_source TEXT,
      entry_execution_audit_json TEXT
    );
  `);
  const now = 1_780_000_000;
  const insertDecision = db.prepare(`
    INSERT INTO paper_decision_events (
      id, event_ts, token_ca, symbol, lifecycle_id, component, event_type,
      decision, reason, data_source, payload_json
    ) VALUES (
      @id, @event_ts, @token_ca, @symbol, @lifecycle_id, @component, @event_type,
      @decision, @reason, @data_source, @payload_json
    )
  `);
  insertDecision.run({
    id: 1,
    event_ts: now - 500,
    token_ca: 'TokenB',
    symbol: 'TB',
    lifecycle_id: 'life-b',
    component: 'markov_reclaim',
    event_type: 'entry_gate',
    decision: 'allow',
    reason: 'lotto_reclaim_cohort_markov_green',
    data_source: 'not_ath_reclaim_fast',
    payload_json: JSON.stringify({
      gate: {
        entry_mode: 'lotto_not_ath_reclaim_tiny_probe',
        markov_bucket: 'green',
        pass: true,
      },
    }),
  });
  insertDecision.run({
    id: 2,
    event_ts: now - 450,
    token_ca: 'TokenB',
    symbol: 'TB',
    lifecycle_id: 'life-b',
    component: 'revival_canary',
    event_type: 'entry_preview',
    decision: 'allow',
    reason: 'revival_canary_markov_green',
    data_source: 'not_ath_reclaim_fast',
    payload_json: JSON.stringify({
      entry_mode: 'lotto_not_ath_reclaim_tiny_probe',
      revival_canary: { markov_bucket: 'green' },
    }),
  });
  insertDecision.run({
    id: 3,
    event_ts: now - 420,
    token_ca: 'TokenA',
    symbol: 'TA',
    lifecycle_id: 'life-a',
    component: 'paper_fast_lane',
    event_type: 'branch_circuit',
    decision: 'watch_only',
    reason: 'branch_circuit_catastrophic_loss',
    data_source: 'not_ath_reclaim_fast',
    payload_json: JSON.stringify({
      entry_branch: 'not_ath_reclaim_quote_clean_tiny_probe',
    }),
  });
  insertDecision.run({
    id: 4,
    event_ts: now - 360,
    token_ca: 'TokenB',
    symbol: 'TB',
    lifecycle_id: 'life-b',
    component: 'paper_fast_lane',
    event_type: 'branch_circuit_learning_bypass',
    decision: 'allow',
    reason: 'branch_circuit_learning_bypass_markov_green_tiny_canary',
    data_source: 'not_ath_reclaim_fast',
    payload_json: JSON.stringify({
      entry_mode: 'lotto_not_ath_reclaim_tiny_probe',
      learning_bypass: {
        entry_branch: 'not_ath_reclaim_quote_clean_tiny_probe',
        markov_bucket: 'green',
      },
    }),
  });

  const insertQueue = db.prepare(`
    INSERT INTO paper_fast_entry_queue (
      id, created_at, updated_at, token_ca, symbol, source_type, entry_mode_hint,
      entry_branch, status, last_error, first_error, payload_json, market_session
    ) VALUES (
      @id, @created_at, @updated_at, @token_ca, @symbol, @source_type, @entry_mode_hint,
      @entry_branch, @status, @last_error, @first_error, @payload_json, @market_session
    )
  `);
  insertQueue.run({
    id: 1,
    created_at: now - 320,
    updated_at: now - 300,
    token_ca: 'TokenA',
    symbol: 'TA',
    source_type: 'not_ath_reclaim_fast',
    entry_mode_hint: 'lotto_not_ath_reclaim_tiny_probe',
    entry_branch: 'not_ath_reclaim_quote_clean_tiny_probe',
    status: 'rejected',
    last_error: 'fast_lane_quote_drift_hard_reject',
    first_error: 'fast_lane_quote_drift_hard_reject',
    payload_json: '{}',
    market_session: 'test',
  });
  insertQueue.run({
    id: 2,
    created_at: now - 280,
    updated_at: now - 260,
    token_ca: 'TokenB',
    symbol: 'TB',
    source_type: 'not_ath_reclaim_fast',
    entry_mode_hint: 'lotto_not_ath_reclaim_tiny_probe',
    entry_branch: 'not_ath_reclaim_quote_clean_tiny_probe',
    status: 'entered',
    last_error: null,
    first_error: null,
    payload_json: '{}',
    market_session: 'test',
  });

  db.prepare(`
    INSERT INTO paper_trades (
      id, symbol, token_ca, lifecycle_id, entry_ts, exit_ts, exit_reason,
      pnl_pct, trusted_peak_pnl, quote_peak_pnl, mark_peak_pnl, peak_trust_status,
      position_size_sol, signal_route, entry_mode, entry_branch, replay_source,
      entry_execution_audit_json
    ) VALUES (
      7, 'TB', 'TokenB', 'life-b', @entry_ts, @exit_ts, 'guardian_gap_crash',
      0.6362, 1.8565, 1.8565, 1.40, 'trusted_quote',
      0.001, 'not_ath_reclaim_fast', 'lotto_not_ath_reclaim_tiny_probe',
      'not_ath_reclaim_quote_clean_tiny_probe', 'paper_fast_lane',
      '{"success":true}'
    )
  `).run({ entry_ts: now - 240, exit_ts: now - 120 });

  const report = buildNotAthReclaimFunnelReport(
    db,
    new Set(['paper_decision_events', 'paper_fast_entry_queue', 'paper_trades']),
    now - 3600,
    { nowTs: now, limit: 100 },
  );

  assert.equal(report.summary.markov_green_unique, 1);
  assert.equal(report.summary.canary_allow_unique, 1);
  assert.equal(report.summary.branch_block_unique, 1);
  assert.equal(report.summary.branch_bypass_unique, 1);
  assert.equal(report.summary.queued_unique, 2);
  assert.equal(report.summary.quote_drift_reject_unique, 1);
  assert.equal(report.summary.entered_unique, 1);
  assert.equal(report.summary.closed_unique, 1);
  assert.equal(report.summary.peak100_unique, 1);
  assert.equal(report.summary.quote_attempt_to_entered_pct, 50);
  assert.deepEqual(report.by_markov_bucket, [{ key: 'green', n: 3 }]);
  assert.equal(report.queue_reason_summary[0].key, 'entered:none');
  assert.equal(report.trade_summary.closed, 1);
  assert.equal(report.trade_summary.win_rate_pct, 100);
  assert.equal(report.trade_summary.avg_pnl_pct, 63.62);
  assert.equal(report.trade_summary.avg_peak_pnl_pct, 185.65);
  assert.equal(report.trade_summary.peak100_n, 1);
  assert.equal(report.trade_summary.est_pnl_sol, 0.000636);
  assert.equal(report.trade_summary.entry_quote_success_rate_pct, 100);
  db.close();
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

test('v27 read-model readers reject mismatched artifact schemas', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'v27-schema-mismatch-'));
  const healthPath = join(dir, 'denominator_freshness.json');
  const modeReadinessPath = join(dir, 'mode_readiness.json');
  fs.writeFileSync(healthPath, JSON.stringify({
    refresh_schema_version: 'v2.6.0.read_model_refresh.v1',
    dashboard_safe: true,
    health: { dashboard_safe: true, normal_tiny_ready: true },
  }));
  fs.writeFileSync(modeReadinessPath, JSON.stringify({
    matrix_schema_version: 'v2.6.0.mode_readiness.v1',
    highest_allowed_mode: 'normal_tiny',
    health: { normal_tiny_ready: true },
  }));

  const health = readV27DenominatorReadModelHealth({ healthPath });
  const readiness = readV27ModeReadiness({ modeReadinessPath });

  assert.equal(health.available, false);
  assert.equal(health.dashboard_safe, false);
  assert.deepEqual(health.blocking_reasons, ['v27_read_model_health_schema_mismatch']);
  assert.equal(health.health.status, 'v27_read_model_health_schema_mismatch');
  assert.equal(readiness.available, false);
  assert.equal(readiness.highest_allowed_mode, null);
  assert.deepEqual(readiness.blocking_reasons, ['v27_mode_readiness_schema_mismatch']);
  assert.equal(readiness.health.status, 'v27_mode_readiness_schema_mismatch');
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

test('v27 read-model worker health treats readiness blockers as healthy governance', () => {
  const nowMs = Date.parse('2026-08-08T04:00:00.000Z');
  const recentMtime = '2026-08-08T03:59:00.000Z';
  const worker = readV27ReadModelWorkerHealth({
    nowMs,
    enabled: true,
    pid: 12345,
    pidAlive: true,
    intervalSec: 60,
    initialDelaySec: 120,
    maxAgeMinutes: 5,
    lockPid: 12345,
    lockPidAlive: true,
    statusPayload: {
      schema_version: 'v2.7.0.read_model_worker_status.v1',
      running: true,
      pid: 12345,
      status: 'readiness_blocked',
      last_refresh_status: 'readiness_blocked',
      started_at: '2026-08-08T03:50:00.000Z',
      last_attempt_at: '2026-08-08T03:58:30.000Z',
      last_success_at: '2026-08-08T03:59:00.000Z',
      last_error_at: null,
      last_error: null,
      error_count: 0,
    },
    statusArtifact: { available: true, path: '/tmp/status.json', mtime: recentMtime, size_bytes: 100 },
    denominatorArtifact: { available: true, path: '/tmp/denominator.json', mtime: recentMtime, size_bytes: 100 },
    readinessArtifact: { available: true, path: '/tmp/readiness.json', mtime: recentMtime, size_bytes: 100 },
    denominatorHealth: {
      available: true,
      dashboard_safe: true,
      blocking_reasons: [],
      health: { status: 'read_model_refresh_ok' },
    },
    modeReadiness: {
      available: true,
      highest_allowed_mode: 'observe_only',
      health: {
        status: 'mode_readiness_evaluated',
        observe_only_ready: true,
        shadow_ready: false,
        ultra_tiny_ready: false,
        normal_tiny_ready: false,
      },
      modes: {
        observe_only: { blocking_contracts: [] },
        shadow: { blocking_contracts: ['ShadowContract'] },
        ultra_tiny: { blocking_contracts: ['UltraContract'] },
        normal_tiny: { blocking_contracts: ['NormalContract'] },
      },
    },
  });

  assert.equal(worker.public_safe, true);
  assert.equal(worker.running, true);
  assert.equal(worker.pid_alive, true);
  assert.equal(worker.lock_pid_alive, true);
  assert.equal(worker.status, 'readiness_blocked');
  assert.equal(worker.healthy, true);
  assert.equal(worker.degraded, false);
  assert.equal(worker.fresh, true);
  assert.equal(worker.artifact_age_minutes, 1);
  assert.equal(worker.mode_readiness.highest_allowed_mode, 'observe_only');
  assert.equal(worker.mode_readiness.normal_tiny_ready, false);
  assert.deepEqual(worker.mode_readiness.blocking_contract_counts, {
    observe_only: 0,
    shadow: 1,
    ultra_tiny: 1,
    normal_tiny: 1,
  });
});

test('v27 read-model worker health covers disabled starting invalid and refresh-error states', () => {
  const nowMs = Date.parse('2026-08-08T04:00:00.000Z');
  const disabled = readV27ReadModelWorkerHealth({
    nowMs,
    enabled: false,
    pidAlive: false,
    lockPidAlive: false,
    statusPayload: null,
    statusArtifact: { available: false },
    denominatorArtifact: { available: false },
    readinessArtifact: { available: false },
    denominatorHealth: { available: false, dashboard_safe: false, health: {} },
    modeReadiness: { available: false, health: {}, modes: {} },
  });
  assert.equal(disabled.status, 'disabled');
  assert.equal(disabled.degraded, false);

  const starting = readV27ReadModelWorkerHealth({
    nowMs,
    enabled: true,
    pid: 33333,
    pidAlive: true,
    lockPid: 33333,
    lockPidAlive: true,
    intervalSec: 60,
    initialDelaySec: 120,
    maxAgeMinutes: 5,
    statusPayload: {
      schema_version: 'v2.7.0.read_model_worker_status.v1',
      running: true,
      pid: 33333,
      started_at: '2026-08-08T03:59:00.000Z',
      status: 'starting',
      error_count: 0,
    },
    statusArtifact: { available: true, mtime: '2026-08-08T03:59:00.000Z' },
    denominatorArtifact: { available: false },
    readinessArtifact: { available: false },
    denominatorHealth: { available: false, dashboard_safe: false, health: {} },
    modeReadiness: { available: false, health: {}, modes: {} },
  });
  assert.equal(starting.status, 'starting');
  assert.equal(starting.degraded, false);

  const invalid = readV27ReadModelWorkerHealth({
    nowMs,
    enabled: true,
    pid: 33333,
    pidAlive: true,
    lockPid: 33333,
    lockPidAlive: true,
    statusPayload: { error_code: 'agent_artifact_json_parse_failed', error: 'bad json' },
    statusArtifact: { available: true, mtime: '2026-08-08T03:59:00.000Z' },
    denominatorArtifact: { available: false },
    readinessArtifact: { available: false },
    denominatorHealth: { available: false, dashboard_safe: false, error: 'schema mismatch', health: {} },
    modeReadiness: { available: false, error: 'schema mismatch', health: {}, modes: {} },
  });
  assert.equal(invalid.status, 'artifact_invalid');
  assert.equal(invalid.degraded, true);

  const refreshError = readV27ReadModelWorkerHealth({
    nowMs,
    enabled: true,
    pid: 33333,
    pidAlive: true,
    lockPid: 33333,
    lockPidAlive: true,
    maxAgeMinutes: 5,
    statusPayload: {
      schema_version: 'v2.7.0.read_model_worker_status.v1',
      running: true,
      pid: 33333,
      started_at: '2026-08-08T03:40:00.000Z',
      last_success_at: '2026-08-08T03:50:00.000Z',
      last_error_at: '2026-08-08T03:59:00.000Z',
      last_refresh_status: 'refresh_error',
      last_error: 'RuntimeError:unit',
      error_count: 1,
    },
    statusArtifact: { available: true, mtime: '2026-08-08T03:59:00.000Z' },
    denominatorArtifact: { available: true, mtime: '2026-08-08T03:59:00.000Z' },
    readinessArtifact: { available: true, mtime: '2026-08-08T03:59:00.000Z' },
    denominatorHealth: { available: true, dashboard_safe: true, health: {} },
    modeReadiness: { available: true, health: { normal_tiny_ready: false }, modes: {} },
  });
  assert.equal(refreshError.status, 'refresh_error');
  assert.equal(refreshError.degraded, true);
});

test('v27 read-model worker health distinguishes stopped and stale workers', () => {
  const nowMs = Date.parse('2026-08-08T04:00:00.000Z');
  const base = {
    nowMs,
    enabled: true,
    pid: 22222,
    intervalSec: 60,
    initialDelaySec: 0,
    maxAgeMinutes: 5,
    lockPid: 22222,
    statusPayload: {
      schema_version: 'v2.7.0.read_model_worker_status.v1',
      running: true,
      pid: 22222,
      started_at: '2026-08-08T03:40:00.000Z',
      last_success_at: '2026-08-08T03:41:00.000Z',
      last_refresh_status: 'readiness_blocked',
      error_count: 0,
    },
    statusArtifact: { available: true, path: '/tmp/status.json', mtime: '2026-08-08T03:59:00.000Z', size_bytes: 100 },
    denominatorHealth: {
      available: true,
      dashboard_safe: true,
      blocking_reasons: [],
      health: { status: 'read_model_refresh_ok' },
    },
    modeReadiness: {
      available: true,
      highest_allowed_mode: 'observe_only',
      health: { status: 'mode_readiness_evaluated', normal_tiny_ready: false },
      modes: {},
    },
  };

  const stopped = readV27ReadModelWorkerHealth({
    ...base,
    pidAlive: false,
    lockPidAlive: false,
    denominatorArtifact: { available: true, path: '/tmp/denominator.json', mtime: '2026-08-08T03:59:00.000Z', size_bytes: 100 },
    readinessArtifact: { available: true, path: '/tmp/readiness.json', mtime: '2026-08-08T03:59:00.000Z', size_bytes: 100 },
  });
  assert.equal(stopped.status, 'worker_not_running');
  assert.equal(stopped.healthy, false);
  assert.equal(stopped.degraded, true);

  const stale = readV27ReadModelWorkerHealth({
    ...base,
    pidAlive: true,
    lockPidAlive: true,
    denominatorArtifact: { available: true, path: '/tmp/denominator.json', mtime: '2026-08-08T03:40:00.000Z', size_bytes: 100 },
    readinessArtifact: { available: true, path: '/tmp/readiness.json', mtime: '2026-08-08T03:40:00.000Z', size_bytes: 100 },
  });
  assert.equal(stale.status, 'artifact_stale');
  assert.equal(stale.fresh, false);
  assert.equal(stale.degraded, true);
});

function evaluatorSnapshotHealthFixture(nowMs) {
  const snapshotId = '20260808T035900Z-1234abcd';
  const snapshotTs = Math.floor(nowMs / 1000) - 60;
  const pinnedOffsets = {
    signal: -0.30,
    paper: -0.10,
    raw: -0.20,
    kline: -0.15,
  };
  const pinnedReadViewIds = {
    signal_main_selective_copy: '1'.repeat(32),
    paper_main_selective_copy: '2'.repeat(32),
    paper_decision_events_parallel_stage: '3'.repeat(32),
    a_class_decision_events_parallel_stage: '4'.repeat(32),
    opportunity_events_parallel_stage: '5'.repeat(32),
    opportunity_event_path_samples_parallel_stage: '6'.repeat(32),
    raw_main_selective_copy: '7'.repeat(32),
    kline_main_selective_copy: '8'.repeat(32),
  };
  const sharedTargetPinnedRoles = {
    candidate_shadow_observations: 'paper_main_selective_copy',
    paper_decision_events: 'paper_decision_events_parallel_stage',
    a_class_decision_events: 'a_class_decision_events_parallel_stage',
    opportunity_events: 'opportunity_events_parallel_stage',
    opportunity_event_path_samples:
      'opportunity_event_path_samples_parallel_stage',
  };
  const databaseReport = (name) => ({
    schema_version: 1,
    snapshot_path: `/snapshot/${name}.db`,
    snapshot_size_bytes: 100,
    quick_check: ['ok'],
    snapshot_sha256: 'a'.repeat(64),
    missing_required_tables: [],
    missing_required_watermarks: [],
    source_read_lock_budget_passed: true,
    source_read_lock_limit_sec: 300,
    source_read_lock_duration_sec: 3.33,
    source_read_lock_released_before_index_build: true,
    source_open_mode: 'read_only_attached_uri',
    selection_upper_epoch: snapshotTs,
    temporary_full_backup_size_bytes: 0,
    database_budget_passed: true,
    pinned_read_views: [
      {
        role: `${name}_main_selective_copy`,
        read_view_id: pinnedReadViewIds[`${name}_main_selective_copy`],
        pinned_midpoint_epoch: snapshotTs + pinnedOffsets[name],
        source_read_lock_limit_sec: 300,
      },
    ],
  });
  const stageSchemaEvidence = (columnCount = 10) => ({
    schema_version: 'parallel_paper_event_stage.v4',
    stage_schema_mode: 'lossless_compressed_chunk_spool',
    source_create_sql_sha256: '1'.repeat(64),
    destination_create_sql_sha256: '1'.repeat(64),
    source_column_contract_sha256: '3'.repeat(64),
    destination_column_contract_sha256: '3'.repeat(64),
    stage_storage_contract_sha256:
      PARALLEL_PAPER_STAGE_STORAGE_CONTRACT_SHA256,
    stage_codec_schema_version: PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION,
    stage_compression: PARALLEL_PAPER_STAGE_COMPRESSION,
    stage_chunk_target_bytes: PARALLEL_PAPER_STAGE_CHUNK_TARGET_BYTES,
    stage_chunk_count: 1,
    stage_raw_size_bytes: 4096,
    stage_compressed_payload_size_bytes: 1024,
    stage_rows_sha256: '4'.repeat(64),
    hydrated_rows_sha256: '4'.repeat(64),
    stage_column_count: columnCount,
    stage_index_count: 0,
    stage_storage_contract_passed: true,
    stage_chunk_integrity_passed: true,
    stage_row_digest_matched: true,
    source_constraints_deferred_off_source_lock: true,
    destination_schema_restored_after_source_read_lock_release: true,
    source_constraints_rebuilt_after_source_read_lock_release: true,
    compressed_during_source_read_lock: true,
    hydrated_after_source_read_lock_release: true,
  });
  const sharedAdvisoryEvidence = ({ target, advisory, actualRows }) => {
    const indexed = target !== 'opportunity_event_path_samples';
    const selectedRows = actualRows;
    const physicalBytes = 4096;
    const pageCount = 1;
    const sourceIndexName = indexed ? `idx_${target}_time` : null;
    const candidateTarget = target === 'candidate_shadow_observations';
    const candidateOrderPhysicalBytes = candidateTarget ? 4096 : null;
    const candidateOrderCellCount = candidateTarget ? actualRows : null;
    return {
      advisory_schema_version: 'sqlite_dbstat_advisory_demand.v1',
      advisory_formula:
        'source_physical_times_selected_row_fraction_plus_per_row_overhead_'
        + 'plus_root_reserve_plus_candidate_signal_index_fraction',
      query_bounded: true,
      physical_upper_bound_claimed: false,
      capacity_sample_used: false,
      dbstat_completed: true,
      dbstat_timed_out: false,
      dbstat_timeout_sec: 20,
      dbstat_elapsed_sec: 0.01,
      source_measurement_trust_boundary: 'same_pinned_read_view_as_copy',
      pinned_read_view_id:
        pinnedReadViewIds[sharedTargetPinnedRoles[target]],
      pinned_read_view_role: sharedTargetPinnedRoles[target],
      estimate_started_after_pin: true,
      estimate_completed_before_copy: true,
      row_count_binding_mode: indexed
        ? 'exact_selected_rows'
        : 'full_source_row_upper',
      sample_limit_rows: 256,
      selected_row_count: selectedRows,
      source_row_count_upper: selectedRows,
      source_row_count_upper_basis: candidateTarget
        ? 'exact_signal_index_entry_count'
        : 'table_dbstat_cell_upper',
      sample_rows: 0,
      average_row_bytes_diagnostic: null,
      sample_max_row_bytes_diagnostic: null,
      sample_row_bytes_basis: null,
      source_dbstat_page_count: pageCount,
      source_dbstat_page_size: 4096,
      source_dbstat_physical_bytes: physicalBytes,
      source_dbstat_payload_bytes: 0,
      source_dbstat_unused_bytes: physicalBytes,
      source_dbstat_max_payload_bytes: 0,
      source_dbstat_cell_upper_count: candidateTarget
        ? selectedRows + 1
        : selectedRows,
      advisory_row_overhead_bytes: 32,
      advisory_index_overhead_bytes: 32,
      advisory_root_reserve_pages: 2,
      source_row_fraction_numerator: selectedRows,
      source_row_fraction_denominator: selectedRows,
      table_sample_payload_advisory_bytes: null,
      table_scaled_physical_advisory_bytes: 4096,
      table_row_overhead_advisory_bytes: 32,
      table_root_reserve_advisory_bytes: 8192,
      table_advisory_bytes: 16384,
      candidate_order_index_scaled_physical_advisory_bytes:
        candidateTarget ? 4096 : 0,
      candidate_order_index_row_overhead_advisory_bytes:
        candidateTarget ? 32 : 0,
      candidate_order_index_advisory_bytes:
        candidateTarget ? 16384 : 0,
      advisory_required_bytes: advisory,
      candidate_order_source_index_name: candidateTarget
        ? 'idx_candidate_shadow_obs_signal'
        : null,
      candidate_order_source_index_columns: candidateTarget
        ? ['signal_id']
        : [],
      candidate_order_source_index_partial: candidateTarget ? false : null,
      candidate_order_source_index_dbstat_page_count: candidateTarget ? 1 : null,
      candidate_order_source_index_dbstat_page_size: candidateTarget ? 4096 : null,
      candidate_order_source_index_dbstat_physical_bytes:
        candidateOrderPhysicalBytes,
      candidate_order_source_index_dbstat_payload_bytes: candidateTarget ? 0 : null,
      candidate_order_source_index_dbstat_unused_bytes: candidateTarget ? 4096 : null,
      candidate_order_source_index_dbstat_max_payload_bytes: candidateTarget ? 0 : null,
      candidate_order_source_index_dbstat_cell_upper_count:
        candidateOrderCellCount,
      candidate_order_source_index_structural_overhead_bytes:
        candidateTarget ? 4096 : null,
      source_index_name: sourceIndexName,
      source_query_plan: indexed
        ? [`SEARCH src.${target} USING INDEX ${sourceIndexName} (ts>? AND ts<?)`]
        : [],
      source_query_plan_uses_index: indexed ? true : null,
      source_query_plan_uses_range_search: indexed ? true : null,
      source_query_plan_full_table_scan_detected: indexed ? false : null,
    };
  };
  const sharedTarget = ({
    target,
    stageFilename,
    advisory,
    baseline,
    allocationWeight,
    grant,
    actual,
    actualRows = 1,
    borrowed = 0,
    historyState = 'none',
  }) => ({
    target,
    source_table: target,
    stage_filename: stageFilename,
    storage_schema_version: target === 'candidate_shadow_observations'
      ? 'candidate_observation_selective_stage.v1'
      : 'parallel_paper_event_stage.v4',
    history_storage_schema_version: null,
    history_storage_compatible: false,
    required: target !== 'opportunity_event_path_samples',
    minimum_cap_bytes: 12288,
    advisory_required_bytes: advisory,
    advisory_strategy: target === 'opportunity_event_path_samples'
      ? 'dbstat_full_btree_advisory_demand'
      : 'dbstat_proportional_advisory_with_indexed_row_count',
    advisory_query_bounded: true,
    physical_upper_bound_claimed: false,
    advisory_evidence: sharedAdvisoryEvidence({
      target,
      advisory,
      actualRows,
    }),
    history_state: historyState,
    history_high_water_bytes: 0,
    history_granted_cap_bytes: 0,
    history_cap_hit: false,
    history_copy_completed: false,
    baseline_required_bytes: baseline,
    allocation_weight_bytes: allocationWeight,
    granted_cap_bytes: grant,
    borrowed_shared_pool_bytes: borrowed,
    advisory_shortfall_bytes: Math.max(0, advisory - grant),
    evidence_sources: ['advisory_source_demand'],
    actual_usage_bytes: actual,
    high_water_bytes: actual,
    actual_rows_copied: actualRows,
    row_count_bound_to_snapshot: true,
    advisory_exceeded: actual > advisory,
    advisory_delta_bytes: actual - advisory,
    copy_completed: true,
    cap_hit: false,
    within_grant: true,
    utilization_ratio: actual / grant,
    evidence_source: 'accepted_producer_stage_report',
  });
  const sharedStageBudget = {
    schema_version: 'shared_stage_budget.v2',
    allocation_mode: 'history_high_water_plus_advisory_source_demand',
    hash_canonicalization: 'json_sorted_float64_bits.v1',
    attempt_id: snapshotId,
    generated_at: '2026-08-08T03:59:00.000Z',
    page_size: 4096,
    total_cap_bytes: 102400,
    active_targets: [
      'candidate_shadow_observations',
      'paper_decision_events',
      'a_class_decision_events',
      'opportunity_events',
      'opportunity_event_path_samples',
    ],
    minimum_total_bytes: 61440,
    baseline_required_total_bytes: 61440,
    advisory_demand_total_bytes: 98304,
    residual_pool_bytes: 40960,
    borrowing_priority_targets: [
      'candidate_shadow_observations',
      'paper_decision_events',
      'a_class_decision_events',
      'opportunity_events',
      'opportunity_event_path_samples',
    ],
    allocation_weight_total_bytes: 98304,
    history_used: false,
    history_reason: 'history_missing',
    history_attempt_id: null,
    fixed_percentage_allocation_used: false,
    pinned_read_view_binding_required: true,
    all_advisory_estimates_pinned_read_view_bound: true,
    all_advisory_queries_bounded: true,
    physical_upper_bound_claimed: false,
    global_hard_cap_enforced: true,
    per_target_max_page_count_enforced: true,
    capacity_sufficient_basis: 'minimum_and_verified_history_high_water',
    targets: {
      candidate_shadow_observations: sharedTarget({
        target: 'candidate_shadow_observations',
        stageFilename: '.candidate-observation-stage.db',
        advisory: 32768,
        baseline: 12288,
        allocationWeight: 32768,
        grant: 24576,
        actual: 16000,
        borrowed: 12288,
      }),
      paper_decision_events: sharedTarget({
        target: 'paper_decision_events',
        stageFilename: '.paper-decision-events-stage.db',
        advisory: 16384,
        baseline: 12288,
        allocationWeight: 16384,
        grant: 20480,
        actual: 8192,
        borrowed: 8192,
      }),
      a_class_decision_events: sharedTarget({
        target: 'a_class_decision_events',
        stageFilename: '.a-class-decision-events-stage.db',
        advisory: 16384,
        baseline: 12288,
        allocationWeight: 16384,
        grant: 20480,
        actual: 8192,
        borrowed: 8192,
      }),
      opportunity_events: sharedTarget({
        target: 'opportunity_events',
        stageFilename: '.opportunity-events-stage.db',
        advisory: 16384,
        baseline: 12288,
        allocationWeight: 16384,
        grant: 20480,
        actual: 8192,
        borrowed: 8192,
      }),
      opportunity_event_path_samples: sharedTarget({
        target: 'opportunity_event_path_samples',
        stageFilename: '.opportunity-event-path-samples-stage.db',
        advisory: 16384,
        baseline: 12288,
        allocationWeight: 16384,
        grant: 16384,
        actual: 8192,
        borrowed: 4096,
      }),
    },
    total_granted_bytes: 102400,
    grants_sum_matches_total_cap: true,
    capacity_sufficient: true,
    accepted: true,
    actual_total_bytes: 48768,
    unconsumed_bytes: 53632,
    all_targets_within_grant: true,
    targets_exceeding_advisory: [],
    advisory_miss_count: 0,
    all_target_row_counts_bound_to_snapshot: true,
    captured_at: '2026-08-08T03:59:20.000Z',
    captured_before_cleanup: true,
    cleanup_completed: true,
    stage_files_removed: true,
    unregistered_stage_files: [],
    no_unregistered_stage_files: true,
    plan_sha256: null,
    evidence_sha256: null,
  };
  sharedStageBudget.plan_sha256 = sharedStageBudgetPlanSha256(
    sharedStageBudget,
  );
  sharedStageBudget.evidence_sha256 = sharedStageBudgetEvidenceSha256(
    sharedStageBudget,
  );
  return {
    statusPayload: {
      schema_version: 'cross_db_evaluator_snapshot_worker_status.v1',
      pid: 33333,
      running: true,
      attempt_running: false,
      status: 'completed',
      started_at: '2026-08-08T03:45:00.000Z',
      last_attempt_at: '2026-08-08T03:59:00.000Z',
      last_success_at: '2026-08-08T03:59:30.000Z',
      consecutive_failure_count: 0,
      success_interval_sec: 21600,
      failure_retry_sec: 60,
      next_attempt_delay_sec: 21600,
      next_attempt_at: '2026-08-08T09:59:30.000Z',
      error_count: 0,
      accepted: true,
      snapshot_id: snapshotId,
      last_accepted_snapshot: {
        snapshot_id: snapshotId,
        manifest_sha256: 'd'.repeat(64),
        max_source_read_lock_duration_sec: 3.33,
        numeric_evidence_schema_version: 'evaluator_snapshot_numeric_evidence.v3',
        numeric_evidence_schema_sha256: JSON_NUMERIC_EVIDENCE_CONTRACT_SHA256,
        numeric_evidence_schema_validated_before_publish: true,
      },
      shared_stage_budget: sharedStageBudget,
      promotion_allowed: false,
    },
    manifestPayload: {
      schema_version: 'cross_db_evaluator_snapshot.v3',
      numeric_evidence_schema_version: 'evaluator_snapshot_numeric_evidence.v3',
      numeric_evidence_schema_sha256: JSON_NUMERIC_EVIDENCE_CONTRACT_SHA256,
      numeric_evidence_schema_validated_before_publish: true,
      snapshot_id: snapshotId,
      snapshot_ts: snapshotTs,
      git_commit: 'f'.repeat(40),
      accepted: true,
      immutable: true,
      quick_checks_passed: true,
      required_tables_present: true,
      required_watermarks_present: true,
      cross_database_time_skew_passed: true,
      pinned_read_view_count: 8,
      cross_database_time_skew_sec: 0.40,
      max_allowed_cross_database_time_skew_sec: 30,
      source_read_lock_budget_passed: true,
      max_source_read_lock_sec: 300,
      indexes_built_after_source_read_lock_release: true,
      candidate_projection_after_source_read_lock_release: true,
      candidate_stage_removed_before_publish: true,
      shared_stage_estimates_bound_to_copy_read_views: true,
      parallel_paper_stage_schema_version: 'parallel_paper_event_stage.v4',
      parallel_paper_stage_tables: [
        'paper_decision_events',
        'a_class_decision_events',
        'opportunity_events',
        'opportunity_event_path_samples',
      ],
      parallel_paper_stage_count: 4,
      parallel_paper_stage_inventory_passed: true,
      parallel_paper_stages_all_pinned: true,
      parallel_paper_stages_all_merged_after_source_read_lock_release: true,
      parallel_paper_stages_all_removed_before_publish: true,
      paper_decision_parallel_read_view_pinned: true,
      paper_decision_parallel_stage_merged_after_source_read_lock_release: true,
      paper_decision_parallel_stage_removed_before_publish: true,
      source_mutation_free: true,
      bounded_selective_snapshot: true,
      selection_upper_bounds_consistent: true,
      selection_contract: {
        schema_version: 'evaluator_snapshot_selection.v1',
        future_rows_excluded: true,
        table_rules_are_explicit: true,
        common_upper_epoch: snapshotTs,
        supported_capture_windows_hours: [24, 48, 72],
      },
      output_cap_passed: true,
      output_size_bytes: 1000,
      output_cap_bytes: 10000,
      partial_artifacts_absent: true,
      active_database_reads_allowed_for_autoloop: false,
      shared_stage_budget: sharedStageBudget,
      shared_stage_budget_passed: true,
      disk_preflight: {
        accepted: true,
        free_bytes: 122400,
        selective_snapshot_output_cap_bytes: 10000,
        temporary_full_backup_bytes: 0,
        temporary_stage_raw_cap_bytes: 102400,
        temporary_stage_alignment_reserve_bytes: 0,
        temporary_stage_total_cap_bytes: 102400,
        temporary_candidate_stage_cap_bytes: 24576,
        temporary_parallel_paper_stage_cap_bytes: {
          paper_decision_events: 20480,
          a_class_decision_events: 20480,
          opportunity_events: 20480,
          opportunity_event_path_samples: 16384,
        },
        temporary_paper_decision_stage_cap_bytes: 20480,
        configured_parallel_paper_stage_tables: [
          'paper_decision_events',
          'a_class_decision_events',
          'opportunity_events',
          'opportunity_event_path_samples',
        ],
        parallel_paper_stage_tables: [
          'paper_decision_events',
          'a_class_decision_events',
          'opportunity_events',
          'opportunity_event_path_samples',
        ],
        omitted_optional_parallel_paper_stage_tables: [],
        candidate_stage_budget_mode: 'shared_stage_budget_coordinator',
        shared_stage_budget: sharedStageBudget,
        fixed_percentage_allocation_used: false,
        candidate_stage_minimum_cap_bytes: 12288,
        parallel_paper_stage_minimum_cap_bytes: 12288,
        paper_decision_stage_minimum_cap_bytes: 12288,
        estimated_peak_working_bytes: 112400,
        estimated_free_after_bytes: 112400,
        estimated_free_at_peak_bytes: 10000,
        required_reserve_bytes: 10000,
        fail_closed_on_insufficient_space: true,
      },
      databases: {
        signal: databaseReport('signal'),
        paper: {
          ...databaseReport('paper'),
          source_upper_watermarks: {
            a_class_decision_events: { event_ts: snapshotTs },
          },
          upper_watermarks: {
            a_class_decision_events: { id: 1, event_ts: snapshotTs },
          },
          source_read_lock_duration_sec: 3.33,
          main_source_read_lock_duration_sec: 3.33,
          temporary_candidate_stage_size_bytes: 16000,
          temporary_candidate_stage_removed_before_publish: true,
          candidate_projection_duration_sec: 0.1,
          shared_stage_estimates_bound_to_copy_read_views: true,
          parallel_paper_source_read_lock_duration_sec: {
            paper_decision_events: 2.5,
            a_class_decision_events: 2.75,
            opportunity_events: 2.25,
            opportunity_event_path_samples: 2.9,
          },
          paper_decision_source_read_lock_duration_sec: 2.5,
          parallel_paper_stage_tables: [
            'paper_decision_events',
            'a_class_decision_events',
            'opportunity_events',
            'opportunity_event_path_samples',
          ],
          parallel_paper_stage_count: 4,
          parallel_paper_stages_all_pinned: true,
          parallel_paper_stages_all_merged_after_source_read_lock_release: true,
          parallel_paper_stages_all_removed_before_publish: true,
          parallel_paper_stages: {
            paper_decision_events: {
              ...stageSchemaEvidence(10),
              role: 'paper_decision_events_parallel_stage',
              stage_size_bytes: 8192,
              stage_budget_bytes: 20480,
              stage_page_size: 4096,
              rows_copied: 1,
              rows_merged: 1,
              merge_duration_sec: 0.25,
              source_read_lock_duration_sec: 2.5,
              source_read_lock_budget_passed: true,
              merged_after_source_read_lock_release: true,
              removed_before_publish: true,
              quick_check: ['ok'],
              full_fidelity_row_copy: true,
              payload_semantics_preserved: true,
            },
            a_class_decision_events: {
              ...stageSchemaEvidence(8),
              role: 'a_class_decision_events_parallel_stage',
              stage_size_bytes: 8192,
              stage_budget_bytes: 20480,
              stage_page_size: 4096,
              rows_copied: 1,
              rows_merged: 1,
              merge_duration_sec: 0.20,
              source_read_lock_duration_sec: 2.75,
              source_read_lock_budget_passed: true,
              merged_after_source_read_lock_release: true,
              removed_before_publish: true,
              quick_check: ['ok'],
              full_fidelity_row_copy: true,
              payload_semantics_preserved: true,
            },
            opportunity_events: {
              ...stageSchemaEvidence(7),
              role: 'opportunity_events_parallel_stage',
              stage_size_bytes: 8192,
              stage_budget_bytes: 20480,
              stage_page_size: 4096,
              rows_copied: 1,
              rows_merged: 1,
              merge_duration_sec: 0.15,
              source_read_lock_duration_sec: 2.25,
              source_read_lock_budget_passed: true,
              merged_after_source_read_lock_release: true,
              removed_before_publish: true,
              quick_check: ['ok'],
              full_fidelity_row_copy: true,
              payload_semantics_preserved: true,
            },
            opportunity_event_path_samples: {
              ...stageSchemaEvidence(6),
              role: 'opportunity_event_path_samples_parallel_stage',
              stage_size_bytes: 8192,
              stage_budget_bytes: 16384,
              stage_page_size: 4096,
              rows_copied: 1,
              rows_merged: 1,
              merge_duration_sec: 0.30,
              source_read_lock_duration_sec: 2.9,
              source_read_lock_budget_passed: true,
              merged_after_source_read_lock_release: true,
              removed_before_publish: true,
              quick_check: ['ok'],
              full_fidelity_row_copy: true,
              payload_semantics_preserved: true,
            },
          },
          paper_decision_parallel_stage_used: true,
          paper_decision_parallel_stage_schema_version: 'parallel_paper_event_stage.v4',
          paper_decision_parallel_read_view_pinned: true,
          paper_decision_parallel_stage_merged_after_source_read_lock_release: true,
          paper_decision_parallel_stage_removed_before_publish: true,
          paper_decision_parallel_stage_size_bytes: 8192,
          paper_decision_parallel_stage_budget_bytes: 20480,
          paper_decision_parallel_stage_page_size: 4096,
          paper_decision_parallel_stage_rows_merged: 1,
          paper_decision_parallel_stage_merge_duration_sec: 0.25,
          pinned_read_views: [
            {
              role: 'paper_main_selective_copy',
              read_view_id: pinnedReadViewIds.paper_main_selective_copy,
              pinned_midpoint_epoch: snapshotTs - 0.10,
              source_read_lock_limit_sec: 300,
            },
            {
              role: 'paper_decision_events_parallel_stage',
              read_view_id:
                pinnedReadViewIds.paper_decision_events_parallel_stage,
              pinned_midpoint_epoch: snapshotTs - 0.05,
              source_read_lock_limit_sec: 300,
            },
            {
              role: 'a_class_decision_events_parallel_stage',
              read_view_id:
                pinnedReadViewIds.a_class_decision_events_parallel_stage,
              pinned_midpoint_epoch: snapshotTs,
              source_read_lock_limit_sec: 300,
            },
            {
              role: 'opportunity_events_parallel_stage',
              read_view_id:
                pinnedReadViewIds.opportunity_events_parallel_stage,
              pinned_midpoint_epoch: snapshotTs + 0.05,
              source_read_lock_limit_sec: 300,
            },
            {
              role: 'opportunity_event_path_samples_parallel_stage',
              read_view_id:
                pinnedReadViewIds.opportunity_event_path_samples_parallel_stage,
              pinned_midpoint_epoch: snapshotTs + 0.10,
              source_read_lock_limit_sec: 300,
            },
          ],
          source_watermark_query_evidence: {
            candidate_shadow_observations: {
              strategy: 'indexed_anchor_max',
              column: 'observed_at',
              source_index_name: 'idx_candidate_shadow_obs_observed',
              query_plan: [
                'SEARCH src.candidate_shadow_observations USING COVERING INDEX idx_candidate_shadow_obs_observed',
              ],
              uses_declared_index: true,
              full_table_scan_detected: false,
            },
            candidate_shadow_virtual_trades: {
              strategy: 'indexed_anchor_max',
              column: 'observed_at',
              source_index_name: 'idx_candidate_shadow_virtual_observed',
              query_plan: [
                'SEARCH src.candidate_shadow_virtual_trades USING COVERING INDEX idx_candidate_shadow_virtual_observed',
              ],
              uses_declared_index: true,
              full_table_scan_detected: false,
            },
            paper_decision_events: {
              strategy: 'indexed_anchor_max',
              column: 'event_ts',
              source_index_name: 'idx_pde_event_ts',
              query_plan: [
                'SEARCH src.paper_decision_events USING COVERING INDEX idx_pde_event_ts',
              ],
              uses_declared_index: true,
              full_table_scan_detected: false,
            },
            a_class_decision_events: {
              strategy: 'indexed_anchor_max',
              column: 'event_ts',
              source_index_name: 'idx_a_class_decision_recent',
              query_plan: [
                'SEARCH src.a_class_decision_events USING COVERING INDEX idx_a_class_decision_recent',
              ],
              uses_declared_index: true,
              full_table_scan_detected: false,
            },
            opportunity_events: {
              strategy: 'indexed_anchor_max',
              column: 'event_ts',
              source_index_name: 'idx_opportunity_events_recent',
              query_plan: [
                'SEARCH src.opportunity_events USING COVERING INDEX idx_opportunity_events_recent',
              ],
              uses_declared_index: true,
              full_table_scan_detected: false,
            },
          },
          selected_tables: {
            candidate_shadow_observations: {
              included: true,
              predicate_strategy: 'indexed_epoch_seconds',
              indexed_time_anchor: 'observed_at',
              source_index_name: 'idx_candidate_shadow_obs_observed',
              source_index_columns: ['observed_at'],
              source_index_partial: false,
              source_query_plan: [
                'SEARCH src.candidate_shadow_observations USING COVERING INDEX idx_candidate_shadow_obs_observed (observed_at>? AND observed_at<?)',
              ],
              source_query_plan_uses_index: true,
              source_query_plan_uses_range_search: true,
              source_query_plan_full_table_scan_detected: false,
              rows_copied: 1,
              storage_projection: {
                schema_version: 'candidate_observation_payload_projection.v1',
                applied: true,
                projection_started_after_source_read_view_release: true,
                source_stage_schema_version:
                  'candidate_observation_selective_stage.v1',
                source_stage_size_bytes: 16000,
                stage_order_index_name: 'idx_a3_candidate_stage_signal',
                stage_query_plan: [
                  'SCAN stage USING INDEX idx_a3_candidate_stage_signal',
                ],
                stage_query_plan_uses_order_index: true,
                stage_query_plan_temp_btree_detected: false,
                payload_semantics_preserved: true,
                unknown_payload_keys_preserved: true,
                missing_and_null_keys_preserved: true,
              },
            },
            candidate_shadow_virtual_trades: {
              included: true,
              predicate_strategy: 'indexed_epoch_seconds',
              indexed_time_anchor: 'observed_at',
              source_index_name: 'idx_candidate_shadow_virtual_observed',
              source_index_columns: ['observed_at'],
              source_index_partial: false,
              source_query_plan: [
                'SEARCH src.candidate_shadow_virtual_trades USING COVERING INDEX idx_candidate_shadow_virtual_observed (observed_at>? AND observed_at<?)',
              ],
              source_query_plan_uses_index: true,
              source_query_plan_uses_range_search: true,
              source_query_plan_full_table_scan_detected: false,
              rows_copied: 13392,
            },
            paper_decision_events: {
              ...stageSchemaEvidence(10),
              included: true,
              predicate_strategy: 'indexed_epoch_seconds',
              indexed_time_anchor: 'event_ts',
              source_index_name: 'idx_pde_event_ts',
              source_index_columns: ['event_ts'],
              source_index_partial: false,
              source_query_plan: [
                'SEARCH src.paper_decision_events USING COVERING INDEX idx_pde_event_ts (event_ts>? AND event_ts<?)',
              ],
              source_query_plan_uses_index: true,
              source_query_plan_uses_range_search: true,
              source_query_plan_full_table_scan_detected: false,
              rows_copied: 1,
              parallel_stage: {
                ...stageSchemaEvidence(10),
                role: 'paper_decision_events_parallel_stage',
                full_fidelity_row_copy: true,
                payload_semantics_preserved: true,
                stage_rows_copied: 1,
                rows_merged: 1,
                merge_duration_sec: 0.25,
                source_read_lock_duration_sec: 2.5,
                row_count_matched: true,
                quick_check: ['ok'],
                stage_page_size: 4096,
                stage_size_bytes: 8192,
                stage_budget_bytes: 20480,
                source_read_lock_budget_passed: true,
                merge_started_after_source_read_view_release: true,
              },
            },
            a_class_decision_events: {
              ...stageSchemaEvidence(8),
              included: true,
              predicate_strategy: 'indexed_epoch_seconds',
              indexed_time_anchor: 'event_ts',
              source_index_name: 'idx_a_class_decision_recent',
              source_index_columns: ['event_ts'],
              source_index_partial: false,
              source_query_plan: [
                'SEARCH src.a_class_decision_events USING COVERING INDEX idx_a_class_decision_recent (event_ts>? AND event_ts<?)',
              ],
              source_query_plan_uses_index: true,
              source_query_plan_uses_range_search: true,
              source_query_plan_full_table_scan_detected: false,
              rows_copied: 1,
              parallel_stage: {
                ...stageSchemaEvidence(8),
                role: 'a_class_decision_events_parallel_stage',
                full_fidelity_row_copy: true,
                payload_semantics_preserved: true,
                stage_rows_copied: 1,
                rows_merged: 1,
                merge_duration_sec: 0.20,
                source_read_lock_duration_sec: 2.75,
                row_count_matched: true,
                quick_check: ['ok'],
                stage_page_size: 4096,
                stage_size_bytes: 8192,
                stage_budget_bytes: 20480,
                source_read_lock_budget_passed: true,
                merge_started_after_source_read_view_release: true,
              },
            },
            opportunity_events: {
              ...stageSchemaEvidence(7),
              included: true,
              predicate_strategy: 'indexed_epoch_seconds',
              indexed_time_anchor: 'event_ts',
              source_index_name: 'idx_opportunity_events_recent',
              source_index_columns: ['event_ts'],
              source_index_partial: false,
              source_query_plan: [
                'SEARCH src.opportunity_events USING COVERING INDEX idx_opportunity_events_recent (event_ts>? AND event_ts<?)',
              ],
              source_query_plan_uses_index: true,
              source_query_plan_uses_range_search: true,
              source_query_plan_full_table_scan_detected: false,
              rows_copied: 1,
              parallel_stage: {
                ...stageSchemaEvidence(7),
                role: 'opportunity_events_parallel_stage',
                full_fidelity_row_copy: true,
                payload_semantics_preserved: true,
                stage_rows_copied: 1,
                rows_merged: 1,
                merge_duration_sec: 0.15,
                source_read_lock_duration_sec: 2.25,
                row_count_matched: true,
                quick_check: ['ok'],
                stage_page_size: 4096,
                stage_size_bytes: 8192,
                stage_budget_bytes: 20480,
                source_read_lock_budget_passed: true,
                merge_started_after_source_read_view_release: true,
              },
            },
            opportunity_event_path_samples: {
              ...stageSchemaEvidence(6),
              included: true,
              selection_mode: 'recent',
              time_semantics: 'event_time',
              future_bound_enforced: true,
              time_columns: ['sample_ts', 'created_at', 'updated_at'],
              upper_bound_columns: ['sample_ts', 'created_at', 'updated_at'],
              rows_copied: 1,
              parallel_stage: {
                ...stageSchemaEvidence(6),
                role: 'opportunity_event_path_samples_parallel_stage',
                full_fidelity_row_copy: true,
                payload_semantics_preserved: true,
                stage_rows_copied: 1,
                rows_merged: 1,
                merge_duration_sec: 0.30,
                source_read_lock_duration_sec: 2.9,
                row_count_matched: true,
                quick_check: ['ok'],
                stage_page_size: 4096,
                stage_size_bytes: 8192,
                stage_budget_bytes: 16384,
                source_read_lock_budget_passed: true,
                merge_started_after_source_read_view_release: true,
              },
            },
          },
        },
        raw: databaseReport('raw'),
        kline: databaseReport('kline'),
      },
      promotion_allowed: false,
    },
    databaseArtifacts: {
      signal: { available: true, size_bytes: 100 },
      paper: { available: true, size_bytes: 100 },
      raw: { available: true, size_bytes: 100 },
      kline: { available: true, size_bytes: 100 },
    },
    authoritativePreflight: {
      schema_version: 'evaluator_snapshot_authoritative_preflight_state.v1',
      checked_at: '2026-08-08T03:59:30.000Z',
      accepted: true,
      snapshot_id: snapshotId,
      manifest_sha256: 'd'.repeat(64),
      producer_manifest_sha256: 'd'.repeat(64),
      blockers: [],
      promotion_allowed: false,
    },
  };
}

test('every current evaluator manifest numeric leaf is type guarded', () => {
  const fixture = evaluatorSnapshotHealthFixture(
    Date.parse('2026-08-08T04:00:00.000Z'),
  );
  const manifest = structuredClone(fixture.manifestPayload);
  assert.equal(jsonNumericEvidenceTypesValid(manifest), true);
  const numericSlots = [];
  const collect = (value) => {
    if (Array.isArray(value)) {
      value.forEach((child, index) => {
        if (typeof child === 'number') numericSlots.push([value, index, child]);
        else if (child && typeof child === 'object') collect(child);
      });
      return;
    }
    if (!value || typeof value !== 'object') return;
    for (const [field, child] of Object.entries(value)) {
      if (typeof child === 'number') numericSlots.push([value, field, child]);
      else if (child && typeof child === 'object') collect(child);
    }
  };
  collect(manifest);
  assert.ok(numericSlots.length >= 150);
  for (const [container, field, original] of numericSlots) {
    container[field] = 'numeric-type-tamper';
    assert.equal(
      jsonNumericEvidenceTypesValid(manifest),
      false,
      String(field),
    );
    container[field] = original;
  }
  assert.equal(jsonNumericEvidenceTypesValid(manifest), true);
});

test('every dashboard numeric leaf obeys declarative type null and range rules', () => {
  const fixture = evaluatorSnapshotHealthFixture(
    Date.parse('2026-08-08T04:00:00.000Z'),
  );
  const manifest = structuredClone(fixture.manifestPayload);
  const numericLeaves = [];
  const collect = (value, path = '') => {
    if (Array.isArray(value)) {
      for (const child of value) {
        const childPath = `${path}[]`;
        if (typeof child === 'number') numericLeaves.push([childPath, child]);
        else if (child && typeof child === 'object') collect(child, childPath);
      }
      return;
    }
    if (!value || typeof value !== 'object') return;
    for (const [field, child] of Object.entries(value)) {
      const childPath = path ? `${path}.${field}` : field;
      if (typeof child === 'number') numericLeaves.push([childPath, child]);
      else if (child && typeof child === 'object') collect(child, childPath);
    }
  };
  collect(manifest);
  assert.ok(numericLeaves.length >= 150);
  for (const [path, original] of numericLeaves) {
    const baseline = validateNumericEvidenceValue(manifest, path, original);
    assert.equal(baseline.accepted, true, `${path}:${JSON.stringify(baseline)}`);
    let ruleMatch = numericEvidenceRule(path);
    let prefix = '';
    if (!ruleMatch) {
      const parent = path.endsWith('[]')
        ? path.slice(0, -2)
        : path.slice(0, path.lastIndexOf('.'));
      ruleMatch = numericEvidenceRule(parent);
      prefix = 'element_';
    }
    assert.ok(ruleMatch, path);
    const rule = ruleMatch[1];
    const kind = rule[`${prefix}kind`];
    const invalidValues = [
      false,
      {},
      [],
      Number.NaN,
      Number.POSITIVE_INFINITY,
    ];
    if (kind === 'safe_integer_or_decimal_identifier') {
      assert.equal(validateNumericEvidenceValue(manifest, path, '123').accepted, true, path);
      invalidValues.push('01', '+1', '1.0');
    } else {
      invalidValues.push('123');
    }
    for (const invalid of invalidValues) {
      const result = validateNumericEvidenceValue(manifest, path, invalid);
      assert.equal(result.accepted, false, `${path}:${String(invalid)}`);
    }
    if (kind.startsWith('safe_integer')) {
      assert.equal(validateNumericEvidenceValue(manifest, path, 0.5).accepted, false, path);
      assert.equal(
        validateNumericEvidenceValue(
          manifest,
          path,
          Number.MAX_SAFE_INTEGER + 1,
        ).accepted,
        false,
        path,
      );
    }
    const minimum = rule[`${prefix}minimum`];
    if (minimum != null) {
      assert.equal(
        validateNumericEvidenceValue(manifest, path, minimum - 1).accepted,
        false,
        path,
      );
    }
    const maximum = rule[`${prefix}maximum`];
    if (maximum != null) {
      assert.equal(
        validateNumericEvidenceValue(manifest, path, maximum + 1).accepted,
        false,
        path,
      );
    }
    const nullResult = validateNumericEvidenceValue(manifest, path, null);
    if (rule[`${prefix}nullable`] === false) {
      assert.equal(nullResult.accepted, false, path);
    } else if (rule.null_policy == null) {
      assert.equal(nullResult.accepted, true, path);
    }
  }
});

test('declarative evidence schema rejects all dynamic watermark tamper shapes', () => {
  const timestamp = 1_786_766_144;
  const payload = {
    numeric_evidence_schema_version: 'evaluator_snapshot_numeric_evidence.v3',
    numeric_evidence_schema_sha256: JSON_NUMERIC_EVIDENCE_CONTRACT_SHA256,
    numeric_evidence_schema_validated_before_publish: true,
    databases: {
      paper: {
        selected_tables: {
          a_class_decision_events: {
            time_column: 'event_ts',
            rows_copied: 1,
          },
        },
        source_upper_watermarks: {
          a_class_decision_events: { event_ts: timestamp },
        },
        upper_watermarks: {
          a_class_decision_events: { id: 1, event_ts: timestamp },
        },
      },
    },
  };
  const baseline = validateNumericEvidenceSchema(payload, { requireBinding: true });
  assert.equal(baseline.accepted, true, JSON.stringify(baseline.errors));
  assert.equal(baseline.numeric_leaf_count, 4);
  assert.equal(baseline.declared_numeric_leaf_count, 4);

  for (const container of [
    payload.databases.paper.source_upper_watermarks.a_class_decision_events,
    payload.databases.paper.upper_watermarks.a_class_decision_events,
  ]) {
    const original = container.event_ts;
    container.event_ts = '2026-08-08T03:59:00.123456Z';
    assert.equal(validateNumericEvidenceSchema(payload).accepted, true);
    container.event_ts = '2026-08-08 03:59:00.123456';
    assert.equal(validateNumericEvidenceSchema(payload).accepted, true);
    container.event_ts = timestamp + 0.125;
    assert.equal(validateNumericEvidenceSchema(payload).accepted, true);
    container.event_ts = original;
  }
  for (const invalidTimestamp of [
    '2026-02-31T00:00:00Z',
    '2026-08-08T25:00:00Z',
    '2026-08-08T03:59:00',
    '2026-02-31 00:00:00',
    '2026-08-08 25:00:00',
    '1969-12-31T23:59:59Z',
  ]) {
    const container = payload.databases.paper.upper_watermarks
      .a_class_decision_events;
    const original = container.event_ts;
    container.event_ts = invalidTimestamp;
    assert.equal(validateNumericEvidenceSchema(payload).accepted, false);
    container.event_ts = original;
  }

  const slots = [
    payload.databases.paper.source_upper_watermarks.a_class_decision_events,
    payload.databases.paper.upper_watermarks.a_class_decision_events,
  ];
  for (const container of slots) {
    const original = container.event_ts;
    for (const invalid of [
      '1700000000',
      false,
      {},
      [],
      -1,
      0.5,
      Number.MAX_SAFE_INTEGER + 1,
      Number.NaN,
      Number.POSITIVE_INFINITY,
    ]) {
      container.event_ts = invalid;
      const report = validateNumericEvidenceSchema(payload);
      assert.equal(report.accepted, false, String(invalid));
      container.event_ts = original;
    }
  }

  payload.databases.paper.source_upper_watermarks
    .a_class_decision_events.event_ts = null;
  payload.databases.paper.upper_watermarks
    .a_class_decision_events.event_ts = null;
  assert.equal(validateNumericEvidenceSchema(payload).accepted, false);
  payload.databases.paper.selected_tables.a_class_decision_events.rows_copied = 0;
  assert.equal(validateNumericEvidenceSchema(payload).accepted, true);

  payload.undeclared_attacker_numeric_evidence = 1;
  const unknown = validateNumericEvidenceSchema(payload);
  assert.equal(unknown.accepted, false);
  assert.ok(unknown.errors.some(
    (error) => error.code === 'undeclared_numeric_evidence',
  ));
  delete payload.undeclared_attacker_numeric_evidence;
  payload.undeclared_attacker_count = 1;
  const suffixSpoof = validateNumericEvidenceSchema(payload);
  assert.equal(suffixSpoof.accepted, false);
  assert.ok(suffixSpoof.errors.some(
    (error) => error.path === 'undeclared_attacker_count'
      && error.code === 'undeclared_numeric_evidence',
  ));
});

test('indexed-count-timeout advisory numerics are strict across both manifest copies', () => {
  const targetNames = [
    'candidate_shadow_observations',
    'paper_decision_events',
    'a_class_decision_events',
    'opportunity_events',
  ];
  const targets = () => Object.fromEntries(targetNames.map((target) => [
    target,
    {
      advisory_evidence: {
        selected_row_count: null,
        sample_row_count_advisory_basis: 256,
      },
    },
  ]));
  const payload = {
    disk_preflight: { shared_stage_budget: { targets: targets() } },
    shared_stage_budget: { targets: targets() },
  };
  const report = validateNumericEvidenceSchema(payload);
  assert.equal(report.accepted, true, JSON.stringify(report.errors));
  assert.equal(report.numeric_leaf_count, 8);
  assert.equal(report.declared_numeric_leaf_count, 8);

  const selectedRule = numericEvidenceRule(
    'disk_preflight.shared_stage_budget.targets.candidate_shadow_observations'
      + '.advisory_evidence.selected_row_count',
    'selected_row_count',
  );
  const sampleRule = numericEvidenceRule(
    'disk_preflight.shared_stage_budget.targets.candidate_shadow_observations'
      + '.advisory_evidence.sample_row_count_advisory_basis',
    'sample_row_count_advisory_basis',
  );
  assert.equal(selectedRule[1].id, 'nullable_shared_advisory_row_count_fields');
  assert.equal(selectedRule[1].nullable, true);
  assert.equal(sampleRule[1].id, 'nullable_shared_advisory_row_count_fields');
  assert.equal(sampleRule[1].nullable, true);

  const parentTargets = [
    [
      'disk_preflight.shared_stage_budget.targets',
      payload.disk_preflight.shared_stage_budget.targets,
    ],
    ['shared_stage_budget.targets', payload.shared_stage_budget.targets],
  ];
  const invalidSelected = [-1, 0.5, '256', false, null, {}, [], Number.MAX_SAFE_INTEGER + 1];
  const invalidSample = [-1, 0.5, '256', false, null, {}, [], Number.MAX_SAFE_INTEGER + 1];
  for (const [parentPath, targetMap] of parentTargets) {
    for (const [target, targetPayload] of Object.entries(targetMap)) {
      const evidence = targetPayload.advisory_evidence;
      const selectedPath = `${parentPath}.${target}.advisory_evidence.selected_row_count`;
      const samplePath = (
        `${parentPath}.${target}.advisory_evidence.sample_row_count_advisory_basis`
      );
      for (const invalid of invalidSelected) {
        evidence.selected_row_count = invalid;
        const selectedReport = validateNumericEvidenceSchema(payload);
        if (invalid === null) {
          assert.equal(selectedReport.accepted, true, selectedPath);
        } else {
          assert.equal(
            selectedReport.accepted,
            false,
            `${selectedPath}:${String(invalid)}`,
          );
        }
      }
      evidence.selected_row_count = null;
      for (const invalid of invalidSample) {
        evidence.sample_row_count_advisory_basis = invalid;
        const report = validateNumericEvidenceSchema(payload);
        if (invalid === null) {
          assert.equal(report.accepted, true, samplePath);
        } else {
          assert.equal(report.accepted, false, `${samplePath}:${String(invalid)}`);
        }
      }
      evidence.sample_row_count_advisory_basis = 256;
    }
  }
  assert.equal(validateNumericEvidenceSchema(payload).accepted, true);
});

test('every field selector requires an allowed parent path', () => {
  const payload = {
    numeric_evidence_schema_version: 'evaluator_snapshot_numeric_evidence.v3',
    numeric_evidence_schema_sha256: JSON_NUMERIC_EVIDENCE_CONTRACT_SHA256,
    numeric_evidence_schema_validated_before_publish: true,
    attacker: {
      rows_copied: 1,
      output_size_bytes: 1,
      duration_sec: 1,
    },
  };
  const fieldRules = [
    ...EVIDENCE_SCHEMA.container_rules,
    ...EVIDENCE_SCHEMA.scalar_rules,
  ].filter((rule) => Array.isArray(rule.fields));
  assert.ok(fieldRules.length >= 10);

  const attackPaths = new Set();
  for (const field of ['rows_copied', 'output_size_bytes', 'duration_sec']) {
    attackPaths.add(`attacker.${field}`);
  }
  const wrongPathValues = [1, 0.5, '1', false, null, {}, []];
  wrongPathValues.forEach((attackValue, index) => {
    payload.attacker[`shape_${index}`] = {};
    for (const rule of fieldRules) {
      payload.attacker[`shape_${index}`][rule.id] = {};
      for (const field of rule.fields) {
        payload.attacker[`shape_${index}`][rule.id][field] = attackValue;
        const attackPath = `attacker.shape_${index}.${rule.id}.${field}`;
        attackPaths.add(attackPath);
        assert.equal(numericEvidenceRule(attackPath, field), null);
        assert.equal(
          validateNumericEvidenceValue(payload, attackPath, attackValue).accepted,
          false,
        );
      }
    }
  });
  for (const rule of fieldRules) {
    assert.ok(Array.isArray(rule.parent_path_patterns));
    assert.ok(rule.parent_path_patterns.length > 0);
    assert.equal(rule.parent_path_patterns.includes('*'), false);
    payload.attacker[rule.id] = {};
    for (const field of rule.fields) {
      payload.attacker[rule.id][field] = 1;
      const rootAttack = `attacker.${rule.id}.${field}`;
      attackPaths.add(rootAttack);
      assert.equal(numericEvidenceRule(rootAttack, field), null);
      assert.equal(
        numericEvidenceRule(`databases.paper.attacker.${rule.id}.${field}`, field),
        null,
      );
      assert.equal(
        numericEvidenceRule(`shared_stage_budget.attacker.${rule.id}.${field}`, field),
        null,
      );
    }
  }

  const report = validateNumericEvidenceSchema(payload, { maxErrors: 4096 });
  assert.equal(report.accepted, false);
  const observed = new Set(
    report.errors
      .filter((error) => [
        'undeclared_numeric_evidence',
        'declared_numeric_field_parent_path_mismatch',
      ].includes(error.code))
      .map((error) => error.path),
  );
  for (const path of attackPaths) assert.equal(observed.has(path), true, path);
  assert.equal(
    numericEvidenceRule('attacker.rows_copied', 'output_size_bytes'),
    null,
  );

  delete payload.attacker;
  payload.databases = {};
  payload.databases.paper = {};
  payload.selection_contract = {};
  payload.shared_stage_budget = {};
  payload.disk_preflight = {};
  payload.database_budget_plan = {};
  const wrongLocationCases = [
    [payload.databases.paper, 'databases.paper', 'rows_copied'],
    [payload.databases.paper, 'databases.paper', 'output_size_bytes'],
    [payload.selection_contract, 'selection_contract', 'output_size_bytes'],
    [payload, '', 'duration_sec'],
    [payload.shared_stage_budget, 'shared_stage_budget', 'duration_sec'],
    [payload.disk_preflight, 'disk_preflight', 'rows_copied'],
    [payload.database_budget_plan, 'database_budget_plan', 'stage_size_bytes'],
    [payload.shared_stage_budget, 'shared_stage_budget', 'page_count'],
    [payload.disk_preflight, 'disk_preflight', 'utilization_ratio'],
  ];
  for (const [container, parentPath, field] of wrongLocationCases) {
    const path = parentPath ? `${parentPath}.${field}` : field;
    assert.equal(numericEvidenceRule(path, field), null, path);
    container[field] = 1;
    const locationReport = validateNumericEvidenceSchema(payload);
    assert.equal(locationReport.accepted, false, path);
    assert.equal(
      locationReport.errors.some(
        (error) => error.path === path
          && error.code === 'declared_numeric_field_parent_path_mismatch',
      ),
      true,
      path,
    );
    delete container[field];
  }
});

test('declarative evidence schema preserves strict production scalar variants', () => {
  assert.equal(isEvidenceTimestamp('2026-08-08T03:59:00.123456Z'), true);
  assert.equal(isEvidenceTimestamp('2026-08-08 03:59:00.123456'), true);
  assert.equal(isEvidenceTimestamp('2026-02-31 00:00:00'), false);
  for (const value of ['0', '1', '47959', String(Number.MAX_SAFE_INTEGER)]) {
    assert.equal(isDecimalIdentifier(value), true, value);
  }
  for (const value of ['', '01', '+1', '-1', '1.0', String(Number.MAX_SAFE_INTEGER + 1)]) {
    assert.equal(isDecimalIdentifier(value), false, value);
  }

  const payload = {
    numeric_evidence_schema_version: 'evaluator_snapshot_numeric_evidence.v3',
    numeric_evidence_schema_sha256: JSON_NUMERIC_EVIDENCE_CONTRACT_SHA256,
    numeric_evidence_schema_validated_before_publish: true,
    databases: {
      paper: {
        selected_tables: {
          paper_decision_events: {
            rows_copied: 1,
            time_column: 'event_ts',
            time_columns: ['event_ts', 'created_at'],
          },
        },
        source_upper_watermarks: {
          paper_decision_events: { event_ts: 1_786_766_144.125 },
        },
        upper_watermarks: {
          paper_decision_events: {
            id: 1,
            event_ts: 1_786_766_144.125,
            created_at: '2026-08-08 03:59:00',
          },
        },
      },
      raw: {
        selected_tables: {
          raw_signal_outcomes: { rows_copied: 1, time_column: 'updated_at' },
        },
        source_upper_watermarks: { raw_signal_outcomes: {} },
        upper_watermarks: {
          raw_signal_outcomes: { id: 1, signal_id: '47959', updated_at: 1_786_766_144 },
        },
      },
    },
  };
  assert.equal(
    validateNumericEvidenceSchema(payload, { requireBinding: true }).accepted,
    true,
  );
  payload.databases.paper.upper_watermarks.paper_decision_events.event_ts = 0.5;
  assert.equal(validateNumericEvidenceSchema(payload).accepted, false);
  payload.databases.paper.upper_watermarks.paper_decision_events.event_ts = 1_786_766_144.125;
  payload.databases.raw.upper_watermarks.raw_signal_outcomes.signal_id = '047959';
  assert.equal(validateNumericEvidenceSchema(payload).accepted, false);
});

test('dashboard rejects coherent watermark and local schema-binding spoofing', () => {
  const nowMs = Date.parse('2026-08-08T04:00:00.000Z');
  const fixture = evaluatorSnapshotHealthFixture(nowMs);
  const base = {
    nowMs,
    enabled: true,
    pid: 33333,
    pidAlive: true,
    lockPid: 33333,
    lockPidAlive: true,
    statusPayload: fixture.statusPayload,
    manifestPayload: fixture.manifestPayload,
    statusArtifact: { available: true, mtime: '2026-08-08T03:59:30.000Z', size_bytes: 100 },
    manifestArtifact: { available: true, mtime: '2026-08-08T03:59:30.000Z', size_bytes: 1000 },
    manifestFileSha256: 'd'.repeat(64),
    databaseArtifacts: fixture.databaseArtifacts,
    authoritativePreflight: fixture.authoritativePreflight,
  };

  for (const invalid of ['1700000000', 0.5, null]) {
    const manifest = structuredClone(fixture.manifestPayload);
    manifest.databases.paper.source_upper_watermarks
      .a_class_decision_events.event_ts = invalid;
    manifest.databases.paper.upper_watermarks
      .a_class_decision_events.event_ts = invalid;
    const health = readEvaluatorSnapshotWorkerHealth({ ...base, manifestPayload: manifest });
    assert.equal(health.status, 'contract_blocked', String(invalid));
    assert.equal(health.consumer_ready, false, String(invalid));
    assert.equal(
      health.manifest_contract.numeric_evidence_types_passed,
      false,
      String(invalid),
    );
  }

  const wrongPathManifest = structuredClone(fixture.manifestPayload);
  wrongPathManifest.attacker = {
    rows_copied: 1,
    output_size_bytes: 1,
    duration_sec: 1,
    rules: Object.fromEntries(
      [...EVIDENCE_SCHEMA.container_rules, ...EVIDENCE_SCHEMA.scalar_rules]
        .filter((rule) => Array.isArray(rule.fields))
        .map((rule) => [
          rule.id,
          Object.fromEntries(rule.fields.map((field) => [field, 1])),
        ]),
    ),
  };
  const wrongPath = readEvaluatorSnapshotWorkerHealth({
    ...base,
    manifestPayload: wrongPathManifest,
  });
  assert.equal(wrongPath.status, 'contract_blocked');
  assert.equal(wrongPath.consumer_ready, false);
  assert.equal(
    wrongPath.manifest_contract.numeric_evidence_types_passed,
    false,
  );

  const spoofedManifest = structuredClone(fixture.manifestPayload);
  const spoofedStatus = structuredClone(fixture.statusPayload);
  spoofedManifest.numeric_evidence_schema_sha256 = 'f'.repeat(64);
  spoofedStatus.last_accepted_snapshot.numeric_evidence_schema_sha256 = (
    'f'.repeat(64)
  );
  const spoofed = readEvaluatorSnapshotWorkerHealth({
    ...base,
    manifestPayload: spoofedManifest,
    statusPayload: spoofedStatus,
  });
  assert.equal(spoofed.status, 'contract_blocked');
  assert.equal(
    spoofed.manifest_contract.numeric_evidence_schema_binding_passed,
    false,
  );
});

test('evaluator snapshot worker health accepts only fresh matching indexed bundles', () => {
  const nowMs = Date.parse('2026-08-08T04:00:00.000Z');
  const fixture = evaluatorSnapshotHealthFixture(nowMs);
  const health = readEvaluatorSnapshotWorkerHealth({
    nowMs,
    enabled: true,
    pid: 33333,
    pidAlive: true,
    lockPid: 33333,
    lockPidAlive: true,
    statusPayload: fixture.statusPayload,
    manifestPayload: fixture.manifestPayload,
    statusArtifact: { available: true, mtime: '2026-08-08T03:59:30.000Z', size_bytes: 100 },
    manifestArtifact: { available: true, mtime: '2026-08-08T03:59:30.000Z', size_bytes: 1000 },
    manifestFileSha256: 'd'.repeat(64),
    databaseArtifacts: fixture.databaseArtifacts,
    authoritativePreflight: fixture.authoritativePreflight,
  });

  assert.equal(
    health.status,
    'producer_accepted',
    JSON.stringify({
      blockers: health.blockers,
      manifest_contract: health.manifest_contract,
      shared_stage_budget: health.shared_stage_budget,
    }, null, 2),
  );
  assert.equal(health.healthy, true);
  assert.equal(health.degraded, false);
  assert.equal(health.consumer_ready, true);
  assert.deepEqual(health.numeric_evidence_schema, {
    version: 'evaluator_snapshot_numeric_evidence.v3',
    sha256: JSON_NUMERIC_EVIDENCE_CONTRACT_SHA256,
    manifest_binding_valid: true,
    producer_binding_valid: true,
    binding_passed: true,
  });
  assert.equal(health.bundle_candidate_available, true);
  assert.equal(health.consumer_state, 'authoritative_preflight_current');
  assert.equal(health.snapshot_identity_matched, true);
  assert.equal(health.snapshot_fresh, true);
  assert.equal(health.manifest_contract.indexed_selection_passed, true);
  assert.equal(health.manifest_contract.indexed_watermarks_passed, true);
  assert.equal(health.manifest_contract.pinned_read_view_lineage_passed, true);
  assert.equal(health.pinned_read_view_lineage.passed, true);
  assert.equal(health.pinned_read_view_lineage.actual_count, 8);
  assert.equal(health.pinned_read_view_lineage.recomputed_skew_sec, 0.40);
  assert.equal(health.indexed_selection.candidate_shadow_observations.rows_copied, 1);
  assert.equal(health.indexed_watermarks.candidate_shadow_observations.passed, true);
  assert.equal(
    health.indexed_watermarks.candidate_shadow_observations.source_index_name,
    'idx_candidate_shadow_obs_observed',
  );
  assert.equal(health.source_read_lock.max_duration_sec, 3.33);
  assert.equal(health.consecutive_failure_count, 0);
  assert.equal(health.next_attempt_delay_sec, 21600);
  assert.equal(health.snapshot_files.paper.size_matches_manifest, true);
  assert.equal(health.manifest_contract.shared_stage_budget_passed, true);
  assert.equal(health.shared_stage_budget.available, true);
  assert.equal(health.shared_stage_budget.contract_passed, true);
  assert.equal(
    health.shared_stage_budget.hash_canonicalization,
    'json_sorted_float64_bits.v1',
  );
  assert.equal(health.shared_stage_budget.plan_sha256_matched, true);
  assert.equal(health.shared_stage_budget.evidence_sha256_matched, true);
  assert.equal(health.shared_stage_budget.fixed_percentage_allocation_used, false);
  assert.equal(health.shared_stage_budget.total_cap_bytes, 102400);
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events.granted_cap_bytes,
    20480,
  );
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events.actual_usage_bytes,
    8192,
  );
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events
      .advisory_evidence_passed,
    true,
  );
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events
      .advisory_schema_version,
    'sqlite_dbstat_advisory_demand.v1',
  );
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events
      .capacity_sample_used,
    false,
  );
  assert.equal(health.manifest_contract.parallel_paper_stages_passed, true);
  assert.equal(health.parallel_paper_stages.passed, true);
  assert.equal(health.parallel_paper_stages.stage_count, 4);
  assert.equal(health.parallel_paper_stages.pinned_read_view_count, 5);
  assert.equal(
    health.parallel_paper_stages.stages.opportunity_event_path_samples.rows_copied,
    1,
  );
  assert.equal(
    health.parallel_paper_stages.stages.opportunity_event_path_samples.rows_merged,
    1,
  );
  assert.equal(
    health.parallel_paper_stages.stages.opportunity_event_path_samples.passed,
    true,
  );
  assert.equal(health.parallel_paper_stages.stages.a_class_decision_events.passed, true);
  assert.equal(health.parallel_paper_stages.stages.opportunity_events.passed, true);
  assert.equal(health.manifest_contract.paper_decision_parallel_stage_passed, true);
  assert.equal(health.parallel_paper_decision_stage.passed, true);
  assert.equal(health.parallel_paper_decision_stage.rows_copied, 1);
  assert.equal(health.parallel_paper_decision_stage.rows_merged, 1);
  assert.equal(health.parallel_paper_decision_stage.stage_page_size, 4096);
  assert.equal(health.parallel_paper_decision_stage.pinned_read_view_count, 5);
  assert.equal(health.parallel_paper_decision_stage.stage_removed_before_publish, true);
  assert.equal(health.authoritative_consumer_preflight.matched_current_bundle, true);
  assert.equal(health.promotion_allowed, false);

  const producerOnly = readEvaluatorSnapshotWorkerHealth({
    nowMs,
    enabled: true,
    pid: 33333,
    pidAlive: true,
    lockPid: 33333,
    lockPidAlive: true,
    statusPayload: fixture.statusPayload,
    manifestPayload: fixture.manifestPayload,
    statusArtifact: { available: true, mtime: '2026-08-08T03:59:30.000Z', size_bytes: 100 },
    manifestArtifact: { available: true, mtime: '2026-08-08T03:59:30.000Z', size_bytes: 1000 },
    manifestFileSha256: 'd'.repeat(64),
    databaseArtifacts: fixture.databaseArtifacts,
    authoritativePreflight: null,
  });
  assert.equal(producerOnly.status, 'producer_accepted');
  assert.equal(producerOnly.bundle_candidate_available, true);
  assert.equal(producerOnly.consumer_ready, false);
  assert.equal(producerOnly.consumer_state, 'authoritative_preflight_required');
});

test('evaluator snapshot worker rejects shared-stage storage lineage tampering', () => {
  const nowMs = Date.parse('2026-08-08T04:00:00.000Z');
  for (const [field, value] of [
    ['storage_schema_version', 'parallel_paper_event_stage.v2'],
    ['history_storage_compatible', true],
    ['history_storage_schema_version', 'parallel_paper_event_stage.v4'],
  ]) {
    const fixture = evaluatorSnapshotHealthFixture(nowMs);
    const manifestPayload = structuredClone(fixture.manifestPayload);
    const shared = manifestPayload.shared_stage_budget;
    shared.targets.paper_decision_events[field] = value;
    shared.plan_sha256 = sharedStageBudgetPlanSha256(shared);
    shared.evidence_sha256 = sharedStageBudgetEvidenceSha256(shared);
    manifestPayload.disk_preflight.shared_stage_budget = structuredClone(shared);
    const statusPayload = structuredClone(fixture.statusPayload);
    statusPayload.shared_stage_budget = structuredClone(shared);

    const health = readEvaluatorSnapshotWorkerHealth({
      nowMs,
      enabled: true,
      pid: 33333,
      pidAlive: true,
      lockPid: 33333,
      lockPidAlive: true,
      statusPayload,
      manifestPayload,
      statusArtifact: {
        available: true,
        mtime: '2026-08-08T03:59:30.000Z',
        size_bytes: 100,
      },
      manifestArtifact: {
        available: true,
        mtime: '2026-08-08T03:59:30.000Z',
        size_bytes: 1000,
      },
      manifestFileSha256: 'd'.repeat(64),
      databaseArtifacts: fixture.databaseArtifacts,
      authoritativePreflight: fixture.authoritativePreflight,
    });

    assert.equal(health.status, 'contract_blocked', field);
    assert.equal(health.shared_stage_budget.contract_passed, false, field);
  }
});

test('evaluator snapshot worker health validates bounded sample advisory fallback', () => {
  const nowMs = Date.parse('2026-08-08T04:00:00.000Z');
  const fixture = evaluatorSnapshotHealthFixture(nowMs);
  const manifestPayload = structuredClone(fixture.manifestPayload);
  const shared = manifestPayload.shared_stage_budget;
  const p9 = shared.targets.paper_decision_events;
  p9.advisory_strategy = 'bounded_index_sample_advisory_fallback';
  Object.assign(p9.advisory_evidence, {
    advisory_schema_version: 'bounded_index_sample_advisory_demand.v1',
    advisory_formula:
      'selected_rows_times_bounded_sample_max_plus_per_row_overhead_'
      + 'plus_root_reserve_plus_candidate_signal_index_overhead',
    capacity_sample_used: true,
    dbstat_completed: false,
    dbstat_timed_out: true,
    dbstat_timeout_sec: 20,
    dbstat_elapsed_sec: 20.5,
    source_row_count_upper: null,
    source_row_count_upper_basis:
      'not_required_for_bounded_index_sample_advisory',
    sample_rows: 1,
    average_row_bytes_diagnostic: 4096,
    sample_max_row_bytes_diagnostic: 4096,
    sample_row_bytes_basis: 4096,
    source_dbstat_page_count: null,
    source_dbstat_page_size: null,
    source_dbstat_physical_bytes: null,
    source_dbstat_payload_bytes: null,
    source_dbstat_unused_bytes: null,
    source_dbstat_max_payload_bytes: null,
    source_dbstat_cell_upper_count: null,
    source_row_fraction_numerator: null,
    source_row_fraction_denominator: null,
    table_sample_payload_advisory_bytes: 4096,
    table_scaled_physical_advisory_bytes: 0,
    table_row_overhead_advisory_bytes: 32,
    table_root_reserve_advisory_bytes: 8192,
    table_advisory_bytes: 16384,
    candidate_order_index_scaled_physical_advisory_bytes: 0,
    candidate_order_index_row_overhead_advisory_bytes: 0,
    candidate_order_index_advisory_bytes: 0,
  });
  shared.plan_sha256 = sharedStageBudgetPlanSha256(shared);
  shared.evidence_sha256 = sharedStageBudgetEvidenceSha256(shared);
  manifestPayload.disk_preflight.shared_stage_budget = structuredClone(shared);
  const statusPayload = structuredClone(fixture.statusPayload);
  statusPayload.shared_stage_budget = structuredClone(shared);
  const healthInput = {
    nowMs,
    enabled: true,
    pid: 33333,
    pidAlive: true,
    lockPid: 33333,
    lockPidAlive: true,
    statusPayload,
    manifestPayload,
    statusArtifact: {
      available: true,
      mtime: '2026-08-08T03:59:30.000Z',
      size_bytes: 100,
    },
    manifestArtifact: {
      available: true,
      mtime: '2026-08-08T03:59:30.000Z',
      size_bytes: 1000,
    },
    manifestFileSha256: 'd'.repeat(64),
    databaseArtifacts: fixture.databaseArtifacts,
    authoritativePreflight: fixture.authoritativePreflight,
  };
  const health = readEvaluatorSnapshotWorkerHealth(healthInput);
  assert.equal(health.status, 'producer_accepted');
  assert.equal(health.shared_stage_budget.contract_passed, true);
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events
      .advisory_evidence_passed,
    true,
  );
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events.capacity_sample_used,
    true,
  );
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events.dbstat_timed_out,
    true,
  );
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events
      .sample_row_bytes_basis,
    4096,
  );

  const tamperedManifest = structuredClone(manifestPayload);
  const tamperedShared = tamperedManifest.shared_stage_budget;
  tamperedShared.targets.paper_decision_events.advisory_evidence
    .sample_row_bytes_basis += 1;
  tamperedShared.plan_sha256 = sharedStageBudgetPlanSha256(tamperedShared);
  tamperedShared.evidence_sha256 = sharedStageBudgetEvidenceSha256(tamperedShared);
  tamperedManifest.disk_preflight.shared_stage_budget = structuredClone(
    tamperedShared,
  );
  const tamperedStatus = structuredClone(statusPayload);
  tamperedStatus.shared_stage_budget = structuredClone(tamperedShared);
  const blocked = readEvaluatorSnapshotWorkerHealth({
    ...healthInput,
    manifestPayload: tamperedManifest,
    statusPayload: tamperedStatus,
  });
  assert.equal(blocked.status, 'contract_blocked');
  assert.equal(blocked.shared_stage_budget.contract_passed, false);
});

test('evaluator snapshot worker health validates indexed-count-timeout advisory fallback', () => {
  const nowMs = Date.parse('2026-08-08T04:00:00.000Z');
  const fixture = evaluatorSnapshotHealthFixture(nowMs);
  const manifestPayload = structuredClone(fixture.manifestPayload);
  const shared = manifestPayload.shared_stage_budget;
  const target = shared.targets.paper_decision_events;
  target.advisory_strategy = 'bounded_index_count_timeout_advisory_fallback';
  Object.assign(target.advisory_evidence, {
    advisory_schema_version: 'bounded_index_count_timeout_advisory_demand.v1',
    advisory_formula:
      'bounded_edge_sample_rows_times_sample_max_plus_per_row_overhead_'
      + 'plus_root_reserve_plus_candidate_signal_index_overhead',
    capacity_sample_used: true,
    indexed_count_completed: false,
    indexed_count_timed_out: true,
    indexed_count_timeout_sec: 20,
    indexed_count_elapsed_sec: 20.5,
    dbstat_completed: false,
    dbstat_timed_out: false,
    dbstat_timeout_sec: 20,
    dbstat_elapsed_sec: 0,
    dbstat_skipped_reason: 'indexed_count_timeout',
    row_count_binding_mode: 'copy_report_exact_after_indexed_count_timeout',
    selected_row_count: null,
    sample_row_count_advisory_basis: 1,
    source_row_count_upper: null,
    source_row_count_upper_basis:
      'unavailable_after_bounded_index_count_timeout',
    sample_rows: 1,
    average_row_bytes_diagnostic: 4096,
    sample_max_row_bytes_diagnostic: 4096,
    sample_row_bytes_basis: 4096,
    source_dbstat_page_count: null,
    source_dbstat_page_size: null,
    source_dbstat_physical_bytes: null,
    source_dbstat_payload_bytes: null,
    source_dbstat_unused_bytes: null,
    source_dbstat_max_payload_bytes: null,
    source_dbstat_cell_upper_count: null,
    source_row_fraction_numerator: null,
    source_row_fraction_denominator: null,
    table_sample_payload_advisory_bytes: 4096,
    table_scaled_physical_advisory_bytes: 0,
    table_row_overhead_advisory_bytes: 32,
    table_root_reserve_advisory_bytes: 8192,
    table_advisory_bytes: 16384,
    candidate_order_index_scaled_physical_advisory_bytes: 0,
    candidate_order_index_row_overhead_advisory_bytes: 0,
    candidate_order_index_advisory_bytes: 0,
  });
  shared.plan_sha256 = sharedStageBudgetPlanSha256(shared);
  shared.evidence_sha256 = sharedStageBudgetEvidenceSha256(shared);
  manifestPayload.disk_preflight.shared_stage_budget = structuredClone(shared);
  const statusPayload = structuredClone(fixture.statusPayload);
  statusPayload.shared_stage_budget = structuredClone(shared);
  const healthInput = {
    nowMs,
    enabled: true,
    pid: 33333,
    pidAlive: true,
    lockPid: 33333,
    lockPidAlive: true,
    statusPayload,
    manifestPayload,
    statusArtifact: {
      available: true,
      mtime: '2026-08-08T03:59:30.000Z',
      size_bytes: 100,
    },
    manifestArtifact: {
      available: true,
      mtime: '2026-08-08T03:59:30.000Z',
      size_bytes: 1000,
    },
    manifestFileSha256: 'd'.repeat(64),
    databaseArtifacts: fixture.databaseArtifacts,
    authoritativePreflight: fixture.authoritativePreflight,
  };
  const health = readEvaluatorSnapshotWorkerHealth(healthInput);
  assert.equal(health.status, 'producer_accepted');
  assert.equal(health.shared_stage_budget.contract_passed, true);
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events
      .advisory_evidence_passed,
    true,
  );

  for (const [field, value] of [
    ['selected_row_count', 1],
    ['selected_row_count', undefined],
    ['sample_row_count_advisory_basis', 2],
    ['sample_row_count_advisory_basis', undefined],
  ]) {
    const tamperedManifest = structuredClone(manifestPayload);
    const tamperedShared = tamperedManifest.shared_stage_budget;
    const tamperedEvidence = (
      tamperedShared.targets.paper_decision_events.advisory_evidence
    );
    if (value === undefined) {
      delete tamperedEvidence[field];
    } else {
      tamperedEvidence[field] = value;
    }
    tamperedShared.plan_sha256 = sharedStageBudgetPlanSha256(tamperedShared);
    tamperedShared.evidence_sha256 = sharedStageBudgetEvidenceSha256(
      tamperedShared,
    );
    tamperedManifest.disk_preflight.shared_stage_budget = structuredClone(
      tamperedShared,
    );
    const tamperedStatus = structuredClone(statusPayload);
    tamperedStatus.shared_stage_budget = structuredClone(tamperedShared);
    const blocked = readEvaluatorSnapshotWorkerHealth({
      ...healthInput,
      manifestPayload: tamperedManifest,
      statusPayload: tamperedStatus,
    });
    assert.equal(blocked.status, 'contract_blocked', field);
    assert.equal(blocked.shared_stage_budget.contract_passed, false, field);
  }
});

test('evaluator snapshot worker health accepts advisory miss within hard grant', () => {
  const nowMs = Date.parse('2026-08-08T04:00:00.000Z');
  const fixture = evaluatorSnapshotHealthFixture(nowMs);
  const manifestPayload = structuredClone(fixture.manifestPayload);
  const shared = manifestPayload.shared_stage_budget;
  const p9 = shared.targets.paper_decision_events;
  const previousActual = p9.actual_usage_bytes;
  const acceptedActual = p9.granted_cap_bytes;
  assert.ok(acceptedActual > p9.advisory_required_bytes);
  p9.actual_usage_bytes = acceptedActual;
  p9.high_water_bytes = acceptedActual;
  p9.advisory_exceeded = true;
  p9.advisory_delta_bytes = acceptedActual - p9.advisory_required_bytes;
  p9.utilization_ratio = 1;
  shared.actual_total_bytes += acceptedActual - previousActual;
  shared.unconsumed_bytes -= acceptedActual - previousActual;
  shared.targets_exceeding_advisory = ['paper_decision_events'];
  shared.advisory_miss_count = 1;
  shared.plan_sha256 = sharedStageBudgetPlanSha256(shared);
  shared.evidence_sha256 = sharedStageBudgetEvidenceSha256(shared);
  manifestPayload.disk_preflight.shared_stage_budget = structuredClone(shared);
  const paper = manifestPayload.databases.paper;
  paper.parallel_paper_stages.paper_decision_events.stage_size_bytes = acceptedActual;
  paper.selected_tables.paper_decision_events.parallel_stage.stage_size_bytes = acceptedActual;
  paper.paper_decision_parallel_stage_size_bytes = acceptedActual;
  const statusPayload = structuredClone(fixture.statusPayload);
  statusPayload.shared_stage_budget = structuredClone(shared);

  const health = readEvaluatorSnapshotWorkerHealth({
    nowMs,
    enabled: true,
    pid: 33333,
    pidAlive: true,
    lockPid: 33333,
    lockPidAlive: true,
    statusPayload,
    manifestPayload,
    statusArtifact: { available: true, mtime: '2026-08-08T03:59:30.000Z', size_bytes: 100 },
    manifestArtifact: { available: true, mtime: '2026-08-08T03:59:30.000Z', size_bytes: 1000 },
    manifestFileSha256: 'd'.repeat(64),
    databaseArtifacts: fixture.databaseArtifacts,
    authoritativePreflight: fixture.authoritativePreflight,
  });

  assert.equal(health.status, 'producer_accepted');
  assert.equal(health.shared_stage_budget.contract_passed, true);
  assert.equal(health.shared_stage_budget.advisory_miss_count, 1);
  assert.deepEqual(
    health.shared_stage_budget.targets_exceeding_advisory,
    ['paper_decision_events'],
  );
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events.advisory_exceeded,
    true,
  );
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events.actual_usage_bytes,
    acceptedActual,
  );
  assert.equal(
    health.shared_stage_budget.targets.paper_decision_events.granted_cap_bytes,
    acceptedActual,
  );
});

test('evaluator snapshot worker health accepts an explicitly absent optional path stage', () => {
  const nowMs = Date.parse('2026-08-08T04:00:00.000Z');
  const fixture = evaluatorSnapshotHealthFixture(nowMs);
  const manifestPayload = structuredClone(fixture.manifestPayload);
  const paperReport = manifestPayload.databases.paper;
  const activeTables = [
    'paper_decision_events',
    'a_class_decision_events',
    'opportunity_events',
  ];

  manifestPayload.parallel_paper_stage_tables = activeTables;
  manifestPayload.parallel_paper_stage_count = activeTables.length;
  manifestPayload.pinned_read_view_count = 7;
  manifestPayload.cross_database_time_skew_sec = 0.35;
  const disk = manifestPayload.disk_preflight;
  const shared = manifestPayload.shared_stage_budget;
  shared.active_targets = [
    'candidate_shadow_observations',
    ...activeTables,
  ];
  delete shared.targets.opportunity_event_path_samples;
  shared.minimum_total_bytes = 49152;
  shared.baseline_required_total_bytes = 49152;
  shared.advisory_demand_total_bytes = 81920;
  shared.residual_pool_bytes = 53248;
  shared.borrowing_priority_targets = [
    'candidate_shadow_observations',
    ...activeTables,
  ];
  shared.allocation_weight_total_bytes = 81920;
  shared.targets.candidate_shadow_observations.granted_cap_bytes = 32768;
  shared.targets.candidate_shadow_observations.borrowed_shared_pool_bytes = 20480;
  shared.targets.candidate_shadow_observations.advisory_shortfall_bytes = 0;
  shared.targets.candidate_shadow_observations.utilization_ratio = 16000 / 32768;
  shared.targets.paper_decision_events.granted_cap_bytes = 24576;
  shared.targets.paper_decision_events.borrowed_shared_pool_bytes = 12288;
  shared.targets.paper_decision_events.utilization_ratio = 8192 / 24576;
  shared.targets.a_class_decision_events.granted_cap_bytes = 24576;
  shared.targets.a_class_decision_events.borrowed_shared_pool_bytes = 12288;
  shared.targets.a_class_decision_events.utilization_ratio = 8192 / 24576;
  shared.targets.opportunity_events.granted_cap_bytes = 20480;
  shared.targets.opportunity_events.borrowed_shared_pool_bytes = 8192;
  shared.targets.opportunity_events.utilization_ratio = 8192 / 20480;
  shared.total_granted_bytes = 102400;
  shared.actual_total_bytes = 40576;
  shared.unconsumed_bytes = 61824;
  shared.plan_sha256 = sharedStageBudgetPlanSha256(shared);
  shared.evidence_sha256 = sharedStageBudgetEvidenceSha256(shared);
  disk.shared_stage_budget = structuredClone(shared);
  fixture.statusPayload.shared_stage_budget = structuredClone(shared);
  disk.temporary_candidate_stage_cap_bytes = 32768;
  disk.temporary_parallel_paper_stage_cap_bytes = {
    paper_decision_events: 24576,
    a_class_decision_events: 24576,
    opportunity_events: 20480,
  };
  disk.temporary_paper_decision_stage_cap_bytes = 24576;
  disk.parallel_paper_stage_tables = activeTables;
  disk.omitted_optional_parallel_paper_stage_tables = [
    'opportunity_event_path_samples',
  ];
  paperReport.parallel_paper_stage_tables = activeTables;
  paperReport.parallel_paper_stage_count = activeTables.length;
  delete paperReport.parallel_paper_stages.opportunity_event_path_samples;
  delete paperReport.parallel_paper_source_read_lock_duration_sec.opportunity_event_path_samples;
  paperReport.parallel_paper_stages.paper_decision_events.stage_budget_bytes = 24576;
  paperReport.parallel_paper_stages.a_class_decision_events.stage_budget_bytes = 24576;
  paperReport.parallel_paper_stages.opportunity_events.stage_budget_bytes = 20480;
  paperReport.paper_decision_parallel_stage_budget_bytes = 24576;
  paperReport.selected_tables.paper_decision_events.parallel_stage.stage_budget_bytes = 24576;
  paperReport.selected_tables.a_class_decision_events.parallel_stage.stage_budget_bytes = 24576;
  paperReport.selected_tables.opportunity_events.parallel_stage.stage_budget_bytes = 20480;
  paperReport.pinned_read_views = paperReport.pinned_read_views.filter(
    (row) => row.role !== 'opportunity_event_path_samples_parallel_stage',
  );
  paperReport.selected_tables.opportunity_event_path_samples = {
    included: false,
    required: false,
    reason: 'optional_source_table_missing',
  };

  const health = readEvaluatorSnapshotWorkerHealth({
    nowMs,
    enabled: true,
    pid: 33333,
    pidAlive: true,
    lockPid: 33333,
    lockPidAlive: true,
    statusPayload: fixture.statusPayload,
    manifestPayload,
    statusArtifact: { available: true, mtime: '2026-08-08T03:59:30.000Z', size_bytes: 100 },
    manifestArtifact: { available: true, mtime: '2026-08-08T03:59:30.000Z', size_bytes: 1000 },
    manifestFileSha256: 'd'.repeat(64),
    databaseArtifacts: fixture.databaseArtifacts,
    authoritativePreflight: fixture.authoritativePreflight,
  });

  assert.equal(
    health.status,
    'producer_accepted',
    JSON.stringify({
      blockers: health.blockers,
      manifest_contract: health.manifest_contract,
      shared_stage_budget: health.shared_stage_budget,
    }, null, 2),
  );
  assert.equal(health.consumer_ready, true);
  assert.equal(health.manifest_contract.parallel_paper_stage_inventory_passed, true);
  assert.equal(health.parallel_paper_stages.passed, true);
  assert.deepEqual(health.parallel_paper_stages.active_tables, activeTables);
  assert.equal(health.parallel_paper_stages.configured_tables.length, 4);
  assert.equal(health.parallel_paper_stages.optional_absence_valid, true);
  assert.equal(health.parallel_paper_stages.stage_count, 3);
  assert.deepEqual(health.shared_stage_budget.active_targets, [
    'candidate_shadow_observations',
    ...activeTables,
  ]);
  assert.equal(
    Object.hasOwn(
      health.shared_stage_budget.targets,
      'opportunity_event_path_samples',
    ),
    false,
  );
  assert.equal(health.pinned_read_view_lineage.expected_count, 7);
  assert.equal(health.pinned_read_view_lineage.actual_count, 7);
});

test('evaluator snapshot worker health distinguishes starting failed stale and contract-blocked states', () => {
  const nowMs = Date.parse('2026-08-08T04:00:00.000Z');
  const fixture = evaluatorSnapshotHealthFixture(nowMs);
  const base = {
    nowMs,
    enabled: true,
    pid: 33333,
    pidAlive: true,
    lockPid: 33333,
    lockPidAlive: true,
    statusPayload: fixture.statusPayload,
    manifestPayload: fixture.manifestPayload,
    statusArtifact: { available: true, mtime: '2026-08-08T03:59:30.000Z', size_bytes: 100 },
    manifestArtifact: { available: true, mtime: '2026-08-08T03:59:30.000Z', size_bytes: 1000 },
    manifestFileSha256: 'd'.repeat(64),
    databaseArtifacts: fixture.databaseArtifacts,
    authoritativePreflight: fixture.authoritativePreflight,
  };

  const starting = readEvaluatorSnapshotWorkerHealth({
    nowMs,
    enabled: true,
    processUptimeSec: 10,
    startupGraceSec: 600,
    pidAlive: false,
    lockPidAlive: false,
    lockPid: null,
    statusPayload: null,
    manifestPayload: null,
    statusArtifact: { available: false },
    manifestArtifact: { available: false },
  });
  assert.equal(starting.status, 'starting');
  assert.equal(starting.degraded, false);

  const failedSharedBudget = structuredClone(
    fixture.statusPayload.shared_stage_budget,
  );
  failedSharedBudget.accepted = false;
  failedSharedBudget.cleanup_completed = true;
  failedSharedBudget.no_unregistered_stage_files = true;
  failedSharedBudget.targets.paper_decision_events.actual_usage_bytes = 32768;
  failedSharedBudget.targets.paper_decision_events.high_water_bytes = 32768;
  failedSharedBudget.targets.paper_decision_events.copy_completed = false;
  failedSharedBudget.targets.paper_decision_events.cap_hit = true;
  failedSharedBudget.targets.paper_decision_events.within_grant = true;
  failedSharedBudget.targets.paper_decision_events.utilization_ratio = 1;
  failedSharedBudget.actual_total_bytes = 98768;
  failedSharedBudget.unconsumed_bytes = 3632;
  failedSharedBudget.plan_sha256 = sharedStageBudgetPlanSha256(
    failedSharedBudget,
  );
  failedSharedBudget.evidence_sha256 = sharedStageBudgetEvidenceSha256(
    failedSharedBudget,
  );
  const failed = readEvaluatorSnapshotWorkerHealth({
    ...base,
    statusPayload: {
      ...fixture.statusPayload,
      status: 'failed',
      accepted: false,
      last_failure_code: 'source_read_lock_budget_exceeded',
      last_failure_details: {
        paper: {
          error_code: 'source_read_lock_budget_exceeded',
          error_type: 'RuntimeError',
          stage: 'copy_table:candidate_shadow_observations',
          sqlite_errorcode: 5,
          sqlite_errorname: 'SQLITE_BUSY',
          copy_timing: {
            current_table: 'candidate_shadow_observations',
            current_table_elapsed_sec: 42.1256789,
            source_lock_elapsed_sec: 299.875,
            source_lock_remaining_sec: 0.125,
            completed_parallel_stages: [
              'paper_decision_events',
              '../../private_key',
              'paper_decision_events',
            ],
            completed_tables: {
              candidate_shadow_virtual_trades: {
                duration_sec: 3.25,
                rows_copied: 1234,
                source_lock_elapsed_sec: 7.5,
                source_lock_remaining_sec: 292.5,
                secret: '/app/data/private.db',
              },
              '../../private_key': {
                duration_sec: 1,
                rows_copied: 999,
              },
            },
            unsafe_path: '/app/data/paper_trades.db',
          },
          unsafe_extra: '/app/data/paper_trades.db',
        },
        raw: {
          error_code: '/app/data/raw.db?token=secret',
          error_type: 'SELECT * FROM secrets',
          stage: 'copy_table:../../private_key',
          sqlite_errorcode: -1,
          sqlite_errorname: 'SQLITE_BUSY /app/data/raw.db',
        },
        unexpected: {
          error_code: 'should_not_be_public',
          stage: 'ignored',
        },
      },
      consecutive_failure_count: 1,
      consecutive_failure_code_count: 1,
      next_attempt_delay_sec: 60,
      next_attempt_at: '2026-08-08T04:01:00.000Z',
      shared_stage_budget: failedSharedBudget,
    },
  });
  assert.equal(failed.status, 'failed');
  assert.equal(failed.degraded, true);
  assert.equal(failed.consumer_ready, true);
  assert.ok(failed.blockers.includes('source_read_lock_budget_exceeded'));
  assert.deepEqual(failed.last_failure_details, {
    paper: {
      error_code: 'source_read_lock_budget_exceeded',
      error_type: 'RuntimeError',
      stage: 'copy_table:candidate_shadow_observations',
      sqlite_errorcode: 5,
      sqlite_errorname: 'SQLITE_BUSY',
      copy_timing: {
        current_table: 'candidate_shadow_observations',
        current_table_elapsed_sec: 42.125679,
        source_lock_elapsed_sec: 299.875,
        source_lock_remaining_sec: 0.125,
        completed_tables: {
          candidate_shadow_virtual_trades: {
            duration_sec: 3.25,
            rows_copied: 1234,
            source_lock_elapsed_sec: 7.5,
            source_lock_remaining_sec: 292.5,
          },
        },
        completed_parallel_stages: ['paper_decision_events'],
      },
    },
    raw: {
      error_code: 'snapshot_component_failed',
      error_type: 'Exception',
      stage: 'unknown',
    },
  });
  assert.equal(failed.failure_retry_sec, 60);
  assert.equal(failed.consecutive_failure_count, 1);
  assert.equal(failed.consecutive_failure_code_count, 1);
  assert.equal(failed.next_attempt_delay_sec, 60);
  assert.equal(failed.next_attempt_at, '2026-08-08T04:01:00.000Z');
  assert.equal(failed.shared_stage_budget.accepted, false);
  assert.equal(
    failed.shared_stage_budget.targets.paper_decision_events.cap_hit,
    true,
  );
  assert.equal(
    failed.shared_stage_budget.targets.paper_decision_events.high_water_bytes,
    32768,
  );
  assert.equal(
    failed.shared_stage_budget.targets.paper_decision_events
      .advisory_evidence_passed,
    true,
  );
  assert.equal(
    failed.shared_stage_budget.targets.paper_decision_events
      .advisory_schema_version,
    'sqlite_dbstat_advisory_demand.v1',
  );
  assert.equal(
    failed.shared_stage_budget.targets.paper_decision_events
      .capacity_sample_used,
    false,
  );
  assert.equal(JSON.stringify(failed).includes('/app/data/paper_trades.db'), false);
  assert.equal(JSON.stringify(failed).includes('/app/data/raw.db'), false);
  assert.equal(JSON.stringify(failed).includes('SQLITE_BUSY /app/data/raw.db'), false);
  assert.equal(JSON.stringify(failed).includes('SELECT * FROM secrets'), false);
  assert.equal(JSON.stringify(failed).includes('private_key'), false);
  assert.equal(Object.hasOwn(failed.last_failure_details, 'unexpected'), false);

  const unsafeTopLevelCode = readEvaluatorSnapshotWorkerHealth({
    ...base,
    statusPayload: {
      ...fixture.statusPayload,
      status: 'failed',
      accepted: false,
      last_failure_code: '/app/data/paper.db?token=secret',
    },
  });
  assert.equal(unsafeTopLevelCode.last_failure_code, 'snapshot_component_failed');
  assert.ok(unsafeTopLevelCode.blockers.includes('snapshot_component_failed'));
  assert.equal(JSON.stringify(unsafeTopLevelCode).includes('/app/data/paper.db'), false);

  const runningNullSchedule = readEvaluatorSnapshotWorkerHealth({
    ...base,
    statusPayload: {
      ...fixture.statusPayload,
      status: 'running',
      attempt_running: true,
      consecutive_failure_count: 2,
      next_attempt_delay_sec: null,
      next_attempt_at: null,
    },
  });
  assert.equal(runningNullSchedule.consecutive_failure_count, 2);
  assert.equal(runningNullSchedule.next_attempt_delay_sec, null);
  assert.equal(runningNullSchedule.next_attempt_at, null);

  const metadataStage = readEvaluatorSnapshotWorkerHealth({
    ...base,
    statusPayload: {
      ...fixture.statusPayload,
      status: 'failed',
      accepted: false,
      last_failure_code: 'source_read_lock_budget_exceeded',
      last_failure_details: {
        paper: {
          error_code: 'source_read_lock_budget_exceeded',
          error_type: 'RuntimeError',
          stage: 'source_metadata:candidate_shadow_observations',
        },
      },
    },
  });
  assert.equal(
    metadataStage.last_failure_details.paper.stage,
    'source_metadata:candidate_shadow_observations',
  );

  const parallelStageFailure = readEvaluatorSnapshotWorkerHealth({
    ...base,
    statusPayload: {
      ...fixture.statusPayload,
      status: 'failed',
      accepted: false,
      last_failure_code: 'paper_decision_parallel_stage_start_timeout',
      last_failure_details: {
        paper: {
          error_code: 'paper_decision_parallel_stage_start_timeout',
          error_type: 'RuntimeError',
          stage: 'paper_parallel_pinned_barrier',
        },
      },
    },
  });
  assert.equal(
    parallelStageFailure.last_failure_code,
    'paper_decision_parallel_stage_start_timeout',
  );
  assert.deepEqual(parallelStageFailure.last_failure_details.paper, {
    error_code: 'paper_decision_parallel_stage_start_timeout',
    error_type: 'RuntimeError',
    stage: 'paper_parallel_pinned_barrier',
  });

  const stale = readEvaluatorSnapshotWorkerHealth({
    ...base,
    nowMs: nowMs + 9 * 3600 * 1000,
    maxAgeSec: 8 * 3600,
  });
  assert.equal(stale.status, 'stale');
  assert.equal(stale.snapshot_fresh, false);

  const contractBlocked = readEvaluatorSnapshotWorkerHealth({
    ...base,
    manifestPayload: {
      ...fixture.manifestPayload,
      disk_preflight: { ...fixture.manifestPayload.disk_preflight, accepted: false },
    },
  });
  assert.equal(contractBlocked.status, 'contract_blocked');
  assert.equal(contractBlocked.consumer_ready, false);
  assert.ok(contractBlocked.blockers.includes('evaluator_snapshot_manifest_contract_blocked'));

  const requiredStageOmittedManifest = structuredClone(fixture.manifestPayload);
  requiredStageOmittedManifest.parallel_paper_stage_tables =
    requiredStageOmittedManifest.parallel_paper_stage_tables.filter(
      (table) => table !== 'opportunity_events',
    );
  requiredStageOmittedManifest.parallel_paper_stage_count =
    requiredStageOmittedManifest.parallel_paper_stage_tables.length;
  const requiredStageOmitted = readEvaluatorSnapshotWorkerHealth({
    ...base,
    manifestPayload: requiredStageOmittedManifest,
  });
  assert.equal(requiredStageOmitted.status, 'contract_blocked');
  assert.equal(requiredStageOmitted.consumer_ready, false);
  assert.equal(
    requiredStageOmitted.manifest_contract.parallel_paper_stage_inventory_passed,
    false,
  );

  const pinnedLineageManifest = structuredClone(fixture.manifestPayload);
  pinnedLineageManifest.pinned_read_view_count = 4;
  pinnedLineageManifest.databases.paper.pinned_read_views[1].role = 'paper_main_selective_copy';
  pinnedLineageManifest.databases.paper.pinned_read_views[1].pinned_midpoint_epoch += 120;
  const pinnedLineageBlocked = readEvaluatorSnapshotWorkerHealth({
    ...base,
    manifestPayload: pinnedLineageManifest,
  });
  assert.equal(pinnedLineageBlocked.status, 'contract_blocked');
  assert.equal(pinnedLineageBlocked.consumer_ready, false);
  assert.equal(
    pinnedLineageBlocked.manifest_contract.pinned_read_view_lineage_passed,
    false,
  );
  assert.equal(pinnedLineageBlocked.pinned_read_view_lineage.passed, false);
  assert.ok(
    pinnedLineageBlocked.blockers.includes('evaluator_snapshot_manifest_contract_blocked'),
  );

  for (const mutation of [
    'advisory_read_view_id_mismatch',
    'advisory_read_view_role_mismatch',
    'manifest_binding_flag_false',
    'paper_binding_flag_false',
    'pinned_view_id_mismatch',
  ]) {
    const bindingManifest = structuredClone(fixture.manifestPayload);
    const shared = bindingManifest.shared_stage_budget;
    const p9 = shared.targets.paper_decision_events;
    let sharedMutated = false;
    if (mutation === 'advisory_read_view_id_mismatch') {
      p9.advisory_evidence.pinned_read_view_id = shared.targets
        .candidate_shadow_observations.advisory_evidence.pinned_read_view_id;
      sharedMutated = true;
    } else if (mutation === 'advisory_read_view_role_mismatch') {
      p9.advisory_evidence.pinned_read_view_role = 'paper_main_selective_copy';
      sharedMutated = true;
    } else if (mutation === 'manifest_binding_flag_false') {
      bindingManifest.shared_stage_estimates_bound_to_copy_read_views = false;
    } else if (mutation === 'paper_binding_flag_false') {
      bindingManifest.databases.paper
        .shared_stage_estimates_bound_to_copy_read_views = false;
    } else if (mutation === 'pinned_view_id_mismatch') {
      const pinnedView = bindingManifest.databases.paper.pinned_read_views.find(
        (row) => row.role === 'paper_decision_events_parallel_stage',
      );
      assert.ok(pinnedView);
      pinnedView.read_view_id = 'f'.repeat(32);
    }
    if (sharedMutated) {
      shared.plan_sha256 = sharedStageBudgetPlanSha256(shared);
      shared.evidence_sha256 = sharedStageBudgetEvidenceSha256(shared);
      bindingManifest.disk_preflight.shared_stage_budget = structuredClone(shared);
    }
    const bindingBlocked = readEvaluatorSnapshotWorkerHealth({
      ...base,
      manifestPayload: bindingManifest,
    });
    assert.equal(bindingBlocked.status, 'contract_blocked', mutation);
    assert.equal(bindingBlocked.consumer_ready, false, mutation);
    assert.equal(
      bindingBlocked.manifest_contract.shared_stage_budget_passed,
      false,
      mutation,
    );
    assert.equal(
      bindingBlocked.shared_stage_budget.contract_passed,
      false,
      mutation,
    );
    assert.ok(
      bindingBlocked.blockers.includes(
        'evaluator_snapshot_manifest_contract_blocked',
      ),
      mutation,
    );
  }

  const paperDecisionManifest = structuredClone(fixture.manifestPayload);
  paperDecisionManifest.databases.paper.selected_tables.paper_decision_events.parallel_stage.row_count_matched = false;
  paperDecisionManifest.databases.paper.paper_decision_parallel_stage_removed_before_publish = false;
  const paperDecisionBlocked = readEvaluatorSnapshotWorkerHealth({
    ...base,
    manifestPayload: paperDecisionManifest,
  });
  assert.equal(paperDecisionBlocked.status, 'contract_blocked');
  assert.equal(paperDecisionBlocked.consumer_ready, false);
  assert.equal(
    paperDecisionBlocked.manifest_contract.paper_decision_parallel_stage_passed,
    false,
  );
  assert.equal(paperDecisionBlocked.parallel_paper_decision_stage.passed, false);
  assert.ok(
    paperDecisionBlocked.blockers.includes('evaluator_snapshot_manifest_contract_blocked'),
  );

  for (const table of [
    'a_class_decision_events',
    'opportunity_events',
    'opportunity_event_path_samples',
  ]) {
    const parallelStageManifest = structuredClone(fixture.manifestPayload);
    parallelStageManifest.databases.paper.parallel_paper_stages[table].rows_merged += 1;
    parallelStageManifest.databases.paper.parallel_paper_stages[table].removed_before_publish = false;
    parallelStageManifest.databases.paper.selected_tables[table].parallel_stage.row_count_matched = false;
    const parallelStageBlocked = readEvaluatorSnapshotWorkerHealth({
      ...base,
      manifestPayload: parallelStageManifest,
    });
    assert.equal(parallelStageBlocked.status, 'contract_blocked');
    assert.equal(parallelStageBlocked.consumer_ready, false);
    assert.equal(
      parallelStageBlocked.manifest_contract.parallel_paper_stages_passed,
      false,
    );
    assert.equal(parallelStageBlocked.parallel_paper_stages.passed, false);
    assert.equal(parallelStageBlocked.parallel_paper_stages.stages[table].passed, false);
    assert.ok(
      parallelStageBlocked.blockers.includes('evaluator_snapshot_manifest_contract_blocked'),
    );
  }

  for (const [field, tamperedValue] of [
    ['stage_schema_mode', 'source_schema_with_constraints'],
    ['source_create_sql_sha256', '0'.repeat(64)],
    ['destination_create_sql_sha256', '0'.repeat(64)],
    ['source_column_contract_sha256', '0'.repeat(64)],
    ['destination_column_contract_sha256', '0'.repeat(64)],
    ['stage_storage_contract_sha256', '0'.repeat(64)],
    ['stage_storage_contract_passed', false],
    ['stage_codec_schema_version', 'unknown-codec'],
    ['stage_compression', 'lossy'],
    ['stage_chunk_target_bytes', 1],
    ['stage_chunk_count', 999],
    ['stage_raw_size_bytes', -1],
    ['stage_compressed_payload_size_bytes', -1],
    ['stage_rows_sha256', '0'.repeat(64)],
    ['hydrated_rows_sha256', '0'.repeat(64)],
    ['stage_chunk_integrity_passed', false],
    ['stage_row_digest_matched', false],
    ['compressed_during_source_read_lock', false],
    ['hydrated_after_source_read_lock_release', false],
    ['stage_column_count', 999],
    ['stage_index_count', 1],
    ['source_constraints_deferred_off_source_lock', false],
    ['destination_schema_restored_after_source_read_lock_release', false],
    ['source_constraints_rebuilt_after_source_read_lock_release', false],
  ]) {
    const stageSchemaManifest = structuredClone(fixture.manifestPayload);
    const table = 'opportunity_events';
    stageSchemaManifest.databases.paper.parallel_paper_stages[table][field] = tamperedValue;
    const stageSchemaBlocked = readEvaluatorSnapshotWorkerHealth({
      ...base,
      manifestPayload: stageSchemaManifest,
    });
    assert.equal(stageSchemaBlocked.status, 'contract_blocked');
    assert.equal(stageSchemaBlocked.consumer_ready, false);
    assert.equal(
      stageSchemaBlocked.manifest_contract.parallel_paper_stages_passed,
      false,
    );
    assert.equal(
      stageSchemaBlocked.parallel_paper_stages.stages[table].passed,
      false,
    );
    assert.ok(
      stageSchemaBlocked.blockers.includes('evaluator_snapshot_manifest_contract_blocked'),
    );
  }

  const fractionalStageManifest = structuredClone(fixture.manifestPayload);
  const fractionalStageTable = 'opportunity_events';
  const fractionalStage = fractionalStageManifest.databases.paper
    .parallel_paper_stages[fractionalStageTable];
  const fractionalNestedStage = fractionalStageManifest.databases.paper
    .selected_tables[fractionalStageTable].parallel_stage;
  for (const field of [
    'stage_chunk_count',
    'stage_raw_size_bytes',
    'stage_compressed_payload_size_bytes',
    'stage_index_count',
  ]) {
    fractionalStage[field] = 0.5;
    fractionalNestedStage[field] = 0.5;
  }
  const fractionalStageBlocked = readEvaluatorSnapshotWorkerHealth({
    ...base,
    manifestPayload: fractionalStageManifest,
  });
  assert.equal(fractionalStageBlocked.status, 'contract_blocked');
  assert.equal(fractionalStageBlocked.consumer_ready, false);
  assert.equal(
    fractionalStageBlocked.manifest_contract.parallel_paper_stages_passed,
    false,
  );
  assert.equal(
    fractionalStageBlocked.parallel_paper_stages.stages[fractionalStageTable]
      .passed,
    false,
  );
  assert.ok(
    fractionalStageBlocked.blockers.includes(
      'evaluator_snapshot_manifest_contract_blocked',
    ),
  );

  const applyNumericEvidenceTamper = (manifest, layer, value) => {
    const paper = manifest.databases.paper;
    if (layer === 'parallel_stage_copies') {
      const table = 'opportunity_events';
      const stage = paper.parallel_paper_stages[table];
      const selection = paper.selected_tables[table];
      const nested = selection.parallel_stage;
      for (const field of [
        'stage_chunk_count',
        'stage_raw_size_bytes',
        'stage_compressed_payload_size_bytes',
        'stage_index_count',
      ]) {
        stage[field] = value;
        selection[field] = value;
        nested[field] = value;
      }
    } else if (layer === 'shared_budget_copies') {
      const shared = manifest.shared_stage_budget;
      shared.advisory_miss_count = value;
      try {
        shared.plan_sha256 = sharedStageBudgetPlanSha256(shared);
        shared.evidence_sha256 = sharedStageBudgetEvidenceSha256(shared);
      } catch (error) {
        assert.match(String(error?.message || error), /safe integer/);
      }
      manifest.disk_preflight.shared_stage_budget = structuredClone(shared);
    } else if (layer === 'disk_preflight') {
      manifest.disk_preflight.temporary_full_backup_bytes = value;
    } else if (layer === 'stage_inventory') {
      manifest.parallel_paper_stage_count = value;
      paper.parallel_paper_stage_count = value;
    } else if (layer === 'duration_copies') {
      manifest.max_source_read_lock_sec = value;
      for (const report of Object.values(manifest.databases)) {
        report.source_read_lock_limit_sec = value;
        for (const pinnedView of report.pinned_read_views) {
          pinnedView.source_read_lock_limit_sec = value;
        }
      }
    } else if (layer === 'paper_alias_copies') {
      const stage = paper.parallel_paper_stages.paper_decision_events;
      const nested = paper.selected_tables.paper_decision_events.parallel_stage;
      paper.paper_decision_parallel_stage_merge_duration_sec = value;
      stage.merge_duration_sec = value;
      nested.merge_duration_sec = value;
    } else if (layer === 'output_budget') {
      manifest.output_size_bytes = value;
    } else {
      assert.fail(`unknown numeric evidence layer: ${layer}`);
    }
  };
  const invalidIntegerValues = [
    [0.5, 'fractional'],
    ['0', 'numeric-string'],
    [false, 'boolean'],
    [null, 'null'],
    [Number.MAX_SAFE_INTEGER + 1, 'unsafe-integer'],
    [{}, 'object'],
    [[], 'array'],
  ];
  const invalidFiniteNumberValues = [
    ['0', 'numeric-string'],
    [false, 'boolean'],
    [null, 'null'],
    [Number.POSITIVE_INFINITY, 'non-finite'],
    [{}, 'object'],
    [[], 'array'],
  ];
  const numericTamperCases = [
    ...[
      'parallel_stage_copies',
      'shared_budget_copies',
      'disk_preflight',
      'stage_inventory',
      'output_budget',
    ].flatMap((layer) => invalidIntegerValues.map(([value, label]) => ({
      layer,
      value,
      label,
    }))),
    ...['duration_copies', 'paper_alias_copies'].flatMap(
      (layer) => invalidFiniteNumberValues.map(([value, label]) => ({
        layer,
        value,
        label,
      })),
    ),
  ];
  for (const { layer, value, label } of numericTamperCases) {
    const manifest = structuredClone(fixture.manifestPayload);
    applyNumericEvidenceTamper(manifest, layer, value);
    const blocked = readEvaluatorSnapshotWorkerHealth({
      ...base,
      manifestPayload: manifest,
    });
    const caseLabel = `${layer}:${label}`;
    assert.equal(blocked.status, 'contract_blocked', caseLabel);
    assert.equal(blocked.consumer_ready, false, caseLabel);
    if (value !== null) {
      assert.equal(
        blocked.manifest_contract.numeric_evidence_types_passed,
        false,
        caseLabel,
      );
    }
    assert.ok(
      blocked.blockers.includes('evaluator_snapshot_manifest_contract_blocked'),
      caseLabel,
    );
  }

  const integralFloatManifest = structuredClone(fixture.manifestPayload);
  const jsonIntegralFloat = (value) => JSON.parse(`${value}.0`);
  const integralStageTable = 'opportunity_events';
  const integralStage = integralFloatManifest.databases.paper
    .parallel_paper_stages[integralStageTable];
  const integralSelection = integralFloatManifest.databases.paper
    .selected_tables[integralStageTable];
  const integralNested = integralSelection.parallel_stage;
  for (const field of [
    'stage_chunk_count',
    'stage_raw_size_bytes',
    'stage_compressed_payload_size_bytes',
    'stage_index_count',
  ]) {
    const value = jsonIntegralFloat(integralStage[field]);
    integralStage[field] = value;
    integralSelection[field] = value;
    integralNested[field] = value;
  }
  integralFloatManifest.parallel_paper_stage_count = jsonIntegralFloat(
    integralFloatManifest.parallel_paper_stage_count,
  );
  integralFloatManifest.databases.paper.parallel_paper_stage_count = (
    jsonIntegralFloat(
      integralFloatManifest.databases.paper.parallel_paper_stage_count,
    )
  );
  integralFloatManifest.pinned_read_view_count = jsonIntegralFloat(
    integralFloatManifest.pinned_read_view_count,
  );
  integralFloatManifest.output_size_bytes = jsonIntegralFloat(
    integralFloatManifest.output_size_bytes,
  );
  integralFloatManifest.disk_preflight.temporary_full_backup_bytes = (
    JSON.parse('0.0')
  );
  const integralShared = integralFloatManifest.shared_stage_budget;
  integralShared.advisory_miss_count = jsonIntegralFloat(
    integralShared.advisory_miss_count,
  );
  integralShared.plan_sha256 = sharedStageBudgetPlanSha256(integralShared);
  integralShared.evidence_sha256 = sharedStageBudgetEvidenceSha256(
    integralShared,
  );
  integralFloatManifest.disk_preflight.shared_stage_budget = structuredClone(
    integralShared,
  );
  const integralFloatAccepted = readEvaluatorSnapshotWorkerHealth({
    ...base,
    manifestPayload: integralFloatManifest,
  });
  assert.equal(
    integralFloatAccepted.status,
    'producer_accepted',
    JSON.stringify(integralFloatAccepted.manifest_contract),
  );
  assert.equal(
    integralFloatAccepted.manifest_contract.numeric_evidence_types_passed,
    true,
  );

  const nullIndexManifest = structuredClone(fixture.manifestPayload);
  nullIndexManifest.databases.paper.parallel_paper_stages.opportunity_events.stage_index_count = null;
  nullIndexManifest.databases.paper.selected_tables.opportunity_events.parallel_stage.stage_index_count = null;
  const nullIndexBlocked = readEvaluatorSnapshotWorkerHealth({
    ...base,
    manifestPayload: nullIndexManifest,
  });
  assert.equal(nullIndexBlocked.status, 'contract_blocked');
  assert.equal(nullIndexBlocked.consumer_ready, false);
  assert.equal(
    nullIndexBlocked.parallel_paper_stages.stages.opportunity_events.stage_index_count,
    null,
  );
  assert.equal(
    nullIndexBlocked.parallel_paper_stages.stages.opportunity_events.passed,
    false,
  );
  assert.ok(
    nullIndexBlocked.blockers.includes('evaluator_snapshot_manifest_contract_blocked'),
  );

  for (const mutation of [
    'legacy_fixed_share',
    'copies_diverged',
    'grant_sum_exceeds_cap',
    'actual_exceeds_grant',
    'cleanup_incomplete',
    'stage_files_not_removed',
    'unregistered_stage_file',
    'null_grant',
    'advisory_sample_used',
    'advisory_scaled_physical_tamper',
    'advisory_physical_bytes_tamper',
    'advisory_candidate_order_index_tamper',
    'advisory_formula_tamper',
    'advisory_claims_physical_upper',
    'allocation_weight_tamper',
    'advisory_miss_inventory_tamper',
    'global_hard_cap_disabled',
    'per_target_max_page_count_disabled',
    'row_count_manifest_mismatch',
    'hash_payload_tamper',
  ]) {
    const sharedManifest = structuredClone(fixture.manifestPayload);
    const shared = sharedManifest.shared_stage_budget;
    const diskShared = sharedManifest.disk_preflight;
    const p9 = shared.targets.paper_decision_events;
    if (mutation === 'legacy_fixed_share') {
      diskShared.candidate_stage_residual_share = 0.12;
    } else if (mutation === 'copies_diverged') {
      diskShared.shared_stage_budget.targets.paper_decision_events.granted_cap_bytes += 4096;
    } else if (mutation === 'grant_sum_exceeds_cap') {
      p9.granted_cap_bytes += 4096;
      p9.borrowed_shared_pool_bytes += 4096;
      shared.total_granted_bytes += 4096;
      diskShared.shared_stage_budget = structuredClone(shared);
      diskShared.temporary_parallel_paper_stage_cap_bytes.paper_decision_events += 4096;
      diskShared.temporary_paper_decision_stage_cap_bytes += 4096;
    } else if (mutation === 'actual_exceeds_grant') {
      const delta = p9.granted_cap_bytes + 1 - p9.actual_usage_bytes;
      p9.actual_usage_bytes += delta;
      p9.high_water_bytes += delta;
      p9.utilization_ratio = p9.actual_usage_bytes / p9.granted_cap_bytes;
      shared.actual_total_bytes += delta;
      shared.unconsumed_bytes -= delta;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'cleanup_incomplete') {
      shared.cleanup_completed = false;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'stage_files_not_removed') {
      shared.stage_files_removed = false;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'unregistered_stage_file') {
      shared.no_unregistered_stage_files = false;
      shared.unregistered_stage_files = ['.rogue-stage.db'];
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'null_grant') {
      p9.granted_cap_bytes = null;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'advisory_sample_used') {
      p9.advisory_evidence.capacity_sample_used = true;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'advisory_scaled_physical_tamper') {
      p9.advisory_evidence.table_scaled_physical_advisory_bytes += 4096;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'advisory_physical_bytes_tamper') {
      p9.advisory_evidence.source_dbstat_physical_bytes += 4096;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'advisory_candidate_order_index_tamper') {
      shared.targets.candidate_shadow_observations.advisory_evidence
        .candidate_order_source_index_dbstat_physical_bytes += 4096;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'advisory_formula_tamper') {
      p9.advisory_evidence.advisory_formula = (
        'edge_sample_average_times_selected_rows'
      );
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'advisory_claims_physical_upper') {
      p9.physical_upper_bound_claimed = true;
      p9.advisory_evidence.physical_upper_bound_claimed = true;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'allocation_weight_tamper') {
      p9.allocation_weight_bytes += 4096;
      shared.allocation_weight_total_bytes += 4096;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'advisory_miss_inventory_tamper') {
      shared.targets_exceeding_advisory = ['paper_decision_events'];
      shared.advisory_miss_count = 1;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'global_hard_cap_disabled') {
      shared.global_hard_cap_enforced = false;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'per_target_max_page_count_disabled') {
      shared.per_target_max_page_count_enforced = false;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'row_count_manifest_mismatch') {
      p9.actual_rows_copied = 2;
      diskShared.shared_stage_budget = structuredClone(shared);
    } else if (mutation === 'hash_payload_tamper') {
      p9.history_state = 'tampered-after-signing';
      diskShared.shared_stage_budget = structuredClone(shared);
    }
    const sharedBlocked = readEvaluatorSnapshotWorkerHealth({
      ...base,
      manifestPayload: sharedManifest,
    });
    assert.equal(sharedBlocked.status, 'contract_blocked', mutation);
    assert.equal(sharedBlocked.consumer_ready, false, mutation);
    assert.equal(
      sharedBlocked.manifest_contract.shared_stage_budget_passed,
      false,
      mutation,
    );
    assert.equal(sharedBlocked.shared_stage_budget.contract_passed, false, mutation);
    assert.ok(
      sharedBlocked.blockers.includes('evaluator_snapshot_manifest_contract_blocked'),
      mutation,
    );
  }

  const shaBlocked = readEvaluatorSnapshotWorkerHealth({
    ...base,
    manifestFileSha256: 'e'.repeat(64),
  });
  assert.equal(shaBlocked.status, 'contract_blocked');
  assert.equal(shaBlocked.manifest_sha256_matched, false);

  const futureBlocked = readEvaluatorSnapshotWorkerHealth({
    ...base,
    manifestPayload: {
      ...fixture.manifestPayload,
      snapshot_ts: Math.floor(nowMs / 1000) + 61,
    },
  });
  assert.equal(futureBlocked.status, 'contract_blocked');
  assert.equal(futureBlocked.snapshot_timestamp_valid, false);
  assert.equal(futureBlocked.snapshot_future_skew_sec, 61);
  assert.ok(futureBlocked.blockers.includes('evaluator_snapshot_future_timestamp'));

  const missingFile = readEvaluatorSnapshotWorkerHealth({
    ...base,
    databaseArtifacts: {
      ...fixture.databaseArtifacts,
      paper: { available: false },
    },
  });
  assert.equal(missingFile.status, 'contract_blocked');
  assert.equal(missingFile.bundle_candidate_available, false);
  assert.ok(missingFile.blockers.includes('evaluator_snapshot_paper_file_missing'));

  const sizeMismatch = readEvaluatorSnapshotWorkerHealth({
    ...base,
    databaseArtifacts: {
      ...fixture.databaseArtifacts,
      raw: { available: true, size_bytes: 99 },
    },
  });
  assert.equal(sizeMismatch.status, 'contract_blocked');
  assert.equal(sizeMismatch.snapshot_files.raw.size_matches_manifest, false);
  assert.ok(sizeMismatch.blockers.includes('evaluator_snapshot_raw_size_mismatch'));
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

test('raw dog discovery static API snapshot is atomic and window-scoped', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'raw-dog-api-snapshot-'));
  const snapshotPath = join(dir, 'raw-dog-discovery-summary.json');
  const payload = buildRawDogDiscoveryApiPayloadFromRollingSummary({
    available: true,
    since_ts: 1_780_000_000,
    summary: {
      total_signals: 3,
      raw_sustained_gold_silver_unique: 2,
    },
    decision_funnel: {
      summary: {
        quote_clean_no_would_enter: 1,
      },
    },
    top_raw_dogs: [
      { token_ca: 'A', max_sustained_peak_pct: 120 },
      { token_ca: 'B', max_sustained_peak_pct: 80 },
    ],
    missed_raw_dogs: [
      { token_ca: 'A' },
      { token_ca: 'B' },
    ],
  }, {
    hours: 24,
    limit: 2,
    coverageTargetPct: 80,
    source: 'unit_worker_snapshot',
    snapshotPath,
  });

  const written = writeRawDogDiscoveryApiSnapshot(payload, { snapshotPath });
  assert.equal(written.path, snapshotPath);
  assert.equal(fs.existsSync(snapshotPath), true);

  const read = readRawDogDiscoveryApiSnapshot({ snapshotPath, hours: 24, limit: 1 });
  assert.equal(read.available, true);
  assert.equal(read.source, 'unit_worker_snapshot');
  assert.equal(read.summary.total_signals, 3);
  assert.equal(read.top_raw_dogs.length, 1);
  assert.equal(read.top_raw_dogs[0].token_ca, 'A');
  assert.equal(read.missed_raw_dogs.length, 1);

  const mismatch = readRawDogDiscoveryApiSnapshot({ snapshotPath, hours: 6, limit: 1 });
  assert.equal(mismatch.available, false);
  assert.equal(mismatch.error_code, 'raw_dog_discovery_snapshot_window_mismatch');
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

test('livePaperQueryGuard rejects wide or oversized live paper queries', () => {
  const wide = livePaperQueryGuard(
    new URL('https://example.test/api/paper/mode-ev?hours=6&limit=500'),
    '/api/paper/mode-ev',
    { nowSec: 100_000, defaultHours: 2, maxHours: 2, defaultLimit: 500, maxLimit: 1000 }
  );
  assert.equal(wide.allowed, false);
  assert.equal(wide.error, 'live_paper_query_window_too_wide');
  assert.equal(wide.max_hours, 2);

  const large = livePaperQueryGuard(
    new URL('https://example.test/api/paper/mode-ev?hours=2&limit=5000'),
    '/api/paper/mode-ev',
    { nowSec: 100_000, defaultHours: 2, maxHours: 2, defaultLimit: 500, maxLimit: 1000 }
  );
  assert.equal(large.allowed, false);
  assert.equal(large.error, 'live_paper_query_limit_too_large');
  assert.equal(large.max_limit, 1000);
});

test('livePaperQueryGuard allows bounded 2h live paper query and computes since', () => {
  const guard = livePaperQueryGuard(
    new URL('https://example.test/api/paper/mode-ev?hours=2&limit=800&bootstrap_iterations=2500'),
    '/api/paper/mode-ev',
    {
      nowSec: 100_000,
      defaultHours: 2,
      maxHours: 2,
      defaultLimit: 500,
      maxLimit: 1000,
      maxBootstrapIterations: 3000,
    }
  );

  assert.equal(guard.allowed, true);
  assert.equal(guard.window_hours, 2);
  assert.equal(guard.since_ts, 100_000 - 2 * 3600);
  assert.equal(guard.limit, 800);
  assert.equal(guard.bootstrap_iterations, 2500);
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

test('rolling 24h goal status is calculated from materialized snapshot', () => {
  const status = buildRolling24hGoalStatusFromLiveSnapshot({
    snapshot_id: 'paper_live_24h_goal_test',
    generated_at: '2026-06-04T00:50:00.000Z',
    window: { since_ts: 1000, since_iso: '2026-06-03T00:50:00.000Z' },
    trades: {
      totals: {
        total: 24,
        closed: 20,
        wins: 12,
        min_pnl: -0.12,
        deployed_sol: 0.05,
        est_pnl_sol: 0.11,
      },
    },
    dog_catch_goal: {
      available: true,
      trades: {
        fills: 24,
        closed: 20,
        captured_gold_silver_unique: 6,
        realized_pnl_sol: 0.11,
        deployed_sol: 0.05,
        realized_roi: 2.2,
      },
      missed: {
        clean_gold_silver_unique: 4,
        by_blocker: [],
      },
      goal: {
        eligible_gold_silver_unique: 10,
        captured_gold_silver_unique: 6,
        clean_gold_silver_capture_rate: 0.6,
        pass: true,
        blockers: [],
      },
    },
    a_class: {
      available: true,
      would_enter: 3,
      enter: 0,
    },
    a_class_p0_discovery: {
      available: true,
      status: 'shadow_ready',
      denominator_key: 'quote_clean_gold_silver_unique:1000:2000',
      quote_clean_gold_silver_seen_count: 10,
      quote_clean_gold_silver_would_enter_count: 5,
      would_enter_no_route_rate: 0,
      would_enter_trapped_rate: 0,
      unknown_data_rate: 0,
      outlier_trimmed_would_rr: 2.25,
      missed_blockers: [
        { route: 'LOTTO', component: 'matrix_gate', reject_reason: 'weak_matrix', unique_tokens: 2, gold_n: 1, silver_n: 1 },
      ],
      discovery_exit: {
        advisory: 'PROMOTE_TINY_CANARY',
        advisory_only: true,
        requires_human_approval: true,
      },
    },
    entry_mode_performance: {
      by_entry_mode: [
        {
          bucket: 'tiny_scout',
          entry_mode: 'A_CLASS_FASTLANE',
          closed: 20,
          win_rate_pct: 60,
          avg_pnl_pct: 8,
          max_loss_pct: -12,
        },
      ],
    },
  }, {
    generatedAt: '2026-06-04T01:00:00.000Z',
    nowMs: Date.parse('2026-06-04T01:00:00.000Z'),
    requestedHours: 24,
    materializedHours: 24,
    dbPath: '/tmp/paper.db',
  });

  assert.equal(status.schema_version, 'v1.rolling_24h_strategy_goal_status');
  assert.equal(status.pass, true);
  assert.equal(status.status, 'pass');
  assert.equal(status.metrics.realized_win_rate, 0.6);
  assert.equal(status.metrics.gold_silver_capture_rate, 0.6);
  assert.equal(status.metrics.strategy_bucket_roi, 2.2);
  assert.equal(status.metrics.max_single_trade_loss_pct, -12);
  assert.equal(status.metrics.quote_clean_gold_silver_seen_24h, 10);
  assert.equal(status.metrics.quote_clean_gold_silver_would_enter_24h, 5);
  assert.equal(status.metrics.outlier_trimmed_would_rr, 2.25);
  assert.deepEqual(status.blockers, []);
  assert.equal(status.a_class_p0_discovery.discovery_exit.advisory, 'PROMOTE_TINY_CANARY');
  assert.equal(status.mode_actions[0].mode, 'A_CLASS_FASTLANE');
  assert.equal(status.mode_actions[0].status, 'SHADOW');
  assert.equal(status.mode_actions[0].recommended_action, 'prepare_0_001_tiny_paper_after_observability_green');
});

test('rolling 24h goal marks stale materialized snapshots unavailable', () => {
  const status = buildRolling24hGoalStatusFromLiveSnapshot({
    snapshot_id: 'paper_live_24h_goal_stale',
    generated_at: '2026-06-04T00:00:00.000Z',
    window: { since_ts: 1000 },
    trades: {
      totals: {
        total: 24,
        closed: 20,
        wins: 12,
        min_pnl: -0.12,
        deployed_sol: 0.05,
        est_pnl_sol: 0.11,
      },
    },
    dog_catch_goal: {
      available: true,
      trades: {
        closed: 20,
        captured_gold_silver_unique: 6,
        deployed_sol: 0.05,
        realized_pnl_sol: 0.11,
        realized_roi: 2.2,
      },
      missed: { clean_gold_silver_unique: 4 },
      goal: {
        eligible_gold_silver_unique: 10,
        captured_gold_silver_unique: 6,
        clean_gold_silver_capture_rate: 0.6,
        pass: true,
        blockers: [],
      },
    },
    a_class_p0_discovery: {
      available: true,
      quote_clean_gold_silver_seen_count: 10,
      quote_clean_gold_silver_would_enter_count: 6,
      outlier_trimmed_would_rr: 3.0,
      would_enter_no_route_rate: 0,
      would_enter_trapped_rate: 0,
      unknown_data_rate: 0,
      missed_blockers: [],
      discovery_exit: { advisory: 'PROMOTE_TINY_CANARY' },
    },
  }, {
    generatedAt: '2026-06-04T01:00:00.000Z',
    nowMs: Date.parse('2026-06-04T01:00:00.000Z'),
    requestedHours: 24,
    materializedHours: 24,
    maxSnapshotAgeMinutes: 30,
    minClosedTrades: 20,
    minGoldSilverCandidates: 5,
  });

  assert.equal(status.pass, false);
  assert.equal(status.available, false);
  assert.equal(status.status, 'evidence_unavailable');
  assert.equal(status.materialized_snapshot_fresh, false);
  assert.equal(status.snapshot_age_minutes, 60);
  assert.match(status.evidence_blockers.join(','), /materialized_review_snapshot_stale_or_undated/);
});

test('rolling 24h goal fails loud when live paper db is unavailable', () => {
  const status = buildRolling24hGoalStatusFromLiveSnapshot({
    snapshot_id: 'paper_live_24h_goal_test',
    generated_at: '2026-06-04T00:50:00.000Z',
    window: { since_ts: 1000, since_iso: '2026-06-03T00:50:00.000Z' },
    trades: {
      totals: {
        total: 24,
        closed: 20,
        wins: 12,
        min_pnl: -0.12,
        deployed_sol: 0.05,
        est_pnl_sol: 0.11,
      },
    },
    dog_catch_goal: {
      available: true,
      trades: {
        fills: 24,
        closed: 20,
        captured_gold_silver_unique: 6,
        realized_pnl_sol: 0.11,
        deployed_sol: 0.05,
        realized_roi: 2.2,
      },
      missed: {
        clean_gold_silver_unique: 4,
        by_blocker: [],
      },
      goal: {
        eligible_gold_silver_unique: 10,
        captured_gold_silver_unique: 6,
        clean_gold_silver_capture_rate: 0.6,
        pass: true,
        blockers: [],
      },
    },
    a_class_p0_discovery: {
      available: true,
      quote_clean_gold_silver_seen_count: 10,
      quote_clean_gold_silver_would_enter_count: 6,
      would_enter_no_route_rate: 0,
      would_enter_trapped_rate: 0,
      unknown_data_rate: 0,
      outlier_trimmed_would_rr: 3,
      missed_blockers: [],
      discovery_exit: { advisory: 'PROMOTE_TINY_CANARY' },
    },
  }, {
    generatedAt: '2026-06-04T01:00:00.000Z',
    nowMs: Date.parse('2026-06-04T01:00:00.000Z'),
    requestedHours: 24,
    materializedHours: 24,
    dbPath: '/tmp/paper.db',
    minClosedTrades: 20,
    minGoldSilverCandidates: 5,
    paperDbHealth: {
      available: true,
      status: 'paper_db_empty',
      size_bytes: 0,
      reason: 'paper_trades_db_zero_bytes',
    },
  });

  assert.equal(status.pass, false);
  assert.equal(status.available, false);
  assert.equal(status.status, 'evidence_unavailable');
  assert.match(status.evidence_blockers.join(','), /live_paper_db_empty/);
  assert.equal(status.live_paper_db_health.status, 'paper_db_empty');
});

test('a class status can be served from materialized live snapshot section', () => {
  const status = aClassStatusFromLiveSnapshot({
    snapshot_id: 'paper_live_8h_test',
    generated_at: '2026-05-21T00:00:00Z',
    window: { since_ts: 1000, since_iso: '2026-05-21T00:00:00Z' },
    a_class: {
      available: true,
      total: 12,
      would_enter: 2,
      enter: 0,
      action_summary: [
        { action: 'BLOCK', n: 10, avg_score: 0, would_enter_size_sol: 0 },
        { action: 'WOULD_ENTER', n: 2, avg_score: 88.126, would_enter_size_sol: 0.004 },
      ],
      grade_summary: [{ grade: 'STRONG_A', action: 'WOULD_ENTER', n: 2, avg_score: 88.126 }],
      source_summary: [{ source_table: 'source_resonance_candidates', source_component: 'source_resonance_shadow', action: 'WOULD_ENTER', n: 2, avg_score: 88.126, would_enter_size_sol: 0.004 }],
      reason_summary: [{ source_table: 'paper_decision_events', source_component: 'scout_quality', source_reason: 'scout_quality_volume_low', action: 'BLOCK', n: 10, max_score: 0 }],
      hard_blockers: [{ blocker: 'quote_not_available', n: 10 }],
      recent_events: [{ id: 1, action: 'WOULD_ENTER', symbol: 'DOG' }],
      runtime_safety: {
        available: true,
        loss_cap_breach_n: 1,
        mode_circuit_broken: true,
        downgraded_modes: [{ mode_key: 'A_CLASS_FASTLANE', status: 'CIRCUIT_BROKEN' }],
        next_safe_action: 'keep_breached_modes_shadow_until_cooldown',
      },
    },
    a_class_p0_discovery: {
      available: true,
      status: 'shadow_ready',
      quote_clean_gold_silver_seen_count: 4,
      quote_clean_gold_silver_would_enter_count: 3,
      would_enter_no_route_rate: 1 / 3,
      would_enter_trapped_rate: 0,
      unknown_data_rate: 0,
      outlier_trimmed_would_rr: 4.5,
      source_breakdown: { opportunity_events: 3, canonical_trade_ledger: 1 },
      source_component_breakdown: { source_resonance_shadow: 2, external_alpha_shadow: 1 },
      hydrate_outcome_breakdown: { skipped_source_budget: 1 },
      observed_hydrate_outcome_breakdown: { success: 4, skipped_source_budget: 1 },
      denominator_exclusion_breakdown: { eligible: 4, path_peak_missing: 2 },
      hydrate_outcome_exclusion_breakdown: { 'skipped_source_budget:eligible': 1 },
      unknown_reason_breakdown: { path_peak_missing: 2 },
      missed_blockers: [
        {
          token_ca: 'TOKEN_SHOULD_NOT_LEAK',
          route: 'ATH',
          component: 'matrix_gate',
          reject_reason: 'weak_matrix',
          unique_tokens: 1,
          gold_n: 1,
          silver_n: 0,
          max_adjusted_peak: 1.07,
        },
      ],
      discovery_exit: {
        advisory: 'PROMOTE_TINY_CANARY',
        advisory_only: true,
        requires_human_approval: true,
      },
    },
  }, {
    dbPath: '/tmp/paper.db',
    requestedHours: 7,
    materializedHours: 8,
    limit: 10,
  });

  assert.equal(status.materialized, true);
  assert.equal(status.live_query, false);
  assert.equal(status.materialized_snapshot_id, 'paper_live_8h_test');
  assert.equal(status.requested_window_hours, 7);
  assert.equal(status.materialized_window_hours, 8);
  assert.equal(status.would_enter, 2);
  assert.equal(status.action_summary[1].avg_score, 88.13);
  assert.equal(status.source_summary[0].would_enter_size_sol, 0.004);
  assert.equal(status.p0_discovery.quote_clean_gold_silver_seen_count, 4);
  assert.equal(status.p0_discovery.quote_clean_gold_silver_would_enter_count, 3);
  assert.equal(status.p0_discovery.would_enter_no_route_rate, 0.333333);
  assert.equal(status.p0_discovery.outlier_trimmed_would_rr, 4.5);
  assert.equal(status.p0_discovery.source_breakdown.opportunity_events, 3);
  assert.equal(status.p0_discovery.source_component_breakdown.source_resonance_shadow, 2);
  assert.equal(status.rr_summary.source_component_breakdown.external_alpha_shadow, 1);
  assert.equal(status.p0_discovery.hydrate_outcome_breakdown.skipped_source_budget, 1);
  assert.equal(status.rr_summary.denominator_exclusion_breakdown.path_peak_missing, 2);
  assert.equal(status.rr_summary.unknown_reason_breakdown.path_peak_missing, 2);
  assert.equal(status.p0_discovery.missed_blockers[0].token_ca, undefined);
  assert.doesNotMatch(JSON.stringify(status.p0_discovery.missed_blockers), /TOKEN_SHOULD_NOT_LEAK/);
  assert.equal(status.loss_cap_breach_n, 1);
  assert.equal(status.mode_circuit_broken, true);
  assert.equal(status.downgraded_modes[0].mode_key, 'A_CLASS_FASTLANE');
  assert.equal(status.next_safe_action, 'keep_breached_modes_shadow_until_cooldown');
});

test('a class materialized helpers degrade to shadow pending without p0 discovery', () => {
  const rolling = buildRolling24hGoalStatusFromLiveSnapshot({
    snapshot_id: 'paper_live_missing_p0',
    generated_at: '2026-06-04T00:50:00.000Z',
    window: { since_ts: 1000 },
    trades: { totals: { closed: 20, wins: 12, min_pnl: -0.1, deployed_sol: 0.05, est_pnl_sol: 0.11 } },
    dog_catch_goal: {
      available: true,
      trades: { closed: 20, captured_gold_silver_unique: 6, deployed_sol: 0.05, realized_pnl_sol: 0.11, realized_roi: 2.2 },
      missed: { clean_gold_silver_unique: 4 },
      goal: { eligible_gold_silver_unique: 10, captured_gold_silver_unique: 6, clean_gold_silver_capture_rate: 0.6, pass: true, blockers: [] },
    },
    a_class: { available: true, would_enter: 3, enter: 0 },
  }, {
    generatedAt: '2026-06-04T01:00:00.000Z',
    nowMs: Date.parse('2026-06-04T01:00:00.000Z'),
    requestedHours: 24,
    materializedHours: 24,
    dbPath: '/tmp/paper.db',
  });
  const status = aClassStatusFromLiveSnapshot({
    snapshot_id: 'paper_live_missing_p0',
    generated_at: '2026-06-04T00:50:00.000Z',
    a_class: { available: true, total: 1, would_enter: 1, enter: 0 },
  }, {
    dbPath: '/tmp/paper.db',
    requestedHours: 24,
    materializedHours: 24,
  });

  assert.equal(rolling.status, 'shadow_pending');
  assert.equal(rolling.available, false);
  assert.equal(rolling.shadow_pending, true);
  assert.match(rolling.evidence_blockers.join(','), /a_class_p0_shadow_discovery_pending/);
  assert.equal(status.status, 'shadow_pending');
  assert.equal(status.available, false);
  assert.equal(status.p0_discovery.reason, 'a_class_p0_discovery_materialized_section_missing');
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

test('a class block cause classifier separates infra market and policy blocks', () => {
  const routeInfra = classifyAClassBlocker('route_unavailable', {
    data_confidence: 'unknown',
    quote_source: '',
  });
  const routeMarket = classifyAClassBlocker('route_unavailable', {
    route_failure_reason: 'no_route',
    data_confidence: 'quote_clean',
    quote_source: 'jupiter',
  });
  const quoteInfra = classifyAClassBlocker('quote_not_executable', {
    data_confidence: 'unknown',
  });
  const policy = classifyAClassBlocker('expected_rr_below_2', {});
  const lowLiquidity = classifyAClassBlocker('liquidity_below_min', {});
  const entrapmentRed = classifyAClassBlocker('entrapment_red_flag', {});
  const bundlerRed = classifyAClassBlocker('bundler_red_flag', {});

  assert.equal(routeInfra.category, 'INFRA');
  assert.equal(routeInfra.recoverability, 'provider_or_evidence_recoverable');
  assert.equal(routeMarket.category, 'MARKET');
  assert.equal(quoteInfra.category, 'INFRA');
  assert.equal(policy.category, 'POLICY');
  assert.equal(lowLiquidity.category, 'MARKET');
  assert.equal(entrapmentRed.category, 'MARKET');
  assert.equal(bundlerRed.category, 'MARKET');

  const marketWins = classifyAClassBlockCause({
    action: 'BLOCK',
    token_ca: 'token-a',
    hard_blockers_json: JSON.stringify(['quote_not_available', 'creator_close']),
    risk_json: JSON.stringify({ data_confidence: 'unknown' }),
  });
  assert.equal(marketWins.category, 'MARKET');
  assert.equal(marketWins.infra_recoverable, false);

  const persistedWins = classifyAClassBlockCause({
    action: 'BLOCK',
    token_ca: 'token-persisted',
    block_cause: 'INFRA',
    recoverability: 'provider_or_evidence_recoverable',
    classification_reason: 'persisted_at_write_time',
    hard_blockers_json: JSON.stringify(['creator_close']),
    blocker_classifications_json: JSON.stringify([
      {
        blocker: 'quote_not_available',
        category: 'INFRA',
        recoverability: 'provider_or_evidence_recoverable',
        reason: 'quote_provider_or_freshness_missing',
      },
    ]),
  });
  assert.equal(persistedWins.category, 'INFRA');
  assert.equal(persistedWins.recoverability, 'provider_or_evidence_recoverable');
  assert.equal(persistedWins.infra_recoverable, true);

  const breakdown = buildAClassBlockCauseBreakdown([
    {
      id: 1,
      source_kind: 'a_class_decision_events',
      event_ts: 1000,
      token_ca: 'infra-token',
      symbol: 'INFRA',
      source_component: 'external_alpha_shadow',
      action: 'BLOCK',
      would_action: 'WOULD_ENTER',
      hard_blockers_json: JSON.stringify(['quote_not_available', 'quote_source_missing', 'route_unavailable']),
      risk_json: JSON.stringify({ data_confidence: 'unknown' }),
      hydrate_outcome: 'skipped_source_budget',
      quote_clean: 0,
    },
    {
      id: 2,
      source_kind: 'a_class_decision_events',
      event_ts: 1001,
      token_ca: 'market-token',
      symbol: 'MARKET',
      source_component: 'external_alpha_shadow',
      action: 'BLOCK',
      hard_blockers_json: JSON.stringify(['creator_close', 'quote_not_available']),
      risk_json: JSON.stringify({ data_confidence: 'unknown' }),
      hydrate_outcome: 'skipped_hard_market_red',
      quote_clean: 0,
    },
    {
      id: 3,
      source_kind: 'opportunity_events',
      event_ts: 1002,
      token_ca: 'policy-token',
      symbol: 'POLICY',
      source_component: 'matrix_evaluator',
      action: 'BLOCK',
      hard_blockers_json: JSON.stringify(['matrices not yet aligned']),
      would_enter_a_class: 0,
      hydrate_outcome: 'success',
      quote_clean: 1,
    },
  ], { limit: 10 });

  assert.equal(breakdown.total_events, 3);
  assert.equal(breakdown.infra_recoverable.events, 1);
  assert.equal(breakdown.infra_recoverable.would_enter_n, 1);
  assert.equal(breakdown.market_unexecutable.events, 1);
  assert.equal(breakdown.policy_guardrail.events, 1);
  assert.equal(breakdown.blocker_summary.find((row) => row.blocker === 'creator_close').category, 'MARKET');
  assert.equal(breakdown.source_component_summary.find((row) => row.category === 'INFRA').source_component, 'external_alpha_shadow');
  assert.equal(breakdown.hydrate_summary.find((row) => row.provider_hydrate_outcome === 'success').quote_clean_n, 1);
  assert.equal(breakdown.hydrate_summary.find((row) => row.provider_hydrate_outcome === 'skipped_source_budget').would_enter_n, 1);
  assert.equal(
    breakdown.hydrate_source_summary.find((row) => row.provider_hydrate_outcome === 'skipped_hard_market_red').source_component,
    'external_alpha_shadow',
  );
});

test('a class matrix and AI advisory helpers summarize shadow evidence safely', () => {
  const events = [
    {
      id: 1,
      symbol: 'DOG',
      action: 'WOULD_ENTER',
      grade: 'A',
      expected_rr: 3.2,
      matrix: {
        matrix_version: 'v1.a_class_18_cell',
        matrix_grade: 'A',
        source_strength: 'GREEN',
        execution_quality: 'GREEN',
        market_flow: 'YELLOW',
        security_cleanliness: 'GREEN',
        freshness_lifecycle: 'GREEN',
        historical_ev: 'YELLOW',
      },
      ai_review: {
        schema_version: 'v1.ai_strategy_advisory.shadow_only',
        ai_grade: 'supportive',
      },
    },
  ];
  const matrix = summarizeAClassMatrixEvents(events);
  const p0 = {
    available: true,
    quote_clean_gold_silver_seen_count: 12,
    quote_clean_gold_silver_would_enter_count: 6,
    outlier_trimmed_would_rr: 3.0,
    would_enter_no_route_rate: 0.01,
    would_enter_trapped_rate: 0,
    unknown_data_rate: 0,
    discovery_exit: { advisory: 'PROMOTE_TINY_CANARY', canary_size_sol: 0.001 },
    missed_blockers: [
      { route: 'ATH', component: 'scout', reject_reason: 'scout_quality_buy_pressure_weak', gold_n: 1, silver_n: 0, unique_tokens: 2, max_adjusted_peak: 1.2 },
      { route: 'LOTTO', component: 'security', reject_reason: 'creator_dump_security_red_flag', gold_n: 1, silver_n: 0, unique_tokens: 1, max_adjusted_peak: 2.0 },
    ],
  };
  const missed = buildMissedDogAiReviewFromP0(p0);
  const audit = buildCounterfactualAiAuditFromP0(p0);
  const controller = buildGoalControllerActions({
    rollingGoalStatus: { status: 'under_target' },
    p0Discovery: p0,
    counterfactualAudit: audit,
    missedDogReview: missed,
  });

  assert.equal(matrix.available, true);
  assert.equal(matrix.grade_counts.A, 1);
  assert.equal(missed.allow_a_class_only_count, 1);
  assert.equal(missed.keep_hard_block_count, 1);
  assert.equal(audit.pass, true);
  assert.equal(controller.can_trigger_trade, false);
  assert.equal(controller.next_safe_action, 'prepare_0_001_tiny_paper_after_observability_green');
});

test('rolling 24h goal exposes matrix rr ai and controller fields', () => {
  const snapshot = {
    snapshot_id: 'snap-ai',
    generated_at: '2026-06-04T00:00:00.000Z',
    window: { since_ts: 1000 },
    trades: {
      totals: {
        closed: 20,
        wins: 13,
        deployed_sol: 1,
        est_pnl_sol: 2.5,
        min_pnl: -0.1,
      },
    },
    dog_catch_goal: {
      available: true,
      goal: {
        eligible_gold_silver_unique: 10,
        captured_gold_silver_unique: 7,
      },
      trades: { captured_gold_silver_unique: 7 },
    },
    a_class_p0_discovery: {
      available: true,
      quote_clean_gold_silver_seen_count: 12,
      quote_clean_gold_silver_would_enter_count: 6,
      outlier_trimmed_would_rr: 3.0,
      would_enter_no_route_rate: 0.01,
      would_enter_trapped_rate: 0,
      unknown_data_rate: 0,
      missed_blockers: [],
      discovery_exit: { advisory: 'PROMOTE_TINY_CANARY', canary_size_sol: 0.001 },
    },
    a_class: {
      available: true,
      would_enter: 1,
      enter: 0,
      recent_events: [{
        id: 1,
        symbol: 'DOG',
        action: 'WOULD_ENTER',
        grade: 'A',
        expected_rr: 3,
        matrix: {
          matrix_version: 'v1.a_class_18_cell',
          matrix_grade: 'A',
          source_strength: 'GREEN',
          execution_quality: 'GREEN',
        },
      }],
    },
  };

  const status = buildRolling24hGoalStatusFromLiveSnapshot(snapshot, {
    generatedAt: '2026-06-04T00:05:00.000Z',
    nowMs: Date.parse('2026-06-04T00:05:00.000Z'),
    requestedHours: 24,
    materializedHours: 24,
  });

  assert.equal(status.matrix_summary.available, true);
  assert.equal(status.rr_summary.outlier_trimmed_would_rr, 3);
  assert.equal(status.ai_advisory.advisory_only, true);
  assert.equal(Array.isArray(status.controller_actions), true);
  assert.equal(status.next_safe_action, 'prepare_0_001_tiny_paper_after_observability_green');
});

test('rolling 24h goal surfaces runtime loss-cap circuit breaker', () => {
  const snapshot = {
    snapshot_id: 'paper_live_runtime_safety',
    generated_at: '2026-06-04T00:00:00.000Z',
    window: { since_ts: 1000 },
    trades: {
      totals: {
        closed: 20,
        wins: 12,
        min_pnl: -0.12,
        deployed_sol: 0.05,
        est_pnl_sol: 0.11,
      },
    },
    dog_catch_goal: {
      available: true,
      trades: {
        closed: 20,
        captured_gold_silver_unique: 6,
        deployed_sol: 0.05,
        realized_pnl_sol: 0.11,
        realized_roi: 2.2,
      },
      missed: { clean_gold_silver_unique: 4 },
      goal: {
        eligible_gold_silver_unique: 10,
        captured_gold_silver_unique: 6,
        clean_gold_silver_capture_rate: 0.6,
        pass: true,
        blockers: [],
      },
    },
    a_class_p0_discovery: {
      available: true,
      quote_clean_gold_silver_seen_count: 10,
      quote_clean_gold_silver_would_enter_count: 6,
      outlier_trimmed_would_rr: 3.0,
      would_enter_no_route_rate: 0,
      would_enter_trapped_rate: 0,
      unknown_data_rate: 0,
      missed_blockers: [],
      discovery_exit: { advisory: 'PROMOTE_TINY_CANARY' },
    },
    a_class: {
      available: true,
      runtime_safety: {
        available: true,
        loss_cap_breach_n: 1,
        mode_circuit_broken: true,
        downgraded_modes: [{ mode_key: 'A_CLASS_FASTLANE', status: 'CIRCUIT_BROKEN' }],
        next_safe_action: 'keep_breached_modes_shadow_until_cooldown',
      },
    },
  };

  const status = buildRolling24hGoalStatusFromLiveSnapshot(snapshot, {
    generatedAt: '2026-06-04T00:05:00.000Z',
    nowMs: Date.parse('2026-06-04T00:05:00.000Z'),
    requestedHours: 24,
    materializedHours: 24,
    minClosedTrades: 20,
    minGoldSilverCandidates: 5,
  });

  assert.equal(status.pass, false);
  assert.equal(status.metrics.loss_cap_breach_n, 1);
  assert.equal(status.metrics.mode_circuit_broken, true);
  assert.match(status.blockers.join(','), /a_class_mode_runtime_circuit_broken/);
  assert.equal(status.next_safe_action, 'keep_breached_modes_shadow_until_cooldown');
});
