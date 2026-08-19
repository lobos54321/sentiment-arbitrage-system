import { execFile, execFileSync } from 'child_process';

function blockedStatus(blocker, context) {
  return {
    schema_version: 'evaluator_snapshot_bundle_contract.v1',
    evidence_db: context.candidates.paper,
    evidence_databases: context.candidates,
    live_databases: context.live,
    evidence_manifest: context.manifestPath,
    producer_status_path: context.producerStatusPath,
    snapshot_id: null,
    snapshot_ts: null,
    accepted: false,
    blockers: [blocker],
    promotion_allowed: false,
  };
}

function preflightInvocation(options) {
  const context = {
    candidates: options.candidates,
    live: options.live,
    manifestPath: options.manifestPath,
    producerStatusPath: options.producerStatusPath || null,
  };
  const args = [
    options.contractScript,
    '--signal-db', options.candidates.signal,
    '--paper-db', options.candidates.paper,
    '--raw-db', options.candidates.raw,
    '--kline-db', options.candidates.kline,
    '--data-dir', options.dataDir,
    '--manifest-path', options.manifestPath,
    ...(options.producerStatusPath
      ? ['--producer-status-path', options.producerStatusPath]
      : []),
    '--max-age-sec', String(options.maxAgeSec),
    '--live-signal-db', options.live.signal,
    '--live-paper-db', options.live.paper,
    '--live-raw-db', options.live.raw,
    '--live-kline-db', options.live.kline,
  ];
  const execOptions = {
    cwd: options.repoRoot,
    encoding: 'utf8',
    timeout: options.timeoutMs,
    maxBuffer: 8 * 1024 * 1024,
  };
  return { context, args, execOptions };
}

function preflightFailureBlocker(error) {
  const code = String(error?.code || '').toUpperCase();
  const signal = String(error?.signal || '').toUpperCase();
  const message = String(error?.message || '').toLowerCase();
  const timedOut = code === 'ETIMEDOUT'
    || (error?.killed === true && signal === 'SIGTERM')
    || message.includes('timed out')
    || message.includes('timeout');
  return timedOut
    ? 'evaluator_snapshot_authoritative_preflight_timeout'
    : 'evaluator_snapshot_authoritative_preflight_failed';
}

function normalizePreflightOutput(raw, context) {
  let status;
  try {
    status = JSON.parse(String(raw || ''));
  } catch {
    return blockedStatus('evaluator_snapshot_authoritative_preflight_invalid_json', context);
  }
  if (!status || typeof status !== 'object' || Array.isArray(status)) {
    return blockedStatus('evaluator_snapshot_authoritative_preflight_invalid_structure', context);
  }
  if (
    status.schema_version !== 'evaluator_snapshot_bundle_contract.v1'
    || typeof status.accepted !== 'boolean'
    || !Array.isArray(status.blockers)
    || !status.blockers.every((value) => typeof value === 'string')
    || status.promotion_allowed !== false
    || status.accepted !== (status.blockers.length === 0)
  ) {
    return blockedStatus('evaluator_snapshot_authoritative_preflight_invalid_contract', context);
  }
  if (
    status.accepted
    && (
      typeof status.snapshot_id !== 'string'
      || status.snapshot_id.length === 0
      || !Number.isFinite(Number(status.snapshot_ts))
      || Number(status.snapshot_ts) <= 0
      || typeof status.manifest_path !== 'string'
      || status.manifest_path.length === 0
      || typeof status.manifest_sha256 !== 'string'
      || !/^[a-f0-9]{64}$/i.test(status.manifest_sha256)
      || typeof status.producer_status_path !== 'string'
      || status.producer_status_path.length === 0
      || status.producer_status_schema_version !== 'cross_db_evaluator_snapshot_worker_status.v1'
      || !status.producer_status
      || typeof status.producer_status !== 'object'
      || status.producer_status.last_accepted_snapshot?.snapshot_id !== status.snapshot_id
      || status.producer_status.last_accepted_snapshot?.manifest_sha256 !== status.manifest_sha256
    )
  ) {
    return blockedStatus('evaluator_snapshot_authoritative_preflight_invalid_contract', context);
  }
  const authoritativeDatabases = status.databases && typeof status.databases === 'object'
    ? status.databases
    : context.candidates;
  return {
    ...status,
    evidence_db: authoritativeDatabases.paper,
    evidence_databases: authoritativeDatabases,
    live_databases: context.live,
    evidence_manifest: status.manifest_path || context.manifestPath,
    promotion_allowed: false,
  };
}

export function runEvaluatorSnapshotPreflight(options) {
  const { context, args, execOptions } = preflightInvocation(options);
  let raw;
  try {
    raw = (options.runner || execFileSync)(options.pythonBin || 'python3', args, execOptions);
  } catch (error) {
    return blockedStatus(preflightFailureBlocker(error), context);
  }
  return normalizePreflightOutput(raw, context);
}

function execFileAsync(command, args, options) {
  return new Promise((resolve, reject) => {
    execFile(command, args, options, (error, stdout) => {
      if (error) {
        reject(error);
        return;
      }
      resolve(stdout);
    });
  });
}

export async function runEvaluatorSnapshotPreflightAsync(options) {
  const { context, args, execOptions } = preflightInvocation(options);
  let raw;
  try {
    raw = options.runner
      ? await options.runner(options.pythonBin || 'python3', args, execOptions)
      : await execFileAsync(options.pythonBin || 'python3', args, execOptions);
  } catch (error) {
    return blockedStatus(preflightFailureBlocker(error), context);
  }
  return normalizePreflightOutput(raw, context);
}

export function evaluatorSnapshotProvenance(status = {}) {
  const verified = status?.verified_integrity && typeof status.verified_integrity === 'object'
    ? status.verified_integrity
    : {};
  const databases = status?.databases && typeof status.databases === 'object'
    ? status.databases
    : status?.evidence_databases && typeof status.evidence_databases === 'object'
      ? status.evidence_databases
      : {};
  const databaseEvidence = {};
  for (const name of ['signal', 'paper', 'raw', 'kline']) {
    const integrity = verified[name] && typeof verified[name] === 'object' ? verified[name] : {};
    databaseEvidence[name] = {
      path: databases[name] || null,
      sha256: integrity.sha256 || null,
      sha256_matches_manifest: integrity.sha256_matches_manifest === true,
      quick_check: Array.isArray(integrity.quick_check) ? integrity.quick_check : [],
    };
  }
  return {
    schema_version: 'evaluator_snapshot_provenance.v1',
    consumer_verified_at: new Date().toISOString(),
    contract_schema_version: status.schema_version || null,
    accepted: status.accepted === true,
    snapshot_id: status.snapshot_id || null,
    snapshot_ts: Number.isFinite(Number(status.snapshot_ts)) ? Number(status.snapshot_ts) : null,
    snapshot_age_sec: Number.isFinite(Number(status.snapshot_age_sec)) ? Number(status.snapshot_age_sec) : null,
    max_snapshot_age_sec: Number.isFinite(Number(status.max_snapshot_age_sec)) ? Number(status.max_snapshot_age_sec) : null,
    git_commit: status.git_commit || null,
    manifest_path: status.manifest_path || status.evidence_manifest || null,
    manifest_sha256: status.manifest_sha256 || null,
    producer_status_path: status.producer_status_path || null,
    producer_status_schema_version: status.producer_status_schema_version || null,
    producer_manifest_sha256: status.producer_status?.last_accepted_snapshot?.manifest_sha256 || null,
    databases: databaseEvidence,
    blockers: Array.isArray(status.blockers) ? status.blockers.map(String) : [],
    promotion_allowed: false,
    strategy_change_allowed: false,
    automatic_runtime_change_allowed: false,
    paper_enablement_allowed: false,
  };
}
