import { execFileSync } from 'child_process';

function blockedStatus(blocker, context) {
  return {
    schema_version: 'evaluator_snapshot_bundle_contract.v1',
    evidence_db: context.candidates.paper,
    evidence_databases: context.candidates,
    live_databases: context.live,
    evidence_manifest: context.manifestPath,
    snapshot_id: null,
    snapshot_ts: null,
    accepted: false,
    blockers: [blocker],
    promotion_allowed: false,
  };
}

export function runEvaluatorSnapshotPreflight(options) {
  const context = {
    candidates: options.candidates,
    live: options.live,
    manifestPath: options.manifestPath,
  };
  const args = [
    options.contractScript,
    '--signal-db', options.candidates.signal,
    '--paper-db', options.candidates.paper,
    '--raw-db', options.candidates.raw,
    '--kline-db', options.candidates.kline,
    '--data-dir', options.dataDir,
    '--manifest-path', options.manifestPath,
    '--max-age-sec', String(options.maxAgeSec),
    '--live-signal-db', options.live.signal,
    '--live-paper-db', options.live.paper,
    '--live-raw-db', options.live.raw,
    '--live-kline-db', options.live.kline,
  ];
  let raw;
  try {
    raw = (options.runner || execFileSync)(options.pythonBin || 'python3', args, {
      cwd: options.repoRoot,
      encoding: 'utf8',
      timeout: options.timeoutMs,
      maxBuffer: 8 * 1024 * 1024,
    });
  } catch (error) {
    const reason = error?.code === 'ETIMEDOUT'
      ? 'evaluator_snapshot_authoritative_preflight_timeout'
      : 'evaluator_snapshot_authoritative_preflight_failed';
    return blockedStatus(reason, context);
  }
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
    )
  ) {
    return blockedStatus('evaluator_snapshot_authoritative_preflight_invalid_contract', context);
  }
  return {
    ...status,
    evidence_db: options.candidates.paper,
    evidence_databases: options.candidates,
    live_databases: options.live,
    evidence_manifest: options.manifestPath,
    promotion_allowed: false,
  };
}
