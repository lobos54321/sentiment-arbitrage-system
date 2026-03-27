#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { spawnSync } from 'child_process';
import StrategyResearchMemoryStore from '../src/database/strategy-research-memory-store.js';
import autonomyConfig from '../src/config/autonomy-config.js';

const ROOT = autonomyConfig.projectRoot;
const syncScript = path.join(ROOT, 'scripts', 'sync-remote-premium-logs.js');
const heliusPoolSyncScript = path.join(ROOT, 'scripts', 'run-helius-pool-sync.js');

function runStep(label, args, extraEnv = {}) {
  const result = spawnSync('node', args, {
    cwd: ROOT,
    env: { ...process.env, ...extraEnv },
    encoding: 'utf8'
  });

  let parsed = null;
  const stdout = String(result.stdout || '').trim();
  try {
    parsed = stdout ? JSON.parse(stdout) : null;
  } catch {}

  return {
    step: label,
    ok: result.status === 0,
    code: result.status,
    stdout,
    stderr: String(result.stderr || '').trim(),
    output: parsed
  };
}

function loadMemoryContext() {
  const explicitPath = process.env.AUTONOMY_MEMORY_CONTEXT_PATH;
  const fallbackPath = path.join(autonomyConfig.dataDir, 'strategy-memory-context.json');
  const targetPath = explicitPath || fallbackPath;

  if (fs.existsSync(targetPath)) {
    return JSON.parse(fs.readFileSync(targetPath, 'utf8'));
  }

  const store = new StrategyResearchMemoryStore(autonomyConfig.dbPath);
  const recent = store.listRecent(5);
  return {
    generatedAt: new Date().toISOString(),
    total: recent.length,
    highlights: recent.map((item) => ({
      title: item.title,
      summary: item.summary,
      nextActions: item.nextActions,
      tags: item.tags,
      updatedAt: item.updatedAt
    }))
  };
}

function main() {
  if (!process.env.DASHBOARD_TOKEN) {
    throw new Error('DASHBOARD_TOKEN is required');
  }

  const memoryContext = loadMemoryContext();
  const summary = {
    startedAt: new Date().toISOString(),
    memoryHighlights: memoryContext.total || memoryContext.highlights?.length || 0,
    steps: []
  };

  const syncResult = runStep('sync-remote-premium-logs', [syncScript]);
  summary.steps.push(syncResult);
  const remoteSync = syncResult.output || {};
  const hasRemoteChanges = Boolean((remoteSync.inserted || 0) > 0 || (remoteSync.updated || 0) > 0);

  let heliusResult = null;
  if (process.env.HELIUS_API_KEY && process.env.AUTONOMY_SKIP_HELIUS_INCREMENTAL_SYNC !== 'true') {
    heliusResult = runStep('run-helius-pool-sync', [heliusPoolSyncScript]);
    summary.steps.push(heliusResult);
  }

  summary.remoteSync = {
    ok: syncResult.ok,
    parserVersion: remoteSync.parserVersion || null,
    downloadedBytes: remoteSync.downloadedBytes || 0,
    parsedSignals: remoteSync.parsedSignals || 0,
    inserted: remoteSync.inserted || 0,
    updated: remoteSync.updated || 0,
    skipped: remoteSync.skipped || 0,
    latest: remoteSync.latest || null,
    changed: hasRemoteChanges,
    reasonCode: hasRemoteChanges ? 'remote_logs_changed' : 'remote_logs_unchanged'
  };

  summary.marketSync = heliusResult ? {
    ok: heliusResult.ok,
    readiness: heliusResult.output?.readiness || null,
    processed: heliusResult.output?.processed || 0,
    trackedCandidates: heliusResult.output?.trackedCandidates || 0,
    reasonCode: heliusResult.ok ? 'market_sync_completed' : 'market_sync_failed'
  } : {
    skipped: true,
    reasonCode: process.env.AUTONOMY_SKIP_HELIUS_INCREMENTAL_SYNC === 'true' ? 'market_sync_skipped_env' : 'market_sync_skipped_no_helius'
  };

  summary.machine = {
    hasRemoteChanges,
    shouldRunPaperEval: hasRemoteChanges,
    nextEventType: hasRemoteChanges ? 'paper_eval_requested' : 'remote_logs_unchanged',
    reasonCode: hasRemoteChanges ? 'new_remote_input_detected' : 'no_new_remote_input'
  };
  summary.completedAt = new Date().toISOString();

  console.log(JSON.stringify(summary, null, 2));
}

try {
  main();
} catch (error) {
  console.error(error.stack || error.message);
  process.exit(1);
}
