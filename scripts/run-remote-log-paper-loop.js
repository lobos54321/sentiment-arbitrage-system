#!/usr/bin/env node
import fs from 'fs';
import os from 'os';
import { spawnSync } from 'child_process';
import path from 'path';
import StrategyResearchMemoryStore from '../src/database/strategy-research-memory-store.js';
import autonomyConfig from '../src/config/autonomy-config.js';

const ROOT = '/Users/boliu/sentiment-arbitrage-system';
const syncScript = path.join(ROOT, 'scripts', 'sync-remote-premium-logs.js');
const heliusPoolSyncScript = path.join(ROOT, 'scripts', 'run-helius-pool-sync.js');
const pipelineScript = path.join(ROOT, 'scripts', 'run-paper-eval-pipeline.js');
const memoryScript = path.join(ROOT, 'scripts', 'record-strategy-research-memory.js');

function runStep(label, args, extraEnv = {}) {
  console.log(`\n=== ${label} ===`);
  return spawnSync('node', args, {
    cwd: ROOT,
    stdio: 'inherit',
    env: { ...process.env, ...extraEnv },
  });
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

  const coveredOnly = process.env.AUTONOMY_COVERED_ONLY_EVAL === 'true';
  const pipelineArgs = coveredOnly ? [pipelineScript, '--covered-only'] : [pipelineScript];
  const pipelineLabel = coveredOnly ? 'Run covered-only paper pipeline' : 'Run deep-research paper pipeline';

  const memoryContext = loadMemoryContext();
  console.log(`\n=== Loaded memory context (${memoryContext.total || memoryContext.highlights?.length || 0} entries) ===`);
  if (memoryContext.backlog?.length) {
    console.log(JSON.stringify({ backlog: memoryContext.backlog.slice(0, 5) }, null, 2));
  }

  const syncResult = runStep('Sync remote premium logs', [syncScript]);
  if (syncResult.status !== 0) {
    console.warn('\n[remote-log-paper-loop] Sync failed, falling back to cached remote-runtime.log');
  }

  if (process.env.HELIUS_API_KEY && process.env.AUTONOMY_SKIP_HELIUS_INCREMENTAL_SYNC !== 'true') {
    const heliusSyncResult = runStep('Run Helius incremental pool sync', [heliusPoolSyncScript]);
    if (heliusSyncResult.status !== 0) {
      console.warn('\n[remote-log-paper-loop] Helius incremental sync failed, continuing with existing cache/fallback coverage');
    }
  }

  const pipelineOutputPath = path.join(os.tmpdir(), `paper-eval-${Date.now()}.json`);
  try {
    const pipelineResult = runStep(pipelineLabel, pipelineArgs, {
      AUTONOMY_SKIP_EXPORT_SYNC: 'true',
      AUTONOMY_EVAL_OUTPUT_PATH: pipelineOutputPath
    });
    if (pipelineResult.status !== 0) {
      throw new Error(`${pipelineLabel} failed with exit code ${pipelineResult.status}`);
    }

    if (!fs.existsSync(pipelineOutputPath)) {
      throw new Error(`Pipeline evaluation output not found: ${pipelineOutputPath}`);
    }

    const memoryResult = runStep('Record strategy research memory', [memoryScript, '--from-json', pipelineOutputPath]);
    if (memoryResult.status !== 0) {
      throw new Error(`Record strategy research memory failed with exit code ${memoryResult.status}`);
    }
  } finally {
    try { fs.unlinkSync(pipelineOutputPath); } catch {}
  }
}

try {
  main();
} catch (error) {
  console.error(error.stack || error.message);
  process.exit(1);
}
