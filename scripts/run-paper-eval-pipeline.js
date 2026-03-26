#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { spawn } from 'child_process';
import autonomyConfig from '../src/config/autonomy-config.js';
import { FixedEvaluator } from '../src/optimizer/fixed-evaluator.js';
import { PaperStrategyRegistry } from '../src/config/paper-strategy-registry.js';

function runNodeScript(scriptPath, args = [], extraEnv = {}) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [scriptPath, ...args], {
      cwd: process.cwd(),
      env: { ...process.env, ...extraEnv },
      stdio: ['ignore', 'pipe', 'pipe']
    });

    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => { stdout += chunk.toString(); process.stdout.write(chunk); });
    child.stderr.on('data', (chunk) => { stderr += chunk.toString(); process.stderr.write(chunk); });
    child.on('close', (code) => resolve({ ok: code === 0, code, stdout, stderr }));
    child.on('error', (error) => resolve({ ok: false, code: null, stdout, stderr: `${stderr}\n${error.message}`.trim() }));
  });
}

const args = process.argv.slice(2);
const coveredOnly = args.includes('--covered-only');
const positionalArgs = args.filter((arg) => !arg.startsWith('--'));
const candidateId = positionalArgs[0] || null;
const envBackfillLimit = process.env.AUTONOMY_BACKFILL_LIMIT;
const limit = positionalArgs[1] || envBackfillLimit || '100';
const skipInspect = process.env.AUTONOMY_SKIP_INSPECT_KLINE_COVERAGE === 'true';
const skipBackfill = process.env.AUTONOMY_SKIP_BACKFILL === 'true';
const result = {
  startedAt: new Date().toISOString(),
  steps: []
};

const exportExists = fs.existsSync(autonomyConfig.exports.filePath);
const skipExportSync = process.env.AUTONOMY_SKIP_EXPORT_SYNC === 'true';
if (!exportExists) {
  if (skipExportSync) {
    result.steps.push({ step: 'sync-zeabur-export', skipped: true, reason: 'autonomy_skip_export_sync_enabled' });
  } else if (process.env.DASHBOARD_TOKEN) {
    result.steps.push({ step: 'sync-zeabur-export', skipped: false });
    const syncResult = await runNodeScript(path.join(autonomyConfig.projectRoot, 'scripts', 'sync-zeabur-export.js'));
    result.steps[result.steps.length - 1] = { step: 'sync-zeabur-export', ...syncResult };
  } else {
    result.steps.push({ step: 'sync-zeabur-export', skipped: true, reason: 'missing_dashboard_token_using_local_dataset' });
  }
}

if (skipInspect) {
  result.steps.push({ step: 'inspect-kline-coverage', skipped: true, reason: 'autonomy_skip_inspect_kline_coverage_enabled' });
} else {
  const inspectResult = await runNodeScript(path.join(autonomyConfig.projectRoot, 'scripts', 'inspect-kline-coverage.js'));
  result.steps.push({ step: 'inspect-kline-coverage', ...inspectResult });
}

if (skipBackfill) {
  result.steps.push({ step: 'backfill-real-kline-data', skipped: true, reason: 'autonomy_skip_backfill_enabled', limit });
} else {
  const backfillResult = await runNodeScript(path.join(autonomyConfig.projectRoot, 'scripts', 'backfill-real-kline-data.js'), [limit]);
  result.steps.push({ step: 'backfill-real-kline-data', ...backfillResult, limit });
  if (!backfillResult.ok) {
    result.steps.push({ step: 'evaluation-skipped', reason: 'backfill_failed' });
    result.completedAt = new Date().toISOString();
    console.log(JSON.stringify(result, null, 2));
    process.exit(1);
  }
}

const registry = new PaperStrategyRegistry();
const candidate = candidateId ? registry.getCandidate(candidateId) : registry.getChallenger() || registry.getBaseline();
if (!candidate) {
  console.error('No candidate available for evaluation');
  process.exit(1);
}

const evaluator = new FixedEvaluator(autonomyConfig);
const datasetLimit = parseInt(process.env.AUTONOMY_EVAL_DATASET_LIMIT || '1000', 10);
const cacheOnly = process.env.AUTONOMY_CACHE_ONLY_EVAL === 'true';
const evaluation = await evaluator.evaluateCandidate(candidate, registry.getBaseline(), { coveredOnly, datasetLimit, cacheOnly });
evaluator.close();
const coveredDatasetRatio = evaluation.datasetSize ? evaluation.coverage.coveredDatasetSize / evaluation.datasetSize : 0;
const baselineCoveredBuyCount = Number(evaluation.coverage.baselineCoveredBuyCount || 0);
result.evaluation = { candidateId: candidate.id, coveredOnly, datasetLimit, cacheOnly, ...evaluation };
result.diagnostics = {
  note: 'When Helius backfill misses and Gecko/Dex fallback also fails, evaluation.error now preserves the Helius root cause while diagnostics expose both Helius and fallback errors at the per-signal level.'
};
result.readiness = {
  evaluatorCoverage: {
    coveredDatasetRatio,
    threshold: 0.8,
    ready: coveredDatasetRatio >= 0.8
  },
  baselineCoveredBuys: {
    count: baselineCoveredBuyCount,
    threshold: 30,
    ready: baselineCoveredBuyCount >= 30
  },
  stage3BacktestReady: coveredDatasetRatio >= 0.8 && baselineCoveredBuyCount >= 30,
  guidance: 'Resume the prior Stage 3 backtest only when both evaluator thresholds are met and sync readiness from run-helius-pool-sync is also green.'
};
result.completedAt = new Date().toISOString();

if (process.env.AUTONOMY_EVAL_OUTPUT_PATH) {
  fs.writeFileSync(process.env.AUTONOMY_EVAL_OUTPUT_PATH, JSON.stringify(result, null, 2));
}

console.log(JSON.stringify(result, null, 2));
