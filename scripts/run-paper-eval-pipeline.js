#!/usr/bin/env node
import fs from 'fs';
import crypto from 'crypto';
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

function hashObject(input) {
  return crypto.createHash('sha256').update(JSON.stringify(input)).digest('hex');
}

function determineReadiness({ evaluation, coveredDatasetRatio, baselineCoveredBuyCount }) {
  const coverageReady = coveredDatasetRatio >= 0.8;
  const baselineBuysReady = baselineCoveredBuyCount >= 30;
  return {
    evaluatorCoverage: {
      coveredDatasetRatio,
      threshold: 0.8,
      ready: coverageReady
    },
    baselineCoveredBuys: {
      count: baselineCoveredBuyCount,
      threshold: 30,
      ready: baselineBuysReady
    },
    stage3BacktestReady: coverageReady && baselineBuysReady,
    guidance: 'Resume Stage 3 only after evaluator coverage and baseline covered BUY thresholds are both green.'
  };
}

function determineReasonCode({ readiness, comparisonScore, candidateMetrics }) {
  if (!readiness.evaluatorCoverage.ready || !readiness.baselineCoveredBuys.ready) {
    return 'coverage_gap_detected';
  }
  if (comparisonScore > 0 && Number(candidateMetrics.expectancy || 0) > 0) {
    return 'promotion_review_ready';
  }
  return 'research_gap_detected';
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
    result.machine = {
      reasonCode: 'market_sync_failed',
      nextEventType: 'market_sync_requested'
    };
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
const errorBreakdownEntries = Object.entries(evaluation.coverage.errorBreakdown || {}).sort((a, b) => b[1] - a[1]);
const heliusBreakdownEntries = Object.entries(evaluation.coverage.heliusErrorBreakdown || {}).sort((a, b) => b[1] - a[1]);
const fallbackBreakdownEntries = Object.entries(evaluation.coverage.fallbackErrorBreakdown || {}).sort((a, b) => b[1] - a[1]);
const readiness = determineReadiness({ evaluation, coveredDatasetRatio, baselineCoveredBuyCount });
const comparisonScore = Number(evaluation.candidateMetrics?.comparisonToBaseline || 0);
const datasetFingerprint = hashObject({
  candidateId: candidate.id,
  datasetLimit,
  coveredOnly,
  cacheOnly,
  coverage: evaluation.coverage,
  baselineMetrics: evaluation.baselineMetrics,
  candidateMetrics: evaluation.candidateMetrics
});
const reasonCode = determineReasonCode({ readiness, comparisonScore, candidateMetrics: evaluation.candidateMetrics });

result.evaluation = { candidateId: candidate.id, coveredOnly, datasetLimit, cacheOnly, ...evaluation };
result.diagnostics = {
  note: 'Helius and fallback coverage errors are preserved for machine-driven event branching.',
  errorBreakdownSummary: errorBreakdownEntries,
  heliusErrorBreakdownSummary: heliusBreakdownEntries,
  fallbackErrorBreakdownSummary: fallbackBreakdownEntries,
  topBlockingCategory: errorBreakdownEntries[0] || null
};
result.readiness = readiness;
result.machine = {
  datasetFingerprint,
  readinessChanged: true,
  reasonCode,
  nextEventType: reasonCode,
  comparisonScore,
  candidateId: candidate.id,
  coverageGapDetected: reasonCode === 'coverage_gap_detected',
  researchGapDetected: reasonCode === 'research_gap_detected',
  promotionReviewReady: reasonCode === 'promotion_review_ready'
};
result.completedAt = new Date().toISOString();

if (process.env.AUTONOMY_EVAL_OUTPUT_PATH) {
  fs.writeFileSync(process.env.AUTONOMY_EVAL_OUTPUT_PATH, JSON.stringify(result, null, 2));
}

console.log(JSON.stringify(result, null, 2));
