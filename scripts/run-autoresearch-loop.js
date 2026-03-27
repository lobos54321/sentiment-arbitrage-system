#!/usr/bin/env node
import crypto from 'crypto';
import autonomyConfig from '../src/config/autonomy-config.js';
import { PaperStrategyRegistry } from '../src/config/paper-strategy-registry.js';
import AutonomyRunStore from '../src/database/autonomy-run-store.js';
import ExperimentStore from '../src/database/experiment-store.js';
import StrategyResearchMemoryStore from '../src/database/strategy-research-memory-store.js';
import { AutoresearchLoop } from '../src/optimizer/autoresearch-loop.js';

const trigger = process.argv[2] || 'manual';
const runId = `autoresearch-${crypto.randomUUID()}`;
const registry = new PaperStrategyRegistry();
const experimentStore = new ExperimentStore(autonomyConfig.dbPath);
const runStore = new AutonomyRunStore(autonomyConfig.dbPath);
const memoryStore = new StrategyResearchMemoryStore(autonomyConfig.dbPath);
const loop = new AutoresearchLoop({ config: autonomyConfig, registry, experimentStore });

runStore.startRun({
  runId,
  startedAt: new Date().toISOString(),
  trigger,
  state: 'started',
  tasks: ['mutate_candidate', 'evaluate_candidate', 'apply_guardrails'],
  candidateIds: []
});

try {
  const result = await loop.runOnce({ trigger });
  runStore.finishRun(runId, {
    state: 'completed',
    stageName: 'autoresearch-loop',
    tasks: ['mutate_candidate', 'evaluate_candidate', 'apply_guardrails'],
    candidateIds: [result.candidate?.id].filter(Boolean),
    researchSummary: result.reason,
    promotionDecision: {
      kept: result.kept,
      reason: result.reason,
      recommendation: result.recommendation || null,
      candidateId: result.candidate?.id || null,
      familyUpdate: result.familyUpdate || null
    },
    machineSummary: {
      recommendation: result.recommendation || null,
      qualified: ['qualified_candidate', 'qualified_promotable'].includes(result.reason),
      promotable: result.reason === 'qualified_promotable'
    },
    errors: []
  });

  if (result.candidate && result.evaluation) {
    const memoryId = `autoresearch-${result.candidate.id}-${Date.now()}`;
    memoryStore.recordFinding({
      memoryId,
      memoryType: 'autoresearch-run',
      title: `${result.candidate.id} 自动研究回合`,
      summary: `trigger=${trigger}；kept=${result.kept}；reason=${result.reason}；recommendation=${result.recommendation || 'none'}`,
      scope: 'autoresearch-loop',
      strategyId: result.candidate.id,
      candidateId: result.candidate.id,
      sourceRunId: runId,
      evidence: {
        baselineMetrics: result.evaluation.baselineMetrics,
        candidateMetrics: result.evaluation.candidateMetrics,
        coverage: result.evaluation.coverage || {}
      },
      metrics: {
        kept: result.kept ? 1 : 0,
        comparisonToBaseline: result.evaluation.candidateMetrics?.comparisonToBaseline || 0,
        candidateExpectancy: result.evaluation.candidateMetrics?.expectancy || 0,
        candidateSampleSize: result.evaluation.candidateMetrics?.sampleSize || 0
      },
      tags: ['autoresearch', trigger, result.kept ? 'kept' : 'rejected', result.recommendation || 'none'],
      nextActions: result.kept
        ? [result.recommendation === 'promotion_review_ready' ? '由 daemon 统一执行 promotion review' : '由 daemon 决定是否激活为 challenger']
        : ['生成新 challenger，并扩大 covered sample 后继续评估']
    });
  }

  console.log(JSON.stringify({ runId, ...result }, null, 2));
} catch (error) {
  runStore.finishRun(runId, {
    state: 'failed',
    tasks: ['mutate_candidate', 'evaluate_candidate', 'apply_guardrails'],
    candidateIds: [],
    researchSummary: null,
    promotionDecision: null,
    errors: [error.message]
  });
  throw error;
}
