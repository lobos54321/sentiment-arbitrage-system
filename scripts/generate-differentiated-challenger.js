#!/usr/bin/env node
import path from 'path';
import autonomyConfig from '../src/config/autonomy-config.js';
import { PaperStrategyRegistry } from '../src/config/paper-strategy-registry.js';
import ExperimentStore from '../src/database/experiment-store.js';
import StrategyResearchMemoryStore from '../src/database/strategy-research-memory-store.js';
import { ChallengerGenerator } from '../src/optimizer/challenger-generator.js';

const registry = new PaperStrategyRegistry();
const experimentStore = new ExperimentStore(autonomyConfig.dbPath);
const memoryStore = new StrategyResearchMemoryStore(autonomyConfig.dbPath);
const generator = new ChallengerGenerator({
  featureResearchPath: path.join(autonomyConfig.dataDir, 'feature-research-latest.json'),
  strategyDraftPath: path.join(autonomyConfig.dataDir, 'strategy-draft-latest.json'),
  gapAnalysisPath: path.join(autonomyConfig.dataDir, 'research-gap-analysis-latest.json')
});

const baseline = registry.getBaseline();
const candidate = generator.generate(baseline);
candidate.status = 'draft';
candidate.datasetRefs = [autonomyConfig.datasets.signalExport, autonomyConfig.datasets.localRecorder, autonomyConfig.datasets.paperTrades];

await registry.registerCandidate(candidate);
experimentStore.upsert(candidate);

memoryStore.recordFinding({
  memoryId: `challenger-generated-${candidate.id}`,
  memoryType: 'challenger-generation',
  title: `${candidate.id} 差异化 challenger 生成`,
  summary: `基于 feature research / strategy draft / research gap 自动生成 draft candidate，包含 ${candidate.mutationSet.length} 个差异化变更。`,
  scope: 'challenger-generator',
  strategyId: candidate.id,
  candidateId: candidate.id,
  evidence: {
    parentId: baseline.id,
    mutationSet: candidate.mutationSet,
    notes: candidate.notes
  },
  metrics: {
    mutationCount: candidate.mutationSet.length
  },
  tags: ['challenger', 'auto-generated', 'strategy-family', 'draft-candidate'],
  nextActions: ['由 autonomy daemon 统一决定是否激活为 challenger', '若效果仍无差异，则继续扩展研究缺口驱动的数据收集层']
});

console.log(JSON.stringify({
  ok: true,
  baselineId: baseline.id,
  candidateId: candidate.id,
  lifecycleStatus: candidate.status,
  mutationCount: candidate.mutationSet.length,
  mutationSet: candidate.mutationSet,
  notes: candidate.notes
}, null, 2));
