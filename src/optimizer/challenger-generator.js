import fs from 'fs';
import { validateCandidate } from '../config/strategy-candidate-schema.js';
import {
  SeededRng,
  createStrategyExperimentRandomnessControl,
} from './randomness-control.js';
import {
  STRATEGY_MUTATION_CATALOG,
  applyMutation,
  makeCandidateNotes,
} from './strategy-mutator.js';

function readJsonIfPresent(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function uniqueMutationSpecs(rng, count) {
  const remaining = [...STRATEGY_MUTATION_CATALOG];
  const selected = [];
  while (remaining.length && selected.length < count) {
    const index = rng.int(remaining.length);
    selected.push(remaining.splice(index, 1)[0]);
  }
  return selected;
}

function summarizeInputs({ featureResearch, strategyDraft, gapAnalysis, summary, researchSummary }) {
  return {
    featureResearchStatus: featureResearch?.status || null,
    strategyDraftStatus: strategyDraft?.status || null,
    gapAnalysisStatus: gapAnalysis?.status || null,
    summaryStatus: summary?.status || null,
    researchSummaryStatus: researchSummary?.status || null,
  };
}

export class ChallengerGenerator {
  constructor({
    featureResearchPath = null,
    strategyDraftPath = null,
    gapAnalysisPath = null,
  } = {}) {
    this.featureResearchPath = featureResearchPath;
    this.strategyDraftPath = strategyDraftPath;
    this.gapAnalysisPath = gapAnalysisPath;
  }

  generate(baseline, context = {}) {
    const createdAt = context.createdAt || new Date().toISOString();
    const featureResearch = readJsonIfPresent(this.featureResearchPath);
    const strategyDraft = readJsonIfPresent(this.strategyDraftPath);
    const gapAnalysis = readJsonIfPresent(this.gapAnalysisPath);
    const inputSummary = summarizeInputs({
      featureResearch,
      strategyDraft,
      gapAnalysis,
      summary: context.summary,
      researchSummary: context.researchSummary,
    });
    const randomnessControl = createStrategyExperimentRandomnessControl({
      parentId: baseline.id,
      createdAt,
      createdBy: context.createdBy || 'challenger-generator',
      source: 'challenger-generator',
      entropy: context.entropy,
    });
    const rng = new SeededRng(randomnessControl.rng_seed);
    let strategyConfig = baseline.strategyConfig;
    const mutationSet = [];

    for (const mutationSpec of uniqueMutationSpecs(rng, 3)) {
      const applied = applyMutation(strategyConfig, mutationSpec, rng);
      strategyConfig = applied.strategyConfig;
      mutationSet.push(applied.mutation);
    }

    return validateCandidate({
      id: randomnessControl.assignment_id,
      parentId: baseline.id,
      createdAt,
      createdBy: context.createdBy || 'challenger-generator',
      configVersion: Number(baseline.configVersion || 1) + 1,
      mutationSet,
      status: 'draft',
      datasetRefs: [],
      metrics: {},
      guardrailResults: {},
      notes: makeCandidateNotes({
        source: 'challenger-generator',
        randomnessControl,
        extra: {
          scoreMode: 'seeded-differentiated-fallback',
          inputSummary,
        },
      }),
      strategyConfig,
    });
  }
}

export default ChallengerGenerator;
