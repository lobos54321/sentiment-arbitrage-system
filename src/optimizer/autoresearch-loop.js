import path from 'path';
import { StrategyMutator } from './strategy-mutator.js';
import { ChallengerGenerator } from './challenger-generator.js';
import { FixedEvaluator } from './fixed-evaluator.js';
import { ChampionChallengerComparator } from './champion-challenger.js';
import { PromotionGuardrails } from './promotion-guardrails.js';

export class AutoresearchLoop {
  constructor({ config, registry, experimentStore }) {
    this.config = config;
    this.registry = registry;
    this.experimentStore = experimentStore;
    this.mutator = new StrategyMutator();
    this.challengerGenerator = new ChallengerGenerator({
      featureResearchPath: path.join(this.config.dataDir, 'feature-research-latest.json'),
      strategyDraftPath: path.join(this.config.dataDir, 'strategy-draft-latest.json'),
      gapAnalysisPath: path.join(this.config.dataDir, 'research-gap-analysis-latest.json')
    });
    this.evaluator = new FixedEvaluator(config);
    this.comparator = new ChampionChallengerComparator();
    this.guardrails = new PromotionGuardrails(config);
  }

  async runOnce({ trigger = 'manual', researchSummary = null, summary = null } = {}) {
    const baseline = this.registry.getBaseline();
    const previousChallenger = this.registry.getChallenger();
    const generatorCandidate = this.challengerGenerator.generate(baseline, { summary, researchSummary });
    const fallbackCandidate = this.mutator.mutate(baseline);
    const candidate = (generatorCandidate.mutationSet?.length || 0) >= 3 ? generatorCandidate : fallbackCandidate;
    candidate.datasetRefs = [this.config.datasets.signalExport, this.config.datasets.localRecorder, this.config.datasets.paperTrades];
    candidate.notes = JSON.stringify({
      trigger,
      researchSummary,
      summary,
      generatorUsed: candidate.id === generatorCandidate.id,
      previousChallengerId: previousChallenger?.id || null
    });
    candidate.status = 'evaluating';
    this.experimentStore.upsert(candidate);

    const evaluation = await this.evaluator.evaluateCandidate(candidate, baseline, {
      coveredOnly: process.env.AUTONOMY_COVERED_ONLY_EVAL === 'true',
      datasetLimit: parseInt(
        process.env.AUTONOMY_DEEP_EVAL_DATASET_LIMIT
          || process.env.AUTONOMY_EVAL_DATASET_LIMIT
          || '1000',
        10
      )
    });
    candidate.metrics = evaluation.candidateMetrics;
    const comparison = this.comparator.compare(evaluation.baselineMetrics, evaluation.candidateMetrics);
    candidate.metrics.comparisonToBaseline = comparison.score;

    const guardrailResult = this.guardrails.evaluate(candidate.metrics);
    candidate.guardrailResults = { ...guardrailResult.results, comparisonScore: comparison.score };

    const qualified = comparison.better && guardrailResult.passed;
    const promotable = qualified
      && comparison.score >= this.config.promotion.promotableMinScore
      && Number(candidate.metrics.expectancy || 0) >= this.config.promotion.promotableMinExpectancy
      && Number(candidate.metrics.winRate || 0) >= this.config.promotion.promotableMinWinRate;

    if (qualified) {
      candidate.status = promotable ? 'promotable' : 'qualified';
      candidate.qualifiedAt = new Date().toISOString();
      this.experimentStore.upsert(candidate);
      await this.registry.registerCandidate(candidate);
      await this.registry.markCandidateStatus(candidate.id, candidate.status, {
        trigger,
        comparisonScore: comparison.score,
        promotable
      });
      return {
        kept: true,
        candidate,
        evaluation,
        reason: promotable ? 'qualified_promotable' : 'qualified_candidate',
        recommendation: promotable ? 'promotion_review_ready' : 'activate_challenger_review',
        familyUpdate: {
          baselineId: baseline.id,
          previousChallengerId: previousChallenger?.id || null,
          activeChallengerId: previousChallenger?.id || null,
          recommendedCandidateId: candidate.id,
          action: promotable ? 'promotion_review' : 'qualify_candidate'
        }
      };
    }

    candidate.status = 'rejected';
    candidate.retiredAt = new Date().toISOString();
    this.experimentStore.upsert(candidate);
    await this.registry.registerCandidate(candidate);
    return {
      kept: false,
      candidate,
      evaluation,
      reason: comparison.better ? 'guardrail_failed' : 'not_better',
      recommendation: 'research_gap_review',
      familyUpdate: {
        baselineId: baseline.id,
        previousChallengerId: previousChallenger?.id || null,
        activeChallengerId: this.registry.getChallenger()?.id || null,
        recommendedCandidateId: null,
        action: 'reject_candidate'
      }
    };
  }
}

export default AutoresearchLoop;
