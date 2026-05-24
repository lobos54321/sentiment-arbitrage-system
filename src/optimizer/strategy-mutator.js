import { strategyConfigSchema, validateCandidate } from '../config/strategy-candidate-schema.js';
import {
  SeededRng,
  createStrategyExperimentRandomnessControl,
  flattenRandomnessControl,
} from './randomness-control.js';

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function getPath(object, path) {
  return String(path).split('.').reduce((cursor, key) => cursor?.[key], object);
}

function setPath(object, path, value) {
  const parts = String(path).split('.');
  let cursor = object;
  for (const part of parts.slice(0, -1)) {
    cursor[part] = cursor[part] && typeof cursor[part] === 'object' ? cursor[part] : {};
    cursor = cursor[part];
  }
  cursor[parts[parts.length - 1]] = value;
}

function numberMutation(path, values, reason) {
  return {
    path,
    reason,
    nextValue(previousValue, rng) {
      const candidates = values.filter((value) => value !== previousValue);
      return rng.pick(candidates.length ? candidates : values);
    },
  };
}

function booleanMutation(path, reason) {
  return {
    path,
    reason,
    nextValue(previousValue) {
      return !Boolean(previousValue);
    },
  };
}

export const STRATEGY_MUTATION_CATALOG = [
  numberMutation('scoreThresholds.buy', [72, 75, 77, 80], 'seeded threshold perturbation'),
  numberMutation('scoreThresholds.watch', [55, 58, 60, 62, 65], 'seeded watch threshold perturbation'),
  numberMutation('entryTimingFilters.minSuperIndex', [70, 75, 80, 85, 90], 'seeded timing filter perturbation'),
  numberMutation('entryTimingFilters.minTradeDelta', [0, 1, 2, 3], 'seeded trade delta perturbation'),
  numberMutation('entryTimingFilters.maxChasePremiumPct', [12, 15, 20, 25], 'seeded chase premium perturbation'),
  numberMutation('paperExitRules.stopLossPct', [25, 30, 35, 38, 42], 'seeded stop loss perturbation'),
  numberMutation('paperExitRules.timeoutMinutes', [20, 30, 45, 60, 90], 'seeded timeout perturbation'),
  numberMutation('paperRiskCaps.maxPositions', [3, 4, 5, 6], 'seeded risk cap perturbation'),
  numberMutation('paperRiskCaps.positionSizeSol', [0.03, 0.04, 0.06, 0.08], 'seeded paper size perturbation'),
  booleanMutation('sourceToggles.requireKlineConfirmation', 'seeded confirmation toggle perturbation'),
];

export function applyMutation(strategyConfig, mutationSpec, rng) {
  const nextConfig = clone(strategyConfig);
  const previousValue = getPath(nextConfig, mutationSpec.path);
  const nextValue = mutationSpec.nextValue(previousValue, rng);
  setPath(nextConfig, mutationSpec.path, nextValue);
  return {
    strategyConfig: strategyConfigSchema.parse(nextConfig),
    mutation: {
      path: mutationSpec.path,
      previousValue,
      nextValue,
      reason: mutationSpec.reason,
    },
  };
}

export function makeCandidateNotes({ source, randomnessControl, extra = {} }) {
  return JSON.stringify({
    source,
    ...flattenRandomnessControl(randomnessControl),
    ...extra,
  });
}

export class StrategyMutator {
  mutate(baseline, options = {}) {
    const createdAt = options.createdAt || new Date().toISOString();
    const randomnessControl = createStrategyExperimentRandomnessControl({
      parentId: baseline.id,
      createdAt,
      createdBy: options.createdBy || 'strategy-mutator',
      source: 'strategy-mutator',
      entropy: options.entropy,
    });
    const rng = new SeededRng(randomnessControl.rng_seed);
    const mutationSpec = rng.pick(STRATEGY_MUTATION_CATALOG);
    const { strategyConfig, mutation } = applyMutation(baseline.strategyConfig, mutationSpec, rng);
    return validateCandidate({
      id: randomnessControl.assignment_id,
      parentId: baseline.id,
      createdAt,
      createdBy: options.createdBy || 'strategy-mutator',
      configVersion: Number(baseline.configVersion || 1) + 1,
      mutationSet: [mutation],
      status: 'draft',
      datasetRefs: [],
      metrics: {},
      guardrailResults: {},
      notes: makeCandidateNotes({
        source: 'strategy-mutator',
        randomnessControl,
      }),
      strategyConfig,
    });
  }
}

export default StrategyMutator;
