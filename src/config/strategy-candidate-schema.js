import { z } from 'zod';
import autonomyConfig from './autonomy-config.js';

export const strategyConfigSchema = z.object({
  sourceWeights: z.object({
    telegram: z.number().min(0).max(1).default(0.35),
    twitter: z.number().min(0).max(1).default(0.15),
    smartMoney: z.number().min(0).max(1).default(0.3),
    narrative: z.number().min(0).max(1).default(0.2)
  }).default({}),
  scoreThresholds: z.object({
    buy: z.number().min(0).max(100).default(75),
    watch: z.number().min(0).max(100).default(60),
    confidence: z.number().min(0).max(100).default(70)
  }).default({}),
  narrativeThresholds: z.object({
    minConfidence: z.number().min(0).max(100).default(65),
    minTierScore: z.number().min(0).max(100).default(60)
  }).default({}),
  entryTimingFilters: z.object({
    minSuperIndex: z.number().min(0).max(1000).default(80),
    minTradeDelta: z.number().min(0).max(100).default(1),
    minAddressIndex: z.number().min(0).max(100).default(3),
    minSecurityIndex: z.number().min(0).max(100).default(15),
    maxChasePremiumPct: z.number().min(0).max(200).default(20)
  }).default({}),
  cooldownWindows: z.object({
    sameSymbolMinutes: z.number().min(0).max(1440).default(15),
    postExitMinutes: z.number().min(0).max(1440).default(10)
  }).default({}),
  paperExitRules: z.object({
    stopLossPct: z.number().min(1).max(100).default(35),
    takeProfitPct: z.array(z.number().min(1).max(1000)).default([50, 100, 200]),
    timeoutMinutes: z.number().min(1).max(1440).default(30)
  }).default({}),
  sourceToggles: z.object({
    allowATH: z.boolean().default(true),
    allowNotAth: z.boolean().default(true),
    requireKlineConfirmation: z.boolean().default(false)
  }).default({}),
  paperRiskCaps: z.object({
    maxPositions: z.number().min(1).max(20).default(5),
    positionSizeSol: z.number().min(0.01).max(5).default(0.06)
  }).default({})
});

export const strategyCandidateSchema = z.object({
  id: z.string().min(1),
  parentId: z.string().nullable().default(null),
  createdAt: z.string().min(1),
  createdBy: z.string().min(1),
  configVersion: z.number().int().positive(),
  mutationSet: z.array(z.object({
    path: z.string().min(1),
    previousValue: z.any(),
    nextValue: z.any(),
    reason: z.string().optional()
  })).default([]),
  status: z.enum(['draft', 'qualified', 'active_challenger', 'promotable', 'promoted', 'rejected', 'paused_target_reached', 'evaluating', 'retired']).default('draft'),
  datasetRefs: z.array(z.string()).default([]),
  metrics: z.object({
    sampleSize: z.number().default(0),
    winRate: z.number().default(0),
    avgPnl: z.number().default(0),
    medianPnl: z.number().default(0),
    expectancy: z.number().default(0),
    profitFactor: z.number().default(0),
    maxDrawdown: z.number().default(0),
    tailLoss95: z.number().default(0),
    falsePositiveRate: z.number().default(0),
    missedGoldRate: z.number().default(0),
    sourceDiversity: z.number().default(0),
    holdingTimeMedian: z.number().default(0),
    comparisonToBaseline: z.number().default(0)
  }).default({}),
  guardrailResults: z.record(z.union([z.boolean(), z.number(), z.string(), z.null()])).default({}),
  notes: z.string().nullable().optional().default(null),
  strategyConfig: strategyConfigSchema
});

export function validateCandidate(candidate) {
  return strategyCandidateSchema.parse(candidate);
}

export function validateMutationSurface(strategyConfig) {
  const parsed = strategyConfigSchema.parse(strategyConfig);
  const forbidden = autonomyConfig.mutationSurface.forbiddenPaths;
  for (const key of Object.keys(parsed)) {
    if (!autonomyConfig.mutationSurface.allowedTopLevelFields.includes(key)) {
      throw new Error(`Field not in allowed mutation surface: ${key}`);
    }
    if (forbidden.some((prefix) => key.startsWith(prefix))) {
      throw new Error(`Forbidden mutation path: ${key}`);
    }
  }
  return parsed;
}

export default strategyCandidateSchema;
