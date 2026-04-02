import { z } from 'zod';
import autonomyConfig from './autonomy-config.js';

const paperExecutionSchema = z.object({
  executionMode: z.enum(['parity']).default('parity'),
  entryPriceSource: z.enum(['quote']).default('quote'),
  exitPriceSource: z.enum(['quote']).default('quote'),
  paperUsesQuoteOnly: z.boolean().default(true),
  applyPaperPenalty: z.boolean().default(true),
  quoteTimeoutMs: z.number().int().min(1000).max(120000).default(25000),
  quoteRetries: z.number().int().min(1).max(20).default(5),
  maxQuoteAgeSec: z.number().int().min(1).max(3600).default(180),
  noRouteFailureThreshold: z.number().int().min(1).max(20).default(3),
  noRouteTrapMinutes: z.number().int().min(1).max(1440).default(15)
}).default({});

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
    trailStartPct: z.number().min(0).max(100).default(3),
    trailFactor: z.number().min(0).max(1).default(0.9),
    takeProfitPct: z.array(z.number().min(1).max(1000)).default([50, 100, 200]),
    timeoutMinutes: z.number().min(1).max(1440).default(30)
  }).default({}),
  sourceToggles: z.object({
    allowATH: z.boolean().default(true),
    allowNotAth: z.boolean().default(true),
    requireKlineConfirmation: z.boolean().default(false)
  }).default({}),
  signalFilters: z.object({
    aiConfidenceMin: z.number().min(0).max(100).default(0),
    holdersMin: z.number().min(0).max(1000000).default(0),
    top10PctPrimaryMin: z.number().min(0).max(100).default(0),
    top10PctPrimaryMax: z.number().min(0).max(100).default(100),
    top10PctSecondaryMin: z.number().min(0).max(100).default(0),
    top10PctSecondaryMax: z.number().min(0).max(100).default(100),
    excludeTop10PctAtOrBelow: z.number().min(0).max(100).default(0),
    allowAthOverride: z.boolean().default(false),
    primaryBandBonus: z.number().default(0),
    secondaryBandBonus: z.number().default(0)
  }).default({}),
  stageRules: z.object({
    stage1: z.object({
      enabled: z.boolean().default(true),
      universe: z.enum(['NOT_ATH']).default('NOT_ATH'),
      aiConfidenceMin: z.number().min(0).max(100).default(0),
      holdersMin: z.number().min(0).max(1000000).default(0),
      top10PctPrimaryMin: z.number().min(0).max(100).default(0),
      top10PctPrimaryMax: z.number().min(0).max(100).default(100),
      top10PctSecondaryMin: z.number().min(0).max(100).default(0),
      top10PctSecondaryMax: z.number().min(0).max(100).default(100),
      excludeTop10PctAtOrBelow: z.number().min(0).max(100).default(0),
      allowAthOverride: z.boolean().default(false)
    }).default({}),
    stage1Exit: z.object({
      stopLossPct: z.number().min(0).max(100).default(3),
      trailStartPct: z.number().min(0).max(100).default(2),
      trailFactor: z.number().min(0).max(1).default(0.9),
      timeoutMinutes: z.number().min(1).max(1440).default(120)
    }).default({}),
    stage2A: z.object({
      enabled: z.boolean().default(true),
      waitBarsAfterStop: z.number().min(0).max(20).default(3),
      reboundFromRollingLowPct: z.number().min(0).max(100).default(18),
      rollingLowBars: z.number().min(1).max(20).default(3),
      stopLossPct: z.number().min(0).max(100).default(4),
      trailStartPct: z.number().min(0).max(100).default(3),
      trailFactor: z.number().min(0).max(1).default(0.9),
      timeoutMinutes: z.number().min(1).max(1440).default(120)
    }).default({}),
    stage3: z.object({
      enabled: z.boolean().default(true),
      firstPeakMinPct: z.number().min(0).max(500).default(10),
      awakeningMinSuperIndex: z.number().min(0).max(1000).default(100),
      priceFloor: z.number().min(0).max(1).default(0.5),
      stopLossPct: z.number().min(0).max(100).default(4),
      trailStartPct: z.number().min(0).max(100).default(3),
      trailFactor: z.number().min(0).max(1).default(0.9),
      timeoutMinutes: z.number().min(1).max(1440).default(120)
    }).default({})
  }).default({}),
  paperRiskCaps: z.object({
    maxPositions: z.number().min(1).max(20).default(5),
    positionSizeSol: z.number().min(0.01).max(5).default(0.06)
  }).default({}),
  paperExecution: paperExecutionSchema
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
  previousStatus: z.string().nullable().optional().default(null),
  pausedAt: z.string().nullable().optional().default(null),
  pauseReason: z.string().nullable().optional().default(null),
  resumedAt: z.string().nullable().optional().default(null),
  resumeReason: z.string().nullable().optional().default(null),
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
    comparisonToBaseline: z.number().default(0),
    comparatorScore: z.number().default(0),
    evaluatedAt: z.string().nullable().optional().default(null)
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
