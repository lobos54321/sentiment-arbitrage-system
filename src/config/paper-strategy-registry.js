import fs from 'fs';
import path from 'path';
import { atomicWriteJSON } from '../utils/atomic-write.js';
import { strategyConfigSchema, validateCandidate } from './strategy-candidate-schema.js';

const registryPath = path.join(process.cwd(), 'data', 'paper-strategy-registry.json');

const defaultStrategyConfig = strategyConfigSchema.parse({
  sourceWeights: { telegram: 0.35, twitter: 0.1, smartMoney: 0.35, narrative: 0.2 },
  scoreThresholds: { buy: 75, watch: 60, confidence: 70 },
  narrativeThresholds: { minConfidence: 65, minTierScore: 60 },
  entryTimingFilters: { minSuperIndex: 80, minTradeDelta: 1, minAddressIndex: 3, minSecurityIndex: 15, maxChasePremiumPct: 20 },
  cooldownWindows: { sameSymbolMinutes: 15, postExitMinutes: 10 },
  paperExitRules: { stopLossPct: 35, trailStartPct: 3, trailFactor: 0.9, takeProfitPct: [50, 100, 200], timeoutMinutes: 30 },
  sourceToggles: { allowATH: true, allowNotAth: true, requireKlineConfirmation: false },
  signalFilters: {
    aiConfidenceMin: 0,
    holdersMin: 0,
    top10PctPrimaryMin: 0,
    top10PctPrimaryMax: 100,
    top10PctSecondaryMin: 0,
    top10PctSecondaryMax: 100,
    excludeTop10PctAtOrBelow: 0,
    allowAthOverride: false,
    primaryBandBonus: 0,
    secondaryBandBonus: 0
  },
  paperRiskCaps: { maxPositions: 5, positionSizeSol: 0.06 }
});

function makeBaselineCandidate() {
  return {
    id: 'baseline-v1',
    parentId: null,
    createdAt: new Date().toISOString(),
    createdBy: 'system',
    configVersion: 1,
    mutationSet: [],
    status: 'promoted',
    datasetRefs: [],
    metrics: {
      sampleSize: 0,
      winRate: 0,
      avgPnl: 0,
      medianPnl: 0,
      expectancy: 0,
      profitFactor: 0,
      maxDrawdown: 0,
      tailLoss95: 0,
      falsePositiveRate: 0,
      missedGoldRate: 0,
      sourceDiversity: 0,
      holdingTimeMedian: 0,
      comparisonToBaseline: 0,
      comparatorScore: 0
    },
    guardrailResults: {},
    strategyConfig: defaultStrategyConfig
  };
}

function makeSelectiveNotAthCandidate() {
  return {
    id: 'notath-selective-v1',
    parentId: 'baseline-v1',
    createdAt: new Date().toISOString(),
    createdBy: 'system',
    configVersion: 2,
    mutationSet: [
      { path: 'signalFilters.aiConfidenceMin', previousValue: 0, nextValue: 60, reason: 'tighten selection to higher-confidence NOT_ATH signals' },
      { path: 'signalFilters.holdersMin', previousValue: 0, nextValue: 100, reason: 'prefer stronger holder base' },
      { path: 'signalFilters.top10PctPrimaryMin', previousValue: 0, nextValue: 20, reason: 'select the stronger top10 concentration band' },
      { path: 'signalFilters.top10PctPrimaryMax', previousValue: 100, nextValue: 25, reason: 'target the strongest observed top10 range' },
      { path: 'signalFilters.top10PctSecondaryMin', previousValue: 0, nextValue: 25, reason: 'permit a secondary band below the primary band' },
      { path: 'signalFilters.top10PctSecondaryMax', previousValue: 100, nextValue: 30, reason: 'allow the secondary band as a lower-ranked fallback' },
      { path: 'signalFilters.excludeTop10PctAtOrBelow', previousValue: 0, nextValue: 20, reason: 'exclude materially weaker concentration cases' },
      { path: 'sourceToggles.allowATH', previousValue: true, nextValue: false, reason: 'keep the new candidate NOT_ATH-only' },
      { path: 'stageRules.stage1.aiConfidenceMin', previousValue: 0, nextValue: 60, reason: 'stage 1 entry floor' },
      { path: 'stageRules.stage1.holdersMin', previousValue: 0, nextValue: 100, reason: 'stage 1 holder floor' },
      { path: 'stageRules.stage1.top10PctPrimaryMin', previousValue: 0, nextValue: 20, reason: 'stage 1 primary band lower bound' },
      { path: 'stageRules.stage1.top10PctPrimaryMax', previousValue: 100, nextValue: 25, reason: 'stage 1 primary band upper bound' },
      { path: 'stageRules.stage1.top10PctSecondaryMin', previousValue: 0, nextValue: 25, reason: 'stage 1 secondary band lower bound' },
      { path: 'stageRules.stage1.top10PctSecondaryMax', previousValue: 100, nextValue: 30, reason: 'stage 1 secondary band upper bound' },
      { path: 'stageRules.stage1.excludeTop10PctAtOrBelow', previousValue: 0, nextValue: 20, reason: 'stage 1 exclude weak top10 buckets' },
      { path: 'stageRules.stage1.allowAthOverride', previousValue: false, nextValue: false, reason: 'explicitly keep ATH override disabled' },
      { path: 'stageRules.stage1Exit.stopLossPct', previousValue: 35, nextValue: 3, reason: 'stage 1 exit stop-loss' },
      { path: 'stageRules.stage1Exit.trailStartPct', previousValue: 3, nextValue: 2, reason: 'stage 1 trailing activation' },
      { path: 'stageRules.stage1Exit.trailFactor', previousValue: 0.9, nextValue: 0.9, reason: 'keep trail factor unchanged' },
      { path: 'stageRules.stage1Exit.timeoutMinutes', previousValue: 30, nextValue: 120, reason: 'stage 1 timeout' },
      { path: 'stageRules.stage2A.enabled', previousValue: true, nextValue: true, reason: 'enable loser recovery re-entry' },
      { path: 'stageRules.stage2A.waitBarsAfterStop', previousValue: 3, nextValue: 3, reason: 'wait 3 bars after stop-loss' },
      { path: 'stageRules.stage2A.reboundFromRollingLowPct', previousValue: 18, nextValue: 18, reason: 'require +18% rebound from rolling low' },
      { path: 'stageRules.stage2A.rollingLowBars', previousValue: 3, nextValue: 3, reason: 'use post-stop rolling low window' },
      { path: 'stageRules.stage2A.entryPriceMode', previousValue: 'close', nextValue: 'close', reason: 'enter stage 2A on candle close' },
      { path: 'stageRules.stage2A.stopLossPct', previousValue: 4, nextValue: 4, reason: 'stage 2A stop-loss' },
      { path: 'stageRules.stage2A.trailStartPct', previousValue: 3, nextValue: 3, reason: 'stage 2A trailing activation' },
      { path: 'stageRules.stage2A.trailFactor', previousValue: 0.9, nextValue: 0.9, reason: 'stage 2A trailing factor' },
      { path: 'stageRules.stage2A.timeoutMinutes', previousValue: 120, nextValue: 120, reason: 'stage 2A timeout' },
      { path: 'stageRules.stage3.enabled', previousValue: false, nextValue: true, reason: 'enable continuation re-entry' },
      { path: 'stageRules.stage3.waitBarsFromSignal', previousValue: 0, nextValue: 30, reason: 'wait 30 bars from original signal' },
      { path: 'stageRules.stage3.firstPeakMinPct', previousValue: 0, nextValue: 10, reason: 'require initial +10% peak before stage 3' },
      { path: 'stageRules.stage3.entryPriceMode', previousValue: 'close', nextValue: 'close', reason: 'enter stage 3 on candle close' },
      { path: 'stageRules.stage3.stopLossPct', previousValue: 4, nextValue: 4, reason: 'stage 3 stop-loss' },
      { path: 'stageRules.stage3.trailStartPct', previousValue: 3, nextValue: 3, reason: 'stage 3 trailing activation' },
      { path: 'stageRules.stage3.trailFactor', previousValue: 0.9, nextValue: 0.9, reason: 'stage 3 trailing factor' },
      { path: 'stageRules.stage3.timeoutMinutes', previousValue: 120, nextValue: 120, reason: 'stage 3 timeout' }
    ],
    status: 'draft',
    datasetRefs: [],
    metrics: {
      sampleSize: 0,
      winRate: 0,
      avgPnl: 0,
      medianPnl: 0,
      expectancy: 0,
      profitFactor: 0,
      maxDrawdown: 0,
      tailLoss95: 0,
      falsePositiveRate: 0,
      missedGoldRate: 0,
      sourceDiversity: 0,
      holdingTimeMedian: 0,
      comparisonToBaseline: 0,
      comparatorScore: 0
    },
    guardrailResults: {},
    notes: 'Selective NOT_ATH candidate with two-stage loser recovery',
    strategyConfig: strategyConfigSchema.parse({
      ...defaultStrategyConfig,
      sourceToggles: { ...defaultStrategyConfig.sourceToggles, allowATH: false, allowNotAth: true },
      signalFilters: {
        aiConfidenceMin: 60,
        holdersMin: 100,
        top10PctPrimaryMin: 20,
        top10PctPrimaryMax: 25,
        top10PctSecondaryMin: 25,
        top10PctSecondaryMax: 30,
        excludeTop10PctAtOrBelow: 20,
        allowAthOverride: false,
        primaryBandBonus: 10,
        secondaryBandBonus: 4
      },
      stageRules: {
        stage1: {
          enabled: true,
          universe: 'NOT_ATH',
          aiConfidenceMin: 60,
          holdersMin: 100,
          top10PctPrimaryMin: 20,
          top10PctPrimaryMax: 25,
          top10PctSecondaryMin: 25,
          top10PctSecondaryMax: 30,
          excludeTop10PctAtOrBelow: 20,
          allowAthOverride: false
        },
        stage1Exit: {
          stopLossPct: 3,
          trailStartPct: 2,
          trailFactor: 0.9,
          timeoutMinutes: 120
        },
        stage2A: {
          enabled: true,
          waitBarsAfterStop: 3,
          reboundFromRollingLowPct: 18,
          rollingLowBars: 3,
          entryPriceMode: 'close',
          stopLossPct: 4,
          trailStartPct: 3,
          trailFactor: 0.9,
          timeoutMinutes: 120
        },
        stage3: {
          enabled: true,
          waitBarsFromSignal: 30,
          firstPeakMinPct: 10,
          entryPriceMode: 'close',
          stopLossPct: 4,
          trailStartPct: 3,
          trailFactor: 0.9,
          timeoutMinutes: 120
        }
      },
      paperExitRules: { stopLossPct: 3, trailStartPct: 2, trailFactor: 0.9, takeProfitPct: [50, 100, 200], timeoutMinutes: 120 }
    })
  };
}

function readIndexValue(index) {
  if (typeof index === 'number') return index;
  if (!index) return 0;
  return Number(index.current ?? index.value ?? 0) || 0;
}

function normalizeNarrativeTier(tier) {
  const raw = String(tier || '').trim().toUpperCase();
  const map = {
    TIER_S: 'TIER_S',
    S: 'TIER_S',
    TIER_A: 'TIER_A',
    A: 'TIER_A',
    TIER_B: 'TIER_B',
    B: 'TIER_B',
    TIER_C: 'TIER_C',
    C: 'TIER_C',
    D: 'TIER_D',
    TIER_D: 'TIER_D'
  };
  return map[raw] || null;
}

function tierScore(tier) {
  switch (normalizeNarrativeTier(tier)) {
    case 'TIER_S': return 100;
    case 'TIER_A': return 90;
    case 'TIER_B': return 75;
    case 'TIER_C': return 60;
    case 'TIER_D': return 40;
    default: return 0;
  }
}

function historicalActionScore(action) {
  switch (String(action || '').toUpperCase()) {
    case 'BUY_FULL': return 90;
    case 'BUY_HALF': return 70;
    case 'WATCH': return 55;
    default: return 25;
  }
}

function hardGateScore(status) {
  const normalized = String(status || '').toUpperCase();
  if (normalized === 'PASS') return 100;
  if (normalized.startsWith('GREYLIST')) return 65;
  if (normalized === 'WASH_HIGH') return 15;
  if (normalized === '5M_DUMP') return 10;
  if (normalized === 'PRECHECK_FAIL' || normalized === 'REJECT') return 0;
  return 20;
}

function normalizeSignalText(text) {
  return String(text || '')
    .replace(/\*\*/g, '')
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .replace(/\r/g, '');
}

function extractNumber(text, regex) {
  const match = normalizeSignalText(text).match(regex);
  if (!match) return 0;
  const numeric = String(match[1] || '').replace(/,/g, '');
  const suffix = String(match[2] || '').toUpperCase();
  const value = Number(numeric);
  if (!Number.isFinite(value)) return 0;
  if (suffix === 'K') return value * 1_000;
  if (suffix === 'M') return value * 1_000_000;
  if (suffix === 'B') return value * 1_000_000_000;
  return value;
}

function deriveAgeMinutes(text) {
  const match = normalizeSignalText(text).match(/Age:\s*(\d+)\s*([MHD])/i);
  if (!match) return 0;
  const value = Number(match[1] || 0);
  const unit = String(match[2] || '').toUpperCase();
  if (unit === 'M') return value;
  if (unit === 'H') return value * 60;
  if (unit === 'D') return value * 60 * 24;
  return 0;
}

function extractSignalContext(signal = {}) {
  const text = normalizeSignalText(signal.description || '');
  const holders = Number(signal.holders || 0) || extractNumber(text, /Holders[^\d]*(\d+(?:\.\d+)?)([KMB])?/i);
  const top10Pct = Number(signal.top10_pct || 0) || extractNumber(text, /Top10[^\d]*(\d+(?:\.\d+)?)/i);
  const marketCap = Number(signal.market_cap || 0) || extractNumber(text, /(?:MC|MarketCap)[^\d]*(\d+(?:\.\d+)?)([KMB])?/i);
  const volume24h = Number(signal.volume_24h || 0) || extractNumber(text, /Vol24H[^\d]*(\d+(?:\.\d+)?)([KMB])?/i);
  const tx24h = extractNumber(text, /Tx24H[^\d]*(\d+(?:\.\d+)?)([KMB])?/i);
  const ageMinutes = deriveAgeMinutes(text);
  return { holders, top10Pct, marketCap, volume24h, tx24h, ageMinutes };
}

function deriveFallbackConfidence({ superCurrent, holders, top10Pct, marketCap, tx24h, isAth }) {
  let confidence = 20;
  confidence += Math.min(35, Math.max(0, superCurrent - 50) * 0.6);
  confidence += Math.min(12, holders * 0.08);
  confidence += Math.min(10, tx24h * 0.02);
  confidence += marketCap >= 15000 ? 8 : 0;
  confidence -= top10Pct > 0 ? Math.max(0, (top10Pct - 25) * 1.1) : 0;
  if (isAth) confidence -= 6;
  return Math.max(5, Math.min(95, Math.round(confidence)));
}

function deriveFallbackAction({ isAth, superCurrent, top10Pct, holders, tx24h }) {
  if (isAth && superCurrent >= 100) return 'WATCH';
  if (superCurrent >= 120 && holders >= 120 && top10Pct > 0 && top10Pct <= 28 && tx24h >= 150) return 'BUY_FULL';
  if (superCurrent >= 90 && holders >= 60 && top10Pct > 0 && top10Pct <= 35) return 'BUY_HALF';
  if (superCurrent >= 70 || (holders >= 80 && tx24h >= 300)) return 'WATCH';
  return 'SKIP';
}

function deriveFallbackTier({ marketCap, holders, top10Pct, superCurrent, tx24h, isAth }) {
  if (isAth && marketCap >= 150000) return 'TIER_A';
  if (superCurrent >= 140 || (marketCap >= 50000 && holders >= 200 && top10Pct > 0 && top10Pct <= 22 && tx24h >= 400)) return 'TIER_A';
  if (superCurrent >= 110 || (marketCap >= 20000 && holders >= 100 && top10Pct > 0 && top10Pct <= 28)) return 'TIER_B';
  if (superCurrent >= 80 || holders >= 60 || marketCap >= 10000) return 'TIER_C';
  return 'TIER_D';
}

export class PaperStrategyRegistry {
  constructor(filePath = registryPath) {
    this.filePath = filePath;
    this.registry = this.load();
  }

  load() {
    if (!fs.existsSync(this.filePath)) {
      const initial = {
        version: 1,
        updatedAt: new Date().toISOString(),
        activeBaselineId: 'baseline-v1',
        activeChallengerId: null,
        promotedIds: ['baseline-v1'],
        candidates: {
          'baseline-v1': makeBaselineCandidate(),
          'notath-selective-v1': makeSelectiveNotAthCandidate()
        },
        history: []
      };
      fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
      fs.writeFileSync(this.filePath, JSON.stringify(initial, null, 2));
      return initial;
    }

    const parsed = JSON.parse(fs.readFileSync(this.filePath, 'utf8'));
    const candidates = parsed.candidates || {};
    if (!candidates['notath-selective-v1']) {
      candidates['notath-selective-v1'] = makeSelectiveNotAthCandidate();
      parsed.candidates = candidates;
      fs.writeFileSync(this.filePath, JSON.stringify(parsed, null, 2));
    }
    for (const candidate of Object.values(parsed.candidates || {})) {
      validateCandidate(candidate);
    }
    return parsed;
  }

  async save() {
    this.registry.updatedAt = new Date().toISOString();
    await atomicWriteJSON(this.filePath, this.registry);
  }

  getBaseline() {
    return this.registry.candidates[this.registry.activeBaselineId];
  }

  getChallenger() {
    return this.registry.activeChallengerId ? this.registry.candidates[this.registry.activeChallengerId] : null;
  }

  listCandidates() {
    return Object.values(this.registry.candidates);
  }

  getCandidate(candidateId) {
    return this.registry.candidates[candidateId] || null;
  }

  async registerCandidate(candidate) {
    const validated = validateCandidate(candidate);
    this.registry.candidates[validated.id] = validated;
    this.registry.history.push({
      action: 'register',
      candidateId: validated.id,
      status: validated.status,
      createdAt: new Date().toISOString()
    });
    await this.save();
    return validated;
  }

  async markCandidateStatus(candidateId, status, metadata = {}) {
    const candidate = this.registry.candidates[candidateId];
    if (!candidate) {
      throw new Error(`Unknown candidate: ${candidateId}`);
    }
    const previousStatus = candidate.status;
    candidate.status = status;
    if (status === 'qualified' && !candidate.qualifiedAt) {
      candidate.qualifiedAt = new Date().toISOString();
    }
    if (status === 'active_challenger') {
      candidate.activatedAt = new Date().toISOString();
    }
    if (status === 'paused_target_reached') {
      candidate.previousStatus = metadata.previousStatus || previousStatus || candidate.previousStatus || 'promotable';
      candidate.pausedAt = new Date().toISOString();
      candidate.pauseReason = metadata.reason || candidate.pauseReason || null;
      candidate.resumedAt = null;
      candidate.resumeReason = null;
    }
    this.registry.history.push({ action: 'mark_status', candidateId, status, metadata, createdAt: new Date().toISOString() });
    await this.save();
    return candidate;
  }

  async resumeCandidateStatus(candidateId, reason = 'manual_resume') {
    const candidate = this.registry.candidates[candidateId];
    if (!candidate) {
      throw new Error(`Unknown candidate: ${candidateId}`);
    }
    const restoredStatus = candidate.previousStatus && candidate.previousStatus !== 'paused_target_reached'
      ? candidate.previousStatus
      : 'promotable';
    candidate.status = restoredStatus;
    candidate.previousStatus = null;
    candidate.pausedAt = null;
    candidate.pauseReason = null;
    candidate.resumedAt = new Date().toISOString();
    candidate.resumeReason = reason;
    this.registry.history.push({ action: 'resume_status', candidateId, restoredStatus, reason, createdAt: new Date().toISOString() });
    await this.save();
    return candidate;
  }

  async setChallenger(candidateId, reason = 'qualified_candidate') {
    const candidate = this.registry.candidates[candidateId];
    if (!candidate) {
      throw new Error(`Unknown candidate: ${candidateId}`);
    }
    if (!['qualified', 'promotable', 'active_challenger'].includes(candidate.status)) {
      throw new Error(`Candidate ${candidateId} must be qualified/promotable before activation (current=${candidate.status})`);
    }
    if (this.registry.activeChallengerId && this.registry.activeChallengerId !== candidateId) {
      const previous = this.registry.candidates[this.registry.activeChallengerId];
      if (previous && previous.status === 'active_challenger') {
        previous.status = 'qualified';
      }
    }
    candidate.status = 'active_challenger';
    candidate.activatedAt = new Date().toISOString();
    this.registry.activeChallengerId = candidateId;
    this.registry.history.push({ action: 'set_challenger', candidateId, reason, createdAt: new Date().toISOString() });
    await this.save();
    return candidate;
  }

  async promote(candidateId, reason = 'guardrail_passed') {
    const candidate = this.registry.candidates[candidateId];
    if (!candidate) throw new Error(`Unknown candidate: ${candidateId}`);

    candidate.status = 'promoted';
    candidate.promotedAt = new Date().toISOString();
    this.registry.activeBaselineId = candidateId;
    this.registry.activeChallengerId = null;
    if (!this.registry.promotedIds.includes(candidateId)) {
      this.registry.promotedIds.push(candidateId);
    }
    this.registry.history.push({ action: 'promote', candidateId, reason, createdAt: new Date().toISOString() });
    await this.save();
    return candidate;
  }

  async rollback(targetId, reason = 'manual_rollback') {
    if (!this.registry.candidates[targetId]) {
      throw new Error(`Unknown candidate: ${targetId}`);
    }
    this.registry.activeBaselineId = targetId;
    this.registry.activeChallengerId = null;
    this.registry.candidates[targetId].status = 'promoted';
    this.registry.history.push({ action: 'rollback', candidateId: targetId, reason, createdAt: new Date().toISOString() });
    await this.save();
    return this.registry.candidates[targetId];
  }

  evaluateSignal(signal, candidate = this.getBaseline()) {
    const config = candidate.strategyConfig;
    const indices = signal.indices || {};
    const superCurrent = readIndexValue(indices?.super_index);
    const tradeCurrent = readIndexValue(indices?.trade_index);
    const tradeSignal = Number(indices?.trade_index?.signal || 0);
    const addressCurrent = readIndexValue(indices?.address_index);
    const securityCurrent = readIndexValue(indices?.security_index);
    const ctx = extractSignalContext(signal);
    const inferredConfidence = deriveFallbackConfidence({
      superCurrent,
      holders: ctx.holders,
      top10Pct: ctx.top10Pct,
      marketCap: ctx.marketCap,
      tx24h: ctx.tx24h,
      isAth: Boolean(signal.is_ath)
    });
    const narrativeConfidence = Number(signal.ai_confidence || signal.confidence || signal.narrative_confidence || inferredConfidence || 0);
    const tradeDelta = tradeCurrent - tradeSignal;
    const derivedAiAction = deriveFallbackAction({
      isAth: Boolean(signal.is_ath),
      superCurrent,
      top10Pct: ctx.top10Pct,
      holders: ctx.holders,
      tx24h: ctx.tx24h
    });
    const normalizedAiAction = String(signal.ai_action || derivedAiAction || '').toUpperCase();
    const normalizedHardGate = String(signal.hard_gate_status || '').toUpperCase();
    const normalizedTier = normalizeNarrativeTier(signal.ai_narrative_tier || deriveFallbackTier({
      marketCap: ctx.marketCap,
      holders: ctx.holders,
      top10Pct: ctx.top10Pct,
      superCurrent,
      tx24h: ctx.tx24h,
      isAth: Boolean(signal.is_ath)
    }));
    const fallbackMode = !superCurrent && !tradeCurrent && !addressCurrent && !securityCurrent;
    const stageRules = config.stageRules || {};

    let score = 0;
    let action = 'SKIP';
    let reason = '';

    if (stageRules.stage1?.enabled) {
      const stage1 = stageRules.stage1;
      const isAth = Boolean(signal.is_ath);
      const top10Pct = ctx.top10Pct;
      const holders = ctx.holders;
      const aiConfidence = narrativeConfidence;
      const primaryBand = top10Pct >= stage1.top10PctPrimaryMin && top10Pct <= stage1.top10PctPrimaryMax;
      const secondaryBand = top10Pct > stage1.top10PctPrimaryMax && top10Pct <= stage1.top10PctSecondaryMax;
      const excludedLow = top10Pct > 0 && top10Pct <= stage1.excludeTop10PctAtOrBelow;
      const allowedAth = !isAth || stage1.allowAthOverride || config.sourceToggles.allowATH;
      const allowedNotAth = !isAth ? config.sourceToggles.allowNotAth : true;
      const bandBonus = primaryBand ? (stage1.primaryBandBonus || 0) : secondaryBand ? (stage1.secondaryBandBonus || 0) : 0;

      if (!allowedAth || !allowedNotAth || excludedLow || aiConfidence < stage1.aiConfidenceMin || holders < stage1.holdersMin) {
        action = 'SKIP';
        score = 0;
        reason = `stage1 reject ai=${aiConfidence}, holders=${holders}, top10=${top10Pct}, isAth=${isAth}`;
      } else if (primaryBand || secondaryBand) {
        action = 'BUY';
        score = primaryBand ? 100 + bandBonus : 90 + bandBonus;
        reason = `stage1 ${primaryBand ? 'primary' : 'secondary'} band ai=${aiConfidence}, holders=${holders}, top10=${top10Pct}`;
      } else {
        action = 'WATCH';
        score = 60 + bandBonus;
        reason = `stage1 watch ai=${aiConfidence}, holders=${holders}, top10=${top10Pct}`;
      }
    } else if (!fallbackMode) {
      score += Math.min(40, (superCurrent / Math.max(config.entryTimingFilters.minSuperIndex, 1)) * 25);
      score += Math.min(20, Math.max(tradeDelta, 0) * 5);
      score += Math.min(15, addressCurrent * 2.5);
      score += Math.min(15, securityCurrent / 2);
      score += Math.min(20, narrativeConfidence / 4);
      score += Math.min(12, ctx.holders * 0.04);
      score += Math.min(8, ctx.tx24h * 0.01);
      score -= ctx.top10Pct > 0 ? Math.min(12, Math.max(0, ctx.top10Pct - 30) * 0.8) : 0;

      const allowedBySource = (signal.is_ath ? config.sourceToggles.allowATH : config.sourceToggles.allowNotAth);
      const strongNotAth = !signal.is_ath && superCurrent >= Math.max(60, config.entryTimingFilters.minSuperIndex - 20) && narrativeConfidence >= 40;
      const strongMetadata = ctx.holders >= 60 && ctx.top10Pct > 0 && ctx.top10Pct <= Math.min(35, config.entryTimingFilters.maxChasePremiumPct + 15);
      action = !allowedBySource
        ? 'SKIP'
        : score >= config.scoreThresholds.buy &&
          superCurrent >= config.entryTimingFilters.minSuperIndex &&
          tradeDelta >= config.entryTimingFilters.minTradeDelta &&
          addressCurrent >= config.entryTimingFilters.minAddressIndex &&
          securityCurrent >= config.entryTimingFilters.minSecurityIndex &&
          narrativeConfidence >= config.narrativeThresholds.minConfidence
          ? 'BUY'
          : score >= Math.max(62, config.scoreThresholds.buy - 10) && strongNotAth && strongMetadata
            ? 'BUY'
            : score >= config.scoreThresholds.watch
              ? 'WATCH'
              : 'SKIP';
      reason = `indices super=${superCurrent}, tradeDelta=${tradeDelta}, addr=${addressCurrent}, sec=${securityCurrent}, holders=${ctx.holders}, top10=${ctx.top10Pct}`;
    } else {
      const tierComponent = tierScore(normalizedTier);
      const hardGateComponent = hardGateScore(normalizedHardGate);
      const historicalActionComponent = historicalActionScore(normalizedAiAction);
      const top10Penalty = ctx.top10Pct > 0 ? Math.min(20, Math.max(0, ctx.top10Pct - 25) * 0.8) : 0;
      const metadataBonus = Math.min(12, ctx.holders * 0.04) + Math.min(8, ctx.tx24h * 0.01) + (ctx.marketCap >= 15000 ? 6 : 0);

      score = (historicalActionComponent * 0.35)
        + (narrativeConfidence * 0.22)
        + (tierComponent * 0.18)
        + (hardGateComponent * 0.15)
        + metadataBonus
        - top10Penalty;

      const allowedBySource = (signal.is_ath ? config.sourceToggles.allowATH : config.sourceToggles.allowNotAth);
      const gateRejected = ['REJECT', 'PRECHECK_FAIL', 'WASH_HIGH', '5M_DUMP'].includes(normalizedHardGate);
      const historicalBuy = normalizedAiAction === 'BUY_FULL' || normalizedAiAction === 'BUY_HALF';
      const strongFallbackBuy = !signal.is_ath
        && ['PASS', 'GREYLIST_SIGNAL'].includes(normalizedHardGate || 'PASS')
        && narrativeConfidence >= 28
        && ctx.holders >= 80
        && ctx.tx24h >= 300
        && ctx.marketCap >= 12000
        && (ctx.top10Pct === 0 || ctx.top10Pct <= 35);
      const moderateFallbackWatch = !signal.is_ath
        && narrativeConfidence >= 24
        && ctx.holders >= 60
        && ctx.marketCap >= 10000;
      action = !allowedBySource || gateRejected
        ? 'SKIP'
        : score >= config.scoreThresholds.buy && historicalBuy && narrativeConfidence >= Math.max(35, config.narrativeThresholds.minConfidence - 30)
          ? 'BUY'
          : score >= Math.max(58, config.scoreThresholds.buy - 12) && (historicalBuy || strongFallbackBuy)
            ? 'BUY'
            : score >= config.scoreThresholds.watch && (historicalBuy || normalizedHardGate === 'PASS' || moderateFallbackWatch)
              ? 'WATCH'
              : 'SKIP';
      reason = `fallback ai=${normalizedAiAction || 'NONE'}, gate=${normalizedHardGate || 'NONE'}, tier=${normalizedTier || 'NONE'}, top10=${ctx.top10Pct}, holders=${ctx.holders}, tx24h=${ctx.tx24h}`;
    }

    return {
      strategyId: candidate.id,
      action,
      score: Number(score.toFixed(2)),
      confidence: narrativeConfidence,
      reason,
      configSnapshot: config
    };
  }
}

export default PaperStrategyRegistry;
