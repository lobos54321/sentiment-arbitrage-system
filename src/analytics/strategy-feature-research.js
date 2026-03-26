import Database from 'better-sqlite3';
import SignalFeatureEnrichmentStore from '../database/signal-feature-enrichment-store.js';

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function median(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function mean(values) {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function percentile(values, p) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * p)));
  return sorted[index];
}

function ratio(numerator, denominator) {
  if (!denominator) return 0;
  return numerator / denominator;
}

function normalizeEpochMs(value) {
  const num = toNum(value, 0);
  if (!num) return 0;
  return num < 1e12 ? num * 1000 : num;
}

function deriveFallbackConfidence({ superIndex, holders, top10Pct, marketCap, tx24h, isAth }) {
  let confidence = 20;
  confidence += Math.min(35, Math.max(0, toNum(superIndex, 0) - 50) * 0.6);
  confidence += Math.min(12, toNum(holders, 0) * 0.08);
  confidence += Math.min(10, toNum(tx24h, 0) * 0.02);
  confidence += toNum(marketCap, 0) >= 15000 ? 8 : 0;
  confidence -= toNum(top10Pct, 0) > 0 ? Math.max(0, (toNum(top10Pct, 0) - 25) * 1.1) : 0;
  if (isAth) confidence -= 6;
  return Math.max(5, Math.min(95, Math.round(confidence)));
}

function deriveFallbackTier({ marketCap, holders, top10Pct, superIndex, tx24h, isAth }) {
  if (isAth && toNum(marketCap, 0) >= 150000) return 'TIER_A';
  if (toNum(superIndex, 0) >= 140 || (toNum(marketCap, 0) >= 50000 && toNum(holders, 0) >= 200 && toNum(top10Pct, 0) > 0 && toNum(top10Pct, 0) <= 22 && toNum(tx24h, 0) >= 400)) return 'TIER_A';
  if (toNum(superIndex, 0) >= 110 || (toNum(marketCap, 0) >= 20000 && toNum(holders, 0) >= 100 && toNum(top10Pct, 0) > 0 && toNum(top10Pct, 0) <= 28)) return 'TIER_B';
  if (toNum(superIndex, 0) >= 80 || toNum(holders, 0) >= 60 || toNum(marketCap, 0) >= 10000) return 'TIER_C';
  return 'TIER_D';
}

function deriveFallbackAction({ isAth, superIndex, top10Pct, holders, tx24h }) {
  if (isAth && toNum(superIndex, 0) >= 100) return 'WATCH';
  if (toNum(superIndex, 0) >= 120 && toNum(holders, 0) >= 120 && toNum(top10Pct, 0) > 0 && toNum(top10Pct, 0) <= 28 && toNum(tx24h, 0) >= 150) return 'BUY_FULL';
  if (toNum(superIndex, 0) >= 90 && toNum(holders, 0) >= 60 && toNum(top10Pct, 0) > 0 && toNum(top10Pct, 0) <= 35) return 'BUY_HALF';
  if (toNum(superIndex, 0) >= 70) return 'WATCH';
  return 'SKIP';
}

function parseDescriptionFeatures(description = '', signal = {}, enrichment = null) {
  const text = String(description || '');
  const normalizedText = text
    .replace(/\*\*/g, '')
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .replace(/\r/g, '');
  const extract = (regex) => {
    const match = normalizedText.match(regex);
    return match ? Number(match[1]) : null;
  };

  const enriched = enrichment?.extracted || {};
  const enrichedIndices = enriched.indices || {};
  const superIndex = Number(enrichedIndices.super_index?.current ?? enrichedIndices.super_index?.value ?? extract(/Super\s+Index[^\d]*(\d+)/i));
  const tradeIndex = Number(enrichedIndices.trade_index?.current ?? enrichedIndices.trade_index?.value ?? extract(/Trade\s+Index[^\d]*(\d+)/i));
  const securityIndex = Number(enrichedIndices.security_index?.current ?? enrichedIndices.security_index?.value ?? extract(/Security\s+Index[^\d]*(\d+)/i));
  const addressIndex = Number(enrichedIndices.address_index?.current ?? enrichedIndices.address_index?.value ?? extract(/Address\s+Index[^\d]*(\d+)/i));
  const sentimentIndex = Number(enrichedIndices.sentiment_index?.current ?? enrichedIndices.sentiment_index?.value ?? extract(/Sentiment\s+Index[^\d]*(\d+)/i));
  const aiIndex = Number(enrichedIndices.ai_index?.current ?? enrichedIndices.ai_index?.value ?? extract(/AI\s+Index[^\d]*(\d+)/i));
  const holders = toNum(enriched.holders, extract(/Holders[^\d]*(\d+)/i) || toNum(signal.holders, 0));
  const top10Pct = enriched.top10Pct != null ? toNum(enriched.top10Pct, 0) : (extract(/Top10[^\d]*(\d+(?:\.\d+)?)/i) ?? toNum(signal.top10_pct, 0));
  const marketCap = toNum(enriched.marketCap, extract(/MC[^\d]*(\d+(?:\.\d+)?)/i) ?? toNum(signal.market_cap, 0));
  const vol24h = toNum(enriched.volume24h, extract(/Vol24H[^\d]*(\d+(?:\.\d+)?)/i) ?? toNum(signal.volume_24h, 0));
  const tx24h = toNum(enriched.tx24h, extract(/Tx24H[^\d]*(\d+)/i) ?? 0);
  const ageMinutes = enriched.ageMinutes != null ? toNum(enriched.ageMinutes, null) : (() => {
    const m = normalizedText.match(/Age:\s*(\d+)([MHD])/i);
    if (!m) return null;
    const count = Number(m[1]);
    const unit = m[2].toUpperCase();
    if (unit === 'M') return count;
    if (unit === 'H') return count * 60;
    if (unit === 'D') return count * 60 * 24;
    return null;
  })();
  const isAth = Boolean(signal.is_ath) || (/\bATH\b/i.test(normalizedText) && !/NOT_ATH/i.test(normalizedText)) || String(signal.hard_gate_status || '').toUpperCase().includes('ATH');
  const sourceType = String(enriched.metadataSource || '').includes('remote-log') || String(enrichment?.source || '').includes('remote-log') || String(signal.hard_gate_status || '').toUpperCase().includes('REMOTE_LOG_IMPORT')
    ? 'remote-log'
    : 'local-premium';
  const hasIndices = [superIndex, tradeIndex, securityIndex, addressIndex].some((v) => Number.isFinite(v) && v > 0);
  const klineScoreHints = enriched.klineScoreHints || null;
  const isRedBarHint = klineScoreHints ? Boolean(klineScoreHints.red) : /RED/i.test(text);
  const activeHint = klineScoreHints ? Boolean(klineScoreHints.active) : /TRENDING|VIRAL|BREAKOUT/i.test(text);
  const lowVolumeHint = klineScoreHints ? Boolean(klineScoreHints.lowVol) : /Top10/i.test(text);
  const derivedConfidence = deriveFallbackConfidence({ superIndex, holders, top10Pct, marketCap, tx24h, isAth });
  const narrativeTier = signal.ai_narrative_tier || enriched.aiNarrativeTier || deriveFallbackTier({ marketCap, holders, top10Pct, superIndex, tx24h, isAth });
  const aiAction = signal.ai_action || enriched.aiAction || deriveFallbackAction({ isAth, superIndex, top10Pct, holders, tx24h });
  const qualityScore = Number(enrichment?.qualityScore || [superIndex, tradeIndex, securityIndex, addressIndex, holders, top10Pct, marketCap, vol24h].filter((v) => v !== null && v !== undefined && !Number.isNaN(v) && v !== 0).length);

  return {
    sourceType,
    isAth,
    hasIndices,
    superIndex,
    tradeIndex,
    securityIndex,
    addressIndex,
    sentimentIndex,
    aiIndex,
    aiConfidence: toNum(signal.ai_confidence || enriched.aiConfidence, derivedConfidence),
    aiAction,
    narrativeTier,
    hardGateStatus: signal.hard_gate_status || enriched.hardGateStatus || null,
    top10Pct,
    holders,
    marketCap,
    volume24h: vol24h,
    tx24h,
    ageMinutes,
    isRedBarHint,
    activeHint,
    lowVolumeHint,
    klineScoreHints,
    qualityScore,
    metadataSource: enriched.metadataSource || null
  };
}

function classifySample(sample) {
  const pnl = toNum(sample.pnlPercent, sample.pnl);
  const maxGain = toNum(sample.maxGainPercent, sample.highPnl);
  const maxDrawdown = Math.abs(toNum(sample.maxDrawdownPercent, sample.lowPnl));
  return {
    isWinner: pnl > 0,
    isGold: maxGain >= 100,
    falsePositive: pnl <= 0 && maxGain < 20,
    slippageSensitive: maxGain > 0 && pnl < maxGain * 0.4,
    quickTakeProfitFriendly: maxGain >= 40,
    trailingFriendly: maxGain >= 100 && pnl > 0,
    pnl,
    maxGain,
    maxDrawdown
  };
}

function computeKlineScoring(sample) {
  const features = sample.features || {};
  if (features.klineScoreHints) {
    const hinted = features.klineScoreHints;
    return {
      red: Boolean(hinted.red),
      lowVolume: Boolean(hinted.lowVol),
      active: Boolean(hinted.active),
      super80: toNum(features.superIndex, 0) >= 80,
      score: toNum(hinted.score, 0),
      momentumPct: toNum(hinted.momentumPct, 0),
      passed: Boolean(hinted.red) && toNum(hinted.score, 0) >= 3
    };
  }

  const red = Boolean(features.isRedBarHint) || toNum(features.tradeIndex, 0) <= 0;
  const lowVolume = toNum(features.top10Pct, 100) <= 25 || Boolean(features.lowVolumeHint);
  const active = toNum(features.superIndex, 0) >= 80 || Boolean(features.activeHint) || toNum(features.tx24h, 0) >= 300;
  const score = (red ? 2 : 0) + (lowVolume ? 1 : 0) + (active ? 1 : 0);
  return {
    red,
    lowVolume,
    active,
    super80: toNum(features.superIndex, 0) >= 80,
    score,
    momentumPct: 0,
    passed: red && score >= 3
  };
}

function summarizeGroup(samples, metricKey = 'pnl') {
  const values = samples.map((sample) => toNum(sample[metricKey], 0));
  return {
    count: samples.length,
    mean: Number(mean(values).toFixed(4)),
    median: Number(median(values).toFixed(4)),
    p75: Number(percentile(values, 0.75).toFixed(4)),
    p90: Number(percentile(values, 0.9).toFixed(4))
  };
}

function compareBooleanFeature(samples, featureName) {
  const yes = samples.filter((sample) => {
    const value = sample.features?.[featureName];
    if (typeof value === 'string') return value === 'remote-log' || value === 'yes' || value === 'true';
    return Boolean(value);
  });
  const no = samples.filter((sample) => !yes.includes(sample));
  return {
    feature: featureName,
    yes: {
      count: yes.length,
      winRate: Number(ratio(yes.filter((s) => s.isWinner).length, yes.length).toFixed(4)),
      avgPnl: Number(mean(yes.map((s) => s.pnlPercent)).toFixed(4)),
      goldRate: Number(ratio(yes.filter((s) => s.isGold).length, yes.length).toFixed(4))
    },
    no: {
      count: no.length,
      winRate: Number(ratio(no.filter((s) => s.isWinner).length, no.length).toFixed(4)),
      avgPnl: Number(mean(no.map((s) => s.pnlPercent)).toFixed(4)),
      goldRate: Number(ratio(no.filter((s) => s.isGold).length, no.length).toFixed(4))
    }
  };
}

function compareThreshold(samples, featureName, thresholds) {
  return thresholds.map((threshold) => {
    const passed = samples.filter((sample) => toNum(sample.features?.[featureName], -Infinity) >= threshold);
    return {
      feature: featureName,
      threshold,
      count: passed.length,
      winRate: Number(ratio(passed.filter((s) => s.isWinner).length, passed.length).toFixed(4)),
      avgPnl: Number(mean(passed.map((s) => s.pnlPercent)).toFixed(4)),
      goldRate: Number(ratio(passed.filter((s) => s.isGold).length, passed.length).toFixed(4)),
      falsePositiveRate: Number(ratio(passed.filter((s) => s.falsePositive).length, passed.length).toFixed(4))
    };
  });
}

function applyExecutionAssumptions(sample, assumptions) {
  const adjustedPnl = sample.pnlPercent - assumptions.roundTripCostPct;
  const delayedPnl = adjustedPnl - assumptions.entryDelayPenaltyPct;
  return {
    ...sample,
    adjustedPnl,
    delayedPnl,
    stillProfitableAfterDelay: delayedPnl > 0,
    stillGoldAfterDelay: sample.maxGainPercent >= assumptions.goldThresholdPct && delayedPnl > 0
  };
}

export class StrategyFeatureResearch {
  constructor({ dbPath, enrichmentStore } = {}) {
    this.db = new Database(dbPath || process.env.DB_PATH || './data/sentiment_arb.db', { readonly: true });
    this.enrichmentStore = enrichmentStore || new SignalFeatureEnrichmentStore(dbPath || process.env.DB_PATH || './data/sentiment_arb.db');
  }

  close() {
    try { this.db.close(); } catch {}
  }

  loadSamples(limit = 500) {
    const outcomeRows = this.db.prepare(`
      SELECT token_ca, pnl_percent, max_gain_percent, max_drawdown_percent, exit_reason, entry_time, exit_time
      FROM signal_outcomes
      ORDER BY rowid DESC
    `).all();

    const shadowRows = this.db.prepare(`
      SELECT token_ca, exit_pnl, high_pnl, low_pnl, exit_reason, entry_time, closed_at
      FROM shadow_pnl
      WHERE closed = 1
      ORDER BY rowid DESC
    `).all();

    const outcomesByToken = new Map();
    for (const row of outcomeRows) {
      if (!outcomesByToken.has(row.token_ca)) outcomesByToken.set(row.token_ca, []);
      outcomesByToken.get(row.token_ca).push({
        source: 'signal_outcomes',
        pnlPercent: toNum(row.pnl_percent, 0),
        maxGainPercent: toNum(row.max_gain_percent, 0),
        maxDrawdownPercent: toNum(row.max_drawdown_percent, 0),
        exitReason: row.exit_reason || null,
        entryTime: normalizeEpochMs(row.entry_time),
        exitTime: normalizeEpochMs(row.exit_time)
      });
    }
    for (const row of shadowRows) {
      if (!outcomesByToken.has(row.token_ca)) outcomesByToken.set(row.token_ca, []);
      outcomesByToken.get(row.token_ca).push({
        source: 'shadow_pnl',
        pnlPercent: toNum(row.exit_pnl, 0),
        maxGainPercent: toNum(row.high_pnl, 0),
        maxDrawdownPercent: Math.abs(toNum(row.low_pnl, 0)),
        exitReason: row.exit_reason || null,
        entryTime: normalizeEpochMs(row.entry_time),
        exitTime: normalizeEpochMs(row.closed_at)
      });
    }

    const candidateTokens = [...outcomesByToken.keys()];
    const shadowHeavyTokens = new Set(shadowRows.map((row) => row.token_ca));
    const tokenLimit = Math.max(limit * 6, 600);
    const selectedTokens = candidateTokens.slice(0, tokenLimit);
    const placeholders = selectedTokens.map(() => '?').join(',');
    const premiumRows = selectedTokens.length
      ? this.db.prepare(`
          SELECT
            p.id AS signal_id,
            p.token_ca,
            p.symbol,
            p.description,
            p.timestamp,
            p.hard_gate_status,
            p.ai_action,
            p.ai_confidence,
            p.ai_narrative_tier,
            p.top10_pct,
            p.holders,
            p.volume_24h,
            p.market_cap,
            p.age,
            s.source AS enrichment_source,
            s.quality_score AS enrichment_quality,
            s.extracted_json AS enrichment_json
          FROM premium_signals p
          LEFT JOIN signal_feature_enrichments s ON s.signal_id = p.id
          WHERE p.token_ca IN (${placeholders})
          ORDER BY
            CASE WHEN s.source = 'remote-log-parser-v2' THEN 0 ELSE 1 END,
            COALESCE(s.quality_score, 0) DESC,
            p.timestamp DESC
          LIMIT ?
        `).all(...selectedTokens, Math.max(limit * 12, 2000))
      : [];

    const pickBestOutcome = (signalTs, tokenCa, preferredSource = null) => {
      const candidates = outcomesByToken.get(tokenCa) || [];
      if (!candidates.length) return null;
      const preferredPool = preferredSource ? candidates.filter((candidate) => candidate.source === preferredSource) : candidates;
      const withSourcePool = preferredPool.length ? preferredPool : candidates;
      const sameEra = withSourcePool.filter((candidate) => {
        if (!candidate.entryTime || !signalTs) return false;
        return Math.abs(candidate.entryTime - signalTs) <= 12 * 60 * 60 * 1000;
      });
      const pool = sameEra.length ? sameEra : withSourcePool;
      return [...pool].sort((a, b) => {
        const aDelta = a.entryTime ? Math.abs(a.entryTime - signalTs) : Number.MAX_SAFE_INTEGER;
        const bDelta = b.entryTime ? Math.abs(b.entryTime - signalTs) : Number.MAX_SAFE_INTEGER;
        return aDelta - bDelta;
      })[0] || null;
    };

    const deduped = [];
    const seenSignalIds = new Set();
    const seenTokenBuckets = new Set();
    for (const row of premiumRows) {
      if (seenSignalIds.has(row.signal_id)) continue;
      seenSignalIds.add(row.signal_id);

      const signalTs = normalizeEpochMs(row.timestamp);
      const preferredSource = shadowHeavyTokens.has(row.token_ca) ? 'shadow_pnl' : null;
      const outcome = pickBestOutcome(signalTs, row.token_ca, preferredSource);
      if (!outcome) continue;

      const existingEnrichment = row.enrichment_json
        ? {
            signalId: row.signal_id,
            tokenCa: row.token_ca,
            source: row.enrichment_source,
            extracted: JSON.parse(row.enrichment_json || '{}'),
            qualityScore: Number(row.enrichment_quality || 0)
          }
        : this.enrichmentStore.get(row.signal_id);
      const features = parseDescriptionFeatures(row.description, row, existingEnrichment);
      this.enrichmentStore.upsert({
        signalId: row.signal_id,
        tokenCa: row.token_ca,
        source: existingEnrichment?.source || 'description-parser',
        extracted: {
          ...(existingEnrichment?.extracted || {}),
          ...features,
          indices: existingEnrichment?.extracted?.indices || null,
          klineScoreHints: existingEnrichment?.extracted?.klineScoreHints || features.klineScoreHints || null,
          metadataSource: existingEnrichment?.extracted?.metadataSource || features.metadataSource || 'description-parser'
        },
        qualityScore: Math.max(existingEnrichment?.qualityScore || 0, features.qualityScore)
      });

      const bucketTs = outcome.entryTime || signalTs;
      const bucket = `${row.token_ca}:${Math.floor(bucketTs / (30 * 60 * 1000))}`;
      if (seenTokenBuckets.has(bucket)) continue;
      seenTokenBuckets.add(bucket);

      const base = {
        signalId: row.signal_id,
        tokenCa: row.token_ca,
        symbol: row.symbol,
        timestamp: signalTs,
        exitReason: outcome.exitReason,
        pnlPercent: outcome.pnlPercent,
        maxGainPercent: outcome.maxGainPercent,
        maxDrawdownPercent: outcome.maxDrawdownPercent,
        outcomeSource: outcome.source,
        features
      };
      const labels = classifySample(base);
      const klineScoring = computeKlineScoring({ ...base, features });
      deduped.push({ ...base, ...labels, klineScoring });
      if (deduped.length >= limit) break;
    }
    return deduped;
  }

  runResearch({ limit = 500, roundTripCostPct = 3.5, entryDelayPenaltyPct = 5, goldThresholdPct = 100 } = {}) {
    const samples = this.loadSamples(limit).map((sample) => applyExecutionAssumptions(sample, {
      roundTripCostPct,
      entryDelayPenaltyPct,
      goldThresholdPct
    }));

    const sourceBreakdown = ['sourceType', 'isAth', 'hasIndices'].map((feature) => compareBooleanFeature(samples, feature));
    const thresholds = {
      superIndex: compareThreshold(samples, 'superIndex', [60, 80, 100, 120]),
      aiConfidence: compareThreshold(samples, 'aiConfidence', [40, 60, 80]),
      holders: compareThreshold(samples, 'holders', [50, 100, 200]),
      top10Inverse: [20, 25, 30, 35].map((threshold) => {
        const passed = samples.filter((sample) => toNum(sample.features?.top10Pct, 1000) <= threshold);
        return {
          feature: 'top10Pct',
          thresholdMax: threshold,
          count: passed.length,
          winRate: Number(ratio(passed.filter((s) => s.isWinner).length, passed.length).toFixed(4)),
          avgPnl: Number(mean(passed.map((s) => s.pnlPercent)).toFixed(4)),
          falsePositiveRate: Number(ratio(passed.filter((s) => s.falsePositive).length, passed.length).toFixed(4))
        };
      })
    };

    const klineVariants = [
      {
        name: 'score>=3',
        samples: samples.filter((sample) => sample.klineScoring.passed)
      },
      {
        name: 'score<3',
        samples: samples.filter((sample) => !sample.klineScoring.passed)
      },
      {
        name: 'super>=80 + score>=3',
        samples: samples.filter((sample) => sample.klineScoring.passed && sample.klineScoring.super80)
      }
    ].map((variant) => ({
      name: variant.name,
      count: variant.samples.length,
      winRate: Number(ratio(variant.samples.filter((s) => s.isWinner).length, variant.samples.length).toFixed(4)),
      avgPnl: Number(mean(variant.samples.map((s) => s.pnlPercent)).toFixed(4)),
      adjustedAvgPnl: Number(mean(variant.samples.map((s) => s.delayedPnl)).toFixed(4)),
      goldRate: Number(ratio(variant.samples.filter((s) => s.isGold).length, variant.samples.length).toFixed(4)),
      falsePositiveRate: Number(ratio(variant.samples.filter((s) => s.falsePositive).length, variant.samples.length).toFixed(4))
    }));

    const strategyLabels = {
      totalSamples: samples.length,
      winners: samples.filter((s) => s.isWinner).length,
      goldSamples: samples.filter((s) => s.isGold).length,
      falsePositives: samples.filter((s) => s.falsePositive).length,
      slippageSensitive: samples.filter((s) => s.slippageSensitive).length,
      stillProfitableAfterDelay: samples.filter((s) => s.stillProfitableAfterDelay).length,
      stillGoldAfterDelay: samples.filter((s) => s.stillGoldAfterDelay).length,
      pnl: summarizeGroup(samples, 'pnlPercent'),
      adjustedPnl: summarizeGroup(samples, 'delayedPnl')
    };

    const findings = [];
    const notAth = sourceBreakdown.find((item) => item.feature === 'isAth');
    const indices = sourceBreakdown.find((item) => item.feature === 'hasIndices');
    if (notAth) {
      findings.push({
        title: 'ATH vs NOT_ATH 对比',
        summary: `ATH样本 winRate=${(notAth.yes.winRate * 100).toFixed(1)}%，NOT_ATH样本 winRate=${(notAth.no.winRate * 100).toFixed(1)}%`,
        metric: notAth
      });
    }
    if (indices) {
      findings.push({
        title: '是否带 indices 对比',
        summary: `带indices样本 avgPnl=${indices.yes.avgPnl.toFixed(2)}，不带indices avgPnl=${indices.no.avgPnl.toFixed(2)}`,
        metric: indices
      });
    }
    if (klineVariants.length) {
      findings.push({
        title: '红K/low volume/active/super>=80 评分逻辑',
        summary: klineVariants.map((item) => `${item.name}: n=${item.count}, WR=${(item.winRate * 100).toFixed(1)}%, adjPnL=${item.adjustedAvgPnl.toFixed(2)}`).join(' | '),
        metric: klineVariants
      });
    }

    const gapAnalysis = [];
    if (samples.length < 100) {
      gapAnalysis.push({
        gapType: 'low_sample_size',
        severity: 'high',
        rationale: '当前特征研究样本数偏少，结论稳定性不足',
        suggestedData: ['继续扩大近期远程导入信号覆盖', '更多 signal_outcomes / paper_trade_records 对齐']
      });
    }
    const remoteSamples = samples.filter((s) => s.features.sourceType === 'remote-log');
    const richRemoteMetadataRatio = ratio(remoteSamples.filter((s) => s.features.aiConfidence > 0 && s.features.narrativeTier && s.features.qualityScore >= 6).length, remoteSamples.length || 1);
    if (remoteSamples.length > samples.length * 0.5 && richRemoteMetadataRatio < 0.6) {
      gapAnalysis.push({
        gapType: 'missing_rich_metadata',
        severity: 'medium',
        rationale: `REMOTE_LOG_IMPORT 样本较多且 rich metadata 覆盖率仅 ${(richRemoteMetadataRatio * 100).toFixed(1)}%`,
        suggestedData: ['补充 richer remote log parsing', '增加 signal snapshot join', '引入更多链上/社交上下文']
      });
    }
    if (strategyLabels.stillProfitableAfterDelay / Math.max(strategyLabels.totalSamples, 1) < 0.35) {
      gapAnalysis.push({
        gapType: 'execution_reality_gap',
        severity: 'high',
        rationale: '考虑延迟、滑点、成本后可盈利样本占比偏低',
        suggestedData: ['补充更细粒度entry后5m K线', '研究更严格的入场确认因子']
      });
    }

    return {
      generatedAt: new Date().toISOString(),
      config: { limit, roundTripCostPct, entryDelayPenaltyPct, goldThresholdPct },
      sampleStats: strategyLabels,
      sourceBreakdown,
      thresholds,
      klineVariants,
      findings,
      gapAnalysis,
      nextActions: [
        '基于研究结果生成策略草案',
        '若 gapAnalysis 非空，则进入研究缺口提案阶段',
        '继续在 daemon 周期中累积新的 findings'
      ]
    };
  }
}

export default StrategyFeatureResearch;
