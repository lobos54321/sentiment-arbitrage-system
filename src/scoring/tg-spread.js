/**
 * TG Spread Scoring Module (0-30 points)
 *
 * CORE of the entire scoring system
 * Measures Telegram diffusion quality and detects matrix manipulation
 *
 * Components:
 * - Quantity Score (0-15): Based on tg_ch_15m
 * - Independence Score (0-15): Tier weighting + cluster diversity
 * - Matrix Penalty (-20 max): MANDATORY detection of coordinated pumps
 */

export class TGSpreadScoring {
  constructor(config, db) {
    this.config = config;
    this.db = db;
    this.thresholds = config.soft_score_thresholds.tg_spread;

    // Tier weights for independence scoring
    this.tierWeights = {
      'A': 1.0,
      'B': 0.6,
      'C': 0.3,
      'BLACKLIST': 0
    };
  }

  /**
   * Main entry: Calculate TG Spread score
   *
   * Enhanced with Twitter validation data from Grok API
   *
   * @param {Object} socialData - social_snapshots data (now includes twitter_* fields)
   * @param {string} tokenCA - Token contract address
   * @returns {Object} { score, breakdown, penalty, reasons }
   */
  calculate(socialData, tokenCA) {
    console.log(`📊 [TG Spread] Scoring ${tokenCA}`);

    // Extract key metrics
    const {
      tg_ch_15m,
      tg_clusters_15m,
      promoted_channels,
      tg_accel,
      N_total,
      // Twitter data (from Grok API)
      twitter_mentions,
      twitter_unique_authors,
      twitter_kol_count,
      twitter_engagement,
      twitter_sentiment
    } = socialData;

    // Parse promoted channels if string
    const channels = typeof promoted_channels === 'string' ?
      JSON.parse(promoted_channels) : promoted_channels || [];

    // Component 1: Telegram Base Score (0-10) - Reduced from 15
    const telegramScore = this.calculateTelegramScore(tg_ch_15m);

    // Component 2: Twitter Validation Score (0-15) - NEW
    const twitterScore = this.calculateTwitterScore(
      twitter_mentions,
      twitter_unique_authors,
      twitter_kol_count,
      twitter_engagement,
      twitter_sentiment
    );

    // Component 3: Chain Social Signal (0-5) - NEW
    const chainSocialScore = this.calculateChainSocialScore(socialData);

    // Component 4: Independence Score (kept as is, 0-15) - Moved from original
    const independenceScore = this.calculateIndependenceScore(channels, tg_clusters_15m);

    // Component 5: Matrix Penalty (0 to -20) - Kept as is
    const matrixPenalty = this.calculateMatrixPenalty(tg_ch_15m, tg_clusters_15m, channels);

    // Total score (can go negative due to penalty)
    // NEW DISTRIBUTION: TG(10) + Twitter(15) + ChainSocial(5) = 30 max
    const rawScore =
      telegramScore.score +
      twitterScore.score +
      chainSocialScore.score +
      matrixPenalty.penalty;

    const finalScore = Math.max(0, Math.min(30, rawScore)); // Clamp to [0, 30]

    return {
      score: finalScore,
      breakdown: {
        telegram: telegramScore,
        twitter: twitterScore,
        chain_social: chainSocialScore,
        independence: independenceScore,
        matrix_penalty: matrixPenalty
      },
      reasons: [
        ...telegramScore.reasons,
        ...twitterScore.reasons,
        ...chainSocialScore.reasons,
        ...independenceScore.reasons,
        ...matrixPenalty.reasons
      ]
    };
  }

  /**
   * Telegram Score (0-10) - Reduced from original 0-15
   *
   * Based on number of Telegram channels in 15min window
   */
  calculateTelegramScore(tg_ch_15m) {
    const channels = tg_ch_15m || 1; // Use actual channel count, default 1

    let score, reason;

    if (channels >= 5) {
      score = 10;
      reason = `Strong TG spread: ${channels} channels in 15min`;
    } else if (channels >= 3) {
      score = 7;
      reason = `Good TG spread: ${channels} channels in 15min`;
    } else if (channels >= 2) {
      score = 5;
      reason = `Moderate TG spread: ${channels} channels in 15min`;
    } else {
      score = 2;
      reason = `Limited TG spread: ${channels} channel(s) in 15min`;
    }

    return {
      score,
      reasons: [reason]
    };
  }

  /**
   * Twitter Validation Score (0-15) - NEW
   *
   * Multi-component Twitter analysis from Grok API:
   * - Mention count (0-10 points)
   * - KOL participation (0-5 points)
   * - Bonus for high engagement
   */
  calculateTwitterScore(mentions, unique_authors, kol_count, engagement, sentiment) {
    let score = 0;
    const reasons = [];

    mentions = mentions || 0;
    unique_authors = unique_authors || 0;
    kol_count = kol_count || 0;
    engagement = engagement || 0;

    // Part 1: Mention count (0-10 points)
    if (mentions >= 50) {
      score += 10;
      reasons.push(`Exceptional Twitter activity: ${mentions} mentions`);
    } else if (mentions >= 20) {
      score += 7;
      reasons.push(`Strong Twitter activity: ${mentions} mentions`);
    } else if (mentions >= 10) {
      score += 5;
      reasons.push(`Moderate Twitter activity: ${mentions} mentions`);
    } else if (mentions >= 5) {
      score += 3;
      reasons.push(`Some Twitter activity: ${mentions} mentions`);
    } else {
      reasons.push(`Limited Twitter activity: ${mentions} mentions`);
    }

    // Part 2: KOL participation (0-5 points)
    if (kol_count >= 3) {
      score += 5;
      reasons.push(`Multiple KOL endorsements: ${kol_count} KOLs`);
    } else if (kol_count >= 1) {
      score += 3;
      reasons.push(`KOL mentioned: ${kol_count} KOL(s)`);
    }

    // Bonus: High engagement (add up to 2 points if engagement is exceptional)
    if (engagement >= 10000 && mentions >= 20) {
      score += 2;
      reasons.push(`Viral engagement: ${engagement} interactions`);
    }

    return {
      score: Math.min(15, score),
      reasons: reasons.length > 0 ? reasons : ['No Twitter activity detected']
    };
  }

  /**
   * Chain Social Signal (0-5 points) - NEW
   *
   * On-chain social indicators (DexScreener boosts, watchlist, etc.)
   * Placeholder for future implementation
   */
  calculateChainSocialScore(socialData) {
    let score = 0;
    const reasons = [];

    // TODO: Implement DexScreener boost detection
    // TODO: Implement watchlist count tracking
    // For now, give a small base score
    score = 2;
    reasons.push('Chain social signals: baseline');

    return {
      score,
      reasons
    };
  }

  /**
   * Independence Score (0-15)
   *
   * Rewards:
   * - Tier A channels appearing (higher weight)
   * - Diverse clusters (not all synchronized)
   *
   * Formula:
   * - Tier weighted average (0-10)
   * - Cluster bonus (0-5)
   */
  calculateIndependenceScore(channels, tg_clusters_15m) {
    if (!channels || channels.length === 0) {
      return {
        score: 0,
        reasons: ['No channel data available']
      };
    }

    // Part 1: Tier-weighted quality (0-10)
    const tierScore = this.calculateTierScore(channels);

    // Part 2: Cluster diversity bonus (0-5)
    const clusterBonus = this.calculateClusterBonus(tg_clusters_15m, channels.length);

    const totalScore = tierScore.score + clusterBonus.score;

    return {
      score: Math.min(15, totalScore),
      reasons: [
        ...tierScore.reasons,
        ...clusterBonus.reasons
      ]
    };
  }

  /**
   * Tier-weighted quality score (0-10)
   */
  calculateTierScore(channels) {
    // Count channels by tier
    const tierCounts = {
      A: 0,
      B: 0,
      C: 0,
      BLACKLIST: 0
    };

    for (const channel of channels) {
      const tier = channel.tier || 'C'; // Default to C if unknown
      tierCounts[tier] = (tierCounts[tier] || 0) + 1;
    }

    // Check for blacklist
    if (tierCounts.BLACKLIST > 0) {
      return {
        score: 0,
        reasons: [`WARNING: ${tierCounts.BLACKLIST} blacklisted channel(s) detected`]
      };
    }

    // Weighted average
    const totalWeight =
      tierCounts.A * this.tierWeights.A +
      tierCounts.B * this.tierWeights.B +
      tierCounts.C * this.tierWeights.C;

    const avgWeight = totalWeight / channels.length;

    // Convert to 0-10 scale
    const score = avgWeight * 10;

    const reasons = [
      `Channel quality: ${tierCounts.A} TierA, ${tierCounts.B} TierB, ${tierCounts.C} TierC`
    ];

    if (tierCounts.A >= 3) {
      reasons.push('✓ Multiple Tier A channels (high quality)');
    }

    return {
      score: Math.round(score),
      reasons
    };
  }

  /**
   * Cluster diversity bonus (0-5)
   *
   * More independent clusters = better
   * Clusters represent different "waves" of promotion
   */
  calculateClusterBonus(tg_clusters_15m, totalChannels) {
    if (tg_clusters_15m === null || tg_clusters_15m === undefined) {
      return {
        score: 0,
        reasons: ['Cluster data not available']
      };
    }

    const minClusters = this.thresholds.min_clusters;

    if (tg_clusters_15m >= minClusters) {
      return {
        score: 5,
        reasons: [`Good cluster diversity: ${tg_clusters_15m} independent clusters`]
      };
    } else if (tg_clusters_15m >= 2) {
      return {
        score: 2,
        reasons: [`Moderate cluster diversity: ${tg_clusters_15m} clusters`]
      };
    } else {
      return {
        score: 0,
        reasons: [`Poor cluster diversity: only ${tg_clusters_15m} cluster(s)`]
      };
    }
  }

  /**
   * Matrix Penalty (0 to -20)
   *
   * CRITICAL: Detect coordinated matrix pump schemes
   *
   * Triggers:
   * 1. tg_ch_15m ≥ 8 BUT tg_clusters_15m ≤ 2 → -20 points
   *    (High volume but all synchronized = matrix)
   *
   * 2. Synchronized posting (channels post within 1-2 min) → -10 to -20
   *
   * **NEW: Tier 1 豁免**
   * - If any Tier A channel participates, skip Matrix Penalty
   * - Tier A channels are high-quality, not part of matrices
   *
   * This is the SAFETY MECHANISM against广告矩阵盘
   */
  calculateMatrixPenalty(tg_ch_15m, tg_clusters_15m, channels) {
    let penalty = 0;
    const reasons = [];

    // Tier 1 豁免检查: If any Tier A channel participates, skip Matrix Penalty
    const hasTierA = channels && channels.some(ch => ch.tier === 'A');
    if (hasTierA) {
      return {
        penalty: 0,
        reasons: ['✅ Tier 1 channel detected - Matrix Penalty exempted'],
        tier1_exemption: true
      };
    }

    // Rule 1: High volume, low diversity
    if (tg_ch_15m >= this.thresholds.ch_15m_high &&
        tg_clusters_15m <= this.thresholds.matrix_penalty_threshold) {

      penalty -= 20;
      reasons.push(`⚠️  MATRIX DETECTED: ${tg_ch_15m} channels but only ${tg_clusters_15m} clusters`);
      reasons.push('This is likely a coordinated matrix pump scheme');
    }

    // Rule 2: Synchronized posting pattern
    const syncResult = this.detectSynchronizedPosting(channels);

    if (syncResult.isSynchronized) {
      const syncPenalty = syncResult.severity === 'HIGH' ? -20 : -10;
      penalty += syncPenalty;
      reasons.push(`⚠️  Synchronized posting detected: ${syncResult.reason}`);
    }

    // Rule 3: All from same tier C (bulk promo)
    if (channels.length >= 5) {
      const tierCCounts = channels.filter(ch => ch.tier === 'C').length;
      const tierCRatio = tierCCounts / channels.length;

      if (tierCRatio >= 0.9 && tg_ch_15m >= 6) {
        penalty -= 10;
        reasons.push('⚠️  90%+ channels are Tier C - likely paid bulk promotion');
      }
    }

    // Cap penalty
    penalty = Math.max(-20, penalty);

    if (penalty < 0) {
      reasons.push(`Total matrix penalty: ${penalty} points`);
    }

    return {
      penalty,
      reasons: reasons.length > 0 ? reasons : ['No matrix behavior detected']
    };
  }

  /**
   * Detect synchronized posting
   *
   * If many channels post within a tight time window (1-2 min),
   * it's likely automated/coordinated
   */
  detectSynchronizedPosting(channels) {
    if (!channels || channels.length < 4) {
      return { isSynchronized: false };
    }

    // Get timestamps
    const timestamps = channels
      .map(ch => ch.timestamp)
      .filter(ts => ts !== null && ts !== undefined)
      .sort((a, b) => a - b);

    if (timestamps.length < 4) {
      return { isSynchronized: false };
    }

    // Check for bursts (many channels within 2min)
    const burstWindow = 2 * 60 * 1000; // 2 minutes

    let maxBurstCount = 0;
    let burstStart = timestamps[0];

    for (let i = 0; i < timestamps.length; i++) {
      const burstEnd = timestamps[i] + burstWindow;
      const burstCount = timestamps.filter(ts => ts >= timestamps[i] && ts <= burstEnd).length;

      if (burstCount > maxBurstCount) {
        maxBurstCount = burstCount;
        burstStart = timestamps[i];
      }
    }

    // If >50% of channels posted within 2min window
    const burstRatio = maxBurstCount / timestamps.length;

    if (burstRatio >= 0.7 && maxBurstCount >= 5) {
      return {
        isSynchronized: true,
        severity: 'HIGH',
        reason: `${maxBurstCount} channels posted within 2 minutes (${(burstRatio * 100).toFixed(0)}% of total)`
      };
    } else if (burstRatio >= 0.5 && maxBurstCount >= 4) {
      return {
        isSynchronized: true,
        severity: 'MEDIUM',
        reason: `${maxBurstCount} channels posted within 2 minutes (${(burstRatio * 100).toFixed(0)}% of total)`
      };
    }

    return { isSynchronized: false };
  }

  /**
   * Batch calculate for multiple tokens
   */
  calculateBatch(socialDataArray) {
    return socialDataArray.map(data => ({
      token_ca: data.token_ca,
      ...this.calculate(data, data.token_ca)
    }));
  }
}

export default TGSpreadScoring;
