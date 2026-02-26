/**
 * Soft Alpha Score Aggregator
 *
 * Combines all scoring modules and applies adjustments
 *
 * Total Formula:
 * Score = 0.25×Narrative + 0.25×Influence + 0.30×TG_Spread + 0.10×Graph + 0.10×Source
 *
 * Adjustments:
 * - Matrix Penalty (from TG_Spread, can be -20)
 * - X Validation (if x_authors < 2, multiply by 0.8)
 */

import TGSpreadScoring from './tg-spread.js';
import NarrativeDetector from './narrative-detector.js';

export class SoftAlphaScorer {
  constructor(config, db) {
    this.config = config;
    this.db = db;
    this.weights = config.soft_score_weights;

    // Initialize component scorers
    this.tgSpreadScorer = new TGSpreadScoring(config, db);
    this.narrativeDetector = new NarrativeDetector();
  }

  /**
   * Main entry: Calculate complete Soft Alpha Score
   *
   * @param {Object} socialData - social_snapshots data
   * @param {Object} tokenData - token basic info
   * @returns {Object} { score, breakdown, reasons }
   */
  async calculate(socialData, tokenData) {
    console.log(`🎯 [Soft Score] Calculating for ${tokenData.token_ca}`);

    // Component scores
    const narrative = this.calculateNarrative(socialData, tokenData);
    const influence = this.calculateInfluence(socialData);
    const tgSpread = this.tgSpreadScorer.calculate(socialData, tokenData.token_ca);
    const graph = this.calculateGraph(socialData);
    const source = this.calculateSource(socialData);

    // Weighted sum (before adjustments)
    const rawScore =
      narrative.score * this.weights.Narrative +
      influence.score * this.weights.Influence +
      tgSpread.score * this.weights.TG_Spread +
      graph.score * this.weights.Graph +
      source.score * this.weights.Source;

    // Matrix Penalty (already in TG_Spread score, but track separately)
    const matrixPenalty = tgSpread.breakdown.matrix_penalty.penalty;

    // X Validation adjustment
    const xMultiplier = this.calculateXValidationMultiplier(socialData);

    // Final score
    const finalScore = Math.max(0, Math.min(100, rawScore * xMultiplier));

    return {
      score: Math.round(finalScore),
      breakdown: {
        narrative,
        influence,
        tg_spread: tgSpread,
        graph,
        source
      },
      adjustments: {
        matrix_penalty: matrixPenalty,
        x_multiplier: xMultiplier
      },
      reasons: this.aggregateReasons([narrative, influence, tgSpread, graph, source])
    };
  }

  /**
   * Narrative Scoring (0-25 points)
   *
   * Data-driven narrative detection using NarrativeDetector module
   * Weights derived from real market data (CoinGecko, DeFi Llama, Messari)
   *
   * Components:
   * - Base narrative weight (0-10 scale from research)
   * - Confidence multiplier (based on keyword match density)
   * - Twitter validation bonus (+20% if Twitter confirms narrative)
   *
   * Final score: 0-25 points
   */
  calculateNarrative(socialData, tokenData) {
    // Extract Twitter data for validation
    const twitterData = {
      mention_count: socialData.twitter_mentions || 0,
      unique_authors: socialData.twitter_unique_authors || 0,
      kol_count: socialData.twitter_kol_count || 0,
      engagement: socialData.twitter_engagement || 0,
      sentiment: socialData.twitter_sentiment || 'neutral',
      top_tweets: [] // Not available from socialData, but detector handles this
    };

    // Use NarrativeDetector to detect narratives
    const detection = this.narrativeDetector.detect(tokenData, twitterData);

    const reasons = [];

    if (detection.topNarrative) {
      const narrative = detection.topNarrative;

      // Log detection details
      console.log(`   📖 Narrative: ${narrative.name} (weight: ${narrative.weight}/10, confidence: ${(narrative.confidence * 100).toFixed(0)}%)`);

      reasons.push(`Narrative: ${narrative.name.replace(/_/g, ' ')} (weight: ${narrative.weight}/10)`);

      if (narrative.confidence >= 0.8) {
        reasons.push(`High confidence match (${(narrative.confidence * 100).toFixed(0)}%)`);
      }

      if (narrative.matchedKeywords.length > 0) {
        reasons.push(`Keywords: ${narrative.matchedKeywords.slice(0, 3).join(', ')}`);
      }

      if (detection.breakdown.twitter_validated) {
        reasons.push('✨ Twitter validates narrative (+20% bonus)');
      }
    } else {
      reasons.push('No narrative detected');
    }

    return {
      score: detection.score,
      reasons,
      narrative_name: detection.topNarrative ? detection.topNarrative.name : null,
      all_narratives: detection.narratives
    };
  }

  /**
   * Influence Scoring (0-25 points)
   *
   * - TG channel quality (0-15): Based on Tier distribution
   * - X Tier1 hit (0-10): If Tier1 KOL mentioned
   */
  calculateInfluence(socialData) {
    let score = 0;
    const reasons = [];

    // Parse promoted channels
    const channels = typeof socialData.promoted_channels === 'string' ?
      JSON.parse(socialData.promoted_channels) : socialData.promoted_channels || [];

    // TG channel quality (0-15)
    if (channels.length > 0) {
      const tierACounts = channels.filter(ch => ch.tier === 'A').length;
      const tierBCounts = channels.filter(ch => ch.tier === 'B').length;
      const blacklistCounts = channels.filter(ch => ch.tier === 'BLACKLIST').length;

      if (blacklistCounts > 0) {
        score = 0;
        reasons.push(`Blacklisted channels detected: ${blacklistCounts}`);
        return { score, reasons };
      }

      const channelScore = tierACounts * 3 + tierBCounts * 1.5;
      score += Math.min(15, channelScore);

      if (tierACounts >= 2) {
        reasons.push(`Strong channel support: ${tierACounts} Tier A channels`);
      } else if (tierACounts >= 1) {
        reasons.push(`Decent channel support: ${tierACounts} Tier A, ${tierBCounts} Tier B`);
      } else {
        reasons.push(`Weak channel support: mostly Tier B/C`);
      }
    }

    // X Tier1 hit (0-10)
    if (socialData.x_tier1_hit) {
      score += 10;
      reasons.push('Tier 1 KOL endorsement detected');
    }

    return {
      score: Math.min(25, score),
      reasons: reasons.length > 0 ? reasons : ['No influence indicators']
    };
  }

  /**
   * Graph Scoring (0-10 points)
   *
   * Simplified:
   * - Check if channels are upstream (historically good lead time)
   * - Check if TG and X are synchronized (both rising)
   */
  calculateGraph(socialData) {
    let score = 5; // Default middle score
    const reasons = ['Graph analysis: standard'];

    // Check TG velocity (if positive and accelerating)
    if (socialData.tg_velocity && socialData.tg_velocity > 0.3) {
      score += 3;
      reasons.push('Strong TG velocity');
    }

    // Check X sync (if X data exists and recent)
    if (socialData.x_unique_authors_15m && socialData.x_unique_authors_15m >= 3) {
      score += 2;
      reasons.push('TG and X synchronized growth');
    }

    return {
      score: Math.min(10, score),
      reasons
    };
  }

  /**
   * Source Scoring (0-10 points)
   *
   * Based on time_lag from earliest mention
   *
   * < 5min  → 10 points
   * 5-15min → 5 points
   * > 20min → 0 points
   */
  calculateSource(socialData) {
    const timeLag = socialData.tg_time_lag;

    if (timeLag === null || timeLag === undefined) {
      return {
        score: 0,
        reasons: ['Source time lag Unknown']
      };
    }

    const thresholds = this.config.soft_score_thresholds.source;

    if (timeLag < thresholds.time_lag_excellent_min) {
      return {
        score: 10,
        reasons: [`Excellent timing: ${timeLag} min from first mention`]
      };
    } else if (timeLag < thresholds.time_lag_good_min) {
      return {
        score: 5,
        reasons: [`Good timing: ${timeLag} min from first mention`]
      };
    } else if (timeLag < thresholds.time_lag_poor_min) {
      return {
        score: 2,
        reasons: [`Late: ${timeLag} min from first mention`]
      };
    } else {
      return {
        score: 0,
        reasons: [`Too late: ${timeLag} min from first mention (missed early entry)`]
      };
    }
  }

  /**
   * X Validation Multiplier
   *
   * If X data is weak (< 2 unique authors in 15min) and mostly Tier C channels,
   * multiply final score by 0.8
   */
  calculateXValidationMultiplier(socialData) {
    const xAuthors = socialData.x_unique_authors_15m;
    const minAuthors = this.config.soft_score_thresholds.x_validation.min_unique_authors;

    // Parse channels
    const channels = typeof socialData.promoted_channels === 'string' ?
      JSON.parse(socialData.promoted_channels) : socialData.promoted_channels || [];

    const tierCCounts = channels.filter(ch => ch.tier === 'C').length;
    const tierCRatio = channels.length > 0 ? tierCCounts / channels.length : 0;

    // Apply penalty if X is weak AND mostly Tier C
    if ((xAuthors === null || xAuthors < minAuthors) && tierCRatio > 0.7) {
      return this.config.soft_score_thresholds.x_validation.score_multiplier_if_low;
    }

    return 1.0;
  }

  /**
   * Aggregate reasons from all components
   */
  aggregateReasons(components) {
    const allReasons = [];

    for (const component of components) {
      if (component.reasons && component.reasons.length > 0) {
        allReasons.push(...component.reasons);
      }
    }

    return allReasons;
  }

  /**
   * Persist score details to database
   */
  async persistScoreDetails(tokenCA, scoreResult) {
    try {
      const stmt = this.db.prepare(`
        INSERT INTO score_details (
          token_ca,
          calculated_at,
          narrative_score,
          narrative_reasons,
          influence_score,
          influence_reasons,
          tg_spread_score,
          tg_spread_reasons,
          graph_score,
          graph_reasons,
          source_score,
          source_reasons,
          matrix_penalty,
          x_validation_multiplier,
          total_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);

      stmt.run(
        tokenCA,
        Date.now(),
        scoreResult.breakdown.narrative.score,
        JSON.stringify(scoreResult.breakdown.narrative.reasons),
        scoreResult.breakdown.influence.score,
        JSON.stringify(scoreResult.breakdown.influence.reasons),
        scoreResult.breakdown.tg_spread.score,
        JSON.stringify(scoreResult.breakdown.tg_spread.reasons),
        scoreResult.breakdown.graph.score,
        JSON.stringify(scoreResult.breakdown.graph.reasons),
        scoreResult.breakdown.source.score,
        JSON.stringify(scoreResult.breakdown.source.reasons),
        scoreResult.adjustments.matrix_penalty,
        scoreResult.adjustments.x_multiplier,
        scoreResult.score
      );

      console.log('✅ [Soft Score] Score details persisted');
    } catch (error) {
      console.error('❌ [Soft Score] Failed to persist:', error.message);
    }
  }
}

export default SoftAlphaScorer;
