/**
 * Decision Matrix
 *
 * Converts gate results + scores into actionable trading decisions
 *
 * Core Rule:
 * - ANY GREYLIST → WATCH_ONLY (Auto Buy disabled)
 * - PASS+PASS + High Score → AUTO_BUY
 * - REJECT → Ignore
 *
 * Output: {rating, action, position_tier, reasons}
 */

export class DecisionMatrix {
  constructor(config, db) {
    this.config = config;
    this.db = db;
    this.matrix = config.decision_matrix;
    this.positionTemplates = config.position_templates;
  }

  /**
   * Main entry: Make trading decision
   *
   * @param {Object} evaluation - Combined evaluation from all systems
   *   {
   *     token_ca, chain,
   *     hard_gate: {status, reasons},
   *     exit_gate: {status, reasons},
   *     soft_score: {score, breakdown, adjustments, reasons}
   *   }
   * @returns {Object} {rating, action, position_tier, position_size, reasons}
   */
  decide(evaluation) {
    console.log(`🎯 [Decision Matrix] Evaluating ${evaluation.chain}/${evaluation.token_ca}`);

    const {hard_gate, exit_gate, soft_score} = evaluation;
    const score = soft_score.score;

    // Critical Rule: ANY GREYLIST → WATCH_ONLY
    const hasGreylist = hard_gate.status === 'GREYLIST' || exit_gate.status === 'GREYLIST';

    // Critical Rule: ANY REJECT → Immediate rejection
    const hasReject = hard_gate.status === 'REJECT' || exit_gate.status === 'REJECT';

    if (hasReject) {
      return this.createDecision(
        'F', // Failed
        'REJECT',
        null,
        null,
        evaluation.chain,
        [...hard_gate.reasons, ...exit_gate.reasons]
      );
    }

    if (hasGreylist) {
      // GREYLIST: Can trade manually but no Auto Buy
      const {rating, tier} = this.getGreylistRating(score);

      return this.createDecision(
        rating,
        'WATCH_ONLY', // Manual only
        tier,
        this.getPositionSize(evaluation.chain, tier),
        evaluation.chain,
        [
          'GREYLIST detected - Auto Buy disabled',
          'Manual verification required',
          ...this.getGreylistReasons(hard_gate, exit_gate)
        ]
      );
    }

    // Both PASS - apply score-based matrix
    return this.applyScoreMatrix(evaluation);
  }

  /**
   * Apply score-based decision matrix for PASS+PASS scenarios
   */
  applyScoreMatrix(evaluation) {
    const score = evaluation.soft_score.score;
    const chain = evaluation.chain;

    // Find matching rule from decision matrix
    for (const rule of this.matrix.rules) {
      if (score >= rule.score_min && score <= rule.score_max) {
        const positionSize = this.getPositionSize(chain, rule.position_tier);

        return this.createDecision(
          rule.rating,
          rule.action,
          rule.position_tier,
          positionSize,
          chain,
          [
            `Score ${score}/100 → ${rule.rating} rating`,
            `Action: ${rule.action}`,
            ...this.getScoreReasons(evaluation.soft_score)
          ]
        );
      }
    }

    // Fallback (should not reach here with proper config)
    return this.createDecision(
      'D',
      'WATCH',
      'Small',
      this.getPositionSize(chain, 'Small'),
      chain,
      ['Score did not match any matrix rule - defaulting to WATCH']
    );
  }

  /**
   * Get rating for GREYLIST scenarios based on score
   */
  getGreylistRating(score) {
    if (score >= 60) {
      return {rating: 'B', tier: 'Normal'};
    } else if (score >= 40) {
      return {rating: 'C', tier: 'Small'};
    } else {
      return {rating: 'D', tier: 'Small'};
    }
  }

  /**
   * Get position size from templates
   */
  getPositionSize(chain, tier) {
    if (!tier) return null;
    return this.positionTemplates[chain][tier] || null;
  }

  /**
   * Create standardized decision object
   */
  createDecision(rating, action, positionTier, positionSize, chain, reasons) {
    return {
      rating,
      action,
      position_tier: positionTier,
      position_size: positionSize,
      chain,
      reasons,
      auto_buy_enabled: action === 'AUTO_BUY',
      timestamp: Date.now()
    };
  }

  /**
   * Extract relevant reasons from GREYLIST gates
   */
  getGreylistReasons(hardGate, exitGate) {
    const reasons = [];

    if (hardGate.status === 'GREYLIST') {
      reasons.push('Hard Gate: ' + hardGate.reasons.join(', '));
    }

    if (exitGate.status === 'GREYLIST') {
      reasons.push('Exit Gate: ' + exitGate.reasons.join(', '));
    }

    return reasons;
  }

  /**
   * Extract key reasons from soft score
   */
  getScoreReasons(softScore) {
    const reasons = [];

    // Highlight Matrix Penalty if present
    if (softScore.adjustments.matrix_penalty < 0) {
      reasons.push(`⚠️ Matrix Penalty: ${softScore.adjustments.matrix_penalty} points`);
    }

    // Highlight X validation if weak
    if (softScore.adjustments.x_multiplier < 1.0) {
      reasons.push(`⚠️ X Validation: ${(softScore.adjustments.x_multiplier * 100).toFixed(0)}%`);
    }

    // Add top component scores
    const components = [
      {name: 'TG Spread', score: softScore.breakdown.tg_spread.score, max: 30},
      {name: 'Narrative', score: softScore.breakdown.narrative.score, max: 25},
      {name: 'Influence', score: softScore.breakdown.influence.score, max: 25}
    ];

    components.sort((a, b) => (b.score / b.max) - (a.score / a.max));

    const topComponent = components[0];
    reasons.push(`Strongest: ${topComponent.name} (${topComponent.score}/${topComponent.max})`);

    return reasons;
  }

  /**
   * Batch decision making
   */
  decideBatch(evaluations) {
    return evaluations.map(evaluation => ({
      token_ca: evaluation.token_ca,
      chain: evaluation.chain,
      ...this.decide(evaluation)
    }));
  }

  /**
   * Persist decision to database
   */
  async persistDecision(tokenCA, decision, evaluation) {
    try {
      // Update tokens table with decision
      const updateStmt = this.db.prepare(`
        UPDATE tokens
        SET
          rating = ?,
          action = ?,
          position_tier = ?,
          position_size = ?,
          auto_buy_enabled = ?,
          decision_reasons = ?,
          decision_timestamp = ?
        WHERE token_ca = ?
      `);

      updateStmt.run(
        decision.rating,
        decision.action,
        decision.position_tier,
        decision.position_size,
        decision.auto_buy_enabled ? 1 : 0,
        JSON.stringify(decision.reasons),
        decision.timestamp,
        tokenCA
      );

      console.log('✅ [Decision Matrix] Decision persisted to database');
    } catch (error) {
      console.error('❌ [Decision Matrix] Failed to persist decision:', error.message);
    }
  }

  /**
   * Get decision summary statistics
   */
  getDecisionStats() {
    try {
      const stmt = this.db.prepare(`
        SELECT
          rating,
          action,
          COUNT(*) as count
        FROM tokens
        WHERE decision_timestamp IS NOT NULL
          AND decision_timestamp > ?
        GROUP BY rating, action
        ORDER BY rating
      `);

      const oneDayAgo = Date.now() - 24 * 60 * 60 * 1000;
      const stats = stmt.all(oneDayAgo);

      return {
        period: '24h',
        breakdown: stats,
        total: stats.reduce((sum, s) => sum + s.count, 0)
      };
    } catch (error) {
      console.error('❌ [Decision Matrix] Failed to get stats:', error.message);
      return null;
    }
  }
}

export default DecisionMatrix;
