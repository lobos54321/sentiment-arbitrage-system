/**
 * Exit Gate Filter
 *
 * Can we actually exit if we enter?
 * Tests liquidity depth and structural risks
 *
 * KEY: Slippage test uses PLANNED POSITION SIZE (not fixed amount)
 *
 * Output: PASS / GREYLIST / REJECT + reasons[]
 */

export class ExitGateFilter {
  constructor(config) {
    this.config = config;
    this.thresholds = config.exit_gate_thresholds;
    this.slippageConfig = config.exit_gate_slippage;
  }

  /**
   * Main entry: Evaluate Exit Gate
   *
   * @param {Object} snapshot - Chain snapshot data
   * @param {number} plannedPosition - Position size in native currency (SOL/BNB)
   * @returns {Object} { status, reasons }
   */
  evaluate(snapshot, plannedPosition) {
    console.log(`🚪 [Exit Gate] Evaluating ${snapshot.chain}/${snapshot.token_ca} with position ${plannedPosition}`);

    if (!plannedPosition) {
      return {
        status: 'GREYLIST',
        reasons: ['No planned position provided - cannot test slippage accurately']
      };
    }

    if (snapshot.chain === 'SOL') {
      return this.evaluateSOL(snapshot, plannedPosition);
    } else if (snapshot.chain === 'BSC') {
      return this.evaluateBSC(snapshot, plannedPosition);
    }

    return {
      status: 'REJECT',
      reasons: ['Unsupported chain']
    };
  }

  /**
   * Evaluate SOL token exit feasibility
   *
   * Requirements:
   * - Liquidity ≥ 50 SOL
   * - Slippage (20% of position) < 2% (Pass) or < 5% (Greylist)
   * - Top10 < 30%
   * - Wash flag acceptable
   *
   * Unknown → GREYLIST
   */
  evaluateSOL(snapshot, plannedPosition) {
    const reasons = [];
    let hasUnknown = false;
    const thresholds = this.thresholds.SOL;

    // Check Liquidity
    if (snapshot.liquidity === null || snapshot.liquidity === undefined) {
      hasUnknown = true;
      reasons.push('Liquidity Unknown');
    } else if (snapshot.liquidity < thresholds.min_liquidity_sol) {
      return {
        status: 'REJECT',
        reasons: [`Liquidity ${snapshot.liquidity.toFixed(2)} SOL < minimum ${thresholds.min_liquidity_sol} SOL`]
      };
    }

    // Check Slippage (CRITICAL -按仓位测试)
    const slippageResult = this.checkSlippage(
      snapshot.slippage_sell_20pct,
      'SOL',
      plannedPosition
    );

    if (slippageResult.reject) {
      return {
        status: 'REJECT',
        reasons: slippageResult.reasons
      };
    }

    if (slippageResult.greylist) {
      hasUnknown = true;
      reasons.push(...slippageResult.reasons);
    }

    // Check Top10
    if (snapshot.top10_percent === null || snapshot.top10_percent === undefined) {
      hasUnknown = true;
      reasons.push('Top10 holder percentage Unknown');
    } else if (snapshot.top10_percent > thresholds.max_top10_percent) {
      return {
        status: 'REJECT',
        reasons: [`Top10 holders own ${snapshot.top10_percent.toFixed(1)}% (max: ${thresholds.max_top10_percent}%)`]
      };
    }

    // Check Wash Trading
    const washResult = this.checkWashTrading(snapshot, 'SOL');
    if (washResult.reject) {
      return {
        status: 'REJECT',
        reasons: washResult.reasons
      };
    }
    if (washResult.greylist) {
      hasUnknown = true;
      reasons.push(...washResult.reasons);
    }

    // Check Key Risk Wallets
    if (snapshot.key_risk_wallets && snapshot.key_risk_wallets.length > 0) {
      reasons.push(`Warning: ${snapshot.key_risk_wallets.length} key risk wallet(s) detected`);
      // This is a warning, not a rejection
    }

    // Decision
    if (hasUnknown) {
      return {
        status: 'GREYLIST',
        reasons: reasons.length > 0 ? reasons : ['Exit feasibility uncertain - needs verification']
      };
    }

    return {
      status: 'PASS',
      reasons: ['All SOL exit gate checks passed']
    };
  }

  /**
   * Evaluate BSC token exit feasibility
   *
   * Requirements (二级接力/CEX预期):
   * - Liquidity ≥ 100 BNB
   * - Volume 24h ≥ $500k
   * - Top10 < 40%
   * - No sell constraints
   *
   * Unknown → GREYLIST
   */
  evaluateBSC(snapshot, plannedPosition) {
    const reasons = [];
    let hasUnknown = false;
    const thresholds = this.thresholds.BSC;

    // Check Liquidity
    if (snapshot.liquidity === null || snapshot.liquidity === undefined) {
      hasUnknown = true;
      reasons.push('Liquidity Unknown');
    } else if (snapshot.liquidity < thresholds.min_liquidity_bnb) {
      return {
        status: 'REJECT',
        reasons: [`Liquidity ${snapshot.liquidity.toFixed(2)} BNB < minimum ${thresholds.min_liquidity_bnb} BNB`]
      };
    }

    // Check 24h Volume
    if (snapshot.vol_24h_usd === null || snapshot.vol_24h_usd === undefined) {
      hasUnknown = true;
      reasons.push('24h volume Unknown');
    } else if (snapshot.vol_24h_usd < thresholds.min_volume_24h_usd) {
      return {
        status: 'REJECT',
        reasons: [`24h volume $${snapshot.vol_24h_usd.toLocaleString()} < minimum $${thresholds.min_volume_24h_usd.toLocaleString()}`]
      };
    }

    // Check Top10
    if (snapshot.top10_percent === null || snapshot.top10_percent === undefined) {
      hasUnknown = true;
      reasons.push('Top10 holder percentage Unknown');
    } else if (snapshot.top10_percent > thresholds.max_top10_percent) {
      return {
        status: 'REJECT',
        reasons: [`Top10 holders own ${snapshot.top10_percent.toFixed(1)}% (max: ${thresholds.max_top10_percent}%)`]
      };
    }

    // Check Sell Constraints
    if (snapshot.sell_constraints_flag === null || snapshot.sell_constraints_flag === undefined) {
      hasUnknown = true;
      reasons.push('Sell constraints status Unknown');
    } else if (snapshot.sell_constraints_flag === true) {
      return {
        status: 'REJECT',
        reasons: [
          'Sell constraints detected',
          ...(snapshot.sell_constraints || [])
        ]
      };
    }

    // Check Slippage (if available)
    if (snapshot.slippage_sell_20pct !== null && snapshot.slippage_sell_20pct !== undefined) {
      const slippageResult = this.checkSlippage(
        snapshot.slippage_sell_20pct,
        'BSC',
        plannedPosition
      );

      if (slippageResult.reject) {
        return {
          status: 'REJECT',
          reasons: slippageResult.reasons
        };
      }

      if (slippageResult.greylist) {
        hasUnknown = true;
        reasons.push(...slippageResult.reasons);
      }
    }

    // Decision
    if (hasUnknown) {
      return {
        status: 'GREYLIST',
        reasons: reasons.length > 0 ? reasons : ['Exit feasibility uncertain - needs verification']
      };
    }

    return {
      status: 'PASS',
      reasons: ['All BSC exit gate checks passed']
    };
  }

  /**
   * Check slippage (CRITICAL: uses planned position size)
   *
   * Test: sell_amount = 20% of planned_position
   *
   * SOL:
   * - < 2% → Pass
   * - 2-5% → Greylist
   * - > 5% → Reject
   *
   * BSC:
   * - < 3% → Pass
   * - 3-8% → Greylist
   * - > 8% → Reject
   */
  checkSlippage(slippageValue, chain, plannedPosition) {
    if (slippageValue === null || slippageValue === undefined) {
      return {
        reject: false,
        greylist: true,
        reasons: ['Slippage test not performed or failed']
      };
    }

    const passThreshold = chain === 'SOL' ?
      this.slippageConfig.sol_pass_threshold_pct :
      this.slippageConfig.bsc_pass_threshold_pct;

    const rejectThreshold = chain === 'SOL' ?
      this.slippageConfig.sol_reject_threshold_pct :
      this.slippageConfig.bsc_reject_threshold_pct;

    // Reject if too high
    if (slippageValue > rejectThreshold) {
      return {
        reject: true,
        reasons: [
          `Slippage ${slippageValue.toFixed(2)}% exceeds maximum ${rejectThreshold}%`,
          `Tested with ${this.slippageConfig.test_sell_percentage}% of position (${(plannedPosition * 0.2).toFixed(3)} ${chain})`
        ]
      };
    }

    // Greylist if borderline
    if (slippageValue > passThreshold) {
      return {
        reject: false,
        greylist: true,
        reasons: [
          `Slippage ${slippageValue.toFixed(2)}% is borderline (pass threshold: ${passThreshold}%)`,
          'Liquidity may be thin - consider smaller position'
        ]
      };
    }

    // Pass
    return {
      reject: false,
      greylist: false,
      reasons: []
    };
  }

  /**
   * Check wash trading risk
   */
  checkWashTrading(snapshot, chain) {
    const washFlag = snapshot.wash_flag;

    if (washFlag === 'Unknown' || !washFlag) {
      return {
        reject: false,
        greylist: true,
        reasons: ['Wash trading detection not performed']
      };
    }

    const maxAcceptable = this.thresholds[chain]?.max_wash_with_risk || 'MEDIUM';

    // Reject if HIGH wash + other risks
    if (washFlag === 'HIGH') {
      // Check if combined with other risks
      const hasOtherRisks =
        (snapshot.top10_percent && snapshot.top10_percent > 25) ||
        (snapshot.slippage_sell_20pct && snapshot.slippage_sell_20pct > 3);

      if (hasOtherRisks) {
        return {
          reject: true,
          reasons: [
            'HIGH wash trading detected',
            'Combined with other structural risks (Top10 or slippage)',
            'High probability of paper gains'
          ]
        };
      }

      // HIGH wash but no other risks - greylist
      return {
        reject: false,
        greylist: true,
        reasons: ['HIGH wash trading detected - monitor closely']
      };
    }

    // MEDIUM or LOW - acceptable
    return {
      reject: false,
      greylist: false,
      reasons: []
    };
  }

  /**
   * Batch evaluate
   */
  evaluateBatch(snapshots, positionsByToken) {
    return snapshots.map(snapshot => {
      const plannedPosition = positionsByToken[snapshot.token_ca];
      return {
        token_ca: snapshot.token_ca,
        chain: snapshot.chain,
        planned_position: plannedPosition,
        ...this.evaluate(snapshot, plannedPosition)
      };
    });
  }
}

export default ExitGateFilter;
