/**
 * Hard Gate Filter
 *
 * Security-first filtering - one vote to reject
 * Implements strict safety checks for both SOL and BSC
 *
 * Output: PASS / GREYLIST / REJECT + reasons[]
 */

export class HardGateFilter {
  constructor(config) {
    this.config = config;
    this.thresholds = config.hard_gate_thresholds;
  }

  /**
   * Main entry: Evaluate Hard Gate for a token
   *
   * @param {Object} snapshot - Chain snapshot data
   * @returns {Object} { status: 'PASS'|'GREYLIST'|'REJECT', reasons: [] }
   */
  evaluate(snapshot) {
    console.log(`🚨 [Hard Gate] Evaluating ${snapshot.chain}/${snapshot.token_ca}`);

    if (snapshot.chain === 'SOL') {
      return this.evaluateSOL(snapshot);
    } else if (snapshot.chain === 'BSC') {
      return this.evaluateBSC(snapshot);
    }

    return {
      status: 'REJECT',
      reasons: ['Unsupported chain']
    };
  }

  /**
   * Evaluate SOL token
   *
   * Hard Requirements:
   * - Freeze Authority = Disabled
   * - Mint Authority = Disabled
   * - LP = Burned OR Locked >30 days
   *
   * Unknown → GREYLIST
   *
   * Pump.fun Exception:
   * - Bonding Curve mechanism doesn't have traditional LP until $69k market cap
   * - LP checks bypassed for pump.fun tokens
   * - Metadata lag is expected (RPC indexing delay)
   */
  evaluateSOL(snapshot) {
    const reasons = [];
    let hasUnknown = false;

    // 🚀 PUMP.FUN DETECTION: Special handling for Bonding Curve tokens
    const isPumpFun = snapshot.token_ca && snapshot.token_ca.toLowerCase().endsWith('pump');

    if (isPumpFun) {
      console.log(`🚀 [Pump.fun] Detected Bonding Curve token - applying special validation`);
    }

    // Check Freeze Authority
    if (snapshot.freeze_authority === 'Unknown') {
      hasUnknown = true;
      reasons.push('Freeze Authority Unknown - needs verification');
    } else if (snapshot.freeze_authority === 'Enabled') {
      return {
        status: 'REJECT',
        reasons: ['Freeze Authority is ENABLED - tokens can be frozen']
      };
    }

    // Check Mint Authority
    if (snapshot.mint_authority === 'Unknown') {
      hasUnknown = true;
      reasons.push('Mint Authority Unknown - needs verification');
    } else if (snapshot.mint_authority === 'Enabled') {
      return {
        status: 'REJECT',
        reasons: ['Mint Authority is ENABLED - unlimited minting possible']
      };
    }

    // Check LP Status
    // 🚀 PUMP.FUN BYPASS: Bonding Curve tokens don't have traditional LP
    if (isPumpFun) {
      console.log(`🚀 [Pump.fun] LP check bypassed - Bonding Curve mechanism (no LP until $69k market cap)`);
      // Pump.fun uses automated bonding curve - no LP rug pull risk
      // Skip LP burn/lock verification entirely
    } else {
      // Traditional AMM tokens require LP burn/lock verification
      if (snapshot.lp_status === 'Unknown' || !snapshot.lp_status) {
        hasUnknown = true;
        reasons.push('LP Status Unknown - cannot verify burn/lock');
      } else {
        const lpOk = this.checkLPStatus(snapshot);

        if (!lpOk.pass) {
          return {
            status: 'REJECT',
            reasons: lpOk.reasons
          };
        }

        if (lpOk.uncertain) {
          hasUnknown = true;
          reasons.push(...lpOk.reasons);
        }
      }
    }

    // Decision
    if (hasUnknown) {
      return {
        status: 'GREYLIST',
        reasons: reasons.length > 0 ? reasons : ['Critical data missing - manual verification required']
      };
    }

    return {
      status: 'PASS',
      reasons: ['All SOL hard gate checks passed']
    };
  }

  /**
   * Check LP status for SOL
   */
  checkLPStatus(snapshot) {
    const status = snapshot.lp_status;

    // Handle undefined/null status
    if (!status || status === 'Unknown') {
      return {
        pass: true,
        uncertain: true,
        reasons: ['LP status unknown - needs verification']
      };
    }

    // Acceptable statuses
    if (status === 'Burned' || (typeof status === 'string' && status.includes('Burned'))) {
      return { pass: true, uncertain: false, reasons: [] };
    }

    if (status === 'Locked' || (typeof status === 'string' && status.includes('Locked'))) {
      // Check duration if available
      if (snapshot.lp_lock_duration) {
        if (snapshot.lp_lock_duration >= this.thresholds.SOL.lp_lock_min_days) {
          return { pass: true, uncertain: false, reasons: [] };
        } else {
          return {
            pass: false,
            reasons: [`LP locked for only ${snapshot.lp_lock_duration} days (min: ${this.thresholds.SOL.lp_lock_min_days})`]
          };
        }
      }

      // Duration unknown but status is "Locked"
      return {
        pass: true,
        uncertain: true,
        reasons: ['LP appears locked but duration not verified']
      };
    }

    // Heuristic status (not definitive)
    if (typeof status === 'string' && status.includes('Likely')) {
      return {
        pass: true,
        uncertain: true,
        reasons: ['LP status based on heuristic - needs manual verification']
      };
    }

    // Not burned, not locked
    return {
      pass: false,
      reasons: ['LP is neither burned nor locked - high rug risk']
    };
  }

  /**
   * Evaluate BSC token
   *
   * Hard Requirements:
   * - Honeypot = Pass
   * - Tax ≤ 5% (buy AND sell)
   * - Tax immutable OR has hardcoded cap
   * - Owner = Renounced OR MultiSig OR TimeLock
   * - No dangerous functions (blacklist/trading control/etc) OR owner safe
   * - LP Locked >30 days
   *
   * Unknown → GREYLIST
   */
  evaluateBSC(snapshot) {
    const reasons = [];
    let hasUnknown = false;

    // Check Honeypot
    if (snapshot.honeypot === 'Unknown') {
      hasUnknown = true;
      reasons.push('Honeypot status Unknown - needs verification');
    } else if (snapshot.honeypot === 'Fail') {
      return {
        status: 'REJECT',
        reasons: [
          'Honeypot DETECTED - cannot sell',
          snapshot.honeypot_reason || 'Unknown reason'
        ]
      };
    }

    // Check Tax
    const taxResult = this.checkTax(snapshot);
    if (taxResult.reject) {
      return {
        status: 'REJECT',
        reasons: taxResult.reasons
      };
    }
    if (taxResult.unknown) {
      hasUnknown = true;
      reasons.push(...taxResult.reasons);
    }

    // Check Owner
    const ownerResult = this.checkOwner(snapshot);
    if (ownerResult.reject) {
      return {
        status: 'REJECT',
        reasons: ownerResult.reasons
      };
    }
    if (ownerResult.unknown) {
      hasUnknown = true;
      reasons.push(...ownerResult.reasons);
    }

    // Check Dangerous Functions
    const dangerResult = this.checkDangerousFunctions(snapshot);
    if (dangerResult.reject) {
      return {
        status: 'REJECT',
        reasons: dangerResult.reasons
      };
    }
    if (dangerResult.unknown) {
      hasUnknown = true;
      reasons.push(...dangerResult.reasons);
    }

    // Check LP Lock
    const lpResult = this.checkBSCLPLock(snapshot);
    if (lpResult.reject) {
      return {
        status: 'REJECT',
        reasons: lpResult.reasons
      };
    }
    if (lpResult.unknown) {
      hasUnknown = true;
      reasons.push(...lpResult.reasons);
    }

    // Decision
    if (hasUnknown) {
      return {
        status: 'GREYLIST',
        reasons: reasons.length > 0 ? reasons : ['Critical data missing - manual verification required']
      };
    }

    return {
      status: 'PASS',
      reasons: ['All BSC hard gate checks passed']
    };
  }

  /**
   * Check BSC tax compliance
   */
  checkTax(snapshot) {
    const maxTax = this.thresholds.BSC.max_tax_percent;

    // Check if tax data is available
    if (snapshot.tax_buy === null || snapshot.tax_buy === undefined) {
      return {
        reject: false,
        unknown: true,
        reasons: ['Buy tax Unknown']
      };
    }

    if (snapshot.tax_sell === null || snapshot.tax_sell === undefined) {
      return {
        reject: false,
        unknown: true,
        reasons: ['Sell tax Unknown']
      };
    }

    // Check tax values
    if (snapshot.tax_buy > maxTax) {
      return {
        reject: true,
        reasons: [`Buy tax ${snapshot.tax_buy}% exceeds maximum ${maxTax}%`]
      };
    }

    if (snapshot.tax_sell > maxTax) {
      return {
        reject: true,
        reasons: [`Sell tax ${snapshot.tax_sell}% exceeds maximum ${maxTax}%`]
      };
    }

    // Check mutability
    if (snapshot.tax_mutable === null || snapshot.tax_mutable === undefined) {
      return {
        reject: false,
        unknown: true,
        reasons: ['Tax mutability Unknown']
      };
    }

    if (snapshot.tax_mutable === true) {
      // Tax is mutable - check if there's a cap
      if (snapshot.tax_max_cap === null || snapshot.tax_max_cap === undefined) {
        return {
          reject: true,
          reasons: ['Tax is MUTABLE with no hardcoded cap - owner can change to 100%']
        };
      }

      if (snapshot.tax_max_cap > maxTax) {
        return {
          reject: true,
          reasons: [`Tax is mutable with cap of ${snapshot.tax_max_cap}% (max allowed: ${maxTax}%)`]
        };
      }

      // Has acceptable cap
      return {
        reject: false,
        unknown: false,
        reasons: []
      };
    }

    // Tax is immutable and within limits
    return {
      reject: false,
      unknown: false,
      reasons: []
    };
  }

  /**
   * Check BSC owner safety
   */
  checkOwner(snapshot) {
    if (!snapshot.owner_type || snapshot.owner_type === 'Unknown') {
      return {
        reject: false,
        unknown: true,
        reasons: ['Owner type Unknown or missing']
      };
    }

    const safeTypes = this.thresholds.BSC.owner_safe_types;

    // Check if owner type is in safe list
    if (safeTypes && Array.isArray(safeTypes) && safeTypes.includes(snapshot.owner_type)) {
      return {
        reject: false,
        unknown: false,
        reasons: []
      };
    }

    // Owner is EOA or unknown contract type - not safe
    return {
      reject: true,
      reasons: [`Owner type '${snapshot.owner_type}' is not safe - must be Renounced/MultiSig/TimeLock`]
    };
  }

  /**
   * Check dangerous functions
   */
  checkDangerousFunctions(snapshot) {
    const dangerousFuncs = snapshot.dangerous_functions || [];

    if (dangerousFuncs === null) {
      return {
        reject: false,
        unknown: true,
        reasons: ['Dangerous functions check not performed (contract not verified?)']
      };
    }

    if (dangerousFuncs.length === 0) {
      // No dangerous functions found
      return {
        reject: false,
        unknown: false,
        reasons: []
      };
    }

    // Has dangerous functions - check if owner is safe
    const ownerSafe = snapshot.owner_safe === true;

    if (!ownerSafe) {
      return {
        reject: true,
        reasons: [
          `Contract has dangerous functions: ${dangerousFuncs.join(', ')}`,
          'Owner is NOT safe (not renounced/multisig/timelock)'
        ]
      };
    }

    // Owner is safe, so dangerous functions are mitigated
    return {
      reject: false,
      unknown: false,
      reasons: []
    };
  }

  /**
   * Check BSC LP lock
   */
  checkBSCLPLock(snapshot) {
    if (snapshot.lp_lock === null || snapshot.lp_lock === undefined) {
      return {
        reject: false,
        unknown: true,
        reasons: ['LP lock status Unknown']
      };
    }

    if (snapshot.lp_lock === false) {
      return {
        reject: true,
        reasons: ['LP is NOT locked - high rug risk']
      };
    }

    // LP is locked - check duration
    if (snapshot.lp_lock_duration) {
      if (snapshot.lp_lock_duration >= this.thresholds.BSC.lp_lock_min_days) {
        return {
          reject: false,
          unknown: false,
          reasons: []
        };
      } else {
        return {
          reject: true,
          reasons: [`LP locked for only ${snapshot.lp_lock_duration} days (min: ${this.thresholds.BSC.lp_lock_min_days})`]
        };
      }
    }

    // Lock duration unknown but status says locked
    return {
      reject: false,
      unknown: true,
      reasons: ['LP appears locked but duration not verified']
    };
  }

  /**
   * Batch evaluate multiple tokens
   */
  evaluateBatch(snapshots) {
    return snapshots.map(snapshot => ({
      token_ca: snapshot.token_ca,
      chain: snapshot.chain,
      ...this.evaluate(snapshot)
    }));
  }
}

export default HardGateFilter;
