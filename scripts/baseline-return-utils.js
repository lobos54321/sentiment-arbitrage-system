function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function pctFromRatio(ratio) {
  return ratio == null ? null : ratio - 1;
}

function computeUnitAwareReturn(row = {}, peak = {}) {
  const unitDomain = row.unit_domain || '';
  const peakDomain = peak.peak_domain || '';

  if (unitDomain === 'usd_gmgn') {
    const baseline = numeric(row.baseline_price_usd_gmgn);
    const peakUsd = numeric(peak.peak_price_usd_gmgn);
    if (baseline == null || baseline <= 0 || peakUsd == null || peakUsd <= 0) {
      return {
        evaluable: false,
        reason: 'missing_usd_gmgn_baseline_or_peak',
        return_pct: null,
      };
    }
    return {
      evaluable: true,
      return_domain: 'usd_gmgn',
      return_calculation_rule: 'usd_gmgn_peak_over_usd_gmgn_baseline',
      return_pct: pctFromRatio(peakUsd / baseline),
    };
  }

  if (unitDomain === 'sol_curve' && peakDomain === 'sol_curve') {
    const baseline = numeric(row.baseline_price_sol_curve);
    const peakSol = numeric(peak.peak_price_sol_curve);
    if (baseline == null || baseline <= 0 || peakSol == null || peakSol <= 0) {
      return {
        evaluable: false,
        reason: 'missing_sol_curve_baseline_or_peak',
        return_pct: null,
      };
    }
    return {
      evaluable: true,
      return_domain: 'sol_curve',
      return_calculation_rule: 'sol_curve_peak_over_sol_curve_baseline',
      return_pct: pctFromRatio(peakSol / baseline),
    };
  }

  if (unitDomain === 'sol_curve' && peakDomain === 'usd_gmgn') {
    const baselineSol = numeric(row.baseline_price_sol_curve);
    const graduationSol = numeric(row.graduation_price_sol_curve ?? peak.graduation_price_sol_curve);
    const graduationUsd = numeric(row.graduation_price_usd_gmgn ?? peak.graduation_price_usd_gmgn);
    const peakUsd = numeric(peak.peak_price_usd_gmgn);
    if (
      baselineSol == null || baselineSol <= 0
      || graduationSol == null || graduationSol <= 0
      || graduationUsd == null || graduationUsd <= 0
      || peakUsd == null || peakUsd <= 0
    ) {
      return {
        evaluable: false,
        reason: 'missing_splice_bridge_or_peak',
        return_pct: null,
      };
    }
    const curveLeg = graduationSol / baselineSol;
    const gmgnLeg = peakUsd / graduationUsd;
    return {
      evaluable: true,
      return_domain: 'spliced_curve_to_gmgn',
      return_calculation_rule: 'graduation_sol_over_baseline_sol_times_peak_usd_over_graduation_usd',
      curve_leg_ratio: curveLeg,
      gmgn_leg_ratio: gmgnLeg,
      return_pct: pctFromRatio(curveLeg * gmgnLeg),
    };
  }

  return {
    evaluable: false,
    reason: `unsupported_unit_or_peak_domain:${unitDomain || 'missing'}:${peakDomain || 'missing'}`,
    return_pct: null,
  };
}

function tierFromReturnPct(returnPct) {
  const value = numeric(returnPct);
  if (value == null) return 'unknown';
  if (value >= 1.0) return 'gold';
  if (value >= 0.5) return 'silver';
  if (value >= 0.25) return 'bronze';
  return 'sub25';
}

export {
  computeUnitAwareReturn,
  tierFromReturnPct,
};
