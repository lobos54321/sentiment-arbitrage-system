function safeNumber(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function parseJsonObject(value) {
  if (!value || typeof value !== 'string') return {};
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function firstValue(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && String(value).trim() !== '') return value;
  }
  return null;
}

export function inferModeEvEntryMode(row) {
  const monitorState = parseJsonObject(row.monitor_state_json);
  const lottoState = parseJsonObject(row.lotto_state_json);
  const entryAudit = parseJsonObject(row.entry_execution_audit_json);
  const entryDecision = lottoState.entryDecision || {};
  const monitorContract = monitorState.entryDecisionContract || {};
  const auditContract = entryAudit.entryDecisionContract || {};
  return String(firstValue(
    row.entry_mode,
    monitorState.entryMode,
    monitorState.entry_mode,
    monitorState.smartEntryReason,
    monitorState.passReason,
    monitorContract.entry_mode,
    auditContract.entry_mode,
    entryDecision.entry_mode,
    lottoState.entry_mode,
    row.signal_route ? `${String(row.signal_route).toLowerCase()}_unknown` : null,
    row.strategy_stage,
    'unknown',
  ));
}

export function inferModeEvPolicyVersion(row) {
  const monitorState = parseJsonObject(row.monitor_state_json);
  const entryAudit = parseJsonObject(row.entry_execution_audit_json);
  return firstValue(
    monitorState.policyVersion,
    monitorState.policy_version,
    entryAudit.policyVersion,
    entryAudit.policy_version,
    null,
  );
}

export function inferModeEvQuoteGuardVersion(row) {
  const monitorState = parseJsonObject(row.monitor_state_json);
  const entryAudit = parseJsonObject(row.entry_execution_audit_json);
  return firstValue(
    monitorState.quoteGuardVersion,
    monitorState.quote_guard_version,
    entryAudit.quoteGuardVersion,
    entryAudit.quote_guard_version,
    null,
  );
}

export function inferModeEvRevivalCanary(row) {
  const monitorState = parseJsonObject(row.monitor_state_json);
  const entryAudit = parseJsonObject(row.entry_execution_audit_json);
  return Boolean(
    monitorState.revivalCanary
    || monitorState.revival_canary
    || entryAudit.revivalCanary
    || entryAudit.revival_canary
  );
}

export function modeEvBucket(entryMode, positionSizeSol) {
  const mode = String(entryMode || '').toLowerCase();
  const size = Number(positionSizeSol || 0);
  if (mode.includes('gmgn') && mode.includes('tiny_scout')) return 'gmgn_tiny_scout';
  if (mode.includes('tiny_scout')) return 'tiny_scout';
  if (mode.includes('tiny_probe')) return 'tiny_scout';
  if (mode.includes('probe') && size > 0 && size <= 0.005) return 'tiny_scout';
  if (mode.includes('scout') && size > 0 && size <= 0.005) return 'tiny_scout';
  if (mode.includes('scout') || mode.includes('probe')) return 'scout';
  return 'primary';
}

export function modeEvAccountingSource(row) {
  const exitAudit = parseJsonObject(row.exit_execution_audit_json);
  const monitorState = parseJsonObject(row.monitor_state_json);
  return String(firstValue(
    row.accounting_source,
    exitAudit.accountingSource,
    exitAudit.accounting_source,
    monitorState.accountingSource,
    '',
  ));
}

export function modeEvQuoteGapPct(row) {
  const exitAudit = parseJsonObject(row.exit_execution_audit_json);
  const monitorState = parseJsonObject(row.monitor_state_json);
  const candidates = [
    row.exit_quote_mark_gap_pct,
    row.max_path_quote_gap_pct,
    exitAudit.quoteMarkGapPct,
    exitAudit.exitQuoteMarkGapPct,
    monitorState.exitQuoteMarkGap,
    monitorState.maxPathQuoteGapPct,
  ]
    .map((value) => Math.abs(safeNumber(value, NaN)))
    .filter(Number.isFinite);
  return candidates.length ? Math.max(...candidates) : 0;
}

export function isModeEvQuoteClean(row, options = {}) {
  const quoteGapMaxPct = safeNumber(options.quoteGapMaxPct, 8);
  const source = modeEvAccountingSource(row);
  return !source.includes('quote_pnl_reprice') && modeEvQuoteGapPct(row) <= quoteGapMaxPct;
}

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function median(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function trimmedMean(values, fraction = 0.1) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const trim = Math.floor(sorted.length * fraction);
  const trimmed = sorted.slice(trim, sorted.length - trim || sorted.length);
  return mean(trimmed.length ? trimmed : sorted);
}

function seededRandom(seed = 1) {
  let x = seed >>> 0;
  return () => {
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    return (x >>> 0) / 4294967296;
  };
}

function bootstrapLowerBound(values, q = 0.05, iterations = 3000) {
  if (!values.length) return 0;
  const random = seededRandom(0x9e3779b9 + values.length);
  const results = [];
  for (let i = 0; i < iterations; i += 1) {
    let sum = 0;
    for (let j = 0; j < values.length; j += 1) {
      sum += values[Math.floor(random() * values.length)];
    }
    results.push(sum / values.length);
  }
  results.sort((a, b) => a - b);
  return results[Math.floor(q * (results.length - 1))];
}

function roundNumber(value, digits = 6) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  const factor = 10 ** digits;
  return Math.round(numeric * factor) / factor;
}

function summarizeRows(entryMode, bucket, rows, options = {}) {
  const extraCostPct = safeNumber(options.extraCostPct, 0);
  const minNetPct = safeNumber(options.unitEconomicsMinNetPct, 1.5);
  const minNetSol = safeNumber(options.unitEconomicsMinNetSol, 0.000045);
  const bootstrapIterations = Math.max(100, Math.floor(safeNumber(options.bootstrapIterations, 3000)));
  const quoteCleanRows = rows.filter((row) => isModeEvQuoteClean(row, options));
  const pnlPctValues = rows.map((row) => safeNumber(row.pnl_pct, 0) * 100 - extraCostPct);
  const pnlSolValues = rows.map((row) => safeNumber(row.position_size_sol, 0) * ((safeNumber(row.pnl_pct, 0) * 100 - extraCostPct) / 100));
  const totalSol = pnlSolValues.reduce((sum, value) => sum + value, 0);
  const leaveOneOutTotals = pnlSolValues.map((value) => totalSol - value);
  const maxPositive = Math.max(0, ...pnlSolValues.filter((value) => value > 0));
  const bootPct = bootstrapLowerBound(pnlPctValues, 0.05, bootstrapIterations);
  const bootSol = bootstrapLowerBound(pnlSolValues, 0.05, bootstrapIterations);
  return {
    bucket,
    entry_mode: entryMode,
    total: rows.length,
    closed: rows.filter((row) => row.exit_ts != null || row.exit_reason != null).length,
    unique_tokens: new Set(rows.map((row) => row.token_ca || row.lifecycle_id || row.id)).size,
    quote_clean_n: quoteCleanRows.length,
    quote_dirty_n: rows.length - quoteCleanRows.length,
    revival_canary_n: rows.filter((row) => row.revival_canary).length,
    policy_versions: [...new Set(rows.map((row) => row.policy_version).filter(Boolean))].sort(),
    quote_guard_versions: [...new Set(rows.map((row) => row.quote_guard_version).filter(Boolean))].sort(),
    wins: pnlPctValues.filter((value) => value > 0).length,
    losses: pnlPctValues.filter((value) => value <= 0).length,
    win_rate_pct: rows.length ? roundNumber((pnlPctValues.filter((value) => value > 0).length / rows.length) * 100, 2) : null,
    total_pnl_sol: roundNumber(totalSol, 8),
    avg_pnl_pct: roundNumber(mean(pnlPctValues), 4),
    median_pnl_pct: roundNumber(median(pnlPctValues), 4),
    trimmed_mean_pnl_pct: roundNumber(trimmedMean(pnlPctValues), 4),
    bootstrap_5pct_lb_pnl_pct: roundNumber(bootPct, 4),
    bootstrap_5pct_lb_sol_per_trade: roundNumber(bootSol, 8),
    leave_one_out_min_total_sol: roundNumber(leaveOneOutTotals.length ? Math.min(...leaveOneOutTotals) : 0, 8),
    max_single_trade_contribution_pct: totalSol > 0 && maxPositive > 0 ? roundNumber((maxPositive / totalSol) * 100, 2) : null,
    avg_position_size_sol: roundNumber(mean(rows.map((row) => safeNumber(row.position_size_sol, 0))), 6),
    max_quote_gap_pct: roundNumber(Math.max(0, ...rows.map(modeEvQuoteGapPct)), 4),
    pass_unit_economics: bootPct > minNetPct && bootSol > minNetSol,
  };
}

export function buildModeEvReport(rows, options = {}) {
  const cleanMode = String(options.clean || 'all').toLowerCase();
  const policyVersionFilter = String(options.policyVersion || '').trim();
  const revivalCanaryFilter = options.revivalCanary === true
    ? true
    : (options.revivalCanary === false ? false : null);
  const prepared = rows.map((row) => {
    const entryMode = inferModeEvEntryMode(row);
    const policyVersion = inferModeEvPolicyVersion(row);
    const quoteGuardVersion = inferModeEvQuoteGuardVersion(row);
    const revivalCanary = inferModeEvRevivalCanary(row);
    return {
      ...row,
      entry_mode: entryMode,
      bucket: modeEvBucket(entryMode, row.position_size_sol),
      policy_version: policyVersion,
      quote_guard_version: quoteGuardVersion,
      revival_canary: revivalCanary,
    };
  });
  let filtered = cleanMode === 'quote'
    ? prepared.filter((row) => isModeEvQuoteClean(row, options))
    : prepared;
  if (policyVersionFilter) {
    filtered = filtered.filter((row) => String(row.policy_version || '') === policyVersionFilter);
  }
  if (revivalCanaryFilter !== null) {
    filtered = filtered.filter((row) => row.revival_canary === revivalCanaryFilter);
  }
  const groups = new Map();
  for (const row of filtered) {
    const key = `${row.bucket}:${row.entry_mode}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }
  const byEntryMode = Array.from(groups.values())
    .map((group) => summarizeRows(group[0].entry_mode, group[0].bucket, group, options))
    .sort((a, b) => {
      if (a.bucket !== b.bucket) return a.bucket.localeCompare(b.bucket);
      return b.total - a.total;
    });
  const bucketSummary = {};
  for (const mode of byEntryMode) {
    if (!bucketSummary[mode.bucket]) {
      bucketSummary[mode.bucket] = {
        total: 0,
        closed: 0,
        total_pnl_sol: 0,
        quote_clean_n: 0,
        quote_dirty_n: 0,
        pass_unit_economics_n: 0,
      };
    }
    const summary = bucketSummary[mode.bucket];
    summary.total += mode.total;
    summary.closed += mode.closed;
    summary.total_pnl_sol += mode.total_pnl_sol || 0;
    summary.quote_clean_n += mode.quote_clean_n || 0;
    summary.quote_dirty_n += mode.quote_dirty_n || 0;
    if (mode.pass_unit_economics) summary.pass_unit_economics_n += 1;
  }
  for (const summary of Object.values(bucketSummary)) {
    summary.total_pnl_sol = roundNumber(summary.total_pnl_sol, 8);
  }
  return {
    clean: cleanMode,
    input_rows: rows.length,
    evaluated_rows: filtered.length,
    quote_clean_rows: prepared.filter((row) => isModeEvQuoteClean(row, options)).length,
    quote_dirty_rows: prepared.filter((row) => !isModeEvQuoteClean(row, options)).length,
    revival_canary_rows: prepared.filter((row) => row.revival_canary).length,
    policy_version_rows: prepared.reduce((acc, row) => {
      const key = row.policy_version || 'unversioned';
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {}),
    bucket_summary: bucketSummary,
    by_entry_mode: byEntryMode,
  };
}
