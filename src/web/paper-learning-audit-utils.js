function finiteNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function roundNumber(value, digits = 2) {
  const n = finiteNumber(value);
  if (n == null) return null;
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
}

function pct(value) {
  const n = finiteNumber(value);
  return n == null ? null : roundNumber(n * 100, 2);
}

function trustedPeakRatio(row = {}) {
  const trusted = finiteNumber(row.trusted_peak_pnl);
  if (trusted != null && trusted > 0) return trusted;
  const quote = finiteNumber(row.quote_peak_pnl);
  if (quote != null && quote > 0) return quote;
  if (row.trusted_peak_pnl === undefined && row.quote_peak_pnl === undefined) {
    return finiteNumber(row.peak_pnl);
  }
  return null;
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

function capitalTier(row = {}) {
  if (row.capital_tier) return String(row.capital_tier);
  const state = parseJsonObject(row.monitor_state_json);
  if (state.capitalTier) return String(state.capitalTier);
  const mode = String(row.entry_mode || state.entryMode || '').toLowerCase();
  const size = finiteNumber(row.position_size_sol) || 0;
  if (mode.includes('tiny') || mode.includes('probe') || mode.includes('scout') || (size > 0 && size <= 0.005)) return 'tiny_probe';
  if (mode.includes('lotto')) return 'lotto_main';
  if (String(row.strategy_stage || '').toLowerCase() === 'stage1' || size >= 0.02) return 'stage1_main';
  return 'unknown';
}

function cohort(row = {}) {
  const state = parseJsonObject(row.monitor_state_json);
  return String(state.resonanceCohort || state.sourceResonanceCohort || row.source_resonance_cohort || 'unknown');
}

function groupKey(row = {}) {
  return `${capitalTier(row)}|${row.entry_mode || 'unknown'}|${cohort(row)}`;
}

function emptyTrailGroup(key) {
  const [tier, mode, sourceCohort] = String(key).split('|');
  return {
    key,
    capital_tier: tier,
    entry_mode: mode,
    source_resonance_cohort: sourceCohort,
    trades: 0,
    quote_clean_counterfactual_n: 0,
    mark_only_counterfactual_n: 0,
    actual_avg_pnl_pct: null,
    actual_avg_giveback_pct: null,
    scenarios: {},
  };
}

function scenarioNames() {
  const scenarios = [];
  for (const peak of [0.10, 0.15]) {
    for (const lock of [0.60, 0.70, 0.80]) {
      scenarios.push({ name: `peak${Math.round(peak * 100)}_lock${Math.round(lock * 100)}`, peak, lock });
    }
  }
  return scenarios;
}

function maxQuoteCleanSample(samples = []) {
  let max = null;
  for (const sample of samples) {
    if (Number(sample.quote_success || 0) !== 1) continue;
    const pnl = finiteNumber(sample.quote_pnl);
    if (pnl == null) continue;
    max = max == null ? pnl : Math.max(max, pnl);
  }
  return max;
}

export function buildShadowTrailAudit({ trades = [], pathSamplesByTrade = new Map() } = {}) {
  const groups = new Map();
  const scenarios = scenarioNames();
  for (const trade of trades) {
    if (trade.pnl_pct == null) continue;
    const key = groupKey(trade);
    if (!groups.has(key)) groups.set(key, emptyTrailGroup(key));
    const group = groups.get(key);
    const actual = finiteNumber(trade.pnl_pct) || 0;
    const storedPeak = Math.max(trustedPeakRatio(trade) || 0, actual);
    const samples = pathSamplesByTrade.get(Number(trade.id)) || [];
    const quotePeak = maxQuoteCleanSample(samples);
    const peak = quotePeak == null ? storedPeak : Math.max(storedPeak, quotePeak);
    const counterfactualSource = quotePeak == null ? 'mark_only_counterfactual' : 'quote_clean_counterfactual';
    group.trades += 1;
    if (counterfactualSource === 'quote_clean_counterfactual') group.quote_clean_counterfactual_n += 1;
    else group.mark_only_counterfactual_n += 1;
    group._actualPnl = (group._actualPnl || 0) + actual;
    group._giveback = (group._giveback || 0) + Math.max(0, peak - actual);
    for (const scenario of scenarios) {
      const floor = peak >= scenario.peak ? Math.max(actual, peak * scenario.lock) : actual;
      if (!group.scenarios[scenario.name]) {
        group.scenarios[scenario.name] = {
          scenario: scenario.name,
          trades: 0,
          simulated_total_pnl: 0,
          actual_total_pnl: 0,
          improvement_total_pnl: 0,
        };
      }
      const row = group.scenarios[scenario.name];
      row.trades += 1;
      row.simulated_total_pnl += floor;
      row.actual_total_pnl += actual;
      row.improvement_total_pnl += floor - actual;
    }
  }
  return Array.from(groups.values()).map((group) => {
    const finalized = {
      ...group,
      actual_avg_pnl_pct: group.trades ? pct(group._actualPnl / group.trades) : null,
      actual_avg_giveback_pct: group.trades ? pct(group._giveback / group.trades) : null,
      scenarios: Object.values(group.scenarios).map((row) => ({
        scenario: row.scenario,
        trades: row.trades,
        simulated_avg_pnl_pct: row.trades ? pct(row.simulated_total_pnl / row.trades) : null,
        actual_avg_pnl_pct: row.trades ? pct(row.actual_total_pnl / row.trades) : null,
        improvement_avg_pct: row.trades ? pct(row.improvement_total_pnl / row.trades) : null,
      })).sort((a, b) => Number(b.improvement_avg_pct || 0) - Number(a.improvement_avg_pct || 0)),
    };
    delete finalized._actualPnl;
    delete finalized._giveback;
    return finalized;
  }).sort((a, b) => Number(b.actual_avg_giveback_pct || 0) - Number(a.actual_avg_giveback_pct || 0));
}

export function buildFastFailCounterfactualAudit({ trades = [], pathSamplesByTrade = new Map() } = {}) {
  const rows = [];
  for (const trade of trades) {
    const reason = String(trade.exit_reason || '').toLowerCase();
    if (!reason.includes('fast_fail') && !reason.includes('no_follow') && !reason.includes('doa')) continue;
    const exitTs = finiteNumber(trade.exit_ts);
    const samples = (pathSamplesByTrade.get(Number(trade.id)) || [])
      .filter((sample) => exitTs == null || Number(sample.sample_ts || 0) > exitTs);
    const quotePeaks = samples
      .filter((sample) => Number(sample.quote_success || 0) === 1)
      .map((sample) => finiteNumber(sample.quote_pnl))
      .filter((value) => value != null);
    const markPeaks = samples
      .map((sample) => finiteNumber(sample.mark_pnl))
      .filter((value) => value != null);
    const bestQuote = quotePeaks.length ? Math.max(...quotePeaks) : null;
    const bestMark = markPeaks.length ? Math.max(...markPeaks) : null;
    rows.push({
      trade_id: trade.id,
      symbol: trade.symbol || null,
      token_ca: trade.token_ca || null,
      entry_mode: trade.entry_mode || null,
      capital_tier: capitalTier(trade),
      exit_reason: trade.exit_reason || null,
      realized_pnl_pct: pct(trade.pnl_pct),
      peak_pnl_pct: pct(trustedPeakRatio(trade)),
      post_exit_sample_n: samples.length,
      post_exit_quote_clean_sample_n: quotePeaks.length,
      post_exit_quote_clean_peak_pct: pct(bestQuote),
      post_exit_mark_peak_pct: pct(bestMark),
      regret_20pct_quote_clean: bestQuote != null ? bestQuote >= 0.20 : null,
      data_quality: quotePeaks.length ? 'quote_clean_counterfactual' : (samples.length ? 'mark_only_counterfactual' : 'missing_post_exit_samples'),
    });
  }
  const quoteRows = rows.filter((row) => row.post_exit_quote_clean_sample_n > 0);
  return {
    trades: rows.length,
    quote_clean_coverage_n: quoteRows.length,
    quote_clean_coverage_pct: rows.length ? roundNumber((quoteRows.length / rows.length) * 100, 1) : null,
    regret_20pct_quote_clean_n: quoteRows.filter((row) => row.regret_20pct_quote_clean).length,
    rows: rows.slice(0, 100),
    note: 'Requires post-exit path samples; missing_post_exit_samples means the analyzer is installed but historical data is insufficient.',
  };
}

export function buildSampleGovernance(rows = [], minN = 30) {
  return rows.map((row) => {
    const n = Number(row.fills || row.n || 0);
    const avg = finiteNumber(row.avg_pnl);
    let decision = 'continue_sampling';
    let reason = `sample_n_below_${minN}`;
    if (n >= minN && avg != null && avg > 0) {
      decision = 'promotion_review';
      reason = 'n_ge_min_and_avg_positive_requires_bootstrap';
    } else if (n >= minN && avg != null && avg < 0) {
      decision = 'loss_budget_review';
      reason = 'n_ge_min_and_avg_negative';
    }
    return {
      entry_mode: row.entry_mode || 'unknown',
      fills: n,
      wins: Number(row.wins || 0),
      win_rate_pct: n ? roundNumber((Number(row.wins || 0) / n) * 100, 1) : null,
      avg_pnl_pct: pct(avg),
      decision,
      reason,
    };
  });
}
