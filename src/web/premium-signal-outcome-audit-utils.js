function numeric(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function positiveNumeric(value) {
  const n = numeric(value);
  return n != null && n > 0 ? n : null;
}

function normalizeTimestampSec(row) {
  const raw = row.timestamp_sec ?? row.timestamp ?? row.receive_ts ?? row.created_ts;
  const n = numeric(raw);
  if (n != null) return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
  const parsed = Date.parse(row.created_at || row.updated_at || '');
  return Number.isNaN(parsed) ? null : Math.floor(parsed / 1000);
}

function roundNumber(value, digits = 3) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
}

function tierForPct(pct) {
  if (pct === undefined || pct === null || pct === '') return 'unknown';
  if (!Number.isFinite(Number(pct))) return 'unknown';
  if (pct >= 100) return 'gold';
  if (pct >= 50) return 'silver';
  if (pct >= 25) return 'bronze';
  return 'sub25';
}

function emptyTierCounts() {
  return { gold: 0, silver: 0, bronze: 0, sub25: 0, unknown: 0 };
}

function incrementTier(counts, tier) {
  counts[tier] = (counts[tier] || 0) + 1;
}

function bestSymbol(rows) {
  for (const row of rows) {
    const symbol = String(row.symbol || '').trim();
    if (symbol && symbol.toUpperCase() !== 'UNKNOWN') return symbol;
  }
  return String(rows[0]?.symbol || 'UNKNOWN');
}

function compactStatusCounts(rows) {
  const counts = {};
  for (const row of rows) {
    const key = String(row.hard_gate_status || 'unknown');
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

function compactTypeCounts(rows) {
  const counts = {};
  for (const row of rows) {
    const key = String(row.signal_type || 'unknown');
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

const LOTTO_OBSERVE_UPSTREAM_STATUSES = [
  'LOTTO_OBSERVE_LOW_MC_VOL',
  'NOT_ATH_PREBUY_KLINE_BLOCK',
  'NOT_ATH_PREBUY_KLINE_UNKNOWN_DATA_BLOCKED',
  'NOT_ATH_PREBUY_KLINE_RETRY_EXPIRED',
  'NOT_ATH_V17',
  'ILLIQUID_JUNK',
  'NOT_ATH_V14',
  'NOT_ATH_V13',
  'NOT_ATH_V16',
  'INSUFFICIENT_KLINE',
  'NO_MC_DATA',
];

const OBSERVE_ONLY_STATUS_LIST = [
  'RISK_BLOCKED',
  ...LOTTO_OBSERVE_UPSTREAM_STATUSES,
];

const OBSERVE_ONLY_STATUSES = new Set(OBSERVE_ONLY_STATUS_LIST);

const SAFETY_REJECT_STATUS_LIST = [
  'GREYLIST',
  'WASH_HIGH',
  'GREYLIST_LOW_CONF',
  'REJECT',
  'RED_K_FAIL',
  '5M_DUMP',
  '5M_OVERHEAT',
  'HONEYPOT',
  'RUG_PULL_RISK',
];

const SAFETY_REJECT_STATUSES = new Set(SAFETY_REJECT_STATUS_LIST);

function coverageForToken(outcome) {
  if (outcome.paper_trade_count > 0) {
    return {
      class: 'paper_trade',
      reason: 'paper_trade_present',
      coverage_gap: false,
    };
  }
  const statuses = new Set(Object.keys(outcome.gate_statuses || {}).map((status) => String(status || '').toUpperCase()));
  if ([...statuses].some((status) => SAFETY_REJECT_STATUSES.has(status))) {
    return {
      class: 'safety_reject',
      reason: 'hard_safety_status',
      coverage_gap: false,
    };
  }
  if (
    outcome.missed_attribution_count > 0
    || [...statuses].some((status) => OBSERVE_ONLY_STATUSES.has(status))
  ) {
    return {
      class: 'observe_only',
      reason: outcome.missed_attribution_count > 0 ? 'missed_attribution_present' : 'observe_only_status',
      coverage_gap: false,
    };
  }
  if (outcome.hard_gate_pass) {
    return {
      class: 'observe_only',
      reason: 'hard_gate_pass_without_paper_trade_or_missed_attribution',
      coverage_gap: true,
    };
  }
  return {
    class: 'observe_only',
    reason: 'unmapped_status_observe_only',
    coverage_gap: true,
  };
}

function coverageClassForToken(outcome) {
  return coverageForToken(outcome).class;
}

function isoFromSec(sec) {
  const n = numeric(sec);
  return n == null ? null : new Date(n * 1000).toISOString();
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

function boolOrNull(value) {
  if (value == null) return null;
  if (typeof value === 'boolean') return value;
  const s = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(s)) return true;
  if (['0', 'false', 'no', 'off'].includes(s)) return false;
  return Boolean(Number(value));
}

function tradeResonanceContext(row) {
  const monitorState = parseJsonObject(row.monitor_state_json);
  const entryAudit = parseJsonObject(row.entry_execution_audit_json);
  return {
    source_resonance_cohort:
      row.source_resonance_cohort ||
      monitorState.resonance_cohort ||
      monitorState.resonanceCohort ||
      monitorState.source_resonance_cohort ||
      entryAudit.resonance_cohort ||
      null,
    gmgn_pre_seen:
      row.gmgn_pre_seen ??
      monitorState.gmgn_pre_seen ??
      monitorState.gmgnPreSeen ??
      entryAudit.gmgn_pre_seen ??
      null,
    gmgn_lead_time_sec:
      row.gmgn_lead_time_sec ??
      monitorState.gmgn_lead_time_sec ??
      monitorState.gmgnLeadTimeSec ??
      entryAudit.gmgn_lead_time_sec ??
      null,
  };
}

function serializeTrade(row) {
  const resonance = tradeResonanceContext(row);
  return {
    id: row.id,
    symbol: row.symbol || null,
    entry_ts: row.entry_ts ?? null,
    entry_iso: isoFromSec(row.entry_ts),
    exit_ts: row.exit_ts ?? null,
    exit_iso: isoFromSec(row.exit_ts),
    entry_mode: row.entry_mode || null,
    signal_route: row.signal_route || null,
    pnl_pct: row.pnl_pct == null ? null : roundNumber(Number(row.pnl_pct) * 100, 2),
    peak_pnl_pct: row.peak_pnl == null ? null : roundNumber(Number(row.peak_pnl) * 100, 2),
    mark_peak_pnl_pct: row.mark_peak_pnl == null ? null : roundNumber(Number(row.mark_peak_pnl) * 100, 2),
    peak_trust_status: row.peak_trust_status || null,
    position_size_sol: row.position_size_sol == null ? null : Number(row.position_size_sol),
    source_resonance_cohort: resonance.source_resonance_cohort,
    gmgn_pre_seen: boolOrNull(resonance.gmgn_pre_seen),
    gmgn_lead_time_sec: resonance.gmgn_lead_time_sec ?? null,
  };
}

function serializeMissed(row) {
  if (!row) return null;
  const maxPnl = numeric(row.max_pnl ?? row.max_pnl_recorded ?? row.tradable_peak_pnl ?? row.pnl_24h ?? row.pnl_60m ?? row.pnl_15m ?? row.pnl_5m);
  const markPnl = numeric(row.row_mark_pnl ?? row.mark_pnl);
  const tradablePeakPnl = numeric(row.tradable_peak_pnl);
  const firstTradablePnl = numeric(row.first_tradable_pnl);
  const quoteClean = row.quote_clean ?? (
    row.tradable_missed != null
      ? (Number(row.tradable_missed || 0) === 1 && Number(row.would_stop_before_peak || 0) !== 1 ? 1 : 0)
      : null
  );
  return {
    count: Number(row.n || row.count || 0),
    signal_id: row.signal_id ?? null,
    signal_ts: row.signal_ts ?? null,
    route: row.route || null,
    entry_mode_candidate: row.entry_mode_candidate || row.entry_mode || null,
    final_component: row.component || row.final_component || null,
    final_reason: row.reject_reason || row.final_reason || null,
    final_decision: row.decision || null,
    final_blocker: {
      component: row.component || row.final_component || null,
      reason: row.reject_reason || row.final_reason || null,
      route: row.route || null,
      decision: row.decision || null,
    },
    quote_clean: quoteClean == null ? null : Number(quoteClean) === 1,
    tradability_status: row.tradability_status || null,
    tradability_reason: row.tradability_reason || null,
    tradable_missed: row.tradable_missed == null ? null : Number(row.tradable_missed) === 1,
    would_stop_before_peak: row.would_stop_before_peak == null ? null : Number(row.would_stop_before_peak) === 1,
    first_tradable_pnl_pct: firstTradablePnl == null ? null : roundNumber(firstTradablePnl * 100, 2),
    tradable_peak_pnl_pct: tradablePeakPnl == null ? null : roundNumber(tradablePeakPnl * 100, 2),
    max_pnl_pct: maxPnl == null ? null : roundNumber(maxPnl * 100, 2),
    mark_pnl_pct: markPnl == null ? null : roundNumber(markPnl * 100, 2),
    peak_trust_status: row.peak_trust_status || null,
    source_resonance_cohort: row.source_resonance_cohort || row.cohort || null,
    source_resonance_level: row.source_resonance_level ?? row.resonance_level ?? null,
    source_resonance_score: row.source_resonance_score ?? row.resonance_score ?? null,
    gmgn_pre_seen: row.gmgn_pre_seen == null ? null : Number(row.gmgn_pre_seen) === 1,
    gmgn_lead_time_sec: row.gmgn_lead_time_sec ?? null,
  };
}

function mergeMissedAttribution(existing, row) {
  const serialized = serializeMissed(row);
  if (!serialized) return existing;
  const current = existing || {
    n: 0,
    max_pnl_pct: null,
    detail: null,
  };
  current.n += serialized.count || 1;
  if (serialized.max_pnl_pct != null && (current.max_pnl_pct == null || serialized.max_pnl_pct > current.max_pnl_pct)) {
    current.max_pnl_pct = serialized.max_pnl_pct;
    current.detail = serialized;
  } else if (!current.detail) {
    current.detail = serialized;
  }
  return current;
}

function tokenOutcome(rows, paperTradesByToken, missedByToken) {
  const sorted = [...rows].sort((a, b) => {
    const aTs = normalizeTimestampSec(a) || 0;
    const bTs = normalizeTimestampSec(b) || 0;
    if (aTs !== bTs) return aTs - bTs;
    return Number(a.id || 0) - Number(b.id || 0);
  });
  const withMc = sorted
    .map((row) => ({ row, ts: normalizeTimestampSec(row), marketCap: positiveNumeric(row.market_cap) }))
    .filter((item) => item.marketCap != null);
  const first = withMc[0] || { row: sorted[0], ts: normalizeTimestampSec(sorted[0]), marketCap: null };
  const max = withMc.reduce((best, item) => (!best || item.marketCap > best.marketCap ? item : best), null);
  const passRows = withMc.filter((item) => String(item.row.hard_gate_status || '') === 'PASS');
  const firstPass = passRows[0] || null;
  const afterPass = firstPass ? withMc.filter((item) => (item.ts || 0) >= (firstPass.ts || 0)) : [];
  const maxAfterPass = afterPass.reduce((best, item) => (!best || item.marketCap > best.marketCap ? item : best), null);
  const streamPnlPct = first.marketCap && max?.marketCap
    ? ((max.marketCap / first.marketCap) - 1) * 100
    : null;
  const passToMaxPct = firstPass?.marketCap && maxAfterPass?.marketCap
    ? ((maxAfterPass.marketCap / firstPass.marketCap) - 1) * 100
    : null;
  const tokenCa = sorted[0]?.token_ca || null;
  const paperTrades = tokenCa ? (paperTradesByToken.get(tokenCa) || []) : [];
  const missed = tokenCa ? missedByToken.get(tokenCa) : null;
  const missedDetail = missed?.detail || null;
  const serializedPaperTrades = paperTrades.map(serializeTrade);
  const firstTradeCohort = serializedPaperTrades.find((trade) => trade.source_resonance_cohort)?.source_resonance_cohort || null;
  const firstTradeGmgnPreSeen = serializedPaperTrades.find((trade) => trade.gmgn_pre_seen != null)?.gmgn_pre_seen ?? null;

  return {
    token_ca: tokenCa,
    symbol: bestSymbol(sorted),
    signal_rows: sorted.length,
    signal_types: compactTypeCounts(sorted),
    gate_statuses: compactStatusCounts(sorted),
    first_signal_id: first.row?.id ?? null,
    first_signal_ts: first.ts ?? null,
    first_signal_iso: isoFromSec(first.ts),
    first_market_cap: first.marketCap,
    max_signal_id: max?.row?.id ?? null,
    max_signal_ts: max?.ts ?? null,
    max_signal_iso: isoFromSec(max?.ts),
    max_market_cap: max?.marketCap ?? null,
    stream_to_max_pct: roundNumber(streamPnlPct, 2),
    stream_tier: tierForPct(streamPnlPct),
    hard_gate_pass: Boolean(firstPass),
    first_pass_signal_id: firstPass?.row?.id ?? null,
    first_pass_ts: firstPass?.ts ?? null,
    first_pass_iso: isoFromSec(firstPass?.ts),
    first_pass_market_cap: firstPass?.marketCap ?? null,
    max_after_pass_signal_id: maxAfterPass?.row?.id ?? null,
    max_after_pass_ts: maxAfterPass?.ts ?? null,
    max_after_pass_iso: isoFromSec(maxAfterPass?.ts),
    max_after_pass_market_cap: maxAfterPass?.marketCap ?? null,
    pass_to_max_pct: roundNumber(passToMaxPct, 2),
    pass_to_max_tier: tierForPct(passToMaxPct),
    paper_trade_count: paperTrades.length,
    paper_trades: serializedPaperTrades,
    missed_attribution_count: missed?.n || 0,
    missed_attribution_max_pnl_pct: missed?.max_pnl_pct ?? null,
    missed_attribution: missedDetail,
    entry_mode_candidate: missedDetail?.entry_mode_candidate || (paperTrades[0]?.entry_mode ?? null),
    final_component: missedDetail?.final_component || null,
    final_reason: missedDetail?.final_reason || null,
    final_blocker: missedDetail?.final_blocker || null,
    quote_clean: missedDetail?.quote_clean ?? null,
    tradable_peak_pnl_pct: missedDetail?.tradable_peak_pnl_pct ?? null,
    source_resonance_cohort: missedDetail?.source_resonance_cohort || firstTradeCohort || null,
    gmgn_pre_seen: missedDetail?.gmgn_pre_seen ?? firstTradeGmgnPreSeen ?? null,
  };
}

function buildCohortScoreboard(tokens) {
  const groups = new Map();
  const ensure = (cohort) => {
    const key = cohort || 'unknown';
    if (!groups.has(key)) {
      groups.set(key, {
        cohort: key,
        unique_tokens: 0,
        hard_gate_pass_unique: 0,
        paper_filled_unique: 0,
        missed_unique: 0,
        gold: 0,
        silver: 0,
        bronze: 0,
        quote_clean_dog: 0,
        realized_trades: 0,
        realized_wins: 0,
        realized_pnl_sum: 0,
        peak_pnl_sum: 0,
        final_blockers: new Map(),
      });
    }
    return groups.get(key);
  };
  for (const item of tokens) {
    const cohort = item.source_resonance_cohort || 'unknown';
    const group = ensure(cohort);
    group.unique_tokens += 1;
    if (item.hard_gate_pass) group.hard_gate_pass_unique += 1;
    if (item.paper_trade_count > 0) group.paper_filled_unique += 1;
    if (item.missed_attribution_count > 0 || item.coverage_gap) group.missed_unique += 1;
    if (item.pass_to_max_tier === 'gold') group.gold += 1;
    if (item.pass_to_max_tier === 'silver') group.silver += 1;
    if (item.pass_to_max_tier === 'bronze') group.bronze += 1;
    if (
      item.quote_clean === true
      && ['gold', 'silver', 'bronze'].includes(item.pass_to_max_tier)
    ) {
      group.quote_clean_dog += 1;
    }
    for (const trade of item.paper_trades || []) {
      if (trade.pnl_pct == null) continue;
      group.realized_trades += 1;
      if (Number(trade.pnl_pct) > 0) group.realized_wins += 1;
      group.realized_pnl_sum += Number(trade.pnl_pct);
      if (trade.peak_pnl_pct != null) group.peak_pnl_sum += Number(trade.peak_pnl_pct);
    }
    const blocker = item.final_blocker
      ? `${item.final_blocker.component || 'unknown'}:${item.final_blocker.reason || 'unknown'}`
      : null;
    if (blocker) group.final_blockers.set(blocker, (group.final_blockers.get(blocker) || 0) + 1);
  }
  return Array.from(groups.values())
    .map((group) => ({
      cohort: group.cohort,
      unique_tokens: group.unique_tokens,
      hard_gate_pass_unique: group.hard_gate_pass_unique,
      paper_filled_unique: group.paper_filled_unique,
      missed_unique: group.missed_unique,
      gold: group.gold,
      silver: group.silver,
      bronze: group.bronze,
      quote_clean_dog: group.quote_clean_dog,
      realized_win_rate: group.realized_trades > 0
        ? roundNumber(group.realized_wins / group.realized_trades, 4)
        : null,
      avg_realized_pnl_pct: group.realized_trades > 0
        ? roundNumber(group.realized_pnl_sum / group.realized_trades, 2)
        : null,
      avg_peak_pnl_pct: group.realized_trades > 0
        ? roundNumber(group.peak_pnl_sum / group.realized_trades, 2)
        : null,
      final_blockers_top: Array.from(group.final_blockers.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 8)
        .map(([blocker, count]) => ({ blocker, count })),
    }))
    .sort((a, b) => {
      const priority = {
        telegram_gmgn_quote_clean: 4,
        telegram_gmgn: 3,
        telegram_quote_clean: 2,
        telegram_only: 1,
      };
      return (priority[b.cohort] || 0) - (priority[a.cohort] || 0)
        || b.hard_gate_pass_unique - a.hard_gate_pass_unique
        || b.unique_tokens - a.unique_tokens;
    });
}

export function buildPremiumSignalOutcomeAudit({
  signals = [],
  paperTrades = [],
  missedAttributions = [],
  sinceTs = null,
  generatedAt = new Date().toISOString(),
} = {}) {
  const paperTradesByToken = new Map();
  for (const trade of paperTrades) {
    if (!trade.token_ca) continue;
    if (!paperTradesByToken.has(trade.token_ca)) paperTradesByToken.set(trade.token_ca, []);
    paperTradesByToken.get(trade.token_ca).push(trade);
  }
  for (const rows of paperTradesByToken.values()) {
    rows.sort((a, b) => Number(b.entry_ts || 0) - Number(a.entry_ts || 0));
  }

  const missedByToken = new Map();
  for (const row of missedAttributions) {
    if (!row.token_ca) continue;
    missedByToken.set(row.token_ca, mergeMissedAttribution(missedByToken.get(row.token_ca), row));
  }

  const signalGroups = new Map();
  for (const row of signals) {
    const tokenCa = String(row.token_ca || '').trim();
    if (!tokenCa) continue;
    if (!signalGroups.has(tokenCa)) signalGroups.set(tokenCa, []);
    signalGroups.get(tokenCa).push(row);
  }

  const tokens = Array.from(signalGroups.values())
    .map((rows) => tokenOutcome(rows, paperTradesByToken, missedByToken));
  const passTokens = tokens.filter((item) => item.hard_gate_pass);
  const passDogTokens = passTokens.filter((item) => ['gold', 'silver', 'bronze'].includes(item.pass_to_max_tier));
  const streamDogTokens = tokens.filter((item) => ['gold', 'silver', 'bronze'].includes(item.stream_tier));
  const streamTierCounts = emptyTierCounts();
  const passTierCounts = emptyTierCounts();
  for (const item of tokens) incrementTier(streamTierCounts, item.stream_tier);
  for (const item of passTokens) incrementTier(passTierCounts, item.pass_to_max_tier);

  const topPassMovers = [...passTokens]
    .sort((a, b) => Number(b.pass_to_max_pct ?? -Infinity) - Number(a.pass_to_max_pct ?? -Infinity));
  const topStreamMovers = [...tokens]
    .sort((a, b) => Number(b.stream_to_max_pct ?? -Infinity) - Number(a.stream_to_max_pct ?? -Infinity));
  const uncoveredPassDogs = passDogTokens
    .filter((item) => item.paper_trade_count <= 0)
    .sort((a, b) => Number(b.pass_to_max_pct ?? -Infinity) - Number(a.pass_to_max_pct ?? -Infinity));
  const uncoveredStreamDogs = streamDogTokens
    .filter((item) => item.paper_trade_count <= 0)
    .sort((a, b) => Number(b.stream_to_max_pct ?? -Infinity) - Number(a.stream_to_max_pct ?? -Infinity));
  const coveragePct = (covered, total) => total > 0 ? roundNumber((covered / total) * 100, 2) : null;
  const coverageCounts = {
    paper_trade: 0,
    observe_only: 0,
    safety_reject: 0,
    unclassified: 0,
  };
  for (const item of tokens) {
    const coverage = coverageForToken(item);
    item.coverage_class = coverage.class;
    item.coverage_reason = coverage.reason;
    item.coverage_gap = coverage.coverage_gap;
    if (coverage.coverage_gap && !item.final_blocker) {
      item.final_component = 'coverage_audit';
      item.final_reason = coverage.reason;
      item.final_blocker = {
        component: 'coverage_audit',
        reason: coverage.reason,
        route: item.hard_gate_pass ? 'PASS' : null,
        decision: 'observe_only',
      };
    }
    coverageCounts[item.coverage_class] = (coverageCounts[item.coverage_class] || 0) + 1;
  }
  const unclassified = tokens
    .filter((item) => item.coverage_class === 'unclassified')
    .sort((a, b) => Number(b.pass_to_max_pct ?? -Infinity) - Number(a.pass_to_max_pct ?? -Infinity));
  const coverageGaps = tokens
    .filter((item) => item.coverage_gap)
    .sort((a, b) => Number(b.pass_to_max_pct ?? -Infinity) - Number(a.pass_to_max_pct ?? -Infinity));
  const cohortScoreboard = buildCohortScoreboard(tokens);

  return {
    generated_at: generatedAt,
    filters: {
      since_ts: sinceTs,
      since_iso: sinceTs ? new Date(Number(sinceTs) * 1000).toISOString() : null,
      tier_definition: 'gold>=100%, silver=50-100%, bronze=25-50% observed market-cap increase',
      outcome_source: 'premium_signals market_cap snapshots; this can undercount tokens with no later premium snapshot',
    },
    summary: {
      premium_signal_rows: signals.length,
      unique_tokens: tokens.length,
      hard_gate_pass_rows: signals.filter((row) => String(row.hard_gate_status || '') === 'PASS').length,
      hard_gate_pass_unique: passTokens.length,
      stream_to_max_tiers: streamTierCounts,
      pass_to_max_tiers: passTierCounts,
      stream_dog_unique: streamDogTokens.length,
      stream_dog_with_paper_trade_unique: streamDogTokens.filter((item) => item.paper_trade_count > 0).length,
      stream_dog_without_paper_trade_unique: uncoveredStreamDogs.length,
      stream_dog_coverage_pct: coveragePct(
        streamDogTokens.filter((item) => item.paper_trade_count > 0).length,
        streamDogTokens.length
      ),
      pass_dog_unique: passDogTokens.length,
      pass_dog_with_paper_trade_unique: passDogTokens.filter((item) => item.paper_trade_count > 0).length,
      pass_dog_without_paper_trade_unique: uncoveredPassDogs.length,
      pass_dog_coverage_pct: coveragePct(
        passDogTokens.filter((item) => item.paper_trade_count > 0).length,
        passDogTokens.length
      ),
      pass_dog_in_missed_attribution_unique: passDogTokens.filter((item) => item.missed_attribution_count > 0).length,
      paper_traded_unique: tokens.filter((item) => item.paper_trade_count > 0).length,
      paper_traded_pass_unique: passTokens.filter((item) => item.paper_trade_count > 0).length,
      coverage_classes: coverageCounts,
      unclassified_unique: coverageCounts.unclassified,
      coverage_gap_unique: coverageGaps.length,
    },
    top_pass_movers: topPassMovers.slice(0, 30),
    uncovered_pass_dogs: uncoveredPassDogs.slice(0, 30),
    uncovered_stream_dogs: uncoveredStreamDogs.slice(0, 30),
    coverage_gap_tokens: coverageGaps.slice(0, 30),
    unclassified_tokens: unclassified.slice(0, 30),
    top_stream_movers: topStreamMovers.slice(0, 30),
    cohort_scoreboard: cohortScoreboard,
  };
}

export {
  coverageClassForToken,
  LOTTO_OBSERVE_UPSTREAM_STATUSES,
  OBSERVE_ONLY_STATUS_LIST,
  SAFETY_REJECT_STATUS_LIST,
  tierForPct,
};
