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

const OBSERVE_ONLY_STATUSES = new Set([
  'RISK_BLOCKED',
  'LOTTO_OBSERVE_LOW_MC_VOL',
  'NOT_ATH_PREBUY_KLINE_UNKNOWN_DATA_BLOCKED',
  'NOT_ATH_PREBUY_KLINE_RETRY_EXPIRED',
  'NOT_ATH_V17',
  'ILLIQUID_JUNK',
  'NOT_ATH_V14',
  'NOT_ATH_V13',
  'NOT_ATH_V16',
  'INSUFFICIENT_KLINE',
  'NO_MC_DATA',
]);

const SAFETY_REJECT_STATUSES = new Set([
  'GREYLIST',
  'WASH_HIGH',
  'GREYLIST_LOW_CONF',
  'REJECT',
  'RED_K_FAIL',
  '5M_DUMP',
  '5M_OVERHEAT',
  'HONEYPOT',
  'RUG_PULL_RISK',
]);

function coverageClassForToken(outcome) {
  if (outcome.paper_trade_count > 0) return 'paper_trade';
  const statuses = new Set(Object.keys(outcome.gate_statuses || {}).map((status) => String(status || '').toUpperCase()));
  if ([...statuses].some((status) => SAFETY_REJECT_STATUSES.has(status))) return 'safety_reject';
  if (
    outcome.missed_attribution_count > 0
    || [...statuses].some((status) => OBSERVE_ONLY_STATUSES.has(status))
  ) {
    return 'observe_only';
  }
  return 'unclassified';
}

function isoFromSec(sec) {
  const n = numeric(sec);
  return n == null ? null : new Date(n * 1000).toISOString();
}

function serializeTrade(row) {
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
    position_size_sol: row.position_size_sol == null ? null : Number(row.position_size_sol),
  };
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
    paper_trades: paperTrades.map(serializeTrade),
    missed_attribution_count: missed?.n || 0,
    missed_attribution_max_pnl_pct: missed?.max_pnl_pct ?? null,
  };
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
    const maxPnl = numeric(row.max_pnl);
    missedByToken.set(row.token_ca, {
      n: Number(row.n || 0),
      max_pnl_pct: maxPnl == null ? null : roundNumber(maxPnl * 100, 2),
    });
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
  const coverageCounts = {
    paper_trade: 0,
    observe_only: 0,
    safety_reject: 0,
    unclassified: 0,
  };
  for (const item of tokens) {
    item.coverage_class = coverageClassForToken(item);
    coverageCounts[item.coverage_class] = (coverageCounts[item.coverage_class] || 0) + 1;
  }
  const unclassified = tokens
    .filter((item) => item.coverage_class === 'unclassified')
    .sort((a, b) => Number(b.pass_to_max_pct ?? -Infinity) - Number(a.pass_to_max_pct ?? -Infinity));

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
      pass_dog_unique: passDogTokens.length,
      pass_dog_with_paper_trade_unique: passDogTokens.filter((item) => item.paper_trade_count > 0).length,
      pass_dog_without_paper_trade_unique: uncoveredPassDogs.length,
      pass_dog_in_missed_attribution_unique: passDogTokens.filter((item) => item.missed_attribution_count > 0).length,
      paper_traded_unique: tokens.filter((item) => item.paper_trade_count > 0).length,
      paper_traded_pass_unique: passTokens.filter((item) => item.paper_trade_count > 0).length,
      coverage_classes: coverageCounts,
      unclassified_unique: coverageCounts.unclassified,
    },
    top_pass_movers: topPassMovers.slice(0, 30),
    uncovered_pass_dogs: uncoveredPassDogs.slice(0, 30),
    unclassified_tokens: unclassified.slice(0, 30),
    top_stream_movers: topStreamMovers.slice(0, 30),
  };
}

export { tierForPct };
