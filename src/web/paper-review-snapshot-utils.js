import fs from 'fs';
import { join } from 'path';

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

function pctFromRatio(value) {
  const n = finiteNumber(value);
  return n == null ? null : roundNumber(n * 100, 2);
}

const PEAK_UNTRUSTED_MARK_GAP_PCT = Number(process.env.PEAK_UNTRUSTED_MARK_GAP_PCT || '0.25');

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

function markPeakRatio(row = {}) {
  const mark = finiteNumber(row.mark_peak_pnl);
  if (mark != null) return mark;
  return finiteNumber(row.peak_pnl);
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

function snapshotIdFor({ generatedAt, commit, window }) {
  const generated = String(generatedAt || new Date().toISOString()).replace(/[^0-9TZ]+/g, '').replace('T', 'T');
  const windowLabel = String(window?.label || 'window').replace(/[^a-zA-Z0-9_-]+/g, '_');
  const shortCommit = String(commit || 'unknown').slice(0, 12).replace(/[^a-zA-Z0-9_-]+/g, '') || 'unknown';
  return `review_${generated}_${windowLabel}_${shortCommit}`;
}

export function inferReviewEntryMode(row = {}) {
  const monitorState = parseJsonObject(row.monitor_state_json);
  const lottoState = parseJsonObject(row.lotto_state_json);
  const entryAudit = parseJsonObject(row.entry_execution_audit_json);
  const monitorContract = monitorState.entryDecisionContract || {};
  const auditContract = entryAudit.entryDecisionContract || {};
  const entryDecision = lottoState.entryDecision || {};
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
    'unknown'
  ));
}

export function inferReviewEntryBranch(row = {}) {
  if (row.entry_branch) return String(row.entry_branch);
  const monitorState = parseJsonObject(row.monitor_state_json);
  const entryAudit = parseJsonObject(row.entry_execution_audit_json);
  return String(firstValue(
    monitorState.entryBranch,
    monitorState.entry_branch,
    entryAudit.entryBranch,
    entryAudit.entry_branch,
    'unbranched'
  ));
}

export function capitalTierForTrade(row = {}) {
  if (row.capital_tier) return String(row.capital_tier);
  const mode = inferReviewEntryMode(row).toLowerCase();
  const stage = String(row.strategy_stage || '').toLowerCase();
  const size = finiteNumber(row.position_size_sol) || 0;
  if (mode.includes('source_resonance') || mode.includes('hard_gate_pass') || mode.includes('pre_pass')) return 'tiny_probe';
  if (mode.includes('tiny') || mode.includes('probe') || mode.includes('scout') || (size > 0 && size <= 0.005)) return 'tiny_probe';
  if (stage.includes('stage1') || mode === 'stage1' || size >= 0.02) return 'stage1_main';
  if (mode.includes('lotto')) return 'lotto';
  if (size > 0 && size < 0.02) return 'small_probe';
  return 'unknown';
}

function emptyGroup(key, extra = {}) {
  return {
    ...extra,
    key,
    total: 0,
    open: 0,
    closed: 0,
    wins: 0,
    losses: 0,
    pnl_n: 0,
    peak_n: 0,
    giveback_n: 0,
    mark_only_peak_spikes: 0,
    total_pnl_pct: 0,
    total_peak_pct: 0,
    total_giveback_pct: 0,
    total_position_size_sol: 0,
    est_pnl_sol: 0,
    latency_n: 0,
    total_signal_to_quote_latency_ms: 0,
    drift_n: 0,
    total_signal_to_quote_drift_pct: 0,
  };
}

function finalizeGroup(group) {
  return {
    ...group,
    win_rate_pct: group.closed ? roundNumber((group.wins / group.closed) * 100, 1) : null,
    avg_pnl_pct: group.pnl_n ? roundNumber(group.total_pnl_pct / group.pnl_n, 2) : null,
    avg_peak_pnl_pct: group.peak_n ? roundNumber(group.total_peak_pct / group.peak_n, 2) : null,
    avg_giveback_pct: group.giveback_n ? roundNumber(group.total_giveback_pct / group.giveback_n, 2) : null,
    mark_only_peak_spikes: group.mark_only_peak_spikes || 0,
    avg_signal_to_quote_latency_ms: group.latency_n ? Math.round(group.total_signal_to_quote_latency_ms / group.latency_n) : null,
    avg_signal_to_quote_drift_pct: group.drift_n ? roundNumber(group.total_signal_to_quote_drift_pct / group.drift_n, 2) : null,
    est_pnl_sol: roundNumber(group.est_pnl_sol, 6),
    total_position_size_sol: roundNumber(group.total_position_size_sol, 6),
  };
}

function applyTrade(group, row, pnlPct, peakPct, givebackPct, markOnlySpike = false) {
  group.total += 1;
  const closed = row.exit_ts != null || row.exit_reason != null || pnlPct != null;
  if (closed) group.closed += 1;
  else group.open += 1;
  const size = finiteNumber(row.position_size_sol) || 0;
  group.total_position_size_sol += size;
  if (pnlPct != null) {
    group.pnl_n += 1;
    group.total_pnl_pct += pnlPct;
    group.est_pnl_sol += (pnlPct / 100) * size;
    if (closed && pnlPct > 0) group.wins += 1;
    if (closed && pnlPct <= 0) group.losses += 1;
  }
  if (peakPct != null) {
    group.peak_n += 1;
    group.total_peak_pct += peakPct;
  }
  if (givebackPct != null) {
    group.giveback_n += 1;
    group.total_giveback_pct += givebackPct;
  }
  if (markOnlySpike) group.mark_only_peak_spikes += 1;
  const latencyMs = finiteNumber(row.signal_to_quote_latency_ms);
  if (latencyMs != null) {
    group.latency_n += 1;
    group.total_signal_to_quote_latency_ms += latencyMs;
  }
  const driftPct = finiteNumber(row.signal_to_quote_drift_pct);
  if (driftPct != null) {
    group.drift_n += 1;
    group.total_signal_to_quote_drift_pct += driftPct;
  }
}

export function buildTradeReviewSummary(rows = []) {
  const byTier = new Map();
  const byEntryMode = new Map();
  const byEntryBranch = new Map();
  const uniqueTokens = new Set();
  const uniqueLifecycles = new Set();
  const topGivebackTrades = [];
  const totals = emptyGroup('all');

  for (const row of rows) {
    const entryMode = inferReviewEntryMode(row);
    const entryBranch = inferReviewEntryBranch(row);
    const capitalTier = capitalTierForTrade(row);
    const pnlPct = pctFromRatio(row.pnl_pct);
    const peakRatio = trustedPeakRatio(row);
    const markPeakRatioValue = markPeakRatio(row);
    const peakPct = pctFromRatio(peakRatio);
    const markPeakPct = pctFromRatio(markPeakRatioValue);
    const givebackPct = pnlPct != null && peakPct != null ? roundNumber(Math.max(0, peakPct - pnlPct), 2) : null;
    const markOnlySpike = (
      markPeakRatioValue != null
      && (peakRatio == null || markPeakRatioValue - peakRatio >= PEAK_UNTRUSTED_MARK_GAP_PCT)
      && markPeakRatioValue >= 0.25
    );

    if (row.token_ca) uniqueTokens.add(String(row.token_ca));
    if (row.lifecycle_id) uniqueLifecycles.add(String(row.lifecycle_id));
    if (!byTier.has(capitalTier)) byTier.set(capitalTier, emptyGroup(capitalTier, { capital_tier: capitalTier }));
    if (!byEntryMode.has(entryMode)) byEntryMode.set(entryMode, emptyGroup(entryMode, { entry_mode: entryMode, capital_tier: capitalTier }));
    if (!byEntryBranch.has(entryBranch)) byEntryBranch.set(entryBranch, emptyGroup(entryBranch, { entry_branch: entryBranch, capital_tier: capitalTier }));

    applyTrade(totals, row, pnlPct, peakPct, givebackPct, markOnlySpike);
    applyTrade(byTier.get(capitalTier), row, pnlPct, peakPct, givebackPct, markOnlySpike);
    applyTrade(byEntryMode.get(entryMode), row, pnlPct, peakPct, givebackPct, markOnlySpike);
    applyTrade(byEntryBranch.get(entryBranch), row, pnlPct, peakPct, givebackPct, markOnlySpike);

    if (givebackPct != null && givebackPct > 0) {
      topGivebackTrades.push({
        id: row.id,
        symbol: row.symbol || null,
        token_ca: row.token_ca || null,
        lifecycle_id: row.lifecycle_id || null,
        entry_mode: entryMode,
        capital_tier: capitalTier,
        entry_ts: row.entry_ts ?? null,
        exit_ts: row.exit_ts ?? null,
        exit_reason: row.exit_reason || null,
        position_size_sol: finiteNumber(row.position_size_sol),
        pnl_pct: pnlPct,
        peak_pnl_pct: peakPct,
        mark_peak_pnl_pct: markPeakPct,
        peak_trust_status: row.peak_trust_status || (markOnlySpike ? 'mark_only_peak_untrusted' : 'trusted_peak'),
        giveback_pct: givebackPct,
      });
    }
  }

  const sortByImpact = (a, b) => (
    Number(b.total || 0) - Number(a.total || 0)
    || Math.abs(Number(b.est_pnl_sol || 0)) - Math.abs(Number(a.est_pnl_sol || 0))
  );

  return {
    totals: finalizeGroup(totals),
    counting_units: {
      paper_trade_rows: rows.length,
      unique_tokens_with_trade: uniqueTokens.size,
      unique_lifecycles_with_trade: uniqueLifecycles.size,
      lifecycle_with_trade: uniqueLifecycles.size,
    },
    by_capital_tier: Array.from(byTier.values()).map(finalizeGroup).sort(sortByImpact),
    by_entry_mode: Array.from(byEntryMode.values()).map(finalizeGroup).sort(sortByImpact),
    by_entry_branch: Array.from(byEntryBranch.values()).map(finalizeGroup).sort(sortByImpact),
    top_giveback_trades: topGivebackTrades
      .sort((a, b) => Number(b.giveback_pct || 0) - Number(a.giveback_pct || 0))
      .slice(0, 20),
  };
}

function compactProbeMode(row) {
  return {
    entry_mode: row.entry_mode,
    fills: row.fills || 0,
    fill_unique: row.fill_unique || 0,
    wins: row.wins || 0,
    win_rate: row.win_rate == null ? null : roundNumber(row.win_rate, 3),
    avg_pnl_pct: row.avg_pnl_pct ?? null,
    max_peak_pnl_pct: row.max_peak_pnl_pct ?? null,
  };
}

export function buildPaperReviewSnapshot({
  generatedAt,
  commit,
  policyFingerprint = {},
  window,
  dbPath,
  closedLoop,
  tradeReview,
  latencySummary = [],
  tableCoverage = [],
  sourceHealth = [],
  externalAlphaHealth = [],
  registrySummary = null,
  notes = [],
}) {
  const probes = closedLoop?.probes?.paper_pnl_by_entry_mode || [];
  const byMode = closedLoop?.probes?.by_mode || {};
  const topProbeModes = probes.slice(0, 12).map(compactProbeMode);
  const hardGateProbe = byMode.hard_gate_pass_tiny_probe || null;
  const sourceProbe = byMode.source_resonance_tiny_probe || null;
  const prePassProbe = byMode.pre_pass_resonance_tiny_probe || null;
  const snapshotId = snapshotIdFor({ generatedAt, commit, window });

  return {
    schema_version: 1,
    snapshot_id: snapshotId,
    generated_at: generatedAt,
    commit: commit || 'unknown',
    policy_fingerprint: policyFingerprint,
    db_path: dbPath,
    window,
    summary: {
      premium_signal_rows: closedLoop?.premium_signals?.premium_signal_rows ?? null,
      premium_unique_tokens: closedLoop?.premium_signals?.premium_unique_tokens ?? null,
      hard_gate_pass_unique: closedLoop?.premium_signals?.hard_gate_pass_unique ?? null,
      source_resonance_unique: closedLoop?.source_resonance?.unique_tokens ?? null,
      gmgn_pre_seen_unique: closedLoop?.source_resonance?.gmgn_pre_seen_unique ?? null,
      quote_clean_unique: closedLoop?.source_resonance?.quote_clean_unique ?? null,
      missed_unique_tokens: closedLoop?.missed_dogs?.unique_tokens ?? null,
      quote_clean_missed_dog_unique: closedLoop?.missed_dogs?.quote_clean_dog_unique ?? null,
      missed_gold_unique: closedLoop?.missed_dogs?.gold_unique ?? null,
      missed_silver_unique: closedLoop?.missed_dogs?.silver_unique ?? null,
      missed_bronze_unique: closedLoop?.missed_dogs?.bronze_unique ?? null,
      missed_mark_only_gold_unique: closedLoop?.missed_dogs?.mark_only_gold_unique ?? null,
      missed_mark_only_silver_unique: closedLoop?.missed_dogs?.mark_only_silver_unique ?? null,
      missed_mark_only_bronze_unique: closedLoop?.missed_dogs?.mark_only_bronze_unique ?? null,
      paper_trades_total: tradeReview?.totals?.total ?? null,
      paper_trade_rows: tradeReview?.counting_units?.paper_trade_rows ?? tradeReview?.totals?.total ?? null,
      unique_tokens_with_trade: tradeReview?.counting_units?.unique_tokens_with_trade ?? null,
      unique_lifecycles_with_trade: tradeReview?.counting_units?.unique_lifecycles_with_trade ?? null,
      lifecycle_with_trade: tradeReview?.counting_units?.lifecycle_with_trade ?? null,
      paper_trades_closed: tradeReview?.totals?.closed ?? null,
      paper_win_rate_pct: tradeReview?.totals?.win_rate_pct ?? null,
      paper_avg_pnl_pct: tradeReview?.totals?.avg_pnl_pct ?? null,
      paper_avg_peak_pnl_pct: tradeReview?.totals?.avg_peak_pnl_pct ?? null,
      paper_avg_giveback_pct: tradeReview?.totals?.avg_giveback_pct ?? null,
      paper_mark_only_peak_spikes: tradeReview?.totals?.mark_only_peak_spikes ?? null,
      hard_gate_pass_probe_fills: hardGateProbe?.fills ?? null,
      hard_gate_pass_probe_avg_pnl_pct: hardGateProbe?.avg_pnl_pct ?? null,
      source_resonance_probe_fills: sourceProbe?.fills ?? null,
      source_resonance_probe_avg_pnl_pct: sourceProbe?.avg_pnl_pct ?? null,
      pre_pass_probe_fills: prePassProbe?.fills ?? null,
      pre_pass_probe_avg_pnl_pct: prePassProbe?.avg_pnl_pct ?? null,
    },
    closed_loop: {
      premium_signals: closedLoop?.premium_signals || null,
      source_resonance: closedLoop?.source_resonance || null,
      probes_by_mode: byMode,
      top_probe_modes: topProbeModes,
      missed_dogs: {
        unique_tokens: closedLoop?.missed_dogs?.unique_tokens ?? null,
        quote_clean_unique: closedLoop?.missed_dogs?.quote_clean_unique ?? null,
        quote_clean_dog_unique: closedLoop?.missed_dogs?.quote_clean_dog_unique ?? null,
        gold_unique: closedLoop?.missed_dogs?.gold_unique ?? null,
        silver_unique: closedLoop?.missed_dogs?.silver_unique ?? null,
        bronze_unique: closedLoop?.missed_dogs?.bronze_unique ?? null,
        mark_only_gold_unique: closedLoop?.missed_dogs?.mark_only_gold_unique ?? null,
        mark_only_silver_unique: closedLoop?.missed_dogs?.mark_only_silver_unique ?? null,
        mark_only_bronze_unique: closedLoop?.missed_dogs?.mark_only_bronze_unique ?? null,
        top_missed_dogs: closedLoop?.missed_dogs?.top_missed_dogs || [],
        by_final_blocker: closedLoop?.missed_dogs?.by_final_blocker || [],
      },
    },
    trade_review: tradeReview,
    counting_units: tradeReview?.counting_units || null,
    latency_summary: latencySummary,
    table_coverage: tableCoverage,
    source_health: sourceHealth,
    external_alpha_health: externalAlphaHealth,
    registry_summary: registrySummary,
    notes,
  };
}

function tableRows(rows, columns) {
  const header = `| ${columns.join(' | ')} |`;
  const sep = `| ${columns.map(() => '---').join(' | ')} |`;
  const body = rows.map((row) => `| ${columns.map((col) => row[col] ?? '').join(' | ')} |`);
  return [header, sep, ...body].join('\n');
}

export function buildPaperReviewMarkdown(snapshot) {
  const summary = snapshot.summary || {};
  const tierRows = (snapshot.trade_review?.by_capital_tier || []).map((row) => ({
    tier: row.capital_tier,
    total: row.total,
    closed: row.closed,
    win_rate_pct: row.win_rate_pct,
    avg_pnl_pct: row.avg_pnl_pct,
    avg_peak_pnl_pct: row.avg_peak_pnl_pct,
    avg_giveback_pct: row.avg_giveback_pct,
  }));
  const blockerRows = (snapshot.closed_loop?.missed_dogs?.by_final_blocker || []).slice(0, 10).map((row) => ({
    route: row.route,
    component: row.final_component,
    reason: row.final_reason,
    unique: row.unique_tokens,
    quote_clean: row.quote_clean_unique,
    gold: row.gold_unique,
    silver: row.silver_unique,
    bronze: row.bronze_unique,
  }));
  const givebackRows = (snapshot.trade_review?.top_giveback_trades || []).slice(0, 10).map((row) => ({
    symbol: row.symbol || row.token_ca,
    mode: row.entry_mode,
    tier: row.capital_tier,
    peak: row.peak_pnl_pct,
    mark_peak: row.mark_peak_pnl_pct,
    exit: row.pnl_pct,
    giveback: row.giveback_pct,
    peak_status: row.peak_trust_status,
    reason: row.exit_reason,
  }));
  const branchRows = (snapshot.trade_review?.by_entry_branch || []).slice(0, 12).map((row) => ({
    branch: row.entry_branch,
    total: row.total,
    closed: row.closed,
    win_rate_pct: row.win_rate_pct,
    avg_pnl_pct: row.avg_pnl_pct,
    avg_giveback_pct: row.avg_giveback_pct,
  }));

  const lines = [
    `# Paper Review Snapshot`,
    '',
    `- snapshot_id: ${snapshot.snapshot_id || ''}`,
    `- generated_at: ${snapshot.generated_at}`,
    `- commit: ${snapshot.commit}`,
    `- window: ${snapshot.window?.label || ''} (${snapshot.window?.since_iso || 'all'} -> ${snapshot.window?.until_iso || ''})`,
    `- db_path: ${snapshot.db_path}`,
    '',
    `## Summary`,
    '',
    `- premium_signals: ${summary.premium_signal_rows ?? ''} rows / ${summary.premium_unique_tokens ?? ''} unique`,
    `- hard_gate_pass_unique: ${summary.hard_gate_pass_unique ?? ''}`,
    `- source_resonance_unique: ${summary.source_resonance_unique ?? ''}, gmgn_pre_seen_unique: ${summary.gmgn_pre_seen_unique ?? ''}`,
    `- missed dogs: trusted gold=${summary.missed_gold_unique ?? ''}, silver=${summary.missed_silver_unique ?? ''}, bronze=${summary.missed_bronze_unique ?? ''}, mark_only=${summary.missed_mark_only_gold_unique ?? ''}/${summary.missed_mark_only_silver_unique ?? ''}/${summary.missed_mark_only_bronze_unique ?? ''}, quote_clean=${summary.quote_clean_missed_dog_unique ?? ''}`,
    `- paper trades: total=${summary.paper_trades_total ?? ''}, closed=${summary.paper_trades_closed ?? ''}, win_rate_pct=${summary.paper_win_rate_pct ?? ''}, avg_pnl_pct=${summary.paper_avg_pnl_pct ?? ''}, avg_giveback_pct=${summary.paper_avg_giveback_pct ?? ''}`,
    `- counting_units: rows=${summary.paper_trade_rows ?? ''}, unique_tokens=${summary.unique_tokens_with_trade ?? ''}, unique_lifecycles=${summary.unique_lifecycles_with_trade ?? ''}`,
    '',
    `## Capital Tiers`,
    '',
    tierRows.length ? tableRows(tierRows, ['tier', 'total', 'closed', 'win_rate_pct', 'avg_pnl_pct', 'avg_peak_pnl_pct', 'avg_giveback_pct']) : '_No trades in window._',
    '',
    `## Top Missed Blockers`,
    '',
    blockerRows.length ? tableRows(blockerRows, ['route', 'component', 'reason', 'unique', 'quote_clean', 'gold', 'silver', 'bronze']) : '_No missed blocker rows._',
    '',
    `## Top Giveback Trades`,
    '',
    givebackRows.length ? tableRows(givebackRows, ['symbol', 'mode', 'tier', 'peak', 'mark_peak', 'exit', 'giveback', 'peak_status', 'reason']) : '_No giveback rows._',
    '',
    `## Entry Branches`,
    '',
    branchRows.length ? tableRows(branchRows, ['branch', 'total', 'closed', 'win_rate_pct', 'avg_pnl_pct', 'avg_giveback_pct']) : '_No branch rows._',
    '',
    `## Notes`,
    '',
    ...(snapshot.notes || []).map((note) => `- ${note}`),
    '',
  ];
  return lines.join('\n');
}

export function reviewSnapshotBaseName(snapshot) {
  const generated = String(snapshot.generated_at || new Date().toISOString())
    .replaceAll(':', '')
    .replaceAll('.', '')
    .replaceAll('-', '')
    .replace('T', '_')
    .replace('Z', 'Z');
  const windowLabel = String(snapshot.window?.label || 'window').replace(/[^a-zA-Z0-9_-]+/g, '_');
  const commit = String(snapshot.commit || 'unknown').slice(0, 12).replace(/[^a-zA-Z0-9_-]+/g, '');
  return `paper_review_${generated}_${windowLabel}_${commit || 'unknown'}`;
}

export function writePaperReviewSnapshotFiles(snapshot, { dir }) {
  fs.mkdirSync(dir, { recursive: true });
  const base = reviewSnapshotBaseName(snapshot);
  const jsonPath = join(dir, `${base}.json`);
  const markdownPath = join(dir, `${base}.md`);
  const jsonTmp = `${jsonPath}.tmp`;
  const markdownTmp = `${markdownPath}.tmp`;
  fs.writeFileSync(jsonTmp, `${JSON.stringify(snapshot, null, 2)}\n`, 'utf8');
  fs.writeFileSync(markdownTmp, buildPaperReviewMarkdown(snapshot), 'utf8');
  fs.renameSync(jsonTmp, jsonPath);
  fs.renameSync(markdownTmp, markdownPath);
  return { json_path: jsonPath, markdown_path: markdownPath };
}
