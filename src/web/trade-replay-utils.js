function parseJsonObject(value) {
  if (!value || typeof value !== 'string') return {};
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function numberOrNull(value) {
  if (value === undefined || value === null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function roundNumber(value, digits = 3) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
}

function firstValue(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && String(value).trim() !== '') return value;
  }
  return null;
}

function normalizeSignalTsSeconds(value) {
  const n = numberOrNull(value);
  if (n == null) return null;
  return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

function pctFromRatio(value, digits = 2) {
  const n = numberOrNull(value);
  return n == null ? null : roundNumber(n * 100, digits);
}

function trustedPeakRatio(trade = {}) {
  const trusted = numberOrNull(trade.trusted_peak_pnl);
  if (trusted != null && trusted > 0) return trusted;
  const quote = numberOrNull(trade.quote_peak_pnl);
  if (quote != null && quote > 0) return quote;
  if (trade.trusted_peak_pnl === undefined && trade.quote_peak_pnl === undefined) {
    return numberOrNull(trade.peak_pnl);
  }
  return null;
}

function pctAlreadyOrRatio(primaryPct, fallbackRatio, digits = 2) {
  const pct = numberOrNull(primaryPct);
  if (pct != null) return roundNumber(pct, digits);
  return pctFromRatio(fallbackRatio, digits);
}

function entryModeFromTrade(trade = {}) {
  const monitorState = parseJsonObject(trade.monitor_state_json);
  const entryAudit = parseJsonObject(trade.entry_execution_audit_json);
  const contract = monitorState.entryDecisionContract || entryAudit.entryDecisionContract || {};
  return String(firstValue(
    trade.entry_mode,
    monitorState.entryMode,
    monitorState.entry_mode,
    monitorState.smartEntryReason,
    contract.entry_mode,
    trade.signal_route ? `${String(trade.signal_route).toLowerCase()}_unknown` : null,
    trade.strategy_stage,
    'unknown',
  ));
}

function entryModeBucket(entryMode, positionSizeSol) {
  const mode = String(entryMode || '').toLowerCase();
  const size = Number(positionSizeSol || 0);
  if (mode.includes('gmgn') && mode.includes('tiny_scout')) return 'gmgn_tiny_scout';
  if (mode.includes('tiny_scout') || mode.includes('tiny_probe')) return 'tiny_scout';
  if ((mode.includes('probe') || mode.includes('scout')) && size > 0 && size <= 0.005) return 'tiny_scout';
  if (mode.includes('probe') || mode.includes('scout')) return 'scout';
  return 'primary';
}

function extractEntryDiagnostics(trade = {}) {
  const monitorState = parseJsonObject(trade.monitor_state_json);
  const entryAudit = parseJsonObject(trade.entry_execution_audit_json);
  const readiness = firstValue(
    monitorState.entryReadinessPolicy,
    monitorState.entry_readiness_policy,
    entryAudit.entryReadinessPolicy,
    entryAudit.entry_readiness_policy,
    {},
  );
  const contract = firstValue(
    monitorState.entryDecisionContract,
    monitorState.entry_decision_contract,
    entryAudit.entryDecisionContract,
    entryAudit.entry_decision_contract,
    {},
  );
  const edge = firstValue(
    monitorState.entryEdgeBudget,
    monitorState.entry_edge_budget,
    entryAudit.entryEdgeBudget,
    entryAudit.entry_edge_budget,
    {},
  );
  const signalTs = normalizeSignalTsSeconds(trade.signal_ts);
  const entryTs = numberOrNull(trade.entry_ts);
  const triggerPrice = numberOrNull(trade.trigger_price);
  const entryPrice = numberOrNull(trade.entry_price);
  let spreadPct = numberOrNull(firstValue(
    monitorState.entrySpreadPct,
    monitorState.entry_spread_pct,
    entryAudit.entrySpreadPct,
    entryAudit.entry_spread_pct,
    edge.spread_pct,
  ));
  if (spreadPct == null && triggerPrice && entryPrice) {
    spreadPct = ((entryPrice - triggerPrice) / triggerPrice) * 100;
  }
  return {
    monitor_state: monitorState,
    entry_audit: entryAudit,
    entry_readiness_policy: readiness && typeof readiness === 'object' ? readiness : {},
    entry_decision_contract: contract && typeof contract === 'object' ? contract : {},
    entry_edge_budget: edge && typeof edge === 'object' ? edge : {},
    entry_mode: entryModeFromTrade(trade),
    entry_mode_bucket: entryModeBucket(entryModeFromTrade(trade), trade.position_size_sol),
    signal_age_sec: signalTs && entryTs ? Math.max(0, Math.round(entryTs - signalTs)) : null,
    spread_pct: spreadPct == null ? null : roundNumber(spreadPct, 3),
  };
}

function extractExitDiagnostics(trade = {}, pathSamples = [], decisionEvents = []) {
  const exitAudit = parseJsonObject(trade.exit_execution_audit_json);
  const monitorState = parseJsonObject(trade.monitor_state_json);
  const peak = trustedPeakRatio(trade) || 0;
  const pnl = numberOrNull(trade.pnl_pct) || 0;
  const pathQuoteGaps = pathSamples
    .map((sample) => {
      const quote = numberOrNull(sample.quote_pnl);
      const mark = numberOrNull(sample.mark_pnl);
      return quote != null && mark != null ? quote - mark : null;
    })
    .filter((gap) => gap != null && Number.isFinite(gap));
  const maxAbsQuoteGap = pathQuoteGaps.length
    ? pathQuoteGaps.reduce((max, gap) => Math.max(max, Math.abs(gap)), 0)
    : null;
  const phasePolicy = decisionEvents.filter(
    (event) => event.component === 'phase_policy' && event.event_type === 'shadow_decision',
  );
  const phaseExitBeforeClose = phasePolicy.find((event) => {
    const decision = String(event.decision || '').toUpperCase();
    const payload = parseJsonObject(event.payload_json);
    return decision === 'EXIT' || String(payload.shadow_action || '').toUpperCase() === 'EXIT';
  });
  return {
    exit_audit: exitAudit,
    accounting_source: firstValue(exitAudit.accountingSource, monitorState.accountingSource),
    exit_quote_pnl_pct: pctAlreadyOrRatio(exitAudit.quotePnlPct, monitorState.exitQuotePnl, 2),
    exit_quote_mark_gap_pct: pctAlreadyOrRatio(exitAudit.quoteMarkGapPct, monitorState.exitQuoteMarkGap, 2),
    max_path_quote_gap_pct: maxAbsQuoteGap == null ? null : roundNumber(maxAbsQuoteGap * 100, 2),
    giveback_pct: roundNumber((peak - pnl) * 100, 2),
    peak_capture_pct: peak > 0 ? roundNumber((pnl / peak) * 100, 1) : null,
    phase_policy_samples: phasePolicy.length,
    phase_policy_exit_shadow: phaseExitBeforeClose
      ? {
          event_ts: phaseExitBeforeClose.event_ts,
          reason: phaseExitBeforeClose.reason,
          decision: phaseExitBeforeClose.decision,
        }
      : null,
  };
}

export function inferLossCause(trade = {}, pathSamples = [], decisionEvents = []) {
  const pnl = numberOrNull(trade.pnl_pct);
  const peak = trustedPeakRatio(trade) || 0;
  const reason = String(trade.exit_reason || '');
  const route = String(trade.signal_route || trade.signal_type || '').toUpperCase();
  const entry = extractEntryDiagnostics(trade);
  const exit = extractExitDiagnostics(trade, pathSamples, decisionEvents);
  const tags = [];

  if (trade.exit_ts == null && !trade.exit_reason) tags.push('open_position');
  if (pnl != null && pnl >= 0) tags.push('not_a_loss');
  if (entry.signal_age_sec != null && entry.signal_age_sec > 3600) tags.push('selection_stale_signal_age');
  if (['DISTRIBUTION', 'DEAD'].includes(String(trade.lifecycle_state || '').toUpperCase())) {
    tags.push('selection_bad_lifecycle_state');
  }
  if (String(entry.entry_readiness_policy.decision || '').toUpperCase() === 'WAIT') {
    tags.push('selection_entered_wait_policy');
  }
  if (entry.spread_pct != null && entry.spread_pct > 3.0) tags.push('execution_entry_spread_gt_3pct');
  if (route === 'LOTTO' && entry.spread_pct != null && entry.spread_pct > 2.0) {
    tags.push('execution_lotto_entry_spread_gt_2pct');
  }
  if (peak < 0.01 && pnl < 0) tags.push('timing_zero_peak_entry');
  else if (peak < 0.05 && pnl < 0) tags.push('timing_low_follow_entry');
  if (/no_follow|doa/i.test(reason)) tags.push('timing_no_follow_exit');
  if (/hard_sl|lotto_sl|_sl|stop_loss/i.test(reason)) tags.push('exit_stop_loss_or_rug');
  if (peak >= 0.08 && pnl <= 0) tags.push('exit_positive_peak_to_loss');
  if (peak >= 0.20 && peak - pnl >= 0.15) tags.push('exit_large_giveback');
  if (peak >= 0.10 && pnl < peak * 0.45) tags.push('exit_poor_peak_capture');
  if (exit.exit_quote_mark_gap_pct != null && Math.abs(exit.exit_quote_mark_gap_pct) >= 8) {
    tags.push('execution_exit_quote_mark_gap_8pp');
  }
  if (exit.max_path_quote_gap_pct != null && exit.max_path_quote_gap_pct >= 8) {
    tags.push('execution_path_quote_mark_gap_8pp');
  }
  if (String(exit.accounting_source || '').includes('override') || String(exit.accounting_source || '').includes('reprice')) {
    tags.push('accounting_reprice_or_trigger_override');
  }
  if (exit.phase_policy_exit_shadow && pnl < 0) tags.push('phase_policy_shadow_exit_not_live_or_late');
  if (entry.entry_mode_bucket === 'tiny_scout') tags.push('probe_position');

  if (pnl != null && pnl >= 0) {
    return {
      root_cause: 'not_loss',
      tags: Array.from(new Set(tags)),
      modification_hint: 'no loss-specific strategy change needed for this trade',
      confidence: pathSamples.length >= 2 && decisionEvents.length >= 2 ? 'high' : (pathSamples.length || decisionEvents.length ? 'medium' : 'low'),
    };
  }

  let rootCause = 'unclassified_loss';
  let modification = 'keep collecting replay evidence before changing live rules';
  if (tags.some((tag) => tag.startsWith('execution_')) || tags.includes('accounting_reprice_or_trigger_override')) {
    rootCause = 'execution_or_accounting_gap';
    modification = 'tighten quote-vs-mark guard and require executable exit quote sanity before trusting mark PnL';
  } else if (tags.some((tag) => tag.startsWith('selection_'))) {
    rootCause = 'selection_quality';
    modification = 'block live entry when lifecycle/readiness shadow says WAIT/DEAD unless a tested recovery override passes';
  } else if (tags.includes('timing_zero_peak_entry') || tags.includes('timing_low_follow_entry') || tags.includes('timing_no_follow_exit')) {
    rootCause = 'entry_timing_no_follow';
    modification = 'tighten live follow-through confirmation; keep weak no-follow candidates in shadow or tiny probe only';
  } else if (tags.includes('exit_large_giveback') || tags.includes('exit_positive_peak_to_loss') || tags.includes('exit_poor_peak_capture')) {
    rootCause = 'exit_capture';
    modification = 'promote phase-policy profit protection or earlier partial/exit floors for high peak-to-loss paths';
  } else if (tags.includes('exit_stop_loss_or_rug')) {
    rootCause = 'rug_or_stop_loss';
    modification = 'separate true rug stops from bad entries using path samples; tighten entry if peak never formed';
  }

  return {
    root_cause: rootCause,
    tags: Array.from(new Set(tags)),
    modification_hint: modification,
    confidence: pathSamples.length >= 2 && decisionEvents.length >= 2 ? 'high' : (pathSamples.length || decisionEvents.length ? 'medium' : 'low'),
  };
}

export function buildReplayTimeline(trade = {}, pathSamples = [], decisionEvents = []) {
  const items = [];
  const entryTs = numberOrNull(trade.entry_ts);
  const exitTs = numberOrNull(trade.exit_ts);
  if (entryTs) {
    items.push({
      ts: entryTs,
      type: 'trade_entry',
      component: 'paper_trades',
      decision: 'entered',
      reason: entryModeFromTrade(trade),
      pnl_pct: null,
      peak_pnl_pct: 0,
    });
  }
  for (const event of decisionEvents) {
    items.push({
      ts: numberOrNull(event.event_ts),
      id: event.id,
      type: event.event_type,
      component: event.component,
      decision: event.decision,
      reason: event.reason,
      data_source: event.data_source,
    });
  }
  for (const sample of pathSamples) {
    items.push({
      ts: numberOrNull(sample.sample_ts),
      id: sample.id,
      type: 'path_sample',
      component: 'paper_trade_path_samples',
      decision: sample.action,
      reason: sample.reason,
      mark_source: sample.mark_source,
      mark_pnl_pct: pctFromRatio(sample.mark_pnl, 2),
      quote_pnl_pct: pctFromRatio(sample.quote_pnl, 2),
      peak_pnl_pct: pctFromRatio(sample.peak_pnl, 2),
      sold_pct: pctFromRatio(sample.sold_pct, 1),
    });
  }
  if (exitTs || trade.exit_reason) {
    items.push({
      ts: exitTs,
      type: 'trade_exit',
      component: 'paper_trades',
      decision: 'closed',
      reason: trade.exit_reason,
      pnl_pct: pctFromRatio(trade.pnl_pct, 2),
      peak_pnl_pct: pctFromRatio(trustedPeakRatio(trade), 2),
    });
  }
  return items
    .filter((item) => item.ts != null)
    .sort((a, b) => (a.ts - b.ts) || String(a.type).localeCompare(String(b.type)));
}

export function buildTradeReplay(trade = {}, pathSamples = [], decisionEvents = [], { includeTimeline = true } = {}) {
  const entry = extractEntryDiagnostics(trade);
  const exit = extractExitDiagnostics(trade, pathSamples, decisionEvents);
  const loss = inferLossCause(trade, pathSamples, decisionEvents);
  const tradePnl = numberOrNull(trade.pnl_pct);
  const peak = trustedPeakRatio(trade);
  const samplesWithQuote = pathSamples.filter((sample) => sample.quote_pnl != null).length;
  const replay = {
    trade_id: trade.id,
    lifecycle_id: trade.lifecycle_id,
    token_ca: trade.token_ca,
    symbol: trade.symbol,
    route: trade.signal_route || trade.signal_type || null,
    strategy_stage: trade.strategy_stage || null,
    entry_mode: entry.entry_mode,
    entry_mode_bucket: entry.entry_mode_bucket,
    position_size_sol: numberOrNull(trade.position_size_sol),
    entry_ts: numberOrNull(trade.entry_ts),
    exit_ts: numberOrNull(trade.exit_ts),
    hold_sec: trade.entry_ts && trade.exit_ts ? Math.max(0, Math.round(Number(trade.exit_ts) - Number(trade.entry_ts))) : null,
    pnl_pct: pctFromRatio(trade.pnl_pct, 2),
    peak_pnl_pct: pctFromRatio(trustedPeakRatio(trade), 2),
    exit_reason: trade.exit_reason || null,
    entry,
    exit,
    loss_attribution: loss,
    supervision_quality: {
      decision_events_n: decisionEvents.length,
      path_samples_n: pathSamples.length,
      path_quote_samples_n: samplesWithQuote,
      replay_confidence: loss.confidence,
    },
  };
  if (includeTimeline) replay.timeline = buildReplayTimeline(trade, pathSamples, decisionEvents);
  return replay;
}

export function summarizeTradeReplays(replays = []) {
  const summary = {
    trades: replays.length,
    closed: 0,
    open: 0,
    losses: 0,
    wins: 0,
    est_pnl_sol: 0,
    by_root_cause: {},
    by_tag: {},
  };
  for (const replay of replays) {
    const pnlPct = numberOrNull(replay.pnl_pct);
    const size = numberOrNull(replay.position_size_sol) || 0;
    if (replay.exit_ts || replay.exit_reason) summary.closed += 1;
    else summary.open += 1;
    if (pnlPct != null && pnlPct < 0) summary.losses += 1;
    if (pnlPct != null && pnlPct > 0) summary.wins += 1;
    if (pnlPct != null && size) summary.est_pnl_sol += (pnlPct / 100) * size;
    const cause = replay.loss_attribution?.root_cause || 'unknown';
    if (!summary.by_root_cause[cause]) {
      summary.by_root_cause[cause] = { n: 0, est_pnl_sol: 0, examples: [], modification_hint: replay.loss_attribution?.modification_hint || null };
    }
    summary.by_root_cause[cause].n += 1;
    if (pnlPct != null && size) summary.by_root_cause[cause].est_pnl_sol += (pnlPct / 100) * size;
    if (summary.by_root_cause[cause].examples.length < 5) {
      summary.by_root_cause[cause].examples.push({
        trade_id: replay.trade_id,
        symbol: replay.symbol,
        pnl_pct: replay.pnl_pct,
        peak_pnl_pct: replay.peak_pnl_pct,
        exit_reason: replay.exit_reason,
      });
    }
    for (const tag of replay.loss_attribution?.tags || []) {
      summary.by_tag[tag] = (summary.by_tag[tag] || 0) + 1;
    }
  }
  summary.est_pnl_sol = roundNumber(summary.est_pnl_sol, 6);
  for (const cause of Object.values(summary.by_root_cause)) {
    cause.est_pnl_sol = roundNumber(cause.est_pnl_sol, 6);
  }
  return summary;
}
