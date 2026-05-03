const BLOCKING_DECISIONS = new Set(['reject', 'skip', 'abort', 'remove', 'expire', 'block', 'fail']);
const ACTIVE_DECISIONS = new Set(['pending', 'pass', 'arm', 'registered', 'candidate', 'received', 'warn']);

function clean(value) {
  if (value === undefined || value === null) return null;
  const str = String(value).trim();
  return str ? str : null;
}

function lower(value) {
  return String(value || '').toLowerCase();
}

export function pipelineStageForDecision(row = {}) {
  const component = lower(row.component);
  const eventType = lower(row.event_type);
  const dataSource = lower(row.data_source);

  if (component === 'execution_api' && eventType.includes('entry')) return 'execution_quote';
  if (component === 'execution_api' && eventType.includes('exit')) return 'exit_execution_quote';
  if (component.includes('readiness') || component.includes('lifecycle')) return 'lifecycle_readiness';
  if (component.includes('token_risk') || component.includes('gatekeeper') || component.includes('gmgn_policy')) return 'hard_risk';
  if (component.includes('lotto') || component.includes('matrix') || component.includes('route')) return 'route_gate';
  if (component.includes('smart') || component.includes('entry_engine') || component.includes('entry_decision')) return 'smart_entry';
  if (component.includes('data') || dataSource.includes('dex') || dataSource.includes('helius') || dataSource.includes('gmgn')) return 'data_health';
  if (eventType.includes('exit') || eventType.includes('close')) return 'exit';
  return 'unknown';
}

export function statusForDecision(row = {}) {
  const decision = lower(row.decision);
  const eventType = lower(row.event_type);
  if (row.component === 'execution_api' && decision === 'filled_paper') return 'entered';
  if (eventType.includes('exit') || eventType.includes('close') || decision === 'closed') return 'closed';
  if (BLOCKING_DECISIONS.has(decision)) return 'blocked';
  if (decision === 'wait') return 'waiting';
  if (ACTIVE_DECISIONS.has(decision)) return 'active';
  return decision || 'unknown';
}

function priorityForStatus(status) {
  switch (status) {
    case 'closed': return 60;
    case 'entered': return 55;
    case 'blocked': return 50;
    case 'waiting': return 40;
    case 'active': return 20;
    default: return 10;
  }
}

export function finalBlockerFromEvent(row = {}, payload = {}) {
  const status = statusForDecision(row);
  const reason = clean(row.reason) || clean(payload.reason) || clean(payload.reject_reason) || clean(payload.failureReason);
  return {
    status,
    is_blocker: status === 'blocked' || status === 'waiting',
    stage: pipelineStageForDecision(row),
    component: clean(row.component),
    event_type: clean(row.event_type),
    decision: clean(row.decision),
    reason,
    data_source: clean(row.data_source),
    event_id: row.id ?? null,
    event_ts: row.event_ts ?? null,
    trade_id: row.trade_id ?? null,
  };
}

export function finalBlockerFromTrade(trade = {}) {
  const closed = Boolean(trade.exit_ts || trade.exit_reason);
  return {
    status: closed ? 'closed' : 'entered',
    is_blocker: false,
    stage: closed ? 'exit' : 'execution_quote',
    component: 'paper_trades',
    event_type: closed ? 'trade_closed' : 'trade_open',
    decision: closed ? 'closed' : 'entered',
    reason: clean(trade.exit_reason) || (closed ? 'closed' : 'open_position'),
    data_source: 'paper_trades',
    event_id: null,
    event_ts: closed ? trade.exit_ts : trade.entry_ts,
    trade_id: trade.id ?? null,
  };
}

export function finalBlockerFromMissed(row = {}) {
  return {
    status: 'blocked',
    is_blocker: true,
    stage: pipelineStageForDecision(row),
    component: clean(row.component),
    event_type: 'missed_attribution',
    decision: clean(row.decision) || 'missed',
    reason: clean(row.reject_reason),
    data_source: 'paper_missed_signal_attribution',
    event_id: null,
    event_ts: row.signal_ts ?? null,
    trade_id: null,
  };
}

export function chooseFinalBlocker(current, candidate) {
  if (!candidate) return current || null;
  if (!current) return candidate;
  const currentPriority = priorityForStatus(current.status);
  const candidatePriority = priorityForStatus(candidate.status);
  if (candidatePriority > currentPriority) return candidate;
  if (candidatePriority < currentPriority) return current;
  const currentTs = Number(current.event_ts || 0);
  const candidateTs = Number(candidate.event_ts || 0);
  if (candidateTs > currentTs) return candidate;
  if (candidateTs < currentTs) return current;
  const currentId = Number(current.event_id || 0);
  const candidateId = Number(candidate.event_id || 0);
  return candidateId >= currentId ? candidate : current;
}

export function applyFinalBlocker(summary = {}) {
  const fallback = summary.final_blocker || {
    status: clean(summary.final_status) || 'unknown',
    is_blocker: ['blocked', 'waiting'].includes(clean(summary.final_status)),
    stage: 'unknown',
    component: clean(summary.final_component),
    event_type: clean(summary.final_event_type),
    decision: clean(summary.final_decision),
    reason: clean(summary.final_reason),
    data_source: clean(summary.final_data_source),
    event_id: summary.final_event_id ?? null,
    event_ts: summary.last_event_ts ?? summary.entry_ts ?? null,
    trade_id: summary.trade_id ?? null,
  };
  summary.final_blocker = fallback;
  summary.final_status = fallback.status || summary.final_status;
  summary.final_decision = fallback.decision || summary.final_decision;
  summary.final_component = fallback.component || summary.final_component;
  summary.final_event_type = fallback.event_type || summary.final_event_type;
  summary.final_reason = fallback.reason || summary.final_reason;
  summary.final_data_source = fallback.data_source || summary.final_data_source;
  summary.final_blocker_key = [
    fallback.stage || 'unknown',
    fallback.component || '-',
    fallback.reason || '-',
  ].join(':');
  return summary;
}
