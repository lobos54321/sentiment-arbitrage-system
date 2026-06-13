function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeTimestampSec(value) {
  const n = numeric(value);
  if (n == null) return null;
  return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

function boolish(value) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    return ['1', 'true', 'yes', 'on', 'ok', 'clean', 'available', 'executable', 'pass', 'enter', 'would_enter'].includes(value.trim().toLowerCase());
  }
  return Boolean(value);
}

function normalizeText(value) {
  const text = String(value ?? '').trim();
  return text || null;
}

function normalizeBlockCause(value) {
  const text = String(value || '').trim().toUpperCase();
  return ['INFRA', 'MARKET', 'POLICY', 'UNKNOWN'].includes(text) ? text : 'UNKNOWN';
}

function increment(map, key, amount = 1) {
  map[key] = Number(map[key] || 0) + amount;
}

function isoFromSec(sec) {
  const n = numeric(sec);
  return n == null ? null : new Date(n * 1000).toISOString();
}

function normalizeRawDog(row = {}) {
  const signalTs = normalizeTimestampSec(row.signal_ts ?? row.timestamp_sec ?? row.timestamp);
  return {
    signal_id: row.signal_id ?? row.id ?? null,
    token_ca: normalizeText(row.token_ca),
    symbol: row.symbol || null,
    signal_ts: signalTs,
    signal_iso: isoFromSec(signalTs),
    lifecycle_id: normalizeText(row.lifecycle_id ?? row.downstream_lifecycle_id),
    raw_primary_tier: row.raw_primary_tier || null,
    max_sustained_peak_pct: numeric(row.max_sustained_peak_pct),
    max_wick_peak_pct: numeric(row.max_wick_peak_pct),
    time_to_sustained_peak_sec: numeric(row.time_to_sustained_peak_sec),
    entry_bar_ts: numeric(row.entry_bar_ts),
    entry_bar_volume: numeric(row.entry_bar_volume),
    entry_bar_volume_raw: numeric(row.entry_bar_volume_raw),
    entry_bar_volume_status: row.entry_bar_volume_status || null,
    entry_bar_volume_provider: row.entry_bar_volume_provider || null,
    entry_bar_volume_source_kind: row.entry_bar_volume_source_kind || null,
    entry_bar_volume_source_family: row.entry_bar_volume_source_family || null,
    entry_bar_trade_count: numeric(row.entry_bar_trade_count),
    early_5m_volume: numeric(row.early_5m_volume),
    early_15m_volume: numeric(row.early_15m_volume),
    early_15m_volume_bar_count: numeric(row.early_15m_volume_bar_count),
    early_15m_positive_volume_bar_count: numeric(row.early_15m_positive_volume_bar_count),
    held_to_silver: boolish(row.held_to_silver),
    held_to_gold: boolish(row.held_to_gold),
    raw_dog_entered: boolish(row.raw_dog_entered),
    raw_dog_realized: boolish(row.raw_dog_realized),
    return_domain: row.return_domain || null,
    return_calculation_rule: row.return_calculation_rule || null,
  };
}

function normalizeDecisionRecord(row = {}) {
  const eventTs = normalizeTimestampSec(row.event_ts ?? row.timestamp ?? row.created_at);
  const action = String(row.action || '').trim().toUpperCase();
  const wouldAction = String(row.would_action || '').trim().toUpperCase();
  const wouldEnter = action === 'WOULD_ENTER'
    || action === 'ENTER'
    || wouldAction === 'WOULD_ENTER'
    || wouldAction === 'ENTER'
    || boolish(row.would_enter_a_class);
  const didEnter = action === 'ENTER' || boolish(row.did_enter);
  return {
    source_kind: row.source_kind || row.source_table || 'decision_record',
    id: row.id ?? null,
    event_ts: eventTs,
    event_iso: isoFromSec(eventTs),
    token_ca: normalizeText(row.token_ca),
    symbol: row.symbol || null,
    lifecycle_id: normalizeText(row.lifecycle_id),
    source_component: row.source_component || null,
    source_reason: row.source_reason || null,
    reason: row.reason || null,
    action: action || null,
    would_action: wouldAction || null,
    quote_available: boolish(row.quote_available),
    quote_executable: boolish(row.quote_executable),
    quote_clean: boolish(row.quote_clean),
    route_available: boolish(row.route_available),
    block_cause: normalizeBlockCause(row.block_cause),
    recoverability: row.recoverability || null,
    classification_reason: row.classification_reason || null,
    evidence_status: row.evidence_status || null,
    provider_hydrate_outcome: row.provider_hydrate_outcome || row.hydrate_outcome || null,
    provider_reason: row.provider_reason || null,
    quote_failure_reason: row.quote_failure_reason || null,
    route_failure_reason: row.route_failure_reason || null,
    hard_blockers_json: row.hard_blockers_json || null,
    expected_rr: numeric(row.expected_rr),
    score: numeric(row.score),
    grade: row.grade || null,
    would_enter: wouldEnter,
    did_enter: didEnter,
  };
}

function rawDogDecisionWindow(rawDog, {
  decisionWindowSec = 900,
  preSignalGraceSec = 60,
} = {}) {
  const signalTs = normalizeTimestampSec(rawDog.signal_ts);
  if (signalTs == null) return { start_ts: null, end_ts: null };
  const peakSec = numeric(rawDog.time_to_sustained_peak_sec);
  const boundedSec = peakSec != null && peakSec > 0
    ? Math.max(60, Math.min(Number(decisionWindowSec || 900), peakSec))
    : Number(decisionWindowSec || 900);
  return {
    start_ts: signalTs - Math.max(0, Number(preSignalGraceSec || 0)),
    end_ts: signalTs + Math.max(0, boundedSec),
  };
}

function decisionSortKey(row) {
  const entered = row.did_enter ? 100 : 0;
  const would = row.would_enter ? 50 : 0;
  const clean = row.quote_clean && row.quote_executable && row.route_available ? 25 : 0;
  return entered + would + clean + Number(row.event_ts || 0) / 1e12;
}

function buildDecisionRecordIndex(decisionRecords = []) {
  const byToken = new Map();
  let count = 0;
  for (const raw of decisionRecords || []) {
    const row = normalizeDecisionRecord(raw);
    if (!row.token_ca || row.event_ts == null) continue;
    if (!byToken.has(row.token_ca)) byToken.set(row.token_ca, []);
    byToken.get(row.token_ca).push(row);
    count += 1;
  }
  for (const rows of byToken.values()) {
    rows.sort((a, b) => Number(a.event_ts || 0) - Number(b.event_ts || 0));
  }
  return { byToken, count };
}

function getIndexedDecisionRecords(decisionRecordsOrIndex, tokenCa) {
  if (decisionRecordsOrIndex?.byToken instanceof Map) {
    return decisionRecordsOrIndex.byToken.get(tokenCa) || [];
  }
  return (decisionRecordsOrIndex || []).map(normalizeDecisionRecord).filter((row) => (
    row.token_ca
    && tokenCa
    && row.token_ca === tokenCa
    && row.event_ts != null
  ));
}

function matchDecisionRecords(rawDog, decisionRecords, options = {}) {
  const dog = normalizeRawDog(rawDog);
  const records = getIndexedDecisionRecords(decisionRecords, dog.token_ca);
  const window = rawDogDecisionWindow(dog, options);
  const inWindow = (row) => (
    window.start_ts != null
    && row.event_ts >= window.start_ts
    && row.event_ts <= window.end_ts
  );
  const lifecycleMatches = dog.lifecycle_id
    ? records.filter((row) => row.lifecycle_id === dog.lifecycle_id && inWindow(row))
    : [];
  const fallbackMatches = records.filter(inWindow);
  const matchedBy = lifecycleMatches.length ? 'lifecycle_id' : fallbackMatches.length ? 'token_time_window' : null;
  const matched = (lifecycleMatches.length ? lifecycleMatches : fallbackMatches)
    .sort((a, b) => decisionSortKey(b) - decisionSortKey(a) || Number(a.event_ts || 0) - Number(b.event_ts || 0));
  return {
    raw_dog: dog,
    matched_by: matchedBy,
    decision_window: {
      ...window,
      start_iso: isoFromSec(window.start_ts),
      end_iso: isoFromSec(window.end_ts),
    },
    records: matched,
  };
}

function classifyMatchedDog(match) {
  const dog = match.raw_dog;
  const records = match.records || [];
  if (!records.length) {
    return {
      ...dog,
      matched_by: null,
      decision_window: match.decision_window,
      decision_record_count: 0,
      terminal_bucket: 'no_decision_record',
      block_cause: null,
      quote_clean: false,
      would_enter: false,
      entered: false,
      held_to_silver_or_gold: dog.raw_dog_realized || dog.held_to_silver || dog.held_to_gold,
      best_decision_record: null,
    };
  }
  const quoteClean = records.some((row) => row.quote_clean && row.quote_executable && row.route_available);
  const wouldEnter = records.some((row) => row.would_enter);
  const entered = records.some((row) => row.did_enter);
  const held = dog.raw_dog_realized || dog.held_to_silver || dog.held_to_gold;
  const blockedRecord = records.find((row) => !row.quote_clean || !row.quote_executable || !row.route_available || row.block_cause !== 'UNKNOWN') || records[0];
  const blockCause = normalizeBlockCause(blockedRecord?.block_cause);
  let terminalBucket;
  if (!quoteClean) terminalBucket = `not_quote_clean_${blockCause}`;
  else if (!wouldEnter) terminalBucket = 'quote_clean_no_would_enter';
  else if (!entered) terminalBucket = 'would_enter_not_entered';
  else if (!held) terminalBucket = 'entered_not_held';
  else terminalBucket = 'held_to_silver_or_gold';
  return {
    ...dog,
    matched_by: match.matched_by,
    decision_window: match.decision_window,
    decision_record_count: records.length,
    terminal_bucket: terminalBucket,
    block_cause: quoteClean ? null : blockCause,
    quote_clean: quoteClean,
    would_enter: wouldEnter,
    entered,
    held_to_silver_or_gold: held,
    best_decision_record: records[0] || null,
  };
}

function buildRawDogDecisionFunnel({
  rawDogs = [],
  decisionRecords = [],
  decisionWindowSec = 900,
  preSignalGraceSec = 60,
} = {}) {
  const decisionIndex = decisionRecords?.byToken instanceof Map
    ? decisionRecords
    : buildDecisionRecordIndex(decisionRecords);
  const matches = (rawDogs || []).map((rawDog) => matchDecisionRecords(rawDog, decisionIndex, {
    decisionWindowSec,
    preSignalGraceSec,
  }));
  const dogs = matches.map(classifyMatchedDog);
  const terminalBuckets = {};
  const blockCause = { INFRA: 0, MARKET: 0, POLICY: 0, UNKNOWN: 0 };
  for (const dog of dogs) {
    increment(terminalBuckets, dog.terminal_bucket);
    if (dog.block_cause) increment(blockCause, normalizeBlockCause(dog.block_cause));
  }
  const hasDecisionRecord = dogs.filter((dog) => dog.decision_record_count > 0).length;
  const notQuoteClean = dogs.filter((dog) => dog.decision_record_count > 0 && !dog.quote_clean).length;
  const quoteClean = dogs.filter((dog) => dog.quote_clean).length;
  const wouldEnter = dogs.filter((dog) => dog.would_enter).length;
  const entered = dogs.filter((dog) => dog.entered).length;
  const held = dogs.filter((dog) => dog.held_to_silver_or_gold).length;
  return {
    schema_version: 'raw_dog_decision_funnel.v1',
    summary: {
      raw_sustained_dogs: dogs.length,
      decision_records_seen: decisionIndex.count ?? (decisionRecords || []).length,
      has_decision_record: hasDecisionRecord,
      no_decision_record: dogs.length - hasDecisionRecord,
      not_quote_clean: notQuoteClean,
      quote_clean: quoteClean,
      would_enter: wouldEnter,
      entered,
      held_to_silver_or_gold: held,
      block_cause: blockCause,
      terminal_buckets: terminalBuckets,
    },
    config: {
      decision_window_sec: decisionWindowSec,
      pre_signal_grace_sec: preSignalGraceSec,
      join_order: 'lifecycle_id_then_token_signal_time_window',
      quote_source: 'decision_time_records_only',
    },
    dogs,
    notes: {
      first_cut: 'no_decision_record determines whether the main bottleneck is pipeline coverage before blocker analysis.',
      denominator: 'Counts are dog-level unique sustained raw dogs, not event rows.',
      interpretation: 'Small samples should be used for dominant-bucket direction only, not precise percentages.',
    },
  };
}

export {
  buildRawDogDecisionFunnel,
  buildDecisionRecordIndex,
  matchDecisionRecords,
  normalizeDecisionRecord,
  normalizeRawDog,
  rawDogDecisionWindow,
};
