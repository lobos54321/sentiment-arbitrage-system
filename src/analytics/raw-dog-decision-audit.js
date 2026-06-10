import {
  buildRawDogDecisionFunnel,
} from './raw-dog-decision-funnel.js';

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function round(value, digits = 3) {
  const n = numeric(value);
  if (n == null) return null;
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
}

function parseJsonValue(value, fallback = null) {
  if (value == null || value === '') return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(String(value));
  } catch {
    return fallback;
  }
}

function parseList(value) {
  const parsed = parseJsonValue(value, value);
  if (Array.isArray(parsed)) return parsed.map((item) => String(item || '').trim()).filter(Boolean);
  if (parsed == null || parsed === '') return [];
  return [String(parsed).trim()].filter(Boolean);
}

function increment(map, key, amount = 1) {
  const normalized = String(key || 'unknown');
  map[normalized] = Number(map[normalized] || 0) + amount;
}

function countBy(rows = [], field) {
  const out = {};
  for (const row of rows || []) {
    const value = typeof field === 'function' ? field(row) : row?.[field];
    increment(out, value ?? 'unknown');
  }
  return out;
}

function percentile(sortedValues, p) {
  if (!sortedValues.length) return null;
  const idx = Math.min(sortedValues.length - 1, Math.max(0, Math.ceil(sortedValues.length * p) - 1));
  return sortedValues[idx];
}

function summarizeNumeric(rows, field) {
  const values = (rows || [])
    .map((row) => numeric(row[field]))
    .filter((value) => value != null)
    .sort((a, b) => a - b);
  if (!values.length) {
    return {
      n: (rows || []).length,
      observed_n: 0,
      missing_n: (rows || []).length,
      min: null,
      median: null,
      p75: null,
      p90: null,
      max: null,
    };
  }
  return {
    n: (rows || []).length,
    observed_n: values.length,
    missing_n: (rows || []).length - values.length,
    min: round(values[0], 6),
    median: round(percentile(values, 0.5), 6),
    p75: round(percentile(values, 0.75), 6),
    p90: round(percentile(values, 0.9), 6),
    max: round(values[values.length - 1], 6),
  };
}

function scoreBand(score) {
  const n = numeric(score);
  if (n == null) return 'matrix_score_unknown';
  if (n < 70) return 'matrix_score_below_70';
  if (n < 82) return 'matrix_score_70_81';
  if (n < 92) return 'matrix_score_82_91';
  return 'matrix_score_92_plus';
}

function rrBand(expectedRr) {
  const n = numeric(expectedRr);
  if (n == null) return 'expected_rr_unknown';
  if (n < 2) return 'expected_rr_below_2';
  if (n < 3) return 'expected_rr_2_3';
  if (n < 5) return 'expected_rr_3_5';
  return 'expected_rr_5_plus';
}

function bestRecord(row = {}) {
  return row.best_decision_record || {};
}

function collectGateReasons(row = {}) {
  const record = bestRecord(row);
  const reasons = [];
  for (const blocker of parseList(record.hard_blockers_json)) reasons.push(blocker);
  if (record.source_component || record.source_reason || record.reason) {
    reasons.push([
      record.source_component || 'unknown_component',
      record.source_reason || record.reason || 'unknown_reason',
    ].join(':'));
  }
  const score = scoreBand(record.score);
  const rr = rrBand(record.expected_rr);
  if (score === 'matrix_score_below_70') reasons.push(score);
  if (rr === 'expected_rr_below_2') reasons.push(rr);
  if (!reasons.length) reasons.push('no_explicit_gate_reason');
  return [...new Set(reasons.map((item) => String(item || '').trim()).filter(Boolean))];
}

function countGateReasons(rows = []) {
  const gateReasonCounts = {};
  const scoreBands = {};
  const expectedRrBands = {};
  const sourceReasonCounts = {};
  for (const row of rows) {
    const record = bestRecord(row);
    for (const reason of collectGateReasons(row)) increment(gateReasonCounts, reason);
    increment(scoreBands, scoreBand(record.score));
    increment(expectedRrBands, rrBand(record.expected_rr));
    if (record.source_component || record.source_reason || record.reason) {
      increment(sourceReasonCounts, [
        record.source_component || 'unknown_component',
        record.source_reason || record.reason || 'unknown_reason',
      ].join(':'));
    }
  }
  return {
    gate_reason_counts: gateReasonCounts,
    score_bands: scoreBands,
    expected_rr_bands: expectedRrBands,
    source_reason_counts: sourceReasonCounts,
  };
}

function volumeSummary(rows = []) {
  const q5Threshold = 15934;
  const observed = rows.filter((row) => numeric(row.entry_bar_volume) != null);
  return {
    entry_bar_volume: summarizeNumeric(rows, 'entry_bar_volume'),
    entry_bar_volume_raw: summarizeNumeric(rows, 'entry_bar_volume_raw'),
    early_5m_volume: summarizeNumeric(rows, 'early_5m_volume'),
    early_15m_volume: summarizeNumeric(rows, 'early_15m_volume'),
    entry_bar_volume_q5_threshold: q5Threshold,
    entry_bar_volume_q5_or_above_n: rows.filter((row) => {
      const volume = numeric(row.entry_bar_volume);
      return volume != null && volume >= q5Threshold;
    }).length,
    observed_rate: rows.length ? round(observed.length / rows.length, 4) : null,
    status_counts: countBy(rows, 'entry_bar_volume_status'),
    provider_counts: countBy(rows, 'entry_bar_volume_provider'),
    source_kind_counts: countBy(rows, 'entry_bar_volume_source_kind'),
  };
}

function compactDogRow(row = {}) {
  const record = bestRecord(row);
  const hydrateOutcome = record.provider_hydrate_outcome || record.hydrate_outcome || null;
  const hydrateReason = record.provider_reason || record.quote_failure_reason || record.route_failure_reason || null;
  return {
    token_ca: row.token_ca,
    symbol: row.symbol || null,
    signal_ts: row.signal_ts,
    raw_primary_tier: row.raw_primary_tier,
    max_sustained_peak_pct: row.max_sustained_peak_pct,
    terminal_bucket: row.terminal_bucket,
    entry_bar_volume: row.entry_bar_volume ?? null,
    entry_bar_volume_raw: row.entry_bar_volume_raw ?? null,
    entry_bar_volume_status: row.entry_bar_volume_status ?? null,
    entry_bar_volume_provider: row.entry_bar_volume_provider ?? null,
    entry_bar_volume_source_kind: row.entry_bar_volume_source_kind ?? null,
    early_5m_volume: row.early_5m_volume ?? null,
    early_15m_volume: row.early_15m_volume ?? null,
    early_15m_positive_volume_bar_count: row.early_15m_positive_volume_bar_count ?? null,
    matched_by: row.matched_by || null,
    decision_record_count: row.decision_record_count || 0,
    provider_hydrate_outcome: hydrateOutcome,
    provider_hydrate_reason: hydrateReason,
    gate_reasons: collectGateReasons(row),
    best_decision_record: {
      source_kind: record.source_kind || null,
      event_ts: record.event_ts || null,
      action: record.action || null,
      score: record.score ?? null,
      expected_rr: record.expected_rr ?? null,
      source_component: record.source_component || null,
      source_reason: record.source_reason || record.reason || null,
      hard_blockers_json: record.hard_blockers_json || null,
      block_cause: record.block_cause || null,
      quote_clean: record.quote_clean ?? null,
      quote_executable: record.quote_executable ?? null,
      route_available: record.route_available ?? null,
      provider_hydrate_outcome: hydrateOutcome,
      provider_hydrate_reason: hydrateReason,
      provider_reason: record.provider_reason || null,
      quote_failure_reason: record.quote_failure_reason || null,
      route_failure_reason: record.route_failure_reason || null,
    },
  };
}

function interpretAudit({ dogRows, dudRows }) {
  const minDogRows = 20;
  if (dogRows.length < minDogRows) {
    return {
      dominant_observation: `Only ${dogRows.length} quote-clean/no-would-enter raw dogs; use as direction only.`,
      next_main_contradiction: 'sample_too_small',
      do_not_change_strategy: true,
    };
  }
  const dogVolume = summarizeNumeric(dogRows, 'entry_bar_volume');
  const dudVolume = summarizeNumeric(dudRows, 'entry_bar_volume');
  if (!dogVolume.observed_n || !dudVolume.observed_n) {
    return {
      dominant_observation: 'Entry-volume coverage is insufficient for dog-vs-dud separation.',
      next_main_contradiction: 'evidence_unavailable',
      do_not_change_strategy: true,
    };
  }
  const dogMedian = numeric(dogVolume.median);
  const dudMedian = numeric(dudVolume.median);
  if (dogMedian != null && dudMedian != null && dogMedian >= Math.max(1, dudMedian) * 2) {
    return {
      dominant_observation: 'Rejected raw dogs show materially higher entry-bar volume than same-bucket duds.',
      next_main_contradiction: 'gate_too_strict',
      do_not_change_strategy: true,
    };
  }
  return {
    dominant_observation: 'Rejected raw dogs do not clearly separate from same-bucket duds on entry-bar volume.',
    next_main_contradiction: 'no_ex_ante_separation',
    do_not_change_strategy: true,
  };
}

function buildRawDogDecisionAudit({
  rawDogs = [],
  dudCandidates = [],
  decisionRecords = [],
  hours = 24,
  sinceTs = null,
  untilTs = Math.floor(Date.now() / 1000),
  rawDbPath = null,
  paperDbPath = null,
  maxDuds = 200,
} = {}) {
  const comparisonRows = (dudCandidates || []).slice(0, maxDuds);
  const funnel = buildRawDogDecisionFunnel({
    rawDogs,
    comparisonRows,
    decisionRecords,
  });
  const dogRows = funnel.dogs.filter((row) => row.terminal_bucket === 'quote_clean_no_would_enter');
  const dudFunnel = buildRawDogDecisionFunnel({
    rawDogs: comparisonRows,
    decisionRecords,
  });
  const dudRows = dudFunnel.dogs.filter((row) => row.terminal_bucket === 'quote_clean_no_would_enter');
  const dogGate = countGateReasons(dogRows);
  const dudGate = countGateReasons(dudRows);
  const interpretation = interpretAudit({ dogRows, dudRows });
  return {
    schema_version: 'raw_dog_decision_audit.v1',
    generated_at: new Date().toISOString(),
    status: 'ok',
    window: {
      hours,
      since_ts: sinceTs,
      since_iso: sinceTs ? new Date(Number(sinceTs) * 1000).toISOString() : null,
      until_ts: untilTs,
      until_iso: untilTs ? new Date(Number(untilTs) * 1000).toISOString() : null,
    },
    inputs: {
      raw_db_path: rawDbPath,
      paper_db_path: paperDbPath,
      raw_dogs_n: rawDogs.length,
      dud_candidates_n: dudCandidates.length,
      dud_sample_n: comparisonRows.length,
      decision_records_n: decisionRecords.length,
    },
    funnel: funnel.summary,
    quote_clean_no_would_enter_audit: {
      raw_dogs_n: dogRows.length,
      comparison_duds_n: dudRows.length,
      raw_dog_gate_reasons: dogGate,
      dud_gate_reasons: dudGate,
      hydrate_outcome_counts: countBy(dogRows, (row) => {
        const record = bestRecord(row);
        return record.provider_hydrate_outcome || record.hydrate_outcome || 'not_recorded';
      }),
      entry_volume: {
        raw_dogs: volumeSummary(dogRows),
        comparison_duds: volumeSummary(dudRows),
      },
      dog_rows: dogRows.map(compactDogRow),
      dud_summary: {
        terminal_bucket: 'quote_clean_no_would_enter',
        examples: dudRows.slice(0, 25).map(compactDogRow),
      },
    },
    interpretation,
  };
}

function markdownForAudit(report = {}) {
  const audit = report.quote_clean_no_would_enter_audit || {};
  const lines = [];
  lines.push('# Raw Dog Decision Audit');
  lines.push('');
  lines.push(`Generated: ${report.generated_at || 'unknown'}`);
  lines.push(`Window: ${report.window?.hours || 'unknown'}h`);
  lines.push('');
  lines.push('## Funnel');
  lines.push('');
  lines.push('```json');
  lines.push(JSON.stringify(report.funnel || {}, null, 2));
  lines.push('```');
  lines.push('');
  lines.push('## Quote-Clean / No-Would-Enter Audit');
  lines.push('');
  lines.push(`Raw dogs: ${audit.raw_dogs_n ?? 0}`);
  lines.push(`Comparison duds: ${audit.comparison_duds_n ?? 0}`);
  lines.push('');
  lines.push('### Raw Dog Gate Reasons');
  lines.push('');
  lines.push('```json');
  lines.push(JSON.stringify(audit.raw_dog_gate_reasons || {}, null, 2));
  lines.push('```');
  lines.push('');
  lines.push('### Entry Volume');
  lines.push('');
  lines.push('```json');
  lines.push(JSON.stringify(audit.entry_volume || {}, null, 2));
  lines.push('```');
  lines.push('');
  lines.push('## Interpretation');
  lines.push('');
  lines.push(`- Dominant observation: ${report.interpretation?.dominant_observation || 'unknown'}`);
  lines.push(`- Next main contradiction: ${report.interpretation?.next_main_contradiction || 'unknown'}`);
  lines.push(`- Do not change strategy: ${report.interpretation?.do_not_change_strategy !== false}`);
  lines.push('');
  return `${lines.join('\n')}\n`;
}

export {
  buildRawDogDecisionAudit,
  collectGateReasons,
  markdownForAudit,
  volumeSummary,
};
