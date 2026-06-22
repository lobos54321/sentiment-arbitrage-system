#!/usr/bin/env node
// Live Fullnet Row Report v2 — research-only module-closure + row-confidence + final repair-owner.
// Implements plan §15.12-§15.15 on top of the frozen v1 generator (build-live-fullnet-row-report.js):
//   - every signal keeps the v1 chain (source->denominator->lifecycle/Markov->decision->quote->
//     would_enter->entry->ledger/exit/friction, each value-or-missing_reason) and gains
//     row_confidence + ev_eligible + final_repair_owner (§15.15.20 ladder);
//   - every discovered module is mapped to an A-G bucket or explicitly excluded (§15.15.18);
//   - actual EV stays null unless a row has VALID entered/fill/exit/ledger/friction evidence.
// No strategy/gate/entry/exit/size change. No threshold tuning. No historical FBR/RED-bar/
// Stage2A/68% evidence used as a trading rule (those stay artifact-only, §15.15.17/§15.15.22).
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';
import { buildReport } from './build-live-fullnet-row-report.js';

function rate(n, d) { return d ? Math.round((n / d) * 1e6) / 1e6 : null; }
function average(values) {
  const a = values.map(Number).filter((v) => Number.isFinite(v));
  return a.length ? Math.round((a.reduce((x, y) => x + y, 0) / a.length) * 1e4) / 1e4 : null;
}
function countBy(rows, fn) { const o = {}; for (const r of rows) { const k = fn(r) ?? 'unknown'; o[k] = (o[k] || 0) + 1; } return o; }
function topReasons(rows, fn) {
  return Object.entries(countBy(rows, fn)).map(([key, n]) => ({ key, n })).sort((a, b) => b.n - a.n || a.key.localeCompare(b.key));
}

// ---- §15.15.20 final repair-owner ladder (deterministic, evidence-truth-before-strategy) ------
const REPAIR_LADDER = [
  'ROW_IDENTITY_UNIT_OR_DENOMINATOR_INVALID',          // 0
  'SOURCE_PARSER_SESSION_OR_REGISTRY_GAP',             // 1
  'FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP',// 2
  'OPS_WORKER_READMODEL_STORAGE_OR_CONFIG_GAP',        // 3
  'PAPER_LIVE_OPERATOR_WRITE_OR_SECURITY_GAP',         // 4
  'QUOTE_INTENT_PROVIDER_EXECUTOR_OR_IDEMPOTENCY_GAP', // 5
  'EX_ANTE_ACTIONABILITY_OR_FILL_ANCHOR_GAP',          // 6
  'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP',        // 7
  'DIRECT_STRATEGY_DECISION_MODULE_GAP',               // 8
  'ENTRY_BRIDGE_GAP',                                  // 9
  'LEDGER_EXIT_HOLD_FRICTION_GAP',                     // 10
  'SHADOW_ONLY_OR_ADVISORY_ONLY',                      // 11
  'INTENTIONALLY_EXCLUDED_NOT_CAPTURE_RELEVANT',       // 12
];

const DATA_PROVENANCE_RE = /KLINE|UNKNOWN_DATA|DATA_BLOCKED|DATA_UNKNOWN|NO_BARS|COVERAGE|RETRY|BACKFILL|PROVENANCE/i;

function quoteRouteBlocked(row) {
  const blockers = Array.isArray(row.hard_blockers) ? row.hard_blockers : [];
  return row.route_missing_reason != null
    || blockers.some((b) => /quote|route|liquidity|spread|quote_age|stale|provider/i.test(String(b).toLowerCase()));
}

// Returns one of REPAIR_LADDER (or NONE_NO_REPAIR_NEEDED for a clean captured row). Pure/deterministic.
function finalRepairOwner(row) {
  const evEligible = Boolean(row.entered && row.ledger_seen && row.exit_ts != null
    && row.round_trip_friction_pct != null && row.realized_pnl_pct != null);
  if (row.held_to_silver_or_gold === true || (row.entered === true && evEligible)) return 'NONE_NO_REPAIR_NEEDED';
  const sourceStatus = String(row.source_hard_gate_status || '');
  const preds = [
    /* 0 */ () => !row.token_ca || row.signal_ts == null || !['dog', 'dud', 'pending'].includes(row.class),
    /* 1 */ () => row.source_seen !== true,
    /* 2 */ () => row.raw_missing_reason != null || DATA_PROVENANCE_RE.test(sourceStatus),
    /* 3 */ () => row.ops_worker_gap === true,                 // not observable in current exports
    /* 4 */ () => row.paper_live_violation === true,           // not observable in current exports
    /* 5 */ () => row.has_decision === true && row.quote_clean !== true && quoteRouteBlocked(row),
    /* 6 */ () => row.ex_ante_actionability_gap === true,      // not observable in current exports
    /* 7 */ () => row.has_decision === true && row.quote_clean === true && row.would_enter !== true && row.readiness_ok === false,
    /* 8 */ () => row.would_enter !== true && row.entered !== true,   // residual strategy-decision gap
    /* 9 */ () => row.would_enter === true && row.entered !== true && row.shadow_only !== true,
    /* 10 */ () => row.entered === true && (!row.ledger_seen || row.exit_ts == null || row.round_trip_friction_pct == null),
    /* 11 */ () => row.shadow_only === true || (row.would_enter === true && row.entered !== true),
    /* 12 */ () => true,
  ];
  for (let i = 0; i < preds.length; i += 1) { if (preds[i]()) return REPAIR_LADDER[i]; }
  return 'INTENTIONALLY_EXCLUDED_NOT_CAPTURE_RELEVANT';
}

// ---- §15.14.14 row confidence ladder ----------------------------------------------------------
// HIGH requires ops + promotion evidence too; those modules (§15.14 P*/§15.15 F) are not in the
// current exports, so real rows top out at MEDIUM. ev_eligible is the ONLY EV gate.
function rowConfidence(row) {
  const warnings = [];
  const evEligible = Boolean(row.entered && row.ledger_seen && row.exit_ts != null
    && row.round_trip_friction_pct != null && row.realized_pnl_pct != null);
  const identityValid = Boolean(row.token_ca) && row.signal_ts != null;
  const opsPresent = row.ops_evidence_present === true;
  const promoPresent = row.promotion_evidence_present === true;
  if (!identityValid) warnings.push('row_identity_or_denominator_invalid');
  if (row.has_decision !== true) warnings.push('no_decision_event');
  if (row.raw_missing_reason != null) warnings.push('raw_label_or_kline_provenance_incomplete');
  if (row.quote_seen !== true) warnings.push('no_quote_evidence');
  if (!opsPresent) warnings.push('operational_truth_modules_not_exported');
  if (!promoPresent) warnings.push('promotion_governance_modules_not_exported');
  if (row.entered === true && !evEligible) warnings.push('entered_without_valid_ledger_fill_exit_friction');

  let confidence;
  if (!identityValid) confidence = 'LOW';
  else if (row.entered === true && !evEligible) confidence = 'INVALID_FOR_EV';
  else {
    const criticalPresent = row.source_seen === true && row.has_decision === true
      && row.quote_seen === true && row.raw_missing_reason == null;
    if (!criticalPresent) confidence = 'LOW';
    else if (opsPresent && promoPresent) confidence = 'HIGH';
    else confidence = 'MEDIUM';
  }
  return { row_confidence: confidence, ev_eligible: evEligible, row_confidence_warnings: warnings };
}

// ---- §15.15.18 module-closure map (every module -> A-G bucket or explicit exclusion) ----------
// coverage_status: covered (present in current exports), blocked (row-relevant but needs a new
// export), intentionally_excluded (bucket G). repair_owner = §15.15.20 owner if this group is the gap.
const MODULE_CLOSURE_MAP = [
  // A. Direct capture — all carried by the v1 chain (value-or-missing_reason)
  { group: 'source_metadata', bucket: 'A', coverage_status: 'covered', row_fields: ['source_seen', 'source_hard_gate_status', 'signal_type', 'is_ath'], repair_owner: 'SOURCE_PARSER_SESSION_OR_REGISTRY_GAP' },
  { group: 'lifecycle', bucket: 'A', coverage_status: 'covered', row_fields: ['lifecycle_identity_seen', 'lifecycle_state', 'lifecycle_route_profile', 'lifecycle_missing_reason'], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP' },
  { group: 'markov', bucket: 'A', coverage_status: 'covered', row_fields: ['markov_bucket', 'markov_regime', 'markov_action', 'markov_not_evaluated_reason'], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP' },
  { group: 'filters_gates', bucket: 'A', coverage_status: 'covered', row_fields: ['filter_family', 'filter_result', 'hard_blockers', 'source_hard_gate_status'], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP' },
  { group: 'matrix', bucket: 'A', coverage_status: 'covered', row_fields: ['matrix_grade', 'matrix_score', 'matrix_missing_reason'], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP' },
  { group: 'readiness', bucket: 'A', coverage_status: 'covered', row_fields: ['readiness_ok', 'readiness_blocker', 'readiness_missing_reason'], repair_owner: 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP' },
  { group: 'quote_route', bucket: 'A', coverage_status: 'covered', row_fields: ['quote_clean', 'executable_quote_clean', 'quote_missing_reason', 'route_missing_reason'], repair_owner: 'QUOTE_INTENT_PROVIDER_EXECUTOR_OR_IDEMPOTENCY_GAP' },
  { group: 'would_enter', bucket: 'A', coverage_status: 'covered', row_fields: ['would_enter', 'would_enter_sec_after_signal'], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP' },
  { group: 'entry', bucket: 'A', coverage_status: 'covered', row_fields: ['entry_mode', 'entry_intent_seen', 'entry_guard_seen', 'execution_bridge_owner', 'execution_missing_reason'], repair_owner: 'ENTRY_BRIDGE_GAP' },
  { group: 'ledger', bucket: 'A', coverage_status: 'covered', row_fields: ['ledger_seen', 'ledger_missing_reason'], repair_owner: 'LEDGER_EXIT_HOLD_FRICTION_GAP' },
  { group: 'exit_hold', bucket: 'A', coverage_status: 'covered', row_fields: ['exit_ts', 'held_to_silver_or_gold', 'time_held_sec'], repair_owner: 'LEDGER_EXIT_HOLD_FRICTION_GAP' },
  { group: 'friction', bucket: 'A', coverage_status: 'covered', row_fields: ['round_trip_friction_pct', 'net_pnl_pct', 'friction_missing_reason'], repair_owner: 'LEDGER_EXIT_HOLD_FRICTION_GAP' },
  // B. Runtime decision / shadow feature modules
  { group: 'gmgn_policy', bucket: 'B', coverage_status: 'blocked', row_fields: [], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP', note: 'not_in_current_exports' },
  { group: 'freshness', bucket: 'B', coverage_status: 'covered', row_fields: ['readiness_data_state', 'freshness'], repair_owner: 'FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP' },
  { group: 'token_memory', bucket: 'B', coverage_status: 'blocked', row_fields: [], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP', note: 'not_in_current_exports' },
  { group: 'a_class_internals', bucket: 'B', coverage_status: 'covered', row_fields: ['decision_sources', 'block_cause'], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP' },
  { group: 'source_resonance', bucket: 'B', coverage_status: 'blocked', row_fields: [], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP', note: 'not_in_current_exports' },
  { group: 'narrative_social', bucket: 'B', coverage_status: 'covered', row_fields: ['narrative_score', 'ai_narrative_tier'], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP' },
  { group: 'smart_money', bucket: 'B', coverage_status: 'blocked', row_fields: [], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP', note: 'not_in_current_exports' },
  { group: 'curve_pumpfun', bucket: 'B', coverage_status: 'blocked', row_fields: [], repair_owner: 'FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP', note: 'not_in_current_exports' },
  { group: 'scout_quality', bucket: 'B', coverage_status: 'blocked', row_fields: [], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP', note: 'not_in_current_exports' },
  // C. Deep persisted fact sources
  { group: 'premium_signals', bucket: 'C', coverage_status: 'covered', row_fields: ['source_seen', 'signal_type'], repair_owner: 'SOURCE_PARSER_SESSION_OR_REGISTRY_GAP' },
  { group: 'source_snapshots', bucket: 'C', coverage_status: 'covered', row_fields: ['source_row_n', 'source_source_ids'], repair_owner: 'SOURCE_PARSER_SESSION_OR_REGISTRY_GAP' },
  { group: 'gates_db', bucket: 'C', coverage_status: 'covered', row_fields: ['source_hard_gate_status', 'source_gate_statuses_seen'], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP' },
  { group: 'shadow_outcomes', bucket: 'C', coverage_status: 'covered', row_fields: ['raw_seen', 'max_sustained_peak_pct', 'max_wick_peak_pct'], repair_owner: 'FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP' },
  { group: 'paper_path_samples', bucket: 'C', coverage_status: 'blocked', row_fields: ['ledger_missing_reason'], repair_owner: 'LEDGER_EXIT_HOLD_FRICTION_GAP', note: 'paper_trades=0_in_window' },
  { group: 'watchlist', bucket: 'C', coverage_status: 'blocked', row_fields: [], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP', note: 'not_in_current_exports' },
  { group: 'lifecycle_tracks', bucket: 'C', coverage_status: 'covered', row_fields: ['lifecycle_state', 'lifecycle_source'], repair_owner: 'DIRECT_STRATEGY_DECISION_MODULE_GAP' },
  { group: 'kline_cache', bucket: 'C', coverage_status: 'blocked', row_fields: ['raw_missing_reason'], repair_owner: 'FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP', note: 'kline_provenance_not_joined_in_current_pack' },
  { group: 'source_channel_dbs', bucket: 'C', coverage_status: 'blocked', row_fields: [], repair_owner: 'SOURCE_PARSER_SESSION_OR_REGISTRY_GAP', note: 'channel_registry_not_in_current_exports' },
  // D. Operational truth / execution-control (§15.14 P0-P11) — not in current exports
  { group: 'parser_session', bucket: 'D', coverage_status: 'blocked', row_fields: [], repair_owner: 'SOURCE_PARSER_SESSION_OR_REGISTRY_GAP', note: 'ingestion_session_truth_not_exported' },
  { group: 'identity_unit_finality', bucket: 'D', coverage_status: 'blocked', row_fields: [], repair_owner: 'ROW_IDENTITY_UNIT_OR_DENOMINATOR_INVALID', note: 'identity_unit_provider_finality_not_exported' },
  { group: 'null_precision', bucket: 'D', coverage_status: 'blocked', row_fields: [], repair_owner: 'ROW_IDENTITY_UNIT_OR_DENOMINATOR_INVALID', note: 'numeric_precision_truth_not_exported' },
  { group: 'raw_provider_evidence', bucket: 'D', coverage_status: 'blocked', row_fields: [], repair_owner: 'QUOTE_INTENT_PROVIDER_EXECUTOR_OR_IDEMPOTENCY_GAP', note: 'raw_provider_evidence_not_exported' },
  { group: 'quote_intent_binding', bucket: 'D', coverage_status: 'blocked', row_fields: [], repair_owner: 'QUOTE_INTENT_PROVIDER_EXECUTOR_OR_IDEMPOTENCY_GAP', note: 'quote_intent_binding_not_exported' },
  { group: 'idempotency_write_path', bucket: 'D', coverage_status: 'blocked', row_fields: [], repair_owner: 'QUOTE_INTENT_PROVIDER_EXECUTOR_OR_IDEMPOTENCY_GAP', note: 'idempotency_write_path_not_exported' },
  { group: 'worker_health', bucket: 'D', coverage_status: 'blocked', row_fields: [], repair_owner: 'OPS_WORKER_READMODEL_STORAGE_OR_CONFIG_GAP', note: 'worker_readmodel_capacity_not_exported' },
  { group: 'paper_live_boundary', bucket: 'D', coverage_status: 'blocked', row_fields: [], repair_owner: 'PAPER_LIVE_OPERATOR_WRITE_OR_SECURITY_GAP', note: 'paper_live_boundary_truth_not_exported' },
  // E. Denominator and evidence eligibility
  { group: 'denominator_projection', bucket: 'E', coverage_status: 'covered', row_fields: ['formal_sustained_dog', 'raw_peak_50_clean', 'raw_peak_100_clean', 'raw_peak_5x_clean', 'raw_peak_10x_clean'], repair_owner: 'ROW_IDENTITY_UNIT_OR_DENOMINATOR_INVALID' },
  { group: 'label_finalization', bucket: 'E', coverage_status: 'covered', row_fields: ['raw_tier', 'class', 'formal_eligible'], repair_owner: 'ROW_IDENTITY_UNIT_OR_DENOMINATOR_INVALID' },
  { group: 'ex_ante_feasibility', bucket: 'E', coverage_status: 'blocked', row_fields: [], repair_owner: 'EX_ANTE_ACTIONABILITY_OR_FILL_ANCHOR_GAP', note: 'ex_ante_feasibility_not_exported' },
  { group: 'earliest_actionable', bucket: 'E', coverage_status: 'blocked', row_fields: [], repair_owner: 'EX_ANTE_ACTIONABILITY_OR_FILL_ANCHOR_GAP', note: 'earliest_actionable_time_not_exported' },
  { group: 'feature_availability', bucket: 'E', coverage_status: 'blocked', row_fields: [], repair_owner: 'FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP', note: 'feature_vector_snapshots_not_exported' },
  { group: 'training_manifest', bucket: 'E', coverage_status: 'blocked', row_fields: [], repair_owner: 'FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP', note: 'training_manifest_not_exported' },
  { group: 'detector_calibration', bucket: 'E', coverage_status: 'blocked', row_fields: [], repair_owner: 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP', note: 'detector_markov_calibration_not_exported' },
  // F. Promotion / governance — not in current exports
  { group: 'mode_readiness', bucket: 'F', coverage_status: 'blocked', row_fields: [], repair_owner: 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP', note: 'mode_readiness_contract_not_exported' },
  { group: 'metric_threshold_catalog', bucket: 'F', coverage_status: 'blocked', row_fields: [], repair_owner: 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP', note: 'metric_threshold_taxonomy_not_exported' },
  { group: 'release_safety', bucket: 'F', coverage_status: 'blocked', row_fields: [], repair_owner: 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP', note: 'release_experiment_safety_not_exported' },
  { group: 'holdout_negative_controls', bucket: 'F', coverage_status: 'blocked', row_fields: [], repair_owner: 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP', note: 'holdout_negative_controls_not_exported' },
  { group: 'promotion_guardrails', bucket: 'F', coverage_status: 'blocked', row_fields: [], repair_owner: 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP', note: 'promotion_guardrails_not_exported' },
  { group: 'evidence_conflict_aging', bucket: 'F', coverage_status: 'blocked', row_fields: [], repair_owner: 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP', note: 'evidence_conflict_aging_not_exported' },
  { group: 'assumptions_false_negative_budget', bucket: 'F', coverage_status: 'blocked', row_fields: [], repair_owner: 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP', note: 'assumptions_fn_budget_not_exported' },
  // G. Explicit non-row exclusions (§15.15.17)
  { group: 'ui_rendering', bucket: 'G', coverage_status: 'intentionally_excluded', row_fields: [], repair_owner: 'INTENTIONALLY_EXCLUDED_NOT_CAPTURE_RELEVANT', note: 'dashboard_layout_ui' },
  { group: 'secrets_keys', bucket: 'G', coverage_status: 'intentionally_excluded', row_fields: [], repair_owner: 'INTENTIONALLY_EXCLUDED_NOT_CAPTURE_RELEVANT', note: 'tokens_private_keys' },
  { group: 'generated_clients', bucket: 'G', coverage_status: 'intentionally_excluded', row_fields: [], repair_owner: 'INTENTIONALLY_EXCLUDED_NOT_CAPTURE_RELEVANT', note: 'unless_schema_compat_fails' },
  { group: 'legal_detail', bucket: 'G', coverage_status: 'intentionally_excluded', row_fields: [], repair_owner: 'INTENTIONALLY_EXCLUDED_NOT_CAPTURE_RELEVANT', note: 'beyond_eligibility_status' },
  { group: 'docs_runbooks', bucket: 'G', coverage_status: 'intentionally_excluded', row_fields: [], repair_owner: 'INTENTIONALLY_EXCLUDED_NOT_CAPTURE_RELEVANT', note: 'except_runbook_freshness' },
  { group: 'deprecated_files', bucket: 'G', coverage_status: 'intentionally_excluded', row_fields: [], repair_owner: 'INTENTIONALLY_EXCLUDED_NOT_CAPTURE_RELEVANT', note: 'unless_runtime_references' },
  { group: 'notification_prompt_log_internals', bucket: 'G', coverage_status: 'intentionally_excluded', row_fields: [], repair_owner: 'INTENTIONALLY_EXCLUDED_NOT_CAPTURE_RELEVANT', note: 'unless_failure_blocks_artifact_or_decision' },
  { group: 'historical_strategy_claims', bucket: 'G', coverage_status: 'intentionally_excluded', row_fields: [], repair_owner: 'INTENTIONALLY_EXCLUDED_NOT_CAPTURE_RELEVANT', note: 'FBR/RED_bar/Stage2A/68%: artifact-only unless reprojected into row v2 and revalidated (§15.15.22)' },
];

const VALID_BUCKETS = new Set(['A', 'B', 'C', 'D', 'E', 'F', 'G']);

// Per-row funnel coverage: which chain links this signal SUBSTANTIVELY reached (not just "projected"
// — projection-completeness is guaranteed separately). Each link reflects real evidence for the row.
function rowModuleClosure(row) {
  return {
    source: row.source_seen === true,
    denominator: ['dog', 'dud'].includes(row.class) || row.formal_eligible != null,
    lifecycle: row.lifecycle_identity_seen === true,
    markov: row.markov_applicable === true,
    decision: row.has_decision === true,
    quote: row.quote_clean === true,
    would_enter: row.would_enter === true,
    entry: row.entry_intent_seen === true || row.entered === true,
    ledger: row.ledger_seen === true,
    exit_hold: row.exit_ts != null || row.held_to_silver_or_gold === true,
    friction: row.round_trip_friction_pct != null,
  };
}

// Augment a v1 row into a v2 row (additive; never mutates labels or the v1 chain).
function augmentRow(row) {
  const conf = rowConfidence(row);
  return {
    ...row,
    ops_evidence_present: false,          // §15.14 D not in current exports
    ops_evidence_missing_reason: 'operational_truth_modules_not_exported',
    promotion_evidence_present: false,    // §15.15 F not in current exports
    promotion_evidence_missing_reason: 'promotion_governance_modules_not_exported',
    module_closure_flags: rowModuleClosure(row),
    row_confidence: conf.row_confidence,
    ev_eligible: conf.ev_eligible,
    row_confidence_warnings: conf.row_confidence_warnings,
    final_repair_owner_v2: finalRepairOwner(row),
  };
}

// ---- reports ----------------------------------------------------------------------------------
function rowConfidenceReport(rows) {
  const byClass = (cls) => rows.filter((r) => r.class === cls);
  const dist = (subset) => countBy(subset, (r) => r.row_confidence);
  return {
    schema_version: 'live_fullnet_row_confidence_report.v1',
    note: 'row_confidence ladder (§15.14.14). HIGH needs ops+promotion evidence (not in current exports) => real rows top out at MEDIUM. ev_eligible is the only actual-EV gate.',
    row_count: rows.length,
    dog_count: byClass('dog').length,
    dud_count: byClass('dud').length,
    pending_count: byClass('pending').length,
    ev_eligible_count: rows.filter((r) => r.ev_eligible).length,
    invalid_for_ev_count: rows.filter((r) => r.row_confidence === 'INVALID_FOR_EV').length,
    invalid_for_promotion_count: rows.filter((r) => r.row_confidence === 'INVALID_FOR_PROMOTION').length,
    distribution_all: dist(rows),
    distribution_dog: dist(byClass('dog')),
    distribution_dud: dist(byClass('dud')),
    top_missing_reasons: topReasons(rows.flatMap((r) => (r.row_confidence_warnings || []).map((w) => ({ w }))), (x) => x.w).slice(0, 15),
  };
}

function repairOwnerReport(rows) {
  const dogs = rows.filter((r) => r.class === 'dog');
  const duds = rows.filter((r) => r.class === 'dud');
  const ladderIndex = (o) => { const i = REPAIR_LADDER.indexOf(o); return i < 0 ? 99 : i; };
  const owners = topReasons(rows, (r) => r.final_repair_owner_v2)
    .map((e) => ({
      ...e,
      ladder_priority: ladderIndex(e.key),
      dog_n: dogs.filter((r) => r.final_repair_owner_v2 === e.key).length,
      dud_n: duds.filter((r) => r.final_repair_owner_v2 === e.key).length,
    }))
    .sort((a, b) => a.ladder_priority - b.ladder_priority || b.n - a.n);
  return {
    schema_version: 'live_fullnet_repair_owner_report.v1',
    note: '§15.15.20 final repair-owner ladder (0 highest priority). Evidence/ops/identity gaps rank above strategy/entry/ledger. Deterministic per-row; first matching ladder level wins.',
    ladder: REPAIR_LADDER,
    row_count: rows.length,
    dog_repair_owners: topReasons(dogs, (r) => r.final_repair_owner_v2),
    owners,
  };
}

function moduleClosureCoverageReport(rows) {
  const byBucket = {};
  for (const m of MODULE_CLOSURE_MAP) (byBucket[m.bucket] ||= []).push(m.group);
  const statusCounts = countBy(MODULE_CLOSURE_MAP, (m) => m.coverage_status);
  const everyMapped = MODULE_CLOSURE_MAP.every((m) => VALID_BUCKETS.has(m.bucket));
  const gAllExcluded = MODULE_CLOSURE_MAP.filter((m) => m.bucket === 'G').every((m) => m.coverage_status === 'intentionally_excluded');
  return {
    schema_version: 'live_fullnet_module_closure_coverage_report.v1',
    note: 'Operationalizes §15.15.18 (A-G map) + §15.15.19 schema. coverage_status: covered=present in current exports; blocked=row-relevant but needs a new export; intentionally_excluded=bucket G (§15.15.17).',
    every_module_mapped_to_A_G: everyMapped,
    bucket_G_all_intentionally_excluded: gAllExcluded,
    buckets: Object.fromEntries(Object.entries(byBucket).map(([b, gs]) => [b, gs.sort()])),
    coverage_status_counts: statusCounts,
    modules: MODULE_CLOSURE_MAP.map((m) => ({
      module_group: m.group,
      bucket: m.bucket,
      source_files_seen: 'static_scan_§15.12-§15.15',
      row_level_fields_added: m.row_fields,
      artifact_level_fields_added: m.coverage_status === 'covered' ? m.row_fields : [],
      explicitly_excluded: m.bucket === 'G',
      reason_for_exclusion: m.bucket === 'G' ? (m.note || 'non_row_exclusion_§15.15.17') : (m.coverage_status === 'blocked' ? (m.note || 'not_in_current_exports') : null),
      repair_owner_added: m.repair_owner,
      tests_added_or_required: m.coverage_status === 'covered' ? 'covered_by_projection_complete_and_v2_tests' : 'requires_new_export_then_projection_test',
      coverage_status: m.coverage_status,
    })),
  };
}

// EV gate (§15.15.22 #7): actual net EV stays null unless ev_eligible rows exist with valid friction.
function evGate(rows) {
  const eligible = rows.filter((r) => r.ev_eligible);
  const dogEligible = eligible.filter((r) => r.formal_sustained_dog);
  const nets = eligible.map((r) => Number(r.net_pnl_pct)).filter((v) => Number.isFinite(v));
  return {
    ev_eligible_n: eligible.length,
    dog_ev_eligible_n: dogEligible.length,
    actual_net_ev_pct: nets.length ? average(nets) : null,
    actual_net_ev_missing_reason: nets.length ? null
      : (eligible.length ? 'ev_eligible_rows_missing_net_pnl' : 'no_ev_eligible_rows_no_valid_entered_fill_exit_ledger'),
    gate: nets.length ? 'ACTUAL_NET_EV_AVAILABLE' : 'BLOCKED_NO_VALID_ENTERED_FILL_EXIT_LEDGER',
  };
}

function buildV2(args) {
  const v1 = buildReport(args);
  const rows = v1.rows.map(augmentRow);
  const projection = v1.summary.projection_completeness || [];
  const projectionComplete = projection.length > 0 && projection.every((p) => p.projected_rate === 1);
  const closure = moduleClosureCoverageReport(rows);
  const summary = {
    schema_version: 'live_fullnet_row_report_v2.v1',
    generated_at: v1.generated_at,
    do_not_change_strategy: true,
    guardrails: {
      ...v1.guardrails,
      research_only: true,
      no_threshold_tuning: true,
      historical_fbr_redbar_stage2a_68pct_are_artifact_only: true,
      row_confidence_and_evidence_eligibility_gate_ev_and_promotion: true,
    },
    inputs: v1.inputs,
    total_signals: rows.length,
    by_class: v1.summary.by_class,
    projection_complete: projectionComplete,
    projection_completeness: projection,
    ev_gate: evGate(rows),
    phase5_verdict: v1.summary.phase5_verdict,
    row_confidence_report: rowConfidenceReport(rows),
    repair_owner_report: repairOwnerReport(rows),
    module_closure_coverage: closure,
    // carry the v1 capture/contribution context for continuity
    dog_capture: v1.summary.dog_capture,
    dog_capture_chain: v1.summary.dog_capture_chain,
    component_separability: v1.summary.component_separability,
  };
  return { summary, rows, component_separability: v1.summary.component_separability, v1 };
}

function writeV2(report, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const w = (name, obj) => { const p = path.join(outDir, name); fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`); return p; };
  const rowPath = path.join(outDir, 'row.jsonl');
  fs.writeFileSync(rowPath, `${report.rows.map((r) => JSON.stringify(r)).join('\n')}\n`);
  return {
    rowPath,
    summaryPath: w('summary.json', report.summary),
    separabilityPath: w('separability.json', report.component_separability),
    rowConfidencePath: w('row-confidence-report.json', report.summary.row_confidence_report),
    repairOwnerPath: w('repair-owner-report.json', report.summary.repair_owner_report),
    moduleClosurePath: w('final-module-closure-coverage-report.json', report.summary.module_closure_coverage),
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = { timeoutMs: 60_000 };
  const map = {
    '--source-to-raw': 'sourceToRaw', '--source-24h': 'source24h', '--raw-discovery': 'rawDiscovery',
    '--a-class-events': 'aClassEvents', '--ledger-export': 'ledgerExport', '--lifecycle-db': 'lifecycleDb',
    '--out-dir': 'outDir',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--help' || a === '-h') { args.help = true; continue; }
    if (map[a]) { args[map[a]] = argv[i + 1]; i += 1; continue; }
    throw new Error(`Unknown argument: ${a}`);
  }
  return args;
}

function runCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    console.log([
      'Usage:',
      '  node scripts/build-live-fullnet-row-v2.js \\',
      '    --source-to-raw source-to-raw-rows.json --source-24h source-rows.json \\',
      '    --raw-discovery raw-dog-discovery-24h.json --a-class-events a-class-events-24h-complete.json \\',
      '    --ledger-export canonical-ledger-window.json --lifecycle-db lifecycle_tracks.snapshot.db \\',
      '    --out-dir <dir>',
      '',
      'Note: --source-24h is REQUIRED for byte-identical reruns and for narrative_score/ai_narrative_tier;',
      '  omitting it nulls those source fields (the row still projects, just with those fields absent).',
      'Deps: this and the v1 generator need better-sqlite3 resolvable. sas-research has no node_modules,',
      '  so run tests/CLI with NODE_PATH=/Users/boliu/sentiment-arbitrage-system/node_modules.',
    ].join('\n'));
    return { help: true };
  }
  if (!args.outDir) throw new Error('--out-dir is required');
  const report = buildV2(args);
  const written = writeV2(report, path.resolve(args.outDir));
  console.log(JSON.stringify({
    ok: true,
    schema_version: report.summary.schema_version,
    total_signals: report.summary.total_signals,
    projection_complete: report.summary.projection_complete,
    ev_gate: report.summary.ev_gate.gate,
    actual_net_ev_pct: report.summary.ev_gate.actual_net_ev_pct,
    row_confidence: report.summary.row_confidence_report.distribution_all,
    every_module_mapped_to_A_G: report.summary.module_closure_coverage.every_module_mapped_to_A_G,
    top_repair_owner: report.summary.repair_owner_report.owners[0]?.key ?? null,
    paths: written,
  }, null, 2));
  return { report, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export {
  REPAIR_LADDER,
  MODULE_CLOSURE_MAP,
  VALID_BUCKETS,
  finalRepairOwner,
  rowConfidence,
  rowModuleClosure,
  augmentRow,
  rowConfidenceReport,
  repairOwnerReport,
  moduleClosureCoverageReport,
  evGate,
  buildV2,
};
