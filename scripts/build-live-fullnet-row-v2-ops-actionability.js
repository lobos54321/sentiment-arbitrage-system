#!/usr/bin/env node
// Fullnet row v2 + ops / worker-readmodel / quote-intent-binding / ex-ante actionability (§15.19, Goal 4).
// Research-only: turns the remaining ops/execution-truth/actionability blocked modules into row-level
// auditable evidence, derived ONLY from same-window (06-21) artifacts already on disk. Anything that
// needs a same-window runtime readmodel / Zeabur export stays BLOCKED with a precise reason (the 06-19
// v27 mirror logs exist but are a DIFFERENT window -> not joined, per the no-cross-window rule).
// Imports buildModeReadiness (Goal 3) unchanged; pure ADDITIVE layer. No live/gate/entry/exit/size
// change, no threshold tuning, no mode opened/promoted, EV stays fail-closed, Goal 1/2/3 fields verbatim.
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';
import { buildModeReadiness } from './build-live-fullnet-row-v2-mode-readiness.js';

const OPS_OWNER = 'OPS_WORKER_READMODEL_STORAGE_OR_CONFIG_GAP';
const QUOTE_OWNER = 'QUOTE_INTENT_PROVIDER_EXECUTOR_OR_IDEMPOTENCY_GAP';
const EXANTE_OWNER = 'EX_ANTE_ACTIONABILITY_OR_FILL_ANCHOR_GAP';
const PAPERLIVE_OWNER = 'PAPER_LIVE_OPERATOR_WRITE_OR_SECURITY_GAP';
const NUMERIC_AUDIT_FIELDS = ['liquidity_usd', 'spread_pct', 'quote_age_sec', 'max_wick_peak_pct', 'max_sustained_peak_pct', 'matrix_score', 'kline_bars_n', 'kline_coverage_pct'];
const NON_NEGATIVE_FIELDS = new Set(['liquidity_usd', 'quote_age_sec', 'max_wick_peak_pct', 'max_sustained_peak_pct', 'kline_bars_n', 'kline_coverage_pct']);

function num(v) { if (v == null || v === '') return null; const n = Number(v); return Number.isFinite(n) ? n : null; }
function rnd(v) { const n = num(v); return n == null ? null : Math.round(n); }
function signalKey(t, s) { return `${t}|${rnd(s) ?? 0}`; }
function countBy(rows, fn) { const o = {}; for (const r of rows) { const k = fn(r) ?? 'unknown'; o[k] = (o[k] || 0) + 1; } return o; }
function topReasons(rows, fn) { return Object.entries(countBy(rows, fn)).map(([key, n]) => ({ key, n })).sort((a, b) => b.n - a.n || a.key.localeCompare(b.key)); }
function rate(n, d) { return d ? Math.round((n / d) * 1e6) / 1e6 : null; }

// Pure: ex-ante feasibility + earliest actionable + fill anchor (same-window, from decision timing).
function exAnteActionability(row) {
  const hasDecision = row.has_decision === true;
  const feasible = hasDecision && row.executable_quote_clean === true;
  const exAnteBlocked = !hasDecision ? 'no_decision_event'
    : row.quote_clean !== true ? 'quote_not_clean'
      : row.executable_quote_clean !== true ? 'quote_not_executable'
        : row.would_enter !== true ? 'decision_did_not_would_enter'
          : null;
  const earliestSec = num(row.first_route_decision_sec_after_signal);
  const sigTs = num(row.signal_ts);
  const anchorLag = num(row.kline_first_bar_lag_sec);
  return {
    ex_ante_feasibility_seen: hasDecision,
    ex_ante_feasible: hasDecision ? feasible : null,
    ex_ante_blocked_reason: exAnteBlocked,
    ex_ante_feasibility_missing_reason: hasDecision ? null : 'no_decision_event_no_ex_ante_assessment',
    earliest_actionable_seen: earliestSec != null,
    earliest_actionable_ts: (earliestSec != null && sigTs != null) ? Math.round(sigTs + earliestSec) : null,
    earliest_actionable_sec_after_signal: earliestSec,
    earliest_actionable_missing_reason: earliestSec != null ? null : 'no_decision_event_no_earliest_actionable',
    fill_anchor_seen: anchorLag != null,
    fill_anchor_ts: (anchorLag != null && sigTs != null) ? Math.round(sigTs + anchorLag) : null,
    fill_anchor_price: null, // first-bar price not exported in raw_signal_outcomes snapshot
    fill_anchor_source: anchorLag != null ? (row.kline_source ?? 'raw_signal_outcomes_first_bar') : null,
    fill_anchor_missing_reason: anchorLag != null ? null : 'no_first_bar_anchor_for_signal',
  };
}

// Pure: paper/live boundary (observable — in-window there is no live execution).
function paperLiveBoundary(row) {
  const entered = row.entered === true;
  const ledgerSource = row.ledger_source ?? null;
  const isCanonical = ledgerSource === 'canonical_trade_ledger';
  const scope = entered ? (isCanonical ? 'canonical_ledger_entry' : 'paper_ledger_entry')
    : row.shadow_only === true ? 'shadow_only_no_execution'
      : row.would_enter === true ? 'would_enter_no_execution'
        : 'no_execution';
  return {
    paper_live_boundary_seen: true, // boundary is observable from ledger/exec evidence
    paper_live_execution_scope: scope,
    paper_live_paper_only: !entered || ledgerSource === 'paper_trades',
    paper_live_violation: entered && isCanonical, // a canonical/live entry would be a boundary crossing
    paper_live_boundary_missing_reason: null,
  };
}

// Pure: numeric-precision audit on the row's own observable numeric fields.
function nullPrecision(row) {
  const invalid = [];
  for (const f of NUMERIC_AUDIT_FIELDS) {
    const v = row[f];
    if (v == null) continue;
    const n = Number(v);
    if (!Number.isFinite(n)) { invalid.push(`${f}:non_finite`); continue; }
    if (NON_NEGATIVE_FIELDS.has(f) && n < 0) invalid.push(`${f}:negative`);
  }
  return {
    null_precision_seen: true, // audited the observable numeric fields
    null_precision_invalid_numeric_fields: invalid,
    null_precision_missing_reason: null,
  };
}

// Pure: identity / unit / finality from the source-to-raw row (price unit + same-source path).
function identityUnitFinality(row, srcRow) {
  const unit = srcRow ? (srcRow.baseline_price_unit ?? null) : null;
  const pathUnit = srcRow ? (srcRow.path_price_unit ?? null) : null;
  const sameSource = srcRow ? Boolean(srcRow.same_source_path) : null;
  const finalityReason = !srcRow ? 'source_to_raw_row_absent'
    : sameSource ? 'same_source_baseline_and_path'
      : (unit && pathUnit && unit !== pathUnit) ? 'mixed_baseline_path_unit'
        : 'baseline_and_path_source_differ_or_unknown';
  return {
    identity_unit_finality_seen: Boolean(row.token_ca) && unit != null,
    identity_unit: unit,
    identity_path_unit: pathUnit,
    identity_finality_reason: finalityReason,
    identity_unit_finality_missing_reason: unit != null ? null : (srcRow ? 'price_unit_null_in_source_to_raw' : 'source_to_raw_row_absent'),
  };
}

// Pure: quote-intent binding — intent side is row-observable; intent->fill binding needs execution-control readmodel.
function quoteIntentBinding(row) {
  const intentSeen = row.entry_intent_seen === true;
  const provider = row.quote_provider_source ?? row.quote_source ?? null;
  return {
    quote_intent_binding_seen: intentSeen,
    quote_intent_id: intentSeen ? (row.entry_intent_event_id ?? null) : null,
    quote_intent_provider: intentSeen ? provider : null,
    quote_intent_route: intentSeen ? (row.route_source ?? null) : null,
    quote_intent_fill_anchor_ts: null, // set below from ex-ante fill anchor
    quote_intent_fill_bound: row.fill_seen === true,
    quote_intent_binding_missing_reason: intentSeen
      ? (row.fill_seen === true ? null : 'intent_recorded_no_fill_binding_no_entries_in_window')
      : (row.has_decision === true ? 'no_entry_intent_recorded' : 'no_decision_event'),
  };
}

// Pure: ops modules that are NOT observable from same-window artifacts -> blocked with precise reasons.
function opsBlockedEvidence() {
  return {
    worker_health_seen: false,
    worker_health_source: 'v27_read_model_refresh_mirror_exists_but_06-19_only',
    worker_health_status: null,
    worker_health_missing_reason: 'worker_readmodel_not_same_window_06-21__requires_runtime_readmodel_export',
    parser_session_seen: false,
    parser_session_source: null,
    parser_session_missing_reason: 'parser_session_not_exported',
    readmodel_snapshot_seen: false,
    readmodel_snapshot_source: 'v27_mirror_logs_06-19_cross_window',
    readmodel_snapshot_freshness_sec: null,
    readmodel_snapshot_missing_reason: 'v27_readmodel_mirror_only_06-19_not_same_window__requires_runtime_readmodel_export',
    idempotency_key_seen: false,
    idempotency_key: null,
    idempotency_write_path_status: null,
    idempotency_write_path_missing_reason: 'idempotency_write_path_not_exported',
  };
}

function opsActionabilityConfidence(m) {
  const warnings = [];
  if (m.ex_ante_feasibility_seen && m.ex_ante_feasible === false) warnings.push(m.ex_ante_blocked_reason || 'ex_ante_not_feasible');
  if (!m.earliest_actionable_seen) warnings.push('no_earliest_actionable');
  if (!m.fill_anchor_seen) warnings.push('no_fill_anchor');
  if (!m.identity_unit_finality_seen) warnings.push(m.identity_unit_finality_missing_reason || 'identity_unit_unknown');
  if (m.null_precision_invalid_numeric_fields.length) warnings.push(`invalid_numeric:${m.null_precision_invalid_numeric_fields.join('|')}`);
  if (!m.worker_health_seen) warnings.push('worker_readmodel_not_same_window');
  if (!m.parser_session_seen) warnings.push('parser_session_not_exported');
  if (m.quote_intent_binding_seen && !m.quote_intent_fill_bound) warnings.push('intent_no_fill_binding');
  // observable-evidence confidence: HIGH only if ops readmodel present too (never same-window) -> caps at MEDIUM
  const observableOk = m.ex_ante_feasibility_seen && m.earliest_actionable_seen && m.identity_unit_finality_seen
    && m.paper_live_boundary_seen && m.null_precision_invalid_numeric_fields.length === 0;
  const confidence = (observableOk && m.worker_health_seen && m.parser_session_seen) ? 'HIGH'
    : observableOk ? 'MEDIUM' : 'LOW';
  return { ops_actionability_confidence: confidence, ops_actionability_warnings: warnings };
}

function opsActionabilityRepairDetail(row, m) {
  const owner = row.final_repair_owner_v2;
  if (owner === QUOTE_OWNER) return row.quote_provider_missing_reason || m.quote_intent_binding_missing_reason || 'quote_intent_or_provider_gap';
  if (owner === EXANTE_OWNER) return m.ex_ante_blocked_reason || 'ex_ante_or_fill_anchor_gap';
  if (owner === OPS_OWNER) return m.worker_health_missing_reason;
  if (owner === PAPERLIVE_OWNER) return m.paper_live_boundary_missing_reason || 'paper_live_boundary_gap';
  return null;
}

// Pure: augment one v2-mode-readiness row with ops/actionability evidence (additive; preserves all prior fields).
function augmentOpsRow(row, srcRow) {
  const ex = exAnteActionability(row);
  const pl = paperLiveBoundary(row);
  const np = nullPrecision(row);
  const id = identityUnitFinality(row, srcRow);
  const qi = quoteIntentBinding(row);
  qi.quote_intent_fill_anchor_ts = ex.fill_anchor_ts;
  const ops = opsBlockedEvidence();
  const merged = { ...row, ...ex, ...pl, ...np, ...id, ...qi, ...ops };
  const conf = opsActionabilityConfidence(merged);
  merged.ops_actionability_confidence = conf.ops_actionability_confidence;
  merged.ops_actionability_warnings = conf.ops_actionability_warnings;
  merged.ops_actionability_repair_detail = opsActionabilityRepairDetail(merged, merged);
  return merged;
}

// ---- closure: layer ops overrides on the mode-readiness closure (proof-gated) -----------------
const OPS_COVERED_OVERRIDES = {
  ex_ante_feasibility: { proof: (rows) => rows.some((r) => r.ex_ante_feasibility_seen), note: 'ex-ante feasibility derived from same-window decision + executable_quote_clean' },
  earliest_actionable: { proof: (rows) => rows.some((r) => r.earliest_actionable_seen), note: 'earliest actionable from first_route_decision_sec_after_signal (same-window)' },
  paper_live_boundary: { proof: (rows) => rows.every((r) => r.paper_live_boundary_seen), note: 'paper/live boundary observable: 0 entries/ledger in window => paper/shadow scope, no live crossing' },
  null_precision: { proof: (rows) => rows.every((r) => r.null_precision_seen), note: 'numeric-precision audited on observable row fields' },
  identity_unit_finality: { proof: (rows) => rows.some((r) => r.identity_unit_finality_seen), note: 'price unit + same-source-path from source-to-raw' },
  quote_intent_binding: { proof: (rows) => rows.some((r) => r.quote_intent_binding_seen), note: 'intent side row-observable (entry_intent + provider/route); intent->fill binding absent (no entries)' },
};
const OPS_REFINED_BLOCKED = {
  worker_health: 'worker_readmodel_not_same_window_06-21__requires_runtime_readmodel_export',
  parser_session: 'parser_session_not_exported__requires_runtime_readmodel_export',
  idempotency_write_path: 'idempotency_write_path_not_exported__requires_runtime_readmodel_export',
};

function layerOpsClosure(modeClosure, rows) {
  const reclassified = [];
  const modules = modeClosure.modules.map((m) => ({ ...m })).map((m) => {
    const status = m.coverage_status_final || m.coverage_status;
    const ov = OPS_COVERED_OVERRIDES[m.module_group];
    if (ov && status === 'blocked' && ov.proof(rows)) {
      reclassified.push({ module_group: m.module_group, from: 'blocked', to: 'covered', evidence: ov.note });
      return { ...m, coverage_status: 'covered', coverage_status_final: 'covered', reason_for_exclusion: null, note: ov.note };
    }
    if (status === 'blocked' && OPS_REFINED_BLOCKED[m.module_group]) return { ...m, reason_for_exclusion: OPS_REFINED_BLOCKED[m.module_group] };
    return m;
  });
  const statusCounts = countBy(modules, (m) => m.coverage_status_final || m.coverage_status);
  const VALID = new Set(['A', 'B', 'C', 'D', 'E', 'F', 'G']);
  return {
    schema_version: 'live_fullnet_module_closure_coverage_report.v4_ops_actionability',
    note: 'Goal 4 (§15.19): ops/actionability reclassified blocked->covered ONLY from same-window derivation (proof-gated). 06-19 v27 readmodel mirrors are cross-window and not joined. Layered on Goal-3 closure (no regression).',
    every_module_mapped_to_A_G: modules.every((m) => VALID.has(m.bucket)),
    bucket_G_all_intentionally_excluded: modules.filter((m) => m.bucket === 'G').every((m) => (m.coverage_status_final || m.coverage_status) === 'intentionally_excluded'),
    module_count_total: modules.length,
    coverage_status_counts: statusCounts,
    reclassified_from_blocked_this_layer: reclassified,
    modules,
  };
}

// ---- reports ----------------------------------------------------------------------------------
function opsWorkerReadmodelReport(rows) {
  return {
    schema_version: 'live_fullnet_ops_worker_readmodel_report.v1',
    note: 'Worker/parser/readmodel/idempotency are NOT observable from same-window artifacts (06-19 v27 mirror is cross-window). All blocked with precise reasons; row-level null_precision IS audited.',
    row_count: rows.length,
    worker_health_seen_n: rows.filter((r) => r.worker_health_seen).length,
    parser_session_seen_n: rows.filter((r) => r.parser_session_seen).length,
    readmodel_snapshot_seen_n: rows.filter((r) => r.readmodel_snapshot_seen).length,
    idempotency_key_seen_n: rows.filter((r) => r.idempotency_key_seen).length,
    null_precision_audited_n: rows.filter((r) => r.null_precision_seen).length,
    null_precision_invalid_rows_n: rows.filter((r) => (r.null_precision_invalid_numeric_fields || []).length).length,
    blocked_reasons: {
      worker_health: 'worker_readmodel_not_same_window_06-21__requires_runtime_readmodel_export',
      parser_session: 'parser_session_not_exported__requires_runtime_readmodel_export',
      idempotency_write_path: 'idempotency_write_path_not_exported__requires_runtime_readmodel_export',
    },
  };
}

function quoteIntentBindingReport(rows) {
  const dogs = rows.filter((r) => r.class === 'dog');
  return {
    schema_version: 'live_fullnet_quote_intent_binding_report.v1',
    note: 'Intent side is row-observable (entry_intent + provider/route). intent->fill binding is absent because there are no entries/fills in window (fail-closed).',
    row_count: rows.length,
    quote_intent_binding_seen_n: rows.filter((r) => r.quote_intent_binding_seen).length,
    quote_intent_binding_dog_n: dogs.filter((r) => r.quote_intent_binding_seen).length,
    intent_provider_distribution: countBy(rows.filter((r) => r.quote_intent_binding_seen), (r) => r.quote_intent_provider ?? '(none)'),
    fill_bound_n: rows.filter((r) => r.quote_intent_fill_bound).length,
    missing_reason_top: topReasons(rows, (r) => r.quote_intent_binding_missing_reason ?? '(seen)'),
    quote_owner_n: rows.filter((r) => r.final_repair_owner_v2 === QUOTE_OWNER).length,
    quote_owner_detail: topReasons(rows.filter((r) => r.final_repair_owner_v2 === QUOTE_OWNER), (r) => r.ops_actionability_repair_detail ?? 'unknown'),
  };
}

function exAnteActionabilityReport(rows) {
  const dogs = rows.filter((r) => r.class === 'dog');
  return {
    schema_version: 'live_fullnet_ex_ante_actionability_report.v1',
    note: 'Ex-ante feasibility + earliest actionable + fill anchor from same-window decision/kline timing. Counterfactual feasibility only; not an edge claim.',
    row_count: rows.length,
    ex_ante_feasible_n: rows.filter((r) => r.ex_ante_feasible === true).length,
    ex_ante_feasible_dog_n: dogs.filter((r) => r.ex_ante_feasible === true).length,
    ex_ante_blocked_reason_top: topReasons(rows.filter((r) => r.ex_ante_feasible !== true), (r) => r.ex_ante_blocked_reason ?? '(feasible)'),
    earliest_actionable_seen_n: rows.filter((r) => r.earliest_actionable_seen).length,
    earliest_actionable_sec_avg: (() => { const v = rows.map((r) => r.earliest_actionable_sec_after_signal).filter((x) => x != null); return v.length ? Math.round((v.reduce((a, b) => a + b, 0) / v.length) * 10) / 10 : null; })(),
    fill_anchor_seen_n: rows.filter((r) => r.fill_anchor_seen).length,
    ex_ante_owner_n: rows.filter((r) => r.final_repair_owner_v2 === EXANTE_OWNER).length,
  };
}

function paperLiveBoundaryReport(rows) {
  return {
    schema_version: 'live_fullnet_paper_live_boundary_report.v1',
    note: 'Paper/live boundary observable from ledger/exec evidence. In-window there are 0 entries -> no live crossing.',
    row_count: rows.length,
    execution_scope_distribution: countBy(rows, (r) => r.paper_live_execution_scope),
    paper_only_n: rows.filter((r) => r.paper_live_paper_only).length,
    live_violation_n: rows.filter((r) => r.paper_live_violation).length,
    paper_live_owner_n: rows.filter((r) => r.final_repair_owner_v2 === PAPERLIVE_OWNER).length,
  };
}

function repairOwnerReportOps(rows) {
  const dogs = rows.filter((r) => r.class === 'dog');
  const owners = topReasons(rows, (r) => r.final_repair_owner_v2).map((e) => ({ ...e, dog_n: dogs.filter((r) => r.final_repair_owner_v2 === e.key).length }));
  return {
    schema_version: 'live_fullnet_repair_owner_report.v4_ops_actionability',
    note: '§15.15.20 ladder unchanged (same predicates => same distribution as v2/v3). QUOTE_INTENT owner itemized; OPS/EX-ANTE/PAPER-LIVE owners have 0 proximate rows (they are closure-level blocked modules, not first-match owners) — reported explicitly.',
    row_count: rows.length,
    owners,
    quote_intent_owner_n: rows.filter((r) => r.final_repair_owner_v2 === QUOTE_OWNER).length,
    quote_intent_owner_detail: topReasons(rows.filter((r) => r.final_repair_owner_v2 === QUOTE_OWNER), (r) => r.ops_actionability_repair_detail ?? 'unknown'),
    ops_owner_n: rows.filter((r) => r.final_repair_owner_v2 === OPS_OWNER).length,
    ex_ante_owner_n: rows.filter((r) => r.final_repair_owner_v2 === EXANTE_OWNER).length,
    paper_live_owner_n: rows.filter((r) => r.final_repair_owner_v2 === PAPERLIVE_OWNER).length,
  };
}

function rowConfidenceReportOps(rows) {
  const byClass = (c) => rows.filter((r) => r.class === c);
  return {
    schema_version: 'live_fullnet_row_confidence_report.v4_ops_actionability',
    row_count: rows.length, dog_count: byClass('dog').length, dud_count: byClass('dud').length, pending_count: byClass('pending').length,
    ev_eligible_count: rows.filter((r) => r.ev_eligible).length,
    distribution_all: countBy(rows, (r) => r.row_confidence),
    ops_actionability_confidence_distribution_all: countBy(rows, (r) => r.ops_actionability_confidence),
  };
}

function loadSourceMap(p) {
  const j = JSON.parse(fs.readFileSync(path.resolve(p), 'utf8'));
  const rows = Array.isArray(j) ? j : (j.rows || []);
  const map = new Map();
  for (const r of rows) { const k = signalKey(r.token_ca, r.signal_ts); if (!map.has(k)) map.set(k, r); }
  return map;
}

function buildOpsActionability(args) {
  const mr = buildModeReadiness(args);
  if (!args.sourceToRaw) throw new Error('--source-to-raw is required (for identity unit)');
  const srcMap = loadSourceMap(args.sourceToRaw);
  const rows = mr.rows.map((r) => augmentOpsRow(r, srcMap.get(signalKey(r.token_ca, r.signal_ts)) || null));
  const closure = layerOpsClosure(mr.summary.module_closure_coverage, rows);
  const summary = {
    schema_version: 'live_fullnet_row_report_v2_ops_actionability.v1',
    generated_at: mr.summary.generated_at,
    do_not_change_strategy: true,
    guardrails: { ...mr.summary.guardrails, ops_actionability_research_only: true, no_cross_window_readmodel_join: true },
    inputs: mr.summary.inputs,
    total_signals: rows.length,
    by_class: mr.summary.by_class,
    projection_complete: mr.summary.projection_complete,
    projection_completeness: mr.summary.projection_completeness,
    ev_gate: mr.summary.ev_gate,
    ops_worker_readmodel_report: opsWorkerReadmodelReport(rows),
    quote_intent_binding_report: quoteIntentBindingReport(rows),
    ex_ante_actionability_report: exAnteActionabilityReport(rows),
    paper_live_boundary_report: paperLiveBoundaryReport(rows),
    repair_owner_report: repairOwnerReportOps(rows),
    row_confidence_report: rowConfidenceReportOps(rows),
    module_closure_coverage: closure,
    phase5_verdict: mr.summary.phase5_verdict,
  };
  return { summary, rows };
}

function writeOps(report, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const w = (name, obj) => { const p = path.join(outDir, name); fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`); return p; };
  const rowPath = path.join(outDir, 'row.jsonl');
  fs.writeFileSync(rowPath, `${report.rows.map((r) => JSON.stringify(r)).join('\n')}\n`);
  return {
    rowPath,
    summaryPath: w('summary.json', report.summary),
    repairOwnerPath: w('repair-owner-report.json', report.summary.repair_owner_report),
    rowConfidencePath: w('row-confidence-report.json', report.summary.row_confidence_report),
    moduleClosurePath: w('final-module-closure-coverage-report.json', report.summary.module_closure_coverage),
    opsWorkerPath: w('ops-worker-readmodel-report.json', report.summary.ops_worker_readmodel_report),
    quoteIntentPath: w('quote-intent-binding-report.json', report.summary.quote_intent_binding_report),
    exAntePath: w('ex-ante-actionability-report.json', report.summary.ex_ante_actionability_report),
    paperLivePath: w('paper-live-boundary-report.json', report.summary.paper_live_boundary_report),
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = { timeoutMs: 60_000 };
  const map = {
    '--source-to-raw': 'sourceToRaw', '--source-24h': 'source24h', '--raw-discovery': 'rawDiscovery',
    '--a-class-events': 'aClassEvents', '--ledger-export': 'ledgerExport', '--lifecycle-db': 'lifecycleDb',
    '--raw-signal-outcomes-db': 'rawSignalOutcomesDb', '--mode-registry': 'modeRegistry',
    '--entry-point-inventory': 'entryPointInventory', '--out-dir': 'outDir',
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
  if (args.help) { console.log('Usage: node scripts/build-live-fullnet-row-v2-ops-actionability.js <all v2-mode-readiness args> --out-dir <dir>  (NODE_PATH=.../sentiment-arbitrage-system/node_modules)'); return { help: true }; }
  if (!args.outDir) throw new Error('--out-dir is required');
  const report = buildOpsActionability(args);
  const written = writeOps(report, path.resolve(args.outDir));
  console.log(JSON.stringify({
    ok: true,
    schema_version: report.summary.schema_version,
    total_signals: report.summary.total_signals,
    projection_complete: report.summary.projection_complete,
    ev_gate: report.summary.ev_gate.gate,
    reclassified_this_layer: report.summary.module_closure_coverage.reclassified_from_blocked_this_layer.map((x) => x.module_group),
    coverage_status_counts: report.summary.module_closure_coverage.coverage_status_counts,
    ex_ante_feasible_n: report.summary.ex_ante_actionability_report.ex_ante_feasible_n,
    quote_intent_binding_seen_n: report.summary.quote_intent_binding_report.quote_intent_binding_seen_n,
    paths: written,
  }, null, 2));
  return { report, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export {
  exAnteActionability,
  paperLiveBoundary,
  nullPrecision,
  identityUnitFinality,
  quoteIntentBinding,
  opsBlockedEvidence,
  opsActionabilityConfidence,
  augmentOpsRow,
  layerOpsClosure,
  buildOpsActionability,
};
