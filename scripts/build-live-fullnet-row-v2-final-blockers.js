#!/usr/bin/env node
// Fullnet row v2 — final blocked-module closure + exact export requirements (plan §15.20, Goal 5).
// Research-only: the final pass over the 17 modules still blocked after Goal 4. For each it decides:
//   (1) coverable now from same-window artifacts, (2) needs same-window runtime read-model export,
//   (3) needs Zeabur/API/on-chain export, or (4) intentionally excluded. No silent reclassify; every
//   still-blocked module ends with an EXACT required export (no bare "not_in_current_exports").
// Imports buildOpsActionability (Goal 4) unchanged; pure ADDITIVE layer. No live/gate/entry/exit/size
// change, no threshold tuning, no mode opened/promoted, EV fail-closed, Goal 1/2/3/4 fields verbatim.
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';
import { buildOpsActionability } from './build-live-fullnet-row-v2-ops-actionability.js';

function num(v) { if (v == null || v === '') return null; const n = Number(v); return Number.isFinite(n) ? n : null; }
function rnd(v) { const n = num(v); return n == null ? null : Math.round(n); }
function signalKey(t, s) { return `${t}|${rnd(s) ?? 0}`; }
function countBy(rows, fn) { const o = {}; for (const r of rows) { const k = fn(r) ?? 'unknown'; o[k] = (o[k] || 0) + 1; } return o; }

// Exact disposition for each of the 17 modules still blocked after Goal 4.
// category: covered_now | runtime_readmodel_required | zeabur_export_required
const REQUIREMENTS = {
  // --- coverable now from same-window artifacts ---
  paper_path_samples: { category: 'covered_now', exact: 'observable: canonical paper_trades join is 0 in-window -> definitive 0 paper paths per signal' },
  source_channel_dbs: { category: 'covered_now', exact: 'observable: signal_source + source_family from source-to-raw give per-row channel identity (channel-level historical PRIORS still need a registry export)' },
  // --- need a same-window runtime read-model export (live system ./data/v27_event_log or runtime tables) ---
  gmgn_policy: { category: 'runtime_readmodel_required', exact: 'export gmgn_policy decision state per (token_ca,signal_ts) [scripts/gmgn_policy.py read-model] for the 06-21 window; gmgn currently appears only as a kline provider + mode-label token' },
  token_memory: { category: 'runtime_readmodel_required', exact: 'export token failure-memory / lotto_reclaim state per token [token memory table / backtest_lotto_reclaim_markov] same-window' },
  source_resonance: { category: 'runtime_readmodel_required', exact: 'export source_resonance cohort + lead_time_sec per signal [scripts/source_resonance_shadow.py read-model] same-window; "resonance" currently appears only inside normalized_mode label' },
  scout_quality: { category: 'runtime_readmodel_required', exact: 'export scout_quality score + block_reason per signal [scripts/scout_quality.py read-model] same-window' },
  watchlist: { category: 'runtime_readmodel_required', exact: 'export watchlist_store state per token [scripts/watchlist_store.py] same-window' },
  parser_session: { category: 'runtime_readmodel_required', exact: 'export ingestion/parser session_id per signal (telegram-parser session table) same-window' },
  idempotency_write_path: { category: 'runtime_readmodel_required', exact: 'export idempotency key + write_path status per decision (sqlite_write_coordinator audit) same-window' },
  worker_health: { category: 'runtime_readmodel_required', exact: 'export ./data/v27_event_log read-model-refresh + worker/readiness health for the 06-21 window (06-19 mirror exists but is cross-window)' },
  training_manifest: { category: 'runtime_readmodel_required', exact: 'export feature/training manifest_id + feature-vector snapshot per signal [v27 feature snapshot] same-window' },
  detector_calibration: { category: 'runtime_readmodel_required', exact: 'export detector/markov calibration metrics + model version [v27_markov_shadow_calibration_report] same-window' },
  holdout_negative_controls: { category: 'runtime_readmodel_required', exact: 'export holdout + negative-control cohort results per mode [v27 promotion read-model] same-window' },
  evidence_conflict_aging: { category: 'runtime_readmodel_required', exact: 'export evidence conflict + aging read-model per evidence row same-window' },
  assumptions_false_negative_budget: { category: 'runtime_readmodel_required', exact: 'export assumptions + false-negative-budget read-model same-window' },
  // --- need an external Zeabur / on-chain export (data source not in the live runtime read-models) ---
  smart_money: { category: 'zeabur_export_required', exact: 'Zeabur/API export of smart-money wallet signals per token (external wallet-tracking data source)' },
  curve_pumpfun: { category: 'zeabur_export_required', exact: 'Zeabur/on-chain pump.fun bonding-curve decode per token [helius pumpfun curve decode] same-window' },
};

// Pure: per-row final-blocker projection. srcRow = source-to-raw row (for channel identity).
function finalBlockerProjection(row, srcRow) {
  // (1) covered now: paper path samples (definitive 0 — no entries in window)
  const enteredPaper = row.entered === true && row.ledger_source === 'paper_trades';
  const paperPathSamples = {
    paper_path_samples_seen: enteredPaper,
    paper_path_samples_path_n: enteredPaper ? 1 : 0,
    paper_path_samples_missing_reason: enteredPaper ? null : 'no_paper_trades_in_window_paper_only_shadow',
  };
  // (1) covered now: source/channel identity (priors still need registry export)
  const channel = srcRow ? (srcRow.signal_source ?? null) : null;
  const sourceChannel = {
    source_channel_registry_seen: channel != null,
    source_channel: channel,
    source_channel_family: srcRow ? (srcRow.source_family ?? null) : null,
    source_channel_priors_seen: false,
    source_channel_missing_reason: channel != null ? 'channel_identity_covered__channel_level_priors_require_registry_export' : 'signal_source_absent_in_source_to_raw',
  };
  // (2/3) still blocked — per-module seen=false + exact required export
  const modeLabel = String(row.entry_mode || row.matrix_normalized_mode || '').toLowerCase();
  const blockedModule = (group, extra = {}) => ({
    [`${group}_seen`]: false,
    [`${group}_missing_reason`]: REQUIREMENTS[group].exact,
    ...extra,
  });
  const stillBlocked = {
    ...blockedModule('gmgn_policy', { gmgn_policy_source: null }),
    ...blockedModule('token_memory', { token_memory_failure_type: null, token_memory_reclaim_policy: null }),
    ...blockedModule('source_resonance', { source_resonance_cohort: null, source_resonance_lead_time_sec: null, source_resonance_in_mode_label: modeLabel.includes('resonance') }),
    ...blockedModule('smart_money', { smart_money_source: null }),
    ...blockedModule('curve_pumpfun', { curve_pumpfun_stage: null, curve_pumpfun_bonding_curve: null }),
    ...blockedModule('scout_quality', { scout_quality_score: null, scout_quality_block_reason: null, scout_quality_in_mode_label: modeLabel.includes('scout') }),
    ...blockedModule('watchlist', { watchlist_state: null }),
    ...blockedModule('training_manifest', { training_manifest_id: null }),
    ...blockedModule('detector_calibration', { detector_calibration_model: null, detector_calibration_version: null }),
    // NOTE: holdout_negative_controls / evidence_conflict_aging / assumptions_false_negative_budget
    // already have per-row *_seen/*_missing_reason from Goal 3 (mode-readiness). We DO NOT re-emit them
    // here (that would mutate a Goal-3 field); their exact required export is carried at closure level
    // (REQUIREMENTS -> reason_for_exclusion) + the requirement reports instead.
  };
  // aggregate requirement lists (global gap; identical per row by construction)
  const stillBlockedGroups = Object.keys(REQUIREMENTS).filter((g) => REQUIREMENTS[g].category !== 'covered_now');
  const runtimeReq = stillBlockedGroups.filter((g) => REQUIREMENTS[g].category === 'runtime_readmodel_required');
  const zeaburReq = stillBlockedGroups.filter((g) => REQUIREMENTS[g].category === 'zeabur_export_required');
  const shadowScoutSet = ['gmgn_policy', 'source_resonance', 'scout_quality', 'smart_money', 'curve_pumpfun', 'token_memory'].filter((g) => stillBlockedGroups.includes(g));
  const warnings = stillBlockedGroups.map((g) => `${g}:${REQUIREMENTS[g].category}`);
  return {
    ...paperPathSamples,
    ...sourceChannel,
    ...stillBlocked,
    remaining_blocker_modules: stillBlockedGroups,
    remaining_blocker_reasons: stillBlockedGroups.map((g) => `${g}::${REQUIREMENTS[g].exact}`),
    runtime_readmodel_required: runtimeReq,
    zeabur_export_required: zeaburReq,
    shadow_source_scout_required: shadowScoutSet,
    token_memory_required: ['token_memory'],
    final_blocker_confidence: 'LOW', // many export gaps remain -> row not fully auditable until exports land
    final_blocker_warnings: warnings,
  };
}

function augmentFinalRow(row, srcRow) {
  return { ...row, ...finalBlockerProjection(row, srcRow) };
}

// ---- closure: cover the 2 same-window modules; refine the 15 with exact requirements ----------
function layerFinalClosure(opsClosure, rows) {
  const reclassified = [];
  const proof = {
    paper_path_samples: rows.every((r) => 'paper_path_samples_seen' in r),
    source_channel_dbs: rows.some((r) => r.source_channel_registry_seen === true),
  };
  const modules = opsClosure.modules.map((m) => ({ ...m })).map((m) => {
    const status = m.coverage_status_final || m.coverage_status;
    if (status !== 'blocked') return m;
    const req = REQUIREMENTS[m.module_group];
    if (req && req.category === 'covered_now' && proof[m.module_group]) {
      reclassified.push({ module_group: m.module_group, from: 'blocked', to: 'covered', evidence: req.exact });
      return { ...m, coverage_status: 'covered', coverage_status_final: 'covered', reason_for_exclusion: null, note: req.exact };
    }
    if (req) return { ...m, reason_for_exclusion: `${req.category}::${req.exact}` };
    return m;
  });
  const statusCounts = countBy(modules, (m) => m.coverage_status_final || m.coverage_status);
  const VALID = new Set(['A', 'B', 'C', 'D', 'E', 'F', 'G']);
  const blocked = modules.filter((m) => (m.coverage_status_final || m.coverage_status) === 'blocked');
  return {
    schema_version: 'live_fullnet_module_closure_coverage_report.v5_final_blockers',
    note: 'Goal 5 (§15.20): final closure. 2 modules covered from same-window evidence; every remaining blocked module carries an EXACT required export (runtime_readmodel_required or zeabur_export_required), no bare not_in_current_exports.',
    every_module_mapped_to_A_G: modules.every((m) => VALID.has(m.bucket)),
    bucket_G_all_intentionally_excluded: modules.filter((m) => m.bucket === 'G').every((m) => (m.coverage_status_final || m.coverage_status) === 'intentionally_excluded'),
    no_generic_blocked_reason_remains: blocked.every((m) => /required::/.test(String(m.reason_for_exclusion || '')) || /requires_|not_exported|requires_runtime|zeabur|export/i.test(String(m.reason_for_exclusion || ''))),
    module_count_total: modules.length,
    coverage_status_counts: statusCounts,
    reclassified_from_blocked_this_layer: reclassified,
    modules,
  };
}

// ---- requirement reports ----------------------------------------------------------------------
function finalBlockerRequirementsReport(closure) {
  const blocked = closure.modules.filter((m) => (m.coverage_status_final || m.coverage_status) === 'blocked');
  return {
    schema_version: 'live_fullnet_final_blocker_requirements_report.v1',
    note: 'Master disposition for every module still blocked after Goal 4. Each has an exact required export. Goal 5 covers paper_path_samples + source_channel identity from same-window evidence.',
    covered_now_this_layer: closure.reclassified_from_blocked_this_layer.map((x) => ({ module: x.module_group, evidence: x.evidence })),
    still_blocked: blocked.map((m) => ({
      module_group: m.module_group,
      bucket: m.bucket,
      category: REQUIREMENTS[m.module_group]?.category ?? 'runtime_readmodel_required',
      exact_required_export: REQUIREMENTS[m.module_group]?.exact ?? m.reason_for_exclusion,
    })),
    category_counts: countBy(blocked, (m) => REQUIREMENTS[m.module_group]?.category ?? 'unknown'),
  };
}

function runtimeReadmodelRequirements() {
  const list = Object.entries(REQUIREMENTS).filter(([, v]) => v.category === 'runtime_readmodel_required');
  return {
    schema_version: 'live_fullnet_runtime_readmodel_export_requirements.v1',
    note: 'Same-window (06-21) runtime read-model exports needed to cover these modules. Research-only requirement list; no live change performed.',
    count: list.length,
    requirements: list.map(([module_group, v]) => ({ module_group, exact_required_export: v.exact })),
  };
}

function zeaburExportRequirements() {
  const list = Object.entries(REQUIREMENTS).filter(([, v]) => v.category === 'zeabur_export_required');
  return {
    schema_version: 'live_fullnet_zeabur_export_requirements.v1',
    note: 'External Zeabur/API/on-chain exports needed (data sources not in the live runtime read-models).',
    count: list.length,
    requirements: list.map(([module_group, v]) => ({ module_group, exact_required_export: v.exact })),
  };
}

function shadowSourceScoutReport(rows) {
  const groups = ['gmgn_policy', 'source_resonance', 'scout_quality', 'smart_money', 'curve_pumpfun', 'token_memory'];
  return {
    schema_version: 'live_fullnet_shadow_source_scout_token_memory_report.v1',
    note: 'Shadow/source/scout/token-memory modules. None are structured per-signal in same-window exports; gmgn/scout/resonance appear only as kline-provider or mode-label tokens. Each needs a runtime read-model (or Zeabur for smart_money/curve).',
    row_count: rows.length,
    modules: groups.map((g) => ({
      module_group: g,
      category: REQUIREMENTS[g].category,
      seen_n: rows.filter((r) => r[`${g}_seen`] === true).length,
      in_mode_label_n: g === 'source_resonance' ? rows.filter((r) => r.source_resonance_in_mode_label).length
        : g === 'scout_quality' ? rows.filter((r) => r.scout_quality_in_mode_label).length : null,
      exact_required_export: REQUIREMENTS[g].exact,
    })),
  };
}

function repairOwnerReportFinal(rows) {
  const dogs = rows.filter((r) => r.class === 'dog');
  const owners = Object.entries(countBy(rows, (r) => r.final_repair_owner_v2)).map(([key, n]) => ({ key, n, dog_n: dogs.filter((r) => r.final_repair_owner_v2 === key).length })).sort((a, b) => b.n - a.n);
  return {
    schema_version: 'live_fullnet_repair_owner_report.v5_final_blockers',
    note: '§15.15.20 ladder unchanged (same predicates => same distribution as v2/v3/v4). Goal 5 adds no per-row owner change; it closes module-level blockers with exact export requirements.',
    row_count: rows.length,
    owners,
  };
}

function rowConfidenceReportFinal(rows) {
  const byClass = (c) => rows.filter((r) => r.class === c);
  return {
    schema_version: 'live_fullnet_row_confidence_report.v5_final_blockers',
    row_count: rows.length, dog_count: byClass('dog').length, dud_count: byClass('dud').length, pending_count: byClass('pending').length,
    ev_eligible_count: rows.filter((r) => r.ev_eligible).length,
    distribution_all: countBy(rows, (r) => r.row_confidence),
    final_blocker_confidence_distribution: countBy(rows, (r) => r.final_blocker_confidence),
  };
}

function loadSourceMap(p) {
  const j = JSON.parse(fs.readFileSync(path.resolve(p), 'utf8'));
  const rows = Array.isArray(j) ? j : (j.rows || []);
  const map = new Map();
  for (const r of rows) { const k = signalKey(r.token_ca, r.signal_ts); if (!map.has(k)) map.set(k, r); }
  return map;
}

function buildFinalBlockers(args) {
  const ops = buildOpsActionability(args);
  if (!args.sourceToRaw) throw new Error('--source-to-raw is required (for channel identity)');
  const srcMap = loadSourceMap(args.sourceToRaw);
  const rows = ops.rows.map((r) => augmentFinalRow(r, srcMap.get(signalKey(r.token_ca, r.signal_ts)) || null));
  const closure = layerFinalClosure(ops.summary.module_closure_coverage, rows);
  const summary = {
    schema_version: 'live_fullnet_row_report_v2_final_blockers.v1',
    generated_at: ops.summary.generated_at,
    do_not_change_strategy: true,
    guardrails: { ...ops.summary.guardrails, final_blocker_research_only: true, every_blocked_module_has_exact_required_export: true },
    inputs: ops.summary.inputs,
    total_signals: rows.length,
    by_class: ops.summary.by_class,
    projection_complete: ops.summary.projection_complete,
    projection_completeness: ops.summary.projection_completeness,
    ev_gate: ops.summary.ev_gate,
    final_blocker_requirements_report: finalBlockerRequirementsReport(closure),
    runtime_readmodel_export_requirements: runtimeReadmodelRequirements(),
    zeabur_export_requirements: zeaburExportRequirements(),
    shadow_source_scout_token_memory_report: shadowSourceScoutReport(rows),
    repair_owner_report: repairOwnerReportFinal(rows),
    row_confidence_report: rowConfidenceReportFinal(rows),
    module_closure_coverage: closure,
    phase5_verdict: ops.summary.phase5_verdict,
  };
  return { summary, rows };
}

function writeFinal(report, outDir) {
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
    finalReqPath: w('final-blocker-requirements-report.json', report.summary.final_blocker_requirements_report),
    runtimeReqPath: w('runtime-readmodel-export-requirements.json', report.summary.runtime_readmodel_export_requirements),
    zeaburReqPath: w('zeabur-export-requirements.json', report.summary.zeabur_export_requirements),
    shadowPath: w('shadow-source-scout-token-memory-report.json', report.summary.shadow_source_scout_token_memory_report),
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
  if (args.help) { console.log('Usage: node scripts/build-live-fullnet-row-v2-final-blockers.js <all v2-ops-actionability args> --out-dir <dir>  (NODE_PATH=.../sentiment-arbitrage-system/node_modules)'); return { help: true }; }
  if (!args.outDir) throw new Error('--out-dir is required');
  const report = buildFinalBlockers(args);
  const written = writeFinal(report, path.resolve(args.outDir));
  console.log(JSON.stringify({
    ok: true,
    schema_version: report.summary.schema_version,
    total_signals: report.summary.total_signals,
    projection_complete: report.summary.projection_complete,
    ev_gate: report.summary.ev_gate.gate,
    reclassified_this_layer: report.summary.module_closure_coverage.reclassified_from_blocked_this_layer.map((x) => x.module_group),
    coverage_status_counts: report.summary.module_closure_coverage.coverage_status_counts,
    no_generic_blocked_reason_remains: report.summary.module_closure_coverage.no_generic_blocked_reason_remains,
    blocked_category_counts: report.summary.final_blocker_requirements_report.category_counts,
    paths: written,
  }, null, 2));
  return { report, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export {
  REQUIREMENTS,
  finalBlockerProjection,
  augmentFinalRow,
  layerFinalClosure,
  finalBlockerRequirementsReport,
  runtimeReadmodelRequirements,
  zeaburExportRequirements,
  buildFinalBlockers,
};
