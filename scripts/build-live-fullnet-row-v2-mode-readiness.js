#!/usr/bin/env node
// Fullnet row v2 + mode-readiness / promotion-governance (plan §15.18, Goal 3).
// Research-only: turns the coarse MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP owner into row-level,
// per-entry_mode, per-tier auditable evidence by joining each row's entry_mode to the (read-only,
// frozen) entry-mode registry + entry-point inventory governance contract.
//   - mode-config/entry-mode-registry.json   -> tiers, modes{tier,route,paper_enabled}, promotion_policy
//   - mode-config/v27-entry-point-inventory.json -> registered entry points
//   - v1/v2 row.shadow_only/readiness_* + entry_mode (already projected)
// Imports buildMarketData (Goal 2) unchanged; this is a pure ADDITIVE layer. It NEVER opens shadow
// modes, NEVER promotes a mode, NEVER changes live/gate/entry/exit/size, NEVER tunes thresholds, keeps
// EV fail-closed, and preserves every Goal-1/Goal-2 row field verbatim (byte-stable subset).
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';
import { buildMarketData } from './build-live-fullnet-row-v2-marketdata.js';

const MODE_OWNER = 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP';
const PROMOTABLE_TIERS = new Set(['live', 'isolated_paper_capped']);
const SHADOW_PRE_TIERS = new Set(['shadow_watch_only', 'revival_canary']);
const NON_PROMOTABLE_TIERS = new Set(['hard_shadow', 'deprecated_shadow']);

function readJson(p) { return JSON.parse(fs.readFileSync(path.resolve(p), 'utf8')); }
function countBy(rows, fn) { const o = {}; for (const r of rows) { const k = fn(r) ?? 'unknown'; o[k] = (o[k] || 0) + 1; } return o; }
function topReasons(rows, fn) { return Object.entries(countBy(rows, fn)).map(([key, n]) => ({ key, n })).sort((a, b) => b.n - a.n || a.key.localeCompare(b.key)); }
function rate(n, d) { return d ? Math.round((n / d) * 1e6) / 1e6 : null; }

// Parse the frozen governance contract into a compact lookup (pure).
function buildModeRegistry(registryJson, inventoryJson) {
  const tiers = registryJson?.tiers || {};
  const modes = registryJson?.modes || {};
  const virtualModes = registryJson?.virtual_modes || {};
  const pp = registryJson?.promotion_policy || {};
  // threshold catalog = promotion_policy stages carrying numeric thresholds
  const hasThresholdCatalog = Object.values(pp).some((stage) => stage && typeof stage === 'object'
    && Object.values(stage).some((v) => typeof v === 'number'));
  const hasPromotionGuardrails = Object.keys(pp).length > 0;
  const hasReleaseSafety = Boolean(pp.deprecation) || Boolean(registryJson?.decision_gates);
  const hasCooldown = JSON.stringify(registryJson || {}).toLowerCase().includes('cooldown');
  // entry-point inventory: set of strings that name registered entry points/modes
  const entryPointBlob = inventoryJson ? JSON.stringify(inventoryJson.entry_points || inventoryJson).toLowerCase() : '';
  return { tiers, modes, virtualModes, promotionPolicy: pp, hasThresholdCatalog, hasPromotionGuardrails, hasReleaseSafety, hasCooldown, entryPointBlob };
}

// Pure: per-row mode-readiness / promotion-governance projection.
function modeReadinessProjection(row, reg) {
  const mode = row.entry_mode ?? null;
  const regEntry = mode ? (reg.modes[mode] || reg.virtualModes[mode] || null) : null;
  const registered = regEntry != null;
  const tier = registered ? (regEntry.tier ?? 'registered_no_tier') : (mode ? 'unregistered_unknown' : 'no_decision_no_mode');
  const tierInfo = registered && reg.tiers[regEntry.tier] ? reg.tiers[regEntry.tier] : null;
  const paperEnabled = tierInfo ? Boolean(tierInfo.paper_enabled) : null;
  const entryPointRegistered = mode ? (reg.entryPointBlob ? reg.entryPointBlob.includes(String(mode).toLowerCase()) : null) : null;
  const shadowOnly = row.shadow_only === true;

  let promotionStatus;
  if (!mode) promotionStatus = 'no_entry_mode';
  else if (!registered) promotionStatus = 'unregistered_no_promotion_path';
  else if (PROMOTABLE_TIERS.has(tier)) promotionStatus = 'in_promotable_tier_requires_policy_gates';
  else if (SHADOW_PRE_TIERS.has(tier)) promotionStatus = 'shadow_tier_pre_promotion';
  else if (NON_PROMOTABLE_TIERS.has(tier)) promotionStatus = 'non_promotable_shadow_tier';
  else promotionStatus = 'tier_unknown';

  // promotion eligibility is fail-closed: requires registry tier + manual review + real evidence,
  // none of which is satisfiable here -> always false, with a precise block reason.
  const promotionEligible = false;
  const promotionBlockReason = !registered ? 'mode_not_in_registry'
    : NON_PROMOTABLE_TIERS.has(tier) ? `tier_${tier}_not_promotable`
      : SHADOW_PRE_TIERS.has(tier) ? 'shadow_tier_requires_promotion_policy_thresholds_and_manual_review'
        : 'requires_promotion_policy_thresholds_and_manual_review_and_real_entries';

  const warnings = [];
  if (!registered && mode) warnings.push(`mode_unregistered:${mode}`);
  if (!mode) warnings.push('no_entry_mode_no_decision');
  if (entryPointRegistered === false) warnings.push('entry_point_not_in_inventory');
  if (shadowOnly) warnings.push('mode_shadow_only');
  if (row.readiness_ok === false && row.readiness_blocker) warnings.push(`readiness_${row.readiness_blocker}`);

  // deterministic 0..1 readiness score from observable evidence
  let score = 0;
  if (registered) score += 0.4;
  if (row.readiness_ok === true) score += 0.3;
  if (paperEnabled === true) score += 0.1;
  if (row.executable_quote_clean === true) score += 0.2;
  score = Math.round(score * 1000) / 1000;
  const confidence = (registered && row.readiness_ok === true && row.executable_quote_clean === true) ? 'HIGH'
    : registered ? 'MEDIUM' : 'LOW';

  return {
    entry_mode_seen: mode != null,
    entry_mode_registry_seen: registered,
    entry_mode_registry_missing_reason: registered ? null : (mode ? `entry_mode_not_in_registry:${mode}` : 'no_decision_no_entry_mode'),
    mode_tier: tier,
    mode_tier_paper_enabled: paperEnabled,
    entry_point_registered: entryPointRegistered,
    mode_promotion_status: promotionStatus,
    mode_promotion_eligible: promotionEligible,
    mode_promotion_block_reason: promotionBlockReason,
    mode_demotion_status: tier === 'deprecated_shadow' ? 'deprecated_shadow' : 'not_demoted_or_unknown',
    mode_daily_cap_seen: false,
    mode_daily_cap_missing_reason: 'daily_cap_not_in_entry_mode_registry',
    mode_loss_cap_seen: false,
    mode_loss_cap_missing_reason: 'loss_cap_not_in_entry_mode_registry',
    mode_cooldown_policy_seen: reg.hasCooldown,
    mode_shadow_only: shadowOnly,
    mode_isolated_paper_capped: tier === 'isolated_paper_capped',
    mode_hard_shadow: tier === 'hard_shadow',
    mode_deprecated_shadow: tier === 'deprecated_shadow',
    mode_live_allowed: tier === 'live' && paperEnabled === true, // 'live' tier = realtime PAPER, not wallet execution
    mode_readiness_score: score,
    mode_readiness_confidence: confidence,
    mode_readiness_warnings: warnings,
    // promotion-governance contract presence (from frozen registry)
    metric_threshold_catalog_seen: reg.hasThresholdCatalog,
    metric_threshold_catalog_missing_reason: reg.hasThresholdCatalog ? null : 'metric_threshold_catalog_not_in_registry',
    promotion_guardrails_seen: reg.hasPromotionGuardrails,
    promotion_guardrails_missing_reason: reg.hasPromotionGuardrails ? null : 'promotion_guardrails_not_in_registry',
    release_safety_seen: reg.hasReleaseSafety,
    release_safety_missing_reason: reg.hasReleaseSafety ? null : 'release_safety_gates_not_in_registry',
    holdout_negative_controls_seen: false,
    holdout_negative_controls_missing_reason: 'holdout_negative_controls_not_in_registry_or_config__requires_runtime_readmodel_export',
    evidence_conflict_aging_seen: false,
    evidence_conflict_aging_missing_reason: 'evidence_conflict_aging_readmodel_not_exported',
    assumptions_false_negative_budget_seen: false,
    assumptions_false_negative_budget_missing_reason: 'assumptions_false_negative_budget_not_exported',
  };
}

// Precise sub-reason for rows whose final repair owner is the mode-readiness gap (de-blackboxing).
function modeReadinessRepairDetail(row, m) {
  if (row.final_repair_owner_v2 !== MODE_OWNER) return null;
  if (row.readiness_blocker && row.readiness_blocker !== 'no_decision_event_for_signal') return `readiness_${row.readiness_blocker}`;
  if (!m.entry_mode_registry_seen) return m.entry_mode_seen ? `mode_unregistered:${row.entry_mode}` : 'no_entry_mode';
  if (m.mode_shadow_only) return 'shadow_only_no_paper_promotion_path';
  if (NON_PROMOTABLE_TIERS.has(m.mode_tier)) return `tier_${m.mode_tier}_not_promotable`;
  return 'mode_not_eligible_for_promotion';
}

// Pure: augment one v2-marketdata row with mode-readiness fields (additive; preserves all prior fields).
function augmentModeRow(row, reg) {
  const m = modeReadinessProjection(row, reg);
  const merged = { ...row, ...m };
  merged.mode_readiness_repair_detail = modeReadinessRepairDetail(merged, merged);
  return merged;
}

// ---- closure: layer mode overrides on top of the marketdata closure (proof-gated) -------------
const MODE_COVERED_OVERRIDES = {
  mode_readiness: { proof: (rows) => rows.some((r) => r.mode_tier && r.mode_tier !== 'no_decision_no_mode'), note: 'per-signal mode->tier projection from frozen entry-mode-registry.json; current-window mode is unregistered (auditable finding, not a black box)' },
  metric_threshold_catalog: { proof: (rows, reg) => reg.hasThresholdCatalog, note: 'promotion_policy numeric thresholds (min_unique_tokens/min_quote_clean_samples/median_pnl_pct_gte/...) from frozen registry' },
  promotion_guardrails: { proof: (rows, reg) => reg.hasPromotionGuardrails, note: 'promotion_policy stages (shadow->isolated->live + deprecation, requires_manual_review) from frozen registry' },
  release_safety: { proof: (rows, reg) => reg.hasReleaseSafety, note: 'deprecation window + decision_gates from frozen registry' },
};
const MODE_REFINED_BLOCKED = {
  holdout_negative_controls: 'holdout_negative_controls_not_in_registry_or_config__requires_runtime_readmodel_export',
  evidence_conflict_aging: 'evidence_conflict_aging_readmodel_not_exported',
  assumptions_false_negative_budget: 'assumptions_false_negative_budget_not_exported',
  mode_readiness: undefined,
};

function layerModeClosure(marketdataClosure, rows, reg) {
  const reclassified = [];
  const baseModules = marketdataClosure.modules.map((m) => ({ ...m }));
  const modules = baseModules.map((m) => {
    const ov = MODE_COVERED_OVERRIDES[m.module_group];
    const status = m.coverage_status_final || m.coverage_status;
    if (ov && status === 'blocked') {
      if (ov.proof(rows, reg)) {
        reclassified.push({ module_group: m.module_group, from: 'blocked', to: 'covered', evidence: ov.note });
        return { ...m, coverage_status: 'covered', coverage_status_final: 'covered', reason_for_exclusion: null, note: ov.note };
      }
    }
    if (status === 'blocked' && MODE_REFINED_BLOCKED[m.module_group]) {
      return { ...m, reason_for_exclusion: MODE_REFINED_BLOCKED[m.module_group] };
    }
    return m;
  });
  const statusCounts = countBy(modules, (m) => m.coverage_status_final || m.coverage_status);
  const VALID = new Set(['A', 'B', 'C', 'D', 'E', 'F', 'G']);
  return {
    schema_version: 'live_fullnet_module_closure_coverage_report.v3_mode_readiness',
    note: 'Goal 3 (§15.18): mode-readiness/promotion-governance reclassified blocked->covered ONLY where the frozen registry provides the contract (proof-gated). Layered on the Goal-2 closure (no regression). Still-blocked governance modules carry a precise reason.',
    every_module_mapped_to_A_G: modules.every((m) => VALID.has(m.bucket)),
    bucket_G_all_intentionally_excluded: modules.filter((m) => m.bucket === 'G').every((m) => (m.coverage_status_final || m.coverage_status) === 'intentionally_excluded'),
    module_count_total: modules.length,
    coverage_status_counts: statusCounts,
    reclassified_from_blocked_this_layer: reclassified,
    modules,
  };
}

// ---- reports ----------------------------------------------------------------------------------
function modeReadinessReport(rows) {
  const dogs = rows.filter((r) => r.class === 'dog');
  return {
    schema_version: 'live_fullnet_mode_readiness_report.v1',
    note: 'Per-signal mode/tier/promotion projection vs frozen entry-mode-registry. Research-only; no mode opened/promoted. mode_promotion_eligible is fail-closed (always false here).',
    row_count: rows.length,
    dog_count: dogs.length,
    entry_mode_registry_seen_n: rows.filter((r) => r.entry_mode_registry_seen).length,
    entry_mode_unregistered_n: rows.filter((r) => r.entry_mode_seen && !r.entry_mode_registry_seen).length,
    mode_tier_distribution: countBy(rows, (r) => r.mode_tier),
    mode_promotion_status_distribution: countBy(rows, (r) => r.mode_promotion_status),
    mode_promotion_eligible_n: rows.filter((r) => r.mode_promotion_eligible).length,
    mode_shadow_only_n: rows.filter((r) => r.mode_shadow_only).length,
    mode_readiness_confidence_distribution: countBy(rows, (r) => r.mode_readiness_confidence),
    mode_readiness_score_avg: rate(rows.reduce((a, r) => a + (r.mode_readiness_score || 0), 0), rows.length),
    dog_vs_dud_registered_rate: { dog: rate(dogs.filter((r) => r.entry_mode_registry_seen).length, dogs.length), dud: rate(rows.filter((r) => r.class === 'dud' && r.entry_mode_registry_seen).length, rows.filter((r) => r.class === 'dud').length) },
    top_mode_readiness_warnings: topReasons(rows.flatMap((r) => (r.mode_readiness_warnings || []).map((w) => ({ w }))), (x) => x.w).slice(0, 12),
  };
}

function promotionGovernanceReport(rows, reg) {
  return {
    schema_version: 'live_fullnet_promotion_governance_report.v1',
    note: 'Promotion-governance CONTRACT presence (from frozen registry) + per-row promotion status. Contract present != any mode eligible; eligibility stays fail-closed.',
    contract: {
      tiers: Object.keys(reg.tiers),
      promotion_policy_stages: Object.keys(reg.promotionPolicy),
      metric_threshold_catalog_seen: reg.hasThresholdCatalog,
      promotion_guardrails_seen: reg.hasPromotionGuardrails,
      release_safety_seen: reg.hasReleaseSafety,
      holdout_negative_controls_seen: false,
      evidence_conflict_aging_seen: false,
      assumptions_false_negative_budget_seen: false,
    },
    blocked_governance_modules: [
      { module: 'holdout_negative_controls', reason: 'holdout_negative_controls_not_in_registry_or_config__requires_runtime_readmodel_export' },
      { module: 'evidence_conflict_aging', reason: 'evidence_conflict_aging_readmodel_not_exported' },
      { module: 'assumptions_false_negative_budget', reason: 'assumptions_false_negative_budget_not_exported' },
    ],
    promotion_block_reason_distribution: countBy(rows, (r) => r.mode_promotion_block_reason),
    rows_promotion_eligible_n: rows.filter((r) => r.mode_promotion_eligible).length,
  };
}

function entryModeTierReport(rows, reg) {
  const byMode = {};
  for (const r of rows) { const m = r.entry_mode ?? '(no_decision)'; (byMode[m] ||= []).push(r); }
  return {
    schema_version: 'live_fullnet_entry_mode_tier_report.v1',
    note: 'Per entry_mode: registry tier + registration + dog/dud + shadow/promotion status. Cross-checks the runtime normalized_mode label against the frozen registry.',
    registry_modes: Object.keys(reg.modes),
    registry_virtual_modes: Object.keys(reg.virtualModes),
    entry_modes: Object.entries(byMode).sort(([a], [b]) => a.localeCompare(b)).map(([mode, subset]) => ({
      entry_mode: mode,
      n: subset.length,
      dog_n: subset.filter((r) => r.class === 'dog').length,
      dud_n: subset.filter((r) => r.class === 'dud').length,
      registry_seen: subset[0].entry_mode_registry_seen,
      mode_tier: subset[0].mode_tier,
      mode_promotion_status: subset[0].mode_promotion_status,
      shadow_only_n: subset.filter((r) => r.mode_shadow_only).length,
    })),
  };
}

function repairOwnerReportMode(rows) {
  const ladderOrder = [
    'ROW_IDENTITY_UNIT_OR_DENOMINATOR_INVALID', 'SOURCE_PARSER_SESSION_OR_REGISTRY_GAP',
    'FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP', 'OPS_WORKER_READMODEL_STORAGE_OR_CONFIG_GAP',
    'PAPER_LIVE_OPERATOR_WRITE_OR_SECURITY_GAP', 'QUOTE_INTENT_PROVIDER_EXECUTOR_OR_IDEMPOTENCY_GAP',
    'EX_ANTE_ACTIONABILITY_OR_FILL_ANCHOR_GAP', 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP',
    'DIRECT_STRATEGY_DECISION_MODULE_GAP', 'ENTRY_BRIDGE_GAP', 'LEDGER_EXIT_HOLD_FRICTION_GAP',
    'SHADOW_ONLY_OR_ADVISORY_ONLY', 'INTENTIONALLY_EXCLUDED_NOT_CAPTURE_RELEVANT',
  ];
  const idx = (o) => { const i = ladderOrder.indexOf(o); return i < 0 ? 99 : i; };
  const dogs = rows.filter((r) => r.class === 'dog');
  const owners = topReasons(rows, (r) => r.final_repair_owner_v2).map((e) => ({ ...e, ladder_priority: idx(e.key), dog_n: dogs.filter((r) => r.final_repair_owner_v2 === e.key).length })).sort((a, b) => a.ladder_priority - b.ladder_priority || b.n - a.n);
  const mr = rows.filter((r) => r.final_repair_owner_v2 === MODE_OWNER);
  return {
    schema_version: 'live_fullnet_repair_owner_report.v3_mode_readiness',
    note: '§15.15.20 ladder unchanged (same predicates => same distribution as v2/v2-marketdata). The MODE_READINESS owner is now itemized by precise sub-reason + mode-registry status.',
    row_count: rows.length,
    owners,
    mode_readiness_owner_n: mr.length,
    mode_readiness_owner_dog_n: mr.filter((r) => r.class === 'dog').length,
    mode_readiness_repair_detail_breakdown: topReasons(mr, (r) => r.mode_readiness_repair_detail ?? 'unknown'),
    mode_readiness_owner_registry_status: topReasons(mr, (r) => (r.entry_mode_registry_seen ? 'registered' : 'unregistered') + ':' + r.mode_tier),
  };
}

function rowConfidenceReportMode(rows) {
  const byClass = (c) => rows.filter((r) => r.class === c);
  return {
    schema_version: 'live_fullnet_row_confidence_report.v3_mode_readiness',
    row_count: rows.length, dog_count: byClass('dog').length, dud_count: byClass('dud').length, pending_count: byClass('pending').length,
    ev_eligible_count: rows.filter((r) => r.ev_eligible).length,
    distribution_all: countBy(rows, (r) => r.row_confidence),
    mode_readiness_confidence_distribution_all: countBy(rows, (r) => r.mode_readiness_confidence),
  };
}

function buildModeReadiness(args) {
  const md = buildMarketData(args);
  if (!args.modeRegistry) throw new Error('--mode-registry is required (frozen entry-mode-registry.json)');
  const registryJson = readJson(args.modeRegistry);
  const inventoryJson = args.entryPointInventory ? readJson(args.entryPointInventory) : null;
  const reg = buildModeRegistry(registryJson, inventoryJson);
  const rows = md.rows.map((r) => augmentModeRow(r, reg));
  const closure = layerModeClosure(md.summary.module_closure_coverage, rows, reg);
  const summary = {
    schema_version: 'live_fullnet_row_report_v2_mode_readiness.v1',
    generated_at: md.summary.generated_at,
    do_not_change_strategy: true,
    guardrails: { ...md.summary.guardrails, mode_readiness_research_only: true, no_mode_opened_or_promoted: true, registry_read_only_frozen_copy: true },
    inputs: { ...md.summary.inputs, mode_registry: path.resolve(args.modeRegistry), entry_point_inventory: args.entryPointInventory ? path.resolve(args.entryPointInventory) : null },
    total_signals: rows.length,
    by_class: md.summary.by_class,
    projection_complete: md.summary.projection_complete,
    projection_completeness: md.summary.projection_completeness,
    ev_gate: md.summary.ev_gate,
    mode_readiness_report: modeReadinessReport(rows),
    promotion_governance_report: promotionGovernanceReport(rows, reg),
    entry_mode_tier_report: entryModeTierReport(rows, reg),
    repair_owner_report: repairOwnerReportMode(rows),
    row_confidence_report: rowConfidenceReportMode(rows),
    module_closure_coverage: closure,
    phase5_verdict: md.summary.phase5_verdict,
  };
  return { summary, rows };
}

function writeModeReadiness(report, outDir) {
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
    modeReadinessPath: w('mode-readiness-report.json', report.summary.mode_readiness_report),
    promotionGovernancePath: w('promotion-governance-report.json', report.summary.promotion_governance_report),
    entryModeTierPath: w('entry-mode-tier-report.json', report.summary.entry_mode_tier_report),
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
  if (args.help) {
    console.log([
      'Usage: node scripts/build-live-fullnet-row-v2-mode-readiness.js \\',
      '  --source-to-raw ... --source-24h ... --raw-discovery ... --a-class-events ... \\',
      '  --ledger-export ... --lifecycle-db ... --raw-signal-outcomes-db ... \\',
      '  --mode-registry mode-config/entry-mode-registry.json \\',
      '  --entry-point-inventory mode-config/v27-entry-point-inventory.json --out-dir <dir>',
      '',
      'Reads frozen registry copies only (no live-dir read). NODE_PATH=/Users/boliu/sentiment-arbitrage-system/node_modules.',
    ].join('\n'));
    return { help: true };
  }
  if (!args.outDir) throw new Error('--out-dir is required');
  const report = buildModeReadiness(args);
  const written = writeModeReadiness(report, path.resolve(args.outDir));
  console.log(JSON.stringify({
    ok: true,
    schema_version: report.summary.schema_version,
    total_signals: report.summary.total_signals,
    projection_complete: report.summary.projection_complete,
    ev_gate: report.summary.ev_gate.gate,
    reclassified_this_layer: report.summary.module_closure_coverage.reclassified_from_blocked_this_layer.map((x) => x.module_group),
    coverage_status_counts: report.summary.module_closure_coverage.coverage_status_counts,
    mode_readiness_owner_detail: report.summary.repair_owner_report.mode_readiness_repair_detail_breakdown,
    entry_mode_unregistered_n: report.summary.mode_readiness_report.entry_mode_unregistered_n,
    paths: written,
  }, null, 2));
  return { report, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export {
  buildModeRegistry,
  modeReadinessProjection,
  modeReadinessRepairDetail,
  augmentModeRow,
  layerModeClosure,
  modeReadinessReport,
  promotionGovernanceReport,
  entryModeTierReport,
  buildModeReadiness,
  MODE_OWNER,
};
