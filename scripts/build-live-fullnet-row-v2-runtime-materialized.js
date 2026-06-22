#!/usr/bin/env node
// Fullnet row v2 — runtime readmodel MATERIALIZED join layer (plan §15.23, Goal 7, script 2).
// Research-only: joins runtime-readmodel-materialized/ (Goal-7 script 1) into fullnet rows; reclassifies
// a module blocked->covered ONLY with HIGH/MEDIUM same-window materialized evidence (or a valid
// deterministic-zero manifest); else keeps it blocked with the materializer-specific exact reason.
// Wraps buildRuntimeReadmodels (Goal 6) unchanged; every new field is namespaced `<module>_materialized_*`
// / `runtime_materialized_*` so Goal 1-6 fields have 0 drift. smart_money + curve_pumpfun NOT touched
// (remain zeabur_export_required -> Goal 8). No live/strategy/gate/entry/exit/size/mode change; EV fail-closed.
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { pathToFileURL } from 'url';
import { buildRuntimeReadmodels } from './build-live-fullnet-row-v2-runtime-readmodels.js';

const TARGET_MODULES = [
  'gmgn_policy', 'token_memory', 'source_resonance', 'scout_quality', 'watchlist', 'idempotency_write_path',
  'worker_health', 'training_manifest', 'detector_calibration', 'holdout_negative_controls',
  'evidence_conflict_aging', 'assumptions_false_negative_budget',
];

function num(v) { if (v == null || v === '') return null; const n = Number(v); if (!Number.isFinite(n)) return null; return n > 1e12 ? Math.floor(n / 1000) : n; }
function rnd(v) { const n = num(v); return n == null ? null : Math.round(n); }
function signalKey(t, s) { return `${t}|${rnd(s) ?? 0}`; }
function shortHash(s) { return s == null ? null : crypto.createHash('sha256').update(String(s)).digest('hex').slice(0, 16); }
function countBy(rows, fn) { const o = {}; for (const r of rows) { const k = fn(r) ?? 'unknown'; o[k] = (o[k] || 0) + 1; } return o; }

function loadMaterialized(dir) {
  const readJsonl = (f) => { const p = path.join(dir, f); return fs.existsSync(p) ? fs.readFileSync(p, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse) : []; };
  const readJson = (f) => { const p = path.join(dir, f); return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, 'utf8')) : null; };
  const rows = readJsonl('runtime-readmodel-materialized.jsonl');
  const health = readJson('runtime-readmodel-materializer-health.json');
  const healthByModule = {};
  for (const h of (health?.modules || [])) healthByModule[h.module_group] = h;
  const bySignal = new Map();
  for (const e of rows) { if (e.token_ca != null) { const k = `${e.module_group}|${signalKey(e.token_ca, e.signal_ts)}`; if (!bySignal.has(k)) bySignal.set(k, e); } }
  return { healthByModule, bySignal };
}

// Pure: per-row materialized evidence for one module (namespaced fields only).
function moduleMaterializedFields(moduleGroup, row, idx) {
  const ev = idx.bySignal.get(`${moduleGroup}|${signalKey(row.token_ca, row.signal_ts)}`) || null;
  const seen = ev != null && ev.same_window_valid === true && (ev.join_confidence === 'HIGH' || ev.join_confidence === 'MEDIUM');
  const health = idx.healthByModule[moduleGroup] || {};
  const p = `${moduleGroup}_materialized_`;
  const out = {
    [`${p}seen`]: seen,
    [`${p}status`]: health.status || 'MATERIALIZER_MISSING_OR_INVALID',
    [`${p}join_confidence`]: ev ? ev.join_confidence : 'NONE',
    [`${p}source`]: ev ? ev.evidence_source : null,
    [`${p}evidence_ts`]: ev ? ev.evidence_ts : null,
    [`${p}payload_hash`]: ev ? shortHash(ev.payload_json) : null,
    [`${p}missing_reason`]: seen ? null : (health.missing_reason || `no_same_window_materialized_evidence_for_${moduleGroup}`),
  };
  if (seen) {
    let pl = {}; try { pl = JSON.parse(ev.payload_json || '{}'); } catch { pl = {}; }
    if (moduleGroup === 'token_memory') { out.token_memory_materialized_prior_n = pl.prior_in_window_signal_n ?? null; out.token_memory_materialized_state = pl.memory_state ?? null; }
    else if (moduleGroup === 'watchlist') { out.watchlist_materialized_state = pl.watchlist_state ?? null; out.watchlist_materialized_watch_reason = pl.watch_reason ?? null; }
    else if (moduleGroup === 'evidence_conflict_aging') { out.evidence_conflict_aging_materialized_age_sec = pl.evidence_age_sec ?? null; out.evidence_conflict_aging_materialized_conflict_state = pl.conflict_state ?? null; }
  }
  return { fields: out, seen };
}

function runtimeMaterializedRow(row, idx) {
  const fields = {}; const covered = []; const blocked = []; const emptyValid = []; const unjoined = [];
  for (const m of TARGET_MODULES) {
    const { fields: mf, seen } = moduleMaterializedFields(m, row, idx);
    Object.assign(fields, mf);
    const status = mf[`${m}_materialized_status`];
    if (seen) covered.push(m);
    else if (status === 'MATERIALIZER_EMPTY_BUT_VALID') emptyValid.push(m);
    else { blocked.push(m); if (mf[`${m}_materialized_join_confidence`] !== 'NONE') unjoined.push(m); }
  }
  fields.runtime_materialized_covered_modules = covered;
  fields.runtime_materialized_still_blocked_modules = blocked;
  fields.runtime_materialized_empty_valid_modules = emptyValid;
  fields.runtime_materialized_unjoined_modules = unjoined;
  fields.runtime_materialized_confidence = covered.length ? 'PARTIAL_SAME_WINDOW' : 'NONE_SAME_WINDOW';
  fields.runtime_materialized_warnings = blocked.map((m) => `${m}:${fields[`${m}_materialized_missing_reason`]}`);
  return fields;
}

function augmentMaterializedRow(row, idx) { return { ...row, ...runtimeMaterializedRow(row, idx) }; }

// closure: cover modules with same-window materialized evidence; refine the rest; leave zeabur alone.
function layerMaterializedClosure(runtimeClosure, rows, healthByModule) {
  const reclassified = [];
  const coveredByEvidence = new Set(TARGET_MODULES.filter((m) => rows.some((r) => r[`${m}_materialized_seen`] === true)));
  const modules = runtimeClosure.modules.map((m) => ({ ...m })).map((m) => {
    const status = m.coverage_status_final || m.coverage_status;
    if (status !== 'blocked') return m;
    if (coveredByEvidence.has(m.module_group)) {
      const h = healthByModule[m.module_group] || {};
      const note = `materialized_same_window: ${h.status} joined=${h.joined_n} (Goal 7 materializer)`;
      reclassified.push({ module_group: m.module_group, from: 'blocked', to: 'covered', evidence: note });
      return { ...m, coverage_status: 'covered', coverage_status_final: 'covered', reason_for_exclusion: null, note };
    }
    const h = healthByModule[m.module_group];
    if (h && h.missing_reason) return { ...m, reason_for_exclusion: `materializer::${h.missing_reason}` };
    return m;
  });
  const statusCounts = countBy(modules, (m) => m.coverage_status_final || m.coverage_status);
  const VALID = new Set(['A', 'B', 'C', 'D', 'E', 'F', 'G']);
  const blocked = modules.filter((m) => (m.coverage_status_final || m.coverage_status) === 'blocked');
  return {
    schema_version: 'live_fullnet_module_closure_coverage_report.v7_runtime_materialized',
    note: 'Goal 7 (§15.23): runtime modules covered ONLY with same-window HIGH/MEDIUM materialized evidence; the rest keep a materializer-specific exact reason. smart_money/curve_pumpfun untouched (zeabur -> Goal 8). Layered on Goal-6 closure (no regression).',
    every_module_mapped_to_A_G: modules.every((mm) => VALID.has(mm.bucket)),
    bucket_G_all_intentionally_excluded: modules.filter((mm) => mm.bucket === 'G').every((mm) => (mm.coverage_status_final || mm.coverage_status) === 'intentionally_excluded'),
    no_generic_blocked_reason_remains: blocked.every((mm) => /::|requires_|not_exported|zeabur|export|materializer/i.test(String(mm.reason_for_exclusion || ''))),
    smart_money_and_curve_pumpfun_still_zeabur: ['smart_money', 'curve_pumpfun'].every((g) => { const mm = modules.find((x) => x.module_group === g); return /zeabur/i.test(String(mm?.reason_for_exclusion || '')); }),
    module_count_total: modules.length,
    coverage_status_counts: statusCounts,
    reclassified_from_blocked_this_layer: reclassified,
    modules,
  };
}

function materializerCoverageReport(rows, idx) {
  return {
    schema_version: 'live_fullnet_runtime_materializer_coverage_report.v1',
    note: 'Per-module materialized coverage after join. Covered = HIGH/MEDIUM same-window materialized evidence.',
    row_count: rows.length,
    modules: TARGET_MODULES.map((m) => ({
      module_group: m, status: idx.healthByModule[m]?.status ?? 'MATERIALIZER_MISSING_OR_INVALID',
      materialized_seen_n: rows.filter((r) => r[`${m}_materialized_seen`] === true).length,
      join_confidence_distribution: countBy(rows, (r) => r[`${m}_materialized_join_confidence`]),
      missing_reason: idx.healthByModule[m]?.missing_reason ?? null,
    })),
    covered_modules: TARGET_MODULES.filter((m) => rows.some((r) => r[`${m}_materialized_seen`] === true)),
  };
}

function joinQualityReport(rows, idx) {
  return {
    schema_version: 'live_fullnet_runtime_materializer_join_quality_report.v1',
    note: 'Join quality for materialized modules. watchlist join rate is low (only signals with an exact same-window lifecycle track); reported transparently.',
    row_count: rows.length,
    modules: TARGET_MODULES.filter((m) => rows.some((r) => r[`${m}_materialized_seen`])).map((m) => ({
      module_group: m, seen_n: rows.filter((r) => r[`${m}_materialized_seen`]).length,
      seen_rate: Math.round((rows.filter((r) => r[`${m}_materialized_seen`]).length / rows.length) * 1e4) / 1e4,
      dog_seen_n: rows.filter((r) => r.class === 'dog' && r[`${m}_materialized_seen`]).length,
    })),
  };
}

function repairOwnerReportMat(rows) {
  const dogs = rows.filter((r) => r.class === 'dog');
  return {
    schema_version: 'live_fullnet_repair_owner_report.v7_runtime_materialized',
    note: '§15.15.20 ladder unchanged (same predicates => same distribution). Goal 7 closes module-level blockers with materialized evidence or sharper reasons; no per-row owner change.',
    row_count: rows.length,
    owners: Object.entries(countBy(rows, (r) => r.final_repair_owner_v2)).map(([key, n]) => ({ key, n, dog_n: dogs.filter((r) => r.final_repair_owner_v2 === key).length })).sort((a, b) => b.n - a.n),
  };
}

function rowConfidenceReportMat(rows) {
  const byClass = (c) => rows.filter((r) => r.class === c);
  return {
    schema_version: 'live_fullnet_row_confidence_report.v7_runtime_materialized',
    row_count: rows.length, dog_count: byClass('dog').length, dud_count: byClass('dud').length, pending_count: byClass('pending').length,
    ev_eligible_count: rows.filter((r) => r.ev_eligible).length,
    distribution_all: countBy(rows, (r) => r.row_confidence),
    runtime_materialized_confidence_distribution: countBy(rows, (r) => r.runtime_materialized_confidence),
  };
}

function buildRuntimeMaterialized(args) {
  const g6 = buildRuntimeReadmodels(args);
  if (!args.runtimeMaterializedDir) throw new Error('--runtime-materialized-dir is required (Goal-7 script-1 output)');
  const idx = loadMaterialized(path.resolve(args.runtimeMaterializedDir));
  const rows = g6.rows.map((r) => augmentMaterializedRow(r, idx));
  const closure = layerMaterializedClosure(g6.summary.module_closure_coverage, rows, idx.healthByModule);
  const summary = {
    schema_version: 'live_fullnet_row_report_v2_runtime_materialized.v1',
    generated_at: g6.summary.generated_at,
    do_not_change_strategy: true,
    guardrails: { ...g6.summary.guardrails, runtime_materialized_research_only: true, no_label_string_as_structured_evidence: true, goal1to6_fields_namespaced_no_drift: true },
    inputs: { ...g6.summary.inputs, runtime_materialized_dir: path.resolve(args.runtimeMaterializedDir) },
    total_signals: rows.length,
    by_class: g6.summary.by_class,
    projection_complete: g6.summary.projection_complete,
    projection_completeness: g6.summary.projection_completeness,
    ev_gate: g6.summary.ev_gate,
    runtime_materializer_coverage_report: materializerCoverageReport(rows, idx),
    runtime_materializer_join_quality_report: joinQualityReport(rows, idx),
    runtime_materializer_contract_report: idx.healthByModule,
    repair_owner_report: repairOwnerReportMat(rows),
    row_confidence_report: rowConfidenceReportMat(rows),
    module_closure_coverage: closure,
    phase5_verdict: g6.summary.phase5_verdict,
  };
  return { summary, rows };
}

function writeMat(report, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const w = (n, o) => { const p = path.join(outDir, n); fs.writeFileSync(p, `${JSON.stringify(o, null, 2)}\n`); return p; };
  const rowPath = path.join(outDir, 'row.jsonl');
  fs.writeFileSync(rowPath, `${report.rows.map((r) => JSON.stringify(r)).join('\n')}\n`);
  return {
    rowPath,
    summaryPath: w('summary.json', report.summary),
    repairOwnerPath: w('repair-owner-report.json', report.summary.repair_owner_report),
    rowConfidencePath: w('row-confidence-report.json', report.summary.row_confidence_report),
    moduleClosurePath: w('final-module-closure-coverage-report.json', report.summary.module_closure_coverage),
    coveragePath: w('runtime-materializer-coverage-report.json', report.summary.runtime_materializer_coverage_report),
    joinQualityPath: w('runtime-materializer-join-quality-report.json', report.summary.runtime_materializer_join_quality_report),
    contractPath: w('runtime-materializer-contract-report.json', report.summary.runtime_materializer_contract_report),
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = { timeoutMs: 60_000 };
  const map = {
    '--source-to-raw': 'sourceToRaw', '--source-24h': 'source24h', '--raw-discovery': 'rawDiscovery',
    '--a-class-events': 'aClassEvents', '--ledger-export': 'ledgerExport', '--lifecycle-db': 'lifecycleDb',
    '--raw-signal-outcomes-db': 'rawSignalOutcomesDb', '--mode-registry': 'modeRegistry',
    '--entry-point-inventory': 'entryPointInventory', '--runtime-readmodel-dir': 'runtimeReadmodelDir',
    '--runtime-materialized-dir': 'runtimeMaterializedDir', '--out-dir': 'outDir',
  };
  for (let i = 0; i < argv.length; i += 1) { const a = argv[i]; if (a === '--help' || a === '-h') { args.help = true; continue; } if (map[a]) { args[map[a]] = argv[i + 1]; i += 1; continue; } throw new Error(`Unknown argument: ${a}`); }
  return args;
}

function runCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) { console.log('Usage: node scripts/build-live-fullnet-row-v2-runtime-materialized.js <all Goal-6 args> --runtime-materialized-dir <dir> --out-dir <dir>'); return { help: true }; }
  if (!args.outDir) throw new Error('--out-dir is required');
  const report = buildRuntimeMaterialized(args);
  const written = writeMat(report, path.resolve(args.outDir));
  console.log(JSON.stringify({
    ok: true,
    schema_version: report.summary.schema_version,
    total_signals: report.summary.total_signals,
    projection_complete: report.summary.projection_complete,
    ev_gate: report.summary.ev_gate.gate,
    reclassified_this_layer: report.summary.module_closure_coverage.reclassified_from_blocked_this_layer.map((x) => x.module_group),
    coverage_status_counts: report.summary.module_closure_coverage.coverage_status_counts,
    smart_money_curve_still_zeabur: report.summary.module_closure_coverage.smart_money_and_curve_pumpfun_still_zeabur,
    materialized_covered: report.summary.runtime_materializer_coverage_report.covered_modules,
    paths: written,
  }, null, 2));
  return { report, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export { loadMaterialized, moduleMaterializedFields, runtimeMaterializedRow, augmentMaterializedRow, layerMaterializedClosure, buildRuntimeMaterialized, TARGET_MODULES };
