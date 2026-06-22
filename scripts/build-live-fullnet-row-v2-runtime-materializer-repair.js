#!/usr/bin/env node
// Fullnet row v2 — runtime materializer REPAIR join layer (plan §15.25, Goal 8, script 2).
// Research-only: joins runtime-materializer-repair/ (Goal-8 script 1) into fullnet rows; reclassifies a
// module blocked->covered ONLY with HIGH/MEDIUM same-window repaired evidence; else keeps the FINAL
// materializer-specific reason. Wraps buildRuntimeMaterialized (Goal 7) unchanged; every new field is
// namespaced `<module>_repair_*` / `runtime_materializer_repair_*` so Goal 1-7 fields have 0 drift.
// smart_money + curve_pumpfun NOT touched (zeabur -> Goal 9). No live/strategy/mode change; EV fail-closed.
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { pathToFileURL } from 'url';
import { buildRuntimeMaterialized } from './build-live-fullnet-row-v2-runtime-materialized.js';

const REPAIR_MODULES = [
  'gmgn_policy', 'source_resonance', 'scout_quality', 'idempotency_write_path', 'worker_health',
  'training_manifest', 'detector_calibration', 'holdout_negative_controls', 'assumptions_false_negative_budget',
];

function num(v) { if (v == null || v === '') return null; const n = Number(v); if (!Number.isFinite(n)) return null; return n > 1e12 ? Math.floor(n / 1000) : n; }
function rnd(v) { const n = num(v); return n == null ? null : Math.round(n); }
function signalKey(t, s) { return `${t}|${rnd(s) ?? 0}`; }
function shortHash(s) { return s == null ? null : crypto.createHash('sha256').update(String(s)).digest('hex').slice(0, 16); }
function countBy(rows, fn) { const o = {}; for (const r of rows) { const k = fn(r) ?? 'unknown'; o[k] = (o[k] || 0) + 1; } return o; }

function loadRepair(dir) {
  const readJsonl = (f) => { const p = path.join(dir, f); return fs.existsSync(p) ? fs.readFileSync(p, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse) : []; };
  const readJson = (f) => { const p = path.join(dir, f); return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, 'utf8')) : null; };
  const rows = readJsonl('runtime-materializer-repair.jsonl');
  const health = readJson('runtime-materializer-repair-health.json');
  const healthByModule = {};
  for (const h of (health?.modules || [])) healthByModule[h.module_group] = h;
  const bySignal = new Map();
  for (const e of rows) { if (e.token_ca != null) { const k = `${e.module_group}|${signalKey(e.token_ca, e.signal_ts)}`; if (!bySignal.has(k)) bySignal.set(k, e); } }
  return { healthByModule, bySignal };
}

function moduleRepairFields(moduleGroup, row, idx) {
  const ev = idx.bySignal.get(`${moduleGroup}|${signalKey(row.token_ca, row.signal_ts)}`) || null;
  const seen = ev != null && ev.same_window_valid === true && (ev.join_confidence === 'HIGH' || ev.join_confidence === 'MEDIUM');
  const health = idx.healthByModule[moduleGroup] || {};
  const p = `${moduleGroup}_repair_`;
  const out = {
    [`${p}seen`]: seen,
    [`${p}status`]: health.status || 'still_missing',
    [`${p}join_confidence`]: ev ? ev.join_confidence : 'NONE',
    [`${p}source`]: ev ? ev.evidence_source : null,
    [`${p}evidence_ts`]: ev ? ev.evidence_ts : null,
    [`${p}payload_hash`]: ev ? shortHash(ev.payload_json) : null,
    [`${p}missing_reason`]: seen ? null : (health.missing_reason || `no_same_window_repair_evidence_for_${moduleGroup}`),
  };
  if (seen) {
    let pl = {}; try { pl = JSON.parse(ev.payload_json || '{}'); } catch { pl = {}; }
    if (moduleGroup === 'scout_quality') { out.scout_quality_repair_score = pl.scout_quality_score ?? null; out.scout_quality_repair_grade = pl.scout_quality_grade ?? null; out.scout_quality_repair_block_reason = pl.scout_quality_block_reason ?? null; }
    else if (moduleGroup === 'detector_calibration') { out.detector_calibration_repair_model_version = pl.model_version ?? null; out.detector_calibration_repair_calibration_bucket = pl.calibration_bucket ?? null; }
    else if (moduleGroup === 'idempotency_write_path') { out.idempotency_write_path_repair_key = pl.idempotency_key ?? null; out.idempotency_write_path_repair_dedupe_result = pl.dedupe_result ?? null; }
  }
  return { fields: out, seen };
}

function runtimeRepairRow(row, idx) {
  const fields = {}; const covered = []; const blocked = [];
  for (const m of REPAIR_MODULES) {
    const { fields: mf, seen } = moduleRepairFields(m, row, idx);
    Object.assign(fields, mf);
    (seen ? covered : blocked).push(m);
  }
  fields.runtime_materializer_repair_covered_modules = covered;
  fields.runtime_materializer_repair_still_blocked_modules = blocked;
  fields.runtime_materializer_repair_confidence = covered.length ? 'PARTIAL_SAME_WINDOW' : 'NONE_SAME_WINDOW';
  fields.runtime_materializer_repair_warnings = blocked.map((m) => `${m}:${fields[`${m}_repair_missing_reason`]}`);
  return fields;
}

function augmentRepairRow(row, idx) { return { ...row, ...runtimeRepairRow(row, idx) }; }

function layerRepairClosure(materializedClosure, rows, healthByModule) {
  const reclassified = [];
  const coveredByEvidence = new Set(REPAIR_MODULES.filter((m) => rows.some((r) => r[`${m}_repair_seen`] === true)));
  const modules = materializedClosure.modules.map((m) => ({ ...m })).map((m) => {
    const status = m.coverage_status_final || m.coverage_status;
    if (status !== 'blocked') return m;
    if (coveredByEvidence.has(m.module_group)) {
      const h = healthByModule[m.module_group] || {};
      const note = `repaired_same_window: ${h.status} joined=${h.joined_n} (Goal 8 materializer repair)`;
      reclassified.push({ module_group: m.module_group, from: 'blocked', to: 'covered', evidence: note });
      return { ...m, coverage_status: 'covered', coverage_status_final: 'covered', reason_for_exclusion: null, note };
    }
    const h = healthByModule[m.module_group];
    if (h && h.missing_reason) return { ...m, reason_for_exclusion: h.missing_reason }; // FINAL reason replaces prior
    return m;
  });
  const statusCounts = countBy(modules, (m) => m.coverage_status_final || m.coverage_status);
  const VALID = new Set(['A', 'B', 'C', 'D', 'E', 'F', 'G']);
  const blocked = modules.filter((m) => (m.coverage_status_final || m.coverage_status) === 'blocked');
  return {
    schema_version: 'live_fullnet_module_closure_coverage_report.v8_runtime_materializer_repair',
    note: 'Goal 8 (§15.25): repaired runtime modules covered ONLY with same-window HIGH/MEDIUM structured decision-subobject evidence (scout_quality component score/grade, expected_rr_detail rr_version/rr_grade, risk.opportunity_key/dedupe); the rest carry a FINAL materializer-specific reason. smart_money/curve_pumpfun untouched (zeabur -> Goal 9). Layered on Goal-7 closure (no regression).',
    every_module_mapped_to_A_G: modules.every((mm) => VALID.has(mm.bucket)),
    bucket_G_all_intentionally_excluded: modules.filter((mm) => mm.bucket === 'G').every((mm) => (mm.coverage_status_final || mm.coverage_status) === 'intentionally_excluded'),
    no_generic_blocked_reason_remains: blocked.every((mm) => /::|FINAL:|requires_|zeabur|export|materializer/i.test(String(mm.reason_for_exclusion || ''))),
    smart_money_and_curve_pumpfun_still_zeabur: ['smart_money', 'curve_pumpfun'].every((g) => { const mm = modules.find((x) => x.module_group === g); return /zeabur/i.test(String(mm?.reason_for_exclusion || '')); }),
    all_remaining_runtime_blockers_have_final_reason: blocked.filter((mm) => !/zeabur/i.test(String(mm.reason_for_exclusion || ''))).every((mm) => /FINAL:/.test(String(mm.reason_for_exclusion || ''))),
    module_count_total: modules.length,
    coverage_status_counts: statusCounts,
    reclassified_from_blocked_this_layer: reclassified,
    modules,
  };
}

function repairCoverageReport(rows, idx) {
  return {
    schema_version: 'live_fullnet_runtime_materializer_repair_coverage_report.v1',
    row_count: rows.length,
    modules: REPAIR_MODULES.map((m) => ({
      module_group: m, status: idx.healthByModule[m]?.status ?? 'still_missing',
      repair_seen_n: rows.filter((r) => r[`${m}_repair_seen`] === true).length,
      join_confidence_distribution: countBy(rows, (r) => r[`${m}_repair_join_confidence`]),
      missing_reason: idx.healthByModule[m]?.missing_reason ?? null,
    })),
    covered_modules: REPAIR_MODULES.filter((m) => rows.some((r) => r[`${m}_repair_seen`] === true)),
  };
}

function joinQualityReport(rows) {
  return {
    schema_version: 'live_fullnet_runtime_materializer_repair_join_quality_report.v1',
    row_count: rows.length,
    modules: REPAIR_MODULES.filter((m) => rows.some((r) => r[`${m}_repair_seen`])).map((m) => ({
      module_group: m, seen_n: rows.filter((r) => r[`${m}_repair_seen`]).length,
      seen_rate: Math.round((rows.filter((r) => r[`${m}_repair_seen`]).length / rows.length) * 1e4) / 1e4,
      dog_seen_n: rows.filter((r) => r.class === 'dog' && r[`${m}_repair_seen`]).length,
    })),
  };
}

function repairOwnerReport(rows) {
  const dogs = rows.filter((r) => r.class === 'dog');
  return {
    schema_version: 'live_fullnet_repair_owner_report.v8_runtime_materializer_repair',
    note: '§15.15.20 ladder unchanged (same predicates => same distribution). Goal 8 closes module-level blockers with repaired evidence or FINAL reasons; no per-row owner change.',
    row_count: rows.length,
    owners: Object.entries(countBy(rows, (r) => r.final_repair_owner_v2)).map(([key, n]) => ({ key, n, dog_n: dogs.filter((r) => r.final_repair_owner_v2 === key).length })).sort((a, b) => b.n - a.n),
  };
}

function rowConfidenceReport(rows) {
  const byClass = (c) => rows.filter((r) => r.class === c);
  return {
    schema_version: 'live_fullnet_row_confidence_report.v8_runtime_materializer_repair',
    row_count: rows.length, dog_count: byClass('dog').length, dud_count: byClass('dud').length, pending_count: byClass('pending').length,
    ev_eligible_count: rows.filter((r) => r.ev_eligible).length,
    distribution_all: countBy(rows, (r) => r.row_confidence),
    runtime_materializer_repair_confidence_distribution: countBy(rows, (r) => r.runtime_materializer_repair_confidence),
  };
}

function buildRuntimeMaterializerRepair(args) {
  const g7 = buildRuntimeMaterialized(args);
  if (!args.runtimeMaterializerRepairDir) throw new Error('--runtime-materializer-repair-dir is required (Goal-8 script-1 output)');
  const idx = loadRepair(path.resolve(args.runtimeMaterializerRepairDir));
  const rows = g7.rows.map((r) => augmentRepairRow(r, idx));
  const closure = layerRepairClosure(g7.summary.module_closure_coverage, rows, idx.healthByModule);
  const summary = {
    schema_version: 'live_fullnet_row_report_v2_runtime_materializer_repair.v1',
    generated_at: g7.summary.generated_at,
    do_not_change_strategy: true,
    guardrails: { ...g7.summary.guardrails, runtime_materializer_repair_research_only: true, no_label_string_as_structured_evidence: true, goal1to7_fields_namespaced_no_drift: true },
    inputs: { ...g7.summary.inputs, runtime_materializer_repair_dir: path.resolve(args.runtimeMaterializerRepairDir) },
    total_signals: rows.length,
    by_class: g7.summary.by_class,
    projection_complete: g7.summary.projection_complete,
    projection_completeness: g7.summary.projection_completeness,
    ev_gate: g7.summary.ev_gate,
    runtime_materializer_repair_coverage_report: repairCoverageReport(rows, idx),
    runtime_materializer_repair_join_quality_report: joinQualityReport(rows),
    runtime_materializer_repair_contract_report: idx.healthByModule,
    repair_owner_report: repairOwnerReport(rows),
    row_confidence_report: rowConfidenceReport(rows),
    module_closure_coverage: closure,
    phase5_verdict: g7.summary.phase5_verdict,
  };
  return { summary, rows };
}

function writeOut(report, outDir) {
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
    coveragePath: w('runtime-materializer-repair-coverage-report.json', report.summary.runtime_materializer_repair_coverage_report),
    joinQualityPath: w('runtime-materializer-repair-join-quality-report.json', report.summary.runtime_materializer_repair_join_quality_report),
    contractPath: w('runtime-materializer-repair-contract-report.json', report.summary.runtime_materializer_repair_contract_report),
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = { timeoutMs: 60_000 };
  const map = {
    '--source-to-raw': 'sourceToRaw', '--source-24h': 'source24h', '--raw-discovery': 'rawDiscovery',
    '--a-class-events': 'aClassEvents', '--ledger-export': 'ledgerExport', '--lifecycle-db': 'lifecycleDb',
    '--raw-signal-outcomes-db': 'rawSignalOutcomesDb', '--mode-registry': 'modeRegistry',
    '--entry-point-inventory': 'entryPointInventory', '--runtime-readmodel-dir': 'runtimeReadmodelDir',
    '--runtime-materialized-dir': 'runtimeMaterializedDir', '--runtime-materializer-repair-dir': 'runtimeMaterializerRepairDir', '--out-dir': 'outDir',
  };
  for (let i = 0; i < argv.length; i += 1) { const a = argv[i]; if (a === '--help' || a === '-h') { args.help = true; continue; } if (map[a]) { args[map[a]] = argv[i + 1]; i += 1; continue; } throw new Error(`Unknown argument: ${a}`); }
  return args;
}

function runCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) { console.log('Usage: node scripts/build-live-fullnet-row-v2-runtime-materializer-repair.js <all Goal-7 args> --runtime-materializer-repair-dir <dir> --out-dir <dir>'); return { help: true }; }
  if (!args.outDir) throw new Error('--out-dir is required');
  const report = buildRuntimeMaterializerRepair(args);
  const written = writeOut(report, path.resolve(args.outDir));
  console.log(JSON.stringify({
    ok: true, schema_version: report.summary.schema_version, total_signals: report.summary.total_signals,
    projection_complete: report.summary.projection_complete, ev_gate: report.summary.ev_gate.gate,
    reclassified_this_layer: report.summary.module_closure_coverage.reclassified_from_blocked_this_layer.map((x) => x.module_group),
    coverage_status_counts: report.summary.module_closure_coverage.coverage_status_counts,
    smart_money_curve_still_zeabur: report.summary.module_closure_coverage.smart_money_and_curve_pumpfun_still_zeabur,
    all_remaining_runtime_blockers_have_final_reason: report.summary.module_closure_coverage.all_remaining_runtime_blockers_have_final_reason,
    repaired_covered: report.summary.runtime_materializer_repair_coverage_report.covered_modules,
    paths: written,
  }, null, 2));
  return { report, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export { loadRepair, moduleRepairFields, runtimeRepairRow, augmentRepairRow, layerRepairClosure, buildRuntimeMaterializerRepair, REPAIR_MODULES };
