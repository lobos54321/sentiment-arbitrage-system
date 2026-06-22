#!/usr/bin/env node
// Fullnet row v2 — same-window runtime readmodel JOIN layer (plan §15.21, Goal 6, script 2).
// Research-only: joins runtime-readmodel-window.jsonl (Goal-6 script 1) back into fullnet rows and
// reclassifies a runtime module blocked->covered ONLY when it has same-window HIGH/MEDIUM joined
// evidence; otherwise it keeps the module blocked with the export's MORE PRECISE reason.
// Imports buildFinalBlockers (Goal 5) unchanged; pure ADDITIVE layer. To guarantee Goal 1/2/3/4/5
// fields have 0 drift, EVERY new row field is namespaced `<module>_runtime_*` / `runtime_readmodel_*`
// (no prior layer used those), so nothing existing is overwritten. smart_money + curve_pumpfun are
// NOT touched (remain zeabur_export_required). No live/strategy/gate/entry/exit/size/mode change; EV fail-closed.
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { pathToFileURL } from 'url';
import { buildFinalBlockers } from './build-live-fullnet-row-v2-final-blockers.js';

const RUNTIME_MODULES = [
  'gmgn_policy', 'token_memory', 'source_resonance', 'scout_quality', 'watchlist', 'parser_session',
  'idempotency_write_path', 'worker_health', 'training_manifest', 'detector_calibration',
  'holdout_negative_controls', 'evidence_conflict_aging', 'assumptions_false_negative_budget',
];

function num(v) { if (v == null || v === '') return null; const n = Number(v); if (!Number.isFinite(n)) return null; return n > 1e12 ? Math.floor(n / 1000) : n; }
function rnd(v) { const n = num(v); return n == null ? null : Math.round(n); }
function signalKey(t, s) { return `${t}|${rnd(s) ?? 0}`; }
function shortHash(s) { return s == null ? null : crypto.createHash('sha256').update(String(s)).digest('hex').slice(0, 16); }
function countBy(rows, fn) { const o = {}; for (const r of rows) { const k = fn(r) ?? 'unknown'; o[k] = (o[k] || 0) + 1; } return o; }

function loadRuntimeExport(dir) {
  const readJsonl = (f) => { const p = path.join(dir, f); return fs.existsSync(p) ? fs.readFileSync(p, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse) : []; };
  const readJson = (f) => { const p = path.join(dir, f); return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, 'utf8')) : null; };
  const joined = readJsonl('runtime-readmodel-window.jsonl');
  const health = readJson('runtime-readmodel-export-health.json');
  const healthByModule = {};
  for (const h of (health?.modules || [])) healthByModule[h.module_group] = h;
  // index joined evidence: parser_session by (token,signal_ts); token-level by token; window-global by module
  const bySignal = new Map(); const byToken = new Map(); const byModuleGlobal = new Map();
  for (const e of joined) {
    if (e.signal_ts != null && e.token_ca) { const k = `${e.module_group}|${signalKey(e.token_ca, e.signal_ts)}`; if (!bySignal.has(k)) bySignal.set(k, e); }
    else if (e.token_ca) { const k = `${e.module_group}|${e.token_ca}`; if (!byToken.has(k)) byToken.set(k, e); }
    else { if (!byModuleGlobal.has(e.module_group)) byModuleGlobal.set(e.module_group, e); }
  }
  return { joined, healthByModule, bySignal, byToken, byModuleGlobal };
}

// Pure: per-row runtime evidence for one module (namespaced fields only).
function moduleRuntimeFields(moduleGroup, row, idx) {
  const k = `${moduleGroup}|${signalKey(row.token_ca, row.signal_ts)}`;
  const kt = `${moduleGroup}|${row.token_ca}`;
  const ev = idx.bySignal.get(k) || idx.byToken.get(kt) || idx.byModuleGlobal.get(moduleGroup) || null;
  const seen = ev != null && ev.same_window_valid === true && (ev.join_confidence === 'HIGH' || ev.join_confidence === 'MEDIUM');
  const health = idx.healthByModule[moduleGroup] || {};
  const p = `${moduleGroup}_runtime_`;
  const out = {
    [`${p}seen`]: seen,
    [`${p}join_confidence`]: ev ? ev.join_confidence : 'NONE',
    [`${p}source`]: ev ? ev.evidence_source : null,
    [`${p}evidence_ts`]: ev ? ev.evidence_ts : null,
    [`${p}payload_hash`]: ev ? shortHash(ev.payload_json) : null,
    [`${p}missing_reason`]: seen ? null : (health.missing_reason || (ev ? `evidence_present_but_${ev.join_confidence}_join_or_cross_window` : `no_same_window_runtime_evidence_for_${moduleGroup}`)),
  };
  // covered-module semantic fields (namespaced; parser_session is the only same-window covered runtime module)
  if (moduleGroup === 'parser_session' && seen) {
    let pl = {}; try { pl = JSON.parse(ev.payload_json || '{}'); } catch { pl = {}; }
    out.parser_session_runtime_session_id = ev.signal_source_id ?? null;
    out.parser_session_runtime_status = pl.status ?? null;
    out.parser_session_runtime_provider = pl.provider ?? null;
    out.parser_session_runtime_lag_sec = pl.first_bar_lag_sec ?? null;
  }
  return { fields: out, seen };
}

function runtimeReadmodelRow(row, idx) {
  const fields = {};
  const covered = []; const blocked = []; const unjoined = [];
  for (const m of RUNTIME_MODULES) {
    const { fields: mf, seen } = moduleRuntimeFields(m, row, idx);
    Object.assign(fields, mf);
    if (seen) covered.push(m);
    else {
      blocked.push(m);
      if (fields[`${m}_runtime_join_confidence`] && fields[`${m}_runtime_join_confidence`] !== 'NONE') unjoined.push(m);
    }
  }
  fields.runtime_readmodel_covered_modules = covered;
  fields.runtime_readmodel_still_blocked_modules = blocked;
  fields.runtime_readmodel_unjoined_modules = unjoined;
  fields.runtime_readmodel_confidence = covered.length ? 'PARTIAL_SAME_WINDOW' : 'NONE_SAME_WINDOW';
  fields.runtime_readmodel_warnings = blocked.map((m) => `${m}:${fields[`${m}_runtime_missing_reason`]}`);
  return fields;
}

function augmentRuntimeRow(row, idx) { return { ...row, ...runtimeReadmodelRow(row, idx) }; }

// ---- closure: cover modules with same-window HIGH/MEDIUM joined evidence; refine the rest --------
function layerRuntimeClosure(finalClosure, rows, healthByModule) {
  const reclassified = [];
  const coveredByEvidence = new Set(RUNTIME_MODULES.filter((m) => rows.some((r) => r[`${m}_runtime_seen`] === true)));
  const modules = finalClosure.modules.map((m) => ({ ...m })).map((m) => {
    const status = m.coverage_status_final || m.coverage_status;
    if (status !== 'blocked') return m;
    if (coveredByEvidence.has(m.module_group)) {
      const h = healthByModule[m.module_group] || {};
      const note = `same_window_runtime_evidence: ${h.note || h.status || 'joined'} (Goal 6 runtime readmodel join)`;
      reclassified.push({ module_group: m.module_group, from: 'blocked', to: 'covered', evidence: note });
      return { ...m, coverage_status: 'covered', coverage_status_final: 'covered', reason_for_exclusion: null, note };
    }
    // still blocked -> refine with the export-health precise reason (more exact than Goal 5)
    const h = healthByModule[m.module_group];
    if (h && h.missing_reason) return { ...m, reason_for_exclusion: `runtime_readmodel_required::${h.missing_reason}` };
    return m;
  });
  const statusCounts = countBy(modules, (m) => m.coverage_status_final || m.coverage_status);
  const VALID = new Set(['A', 'B', 'C', 'D', 'E', 'F', 'G']);
  const blocked = modules.filter((m) => (m.coverage_status_final || m.coverage_status) === 'blocked');
  return {
    schema_version: 'live_fullnet_module_closure_coverage_report.v6_runtime_readmodels',
    note: 'Goal 6 (§15.21): runtime modules covered ONLY with same-window HIGH/MEDIUM joined evidence; the rest keep a MORE PRECISE export-gap reason. smart_money/curve_pumpfun untouched (zeabur). Layered on Goal-5 closure (no regression).',
    every_module_mapped_to_A_G: modules.every((mm) => VALID.has(mm.bucket)),
    bucket_G_all_intentionally_excluded: modules.filter((mm) => mm.bucket === 'G').every((mm) => (mm.coverage_status_final || mm.coverage_status) === 'intentionally_excluded'),
    no_generic_blocked_reason_remains: blocked.every((mm) => /::|requires_|not_exported|zeabur|export/i.test(String(mm.reason_for_exclusion || ''))),
    smart_money_and_curve_pumpfun_still_zeabur: ['smart_money', 'curve_pumpfun'].every((g) => { const mm = modules.find((x) => x.module_group === g); return /zeabur/i.test(String(mm?.reason_for_exclusion || '')); }),
    module_count_total: modules.length,
    coverage_status_counts: statusCounts,
    reclassified_from_blocked_this_layer: reclassified,
    modules,
  };
}

function runtimeReadmodelReport(rows, idx) {
  return {
    schema_version: 'live_fullnet_runtime_readmodel_report.v1',
    note: 'Per-module same-window runtime evidence coverage after join. Only modules with HIGH/MEDIUM same-window joins are covered.',
    row_count: rows.length,
    modules: RUNTIME_MODULES.map((m) => ({
      module_group: m,
      runtime_seen_n: rows.filter((r) => r[`${m}_runtime_seen`] === true).length,
      join_confidence_distribution: countBy(rows, (r) => r[`${m}_runtime_join_confidence`]),
      export_health: idx.healthByModule[m]?.status ?? 'missing',
      missing_reason: idx.healthByModule[m]?.missing_reason ?? null,
    })),
    covered_modules: RUNTIME_MODULES.filter((m) => rows.some((r) => r[`${m}_runtime_seen`] === true)),
  };
}

function repairOwnerReportRuntime(rows) {
  const dogs = rows.filter((r) => r.class === 'dog');
  return {
    schema_version: 'live_fullnet_repair_owner_report.v6_runtime_readmodels',
    note: '§15.15.20 ladder unchanged (same predicates => same distribution). Goal 6 closes module-level runtime blockers with same-window evidence or more-precise reasons; no per-row owner change.',
    row_count: rows.length,
    owners: Object.entries(countBy(rows, (r) => r.final_repair_owner_v2)).map(([key, n]) => ({ key, n, dog_n: dogs.filter((r) => r.final_repair_owner_v2 === key).length })).sort((a, b) => b.n - a.n),
  };
}

function rowConfidenceReportRuntime(rows) {
  const byClass = (c) => rows.filter((r) => r.class === c);
  return {
    schema_version: 'live_fullnet_row_confidence_report.v6_runtime_readmodels',
    row_count: rows.length, dog_count: byClass('dog').length, dud_count: byClass('dud').length, pending_count: byClass('pending').length,
    ev_eligible_count: rows.filter((r) => r.ev_eligible).length,
    distribution_all: countBy(rows, (r) => r.row_confidence),
    runtime_readmodel_confidence_distribution: countBy(rows, (r) => r.runtime_readmodel_confidence),
  };
}

function buildRuntimeReadmodels(args) {
  const fb = buildFinalBlockers(args);
  if (!args.runtimeReadmodelDir) throw new Error('--runtime-readmodel-dir is required (Goal-6 script-1 output)');
  const idx = loadRuntimeExport(path.resolve(args.runtimeReadmodelDir));
  const rows = fb.rows.map((r) => augmentRuntimeRow(r, idx));
  const closure = layerRuntimeClosure(fb.summary.module_closure_coverage, rows, idx.healthByModule);
  const summary = {
    schema_version: 'live_fullnet_row_report_v2_runtime_readmodels.v1',
    generated_at: fb.summary.generated_at,
    do_not_change_strategy: true,
    guardrails: { ...fb.summary.guardrails, runtime_readmodel_join_research_only: true, smart_money_curve_pumpfun_not_reclassified: true, goal1to5_fields_namespaced_no_drift: true },
    inputs: { ...fb.summary.inputs, runtime_readmodel_dir: path.resolve(args.runtimeReadmodelDir) },
    total_signals: rows.length,
    by_class: fb.summary.by_class,
    projection_complete: fb.summary.projection_complete,
    projection_completeness: fb.summary.projection_completeness,
    ev_gate: fb.summary.ev_gate,
    runtime_readmodel_report: runtimeReadmodelReport(rows, idx),
    repair_owner_report: repairOwnerReportRuntime(rows),
    row_confidence_report: rowConfidenceReportRuntime(rows),
    module_closure_coverage: closure,
    phase5_verdict: fb.summary.phase5_verdict,
  };
  return { summary, rows };
}

function writeRuntime(report, outDir) {
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
    runtimeReportPath: w('runtime-readmodel-join-report.json', report.summary.runtime_readmodel_report),
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = { timeoutMs: 60_000 };
  const map = {
    '--source-to-raw': 'sourceToRaw', '--source-24h': 'source24h', '--raw-discovery': 'rawDiscovery',
    '--a-class-events': 'aClassEvents', '--ledger-export': 'ledgerExport', '--lifecycle-db': 'lifecycleDb',
    '--raw-signal-outcomes-db': 'rawSignalOutcomesDb', '--mode-registry': 'modeRegistry',
    '--entry-point-inventory': 'entryPointInventory', '--runtime-readmodel-dir': 'runtimeReadmodelDir', '--out-dir': 'outDir',
  };
  for (let i = 0; i < argv.length; i += 1) { const a = argv[i]; if (a === '--help' || a === '-h') { args.help = true; continue; } if (map[a]) { args[map[a]] = argv[i + 1]; i += 1; continue; } throw new Error(`Unknown argument: ${a}`); }
  return args;
}

function runCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) { console.log('Usage: node scripts/build-live-fullnet-row-v2-runtime-readmodels.js <all v2-final-blockers args> --runtime-readmodel-dir <runtime-readmodel-window> --out-dir <dir>'); return { help: true }; }
  if (!args.outDir) throw new Error('--out-dir is required');
  const report = buildRuntimeReadmodels(args);
  const written = writeRuntime(report, path.resolve(args.outDir));
  console.log(JSON.stringify({
    ok: true,
    schema_version: report.summary.schema_version,
    total_signals: report.summary.total_signals,
    projection_complete: report.summary.projection_complete,
    ev_gate: report.summary.ev_gate.gate,
    reclassified_this_layer: report.summary.module_closure_coverage.reclassified_from_blocked_this_layer.map((x) => x.module_group),
    coverage_status_counts: report.summary.module_closure_coverage.coverage_status_counts,
    smart_money_curve_still_zeabur: report.summary.module_closure_coverage.smart_money_and_curve_pumpfun_still_zeabur,
    runtime_covered_modules: report.summary.runtime_readmodel_report.covered_modules,
    paths: written,
  }, null, 2));
  return { report, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export { loadRuntimeExport, moduleRuntimeFields, runtimeReadmodelRow, augmentRuntimeRow, layerRuntimeClosure, buildRuntimeReadmodels, RUNTIME_MODULES };
