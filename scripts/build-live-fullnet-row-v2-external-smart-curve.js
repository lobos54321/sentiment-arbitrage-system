#!/usr/bin/env node
// Goal 9 (plan §15.27) join layer: external smart_money + curve_pumpfun evidence on top of the Goal 8
// runtime-materializer-repair row. Research-only, additive: only `<module>_external_*` + `external_smart_curve_*`
// keys are added (Goal 1-8 fields byte-identical after stripping them). Reclassifies smart_money/curve from
// zeabur-blocked to covered IF same-window HIGH/MEDIUM external evidence joined, else to a FINAL external-export
// reason. No live calls, no strategy/gate/entry/exit/size/mode change. EV stays fail-closed.
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { pathToFileURL } from 'url';
import { buildRuntimeMaterializerRepair, augmentRepairRow } from './build-live-fullnet-row-v2-runtime-materializer-repair.js';

const EXTERNAL_MODULES = ['smart_money', 'curve_pumpfun'];

function num(v) { if (v == null || v === '') return null; const n = Number(v); if (!Number.isFinite(n)) return null; return n > 1e12 ? Math.floor(n / 1000) : n; }
function rnd(v) { const n = num(v); return n == null ? null : Math.round(n); }
function signalKey(t, s) { return `${t}|${rnd(s) ?? 0}`; }
function hash8(s) { return s == null ? null : crypto.createHash('sha256').update(String(s)).digest('hex').slice(0, 16); }

// Load Goal-9 script-1 output (external-smart-curve-window dir): joined evidence + per-module health/FINAL reason.
function loadExternal(dir) {
  const healthByModule = {}; const bySignal = new Map();
  const hp = path.join(dir, 'external-smart-curve-health.json');
  if (fs.existsSync(hp)) for (const m of JSON.parse(fs.readFileSync(hp, 'utf8')).modules || []) healthByModule[m.module_group] = m;
  const wp = path.join(dir, 'external-smart-curve-window.jsonl');
  if (fs.existsSync(wp)) for (const line of fs.readFileSync(wp, 'utf8').trim().split('\n').filter(Boolean)) { const r = JSON.parse(line); bySignal.set(`${r.module_group}|${signalKey(r.token_ca, r.signal_ts)}`, r); }
  return { healthByModule, bySignal };
}

// Per-module per-row external fields. Covered only on same-window HIGH/MEDIUM joined evidence; else FINAL reason.
function moduleExternalFields(moduleGroup, row, ext) {
  const ev = ext.bySignal.get(`${moduleGroup}|${signalKey(row.token_ca, row.signal_ts)}`);
  const health = ext.healthByModule[moduleGroup] || {};
  const seen = !!(ev && ev.same_window_valid && (ev.join_confidence === 'HIGH' || ev.join_confidence === 'MEDIUM') && !ev.missing_reason);
  const p = `${moduleGroup}_external_`;
  return {
    seen,
    fields: {
      [`${p}seen`]: seen,
      [`${p}status`]: seen ? 'EXTERNAL_EVIDENCE_JOINED' : (health.status || 'EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON'),
      [`${p}join_confidence`]: seen ? ev.join_confidence : 'NONE',
      [`${p}source`]: seen ? ev.evidence_source : null,
      [`${p}evidence_ts`]: seen ? (ev.evidence_ts ?? null) : null,
      [`${p}payload_hash`]: seen ? hash8(ev.payload_json) : null,
      [`${p}missing_reason`]: seen ? null : (health.missing_reason || `FINAL: no same-window external evidence for ${moduleGroup}`),
    },
  };
}

function externalRow(row, ext) {
  const covered = []; const blocked = []; const emptyValid = []; const unjoined = []; const warnings = [];
  const out = {};
  for (const m of EXTERNAL_MODULES) {
    const { seen, fields } = moduleExternalFields(m, row, ext);
    Object.assign(out, fields);
    const status = fields[`${m}_external_status`];
    if (seen) covered.push(m);
    else if (status === 'EXTERNAL_EMPTY_BUT_VALID') emptyValid.push(m);
    else { blocked.push(m); unjoined.push(m); }
    if (!seen && !/FINAL:/.test(String(fields[`${m}_external_missing_reason`]))) warnings.push(`${m}: non-final external reason`);
  }
  const conf = covered.length === EXTERNAL_MODULES.length ? 'HIGH' : (covered.length ? 'MEDIUM' : 'NONE');
  return {
    ...out,
    external_smart_curve_covered_modules: covered,
    external_smart_curve_still_blocked_modules: blocked,
    external_smart_curve_empty_valid_modules: emptyValid,
    external_smart_curve_unjoined_modules: unjoined,
    external_smart_curve_confidence: conf,
    external_smart_curve_warnings: warnings,
  };
}

function augmentExternalRow(row, ext) { return { ...row, ...externalRow(row, ext) }; }

function countBy(arr, fn) { const o = {}; for (const x of arr) { const k = fn(x); o[k] = (o[k] || 0) + 1; } return o; }

// Reclassify smart_money/curve on top of the Goal-8 closure: covered if joined, else FINAL external reason.
function layerExternalClosure(g8Closure, ext) {
  const reclassified = [];
  const modules = (g8Closure.modules || []).map((m) => {
    if (!EXTERNAL_MODULES.includes(m.module_group)) return m;
    const h = ext.healthByModule[m.module_group] || {};
    const status = m.coverage_status_final || m.coverage_status;
    if (h.status === 'EXTERNAL_EVIDENCE_JOINED' && (h.joined_n || 0) > 0) {
      reclassified.push({ module_group: m.module_group, from: status, to: 'covered' });
      return { ...m, coverage_status: 'covered', coverage_status_final: 'covered', reason_for_exclusion: null, note: `Goal9 external: ${h.joined_n} same-window rows joined` };
    }
    // stays blocked, but now with a FINAL external-export reason (replaces generic zeabur_export_required)
    return { ...m, coverage_status: 'blocked', coverage_status_final: 'blocked', reason_for_exclusion: h.missing_reason || `FINAL: no same-window external evidence for ${m.module_group}` };
  });
  const statusCounts = countBy(modules, (m) => m.coverage_status_final || m.coverage_status);
  const blocked = modules.filter((m) => (m.coverage_status_final || m.coverage_status) === 'blocked');
  const ext2 = EXTERNAL_MODULES.map((g) => modules.find((m) => m.module_group === g));
  return {
    ...g8Closure,
    modules,
    reclassified_from_blocked_this_layer: reclassified,
    coverage_status_counts: statusCounts,
    total_modules: modules.length,
    no_generic_blocked_reason_remains: blocked.every((mm) => /::|FINAL:|requires_|zeabur|export|materializer/i.test(String(mm.reason_for_exclusion || ''))),
    // Goal-9 success: smart_money + curve_pumpfun each end covered OR carry a FINAL external-export reason.
    smart_money_and_curve_pumpfun_resolved: ext2.every((mm) => (mm.coverage_status_final || mm.coverage_status) === 'covered' || /FINAL:/.test(String(mm.reason_for_exclusion || ''))),
    smart_money_and_curve_pumpfun_status: Object.fromEntries(ext2.map((mm) => [mm.module_group, (mm.coverage_status_final || mm.coverage_status) === 'covered' ? 'covered' : 'final_blocked'])),
    // runtime FINAL invariant preserved (the 6 runtime blockers from Goal 8 still carry FINAL reasons)
    all_remaining_runtime_blockers_have_final_reason: blocked.filter((mm) => !EXTERNAL_MODULES.includes(mm.module_group)).every((mm) => /FINAL:|zeabur/i.test(String(mm.reason_for_exclusion || ''))),
    all_blocked_have_final_reason: blocked.every((mm) => /FINAL:/.test(String(mm.reason_for_exclusion || ''))),
  };
}

function externalCoverageReport(rows, ext) {
  const covered = EXTERNAL_MODULES.filter((m) => rows.some((r) => r[`${m}_external_seen`]));
  return {
    schema_version: 'external_smart_curve_coverage.v1',
    covered_modules: covered,
    still_blocked_modules: EXTERNAL_MODULES.filter((m) => !covered.includes(m)),
    module_status: Object.fromEntries(EXTERNAL_MODULES.map((m) => [m, (ext.healthByModule[m] || {}).status || 'EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON'])),
    module_final_reason: Object.fromEntries(EXTERNAL_MODULES.map((m) => [m, (ext.healthByModule[m] || {}).missing_reason || null])),
    rows_with_smart_money_external: rows.filter((r) => r.smart_money_external_seen).length,
    rows_with_curve_pumpfun_external: rows.filter((r) => r.curve_pumpfun_external_seen).length,
  };
}

function buildExternalSmartCurve(args) {
  const g8 = buildRuntimeMaterializerRepair(args);
  if (!args.externalSmartCurveDir) throw new Error('--external-smart-curve-dir is required (Goal-9 script-1 output)');
  const ext = loadExternal(path.resolve(args.externalSmartCurveDir));
  const rows = g8.rows.map((r) => augmentExternalRow(r, ext));
  const closure = layerExternalClosure(g8.summary.module_closure_coverage, ext);
  const summary = {
    schema_version: 'live_fullnet_row_report_v2_external_smart_curve.v1',
    generated_at: g8.summary.generated_at,
    do_not_change_strategy: true,
    guardrails: { ...g8.summary.guardrails, external_smart_curve_research_only: true, no_live_calls: true, no_label_string_as_structured_evidence: true, goal1to8_fields_namespaced_no_drift: true },
    inputs: { ...g8.summary.inputs, external_smart_curve_dir: path.resolve(args.externalSmartCurveDir) },
    total_signals: rows.length,
    by_class: g8.summary.by_class,
    projection_complete: g8.summary.projection_complete,
    projection_completeness: g8.summary.projection_completeness,
    ev_gate: g8.summary.ev_gate,
    external_smart_curve_coverage_report: externalCoverageReport(rows, ext),
    external_smart_curve_contract_report: ext.healthByModule,
    repair_owner_report: g8.summary.repair_owner_report,
    row_confidence_report: g8.summary.row_confidence_report,
    module_closure_coverage: closure,
    phase5_verdict: g8.summary.phase5_verdict,
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
    coveragePath: w('external-smart-curve-coverage-report.json', report.summary.external_smart_curve_coverage_report),
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = { timeoutMs: 60_000 };
  const map = {
    '--source-to-raw': 'sourceToRaw', '--source-24h': 'source24h', '--raw-discovery': 'rawDiscovery',
    '--a-class-events': 'aClassEvents', '--ledger-export': 'ledgerExport', '--lifecycle-db': 'lifecycleDb',
    '--raw-signal-outcomes-db': 'rawSignalOutcomesDb', '--mode-registry': 'modeRegistry',
    '--entry-point-inventory': 'entryPointInventory', '--runtime-readmodel-dir': 'runtimeReadmodelDir',
    '--runtime-materialized-dir': 'runtimeMaterializedDir', '--runtime-materializer-repair-dir': 'runtimeMaterializerRepairDir',
    '--external-smart-curve-dir': 'externalSmartCurveDir', '--out-dir': 'outDir',
  };
  for (let i = 0; i < argv.length; i += 1) { const a = argv[i]; if (a === '--help' || a === '-h') { args.help = true; continue; } if (map[a]) { args[map[a]] = argv[i + 1]; i += 1; continue; } throw new Error(`Unknown argument: ${a}`); }
  return args;
}

function runCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) { console.log('Usage: node scripts/build-live-fullnet-row-v2-external-smart-curve.js <all Goal-8 args> --external-smart-curve-dir <dir> --out-dir <dir>'); return { help: true }; }
  if (!args.outDir) throw new Error('--out-dir is required');
  const report = buildExternalSmartCurve(args);
  const written = writeOut(report, path.resolve(args.outDir));
  const c = report.summary.module_closure_coverage;
  console.log(JSON.stringify({
    ok: true, schema_version: report.summary.schema_version, total_signals: report.summary.total_signals,
    projection_complete: report.summary.projection_complete, ev_gate: report.summary.ev_gate.gate,
    reclassified_this_layer: c.reclassified_from_blocked_this_layer.map((x) => x.module_group),
    coverage_status_counts: c.coverage_status_counts,
    smart_money_and_curve_pumpfun_status: c.smart_money_and_curve_pumpfun_status,
    smart_money_and_curve_pumpfun_resolved: c.smart_money_and_curve_pumpfun_resolved,
    all_blocked_have_final_reason: c.all_blocked_have_final_reason,
    no_generic_blocked_reason_remains: c.no_generic_blocked_reason_remains,
    paths: written,
  }, null, 2));
  return { report, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export { loadExternal, moduleExternalFields, externalRow, augmentExternalRow, layerExternalClosure, buildExternalSmartCurve, EXTERNAL_MODULES };
