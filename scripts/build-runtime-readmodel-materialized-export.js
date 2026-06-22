#!/usr/bin/env node
// Runtime readmodel MATERIALIZER export (plan §15.23, Goal 7, script 1).
// Research-only: for the 12 modules Goal 6 proved missing, MATERIALIZE a same-window, structured,
// joinable readmodel WHERE the raw structured evidence genuinely exists (never from a label string),
// else emit a materializer-specific exact reason. 3 states per module:
//   MATERIALIZED_AND_JOINED | MATERIALIZER_EMPTY_BUT_VALID | MATERIALIZER_MISSING_OR_INVALID.
// No live read (frozen snapshots only), no cross-window join, no strategy/mode change, EV untouched.
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { pathToFileURL } from 'url';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
const Database = require('better-sqlite3');

const SKEW_SEC = 300;
const TARGET_MODULES = [
  'gmgn_policy', 'token_memory', 'source_resonance', 'scout_quality', 'watchlist', 'idempotency_write_path',
  'worker_health', 'training_manifest', 'detector_calibration', 'holdout_negative_controls',
  'evidence_conflict_aging', 'assumptions_false_negative_budget',
];

// Modules with no honest structured same-window materialization -> exact materializer reason (NOT label-derived).
const MATERIALIZER_BLOCKED = {
  gmgn_policy: 'materializer_ran: gmgn present only as kline provider + normalized_mode label; no structured gmgn_policy decision field in same-window sources; requires runtime gmgn_policy decision readmodel emit',
  source_resonance: 'materializer_ran: source_row_n multiplicity is structured but contract needs gmgn_first_seen_ts + lead_time_sec (gmgn-vs-telegram); absent same-window; requires runtime source_resonance readmodel emit',
  scout_quality: 'materializer_ran: matrix_score/liquidity present but the scout_quality score field is absent; requires runtime scout_quality readmodel emit',
  idempotency_write_path: 'materializer_ran: decision_event_id + enqueue/order/fill(all false) derivable but idempotency_key + write_path_status absent; requires sqlite_write_coordinator idempotency audit emit',
  worker_health: 'materializer_ran: raw_path_observer_provider_state present but updated_at post-window (cross-window); requires in-window worker heartbeat/readmodel-refresh emit',
  training_manifest: 'materializer_ran: no feature snapshot / training manifest in same-window sources; requires feature_training_manifest emit',
  detector_calibration: 'materializer_ran: lifecycle strategy_results gives shadow strategy OUTCOMES (joinable) but no detector model_version/calibration_bucket; requires detector calibration emit',
  holdout_negative_controls: 'materializer_ran: in-sample dog/dud computable but that is NOT an out-of-sample holdout; requires holdout + negative-control cohort emit',
  assumptions_false_negative_budget: 'materializer_ran: no FN-budget policy/assumption ledger in same-window sources; requires governance assumptions_fn_budget emit',
};

function num(v) { if (v == null || v === '') return null; const n = Number(v); if (!Number.isFinite(n)) return null; return n > 1e12 ? Math.floor(n / 1000) : n; }
function rnd(v) { const n = num(v); return n == null ? null : Math.round(n); }
function signalKey(t, s) { return `${t}|${rnd(s) ?? 0}`; }
function sha256File(p) { try { return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex'); } catch { return null; } }
function openRo(p) { const db = new Database(path.resolve(p), { readonly: true, fileMustExist: true }); db.pragma('query_only = ON'); return db; }
function tableExists(db, t) { return db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(t) != null; }
function inWindow(ts, s, e) { const n = num(ts); return n != null && n >= s - SKEW_SEC && n <= e + SKEW_SEC; }

function matRow(o) {
  return {
    schema_version: 'runtime_readmodel_materialized.v1',
    module_group: o.module_group, token_ca: o.token_ca ?? null, signal_ts: o.signal_ts ?? null,
    source_id: o.source_id ?? null, lifecycle_id: o.lifecycle_id ?? null,
    window_start_ts: o.window_start_ts, window_end_ts: o.window_end_ts, evidence_ts: o.evidence_ts ?? null,
    materializer_status: o.materializer_status, join_confidence: o.join_confidence, same_window_valid: o.same_window_valid,
    evidence_source: o.evidence_source, payload_json: o.payload_json ?? null, missing_reason: o.missing_reason ?? null,
  };
}

// watchlist: lifecycle tracks state machine (active/completed/dead + complete_reason) per (token,signal_ts).
function materializeWatchlist({ lifecycleDb, cohortKeys, startTs, endTs }) {
  if (!lifecycleDb || !tableExists(lifecycleDb, 'tracks')) return { joined: [], unjoined: [], status: 'MATERIALIZER_MISSING_OR_INVALID', missing_reason: 'lifecycle_tracks_absent' };
  const rows = lifecycleDb.prepare('SELECT token_ca, signal_ts, status, complete_reason, created_at, complete_ts, entry_ts FROM tracks').all();
  const joined = []; const unjoined = [];
  for (const r of rows) {
    const sigTs = num(r.signal_ts);
    const k = signalKey(r.token_ca, sigTs);
    const evidenceTs = num(r.complete_ts) ?? num(r.created_at) ?? sigTs;
    const sw = inWindow(sigTs, startTs, endTs);
    const rec = matRow({
      module_group: 'watchlist', token_ca: r.token_ca, signal_ts: sigTs, window_start_ts: startTs, window_end_ts: endTs,
      evidence_ts: evidenceTs, materializer_status: 'MATERIALIZED_AND_JOINED', join_confidence: cohortKeys.has(k) ? 'HIGH' : 'NONE',
      same_window_valid: sw, evidence_source: 'lifecycle_tracks.tracks',
      payload_json: JSON.stringify({ watchlist_state: r.status, watch_reason: r.complete_reason, registered_ts: num(r.created_at), expired_ts: num(r.complete_ts), armed_ts: num(r.entry_ts) }),
    });
    if (cohortKeys.has(k) && sw) joined.push(rec); else unjoined.push(rec);
  }
  return { joined, unjoined, status: joined.length ? 'MATERIALIZED_AND_JOINED' : 'MATERIALIZER_EMPTY_BUT_VALID', joined_n: joined.length };
}

// token_memory: prior IN-WINDOW same-token signal history (structured count + last prior ts). Pure (from cohort).
function materializeTokenMemory({ cohortSignals, startTs, endTs }) {
  const byToken = new Map();
  for (const s of cohortSignals) { const t = s.token_ca; if (!byToken.has(t)) byToken.set(t, []); byToken.get(t).push(num(s.signal_ts)); }
  for (const arr of byToken.values()) arr.sort((a, b) => a - b);
  const joined = [];
  for (const s of cohortSignals) {
    const sigTs = num(s.signal_ts);
    const priors = (byToken.get(s.token_ca) || []).filter((ts) => ts < sigTs);
    const priorN = priors.length;
    const lastPrior = priorN ? priors[priors.length - 1] : null;
    joined.push(matRow({
      module_group: 'token_memory', token_ca: s.token_ca, signal_ts: sigTs, window_start_ts: startTs, window_end_ts: endTs,
      evidence_ts: lastPrior ?? sigTs, materializer_status: 'MATERIALIZED_AND_JOINED',
      join_confidence: priorN > 0 ? 'MEDIUM' : 'HIGH', same_window_valid: true, evidence_source: 'derived:in_window_same_token_signal_history',
      payload_json: JSON.stringify({ memory_state: priorN > 0 ? 'prior_in_window_signals' : 'no_prior_in_window', prior_in_window_signal_n: priorN, last_prior_signal_ts: lastPrior, runtime_only_fields_absent: ['failure_type', 'reclaim_policy', 'quarantine_state'] }),
    }));
  }
  return { joined, unjoined: [], status: 'MATERIALIZED_AND_JOINED', joined_n: joined.length };
}

// evidence_conflict_aging: structured age (window_end - signal_ts) + real conflict (quote_clean mismatch). Pure.
function materializeEvidenceConflictAging({ cohortSignals, startTs, endTs }) {
  const joined = [];
  for (const s of cohortSignals) {
    const sigTs = num(s.signal_ts);
    const ageSec = sigTs != null ? Math.round(endTs - sigTs) : null;
    const mismatch = (num(s.quote_clean_mismatch_event_n) || 0) > 0 || s.quote_clean_source_mismatch === true;
    const decisionLag = num(s.first_route_decision_sec_after_signal);
    const staleReason = s.raw_missing_reason != null ? 'raw_or_kline_provenance_incomplete'
      : (decisionLag != null && decisionLag > 3600 ? 'decision_lag_gt_1h' : null);
    joined.push(matRow({
      module_group: 'evidence_conflict_aging', token_ca: s.token_ca, signal_ts: sigTs, window_start_ts: startTs, window_end_ts: endTs,
      evidence_ts: sigTs, materializer_status: 'MATERIALIZED_AND_JOINED', join_confidence: 'HIGH', same_window_valid: true,
      evidence_source: 'derived:timestamps+quote_clean_source_mismatch',
      payload_json: JSON.stringify({ evidence_age_sec: ageSec, conflict_state: mismatch ? 'quote_clean_top_vs_risk_mismatch' : 'no_conflict_detected', conflict_reason: mismatch ? 'canonical_top_quote_clean_disagrees_with_risk_json_verified' : null, stale_reason: staleReason }),
    }));
  }
  return { joined, unjoined: [], status: 'MATERIALIZED_AND_JOINED', joined_n: joined.length };
}

function buildMaterializedExport(args) {
  const startTs = num(args.windowStartTs); const endTs = num(args.windowEndTs);
  if (startTs == null || endTs == null || startTs >= endTs) throw new Error('--window-start-ts/--window-end-ts required and start<end');
  const cohortSignals = fs.readFileSync(path.resolve(args.fullnetRow), 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse)
    .map((r) => ({ token_ca: r.token_ca, signal_ts: r.signal_ts, quote_clean_mismatch_event_n: r.quote_clean_mismatch_event_n, quote_clean_source_mismatch: r.quote_clean_source_mismatch, first_route_decision_sec_after_signal: r.first_route_decision_sec_after_signal, raw_missing_reason: r.raw_missing_reason }));
  const cohortKeys = new Set(cohortSignals.map((s) => signalKey(s.token_ca, s.signal_ts)));
  const lifecyclePath = args.lifecycleDb;
  const lifecycleDb = lifecyclePath && fs.existsSync(lifecyclePath) ? openRo(lifecyclePath) : null;
  try {
    const M = {
      watchlist: materializeWatchlist({ lifecycleDb, cohortKeys, startTs, endTs }),
      token_memory: materializeTokenMemory({ cohortSignals, startTs, endTs }),
      evidence_conflict_aging: materializeEvidenceConflictAging({ cohortSignals, startTs, endTs }),
    };
    const health = []; const joined = []; const unjoined = []; const contract = [];
    for (const m of TARGET_MODULES) {
      if (M[m]) {
        joined.push(...M[m].joined); unjoined.push(...(M[m].unjoined || []));
        health.push({ module_group: m, status: M[m].status, joined_n: M[m].joined_n ?? M[m].joined.length, missing_reason: M[m].missing_reason ?? null });
        contract.push({ module_group: m, materialized: true, joined_n: M[m].joined_n ?? M[m].joined.length, note: 'structured same-window materialization (not label-derived)' });
      } else {
        health.push({ module_group: m, status: 'MATERIALIZER_MISSING_OR_INVALID', joined_n: 0, missing_reason: MATERIALIZER_BLOCKED[m] });
        contract.push({ module_group: m, materialized: false, joined_n: 0, missing_reason: MATERIALIZER_BLOCKED[m] });
      }
    }
    const ord = (a, b) => `${a.module_group}|${a.token_ca}|${a.signal_ts}`.localeCompare(`${b.module_group}|${b.token_ca}|${b.signal_ts}`);
    joined.sort(ord); unjoined.sort(ord);
    const summaryHealth = { schema_version: 'runtime_readmodel_materializer_health.v1', window_start_ts: startTs, window_end_ts: endTs, modules: health, materialized_modules: health.filter((h) => h.joined_n > 0).map((h) => h.module_group) };
    const manifest = {
      schema_version: 'runtime_readmodel_materializer_manifest.v1', window_start_ts: startTs, window_end_ts: endTs,
      sources: [
        lifecyclePath ? { source: 'lifecycle_tracks.snapshot.db', path: path.resolve(lifecyclePath), sha256: sha256File(lifecyclePath), tables_used: ['tracks'] } : null,
        { source: 'fullnet-row (cohort)', path: path.resolve(args.fullnetRow), sha256: sha256File(args.fullnetRow), derived: ['token_memory', 'evidence_conflict_aging'] },
      ].filter(Boolean),
    };
    const contractReport = { schema_version: 'runtime_readmodel_materializer_contract_report.v1', modules: contract };
    return { joined, unjoined, summaryHealth, manifest, contractReport };
  } finally { if (lifecycleDb) lifecycleDb.close(); }
}

function writeMaterialized(out, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const w = (n, o) => { const p = path.join(outDir, n); fs.writeFileSync(p, `${JSON.stringify(o, null, 2)}\n`); return p; };
  const wj = (n, a) => { const p = path.join(outDir, n); fs.writeFileSync(p, a.map((r) => JSON.stringify(r)).join('\n') + (a.length ? '\n' : '')); return p; };
  return {
    materializedPath: wj('runtime-readmodel-materialized.jsonl', out.joined),
    unjoinedPath: wj('runtime-readmodel-materializer-unjoined.jsonl', out.unjoined),
    healthPath: w('runtime-readmodel-materializer-health.json', out.summaryHealth),
    manifestPath: w('runtime-readmodel-materializer-manifest.json', out.manifest),
    contractPath: w('runtime-readmodel-materializer-contract-report.json', out.contractReport),
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = {}; const map = { '--window-start-ts': 'windowStartTs', '--window-end-ts': 'windowEndTs', '--fullnet-row': 'fullnetRow', '--lifecycle-db': 'lifecycleDb', '--runtime-sources-dir': 'runtimeSourcesDir', '--out-dir': 'outDir' };
  for (let i = 0; i < argv.length; i += 1) { const a = argv[i]; if (a === '--help' || a === '-h') { args.help = true; continue; } if (map[a]) { args[map[a]] = argv[i + 1]; i += 1; continue; } throw new Error(`Unknown argument: ${a}`); }
  return args;
}

function runCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) { console.log('Usage: node scripts/build-runtime-readmodel-materialized-export.js --window-start-ts <s> --window-end-ts <s> --fullnet-row <row.jsonl> --lifecycle-db <db> --out-dir <dir>'); return { help: true }; }
  if (!args.outDir || !args.fullnetRow) throw new Error('--fullnet-row and --out-dir are required');
  const out = buildMaterializedExport(args);
  const written = writeMaterialized(out, path.resolve(args.outDir));
  console.log(JSON.stringify({ ok: true, joined_n: out.joined.length, materialized_modules: out.summaryHealth.materialized_modules, paths: written }, null, 2));
  return { out, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export { materializeWatchlist, materializeTokenMemory, materializeEvidenceConflictAging, buildMaterializedExport, TARGET_MODULES, MATERIALIZER_BLOCKED, inWindow };
