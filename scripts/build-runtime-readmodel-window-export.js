#!/usr/bin/env node
// Same-window runtime readmodel export (plan §15.21, Goal 6, script 1).
// Research-only: normalizes whatever SAME-WINDOW (06-21) runtime read-model evidence exists in the
// FROZEN snapshot artifacts into one contract, for the 13 runtime_readmodel_required modules. It never
// reads the live dir, never mixes the 06-19 mirror logs, never tunes/opens/promotes anything.
// Strict same-window rule: each evidence row carries window bounds + same_window_valid; cross-window or
// post-window state rows are flagged (stale_or_cross_window_reason) and routed to unjoined, NOT silently joined.
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { pathToFileURL } from 'url';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
const Database = require('better-sqlite3');

const SKEW_SEC = 300;
const ALL_MODULES = [
  'gmgn_policy', 'token_memory', 'source_resonance', 'scout_quality', 'watchlist', 'parser_session',
  'idempotency_write_path', 'worker_health', 'training_manifest', 'detector_calibration',
  'holdout_negative_controls', 'evidence_conflict_aging', 'assumptions_false_negative_budget',
];

function num(v) { if (v == null || v === '') return null; const n = Number(v); if (!Number.isFinite(n)) return null; return n > 1e12 ? Math.floor(n / 1000) : n; }
function rnd(v) { const n = num(v); return n == null ? null : Math.round(n); }
function signalKey(t, s) { return `${t}|${rnd(s) ?? 0}`; }
function sha256File(p) { try { return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex'); } catch { return null; } }
function openRo(p) { const db = new Database(path.resolve(p), { readonly: true, fileMustExist: true }); db.pragma('query_only = ON'); return db; }
function tableExists(db, t) { return db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(t) != null; }

// Pure: same-window validity for an evidence timestamp.
function windowValidity(evidenceTs, startTs, endTs) {
  const ts = num(evidenceTs);
  if (ts == null) return { same_window_valid: false, reason: 'evidence_ts_null' };
  if (ts > endTs + SKEW_SEC) return { same_window_valid: false, reason: `evidence_ts_${Math.round(ts)}_after_window_end_${endTs}` };
  if (ts < startTs - SKEW_SEC) return { same_window_valid: false, reason: `evidence_ts_${Math.round(ts)}_before_window_start_${startTs}` };
  return { same_window_valid: true, reason: null };
}

function evidenceRow(o) {
  return {
    schema_version: 'runtime_readmodel_window.v1',
    module_group: o.module_group,
    token_ca: o.token_ca ?? null,
    symbol: o.symbol ?? null,
    signal_ts: o.signal_ts ?? null,
    signal_source_id: o.signal_source_id ?? null,
    lifecycle_id: o.lifecycle_id ?? null,
    window_start_ts: o.window_start_ts,
    window_end_ts: o.window_end_ts,
    evidence_ts: o.evidence_ts ?? null,
    evidence_source: o.evidence_source,
    evidence_source_sha256: o.evidence_source_sha256 ?? null,
    join_key_type: o.join_key_type,
    join_confidence: o.join_confidence,
    same_window_valid: o.same_window_valid,
    stale_or_cross_window_reason: o.stale_or_cross_window_reason ?? null,
    payload_json: o.payload_json ?? null,
    missing_reason: o.missing_reason ?? null,
  };
}

// Per-module extractors. Each returns { joined: [evidenceRow], unjoined: [evidenceRow], health }.
// Sources that don't exist -> module-level missing_reason, no throw.
function extractParserSession({ rawDb, rawSha, cohortKeys, startTs, endTs }) {
  if (!rawDb || !tableExists(rawDb, 'raw_signal_observations')) {
    return { joined: [], unjoined: [], health: { module_group: 'parser_session', status: 'missing', missing_reason: 'raw_signal_observations_absent__requires_parser_ingestion_session_export' } };
  }
  const rows = rawDb.prepare('SELECT signal_id, token_ca, symbol, signal_ts, status, source_kind, provider, first_bar_lag_sec, matured_at_ts, created_at, updated_at FROM raw_signal_observations').all();
  const joined = []; const unjoined = [];
  for (const r of rows) {
    const sigTs = num(r.signal_ts);
    const inCohort = cohortKeys.has(signalKey(r.token_ca, sigTs));
    const wv = windowValidity(sigTs, startTs, endTs);
    const er = evidenceRow({
      module_group: 'parser_session', token_ca: r.token_ca, symbol: r.symbol, signal_ts: sigTs,
      signal_source_id: r.signal_id, window_start_ts: startTs, window_end_ts: endTs, evidence_ts: sigTs,
      evidence_source: 'raw_signal_observations', evidence_source_sha256: rawSha,
      join_key_type: 'token_ca+signal_ts', join_confidence: inCohort ? 'HIGH' : 'NONE',
      same_window_valid: wv.same_window_valid, stale_or_cross_window_reason: wv.reason,
      payload_json: JSON.stringify({ status: r.status, source_kind: r.source_kind, provider: r.provider, first_bar_lag_sec: r.first_bar_lag_sec, created_at: num(r.created_at) }),
    });
    if (inCohort && wv.same_window_valid) joined.push(er); else unjoined.push(er);
  }
  return { joined, unjoined, health: { module_group: 'parser_session', status: joined.length ? 'clean' : 'empty', joined_n: joined.length, note: 'raw-observation ingestion session per signal (signal_id=observation id)' } };
}

function extractWorkerHealth({ rawDb, rawSha, startTs, endTs }) {
  if (!rawDb || !tableExists(rawDb, 'raw_path_observer_provider_state')) {
    return { joined: [], unjoined: [], health: { module_group: 'worker_health', status: 'missing', missing_reason: 'raw_path_observer_provider_state_absent__requires_worker_readmodel_export' } };
  }
  const rows = rawDb.prepare('SELECT provider, cooldown_until, last_error, updated_at FROM raw_path_observer_provider_state').all();
  const joined = []; const unjoined = [];
  for (const r of rows) {
    const wv = windowValidity(num(r.updated_at), startTs, endTs);
    const er = evidenceRow({
      module_group: 'worker_health', window_start_ts: startTs, window_end_ts: endTs, evidence_ts: num(r.updated_at),
      evidence_source: 'raw_path_observer_provider_state', evidence_source_sha256: rawSha,
      join_key_type: 'window_global', join_confidence: 'LOW',
      same_window_valid: wv.same_window_valid, stale_or_cross_window_reason: wv.reason,
      payload_json: JSON.stringify({ provider: r.provider, cooldown_until: num(r.cooldown_until), last_error: r.last_error }),
    });
    if (wv.same_window_valid) joined.push(er); else unjoined.push(er);
  }
  const ok = joined.length > 0;
  return { joined, unjoined, health: { module_group: 'worker_health', status: ok ? 'partial' : 'stale', joined_n: joined.length,
    missing_reason: ok ? null : 'raw_path_observer_provider_state_present_but_updated_at_post_window__requires_in_window_worker_health_snapshot__window_global_LOW_join_only' } };
}

function extractTokenLevel({ sentDb, sentSha, cohortTokens, startTs, endTs, moduleGroup }) {
  if (!sentDb || !tableExists(sentDb, 'tokens')) {
    return { joined: [], unjoined: [], health: { module_group: moduleGroup, status: 'missing', missing_reason: `sentiment_arb_tokens_absent__requires_same_window_${moduleGroup}_readmodel_export` } };
  }
  const rows = sentDb.prepare('SELECT token_ca, symbol, rating, action, position_tier, auto_buy_enabled, decision_reasons, decision_timestamp FROM tokens').all();
  const joined = []; const unjoined = [];
  for (const r of rows) {
    const inCohort = cohortTokens.has(r.token_ca);
    const wv = windowValidity(num(r.decision_timestamp), startTs, endTs);
    const er = evidenceRow({
      module_group: moduleGroup, token_ca: r.token_ca, symbol: r.symbol, window_start_ts: startTs, window_end_ts: endTs,
      evidence_ts: num(r.decision_timestamp), evidence_source: 'sentiment_arb.tokens', evidence_source_sha256: sentSha,
      join_key_type: 'token_ca', join_confidence: inCohort && wv.same_window_valid ? 'MEDIUM' : (inCohort ? 'LOW' : 'NONE'),
      same_window_valid: wv.same_window_valid, stale_or_cross_window_reason: wv.reason ?? (inCohort ? null : 'token_ca_not_in_06-21_cohort'),
      payload_json: JSON.stringify({ rating: r.rating, action: r.action, position_tier: r.position_tier, decision_reasons: r.decision_reasons }),
    });
    if (inCohort && wv.same_window_valid) joined.push(er); else unjoined.push(er);
  }
  return { joined, unjoined, health: { module_group: moduleGroup, status: joined.length ? 'partial' : 'empty', joined_n: joined.length,
    missing_reason: joined.length ? null : `sentiment_arb_tokens_present_${rows.length}_rows_but_0_cohort_join_and_decision_timestamp_null__requires_same_window_${moduleGroup}_readmodel_export` } };
}

// Modules with no same-window structured source in the snapshot pack -> module-level missing (precise).
const NO_SOURCE_REASON = {
  gmgn_policy: 'no_gmgn_policy_readmodel_table_in_same_window_snapshots__gmgn_present_only_as_kline_provider__requires_gmgn_policy_decision_readmodel_export',
  source_resonance: 'no_source_resonance_readmodel_in_same_window_snapshots__appears_only_as_normalized_mode_label__requires_source_resonance_shadow_readmodel_export',
  scout_quality: 'no_structured_scout_quality_readmodel_in_same_window_snapshots__requires_scout_quality_readmodel_export',
  idempotency_write_path: 'no_idempotency_or_write_path_table_in_same_window_snapshots__requires_sqlite_write_coordinator_audit_export',
  training_manifest: 'no_feature_or_training_manifest_in_same_window_snapshots__requires_feature_training_manifest_export',
  detector_calibration: 'lifecycle_strategy_results_present_but_shadow_strategy_outcomes_only_no_detector_model_version_or_calibration__requires_detector_markov_calibration_export',
  holdout_negative_controls: 'no_governance_holdout_readmodel_in_same_window_snapshots__requires_v27_promotion_holdout_negative_control_export',
  evidence_conflict_aging: 'no_evidence_conflict_aging_readmodel_in_same_window_snapshots__requires_evidence_conflict_aging_export',
  assumptions_false_negative_budget: 'no_assumptions_fn_budget_readmodel_in_same_window_snapshots__requires_assumptions_false_negative_budget_export',
};

function buildExport(args) {
  const startTs = num(args.windowStartTs); const endTs = num(args.windowEndTs);
  if (startTs == null || endTs == null || startTs >= endTs) throw new Error('--window-start-ts/--window-end-ts required and start<end (ambiguous window refused)');
  const rows = fs.readFileSync(path.resolve(args.fullnetRow), 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse);
  const cohortKeys = new Set(rows.map((r) => signalKey(r.token_ca, r.signal_ts)));
  const cohortTokens = new Set(rows.map((r) => r.token_ca));
  const rawPath = args.rawSignalOutcomesDb; const sentPath = args.sentimentDb;
  const rawSha = rawPath ? sha256File(rawPath) : null; const sentSha = sentPath ? sha256File(sentPath) : null;
  const rawDb = rawPath && fs.existsSync(rawPath) ? openRo(rawPath) : null;
  const sentDb = sentPath && fs.existsSync(sentPath) ? openRo(sentPath) : null;
  try {
    const results = {
      parser_session: extractParserSession({ rawDb, rawSha, cohortKeys, startTs, endTs }),
      worker_health: extractWorkerHealth({ rawDb, rawSha, startTs, endTs }),
      token_memory: extractTokenLevel({ sentDb, sentSha, cohortTokens, startTs, endTs, moduleGroup: 'token_memory' }),
      watchlist: extractTokenLevel({ sentDb, sentSha, cohortTokens, startTs, endTs, moduleGroup: 'watchlist' }),
    };
    for (const m of ALL_MODULES) {
      if (results[m]) continue;
      results[m] = { joined: [], unjoined: [], health: { module_group: m, status: 'missing', missing_reason: NO_SOURCE_REASON[m] || `no_same_window_source_for_${m}__requires_runtime_readmodel_export` } };
    }
    const joined = ALL_MODULES.flatMap((m) => results[m].joined);
    const unjoined = ALL_MODULES.flatMap((m) => results[m].unjoined);
    // deterministic order
    const ord = (a, b) => `${a.module_group}|${a.token_ca}|${a.signal_ts}|${a.evidence_source}`.localeCompare(`${b.module_group}|${b.token_ca}|${b.signal_ts}|${b.evidence_source}`);
    joined.sort(ord); unjoined.sort(ord);
    const health = ALL_MODULES.map((m) => results[m].health);
    const covered = health.filter((h) => h.status === 'clean' || (h.joined_n > 0)).map((h) => h.module_group);
    const summary = {
      schema_version: 'runtime_readmodel_summary.v1',
      window_start_ts: startTs, window_end_ts: endTs, cohort_signals: rows.length,
      joined_evidence_n: joined.length, unjoined_evidence_n: unjoined.length,
      module_health: health,
      modules_with_same_window_joined_evidence: covered,
      note: 'Research-only same-window runtime readmodel export. Joined = in-cohort + same_window_valid. Cross-window/post-window/absent => unjoined or module missing_reason. 06-19 mirrors intentionally not read.',
    };
    const manifest = {
      schema_version: 'runtime_readmodel_source_manifest.v1',
      sources: [
        rawPath ? { source: 'raw_signal_outcomes.snapshot.db', path: path.resolve(rawPath), sha256: rawSha, tables_used: ['raw_signal_observations', 'raw_path_observer_provider_state'] } : null,
        sentPath ? { source: 'sentiment_arb.snapshot.db', path: path.resolve(sentPath), sha256: sentSha, tables_used: ['tokens'] } : null,
      ].filter(Boolean),
      modules_without_source: ALL_MODULES.filter((m) => results[m].health.status === 'missing'),
    };
    return { joined, unjoined, summary, manifest, health };
  } finally { if (rawDb) rawDb.close(); if (sentDb) sentDb.close(); }
}

function writeExport(out, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const w = (n, o) => { const p = path.join(outDir, n); fs.writeFileSync(p, `${JSON.stringify(o, null, 2)}\n`); return p; };
  const wj = (n, arr) => { const p = path.join(outDir, n); fs.writeFileSync(p, arr.map((r) => JSON.stringify(r)).join('\n') + (arr.length ? '\n' : '')); return p; };
  return {
    windowPath: wj('runtime-readmodel-window.jsonl', out.joined),
    unjoinedPath: wj('runtime-readmodel-unjoined-records.jsonl', out.unjoined),
    summaryPath: w('runtime-readmodel-summary.json', out.summary),
    manifestPath: w('runtime-readmodel-source-manifest.json', out.manifest),
    healthPath: w('runtime-readmodel-export-health.json', { schema_version: 'runtime_readmodel_export_health.v1', modules: out.health }),
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = {};
  const map = { '--window-start-ts': 'windowStartTs', '--window-end-ts': 'windowEndTs', '--fullnet-row': 'fullnetRow', '--raw-signal-outcomes-db': 'rawSignalOutcomesDb', '--sentiment-db': 'sentimentDb', '--out-dir': 'outDir' };
  for (let i = 0; i < argv.length; i += 1) { const a = argv[i]; if (a === '--help' || a === '-h') { args.help = true; continue; } if (map[a]) { args[map[a]] = argv[i + 1]; i += 1; continue; } throw new Error(`Unknown argument: ${a}`); }
  return args;
}

function runCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) { console.log('Usage: node scripts/build-runtime-readmodel-window-export.js --window-start-ts <s> --window-end-ts <s> --fullnet-row <row.jsonl> --raw-signal-outcomes-db <db> --sentiment-db <db> --out-dir <dir>  (NODE_PATH=.../node_modules)'); return { help: true }; }
  if (!args.outDir) throw new Error('--out-dir is required');
  if (!args.fullnetRow) throw new Error('--fullnet-row is required');
  const out = buildExport(args);
  const written = writeExport(out, path.resolve(args.outDir));
  console.log(JSON.stringify({ ok: true, joined_n: out.joined.length, unjoined_n: out.unjoined.length, modules_with_evidence: out.summary.modules_with_same_window_joined_evidence, paths: written }, null, 2));
  return { out, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export { windowValidity, extractParserSession, extractWorkerHealth, extractTokenLevel, buildExport, ALL_MODULES, NO_SOURCE_REASON };
