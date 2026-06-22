#!/usr/bin/env node
// Runtime materializer REPAIR export (plan §15.25, Goal 8, script 1).
// Research-only: for the 9 modules still blocked after Goal 7, mine the STRUCTURED a-class decision
// sub-objects (joined with the SAME v1 matcher as has_decision) — scout_quality component score/grade,
// expected_rr_detail.rr_version/rr_grade (model/calibration), risk.opportunity_key/duplicate_of_event_id
// (idempotency/dedupe) — to materialize 3 of them; the other 6 get a FINAL materializer-specific reason.
// Never label-derived (uses structured object fields, not normalized_mode strings). No live read, no
// cross-window, no strategy/mode change, EV untouched.
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { pathToFileURL } from 'url';
import { normalizeSourceCohort, buildSignalMatcher, mergeDecisionEvents } from './build-live-fullnet-row-report.js';

const REPAIR_MODULES = [
  'gmgn_policy', 'source_resonance', 'scout_quality', 'idempotency_write_path', 'worker_health',
  'training_manifest', 'detector_calibration', 'holdout_negative_controls', 'assumptions_false_negative_budget',
];

// Modules with no honest structured same-window evidence even after mining decision sub-objects -> FINAL reason.
const FINAL_BLOCKED = {
  gmgn_policy: 'FINAL: no gmgn_policy source_component in same-window a-class decision events (components are matrix_evaluator/scout_quality/ath_*/smart_entry/lotto_*/revival_canary/markov_reclaim/...); gmgn is kline-provider only; requires runtime gmgn_policy decision component emit',
  source_resonance: 'FINAL: resonance-probe components fire (pre_pass_resonance_probe/revival_canary) but gmgn_first_seen_ts + lead_time_sec (gmgn-vs-telegram) are not emitted; requires runtime source_resonance lead-time emit',
  worker_health: 'FINAL: only raw_path_observer_provider_state with post-window updated_at; no in-window worker heartbeat/readmodel-refresh; requires runtime worker heartbeat emit',
  training_manifest: 'FINAL: no feature_snapshot_id/feature_schema_version/training_manifest_id/feature_vector_hash in any same-window source; requires runtime feature/training manifest emit',
  holdout_negative_controls: 'FINAL: only in-sample dog/dud computable; no out-of-sample holdout cohort/negative-control result; requires runtime holdout/negative-control emit',
  assumptions_false_negative_budget: 'FINAL: budget object carries daily_loss_budget_sol (loss cap) not a false-negative/missed-dog budget or assumption ledger; requires governance FN-budget/assumption emit',
};

function num(v) { if (v == null || v === '') return null; const n = Number(v); if (!Number.isFinite(n)) return null; return n > 1e12 ? Math.floor(n / 1000) : n; }
function readJson(p) { return JSON.parse(fs.readFileSync(path.resolve(p), 'utf8')); }
function sha256File(p) { try { return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex'); } catch { return null; } }
function tableRows(t) { return Array.isArray(t) ? t : (t && Array.isArray(t.rows) ? t.rows : []); }

function repairRow(o) {
  return {
    schema_version: 'runtime_materializer_repair.v1',
    module_group: o.module_group, token_ca: o.token_ca ?? null, signal_ts: o.signal_ts ?? null,
    window_start_ts: o.window_start_ts, window_end_ts: o.window_end_ts, evidence_ts: o.evidence_ts ?? null,
    repair_status: o.repair_status, join_confidence: o.join_confidence, same_window_valid: o.same_window_valid,
    evidence_source: o.evidence_source, payload_json: o.payload_json ?? null, missing_reason: o.missing_reason ?? null,
  };
}

function buildRepairExport(args) {
  const startTs = num(args.windowStartTs); const endTs = num(args.windowEndTs);
  if (startTs == null || endTs == null || startTs >= endTs) throw new Error('--window-start-ts/--window-end-ts required and start<end');
  const s2rJson = readJson(args.sourceToRaw); const s2r = Array.isArray(s2rJson) ? s2rJson : (s2rJson.rows || []);
  const cohort = normalizeSourceCohort(s2r);
  const match = buildSignalMatcher(cohort);
  const volumeByKey = new Map(); // source-to-raw volume_24h per signal (scout volume_score)
  for (const r of cohort) volumeByKey.set(r._key, { volume_24h: num(r.volume_24h), market_cap: num(r.market_cap) });
  const aev = readJson(args.aClassEvents); const apiEvents = aev.events || aev;
  const led = readJson(args.ledgerExport); const canon = tableRows(led?.tables?.a_class_decision_events);
  const events = mergeDecisionEvents(apiEvents, canon);
  // bucket matched events per cohort signal (same matcher as v1 has_decision)
  const byKey = new Map();
  for (const e of events) {
    const m = match(String(e.token_ca || ''), e.signal_ts, e.event_ts);
    if (!m) continue;
    if (!byKey.has(m.key)) byKey.set(m.key, []);
    byKey.get(m.key).push(e);
  }
  const joined = []; const counts = { scout_quality: 0, detector_calibration: 0, idempotency_write_path: 0 };
  for (const sig of cohort) {
    const evs = (byKey.get(sig._key) || []).sort((a, b) => (num(a.event_ts) ?? 0) - (num(b.event_ts) ?? 0));
    if (!evs.length) continue;
    const token = sig.token_ca; const sigTs = sig._signalTs;
    const best = evs[evs.length - 1];
    // scout_quality: structured scout_quality component event(s)
    const scoutEv = evs.find((e) => String(e.source_component || '') === 'scout_quality');
    if (scoutEv) {
      counts.scout_quality += 1;
      const risk = scoutEv.risk || best.risk || {};
      const vol = volumeByKey.get(sig._key) || {};
      joined.push(repairRow({
        module_group: 'scout_quality', token_ca: token, signal_ts: sigTs, window_start_ts: startTs, window_end_ts: endTs,
        evidence_ts: num(scoutEv.event_ts), repair_status: 'MATERIALIZED_AND_JOINED', join_confidence: 'HIGH', same_window_valid: true,
        evidence_source: 'a_class_decision_event[source_component=scout_quality]',
        payload_json: JSON.stringify({ scout_quality_score: num(scoutEv.score), scout_quality_grade: scoutEv.grade ?? null, scout_quality_block_reason: scoutEv.reason ?? scoutEv.source_reason ?? null, liquidity_score: num(risk.liquidity_usd), volume_score: vol.volume_24h ?? null, market_cap: vol.market_cap ?? null, quote_executable: risk.quote_clean_verified === true || risk.quote_clean_verified === 1 }),
      }));
    }
    // detector_calibration: expected_rr_detail.rr_version (model) + rr_grade (calibration bucket)
    const rrEv = evs.find((e) => e.expected_rr_detail && typeof e.expected_rr_detail === 'object' && (e.expected_rr_detail.rr_version != null || e.expected_rr_detail.rr_grade != null));
    if (rrEv) {
      counts.detector_calibration += 1;
      const d = rrEv.expected_rr_detail; const air = rrEv.ai_review || {};
      joined.push(repairRow({
        module_group: 'detector_calibration', token_ca: token, signal_ts: sigTs, window_start_ts: startTs, window_end_ts: endTs,
        evidence_ts: num(rrEv.event_ts), repair_status: 'MATERIALIZED_AND_JOINED', join_confidence: 'HIGH', same_window_valid: true,
        evidence_source: 'a_class_decision_event.expected_rr_detail',
        payload_json: JSON.stringify({ detector_name: 'expected_rr_matrix', model_version: d.rr_version ?? null, calibration_bucket: d.rr_grade ?? null, expected_rr: num(d.expected_rr), expected_precision_bucket: d.rr_grade ?? null, confidence_bucket: air.ai_grade ?? rrEv.grade ?? null }),
      }));
    }
    // idempotency_write_path: risk.opportunity_key + duplicate_of_event_id (dedupe) + decision id
    const idemEv = evs.find((e) => e.risk && typeof e.risk === 'object' && e.risk.opportunity_key != null);
    if (idemEv) {
      counts.idempotency_write_path += 1;
      const risk = idemEv.risk;
      const dup = risk.duplicate_of_event_id;
      joined.push(repairRow({
        module_group: 'idempotency_write_path', token_ca: token, signal_ts: sigTs, window_start_ts: startTs, window_end_ts: endTs,
        evidence_ts: num(idemEv.event_ts), repair_status: 'MATERIALIZED_AND_JOINED', join_confidence: 'HIGH', same_window_valid: true,
        evidence_source: 'a_class_decision_event.risk[opportunity_key,duplicate_of_event_id]',
        payload_json: JSON.stringify({ decision_event_id: idemEv.id ?? null, idempotency_key: risk.opportunity_key, dedupe_result: dup != null ? `duplicate_of_event_id:${dup}` : 'unique', write_path_status: dup != null ? 'deduped' : 'written', enqueue_attempted: false, order_attempted: false, fill_attempted: false, ledger_write_attempted: false }),
      }));
    }
  }
  joined.sort((a, b) => `${a.module_group}|${a.token_ca}|${a.signal_ts}`.localeCompare(`${b.module_group}|${b.token_ca}|${b.signal_ts}`));
  const materializedSet = new Set(['scout_quality', 'detector_calibration', 'idempotency_write_path']);
  const health = REPAIR_MODULES.map((m) => materializedSet.has(m)
    ? { module_group: m, status: counts[m] > 0 ? 'repaired_and_joined' : 'repaired_empty_valid', joined_n: counts[m] || 0, missing_reason: null }
    : { module_group: m, status: 'still_missing', joined_n: 0, missing_reason: FINAL_BLOCKED[m] });
  const contract = REPAIR_MODULES.map((m) => ({ module_group: m, repaired: materializedSet.has(m), joined_n: counts[m] || 0, final_reason: FINAL_BLOCKED[m] ?? null }));
  return {
    joined,
    health: { schema_version: 'runtime_materializer_repair_health.v1', window_start_ts: startTs, window_end_ts: endTs, modules: health, repaired_modules: [...materializedSet].filter((m) => counts[m] > 0) },
    manifest: { schema_version: 'runtime_materializer_repair_manifest.v1', window_start_ts: startTs, window_end_ts: endTs, sources: [
      { source: 'a-class-events-24h-complete.json', path: path.resolve(args.aClassEvents), sha256: sha256File(args.aClassEvents) },
      { source: 'canonical-ledger-window.json[a_class_decision_events]', path: path.resolve(args.ledgerExport), sha256: sha256File(args.ledgerExport) },
      { source: 'source-to-raw-rows.json', path: path.resolve(args.sourceToRaw), sha256: sha256File(args.sourceToRaw) },
    ], join: 'v1 buildSignalMatcher (exact signal_ts / bounded event_ts) — same join as has_decision' },
    contractReport: { schema_version: 'runtime_materializer_repair_contract_report.v1', modules: contract },
  };
}

function writeRepair(out, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const w = (n, o) => { const p = path.join(outDir, n); fs.writeFileSync(p, `${JSON.stringify(o, null, 2)}\n`); return p; };
  const wj = (n, a) => { const p = path.join(outDir, n); fs.writeFileSync(p, a.map((r) => JSON.stringify(r)).join('\n') + (a.length ? '\n' : '')); return p; };
  return {
    repairPath: wj('runtime-materializer-repair.jsonl', out.joined),
    unjoinedPath: wj('runtime-materializer-repair-unjoined.jsonl', []),
    healthPath: w('runtime-materializer-repair-health.json', out.health),
    manifestPath: w('runtime-materializer-repair-manifest.json', out.manifest),
    contractPath: w('runtime-materializer-repair-contract-report.json', out.contractReport),
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = {}; const map = { '--window-start-ts': 'windowStartTs', '--window-end-ts': 'windowEndTs', '--fullnet-row': 'fullnetRow', '--source-to-raw': 'sourceToRaw', '--a-class-events': 'aClassEvents', '--ledger-export': 'ledgerExport', '--runtime-materialized-dir': 'runtimeMaterializedDir', '--runtime-sources-dir': 'runtimeSourcesDir', '--out-dir': 'outDir' };
  for (let i = 0; i < argv.length; i += 1) { const a = argv[i]; if (a === '--help' || a === '-h') { args.help = true; continue; } if (map[a]) { args[map[a]] = argv[i + 1]; i += 1; continue; } throw new Error(`Unknown argument: ${a}`); }
  return args;
}

function runCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) { console.log('Usage: node scripts/build-runtime-materializer-repair-export.js --window-start-ts <s> --window-end-ts <s> --source-to-raw <s2r> --a-class-events <ev> --ledger-export <led> --out-dir <dir>'); return { help: true }; }
  if (!args.outDir || !args.sourceToRaw || !args.aClassEvents || !args.ledgerExport) throw new Error('--source-to-raw, --a-class-events, --ledger-export, --out-dir required');
  const out = buildRepairExport(args);
  const written = writeRepair(out, path.resolve(args.outDir));
  console.log(JSON.stringify({ ok: true, joined_n: out.joined.length, repaired_modules: out.health.repaired_modules, paths: written }, null, 2));
  return { out, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export { buildRepairExport, REPAIR_MODULES, FINAL_BLOCKED };
