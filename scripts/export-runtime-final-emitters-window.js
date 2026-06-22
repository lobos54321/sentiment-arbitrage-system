#!/usr/bin/env node
// Goal 10: bounded same-window export contract for the 6 FINAL runtime emitters.
// Research-only. This validates an already-produced JSONL export; it does not call live services.
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

const FINAL_MODULES = [
  'gmgn_policy',
  'source_resonance',
  'worker_health',
  'training_manifest',
  'holdout_negative_controls',
  'assumptions_false_negative_budget',
];

const REQUIRED_FIELDS = {
  gmgn_policy: ['gmgn_policy_decision', 'gmgn_policy_reason', 'gmgn_policy_source', 'gmgn_policy_version'],
  source_resonance: ['gmgn_first_seen_ts', 'gmgn_last_seen_ts', 'lead_time_sec', 'resonance_source', 'resonance_score', 'timestamp_valid'],
  worker_health: ['worker_name', 'worker_status', 'heartbeat_ts', 'provider_status', 'error_count_window', 'degraded_reason'],
  training_manifest: ['manifest_id', 'feature_schema_version', 'model_or_ruleset_version', 'generated_at_ts', 'training_window_start_ts', 'training_window_end_ts'],
  holdout_negative_controls: ['holdout_id', 'holdout_window_start_ts', 'holdout_window_end_ts', 'negative_control_name', 'control_result', 'leakage_check_pass'],
  assumptions_false_negative_budget: ['budget_id', 'false_negative_budget_n', 'false_negative_budget_pct', 'observed_false_negative_n', 'budget_status', 'assumption_version'],
};

const FINAL_REASON = {
  gmgn_policy: 'FINAL: no same-window gmgn_policy decision emitter export; requires gmgn_policy_decision/reason/source/version per token+signal',
  source_resonance: 'FINAL: no same-window source_resonance emitter export; requires gmgn_first_seen_ts/gmgn_last_seen_ts/lead_time_sec/resonance fields per token+signal',
  worker_health: 'FINAL: no same-window worker_health emitter export; requires worker heartbeat/provider/error state over the signal window',
  training_manifest: 'FINAL: no same-window training_manifest emitter export; requires feature schema/ruleset/training manifest identity',
  holdout_negative_controls: 'FINAL: no same-window holdout_negative_controls emitter export; requires holdout cohort + negative-control result',
  assumptions_false_negative_budget: 'FINAL: no same-window assumptions_false_negative_budget emitter export; requires FN-budget/assumption ledger',
};

function num(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  return n > 1e12 ? Math.floor(n / 1000) : n;
}

function roundTs(v) {
  const n = num(v);
  return n == null ? null : Math.round(n);
}

function signalKey(tokenCa, signalTs) {
  return `${tokenCa || ''}|${roundTs(signalTs) ?? 0}`;
}

function readJsonl(file) {
  if (!file || !fs.existsSync(file)) return [];
  const text = fs.readFileSync(file, 'utf8').trim();
  return text ? text.split('\n').filter(Boolean).map((line) => JSON.parse(line)) : [];
}

function readFullnetRows(file) {
  return readJsonl(file).map((row) => ({
    token_ca: row.token_ca,
    signal_ts: roundTs(row.signal_ts),
    premium_signal_id: row.premium_signal_id ?? row.signal_id ?? null,
  }));
}

function payload(row) {
  if (row.payload_json && typeof row.payload_json === 'string') {
    try { return JSON.parse(row.payload_json); } catch { return {}; }
  }
  return row.payload && typeof row.payload === 'object' ? row.payload : row;
}

function missingFields(row) {
  const p = payload(row);
  const base = ['token_ca', 'signal_ts', 'module_group', 'evidence_ts', 'window_start_ts', 'window_end_ts', 'join_confidence', 'payload_hash', 'source'];
  const moduleFields = REQUIRED_FIELDS[row.module_group] || [];
  return [...base, ...moduleFields].filter((field) => {
    const value = row[field] ?? p[field];
    return value == null || value === '';
  });
}

function rejectReason(row, cohortKeys, startTs, endTs) {
  if (!FINAL_MODULES.includes(row.module_group)) return `unknown_module:${row.module_group}`;
  const missing = missingFields(row);
  if (missing.length) return `missing_required_fields:${missing.join(',')}`;
  const evidenceTs = roundTs(row.evidence_ts);
  if (evidenceTs == null || evidenceTs < startTs || evidenceTs > endTs) return 'cross_window_or_missing_evidence_ts';
  if (roundTs(row.window_start_ts) !== startTs || roundTs(row.window_end_ts) !== endTs) return 'declared_window_mismatch';
  if (!['HIGH', 'MEDIUM'].includes(String(row.join_confidence))) return `join_confidence_not_coverable:${row.join_confidence}`;
  if (!cohortKeys.has(signalKey(row.token_ca, row.signal_ts))) return 'token_signal_not_in_fullnet_cohort';
  return null;
}

function normalizeJoined(row, startTs, endTs) {
  const p = payload(row);
  const modulePayload = {};
  for (const field of REQUIRED_FIELDS[row.module_group]) modulePayload[field] = row[field] ?? p[field];
  return {
    schema_version: 'runtime_final_emitter_window.v1',
    module_group: row.module_group,
    token_ca: row.token_ca,
    signal_ts: roundTs(row.signal_ts),
    premium_signal_id: row.premium_signal_id ?? null,
    window_start_ts: startTs,
    window_end_ts: endTs,
    evidence_ts: roundTs(row.evidence_ts),
    source: row.source,
    payload_hash: row.payload_hash,
    join_confidence: row.join_confidence,
    emitter_status: 'COVERED_WITH_SAME_WINDOW_PROOF',
    payload_json: JSON.stringify(modulePayload),
  };
}

function buildFinalEmitterExport(args) {
  const startTs = roundTs(args.windowStartTs);
  const endTs = roundTs(args.windowEndTs);
  if (startTs == null || endTs == null || startTs >= endTs) throw new Error('--window-start-ts/--window-end-ts required and start<end');
  if (!args.fullnetRow) throw new Error('--fullnet-row is required');
  const cohort = readFullnetRows(path.resolve(args.fullnetRow));
  const cohortKeys = new Set(cohort.map((row) => signalKey(row.token_ca, row.signal_ts)));
  const inputRows = readJsonl(args.runtimeFinalExport ? path.resolve(args.runtimeFinalExport) : null);
  const joined = [];
  const unjoined = [];
  for (const row of inputRows) {
    const reason = rejectReason(row, cohortKeys, startTs, endTs);
    if (reason) {
      unjoined.push({ ...row, reject_reason: reason });
    } else {
      joined.push(normalizeJoined(row, startTs, endTs));
    }
  }
  joined.sort((a, b) => `${a.module_group}|${a.token_ca}|${a.signal_ts}`.localeCompare(`${b.module_group}|${b.token_ca}|${b.signal_ts}`));
  unjoined.sort((a, b) => `${a.module_group}|${a.token_ca}|${a.signal_ts}|${a.reject_reason}`.localeCompare(`${b.module_group}|${b.token_ca}|${b.signal_ts}|${b.reject_reason}`));
  const covered = new Set(joined.map((row) => row.module_group));
  const modules = FINAL_MODULES.map((module) => ({
    module_group: module,
    status: covered.has(module) ? 'COVERED_WITH_SAME_WINDOW_PROOF' : 'FINAL_EMITTER_MISSING_OR_INVALID_WITH_EXACT_REASON',
    joined_n: joined.filter((row) => row.module_group === module).length,
    missing_reason: covered.has(module) ? null : FINAL_REASON[module],
    required_fields: REQUIRED_FIELDS[module],
  }));
  return {
    joined,
    unjoined,
    summary: {
      schema_version: 'runtime_final_emitter_summary.v1',
      window_start_ts: startTs,
      window_end_ts: endTs,
      cohort_signals: cohort.length,
      input_rows: inputRows.length,
      joined_n: joined.length,
      unjoined_n: unjoined.length,
      covered_modules: [...covered].sort(),
      still_blocked_modules: FINAL_MODULES.filter((module) => !covered.has(module)),
    },
    health: { schema_version: 'runtime_final_emitter_health.v1', modules },
    contract: {
      schema_version: 'runtime_final_emitter_contract_report.v1',
      note: 'Only same-window HIGH/MEDIUM per-(token,signal) rows with all module fields cover a FINAL runtime blocker.',
      modules: FINAL_MODULES.map((module) => ({ module_group: module, required_fields: REQUIRED_FIELDS[module], final_reason_if_missing: FINAL_REASON[module] })),
    },
    manifest: {
      schema_version: 'runtime_final_emitter_source_manifest.v1',
      runtime_final_export: args.runtimeFinalExport ? path.resolve(args.runtimeFinalExport) : null,
      fullnet_row: path.resolve(args.fullnetRow),
      live_calls_made: false,
    },
  };
}

function writeFinalEmitterExport(out, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const writeJson = (name, value) => {
    const file = path.join(outDir, name);
    fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
    return file;
  };
  const writeJsonl = (name, rows) => {
    const file = path.join(outDir, name);
    fs.writeFileSync(file, rows.map((row) => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : ''));
    return file;
  };
  return {
    windowPath: writeJsonl('runtime-final-emitter-window.jsonl', out.joined),
    unjoinedPath: writeJsonl('runtime-final-emitter-unjoined.jsonl', out.unjoined),
    summaryPath: writeJson('runtime-final-emitter-summary.json', out.summary),
    healthPath: writeJson('runtime-final-emitter-health.json', out.health),
    contractPath: writeJson('runtime-final-emitter-contract-report.json', out.contract),
    manifestPath: writeJson('runtime-final-emitter-source-manifest.json', out.manifest),
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = {};
  const map = {
    '--window-start-ts': 'windowStartTs',
    '--window-end-ts': 'windowEndTs',
    '--fullnet-row': 'fullnetRow',
    '--runtime-final-export': 'runtimeFinalExport',
    '--out-dir': 'outDir',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--help' || arg === '-h') { args.help = true; continue; }
    if (!map[arg]) throw new Error(`Unknown argument: ${arg}`);
    args[map[arg]] = argv[i + 1];
    i += 1;
  }
  return args;
}

function runCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    console.log('Usage: node scripts/export-runtime-final-emitters-window.js --window-start-ts <s> --window-end-ts <s> --fullnet-row <row.jsonl> [--runtime-final-export <jsonl>] --out-dir <dir>');
    return { help: true };
  }
  if (!args.outDir) throw new Error('--out-dir is required');
  const out = buildFinalEmitterExport(args);
  const written = writeFinalEmitterExport(out, path.resolve(args.outDir));
  console.log(JSON.stringify({ ok: true, joined_n: out.joined.length, unjoined_n: out.unjoined.length, covered_modules: out.summary.covered_modules, paths: written }, null, 2));
  return { out, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (error) { console.error(error.stack || error.message); process.exit(1); }
}

export {
  FINAL_MODULES,
  REQUIRED_FIELDS,
  FINAL_REASON,
  missingFields,
  rejectReason,
  buildFinalEmitterExport,
  writeFinalEmitterExport,
};
