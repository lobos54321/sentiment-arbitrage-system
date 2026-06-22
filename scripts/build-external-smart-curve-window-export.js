#!/usr/bin/env node
// External smart_money + pump.fun curve export (plan §15.27, Goal 9, script 1).
// Research-only: joins ALREADY-DOWNLOADED same-window external exports (wallet/KOL + on-chain curve)
// for smart_money and curve_pumpfun. No live calls. If an export is absent / cross-window / schema-invalid,
// the module ends EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON carrying the exact bounded-export spec.
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { pathToFileURL } from 'url';

const SKEW_SEC = 300;
const REQUIRED = {
  smart_money: ['smart_wallet_buy_count', 'smart_wallet_sell_count', 'smart_wallet_net_sol', 'smart_wallet_unique_n', 'wallet_signal_score', 'wallet_signal_direction'],
  curve_pumpfun: ['curve_platform', 'bonding_curve_address', 'bonding_curve_progress_pct', 'virtual_sol_reserves', 'migration_state', 'curve_stage'],
};
const FINAL_REASON = {
  smart_money: 'FINAL: no same-window smart-money/wallet/KOL export present (no --smart-money-export, none frozen on disk); requires a bounded read-only Zeabur/API export of per-token wallet buy/sell influence (smart_wallet_buy_count/sell_count/net_sol/unique_n/top_wallet_concentration, kol_buy_count/sell_count, wallet_signal_score/direction) over [window_start_ts,window_end_ts]; no live call made',
  curve_pumpfun: 'FINAL: no same-window pump.fun curve decode present (no --curve-pumpfun-export; the 06-18 curve-stage artifacts are cross-window + contract-level, not per-token decode); requires a bounded on-chain/Helius export of per-token bonding-curve (curve_platform/bonding_curve_address/bonding_curve_progress_pct/virtual+real reserves/migration_state/curve_stage) over the window; no live call made',
};

function num(v) { if (v == null || v === '') return null; const n = Number(v); if (!Number.isFinite(n)) return null; return n > 1e12 ? Math.floor(n / 1000) : n; }
function rnd(v) { const n = num(v); return n == null ? null : Math.round(n); }
function signalKey(t, s) { return `${t}|${rnd(s) ?? 0}`; }
function sha256File(p) { try { return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex'); } catch { return null; } }
function readJsonl(p) { return fs.readFileSync(p, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse); }

// Join one external export (if present) for a module. Returns {joined, unjoined, status, missing_reason, sha, count}.
function joinExternal(moduleGroup, exportPath, cohortKeys, cohortTokens, startTs, endTs) {
  if (!exportPath || !fs.existsSync(exportPath)) {
    return { joined: [], unjoined: [], status: 'EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON', missing_reason: FINAL_REASON[moduleGroup], sha: null, count: 0 };
  }
  let rows;
  try { rows = readJsonl(exportPath); } catch (e) { return { joined: [], unjoined: [], status: 'EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON', missing_reason: `FINAL: external export unreadable (${e.message.slice(0, 60)})`, sha: null, count: 0 }; }
  const sha = sha256File(exportPath);
  const req = REQUIRED[moduleGroup];
  const joined = []; const unjoined = [];
  for (const r of rows) {
    const token = r.token_ca; const sigTs = num(r.signal_ts);
    const ev = num(r.evidence_ts) ?? sigTs;
    const sw = ev != null && ev >= startTs - SKEW_SEC && ev <= endTs + SKEW_SEC;
    const fieldsPresent = req.every((f) => r[f] !== undefined && r[f] !== null);
    const exactKey = sigTs != null && cohortKeys.has(signalKey(token, sigTs));
    const tokenOnly = !exactKey && cohortTokens.has(token);
    const conf = exactKey ? 'HIGH' : (tokenOnly ? 'LOW' : 'NONE');
    const rec = {
      schema_version: 'external_smart_curve_window.v1', module_group: moduleGroup, token_ca: token ?? null, symbol: r.symbol ?? null,
      signal_ts: sigTs, source_id: r.source_id ?? null, lifecycle_id: r.lifecycle_id ?? null, window_start_ts: startTs, window_end_ts: endTs,
      evidence_ts: ev, same_window_valid: sw, join_confidence: conf, evidence_source: moduleGroup === 'smart_money' ? 'external_smart_money_export' : 'external_curve_pumpfun_export',
      evidence_source_sha256: sha, payload_json: JSON.stringify(Object.fromEntries(req.concat(['top_wallet_concentration', 'kol_buy_count', 'kol_sell_count', 'real_sol_reserves', 'migrated_pool_address', 'curve_stage_confidence']).filter((f) => f in r).map((f) => [f, r[f]]))),
      missing_reason: fieldsPresent ? null : `required fields missing: ${req.filter((f) => r[f] == null).join(',')}`,
    };
    // cover only HIGH/MEDIUM + same-window + required fields present; token-only LOW never covers
    if (sw && (conf === 'HIGH' || conf === 'MEDIUM') && fieldsPresent) joined.push(rec); else unjoined.push(rec);
  }
  const status = joined.length ? 'EXTERNAL_EVIDENCE_JOINED'
    : (rows.length === 0 ? 'EXTERNAL_EMPTY_BUT_VALID' : 'EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON');
  const missing_reason = joined.length ? null
    : (rows.length === 0 ? null : `FINAL: external export present (${rows.length} rows) but none joined same-window HIGH/MEDIUM with required fields (cross-window / token-only LOW / missing fields); ${FINAL_REASON[moduleGroup]}`);
  return { joined, unjoined, status, missing_reason, sha, count: rows.length };
}

function buildExternalExport(args) {
  const startTs = num(args.windowStartTs); const endTs = num(args.windowEndTs);
  if (startTs == null || endTs == null || startTs >= endTs) throw new Error('--window-start-ts/--window-end-ts required and start<end');
  const cohort = fs.readFileSync(path.resolve(args.fullnetRow), 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse);
  const cohortKeys = new Set(cohort.map((r) => signalKey(r.token_ca, r.signal_ts)));
  const cohortTokens = new Set(cohort.map((r) => r.token_ca));
  const sm = joinExternal('smart_money', args.smartMoneyExport, cohortKeys, cohortTokens, startTs, endTs);
  const cv = joinExternal('curve_pumpfun', args.curvePumpfunExport, cohortKeys, cohortTokens, startTs, endTs);
  const joined = [...sm.joined, ...cv.joined].sort((a, b) => `${a.module_group}|${a.token_ca}|${a.signal_ts}`.localeCompare(`${b.module_group}|${b.token_ca}|${b.signal_ts}`));
  const unjoined = [...sm.unjoined, ...cv.unjoined];
  const health = { schema_version: 'external_smart_curve_health.v1', window_start_ts: startTs, window_end_ts: endTs, modules: [
    { module_group: 'smart_money', status: sm.status, joined_n: sm.joined.length, source_rows: sm.count, missing_reason: sm.missing_reason },
    { module_group: 'curve_pumpfun', status: cv.status, joined_n: cv.joined.length, source_rows: cv.count, missing_reason: cv.missing_reason },
  ], covered_modules: [['smart_money', sm], ['curve_pumpfun', cv]].filter(([, x]) => x.joined.length > 0).map(([m]) => m) };
  const manifest = { schema_version: 'external_smart_curve_source_manifest.v1', window_start_ts: startTs, window_end_ts: endTs, sources: [
    args.smartMoneyExport ? { module: 'smart_money', path: path.resolve(args.smartMoneyExport), sha256: sm.sha, rows: sm.count } : { module: 'smart_money', path: null, note: 'no --smart-money-export provided' },
    args.curvePumpfunExport ? { module: 'curve_pumpfun', path: path.resolve(args.curvePumpfunExport), sha256: cv.sha, rows: cv.count } : { module: 'curve_pumpfun', path: null, note: 'no --curve-pumpfun-export provided' },
  ] };
  const contractReport = { schema_version: 'external_smart_curve_contract_report.v1', modules: [
    { module_group: 'smart_money', required_fields: REQUIRED.smart_money, present: sm.joined.length > 0, final_reason: sm.missing_reason },
    { module_group: 'curve_pumpfun', required_fields: REQUIRED.curve_pumpfun, present: cv.joined.length > 0, final_reason: cv.missing_reason },
  ] };
  const summary = { schema_version: 'external_smart_curve_summary.v1', window_start_ts: startTs, window_end_ts: endTs, joined_n: joined.length, unjoined_n: unjoined.length, module_status: { smart_money: sm.status, curve_pumpfun: cv.status }, note: 'Research-only same-window external evidence join. No live calls. Absent/cross-window/token-only-LOW => FINAL external-export reason.' };
  return { joined, unjoined, health, manifest, contractReport, summary };
}

function writeOut(out, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const w = (n, o) => { const p = path.join(outDir, n); fs.writeFileSync(p, `${JSON.stringify(o, null, 2)}\n`); return p; };
  const wj = (n, a) => { const p = path.join(outDir, n); fs.writeFileSync(p, a.map((r) => JSON.stringify(r)).join('\n') + (a.length ? '\n' : '')); return p; };
  return {
    windowPath: wj('external-smart-curve-window.jsonl', out.joined),
    unjoinedPath: wj('external-smart-curve-unjoined.jsonl', out.unjoined),
    summaryPath: w('external-smart-curve-summary.json', out.summary),
    manifestPath: w('external-smart-curve-source-manifest.json', out.manifest),
    healthPath: w('external-smart-curve-health.json', out.health),
    contractPath: w('external-smart-curve-contract-report.json', out.contractReport),
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = {}; const map = { '--window-start-ts': 'windowStartTs', '--window-end-ts': 'windowEndTs', '--fullnet-row': 'fullnetRow', '--smart-money-export': 'smartMoneyExport', '--curve-pumpfun-export': 'curvePumpfunExport', '--out-dir': 'outDir' };
  for (let i = 0; i < argv.length; i += 1) { const a = argv[i]; if (a === '--help' || a === '-h') { args.help = true; continue; } if (map[a]) { args[map[a]] = argv[i + 1]; i += 1; continue; } throw new Error(`Unknown argument: ${a}`); }
  return args;
}

function runCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) { console.log('Usage: node scripts/build-external-smart-curve-window-export.js --window-start-ts <s> --window-end-ts <s> --fullnet-row <row.jsonl> [--smart-money-export <p>] [--curve-pumpfun-export <p>] --out-dir <dir>'); return { help: true }; }
  if (!args.outDir || !args.fullnetRow) throw new Error('--fullnet-row and --out-dir required');
  const out = buildExternalExport(args);
  const written = writeOut(out, path.resolve(args.outDir));
  console.log(JSON.stringify({ ok: true, module_status: out.summary.module_status, joined_n: out.joined.length, covered_modules: out.health.covered_modules, paths: written }, null, 2));
  return { out, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export { joinExternal, buildExternalExport, REQUIRED, FINAL_REASON };
