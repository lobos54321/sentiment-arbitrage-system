#!/usr/bin/env node
'use strict';

/**
 * run-oos-daily-cycle.js
 *
 * ONE-COMMAND deterministic daily OOS accumulation cycle. It chains the
 * signed-off components end-to-end so a human (or a real OS cron / launchd job)
 * can run exactly ONE cycle per calendar day with no interactive agent:
 *
 *   pull fresh snapshot (curl + size + sqlite integrity, retry) -> frozen pack
 *   -> production_commit = `git ls-remote origin main` (prod auto-deploys main HEAD)
 *   -> run-oos-daily-operation.js PREPARE   (producer -> OOS selection -> oos.sql)
 *   -> [0 OOS candidates? -> DATA_INSUFFICIENT_WAIT, clean exit 0]
 *   -> run-dune-sql-export.py on oos.sql    (trades.jsonl + dune-manifest.json)
 *   -> validate-v10-curve-feature-trade-export.js (validation.json; out_of_window must be 0)
 *   -> run-oos-daily-operation.js INGEST    (--dune-* --force-smoke; coverage/leak/dune gates)
 *   -> read cumulative_counts.json:
 *        dog>=50 AND dud>=50 -> HALT: LOOKPOINT_READY_N50_AUDIT_REQUIRED (exit 10); AUC NOT read
 *        else                -> DAILY_ACCUMULATION_CONTINUE (exit 0)
 *
 * HARD GUARANTEES (never relaxed here):
 *   - AUC is never read or computed. `--look-point` / `--reveal-sealed-auc` are refused.
 *   - No strategy/gate/exit/size file is touched; no main/production write path is touched.
 *   - The cumulative table is mutated ONLY through the signed-off accumulator (via the orchestrator).
 *   - 1 cycle/day: if today's (UTC) out-dir already shows phase=ingested, it skips (no double count).
 *   - Fail-closed: any snapshot / Dune / validation / coverage / leak anomaly => non-zero exit, no bypass.
 *
 * KNOWN DEBT (for Codex review): the two-phase PREPARE->INGEST shares one out-dir, so INGEST
 * re-enters it via `--force-smoke`. production_commit is still supplied real, so NO real guard is
 * relaxed -- only the no-overwrite guard, which the 1/day idempotency check already protects. Proper
 * fix: add a dedicated `--resume` to run-oos-daily-operation.js. Until then this is the proven path
 * (used across the soak + formal day-1 cycles).
 *
 * NETWORK: the snapshot curl + Dune export need real network. Run in a normal shell (no sandbox).
 * zeabur can cold-start; the snapshot pull uses a generous timeout + retry.
 *
 * CONFIG (env, all optional, sane defaults):
 *   OOS_BASE_URL, OOS_DASHBOARD_TOKEN_FILE, OOS_DATAROOM, OOS_CUMULATIVE_DIR,
 *   OOS_TRAINING_TOKENS, DUNE_KEY_FILE, PYTHON
 *
 * USAGE:
 *   node scripts/run-oos-daily-cycle.js            # run one real cycle (1/day, fail-closed)
 *   node scripts/run-oos-daily-cycle.js --check    # preconditions + plan only, no pull/ingest, no side effects
 */

import fs from 'fs';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import { execFileSync, spawn } from 'child_process';

const SCRIPTS = path.dirname(new URL(import.meta.url).pathname);
const REPO = path.resolve(SCRIPTS, '..');
const NODE = process.execPath;
const E = process.env;

const BASE_URL = E.OOS_BASE_URL || 'https://sentiment-arbitrage.zeabur.app';
const TOKEN_FILE = E.OOS_DASHBOARD_TOKEN_FILE || '/tmp/sas-dashboard-token';
const DATAROOM = E.OOS_DATAROOM || '/Users/boliu/sas-data-room';
const CUM_DIR = E.OOS_CUMULATIVE_DIR || path.join(DATAROOM, 'oos-cumulative-sol-curve-unique-buyers');
const TRAINING = E.OOS_TRAINING_TOKENS || path.join(DATAROOM, 'chain-truth-recut-20260612T011545Z/oos-training-token-exclusion/training-tokens.txt');
const DUNE_KEY_FILE = E.DUNE_KEY_FILE || path.join(os.homedir(), '.dune_api_key');
const PYTHON = E.PYTHON || 'python3';

const ORCH = path.join(SCRIPTS, 'run-oos-daily-operation.js');
const DUNE_EXPORT = path.join(SCRIPTS, 'run-dune-sql-export.py');
const VALIDATOR = path.join(SCRIPTS, 'validate-v10-curve-feature-trade-export.js');
const PREREG = path.join(REPO, 'claudedocs/oos-sol-curve-unique-buyers-preregister.md');
const PREREG_LOCK = path.join(REPO, 'claudedocs/oos-sol-curve-unique-buyers-preregister.sha256');
const DUNE_TEMPLATE = path.join(SCRIPTS, 'oos-dune-trade-export.template.sql');

const N50 = 50;                                  // futility look point per class (dog AND dud)
const SNAPSHOT_MIN_BYTES = 60 * 1024 * 1024;     // valid snapshots have been ~64-71MB; smaller => truncated
const PULL_RETRIES = 3;
const PULL_TIMEOUT_S = 240;                       // generous: 64MB body + possible zeabur cold-start
const PULL_WALL_TIMEOUT_MS = (PULL_TIMEOUT_S + 30) * 1000;
const PULL_STALL_TIMEOUT_MS = 90 * 1000;
const DUNE_CHUNK_SIZE = Number(E.OOS_DUNE_CHUNK_SIZE || 3);
const DUNE_CHUNK_MAX_SPAN_S = Number(E.OOS_DUNE_CHUNK_MAX_SPAN_S || 3600);
const DUNE_CHUNK_MIN_SPAN_S = Number(E.OOS_DUNE_CHUNK_MIN_SPAN_S || 60);
const OPS_LOG = path.join(DATAROOM, 'oos-daily-ops-log.jsonl');

function die(msg) { console.error(`run-oos-daily-cycle: FAIL-CLOSED: ${msg}`); process.exit(2); }
function readJson(p) { return JSON.parse(fs.readFileSync(p, 'utf8')); }
function sha256File(p) { return fs.existsSync(p) ? crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex') : null; }
function utcDate() { return new Date().toISOString().slice(0, 10).replace(/-/g, ''); }            // YYYYMMDD
function utcStamp() { return new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z'); } // YYYYMMDDTHHMMSSZ
function exists(p) { return fs.existsSync(p); }

function runStage(label, cmd, args, opts = {}) {
  // None of these stages receive the dashboard token, so args are safe to echo.
  console.error(`\n[stage] ${label}: ${path.basename(cmd)} ${args.join(' ')}`);
  if (opts.captureOutput) {
    try {
      const out = execFileSync(cmd, args, { encoding: 'utf8', stdio: 'pipe' });
      if (out) process.stderr.write(out);
    } catch (error) {
      if (error.stdout) process.stderr.write(String(error.stdout));
      if (error.stderr) process.stderr.write(String(error.stderr));
      throw error;
    }
    return;
  }
  execFileSync(cmd, args, { stdio: 'inherit' });
}

function curlConfigQuote(value) {
  return String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '');
}

function runCurlWithWatchdog(curlConfigPath, dbPath) {
  return new Promise((resolve, reject) => {
    const child = spawn('curl', ['--config', curlConfigPath], { stdio: ['ignore', 'ignore', 'pipe'] });
    const startedAt = Date.now();
    let stderr = '';
    let settled = false;
    let killReason = '';
    let lastSize = 0;
    let lastProgressAt = startedAt;

    try {
      lastSize = exists(dbPath) ? fs.statSync(dbPath).size : 0;
    } catch {
      lastSize = 0;
    }

    const killFor = (reason) => {
      if (settled || killReason) return;
      killReason = reason;
      try { child.kill('SIGKILL'); } catch { /* process may already be gone */ }
    };

    const timer = setInterval(() => {
      let size = 0;
      try { size = exists(dbPath) ? fs.statSync(dbPath).size : 0; } catch { size = 0; }
      if (size !== lastSize) {
        lastSize = size;
        lastProgressAt = Date.now();
      }
      const elapsed = Date.now() - startedAt;
      const idle = Date.now() - lastProgressAt;
      if (elapsed > PULL_WALL_TIMEOUT_MS) {
        killFor(`wall timeout after ${Math.round(elapsed / 1000)}s`);
      } else if (elapsed > 30_000 && idle > PULL_STALL_TIMEOUT_MS) {
        killFor(`download stalled for ${Math.round(idle / 1000)}s at ${lastSize} bytes`);
      }
    }, 5_000);

    child.stderr.on('data', (chunk) => { stderr += chunk.toString('utf8'); });
    child.on('error', (err) => {
      settled = true;
      clearInterval(timer);
      reject(err);
    });
    child.on('close', (code, signal) => {
      settled = true;
      clearInterval(timer);
      if (code === 0) {
        resolve();
        return;
      }
      const reason = killReason || `curl exited code=${code} signal=${signal || 'none'}`;
      reject(new Error(`${reason}: ${stderr.slice(0, 160)}`));
    });
  });
}

function remoteMainHead() {
  try {
    const out = execFileSync('git', ['-C', REPO, 'ls-remote', 'origin', 'main'], { encoding: 'utf8', timeout: 30000 }).trim();
    const sha = (out.split(/\s+/)[0] || '').trim();
    return /^[0-9a-f]{40}$/.test(sha) ? sha : null;
  } catch { return null; }
}

function appendOpsLog(result) {
  try { fs.appendFileSync(OPS_LOG, `${JSON.stringify(result)}\n`); } catch { /* logging is best-effort */ }
}

function readJsonlRows(p) {
  if (!exists(p)) return [];
  return fs.readFileSync(p, 'utf8').trim().split('\n').filter(Boolean).map((line) => JSON.parse(line));
}

function writeJsonlRows(p, rows) {
  fs.writeFileSync(p, rows.map((row) => JSON.stringify(sortObjectKeys(row), null, 0)).join('\n') + (rows.length ? '\n' : ''));
}

function sortObjectKeys(row) {
  if (!row || typeof row !== 'object' || Array.isArray(row)) return row;
  return Object.keys(row).sort().reduce((out, key) => { out[key] = row[key]; return out; }, {});
}

function parseSignalWindowsCsv(csvPath) {
  const lines = fs.readFileSync(csvPath, 'utf8').trim().split(/\r?\n/).filter(Boolean);
  const header = lines.shift()?.split(',') || [];
  return lines.map((line) => {
    const cells = line.split(',');
    const row = {};
    header.forEach((key, idx) => { row[key] = cells[idx] ?? ''; });
    return row;
  });
}

function writeSignalWindowsCsv(csvPath, rows) {
  const header = ['window_id', 'token_ca', 'signal_ts', 'window_start_ts', 'window_end_ts', 'label', 'return_domain', 'effective_tier', 'query_start_ts', 'query_end_ts'];
  fs.writeFileSync(csvPath, `${header.join(',')}\n${rows.map((row) => header.map((key) => row[key]).join(',')).join('\n')}\n`);
}

function sqlQuote(value) {
  return String(value).replace(/'/g, "''");
}

function writeDuneSqlForWindows(sqlPath, rows) {
  const template = fs.readFileSync(DUNE_TEMPLATE, 'utf8');
  const values = rows.map((r) => (
    `    ('${sqlQuote(r.window_id)}', '${sqlQuote(r.token_ca)}', ${Number(r.signal_ts)}, ${Number(r.window_start_ts)}, ${Number(r.window_end_ts)}, '${sqlQuote(r.label)}', '${sqlQuote(r.return_domain)}', '${sqlQuote(r.effective_tier)}', ${queryStartTs(r)}, ${queryEndTs(r)})`
  )).join(',\n');
  fs.writeFileSync(sqlPath, template.replace('{{SIGNAL_WINDOWS_VALUES}}', values));
}

function queryStartTs(row) { return Number(row.query_start_ts || row.window_start_ts); }
function queryEndTs(row) { return Number(row.query_end_ts || row.window_end_ts); }

function chunkSignalWindows(rows) {
  const chunks = [];
  let current = [];
  const maxRows = Math.max(1, DUNE_CHUNK_SIZE);
  const maxSpan = Math.max(900, DUNE_CHUNK_MAX_SPAN_S);
  const spanWith = (candidate) => {
    const all = [...current, candidate];
    const minStart = Math.min(...all.map(queryStartTs));
    const maxEnd = Math.max(...all.map(queryEndTs));
    return maxEnd - minStart;
  };
  for (const row of rows) {
    if (current.length && (current.length >= maxRows || spanWith(row) > maxSpan)) {
      chunks.push(current);
      current = [];
    }
    current.push(row);
  }
  if (current.length) chunks.push(current);
  return chunks;
}

function splitFailedDuneRows(rows) {
  if (rows.length > 1) {
    const mid = Math.ceil(rows.length / 2);
    return [rows.slice(0, mid), rows.slice(mid)];
  }
  const row = rows[0];
  const start = queryStartTs(row);
  const end = queryEndTs(row);
  const span = end - start + 1;
  if (span <= Math.max(1, DUNE_CHUNK_MIN_SPAN_S)) return null;
  const mid = Math.floor((start + end) / 2);
  return [
    [{ ...row, query_start_ts: start, query_end_ts: mid }],
    [{ ...row, query_start_ts: mid + 1, query_end_ts: end }],
  ];
}

function isDuneSplittableQueryFailure(error) {
  const msg = String(error?.stderr || error?.stdout || error?.message || error || '');
  return msg.includes('FAILED_TYPE_RESOURCES_CAP_REACHED')
    || msg.includes('FAILED_TYPE_EXECUTION_TIMEOUT')
    || /query execution timed out/i.test(msg)
    || /exceeded .*resources/i.test(msg)
    || /resources cap/i.test(msg);
}

function runDuneChunk({ rows, chunkId, rawDir, chunks, combinedRows }) {
  const chunkDir = path.join(rawDir, chunkId);
  fs.mkdirSync(chunkDir, { recursive: true });
  const chunkWindows = path.join(chunkDir, 'signal_windows.csv');
  const chunkSql = path.join(chunkDir, 'oos.sql');
  const chunkTrades = path.join(chunkDir, 'trades.jsonl');
  const chunkManifest = path.join(chunkDir, 'dune-manifest.json');
  writeSignalWindowsCsv(chunkWindows, rows);
  writeDuneSqlForWindows(chunkSql, rows);
  try {
    runStage(`DUNE ${chunkId}`, PYTHON, [DUNE_EXPORT, '--sql', chunkSql,
      '--out-jsonl', chunkTrades, '--manifest', chunkManifest, '--key-file', DUNE_KEY_FILE],
    { captureOutput: true });
  } catch (error) {
    if (!isDuneSplittableQueryFailure(error)) throw error;
    const splits = splitFailedDuneRows(rows);
    if (!splits) throw error;
    console.error(`[dune] ${chunkId} failed; splitting into ${splits.length} smaller chunk(s) and retrying.`);
    splits.forEach((splitRows, idx) => runDuneChunk({
      rows: splitRows,
      chunkId: `${chunkId}-${String.fromCharCode(97 + idx)}`,
      rawDir,
      chunks,
      combinedRows,
    }));
    return;
  }
  const cm = readJson(chunkManifest);
  const chunkRows = readJsonlRows(chunkTrades);
  combinedRows.push(...chunkRows);
  chunks.push({
    chunk_id: chunkId,
    windows: new Set(rows.map((r) => r.window_id)).size,
    query_slices: rows.length,
    min_window_start_ts: Math.min(...rows.map((r) => Number(r.window_start_ts))),
    max_window_end_ts: Math.max(...rows.map((r) => Number(r.window_end_ts))),
    min_query_start_ts: Math.min(...rows.map(queryStartTs)),
    max_query_end_ts: Math.max(...rows.map(queryEndTs)),
    max_span_s: DUNE_CHUNK_MAX_SPAN_S,
    min_span_s: DUNE_CHUNK_MIN_SPAN_S,
    execution_id: cm.execution_id,
    row_count: cm.row_count,
    out_jsonl_sha256: cm.out_jsonl_sha256,
    sql_sha256: cm.sql_sha256,
  });
}

function exportDuneInChunks({ signalWindowsPath, rawDir, tradesPath, manifestPath }) {
  const allWindows = parseSignalWindowsCsv(signalWindowsPath)
    .map((row) => ({ ...row, query_start_ts: row.window_start_ts, query_end_ts: row.window_end_ts }))
    .sort((a, b) => Number(a.window_end_ts) - Number(b.window_end_ts) || String(a.window_id).localeCompare(String(b.window_id)));
  fs.mkdirSync(rawDir, { recursive: true });
  const windowChunks = chunkSignalWindows(allWindows);
  const chunks = [];
  const combinedRows = [];
  for (let i = 0; i < windowChunks.length; i += 1) {
    const rows = windowChunks[i];
    const chunkIndex = i + 1;
    const chunkId = `chunk-${String(chunkIndex).padStart(3, '0')}`;
    runDuneChunk({ rows, chunkId: `${chunkId}-of-${String(windowChunks.length).padStart(3, '0')}`, rawDir, chunks, combinedRows });
  }
  writeJsonlRows(tradesPath, combinedRows);
  const generatedAt = new Date().toISOString();
  const manifest = {
    schema_version: 'dune_sql_export_manifest.v1',
    generated_at: generatedAt,
    started_at: chunks[0]?.started_at || generatedAt,
    sql_file: 'chunked',
    sql_sha256: crypto.createHash('sha256').update(chunks.map((c) => c.sql_sha256).join('\n')).digest('hex'),
    execution_id: `chunked:${chunks.map((c) => c.execution_id).join(',')}`,
    chunked: true,
    chunks,
    out_jsonl: tradesPath,
    out_jsonl_sha256: sha256File(tradesPath),
    row_count: combinedRows.length,
    columns: combinedRows.length ? Object.keys(combinedRows[0]).sort() : [],
    page_limit: null,
  };
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
}

function checkPreconditions() {
  const need = (p, what) => { if (!exists(p)) die(`${what} not found: ${p}`); };
  need(TOKEN_FILE, 'dashboard token file');
  if (!E.DUNE_API_KEY && !exists(DUNE_KEY_FILE)) die(`Dune API key not found: set DUNE_API_KEY or create ${DUNE_KEY_FILE}`);
  need(TRAINING, 'training tokens');
  need(CUM_DIR, 'cumulative dir');
  need(ORCH, 'orchestrator'); need(DUNE_EXPORT, 'dune export'); need(VALIDATOR, 'validator');
  need(PREREG, 'prereg spec'); need(PREREG_LOCK, 'prereg lock');
  // prereg lock self-check (the orchestrator re-checks too; fail early with a clear message)
  const expected = (fs.readFileSync(PREREG_LOCK, 'utf8').trim().split(/\s+/)[0] || '').toLowerCase();
  const actual = crypto.createHash('sha256').update(fs.readFileSync(PREREG)).digest('hex').toLowerCase();
  if (expected !== actual) die(`prereg lock mismatch: ${actual.slice(0, 12)} != ${expected.slice(0, 12)}`);
  return { preregSha: actual };
}

async function pullSnapshot(packDir) {
  fs.mkdirSync(packDir, { recursive: true });
  const token = fs.readFileSync(TOKEN_FILE, 'utf8').trim();
  if (!token) die(`dashboard token file is empty: ${TOKEN_FILE}`);
  const dbPath = path.join(packDir, 'raw_signal_outcomes.snapshot.db');
  const url = `${BASE_URL}/api/data/download/raw-signal-outcomes?token=${encodeURIComponent(token)}`;
  let lastErr = '';
  for (let attempt = 1; attempt <= PULL_RETRIES; attempt += 1) {
    const curlConfigPath = path.join(packDir, `.snapshot-curl-${attempt}.conf`);
    try { fs.rmSync(dbPath, { force: true }); } catch { /* fresh each attempt */ }
    try {
      // Keep secrets out of argv/ps output. The temporary curl config is 0600 and
      // deleted after each attempt; stdout/stderr never include the token.
      const curlConfig = [
        'fail',
        'show-error',
        'silent',
        `max-time = ${PULL_TIMEOUT_S}`,
        'connect-timeout = 30',
        'speed-time = 60',
        'speed-limit = 1024',
        `header = "Authorization: Bearer ${curlConfigQuote(token)}"`,
        `url = "${curlConfigQuote(url)}"`,
        `output = "${curlConfigQuote(dbPath)}"`,
        '',
      ].join('\n');
      fs.writeFileSync(curlConfigPath, curlConfig, { mode: 0o600 });
      await runCurlWithWatchdog(curlConfigPath, dbPath);
    } catch (e) {
      lastErr = `curl failed (attempt ${attempt}/${PULL_RETRIES}): ${String(e.stderr || e.message).replace(token, '<redacted>').slice(0, 160)}`;
      console.error(`[snapshot] ${lastErr} — retrying`); continue;
    } finally {
      try { fs.rmSync(curlConfigPath, { force: true }); } catch { /* best effort */ }
    }
    const bytes = exists(dbPath) ? fs.statSync(dbPath).size : 0;
    if (bytes < SNAPSHOT_MIN_BYTES) { lastErr = `snapshot too small (${bytes} bytes < ${SNAPSHOT_MIN_BYTES}); likely truncated`; console.error(`[snapshot] ${lastErr} — retrying`); continue; }
    let integ = '';
    try { integ = execFileSync('sqlite3', [`file:${dbPath}?mode=ro`, 'PRAGMA integrity_check;'], { encoding: 'utf8' }).trim(); }
    catch (e) { integ = `error:${String(e.message).slice(0, 100)}`; }
    if (integ !== 'ok') { lastErr = `integrity_check != ok: ${integ}`; console.error(`[snapshot] ${lastErr} — retrying`); continue; }
    return { dbPath, bytes, integrity: integ, sha256: sha256File(dbPath) };
  }
  die(`snapshot pull failed after ${PULL_RETRIES} attempts: ${lastErr} (server may be asleep/cold-starting — re-run later).`);
  return null; // unreachable
}

async function main() {
  const argv = process.argv.slice(2);
  let checkOnly = false;
  for (const a of argv) {
    if (a === '--look-point' || a === '--reveal-sealed-auc') die(`refused: ${a} is never permitted in the daily cycle (AUC stays sealed until a Codex-audited look point).`);
    else if (a === '--check') checkOnly = true;
    else if (a === '--help' || a === '-h') { console.log(fs.readFileSync(new URL(import.meta.url).pathname, 'utf8').split('\n').filter((l) => l.startsWith(' *')).join('\n')); return; }
    else die(`unknown arg: ${a}`);
  }

  const today = utcDate();
  const outDir = path.join(DATAROOM, `oos-daily-formal-${today}`);
  const packId = `oos-daily-formal-${today}`;
  const opManifestPath = path.join(outDir, 'operation-manifest.json');

  // ---- 1/day idempotency (no double-count) ----
  if (exists(opManifestPath)) {
    const m = readJson(opManifestPath);
    if (m.phase === 'ingested') {
      const cum = readJson(path.join(CUM_DIR, 'cumulative_counts.json'));
      console.log(JSON.stringify({ ok: true, status: 'ALREADY_RAN_TODAY', utc_date: today, out_dir: outDir, cumulative: cum }, null, 2));
      return;
    }
    die(`out-dir exists but phase=${m.phase} (a prior run left it incomplete). Inspect, then remove ${outDir} before re-running (no auto-overwrite).`);
  }
  if (exists(outDir)) die(`out-dir exists without a manifest: ${outDir} (inspect/remove before re-running).`);

  const { preregSha } = checkPreconditions();
  const productionCommit = remoteMainHead();
  if (!productionCommit) die('could not resolve production commit (`git ls-remote origin main` failed); production auto-deploys main HEAD.');

  if (checkOnly) {
    console.log(JSON.stringify({
      ok: true, mode: 'check', utc_date: today, out_dir: outDir, pack_id: packId,
      production_commit: productionCommit, prereg_sha256_12: preregSha.slice(0, 12),
      base_url: BASE_URL, token_file: TOKEN_FILE, dune_key: E.DUNE_API_KEY ? 'env' : DUNE_KEY_FILE,
      cumulative: readJson(path.join(CUM_DIR, 'cumulative_counts.json')),
      plan: ['pull snapshot -> frozen pack', 'PREPARE', 'Dune export', 'validate', 'INGEST (--force-smoke)', 'leak sweep', 'n=50 gate'],
      note: 'check mode performed NO network pull and NO ingest; no side effects.',
    }, null, 2));
    return;
  }

  // ---- 1. fresh snapshot -> frozen pack ----
  const packDir = path.join(DATAROOM, `oos-frozen-pack-${utcStamp()}`);
  const pack = await pullSnapshot(packDir);
  fs.writeFileSync(path.join(packDir, 'manifest.json'), JSON.stringify({
    schema_version: 'oos_frozen_pack_manifest.v1', generated_at: new Date().toISOString(),
    snapshot: { path: pack.dbPath, sha256: pack.sha256, bytes: pack.bytes, integrity: pack.integrity },
    production_commit: productionCommit, production_commit_source: 'git ls-remote origin main (prod auto-deploys main HEAD)',
    base_url: BASE_URL,
  }, null, 2));
  console.error(`[snapshot] ok: ${pack.bytes} bytes, sha ${pack.sha256.slice(0, 12)}, integrity ${pack.integrity}, prod ${productionCommit.slice(0, 8)}`);

  // ---- 2. PREPARE (producer -> OOS selection -> oos.sql + signal_windows.csv) ----
  runStage('PREPARE', NODE, [ORCH, '--pack-id', packId, '--snapshot', pack.dbPath,
    '--production-commit', productionCommit, '--cumulative-dir', CUM_DIR, '--out-dir', outDir, '--training-tokens', TRAINING]);

  const sel = readJson(path.join(outDir, 'cohort', 'oos-cohort-selection.json'));
  const oosN = (sel.dogs || 0) + (sel.duds || 0);
  if (oosN === 0) {
    const result = { status: 'DATA_INSUFFICIENT_WAIT', utc_date: today, pack_id: packId, out_dir: outDir,
      production_commit: productionCommit, snapshot_sha256: pack.sha256, oos_selection: sel.cohort_stage_exclusions,
      cumulative: readJson(path.join(CUM_DIR, 'cumulative_counts.json')), auc_read: false };
    fs.writeFileSync(path.join(outDir, 'daily-cycle-result.json'), JSON.stringify(result, null, 2));
    appendOpsLog(result);
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  // ---- 3. Dune export (write to a SEPARATE dir so INGEST's copy is not src==dest) ----
  const duneDir = path.join(outDir, 'dune');         // orchestrator-owned: oos.sql, signal_windows.csv
  const rawDir = path.join(outDir, '_dune_raw');      // runner-owned Dune output
  fs.mkdirSync(rawDir, { recursive: true });
  const tradesPath = path.join(rawDir, 'trades.jsonl');
  const duneManifestPath = path.join(rawDir, 'dune-manifest.json');
  const validationPath = path.join(rawDir, 'validation.json');
  exportDuneInChunks({ signalWindowsPath: path.join(duneDir, 'signal_windows.csv'), rawDir, tradesPath, manifestPath: duneManifestPath });

  // ---- 4. validate (out_of_window must be 0; orchestrator also re-checks) ----
  runStage('VALIDATE', NODE, [VALIDATOR, '--windows', path.join(duneDir, 'signal_windows.csv'), '--trades', tradesPath, '--out', validationPath]);
  const dv = readJson(validationPath);
  if (Number(dv.summary.out_of_window_trades_n) > 0) die(`validation out_of_window_trades_n=${dv.summary.out_of_window_trades_n} > 0 (feature_ts must be <= signal_ts).`);

  // ---- 5. INGEST (coverage gate + dune-metadata gate + leak gate live inside the orchestrator) ----
  runStage('INGEST', NODE, [ORCH, '--pack-id', packId, '--snapshot', pack.dbPath, '--production-commit', productionCommit,
    '--dune-trades', tradesPath, '--dune-manifest', duneManifestPath, '--dune-validation', validationPath,
    '--cumulative-dir', CUM_DIR, '--out-dir', outDir, '--training-tokens', TRAINING, '--force-smoke']);

  // ---- 6. leak sweep (defense-in-depth on top of the orchestrator's own sweep) ----
  const accDir = path.join(outDir, 'work', 'accumulate-out');
  const leak = exists(accDir) ? fs.readdirSync(accDir).filter((f) => /lookpoint|sealed|auc/i.test(f)) : [];
  if (leak.length) die(`AUC artifact leak: ${leak.join(', ')}`);
  const qa = readJson(path.join(accDir, 'daily_qa_report.json'));
  if ('auc' in qa) die('AUC field present in daily QA before a look point.');

  // ---- 7. n=50 gate (futility-only; NEVER read AUC here) ----
  const dm = readJson(duneManifestPath);
  const cum = readJson(path.join(CUM_DIR, 'cumulative_counts.json'));
  const reached = cum.dog >= N50 && cum.dud >= N50;
  const result = {
    status: reached ? 'LOOKPOINT_READY_N50_AUDIT_REQUIRED' : 'DAILY_ACCUMULATION_CONTINUE',
    utc_date: today, pack_id: packId, out_dir: outDir, production_commit: productionCommit,
    snapshot_sha256: pack.sha256, snapshot_bytes: pack.bytes,
    dune: { execution_id: dm.execution_id, row_count: dm.row_count, out_of_window: dv.summary.out_of_window_trades_n },
    daily_cohort: qa.daily_cohort, cumulative: { dog: cum.dog, dud: cum.dud },
    coverage_asymmetry_pp: qa.cumulative_coverage_asymmetry_pp, coverage_gate_ok: qa.coverage_gate.ok,
    to_n50: { dog: Math.max(0, N50 - cum.dog), dud: Math.max(0, N50 - cum.dud) },
    auc_read: false, leak: 0,
  };
  fs.writeFileSync(path.join(outDir, 'daily-cycle-result.json'), JSON.stringify(result, null, 2));
  appendOpsLog(result);

  if (reached) {
    console.error('\n================ HALT ================');
    console.error('n=50 reached (dog>=50 AND dud>=50).  Status: LOOKPOINT_READY_N50_AUDIT_REQUIRED');
    console.error('AUC is NOT read by this runner. Codex audit is required BEFORE any look point.');
    console.error('Stop the daily runner; do not schedule further cycles until audited.');
    console.error('=====================================');
    console.log(JSON.stringify(result, null, 2));
    process.exit(10);
  }
  console.log(JSON.stringify(result, null, 2));
}

main().catch((e) => die(e?.message || String(e)));
