#!/usr/bin/env node
'use strict';

/**
 * export-oos-training-tokens.js
 *
 * ONE-TIME export of the training/discovery token exclusion list for the locked
 * preregistration:
 *   claudedocs/oos-sol-curve-unique-buyers-preregister.md  (§2)
 *
 * Prereg §2 (verbatim): "Exclude any `token_ca` that appears in the
 * discovery/training cohort (2026-06-06 through 2026-06-11, inclusive)."
 *
 * The exclusion set is therefore the UNION of every token_ca observed in the
 * discovery window, NOT just the 299-window stratified sample used for the
 * discovery AUC. Over-exclusion is the safe direction: it can only make the OOS
 * test more conservative (fewer falsely-"fresh" tokens), never inflate an edge.
 *
 * Authoritative source (verified): the prereg-pinned discovery cohort's FINAL
 * rebuild, whose rebuilt-rows.jsonl union of token_ca is a strict superset of
 * the clean-dog / clean-dud / polluted / quarantine / 299-sample token sets.
 *
 * This tool is deterministic (sorted output) and fail-closed:
 *   - the prereg spec must match its sha256 lock;
 *   - every source row must fall inside the declared discovery window;
 *   - the output + a provenance manifest (source sha256, counts, subset audit)
 *     are written so the exclusion set is fully reproducible and auditable.
 *
 * It reads no AUC and changes nothing live.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const DEFAULT_ROOT = '/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z';
const DEFAULT_SOURCE = path.join(DEFAULT_ROOT, 'cohort-rebuild-v10-final-native-return-guard/rebuilt-rows.jsonl');
const DEFAULT_PREREG = '/Users/boliu/sas-research/claudedocs/oos-sol-curve-unique-buyers-preregister.md';
const DEFAULT_PREREG_LOCK = '/Users/boliu/sas-research/claudedocs/oos-sol-curve-unique-buyers-preregister.sha256';
const WINDOW_START = '2026-06-06';
const WINDOW_END = '2026-06-11';
// subset-invariant audit sources (must ALL be subsets of the exported union);
// each line may be a bare token_ca or a composite "token_ca|signal_ts|symbol".
const DEFAULT_SUBSET_CHECKS = [
  'clean-pack/clean-dog-tokens.txt',
  'clean-pack/clean-dud-tokens.txt',
  'clean-pack/polluted-tokens.txt',
  'clean-pack/quarantine-tokens.txt',
  'v10-curve-feature-v1/stratified-samples/export-pack-v2/tokens.csv',
];

function sha256File(p) { return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex'); }
function sha256Str(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
function tokenOf(s) { return String(s).trim().split(/[|,\s]/)[0]; }
function utcDate(ts) { return new Date(Number(ts) * 1000).toISOString().slice(0, 10); }
function readJsonlRows(p) {
  return fs.readFileSync(p, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l));
}
function readTokenList(p) {
  if (!fs.existsSync(p)) return null;
  return [...new Set(fs.readFileSync(p, 'utf8').trim().split('\n')
    .map(tokenOf).filter(Boolean).filter((x) => x !== 'token_ca'))];
}

function verifyPreregLock(specPath, lockPath) {
  if (!fs.existsSync(specPath) || !fs.existsSync(lockPath)) {
    throw new Error(`prereg_lock_missing: spec=${fs.existsSync(specPath)} lock=${fs.existsSync(lockPath)}`);
  }
  const expected = (fs.readFileSync(lockPath, 'utf8').trim().split(/\s+/)[0] || '').toLowerCase();
  const actual = sha256File(specPath).toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(expected)) throw new Error(`prereg_lock_unreadable: no sha256 in ${lockPath}`);
  if (expected !== actual) {
    throw new Error(`prereg_lock_mismatch: ${specPath} ${actual.slice(0, 12)} != locked ${expected.slice(0, 12)} (fail-closed)`);
  }
  return actual;
}

function parseArgs(argv) {
  const a = {
    source: DEFAULT_SOURCE, prereg: DEFAULT_PREREG, preregLock: DEFAULT_PREREG_LOCK,
    windowStart: WINDOW_START, windowEnd: WINDOW_END, root: DEFAULT_ROOT,
    allowOutOfWindow: false,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i]; const v = argv[i + 1];
    if (k === '--source') { a.source = v; i += 1; }
    else if (k === '--prereg') { a.prereg = v; i += 1; }
    else if (k === '--prereg-lock') { a.preregLock = v; i += 1; }
    else if (k === '--window-start') { a.windowStart = v; i += 1; }
    else if (k === '--window-end') { a.windowEnd = v; i += 1; }
    else if (k === '--out-dir') { a.outDir = v; i += 1; }
    else if (k === '--subset-root') { a.root = v; i += 1; }
    else if (k === '--allow-out-of-window') { a.allowOutOfWindow = true; }
  }
  return a;
}

function main() {
  const a = parseArgs(process.argv);
  if (!a.outDir) {
    a.outDir = path.join(a.root, 'oos-training-token-exclusion');
  }
  if (!fs.existsSync(a.source)) throw new Error(`source_missing: ${a.source}`);

  // 1. prereg lock (fail-closed)
  const preregSha = verifyPreregLock(a.prereg, a.preregLock);

  // 2. read source rows; require token_ca + signal_ts, validate the discovery window
  const rows = readJsonlRows(a.source);
  let inWindow = 0; let outWindow = 0; let missingTs = 0;
  const tokens = new Set();
  const outOfWindowSample = [];
  for (const r of rows) {
    const tk = r.token_ca ? tokenOf(r.token_ca) : null;
    const ts = Number(r.signal_ts ?? r.anchor_ts);
    if (!tk) continue;
    if (!ts || Number.isNaN(ts)) { missingTs += 1; tokens.add(tk); continue; }
    const d = utcDate(ts);
    if (d >= a.windowStart && d <= a.windowEnd) { inWindow += 1; tokens.add(tk); }
    else {
      outWindow += 1;
      if (outOfWindowSample.length < 5) outOfWindowSample.push({ token_ca: tk, date: d });
    }
  }
  if (outWindow > 0 && !a.allowOutOfWindow) {
    throw new Error(`source_out_of_window: ${outWindow} rows outside [${a.windowStart}..${a.windowEnd}] in ${a.source} `
      + `(sample ${JSON.stringify(outOfWindowSample)}). This is not the pinned discovery cohort, or the window is wrong. `
      + 'Pass --allow-out-of-window only if you have explicitly re-scoped the discovery window (fail-closed).');
  }
  const sorted = [...tokens].sort();

  // 3. subset-invariant audit: every named "seen in window" source must be a
  //    subset of the exported union (else the union is incomplete -> fail-closed).
  const exported = new Set(sorted);
  const subsetAudit = [];
  for (const rel of DEFAULT_SUBSET_CHECKS) {
    const p = path.join(a.root, rel);
    const list = readTokenList(p);
    if (list === null) { subsetAudit.push({ source: rel, status: 'absent' }); continue; }
    const missing = list.filter((x) => !exported.has(x));
    subsetAudit.push({ source: rel, n: list.length, missing_from_union: missing.length, sample_missing: missing.slice(0, 5) });
    if (missing.length > 0) {
      throw new Error(`subset_invariant_violated: ${missing.length} token_ca in ${rel} are NOT in the exported union `
        + `(e.g. ${JSON.stringify(missing.slice(0, 3))}). The exclusion union is incomplete (fail-closed).`);
    }
  }

  // 4. write deterministic outputs + provenance manifest
  fs.mkdirSync(a.outDir, { recursive: true });
  const tokensTxt = sorted.join('\n') + (sorted.length ? '\n' : '');
  const txtPath = path.join(a.outDir, 'training-tokens.txt');
  fs.writeFileSync(txtPath, tokensTxt);
  const manifest = {
    schema_version: 'oos_training_token_exclusion.v1',
    purpose: 'Prereg §2 training/discovery token exclusion list (2026-06-06..2026-06-11 inclusive).',
    prereg: { path: a.prereg, sha256: preregSha },
    discovery_window: { start: a.windowStart, end: a.windowEnd, inclusive: true },
    source: { path: a.source, sha256: sha256File(a.source), rows: rows.length },
    row_window_tally: { in_window: inWindow, out_of_window: outWindow, missing_ts: missingTs },
    exported: { unique_token_count: sorted.length, sha256: sha256Str(tokensTxt), file: txtPath },
    subset_invariant_audit: subsetAudit,
    note: 'Deterministic (sorted). Over-exclusion is intentional: every token seen in the discovery window is excluded, '
      + 'not only the 299-window AUC sample. Re-run is byte-identical while source + window are unchanged.',
  };
  const manifestPath = path.join(a.outDir, 'training-tokens-manifest.json');
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));

  console.log(JSON.stringify({
    ok: true,
    exported_tokens: sorted.length,
    tokens_sha256: manifest.exported.sha256.slice(0, 12),
    source_sha256: manifest.source.sha256.slice(0, 12),
    in_window: inWindow, out_of_window: outWindow, missing_ts: missingTs,
    subset_audit: subsetAudit.map((s) => `${s.source.split('/').pop()}:${s.status || `${s.n}/${s.missing_from_union}miss`}`),
    out_dir: a.outDir,
  }, null, 2));
}

if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  main();
}

export { tokenOf, utcDate, verifyPreregLock };
