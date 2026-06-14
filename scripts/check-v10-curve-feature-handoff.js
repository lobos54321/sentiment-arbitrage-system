#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';
import { spawnSync } from 'child_process';
import { parse as parseCsv } from 'csv-parse/sync';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    packDir: '',
    tar: '',
    expectedTarSha256: '',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--pack-dir') { args.packDir = next; i += 1; continue; }
    if (key === '--tar') { args.tar = next; i += 1; continue; }
    if (key === '--expected-tar-sha256') { args.expectedTarSha256 = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/check-v10-curve-feature-handoff.js --pack-dir <export-pack-v2> [--tar pack.tgz --expected-tar-sha256 <sha>]',
    '',
    'Checks the V10 curve-feature indexed export handoff pack before it is sent to an indexed/Dune operator.',
  ].join('\n');
}

function sha256(filePath) {
  return createHash('sha256').update(fs.readFileSync(filePath)).digest('hex');
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function readCsv(filePath) {
  return parseCsv(fs.readFileSync(filePath, 'utf8'), {
    columns: true,
    skip_empty_lines: true,
    bom: true,
    trim: true,
  });
}

function countBy(rows, keyFn) {
  const out = {};
  for (const row of rows) {
    const key = keyFn(row) || 'unknown';
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function fileExists(filePath) {
  try {
    return fs.statSync(filePath).isFile();
  } catch {
    return false;
  }
}

function checkNoSecretLikeStrings(filePaths) {
  const patterns = [
    /API[_-]?KEY\s*=/i,
    /SECRET\s*=/i,
    /PRIVATE[_-]?KEY\s*=/i,
    /BEARER\s+[A-Za-z0-9._-]{16,}/i,
    /HELIUS[_-]?API/i,
    /ALCHEMY[_-]?API/i,
    /DUNE[_-]?API/i,
  ];
  const hits = [];
  for (const filePath of filePaths) {
    const text = fs.readFileSync(filePath, 'utf8');
    const lines = text.split(/\r?\n/);
    lines.forEach((line, index) => {
      for (const pattern of patterns) {
        if (pattern.test(line)) {
          hits.push({ file: filePath, line: index + 1, pattern: String(pattern) });
          break;
        }
      }
    });
  }
  return hits;
}

function listTar(tarPath) {
  const result = spawnSync('tar', ['-tzf', tarPath], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, LC_ALL: 'C', LANG: 'C' },
  });
  if (result.status !== 0) {
    return { ok: false, error: result.stderr || result.stdout || 'tar list failed' };
  }
  return { ok: true, entries: result.stdout.split(/\r?\n/).filter(Boolean) };
}

function main() {
  const args = parseArgs();
  if (args.help || !args.packDir) {
    console.log(usage());
    process.exit(args.help ? 0 : 1);
  }
  const packDir = path.resolve(args.packDir);
  const required = [
    'README.md',
    'SHA256SUMS.json',
    'indexed_trade_export_template.sql',
    'manifest.json',
    'signal_windows.csv',
    'signal_windows_values.sql',
    'tokens.csv',
  ];
  const blockers = [];
  const warnings = [];
  const files = Object.fromEntries(required.map((name) => [name, path.join(packDir, name)]));
  for (const [name, filePath] of Object.entries(files)) {
    if (!fileExists(filePath)) blockers.push(`missing_required_file:${name}`);
  }
  let manifest = null;
  let windows = [];
  let tokens = [];
  if (!blockers.length) {
    manifest = readJson(files['manifest.json']);
    windows = readCsv(files['signal_windows.csv']);
    tokens = readCsv(files['tokens.csv']);
    const shaManifest = readJson(files['SHA256SUMS.json']);
    for (const [name, expected] of Object.entries(shaManifest)) {
      const filePath = path.join(packDir, name);
      if (!fileExists(filePath)) {
        blockers.push(`sha_file_missing:${name}`);
        continue;
      }
      const actual = sha256(filePath);
      if (actual !== expected) blockers.push(`sha_mismatch:${name}`);
    }
    if (windows.length !== manifest.rows) blockers.push(`window_count_mismatch:${windows.length}!=${manifest.rows}`);
    if (tokens.length !== manifest.unique_tokens) blockers.push(`token_count_mismatch:${tokens.length}!=${manifest.unique_tokens}`);
    const labelCounts = countBy(windows, (row) => row.label);
    if ((labelCounts.dog || 0) !== manifest.dogs) blockers.push(`dog_count_mismatch:${labelCounts.dog || 0}!=${manifest.dogs}`);
    if ((labelCounts.dud || 0) !== manifest.duds) blockers.push(`dud_count_mismatch:${labelCounts.dud || 0}!=${manifest.duds}`);
    if ((manifest.coverage_guardrail?.missing_cohort_meta_rows || 0) !== 0) blockers.push('missing_cohort_meta_rows_nonzero');
    const domainLabelCounts = countBy(windows, (row) => `${row.return_domain}|${row.label}`);
    for (const key of ['sol_curve|dog', 'sol_curve|dud', 'spliced_curve_to_gmgn|dog', 'spliced_curve_to_gmgn|dud', 'usd_gmgn|dog', 'usd_gmgn|dud']) {
      if (!domainLabelCounts[key]) blockers.push(`missing_domain_label_bucket:${key}`);
    }
    const template = fs.readFileSync(files['indexed_trade_export_template.sql'], 'utf8');
    if (!template.includes('YOUR_PUMPFUN_TRADE_EVENT_TABLE')) warnings.push('query_template_placeholder_not_found');
    if (!template.includes('block_time BETWEEN w.window_start_ts AND w.window_end_ts')) blockers.push('query_template_window_join_missing');
    const secretHits = checkNoSecretLikeStrings(Object.values(files).filter((filePath) => filePath.endsWith('.md') || filePath.endsWith('.sql') || filePath.endsWith('.csv') || filePath.endsWith('.json')));
    if (secretHits.length) blockers.push('secret_like_string_found');
    if (secretHits.length) warnings.push({ secret_hits: secretHits.slice(0, 10) });
  }
  let tar = null;
  if (args.tar) {
    const tarPath = path.resolve(args.tar);
    if (!fileExists(tarPath)) {
      blockers.push('tar_missing');
      tar = { path: tarPath, exists: false };
    } else {
      const actual = sha256(tarPath);
      const listed = listTar(tarPath);
      tar = {
        path: tarPath,
        exists: true,
        sha256: actual,
        expected_sha256: args.expectedTarSha256 || null,
        sha256_matches_expected: args.expectedTarSha256 ? actual === args.expectedTarSha256 : null,
        entries_n: listed.ok ? listed.entries.length : null,
        list_error: listed.ok ? null : listed.error,
      };
      if (args.expectedTarSha256 && actual !== args.expectedTarSha256) blockers.push('tar_sha256_mismatch');
      if (!listed.ok) blockers.push('tar_list_failed');
    }
  }
  const report = {
    schema_version: 'v10_curve_feature_handoff_check.v1',
    generated_at: new Date().toISOString(),
    status: blockers.length ? 'blocked' : 'ready_to_send',
    blockers,
    warnings,
    pack_dir: packDir,
    manifest_summary: manifest ? {
      rows: manifest.rows,
      dogs: manifest.dogs,
      duds: manifest.duds,
      unique_tokens: manifest.unique_tokens,
      coverage_guardrail: manifest.coverage_guardrail,
    } : null,
    csv_summary: windows.length ? {
      rows: windows.length,
      label_counts: countBy(windows, (row) => row.label),
      return_domain_counts: countBy(windows, (row) => row.return_domain),
      return_domain_x_label_counts: countBy(windows, (row) => `${row.return_domain}|${row.label}`),
      tokens: tokens.length,
    } : null,
    tar,
  };
  console.log(JSON.stringify(report, null, 2));
  if (blockers.length) process.exit(1);
}

main();
