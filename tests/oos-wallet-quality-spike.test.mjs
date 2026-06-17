import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync, spawnSync } from 'node:child_process';
import { buildWindows, valuesSql } from '../scripts/build-oos-wallet-quality-spike-windows.js';
import { summarize, hasForbiddenKey } from '../scripts/validate-oos-wallet-quality-spike.js';

const WINDOWS_MANIFEST_365D = { selection: { history_days: 365 } };

function completeSpikeRow(extra = {}) {
  return {
    window_id: 'w1',
    token_ca: 'tok1',
    signal_ts: 1000,
    window_complete: true,
    n_buyers: 2,
    buy_sol_total: 3,
    n_buyers_with_prior_history: 1,
    n_buyers_qualify_k1_7d: 1,
    n_buyers_qualify_k3_7d: 0,
    n_buyers_qualify_k5_7d: 0,
    buy_sol_from_qualify_k3_7d: 0,
    n_buyers_qualify_k1_7d_nonsniper: 1,
    n_buyers_qualify_k3_7d_nonsniper: 0,
    n_buyers_qualify_k5_7d_nonsniper: 0,
    buy_sol_from_qualify_k3_7d_nonsniper: 0,
    n_buyers_qualify_k1_14d: 1,
    n_buyers_qualify_k3_14d: 0,
    n_buyers_qualify_k5_14d: 0,
    buy_sol_from_qualify_k3_14d: 0,
    n_buyers_qualify_k1_14d_nonsniper: 1,
    n_buyers_qualify_k3_14d_nonsniper: 0,
    n_buyers_qualify_k5_14d_nonsniper: 0,
    buy_sol_from_qualify_k3_14d_nonsniper: 0,
    n_buyers_qualify_k1_30d: 1,
    n_buyers_qualify_k3_30d: 0,
    n_buyers_qualify_k5_30d: 0,
    buy_sol_from_qualify_k3_30d: 0,
    n_buyers_qualify_k1_30d_nonsniper: 1,
    n_buyers_qualify_k3_30d_nonsniper: 0,
    n_buyers_qualify_k5_30d_nonsniper: 0,
    buy_sol_from_qualify_k3_30d_nonsniper: 0,
    n_buyers_qualify_k1_all: 1,
    n_buyers_qualify_k3_all: 1,
    n_buyers_qualify_k5_all: 0,
    buy_sol_from_qualify_k3_all: 1.2,
    n_buyers_qualify_k1_all_nonsniper: 1,
    n_buyers_qualify_k3_all_nonsniper: 1,
    n_buyers_qualify_k5_all_nonsniper: 0,
    buy_sol_from_qualify_k3_all_nonsniper: 1.2,
    n_buyers_creator_excluded: 0,
    n_buyers_first_block_sniper: 1,
    n_buyers_prior_sniper: 0,
    n_buyers_any_sniper_proxy: 1,
    prior_trades_per_buyer_median: 2,
    prior_trades_per_buyer_p90: 4,
    prior_snipe_rate_buyer_median: 0,
    prior_snipe_rate_buyer_p90: 0.25,
    max_prior_bt: 900,
    asof_ok: true,
    ...extra,
  };
}

test('buildWindows emits deterministic label-stripped windows', () => {
  const rows = [
    { token_ca: 'b', signal_ts: 200, label: 'dog', unique_buyers: 9 },
    { token_ca: 'a', signal_ts: 300, label: 'dud', unique_buyers: 1 },
    { token_ca: 'b', signal_ts: 200, label: 'dog', unique_buyers: 99 },
  ];
  const windows = buildWindows(rows, { limit: 2, order: 'newest', preSec: 15, historyDays: 7 });
  assert.deepEqual(windows.map((w) => [w.window_id, w.token_ca, w.signal_ts, w.window_start_ts, w.history_start_ts]), [
    ['w0001', 'a', 300, 285, 300 - 7 * 86400],
    ['w0002', 'b', 200, 185, 200 - 7 * 86400],
  ]);
  assert.equal(Object.keys(windows[0]).includes('label'), false);
  assert.equal(Object.keys(windows[0]).includes('unique_buyers'), false);
  assert.match(valuesSql(windows), /w0001/);
  assert.doesNotMatch(valuesSql(windows), /dog|dud/);
});

test('window builder CLI writes CSV/SQL without labels or AUC', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wallet-spike-'));
  const features = path.join(dir, 'features.jsonl');
  fs.writeFileSync(features, [
    JSON.stringify({ token_ca: 'tok1', signal_ts: 1000, label: 'dog', return_domain: 'sol_curve' }),
    JSON.stringify({ token_ca: 'tok2', signal_ts: 2000, label: 'dud', return_domain: 'sol_curve' }),
  ].join('\n') + '\n');
  const outCsv = path.join(dir, 'windows.csv');
  const outVals = path.join(dir, 'values.sql');
  const outSql = path.join(dir, 'query.sql');
  const outManifest = path.join(dir, 'manifest.json');
  execFileSync(process.execPath, ['scripts/build-oos-wallet-quality-spike-windows.js',
    '--features', features, '--limit', '2', '--out-csv', outCsv, '--out-values-sql', outVals,
    '--out-sql', outSql, '--out-manifest', outManifest], { cwd: '/Users/boliu/sas-research' });
  const csv = fs.readFileSync(outCsv, 'utf8');
  const sql = fs.readFileSync(outSql, 'utf8');
  const manifest = JSON.parse(fs.readFileSync(outManifest, 'utf8'));
  assert.equal(csv.includes('label'), false);
  assert.equal(sql.includes('dog'), false);
  assert.equal(sql.includes('dud'), false);
  assert.equal(sql.includes('{{SIGNAL_WINDOWS_VALUES}}'), false);
  assert.equal(manifest.label_stripped, true);
});

test('validator summarizes coverage only and forbids labels/AUC/as-of leaks', () => {
  const rows = [completeSpikeRow()];
  const report = summarize(rows, null, WINDOWS_MANIFEST_365D);
  assert.equal(report.verdict, 'SPIKE_QA_PASS_NO_EDGE_CLAIM');
  assert.equal(report.windows_total, 1);
  assert.equal(report.availability_frac_window_ge1_qualify.all.frac_window_ge1_k3, 1);
  assert.equal(report.availability_frac_window_ge1_qualify.all.frac_window_ge1_k3_nonsniper, 1);
  assert.equal(report.asof_integrity_violations, 0);
  assert.equal(report.lookback_guard.ok, true);
  assert.equal(report.forbidden_outputs.no_auc, true);

  assert.equal(hasForbiddenKey([{ ...rows[0], label: 'dog' }]), 'label');
  assert.equal(hasForbiddenKey([{ nested: { auc: 0.7 } }]), 'auc');
  assert.equal(summarize([{ ...rows[0], max_prior_bt: 1000 }], null, WINDOWS_MANIFEST_365D).verdict, 'SPIKE_QA_FAIL_FIX_PIPELINE');
  assert.equal(summarize(rows, null, { selection: { history_days: 7 } }).verdict, 'SPIKE_QA_FAIL_FIX_PIPELINE');
  assert.equal(summarize(rows, null, { selection: { history_days: 7 } }).lookback_guard.reason, 'history_days_missing_or_less_than_max_lookback');
});

test('validator CLI fails closed on forbidden label field', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wallet-spike-'));
  const rows = path.join(dir, 'rows.jsonl');
  const out = path.join(dir, 'validation.json');
  const windowsManifest = path.join(dir, 'windows-manifest.json');
  fs.writeFileSync(windowsManifest, JSON.stringify(WINDOWS_MANIFEST_365D));
  fs.writeFileSync(rows, JSON.stringify(completeSpikeRow({ label: 'dog', n_buyers: 0, buy_sol_total: 0 })) + '\n');
  const res = spawnSync(process.execPath, ['scripts/validate-oos-wallet-quality-spike.js', '--rows', rows, '--windows-manifest', windowsManifest, '--out', out], {
    cwd: '/Users/boliu/sas-research', encoding: 'utf8',
  });
  assert.notEqual(res.status, 0);
  const report = JSON.parse(fs.readFileSync(out, 'utf8'));
  assert.equal(report.verdict, 'SPIKE_QA_FAIL_FIX_PIPELINE');
  assert.equal(report.forbidden_key_found, 'label');
});

test('validator CLI requires a 365d windows manifest', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wallet-spike-'));
  const rows = path.join(dir, 'rows.jsonl');
  const out = path.join(dir, 'validation.json');
  fs.writeFileSync(rows, `${JSON.stringify(completeSpikeRow())}\n`);
  const missing = spawnSync(process.execPath, ['scripts/validate-oos-wallet-quality-spike.js', '--rows', rows, '--out', out], {
    cwd: '/Users/boliu/sas-research', encoding: 'utf8',
  });
  assert.notEqual(missing.status, 0);

  const shortManifest = path.join(dir, 'windows-manifest-short.json');
  fs.writeFileSync(shortManifest, JSON.stringify({ selection: { history_days: 30 } }));
  const short = spawnSync(process.execPath, ['scripts/validate-oos-wallet-quality-spike.js', '--rows', rows, '--windows-manifest', shortManifest, '--out', out], {
    cwd: '/Users/boliu/sas-research', encoding: 'utf8',
  });
  assert.notEqual(short.status, 0);
  const report = JSON.parse(fs.readFileSync(out, 'utf8'));
  assert.equal(report.lookback_guard.ok, false);
});
