import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { buildFeatureRow } from '../scripts/build-v10-curve-feature-table.js';

// baseRow comes from the producer; decodeRow from the curve decode.
const base = (over = {}) => ({ token_ca: 'tok', signal_ts: 1781300000, return_domain: 'native_sol', effective_tier: 'gold', ...over });
const decode = (over = {}) => ({ status: 'ok', trades_n: 5, history_reached_start: true, unique_buyers: 12, bars: [], ...over });
const hasExactKey = (obj, key) => {
  if (!obj || typeof obj !== 'object') return false;
  if (Object.prototype.hasOwnProperty.call(obj, key)) return true;
  if (Array.isArray(obj)) return obj.some((v) => hasExactKey(v, key));
  return Object.values(obj).some((v) => hasExactKey(v, key));
};

test('case1: native_sol + decode ok + trades>0 + complete -> sol_curve, complete_window, features filled', () => {
  const r = buildFeatureRow(base(), 'dog', decode());
  assert.equal(r.return_domain, 'sol_curve');
  assert.equal(r.input_return_domain, 'native_sol');
  assert.equal(r.curve_confirmed, true);
  assert.equal(r.curve_trade_observed_n, 5);
  assert.equal(r.feature_coverage_status, 'complete_window');
  assert.equal(r.unique_buyers, 12); // feature filled on complete window
  assert.equal(r.return_domain_upgrade_reason, 'upgraded_native_sol_to_sol_curve_curve_trades_observed');
});

test('case2: native_sol + decode ok + trades=0 -> stays native_sol (excluded downstream)', () => {
  const r = buildFeatureRow(base(), 'dud', decode({ trades_n: 0 }));
  assert.equal(r.return_domain, 'native_sol');
  assert.equal(r.curve_confirmed, false);
  assert.equal(r.return_domain_upgrade_reason, 'kept_native_sol_no_curve_trades');
});

test('case3: native_sol + decode ok + trades>0 + INCOMPLETE -> sol_curve + incomplete_window; features withheld', () => {
  const r = buildFeatureRow(base(), 'dog', decode({ history_reached_start: false }));
  assert.equal(r.return_domain, 'sol_curve'); // upgraded despite incomplete -> coverage gate can see it
  assert.equal(r.curve_confirmed, true);
  assert.equal(r.curve_trade_observed_n, 5); // coverage evidence present
  assert.equal(r.feature_coverage_status, 'incomplete_window');
  assert.equal(r.unique_buyers, null); // AUC features withheld unless complete_window
  assert.equal(r.trades_n, null); // usable-gated feature trades_n withheld
});

test('case4: usd_gmgn / spliced / already-sol_curve inputs are NOT touched by the upgrade', () => {
  assert.equal(buildFeatureRow(base({ return_domain: 'usd_gmgn' }), 'dog', decode()).return_domain, 'usd_gmgn');
  assert.equal(buildFeatureRow(base({ return_domain: 'usd_gmgn' }), 'dog', decode()).return_domain_upgrade_reason, 'input_domain_untouched');
  assert.equal(buildFeatureRow(base({ return_domain: 'spliced_curve_to_gmgn' }), 'dog', decode()).return_domain, 'spliced_curve_to_gmgn');
  // a legacy discovery-cohort row already marked sol_curve stays sol_curve, untouched
  const legacy = buildFeatureRow(base({ return_domain: 'sol_curve' }), 'dog', decode());
  assert.equal(legacy.return_domain, 'sol_curve');
  assert.equal(legacy.return_domain_upgrade_reason, 'input_domain_untouched');
});

test('native_sol + decode NOT ok -> stays native_sol, decode_unavailable', () => {
  const r = buildFeatureRow(base(), 'dud', { status: 'missing_decode' });
  assert.equal(r.return_domain, 'native_sol');
  assert.equal(r.curve_confirmed, false);
  assert.equal(r.curve_trade_observed_n, 0);
  assert.equal(r.feature_coverage_status, 'decode_unavailable');
  assert.equal(r.return_domain_upgrade_reason, 'kept_native_sol_decode_missing_decode');
});

test('daily/OOS default emits no discrimination report and no auc key', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'curve-feature-table-'));
  const dogs = path.join(dir, 'dogs.json');
  const duds = path.join(dir, 'duds.json');
  const dec = path.join(dir, 'decode.json');
  const out = path.join(dir, 'feature.json');
  fs.writeFileSync(dogs, JSON.stringify([base({ token_ca: 'dog1' })]));
  fs.writeFileSync(duds, JSON.stringify([base({ token_ca: 'dud1', effective_tier: 'bronze' })]));
  fs.writeFileSync(dec, JSON.stringify([
    { ...decode({ anchor_ts: 1781300000, unique_buyers: 5 }), token_ca: 'dog1' },
    { ...decode({ anchor_ts: 1781300000, unique_buyers: 4 }), token_ca: 'dud1' },
  ]));
  execFileSync(process.execPath, ['scripts/build-v10-curve-feature-table.js',
    '--dogs', dogs, '--duds', duds, '--decode', dec, '--out', out], { cwd: '/Users/boliu/sas-research' });
  const report = JSON.parse(fs.readFileSync(out, 'utf8'));
  assert.equal(report.strata, undefined);
  assert.equal(hasExactKey(report, 'auc'), false);
});

test('offline discovery mode can explicitly include discrimination report', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'curve-feature-table-'));
  const dogs = path.join(dir, 'dogs.json');
  const duds = path.join(dir, 'duds.json');
  const dec = path.join(dir, 'decode.json');
  const out = path.join(dir, 'feature.json');
  fs.writeFileSync(dogs, JSON.stringify([base({ token_ca: 'dog1' })]));
  fs.writeFileSync(duds, JSON.stringify([base({ token_ca: 'dud1', effective_tier: 'bronze' })]));
  fs.writeFileSync(dec, JSON.stringify([
    { ...decode({ anchor_ts: 1781300000, unique_buyers: 5 }), token_ca: 'dog1' },
    { ...decode({ anchor_ts: 1781300000, unique_buyers: 4 }), token_ca: 'dud1' },
  ]));
  execFileSync(process.execPath, ['scripts/build-v10-curve-feature-table.js',
    '--dogs', dogs, '--duds', duds, '--decode', dec, '--out', out, '--include-discrimination-report'], { cwd: '/Users/boliu/sas-research' });
  const report = JSON.parse(fs.readFileSync(out, 'utf8'));
  assert.ok(report.strata?.all?.all);
  assert.equal(typeof report.strata.all.all.features.unique_buyers.auc, 'number');
});
