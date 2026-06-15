import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';

const ACC = new URL('../scripts/accumulate-oos-features.js', import.meta.url).pathname;

function setup() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'oos-cov-'));
  fs.writeFileSync(path.join(dir, 'prereg.md'), 'COVERAGE IDEMPOTENCY TEST PREREG\n');
  const sha = crypto.createHash('sha256').update(fs.readFileSync(path.join(dir, 'prereg.md'))).digest('hex');
  fs.writeFileSync(path.join(dir, 'prereg.sha256'), `${sha}  prereg.md\n`);
  fs.writeFileSync(path.join(dir, 'training.txt'), 'NONEXISTENT_TOKEN\n');
  fs.mkdirSync(path.join(dir, 'cum'));
  return dir;
}
// complete sol_curve row = kept cohort row; incomplete sol_curve row = candidate (denominator) but not kept
const row = (tok, ts, label, complete = true) => ({
  token_ca: tok, signal_ts: ts, label, return_domain: 'sol_curve',
  trades_n: complete ? 5 : null,
  feature_coverage_status: complete ? 'complete_window' : 'incomplete_window',
  unique_buyers: complete ? 10 : null,
  progress_stage: complete ? 'mid' : 'decode_unavailable',
  schema_version: 'feat.v1',
});
function ingest(dir, rows, packId) {
  const fr = path.join(dir, `${packId}.json`);
  fs.writeFileSync(fr, JSON.stringify({ rows }));
  const out = path.join(dir, `out-${packId}`);
  execFileSync(process.execPath, [ACC, '--feature-rows', fr, '--pack-id', packId,
    '--training-tokens', path.join(dir, 'training.txt'), '--cumulative-dir', path.join(dir, 'cum'),
    '--out-dir', out, '--schema-version', 'feat.v1',
    '--prereg', path.join(dir, 'prereg.md'), '--prereg-lock', path.join(dir, 'prereg.sha256')], { stdio: 'pipe' });
  return JSON.parse(fs.readFileSync(path.join(out, 'daily_qa_report.json'), 'utf8'));
}

test('coverage_tally: pack A baseline (1 dog / 1 dud)', () => {
  const d = setup();
  const a = ingest(d, [row('T1', 100, 'dog'), row('U1', 200, 'dud')], 'packA');
  assert.deepEqual(a.cumulative_cohort, { dog: 1, dud: 1 });
  assert.deepEqual(a.cumulative_coverage_tally, { dog: { sol_curve_total: 1, kept: 1 }, dud: { sol_curve_total: 1, kept: 1 } });
  assert.equal(a.cumulative_coverage_asymmetry_pp, 0);
});

test('coverage_tally: pack B different pack-id, SAME signals -> rows + tally UNCHANGED', () => {
  const d = setup();
  ingest(d, [row('T1', 100, 'dog'), row('U1', 200, 'dud')], 'packA');
  const b = ingest(d, [row('T1', 100, 'dog'), row('U1', 200, 'dud')], 'packB');
  assert.deepEqual(b.cumulative_cohort, { dog: 1, dud: 1 }); // cross-pack dedup: no new rows
  assert.deepEqual(b.cumulative_coverage_tally, { dog: { sol_curve_total: 1, kept: 1 }, dud: { sol_curve_total: 1, kept: 1 } }); // NOT inflated
});

test('coverage_tally: same pack-id re-ingest -> idempotent (tally unchanged)', () => {
  const d = setup();
  ingest(d, [row('T1', 100, 'dog'), row('U1', 200, 'dud')], 'packA');
  const again = ingest(d, [row('T1', 100, 'dog'), row('U1', 200, 'dud')], 'packA');
  assert.equal(again.already_ingested, true);
  assert.deepEqual(again.cumulative_coverage_tally, { dog: { sol_curve_total: 1, kept: 1 }, dud: { sol_curve_total: 1, kept: 1 } });
});

test('coverage_tally: pack C one old + one new -> only new contributes', () => {
  const d = setup();
  ingest(d, [row('T1', 100, 'dog'), row('U1', 200, 'dud')], 'packA');
  const c = ingest(d, [row('T1', 100, 'dog'), row('T2', 300, 'dog')], 'packC'); // T1 already-seen, T2 new
  assert.deepEqual(c.cumulative_cohort, { dog: 2, dud: 1 });
  assert.deepEqual(c.cumulative_coverage_tally.dog, { sol_curve_total: 2, kept: 2 }); // T1 NOT double-counted
});

test('coverage_tally: asymmetry gate still fires on a biased (deduped) NEW denominator', () => {
  const d = setup();
  // dog: 2 sol_curve candidates, only 1 complete(kept); dud: 2 complete(kept)
  const a = ingest(d, [row('T1', 100, 'dog', true), row('T2', 101, 'dog', false), row('U1', 200, 'dud', true), row('U2', 201, 'dud', true)], 'packA');
  assert.deepEqual(a.cumulative_coverage_tally, { dog: { sol_curve_total: 2, kept: 1 }, dud: { sol_curve_total: 2, kept: 2 } });
  assert.equal(a.cumulative_coverage_asymmetry_pp, -50); // 0.5 - 1.0
  assert.equal(a.coverage_gate.ok, false); // |50| > 15
});
