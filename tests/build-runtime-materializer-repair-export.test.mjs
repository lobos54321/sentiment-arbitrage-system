import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { buildRepairExport, REPAIR_MODULES, FINAL_BLOCKED } from '../scripts/build-runtime-materializer-repair-export.js';

// module set + FINAL reasons
{
  assert.equal(REPAIR_MODULES.length, 9);
  assert.equal(Object.keys(FINAL_BLOCKED).length, 6); // 9 - 3 materializable
  for (const m of Object.keys(FINAL_BLOCKED)) {
    assert.ok(FINAL_BLOCKED[m].startsWith('FINAL:'), `final reason for ${m}`);
    assert.ok(/requires_|emit|not |only /.test(FINAL_BLOCKED[m]), `exact gap for ${m}`);
  }
}

// buildRepairExport — materialize scout_quality / detector_calibration / idempotency from structured
// decision sub-objects, joined via the v1 matcher (lifecycle_id signal_ts). The 6 others stay FINAL.
{
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'g8-'));
  const START = 1781942715; const END = 1782029115; const SIG = 1781950000;
  // source-to-raw cohort (one signal, with volume for scout volume_score)
  fs.writeFileSync(path.join(dir, 's2r.json'), JSON.stringify([
    { token_ca: 'T', signal_ts: SIG, source_id: 1, hard_gate_status: 'PASS', raw_tier: 'gold', volume_24h: 50000, market_cap: 200000 },
  ]));
  // a-class events: lifecycle_id encodes signal_ts so the v1 matcher joins; structured sub-objects present
  fs.writeFileSync(path.join(dir, 'aev.json'), JSON.stringify({ events: [
    { id: 9001, token_ca: 'T', lifecycle_id: `T:${SIG}`, event_ts: SIG + 60, source_component: 'scout_quality', score: 0, grade: 'REJECT', reason: 'hard_prefilter_failed', risk: { liquidity_usd: 1200, quote_clean_verified: false, opportunity_key: 'T:opp:1', duplicate_of_event_id: 192459 }, expected_rr_detail: { rr_version: 'v1.a_class_2_to_1', rr_grade: 'A_PLUS', expected_rr: 5 }, ai_review: { ai_grade: 'B' } },
  ] }));
  // empty canonical ledger decision events
  fs.writeFileSync(path.join(dir, 'led.json'), JSON.stringify({ tables: { a_class_decision_events: { rows: [] } } }));

  const out = buildRepairExport({ windowStartTs: START, windowEndTs: END, sourceToRaw: path.join(dir, 's2r.json'), aClassEvents: path.join(dir, 'aev.json'), ledgerExport: path.join(dir, 'led.json') });
  const byModule = {}; for (const r of out.joined) byModule[r.module_group] = r;
  assert.ok(byModule.scout_quality, 'scout_quality materialized');
  assert.equal(JSON.parse(byModule.scout_quality.payload_json).scout_quality_grade, 'REJECT');
  assert.equal(JSON.parse(byModule.scout_quality.payload_json).volume_score, 50000);
  assert.ok(byModule.detector_calibration, 'detector_calibration materialized');
  assert.equal(JSON.parse(byModule.detector_calibration.payload_json).model_version, 'v1.a_class_2_to_1');
  assert.equal(JSON.parse(byModule.detector_calibration.payload_json).calibration_bucket, 'A_PLUS');
  assert.ok(byModule.idempotency_write_path, 'idempotency materialized');
  assert.equal(JSON.parse(byModule.idempotency_write_path.payload_json).dedupe_result, 'duplicate_of_event_id:192459');
  // the 6 others are still_missing with FINAL reasons
  const health = Object.fromEntries(out.health.modules.map((h) => [h.module_group, h]));
  for (const m of ['gmgn_policy', 'source_resonance', 'worker_health', 'training_manifest', 'holdout_negative_controls', 'assumptions_false_negative_budget']) {
    assert.equal(health[m].status, 'still_missing', `${m} still_missing`);
    assert.ok(health[m].missing_reason.startsWith('FINAL:'), `${m} FINAL reason`);
  }
  assert.deepEqual(out.health.repaired_modules.sort(), ['detector_calibration', 'idempotency_write_path', 'scout_quality']);
  fs.rmSync(dir, { recursive: true, force: true });
}

console.log('build-runtime-materializer-repair-export tests passed');
