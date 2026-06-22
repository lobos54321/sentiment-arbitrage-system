import assert from 'node:assert/strict';

import {
  moduleRepairFields,
  runtimeRepairRow,
  augmentRepairRow,
  layerRepairClosure,
  REPAIR_MODULES,
} from '../scripts/build-live-fullnet-row-v2-runtime-materializer-repair.js';

const idx = {
  healthByModule: {
    scout_quality: { module_group: 'scout_quality', status: 'repaired_and_joined', joined_n: 188 },
    detector_calibration: { module_group: 'detector_calibration', status: 'repaired_and_joined', joined_n: 110 },
    idempotency_write_path: { module_group: 'idempotency_write_path', status: 'repaired_and_joined', joined_n: 29 },
    gmgn_policy: { module_group: 'gmgn_policy', status: 'still_missing', joined_n: 0, missing_reason: 'FINAL: no gmgn_policy source_component; requires runtime gmgn_policy decision emit' },
    worker_health: { module_group: 'worker_health', status: 'still_missing', joined_n: 0, missing_reason: 'FINAL: only post-window provider state' },
  },
  bySignal: new Map([
    ['scout_quality|T|1000', { module_group: 'scout_quality', token_ca: 'T', signal_ts: 1000, evidence_ts: 1060, join_confidence: 'HIGH', same_window_valid: true, evidence_source: 'a_class_decision_event[source_component=scout_quality]', payload_json: JSON.stringify({ scout_quality_score: 0, scout_quality_grade: 'REJECT', scout_quality_block_reason: 'hard_prefilter_failed' }) }],
    ['detector_calibration|T|1000', { module_group: 'detector_calibration', token_ca: 'T', signal_ts: 1000, evidence_ts: 1060, join_confidence: 'HIGH', same_window_valid: true, evidence_source: 'expected_rr_detail', payload_json: JSON.stringify({ model_version: 'v1.x', calibration_bucket: 'A_PLUS' }) }],
  ]),
};

// moduleRepairFields — covered (HIGH same-window) vs FINAL-blocked; namespaced fields
{
  const row = { token_ca: 'T', signal_ts: 1000 };
  const sq = moduleRepairFields('scout_quality', row, idx);
  assert.equal(sq.seen, true);
  assert.equal(sq.fields.scout_quality_repair_seen, true);
  assert.equal(sq.fields.scout_quality_repair_status, 'repaired_and_joined');
  assert.equal(sq.fields.scout_quality_repair_grade, 'REJECT');
  assert.equal(sq.fields.scout_quality_repair_block_reason, 'hard_prefilter_failed');
  assert.ok(sq.fields.scout_quality_repair_payload_hash);

  const gm = moduleRepairFields('gmgn_policy', row, idx);
  assert.equal(gm.seen, false);
  assert.ok(gm.fields.gmgn_policy_repair_missing_reason.startsWith('FINAL:'));

  const dc = moduleRepairFields('detector_calibration', row, idx);
  assert.equal(dc.fields.detector_calibration_repair_model_version, 'v1.x');
}

// runtimeRepairRow — aggregate + all keys namespaced (no Goal1-7 collision)
{
  const f = runtimeRepairRow({ token_ca: 'T', signal_ts: 1000 }, idx);
  assert.ok(f.runtime_materializer_repair_covered_modules.includes('scout_quality'));
  assert.ok(f.runtime_materializer_repair_covered_modules.includes('detector_calibration'));
  assert.ok(f.runtime_materializer_repair_still_blocked_modules.includes('gmgn_policy'));
  assert.ok(Object.keys(f).every((k) => /_repair_|^runtime_materializer_repair_/.test(k)));
}

// augmentRepairRow — preserves prior fields (incl. Goal-5 scout_quality_score) + deterministic
{
  const row = { token_ca: 'T', signal_ts: 1000, class: 'dog', kline_seen: true, token_memory_materialized_seen: true, scout_quality_score: null /* Goal-5 bare field */, final_repair_owner_v2: 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP' };
  const a = augmentRepairRow(row, idx);
  assert.equal(a.kline_seen, true);                       // Goal 2
  assert.equal(a.token_memory_materialized_seen, true);   // Goal 7
  assert.equal(a.scout_quality_score, null);              // Goal-5 bare field NOT overwritten
  assert.equal(a.scout_quality_repair_seen, true);        // Goal-8 namespaced field added
  assert.deepEqual(augmentRepairRow(row, idx), a);
}

// layerRepairClosure — reclassify repaired; FINAL for the rest; zeabur untouched; all_runtime_final
{
  const closure = {
    modules: [
      { module_group: 'scout_quality', bucket: 'B', coverage_status: 'blocked' },
      { module_group: 'detector_calibration', bucket: 'E', coverage_status: 'blocked' },
      { module_group: 'idempotency_write_path', bucket: 'D', coverage_status: 'blocked' },
      { module_group: 'gmgn_policy', bucket: 'B', coverage_status: 'blocked' },
      { module_group: 'worker_health', bucket: 'D', coverage_status: 'blocked' },
      { module_group: 'smart_money', bucket: 'B', coverage_status: 'blocked', reason_for_exclusion: 'zeabur_export_required::wallets' },
      { module_group: 'curve_pumpfun', bucket: 'B', coverage_status: 'blocked', reason_for_exclusion: 'zeabur_export_required::curve' },
      { module_group: 'token_memory', bucket: 'B', coverage_status: 'covered' },
      { module_group: 'ui_rendering', bucket: 'G', coverage_status: 'intentionally_excluded' },
    ],
  };
  const rows = [{ scout_quality_repair_seen: true, detector_calibration_repair_seen: true, idempotency_write_path_repair_seen: true, gmgn_policy_repair_seen: false, worker_health_repair_seen: false }];
  const out = layerRepairClosure(closure, rows, idx.healthByModule);
  assert.deepEqual(out.reclassified_from_blocked_this_layer.map((x) => x.module_group).sort(), ['detector_calibration', 'idempotency_write_path', 'scout_quality']);
  assert.equal(out.coverage_status_counts.covered, 4); // token_memory + 3 reclassified
  assert.equal(out.smart_money_and_curve_pumpfun_still_zeabur, true);
  assert.equal(out.no_generic_blocked_reason_remains, true);
  assert.equal(out.all_remaining_runtime_blockers_have_final_reason, true); // gmgn_policy + worker_health are FINAL
  const gm = out.modules.find((m) => m.module_group === 'gmgn_policy');
  assert.ok(gm.reason_for_exclusion.startsWith('FINAL:'));
  assert.equal(REPAIR_MODULES.length, 9);
}

console.log('build-live-fullnet-row-v2-runtime-materializer-repair tests passed');
