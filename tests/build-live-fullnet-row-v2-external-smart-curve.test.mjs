import assert from 'node:assert/strict';
import test from 'node:test';

import {
  moduleExternalFields,
  externalRow,
  augmentExternalRow,
  layerExternalClosure,
  EXTERNAL_MODULES,
} from '../scripts/build-live-fullnet-row-v2-external-smart-curve.js';

// No-evidence index (the real Goal-9 state: nothing joined) -> both modules carry a FINAL reason.
const extNone = {
  healthByModule: {
    smart_money: { module_group: 'smart_money', status: 'EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON', joined_n: 0, missing_reason: 'FINAL: no same-window smart-money export' },
    curve_pumpfun: { module_group: 'curve_pumpfun', status: 'EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON', joined_n: 0, missing_reason: 'FINAL: no same-window curve decode' },
  },
  bySignal: new Map(),
};
// With-evidence index: smart_money joined HIGH same-window for (T,1000).
const extJoined = {
  healthByModule: {
    smart_money: { module_group: 'smart_money', status: 'EXTERNAL_EVIDENCE_JOINED', joined_n: 1, missing_reason: null },
    curve_pumpfun: { module_group: 'curve_pumpfun', status: 'EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON', joined_n: 0, missing_reason: 'FINAL: no same-window curve decode' },
  },
  bySignal: new Map([['smart_money|T|1000', { module_group: 'smart_money', token_ca: 'T', signal_ts: 1000, evidence_ts: 1000, same_window_valid: true, join_confidence: 'HIGH', evidence_source: 'external_smart_money_export', payload_json: JSON.stringify({ wallet_signal_score: 0.8 }), missing_reason: null }]]),
};

test('EXTERNAL_MODULES are exactly smart_money + curve_pumpfun', () => {
  assert.deepEqual([...EXTERNAL_MODULES].sort(), ['curve_pumpfun', 'smart_money']);
});

test('moduleExternalFields: FINAL when no evidence; covered when joined; all keys namespaced', () => {
  const row = { token_ca: 'T', signal_ts: 1000 };
  const noev = moduleExternalFields('smart_money', row, extNone);
  assert.equal(noev.seen, false);
  assert.equal(noev.fields.smart_money_external_seen, false);
  assert.equal(noev.fields.smart_money_external_status, 'EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON');
  assert.ok(noev.fields.smart_money_external_missing_reason.startsWith('FINAL:'));

  const yes = moduleExternalFields('smart_money', row, extJoined);
  assert.equal(yes.seen, true);
  assert.equal(yes.fields.smart_money_external_status, 'EXTERNAL_EVIDENCE_JOINED');
  assert.equal(yes.fields.smart_money_external_join_confidence, 'HIGH');
  assert.ok(yes.fields.smart_money_external_payload_hash);
  assert.equal(yes.fields.smart_money_external_missing_reason, null);
  assert.ok(Object.keys(yes.fields).every((k) => k.startsWith('smart_money_external_')));
});

test('externalRow aggregates; covered vs blocked split', () => {
  const f = externalRow({ token_ca: 'T', signal_ts: 1000 }, extJoined);
  assert.deepEqual(f.external_smart_curve_covered_modules, ['smart_money']);
  assert.deepEqual(f.external_smart_curve_still_blocked_modules, ['curve_pumpfun']);
  assert.equal(f.external_smart_curve_confidence, 'MEDIUM'); // 1 of 2 covered
  assert.ok(Object.keys(f).every((k) => /_external_|^external_smart_curve_/.test(k)));
});

test('augmentExternalRow preserves Goal 1-8 fields + deterministic', () => {
  const row = { token_ca: 'T', signal_ts: 1000, class: 'dog', kline_seen: true, scout_quality_repair_seen: true, smart_money_external_seen: 'STALE' /* must be overwritten by namespace */ };
  const a = augmentExternalRow(row, extNone);
  assert.equal(a.kline_seen, true);                 // Goal 2 preserved
  assert.equal(a.scout_quality_repair_seen, true);  // Goal 8 preserved
  assert.equal(a.smart_money_external_seen, false);  // Goal 9 namespaced field set
  assert.deepEqual(augmentExternalRow(row, extNone), a);
});

test('layerExternalClosure: no evidence => both FINAL + resolved; with evidence => reclassify covered', () => {
  const g8Closure = {
    modules: [
      { module_group: 'smart_money', bucket: 'B', coverage_status: 'blocked', reason_for_exclusion: 'zeabur_export_required::wallets' },
      { module_group: 'curve_pumpfun', bucket: 'B', coverage_status: 'blocked', reason_for_exclusion: 'zeabur_export_required::curve' },
      { module_group: 'gmgn_policy', bucket: 'B', coverage_status: 'blocked', reason_for_exclusion: 'FINAL: no gmgn_policy component' },
      { module_group: 'token_memory', bucket: 'B', coverage_status: 'covered' },
      { module_group: 'ui_rendering', bucket: 'G', coverage_status: 'intentionally_excluded' },
    ],
  };
  const none = layerExternalClosure(g8Closure, extNone);
  assert.equal(none.smart_money_and_curve_pumpfun_resolved, true);              // both final-blocked counts as resolved
  assert.deepEqual(none.smart_money_and_curve_pumpfun_status, { smart_money: 'final_blocked', curve_pumpfun: 'final_blocked' });
  assert.equal(none.all_blocked_have_final_reason, true);                       // all 3 blocked now FINAL
  assert.equal(none.no_generic_blocked_reason_remains, true);
  assert.equal(none.reclassified_from_blocked_this_layer.length, 0);
  const sm = none.modules.find((m) => m.module_group === 'smart_money');
  assert.ok(sm.reason_for_exclusion.startsWith('FINAL:'));                      // generic zeabur replaced by FINAL

  const joined = layerExternalClosure(g8Closure, extJoined);
  assert.deepEqual(joined.reclassified_from_blocked_this_layer.map((x) => x.module_group), ['smart_money']);
  assert.equal(joined.coverage_status_counts.covered, 2);                       // token_memory + smart_money
  assert.equal(joined.smart_money_and_curve_pumpfun_resolved, true);           // smart covered, curve FINAL
});
