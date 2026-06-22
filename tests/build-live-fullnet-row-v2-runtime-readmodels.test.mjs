import assert from 'node:assert/strict';

import { windowValidity, ALL_MODULES, NO_SOURCE_REASON } from '../scripts/build-runtime-readmodel-window-export.js';
import {
  moduleRuntimeFields,
  runtimeReadmodelRow,
  layerRuntimeClosure,
  augmentRuntimeRow,
  RUNTIME_MODULES,
} from '../scripts/build-live-fullnet-row-v2-runtime-readmodels.js';

// ---- script 1: windowValidity + module coverage map -------------------------------------------
{
  assert.equal(windowValidity(1500, 1000, 2000).same_window_valid, true);
  const post = windowValidity(9000, 1000, 2000);
  assert.equal(post.same_window_valid, false);
  assert.ok(post.reason.includes('after_window_end'));
  const pre = windowValidity(10, 1000, 2000);
  assert.equal(pre.same_window_valid, false);
  assert.ok(pre.reason.includes('before_window_start'));
  assert.equal(windowValidity(null, 1000, 2000).same_window_valid, false);
  assert.equal(ALL_MODULES.length, 13);
  // every no-source module has a precise reason naming a required export
  for (const m of ['gmgn_policy', 'source_resonance', 'idempotency_write_path', 'detector_calibration']) {
    assert.ok(NO_SOURCE_REASON[m] && /requires_|export/.test(NO_SOURCE_REASON[m]), `precise reason for ${m}`);
  }
}

// ---- script 2: moduleRuntimeFields (namespaced; HIGH/MEDIUM cover, LOW/cross-window do not) -----
const idx = {
  healthByModule: {
    parser_session: { module_group: 'parser_session', status: 'clean', note: 'raw-observation ingestion session' },
    worker_health: { module_group: 'worker_health', status: 'stale', missing_reason: 'updated_at_post_window' },
    gmgn_policy: { module_group: 'gmgn_policy', status: 'missing', missing_reason: 'no_gmgn_policy_readmodel__requires_gmgn_policy_decision_readmodel_export' },
  },
  bySignal: new Map([
    ['parser_session|T|1000', { module_group: 'parser_session', token_ca: 'T', signal_ts: 1000, signal_source_id: 'sig9', evidence_source: 'raw_signal_observations', evidence_ts: 1000, join_confidence: 'HIGH', same_window_valid: true, payload_json: JSON.stringify({ status: 'matured', provider: 'gmgn', first_bar_lag_sec: 7 }) }],
  ]),
  byToken: new Map(),
  byModuleGlobal: new Map([
    ['worker_health', { module_group: 'worker_health', evidence_source: 'raw_path_observer_provider_state', evidence_ts: 9999, join_confidence: 'LOW', same_window_valid: false, payload_json: '{}' }],
  ]),
};
{
  const row = { token_ca: 'T', signal_ts: 1000 };
  const ps = moduleRuntimeFields('parser_session', row, idx);
  assert.equal(ps.seen, true);
  assert.equal(ps.fields.parser_session_runtime_seen, true);
  assert.equal(ps.fields.parser_session_runtime_join_confidence, 'HIGH');
  assert.equal(ps.fields.parser_session_runtime_source, 'raw_signal_observations');
  assert.equal(ps.fields.parser_session_runtime_session_id, 'sig9');
  assert.equal(ps.fields.parser_session_runtime_lag_sec, 7);
  assert.ok(ps.fields.parser_session_runtime_payload_hash); // hash present
  // worker_health: window-global but cross-window (same_window_valid=false) -> NOT seen
  const wh = moduleRuntimeFields('worker_health', row, idx);
  assert.equal(wh.seen, false);
  assert.equal(wh.fields.worker_health_runtime_join_confidence, 'LOW');
  assert.ok(wh.fields.worker_health_runtime_missing_reason.includes('post_window'));
  // gmgn_policy: no evidence -> not seen + precise missing reason
  const gm = moduleRuntimeFields('gmgn_policy', row, idx);
  assert.equal(gm.seen, false);
  assert.ok(gm.fields.gmgn_policy_runtime_missing_reason.includes('requires_gmgn_policy'));
}

// runtimeReadmodelRow — aggregate covered/blocked + all fields namespaced (no Goal1-5 collision risk)
{
  const row = { token_ca: 'T', signal_ts: 1000 };
  const f = runtimeReadmodelRow(row, idx);
  assert.deepEqual(f.runtime_readmodel_covered_modules, ['parser_session']);
  assert.equal(f.runtime_readmodel_still_blocked_modules.length, 12);
  assert.equal(f.runtime_readmodel_confidence, 'PARTIAL_SAME_WINDOW');
  // every new key is namespaced -> cannot collide with Goal 1-5
  assert.ok(Object.keys(f).every((k) => /_runtime_|^runtime_readmodel_/.test(k)));
}

// augmentRuntimeRow — preserves prior fields + deterministic
{
  const row = { token_ca: 'T', signal_ts: 1000, class: 'dog', kline_seen: true, mode_tier: 'x', ex_ante_feasible: true, paper_path_samples_seen: false, gmgn_policy_seen: false, gmgn_policy_missing_reason: 'GOAL5' };
  const a = augmentRuntimeRow(row, idx);
  assert.equal(a.kline_seen, true);
  assert.equal(a.gmgn_policy_missing_reason, 'GOAL5'); // Goal-5 field NOT overwritten
  assert.equal(a.parser_session_runtime_seen, true);
  assert.deepEqual(augmentRuntimeRow(row, idx), a);
}

// layerRuntimeClosure — cover parser_session (evidence), refine others, leave zeabur untouched
{
  const finalClosure = {
    modules: [
      { module_group: 'parser_session', bucket: 'D', coverage_status: 'blocked' },
      { module_group: 'gmgn_policy', bucket: 'B', coverage_status: 'blocked' },
      { module_group: 'smart_money', bucket: 'B', coverage_status: 'blocked', reason_for_exclusion: 'zeabur_export_required::external wallets' },
      { module_group: 'curve_pumpfun', bucket: 'B', coverage_status: 'blocked', reason_for_exclusion: 'zeabur_export_required::on-chain curve decode' },
      { module_group: 'kline_cache', bucket: 'C', coverage_status: 'covered' },
      { module_group: 'ui_rendering', bucket: 'G', coverage_status: 'intentionally_excluded' },
    ],
  };
  const rows = [{ parser_session_runtime_seen: true, gmgn_policy_runtime_seen: false }];
  const out = layerRuntimeClosure(finalClosure, rows, idx.healthByModule);
  assert.deepEqual(out.reclassified_from_blocked_this_layer.map((x) => x.module_group), ['parser_session']);
  assert.equal(out.coverage_status_counts.covered, 2); // kline_cache + parser_session
  assert.equal(out.smart_money_and_curve_pumpfun_still_zeabur, true);
  const gm = out.modules.find((m) => m.module_group === 'gmgn_policy');
  assert.ok(gm.reason_for_exclusion.startsWith('runtime_readmodel_required::'));
  const sm = out.modules.find((m) => m.module_group === 'smart_money');
  assert.ok(/zeabur/i.test(sm.reason_for_exclusion)); // untouched
  assert.equal(RUNTIME_MODULES.length, 13);
}

console.log('build-live-fullnet-row-v2-runtime-readmodels tests passed');
