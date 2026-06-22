import assert from 'node:assert/strict';

import {
  moduleMaterializedFields,
  runtimeMaterializedRow,
  augmentMaterializedRow,
  layerMaterializedClosure,
  TARGET_MODULES,
} from '../scripts/build-live-fullnet-row-v2-runtime-materialized.js';

const idx = {
  healthByModule: {
    token_memory: { module_group: 'token_memory', status: 'MATERIALIZED_AND_JOINED', joined_n: 329 },
    watchlist: { module_group: 'watchlist', status: 'MATERIALIZED_AND_JOINED', joined_n: 13 },
    evidence_conflict_aging: { module_group: 'evidence_conflict_aging', status: 'MATERIALIZED_AND_JOINED', joined_n: 329 },
    gmgn_policy: { module_group: 'gmgn_policy', status: 'MATERIALIZER_MISSING_OR_INVALID', joined_n: 0, missing_reason: 'materializer_ran: no structured gmgn_policy decision; requires runtime emit' },
  },
  bySignal: new Map([
    ['token_memory|A|1000', { module_group: 'token_memory', token_ca: 'A', signal_ts: 1000, evidence_ts: 900, join_confidence: 'MEDIUM', same_window_valid: true, evidence_source: 'derived:in_window_same_token_signal_history', payload_json: JSON.stringify({ memory_state: 'prior_in_window_signals', prior_in_window_signal_n: 2 }) }],
    ['evidence_conflict_aging|A|1000', { module_group: 'evidence_conflict_aging', token_ca: 'A', signal_ts: 1000, evidence_ts: 1000, join_confidence: 'HIGH', same_window_valid: true, evidence_source: 'derived', payload_json: JSON.stringify({ evidence_age_sec: 500, conflict_state: 'no_conflict_detected' }) }],
  ]),
};

// moduleMaterializedFields — covered (HIGH/MEDIUM same-window) vs blocked (materializer reason)
{
  const row = { token_ca: 'A', signal_ts: 1000 };
  const tm = moduleMaterializedFields('token_memory', row, idx);
  assert.equal(tm.seen, true);
  assert.equal(tm.fields.token_memory_materialized_seen, true);
  assert.equal(tm.fields.token_memory_materialized_join_confidence, 'MEDIUM');
  assert.equal(tm.fields.token_memory_materialized_status, 'MATERIALIZED_AND_JOINED');
  assert.equal(tm.fields.token_memory_materialized_prior_n, 2);
  assert.ok(tm.fields.token_memory_materialized_payload_hash);

  const gm = moduleMaterializedFields('gmgn_policy', row, idx);
  assert.equal(gm.seen, false);
  assert.ok(gm.fields.gmgn_policy_materialized_missing_reason.includes('requires runtime emit'));
  // a module with no evidence for THIS row but no health entry -> still not seen + a reason
  const wl = moduleMaterializedFields('watchlist', row, idx);
  assert.equal(wl.seen, false); // no bySignal entry for watchlist|A|1000
}

// runtimeMaterializedRow — aggregate lists + all keys namespaced (no Goal1-6 collision)
{
  const f = runtimeMaterializedRow({ token_ca: 'A', signal_ts: 1000 }, idx);
  assert.ok(f.runtime_materialized_covered_modules.includes('token_memory'));
  assert.ok(f.runtime_materialized_covered_modules.includes('evidence_conflict_aging'));
  assert.ok(f.runtime_materialized_still_blocked_modules.includes('gmgn_policy'));
  assert.equal(f.runtime_materialized_confidence, 'PARTIAL_SAME_WINDOW');
  assert.ok(Object.keys(f).every((k) => /_materialized_|^runtime_materialized_/.test(k)));
}

// augmentMaterializedRow — preserves prior fields + deterministic
{
  const row = { token_ca: 'A', signal_ts: 1000, class: 'dog', kline_seen: true, mode_tier: 'x', ex_ante_feasible: true, parser_session_runtime_seen: true, token_memory_failure_type: null /* Goal-5 field */ };
  const a = augmentMaterializedRow(row, idx);
  assert.equal(a.kline_seen, true);
  assert.equal(a.parser_session_runtime_seen, true);     // Goal-6 field intact
  assert.equal(a.token_memory_failure_type, null);       // Goal-5 field NOT overwritten
  assert.equal(a.token_memory_materialized_seen, true);  // Goal-7 namespaced field added
  assert.deepEqual(augmentMaterializedRow(row, idx), a);
}

// layerMaterializedClosure — reclassify materialized modules; refine others; zeabur untouched
{
  const closure = {
    modules: [
      { module_group: 'token_memory', bucket: 'B', coverage_status: 'blocked' },
      { module_group: 'evidence_conflict_aging', bucket: 'F', coverage_status: 'blocked' },
      { module_group: 'watchlist', bucket: 'C', coverage_status: 'blocked' },
      { module_group: 'gmgn_policy', bucket: 'B', coverage_status: 'blocked' },
      { module_group: 'smart_money', bucket: 'B', coverage_status: 'blocked', reason_for_exclusion: 'zeabur_export_required::wallets' },
      { module_group: 'curve_pumpfun', bucket: 'B', coverage_status: 'blocked', reason_for_exclusion: 'zeabur_export_required::curve' },
      { module_group: 'parser_session', bucket: 'D', coverage_status: 'covered' },
      { module_group: 'ui_rendering', bucket: 'G', coverage_status: 'intentionally_excluded' },
    ],
  };
  const rows = [{ token_memory_materialized_seen: true, evidence_conflict_aging_materialized_seen: true, watchlist_materialized_seen: true, gmgn_policy_materialized_seen: false }];
  const out = layerMaterializedClosure(closure, rows, idx.healthByModule);
  assert.deepEqual(out.reclassified_from_blocked_this_layer.map((x) => x.module_group).sort(), ['evidence_conflict_aging', 'token_memory', 'watchlist']);
  assert.equal(out.coverage_status_counts.covered, 4); // parser_session + 3 reclassified
  assert.equal(out.smart_money_and_curve_pumpfun_still_zeabur, true);
  assert.equal(out.no_generic_blocked_reason_remains, true);
  const gm = out.modules.find((m) => m.module_group === 'gmgn_policy');
  assert.ok(gm.reason_for_exclusion.startsWith('materializer::'));
  assert.equal(TARGET_MODULES.length, 12);
}

console.log('build-live-fullnet-row-v2-runtime-materialized tests passed');
