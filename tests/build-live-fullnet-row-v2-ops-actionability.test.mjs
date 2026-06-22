import assert from 'node:assert/strict';

import {
  exAnteActionability,
  paperLiveBoundary,
  nullPrecision,
  identityUnitFinality,
  quoteIntentBinding,
  opsBlockedEvidence,
  augmentOpsRow,
  layerOpsClosure,
} from '../scripts/build-live-fullnet-row-v2-ops-actionability.js';

// exAnteActionability — feasibility + earliest actionable + fill anchor
{
  const feas = exAnteActionability({ has_decision: true, quote_clean: true, executable_quote_clean: true, would_enter: true, signal_ts: 1000, first_route_decision_sec_after_signal: 30, kline_first_bar_lag_sec: 12, kline_source: 'gmgn' });
  assert.equal(feas.ex_ante_feasibility_seen, true);
  assert.equal(feas.ex_ante_feasible, true);
  assert.equal(feas.ex_ante_blocked_reason, null);
  assert.equal(feas.earliest_actionable_seen, true);
  assert.equal(feas.earliest_actionable_ts, 1030);
  assert.equal(feas.fill_anchor_seen, true);
  assert.equal(feas.fill_anchor_ts, 1012);
  assert.equal(feas.fill_anchor_source, 'gmgn');

  const notExec = exAnteActionability({ has_decision: true, quote_clean: true, executable_quote_clean: false, signal_ts: 1, first_route_decision_sec_after_signal: null, kline_first_bar_lag_sec: null });
  assert.equal(notExec.ex_ante_feasible, false);
  assert.equal(notExec.ex_ante_blocked_reason, 'quote_not_executable');
  assert.equal(notExec.earliest_actionable_seen, false);
  assert.equal(notExec.earliest_actionable_missing_reason, 'no_decision_event_no_earliest_actionable');

  const noDec = exAnteActionability({ has_decision: false, signal_ts: 1 });
  assert.equal(noDec.ex_ante_feasibility_seen, false);
  assert.equal(noDec.ex_ante_blocked_reason, 'no_decision_event');
}

// paperLiveBoundary — no live execution in window
{
  const none = paperLiveBoundary({ entered: false, would_enter: false, shadow_only: false });
  assert.equal(none.paper_live_boundary_seen, true);
  assert.equal(none.paper_live_execution_scope, 'no_execution');
  assert.equal(none.paper_live_paper_only, true);
  assert.equal(none.paper_live_violation, false);
  assert.equal(paperLiveBoundary({ entered: false, shadow_only: true }).paper_live_execution_scope, 'shadow_only_no_execution');
  // a canonical/live entry would be flagged as a boundary crossing
  const live = paperLiveBoundary({ entered: true, ledger_source: 'canonical_trade_ledger' });
  assert.equal(live.paper_live_violation, true);
  assert.equal(live.paper_live_paper_only, false);
}

// nullPrecision — audits observable numeric fields
{
  assert.deepEqual(nullPrecision({ liquidity_usd: 1000, spread_pct: 2, kline_bars_n: 8 }).null_precision_invalid_numeric_fields, []);
  const bad = nullPrecision({ liquidity_usd: -5, kline_bars_n: 3, matrix_score: Infinity });
  assert.ok(bad.null_precision_invalid_numeric_fields.includes('liquidity_usd:negative'));
  assert.ok(bad.null_precision_invalid_numeric_fields.includes('matrix_score:non_finite'));
  assert.equal(bad.null_precision_seen, true);
}

// identityUnitFinality — from source-to-raw row
{
  const ok = identityUnitFinality({ token_ca: 'T' }, { baseline_price_unit: 'usd_per_token', path_price_unit: 'usd_per_token', same_source_path: 1 });
  assert.equal(ok.identity_unit_finality_seen, true);
  assert.equal(ok.identity_unit, 'usd_per_token');
  assert.equal(ok.identity_finality_reason, 'same_source_baseline_and_path');
  const absent = identityUnitFinality({ token_ca: 'T' }, null);
  assert.equal(absent.identity_unit_finality_seen, false);
  assert.equal(absent.identity_unit_finality_missing_reason, 'source_to_raw_row_absent');
}

// quoteIntentBinding — intent side observable; fill binding absent (no entries)
{
  const seen = quoteIntentBinding({ entry_intent_seen: true, entry_intent_event_id: 'evt9', quote_provider_source: 'jupiter', route_source: 'amm', fill_seen: false });
  assert.equal(seen.quote_intent_binding_seen, true);
  assert.equal(seen.quote_intent_id, 'evt9');
  assert.equal(seen.quote_intent_provider, 'jupiter');
  assert.equal(seen.quote_intent_fill_bound, false);
  assert.equal(seen.quote_intent_binding_missing_reason, 'intent_recorded_no_fill_binding_no_entries_in_window');
  const noIntent = quoteIntentBinding({ entry_intent_seen: false, has_decision: true });
  assert.equal(noIntent.quote_intent_binding_seen, false);
  assert.equal(noIntent.quote_intent_binding_missing_reason, 'no_entry_intent_recorded');
}

// opsBlockedEvidence — worker/parser/idempotency stay blocked with precise reasons
{
  const o = opsBlockedEvidence();
  assert.equal(o.worker_health_seen, false);
  assert.ok(o.worker_health_missing_reason.includes('requires_runtime_readmodel_export'));
  assert.equal(o.parser_session_seen, false);
  assert.equal(o.idempotency_key_seen, false);
}

// augmentOpsRow — additive + preserves prior fields + deterministic
{
  const row = { token_ca: 'T', signal_ts: 1, class: 'dog', has_decision: true, executable_quote_clean: true, would_enter: true, first_route_decision_sec_after_signal: 10, kline_first_bar_lag_sec: 5, kline_source: 'gmgn', entered: false, shadow_only: false, entry_intent_seen: false, kline_seen: true, row_confidence: 'MEDIUM', mode_tier: 'unregistered_unknown', final_repair_owner_v2: 'ENTRY_BRIDGE_GAP' };
  const a = augmentOpsRow(row, { baseline_price_unit: 'native', same_source_path: 1 });
  assert.equal(a.kline_seen, true);          // Goal 2 field intact
  assert.equal(a.mode_tier, 'unregistered_unknown'); // Goal 3 field intact
  assert.equal(a.ex_ante_feasible, true);
  assert.equal(a.identity_unit, 'native');
  assert.equal(a.worker_health_seen, false);
  assert.deepEqual(augmentOpsRow(row, { baseline_price_unit: 'native', same_source_path: 1 }), a); // deterministic
}

// layerOpsClosure — proof-gated reclassification (6 covered); worker stays blocked
{
  const baseClosure = {
    modules: [
      { module_group: 'ex_ante_feasibility', bucket: 'E', coverage_status: 'blocked' },
      { module_group: 'earliest_actionable', bucket: 'E', coverage_status: 'blocked' },
      { module_group: 'paper_live_boundary', bucket: 'D', coverage_status: 'blocked' },
      { module_group: 'null_precision', bucket: 'D', coverage_status: 'blocked' },
      { module_group: 'identity_unit_finality', bucket: 'D', coverage_status: 'blocked' },
      { module_group: 'quote_intent_binding', bucket: 'D', coverage_status: 'blocked' },
      { module_group: 'worker_health', bucket: 'D', coverage_status: 'blocked' },
      { module_group: 'parser_session', bucket: 'D', coverage_status: 'blocked' },
      { module_group: 'mode_readiness', bucket: 'F', coverage_status: 'covered' },
      { module_group: 'ui_rendering', bucket: 'G', coverage_status: 'intentionally_excluded' },
    ],
  };
  const rows = [{ ex_ante_feasibility_seen: true, earliest_actionable_seen: true, paper_live_boundary_seen: true, null_precision_seen: true, identity_unit_finality_seen: true, quote_intent_binding_seen: true }];
  const out = layerOpsClosure(baseClosure, rows);
  const reclassed = out.reclassified_from_blocked_this_layer.map((x) => x.module_group).sort();
  assert.deepEqual(reclassed, ['earliest_actionable', 'ex_ante_feasibility', 'identity_unit_finality', 'null_precision', 'paper_live_boundary', 'quote_intent_binding']);
  assert.equal(out.coverage_status_counts.covered, 7);   // 1 base + 6
  assert.equal(out.coverage_status_counts.blocked, 2);   // worker_health + parser_session
  assert.equal(out.coverage_status_counts.intentionally_excluded, 1);
  assert.equal(out.every_module_mapped_to_A_G, true);
  const worker = out.modules.find((m) => m.module_group === 'worker_health');
  assert.ok(String(worker.reason_for_exclusion).includes('requires_runtime_readmodel_export'));
}

console.log('build-live-fullnet-row-v2-ops-actionability tests passed');
