import assert from 'node:assert/strict';

import {
  REPAIR_LADDER,
  MODULE_CLOSURE_MAP,
  VALID_BUCKETS,
  finalRepairOwner,
  rowConfidence,
  rowModuleClosure,
  moduleClosureCoverageReport,
  evGate,
} from '../scripts/build-live-fullnet-row-v2.js';

const base = (o = {}) => ({
  token_ca: 'Tkn', signal_ts: 1000, class: 'dud', source_seen: true, source_hard_gate_status: 'PASS',
  raw_missing_reason: null, has_decision: true, quote_seen: true, quote_clean: true, route_missing_reason: null,
  hard_blockers: [], readiness_ok: true, would_enter: false, entered: false, shadow_only: false,
  ledger_seen: false, exit_ts: null, round_trip_friction_pct: null, realized_pnl_pct: null,
  held_to_silver_or_gold: false, ...o,
});

// ---- §15.15.20 final repair-owner ladder: deterministic + every output on the ladder -----------
{
  const captured = base({ entered: true, ledger_seen: true, exit_ts: 1100, round_trip_friction_pct: 5, realized_pnl_pct: 20 });
  assert.equal(finalRepairOwner(captured), 'NONE_NO_REPAIR_NEEDED');

  assert.equal(finalRepairOwner(base({ signal_ts: null })), 'ROW_IDENTITY_UNIT_OR_DENOMINATOR_INVALID');
  assert.equal(finalRepairOwner(base({ source_seen: false })), 'SOURCE_PARSER_SESSION_OR_REGISTRY_GAP');
  assert.equal(finalRepairOwner(base({ raw_missing_reason: 'no_exact_raw_row' })), 'FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP');
  assert.equal(finalRepairOwner(base({ source_hard_gate_status: 'NOT_ATH_PREBUY_KLINE_UNKNOWN_DATA_BLOCKED' })), 'FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP');
  assert.equal(finalRepairOwner(base({ quote_clean: false, route_missing_reason: 'quote_source_missing' })), 'QUOTE_INTENT_PROVIDER_EXECUTOR_OR_IDEMPOTENCY_GAP');
  assert.equal(finalRepairOwner(base({ would_enter: false, readiness_ok: false })), 'MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP');
  assert.equal(finalRepairOwner(base({ would_enter: false, readiness_ok: true })), 'DIRECT_STRATEGY_DECISION_MODULE_GAP');
  assert.equal(finalRepairOwner(base({ would_enter: true, entered: false, shadow_only: false })), 'ENTRY_BRIDGE_GAP');
  assert.equal(finalRepairOwner(base({ would_enter: true, entered: false, shadow_only: true })), 'SHADOW_ONLY_OR_ADVISORY_ONLY');
  assert.equal(finalRepairOwner(base({ would_enter: true, entered: true, ledger_seen: false })), 'LEDGER_EXIT_HOLD_FRICTION_GAP');

  // determinism
  const r = base({ would_enter: true, entered: false });
  assert.equal(finalRepairOwner(r), finalRepairOwner(r));
}

// every MODULE_CLOSURE_MAP repair_owner is a real ladder owner; every bucket is A-G.
{
  for (const m of MODULE_CLOSURE_MAP) {
    assert.ok(VALID_BUCKETS.has(m.bucket), `bucket ${m.bucket} valid for ${m.group}`);
    assert.ok(REPAIR_LADDER.includes(m.repair_owner), `repair_owner ${m.repair_owner} on ladder for ${m.group}`);
  }
}

// ---- §15.14.14 row confidence ----------------------------------------------------------------
{
  // entered without valid ledger/fill/exit/friction => INVALID_FOR_EV
  const c1 = rowConfidence(base({ entered: true, ledger_seen: false }));
  assert.equal(c1.row_confidence, 'INVALID_FOR_EV');
  assert.equal(c1.ev_eligible, false);

  // no decision => LOW
  assert.equal(rowConfidence(base({ has_decision: false, quote_seen: false })).row_confidence, 'LOW');

  // full strategy chain but ops/promotion not exported => MEDIUM (HIGH unreachable on current data)
  assert.equal(rowConfidence(base({})).row_confidence, 'MEDIUM');

  // ops + promotion present => HIGH (reachable only if those modules are exported)
  assert.equal(rowConfidence(base({ ops_evidence_present: true, promotion_evidence_present: true })).row_confidence, 'HIGH');

  // identity invalid => LOW
  assert.equal(rowConfidence(base({ token_ca: '', signal_ts: null })).row_confidence, 'LOW');

  // deterministic
  const r = base({});
  assert.deepEqual(rowConfidence(r), rowConfidence(r));
}

// ---- EV gate (§15.15.22 #7): actual EV null unless a valid entered/fill/exit/ledger row exists --
{
  const noneEligible = evGate([base({}), base({ would_enter: true })]);
  assert.equal(noneEligible.actual_net_ev_pct, null);
  assert.equal(noneEligible.gate, 'BLOCKED_NO_VALID_ENTERED_FILL_EXIT_LEDGER');

  const eligibleRow = base({ entered: true, ledger_seen: true, exit_ts: 1200, round_trip_friction_pct: 6, realized_pnl_pct: 30, net_pnl_pct: 24, formal_sustained_dog: true });
  // confirm it is ev_eligible per rowConfidence
  assert.equal(rowConfidence(eligibleRow).ev_eligible, true);
  const withEv = evGate([{ ...eligibleRow, ev_eligible: true }]);
  assert.equal(withEv.actual_net_ev_pct, 24);
  assert.equal(withEv.gate, 'ACTUAL_NET_EV_AVAILABLE');
}

// ---- module-closure coverage: every module mapped to A-G; bucket G all excluded ----------------
{
  const rep = moduleClosureCoverageReport([]);
  assert.equal(rep.every_module_mapped_to_A_G, true);
  assert.equal(rep.bucket_G_all_intentionally_excluded, true);
  assert.ok(rep.modules.length >= 50);
  assert.ok(rep.coverage_status_counts.covered > 0 && rep.coverage_status_counts.blocked > 0 && rep.coverage_status_counts.intentionally_excluded > 0);
  for (const b of ['A', 'B', 'C', 'D', 'E', 'F', 'G']) assert.ok(Array.isArray(rep.buckets[b]) && rep.buckets[b].length > 0, `bucket ${b} populated`);
}

// ---- rowModuleClosure funnel consistency: no decision => downstream links false ----------------
{
  const f = rowModuleClosure(base({ has_decision: false, quote_clean: false, would_enter: false, entered: false, ledger_seen: false }));
  assert.equal(f.decision, false);
  assert.equal(f.quote, false);
  assert.equal(f.would_enter, false);
  assert.equal(f.entry, false);
  assert.equal(f.ledger, false);
  assert.equal(f.source, true);
}

console.log('build-live-fullnet-row-v2 tests passed');
