import assert from 'node:assert/strict';

import {
  buildModeRegistry,
  modeReadinessProjection,
  modeReadinessRepairDetail,
  augmentModeRow,
  layerModeClosure,
  MODE_OWNER,
} from '../scripts/build-live-fullnet-row-v2-mode-readiness.js';

const registryJson = {
  tiers: { live: { paper_enabled: true }, shadow_watch_only: { paper_enabled: true }, hard_shadow: { paper_enabled: false }, deprecated_shadow: { paper_enabled: false } },
  modes: { stage1: { tier: 'live', route: 'ATH', paper_enabled: true }, scout: { tier: 'shadow_watch_only' }, oldmode: { tier: 'deprecated_shadow' } },
  virtual_modes: { vmode: { tier: 'hard_shadow' } },
  promotion_policy: { shadow_watch_to_isolated: { min_unique_tokens: 30 }, isolated_to_live: { median_pnl_pct_gte: -2 }, deprecation: { window_days: 14 } },
  decision_gates: { gate1: {} },
};
const reg = buildModeRegistry(registryJson, { entry_points: [{ id: 'stage1' }] });

// buildModeRegistry — contract presence flags
{
  assert.equal(reg.hasThresholdCatalog, true);    // numeric thresholds present
  assert.equal(reg.hasPromotionGuardrails, true); // promotion_policy present
  assert.equal(reg.hasReleaseSafety, true);       // deprecation + decision_gates
  assert.ok(reg.tiers.live && reg.modes.stage1);
}

// modeReadinessProjection — registered live mode
{
  const m = modeReadinessProjection({ entry_mode: 'stage1', shadow_only: false, readiness_ok: true, executable_quote_clean: true }, reg);
  assert.equal(m.entry_mode_registry_seen, true);
  assert.equal(m.mode_tier, 'live');
  assert.equal(m.mode_tier_paper_enabled, true);
  assert.equal(m.mode_live_allowed, true);
  assert.equal(m.mode_promotion_eligible, false);  // fail-closed always
  assert.equal(m.mode_readiness_confidence, 'HIGH');
  assert.equal(m.metric_threshold_catalog_seen, true);
  assert.equal(m.promotion_guardrails_seen, true);
  assert.equal(m.holdout_negative_controls_seen, false);
  assert.ok(m.holdout_negative_controls_missing_reason.includes('holdout'));
}

// modeReadinessProjection — UNREGISTERED runtime mode (the real-window case)
{
  const m = modeReadinessProjection({ entry_mode: 'a_grade_resonance_fastlane', shadow_only: false, readiness_ok: false, readiness_blocker: 'route_or_quote_not_executable' }, reg);
  assert.equal(m.entry_mode_registry_seen, false);
  assert.equal(m.mode_tier, 'unregistered_unknown');
  assert.equal(m.entry_mode_registry_missing_reason, 'entry_mode_not_in_registry:a_grade_resonance_fastlane');
  assert.equal(m.mode_promotion_status, 'unregistered_no_promotion_path');
  assert.equal(m.mode_promotion_block_reason, 'mode_not_in_registry');
  assert.equal(m.mode_readiness_confidence, 'LOW');
}

// modeReadinessProjection — deprecated tier + no-decision
{
  assert.equal(modeReadinessProjection({ entry_mode: 'oldmode' }, reg).mode_deprecated_shadow, true);
  assert.equal(modeReadinessProjection({ entry_mode: 'oldmode' }, reg).mode_demotion_status, 'deprecated_shadow');
  const nd = modeReadinessProjection({ entry_mode: null, has_decision: false }, reg);
  assert.equal(nd.mode_tier, 'no_decision_no_mode');
  assert.equal(nd.mode_promotion_status, 'no_entry_mode');
}

// modeReadinessRepairDetail — only for the mode-readiness owner; readiness_blocker precedence
{
  assert.equal(modeReadinessRepairDetail({ final_repair_owner_v2: 'ENTRY_BRIDGE_GAP' }, {}), null);
  assert.equal(modeReadinessRepairDetail({ final_repair_owner_v2: MODE_OWNER, readiness_blocker: 'route_or_quote_not_executable' }, {}), 'readiness_route_or_quote_not_executable');
  assert.equal(modeReadinessRepairDetail({ final_repair_owner_v2: MODE_OWNER, readiness_blocker: null, entry_mode: 'x' }, { entry_mode_registry_seen: false, entry_mode_seen: true }), 'mode_unregistered:x');
}

// augmentModeRow — additive + preserves prior fields + deterministic
{
  const row = { token_ca: 'T', signal_ts: 1, class: 'dog', entry_mode: 'stage1', shadow_only: false, readiness_ok: true, executable_quote_clean: true, kline_seen: true, final_repair_owner_v2: 'DIRECT_STRATEGY_DECISION_MODULE_GAP' };
  const a = augmentModeRow(row, reg);
  assert.equal(a.token_ca, 'T');        // prior fields intact
  assert.equal(a.kline_seen, true);     // Goal-2 field intact
  assert.equal(a.mode_tier, 'live');
  assert.deepEqual(augmentModeRow(row, reg), a); // deterministic
}

// layerModeClosure — proof-gated reclassification on top of the marketdata closure
{
  const baseClosure = {
    modules: [
      { module_group: 'mode_readiness', bucket: 'F', coverage_status: 'blocked' },
      { module_group: 'metric_threshold_catalog', bucket: 'F', coverage_status: 'blocked' },
      { module_group: 'promotion_guardrails', bucket: 'F', coverage_status: 'blocked' },
      { module_group: 'release_safety', bucket: 'F', coverage_status: 'blocked' },
      { module_group: 'holdout_negative_controls', bucket: 'F', coverage_status: 'blocked' },
      { module_group: 'kline_cache', bucket: 'C', coverage_status: 'covered' },
      { module_group: 'ui_rendering', bucket: 'G', coverage_status: 'intentionally_excluded' },
    ],
  };
  const rows = [{ mode_tier: 'unregistered_unknown' }];
  const out = layerModeClosure(baseClosure, rows, reg);
  const reclassed = out.reclassified_from_blocked_this_layer.map((x) => x.module_group).sort();
  assert.deepEqual(reclassed, ['metric_threshold_catalog', 'mode_readiness', 'promotion_guardrails', 'release_safety']);
  assert.equal(out.coverage_status_counts.covered, 5);   // 1 base + 4 reclassified
  assert.equal(out.coverage_status_counts.blocked, 1);   // holdout stays blocked
  assert.equal(out.coverage_status_counts.intentionally_excluded, 1);
  assert.equal(out.every_module_mapped_to_A_G, true);
  assert.equal(out.bucket_G_all_intentionally_excluded, true);
  // holdout still blocked with a precise reason
  const holdout = out.modules.find((m) => m.module_group === 'holdout_negative_controls');
  assert.equal(holdout.coverage_status_final || holdout.coverage_status, 'blocked');
  assert.ok(String(holdout.reason_for_exclusion).includes('holdout'));
}

console.log('build-live-fullnet-row-v2-mode-readiness tests passed');
