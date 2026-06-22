import assert from 'node:assert/strict';

import {
  REQUIREMENTS,
  finalBlockerProjection,
  augmentFinalRow,
  layerFinalClosure,
  runtimeReadmodelRequirements,
  zeaburExportRequirements,
} from '../scripts/build-live-fullnet-row-v2-final-blockers.js';

// REQUIREMENTS — all 17 modules, valid categories, exact (non-empty) export strings
{
  const groups = Object.keys(REQUIREMENTS);
  assert.equal(groups.length, 17);
  for (const g of groups) {
    assert.ok(['covered_now', 'runtime_readmodel_required', 'zeabur_export_required'].includes(REQUIREMENTS[g].category), `category for ${g}`);
    assert.ok(REQUIREMENTS[g].exact && REQUIREMENTS[g].exact.length > 10, `exact export for ${g}`);
  }
  assert.equal(REQUIREMENTS.smart_money.category, 'zeabur_export_required');
  assert.equal(REQUIREMENTS.curve_pumpfun.category, 'zeabur_export_required');
  assert.equal(REQUIREMENTS.paper_path_samples.category, 'covered_now');
  assert.equal(REQUIREMENTS.source_channel_dbs.category, 'covered_now');
}

// finalBlockerProjection — covered-now (paper path + channel), blocked with exact reasons, no Goal-3 collision
{
  const row = { token_ca: 'T', signal_ts: 1, entered: false, ledger_source: null, entry_mode: 'a_grade_resonance_fastlane' };
  const proj = finalBlockerProjection(row, { signal_source: 'premium_channel_ath', source_family: 'third_party_kline' });
  // paper path samples: definitive 0 (no entries)
  assert.equal(proj.paper_path_samples_seen, false);
  assert.equal(proj.paper_path_samples_path_n, 0);
  assert.equal(proj.paper_path_samples_missing_reason, 'no_paper_trades_in_window_paper_only_shadow');
  // channel identity covered
  assert.equal(proj.source_channel_registry_seen, true);
  assert.equal(proj.source_channel, 'premium_channel_ath');
  assert.equal(proj.source_channel_family, 'third_party_kline');
  // blocked modules carry exact required export
  assert.equal(proj.gmgn_policy_seen, false);
  assert.ok(proj.gmgn_policy_missing_reason.includes('export gmgn_policy'));
  assert.equal(proj.smart_money_seen, false);
  // resonance/scout appear in mode label (transparency flags)
  assert.equal(proj.source_resonance_in_mode_label, true);
  assert.equal(proj.scout_quality_in_mode_label, false);
  // aggregate requirement lists
  assert.ok(proj.runtime_readmodel_required.includes('gmgn_policy'));
  assert.deepEqual(proj.zeabur_export_required.sort(), ['curve_pumpfun', 'smart_money']);
  assert.deepEqual(proj.token_memory_required, ['token_memory']);
  assert.equal(proj.final_blocker_confidence, 'LOW');
  // MUST NOT re-emit Goal-3 per-row fields (no collision)
  assert.ok(!('holdout_negative_controls_missing_reason' in proj));
  assert.ok(!('evidence_conflict_aging_missing_reason' in proj));
  assert.ok(!('assumptions_false_negative_budget_missing_reason' in proj));
}

// augmentFinalRow — additive + preserves prior fields + deterministic
{
  const row = { token_ca: 'T', signal_ts: 1, class: 'dog', entered: false, kline_seen: true, mode_tier: 'unregistered_unknown', ex_ante_feasible: true, holdout_negative_controls_missing_reason: 'GOAL3_VALUE' };
  const a = augmentFinalRow(row, { signal_source: 'ch1' });
  assert.equal(a.kline_seen, true);                 // Goal 2
  assert.equal(a.mode_tier, 'unregistered_unknown'); // Goal 3
  assert.equal(a.ex_ante_feasible, true);            // Goal 4
  assert.equal(a.holdout_negative_controls_missing_reason, 'GOAL3_VALUE'); // Goal-3 field NOT overwritten
  assert.equal(a.source_channel, 'ch1');
  assert.deepEqual(augmentFinalRow(row, { signal_source: 'ch1' }), a); // deterministic
}

// layerFinalClosure — cover 2 same-window modules; refine the rest with exact reasons; no generic remains
{
  const baseClosure = {
    modules: [
      { module_group: 'paper_path_samples', bucket: 'C', coverage_status: 'blocked' },
      { module_group: 'source_channel_dbs', bucket: 'C', coverage_status: 'blocked' },
      { module_group: 'gmgn_policy', bucket: 'B', coverage_status: 'blocked' },
      { module_group: 'smart_money', bucket: 'B', coverage_status: 'blocked' },
      { module_group: 'holdout_negative_controls', bucket: 'F', coverage_status: 'blocked' },
      { module_group: 'kline_cache', bucket: 'C', coverage_status: 'covered' },
      { module_group: 'ui_rendering', bucket: 'G', coverage_status: 'intentionally_excluded' },
    ],
  };
  const rows = [{ source_channel_registry_seen: true, paper_path_samples_seen: false }];
  const out = layerFinalClosure(baseClosure, rows);
  const reclassed = out.reclassified_from_blocked_this_layer.map((x) => x.module_group).sort();
  assert.deepEqual(reclassed, ['paper_path_samples', 'source_channel_dbs']);
  assert.equal(out.coverage_status_counts.covered, 3);  // kline_cache + 2 reclassified
  assert.equal(out.coverage_status_counts.blocked, 3);  // gmgn_policy, smart_money, holdout
  assert.equal(out.coverage_status_counts.intentionally_excluded, 1);
  assert.equal(out.no_generic_blocked_reason_remains, true);
  const gmgn = out.modules.find((m) => m.module_group === 'gmgn_policy');
  assert.ok(gmgn.reason_for_exclusion.startsWith('runtime_readmodel_required::'));
  const sm = out.modules.find((m) => m.module_group === 'smart_money');
  assert.ok(sm.reason_for_exclusion.startsWith('zeabur_export_required::'));
}

// requirement reports — 13 runtime + 2 zeabur
{
  assert.equal(runtimeReadmodelRequirements().count, 13);
  assert.equal(zeaburExportRequirements().count, 2);
}

console.log('build-live-fullnet-row-v2-final-blockers tests passed');
