import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  loadEntryModeRegistry,
  registryModesByTier,
  summarizeEntryModeRegistry,
} from '../src/web/mode-registry-utils.js';

test('entry mode registry exposes governance tiers and blocked modes', () => {
  const registry = loadEntryModeRegistry();
  const summary = summarizeEntryModeRegistry(registry);

  assert.equal(summary.has_unknown_tiers, false);
  assert.equal(summary.by_tier.live, 9);
  assert.ok(summary.by_tier.hard_shadow >= 4);
  assert.ok(summary.by_tier.isolated_paper_capped >= 1);
  assert.ok(summary.by_tier.shadow_watch_only >= 1);
  assert.ok(summary.paper_blocked_modes.includes('lotto_low_liquidity_reclaim_tiny_probe'));
  assert.ok(!summary.paper_blocked_modes.includes('lotto_fast_lane'));
  assert.ok(!summary.paper_blocked_modes.includes('source_resonance_tiny_probe'));
  assert.ok(!summary.paper_blocked_modes.includes('hard_gate_pass_tiny_probe'));
  assert.ok(!summary.paper_blocked_modes.includes('pre_pass_resonance_tiny_probe'));
});

test('registry groups virtual not-ath watch separately from entry modes', () => {
  const registry = loadEntryModeRegistry();
  const grouped = registryModesByTier(registry);

  assert.equal(registry.virtual_modes.lotto_not_ath_watch_shadow.tier, 'shadow_watch_only');
  assert.ok(grouped.live.some((entry) => entry.mode === 'explosive_newborn_direct_scout'));
  assert.ok(grouped.live.some((entry) => entry.mode === 'source_resonance_tiny_probe'));
  assert.ok(grouped.live.some((entry) => entry.mode === 'hard_gate_pass_tiny_probe'));
  assert.ok(grouped.live.some((entry) => entry.mode === 'pre_pass_resonance_tiny_probe'));
  assert.ok(grouped.isolated_paper_capped.some((entry) => entry.mode === 'ath_flat_structure_tiny_scout'));
});
