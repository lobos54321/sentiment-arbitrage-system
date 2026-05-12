import fs from 'fs';
import { join } from 'path';

export const DEFAULT_ENTRY_MODE_REGISTRY_PATH = join(process.cwd(), 'config', 'entry-mode-registry.json');

export function loadEntryModeRegistry(filePath = DEFAULT_ENTRY_MODE_REGISTRY_PATH) {
  const raw = fs.readFileSync(filePath, 'utf8');
  const registry = JSON.parse(raw);
  if (!registry || typeof registry !== 'object') {
    throw new Error('entry mode registry is not an object');
  }
  return registry;
}

export function summarizeEntryModeRegistry(registry) {
  const modes = registry?.modes && typeof registry.modes === 'object' ? registry.modes : {};
  const virtualModes = registry?.virtual_modes && typeof registry.virtual_modes === 'object'
    ? registry.virtual_modes
    : {};
  const byTier = {};
  const paperEnabledModes = [];
  const paperBlockedModes = [];
  const unknownTierModes = [];

  for (const [mode, entry] of Object.entries(modes)) {
    const tier = String(entry?.tier || 'unknown');
    byTier[tier] = (byTier[tier] || 0) + 1;
    if (!registry?.tiers?.[tier]) {
      unknownTierModes.push(mode);
    }
    if (entry?.paper_enabled === true) {
      paperEnabledModes.push(mode);
    } else {
      paperBlockedModes.push(mode);
    }
  }

  return {
    version: registry?.version ?? null,
    updated_at: registry?.updated_at ?? null,
    mode_count: Object.keys(modes).length,
    virtual_mode_count: Object.keys(virtualModes).length,
    by_tier: byTier,
    paper_enabled_modes: paperEnabledModes.sort(),
    paper_blocked_modes: paperBlockedModes.sort(),
    unknown_tier_modes: unknownTierModes.sort(),
    has_unknown_tiers: unknownTierModes.length > 0,
  };
}

export function registryModesByTier(registry) {
  const modes = registry?.modes && typeof registry.modes === 'object' ? registry.modes : {};
  const grouped = {};
  for (const [mode, entry] of Object.entries(modes)) {
    const tier = String(entry?.tier || 'unknown');
    if (!grouped[tier]) grouped[tier] = [];
    grouped[tier].push({ mode, ...entry });
  }
  for (const list of Object.values(grouped)) {
    list.sort((a, b) => a.mode.localeCompare(b.mode));
  }
  return grouped;
}
