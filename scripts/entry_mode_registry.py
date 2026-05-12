#!/usr/bin/env python3
"""Shared entry-mode governance registry loader."""

import json
import os
from pathlib import Path


CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
ENTRY_MODE_REGISTRY_JSON = os.environ.get(
    "ENTRY_MODE_REGISTRY_JSON",
    str(CONFIG_DIR / "entry-mode-registry.json"),
)

FALLBACK_SHADOW_ONLY_MODES = {
    "ath_flat_structure_tiny_scout",
    "ath_high_mc_tiny_probe",
    "ath_matrix_dissonance_tiny_probe",
    "ath_micro_reclaim_tiny_probe",
    "ath_no_kline_tiny_probe",
    "ath_reclaim_after_failure_tiny_probe",
    "ath_soft_reclaim_tiny_scout",
    "ath_uncertainty_tiny_scout",
    "gmgn_concentration_tiny_scout",
    "gmgn_low_kline_tiny_scout",
    "gmgn_midcap_near_miss_scout",
    "gmgn_reclaim_tiny_scout",
    "gmgn_unknown_data_tiny_scout",
    "lotto_high_risk_discovery_probe",
    "lotto_low_liquidity_reclaim_tiny_probe",
    "lotto_micro_reclaim_tiny_probe",
    "lotto_not_ath_reclaim_tiny_probe",
    "lotto_upstream_miss_tiny_scout",
    "lotto_upstream_realtime_tiny_scout",
    "matrix_micro_momentum_tiny_probe",
    "matrix_reclaim_tiny_probe",
    "momentum_direct_entry",
    "newborn_momentum_tiny_scout",
    "pullback_tiny_scout",
    "smart_entry_reclaim_tiny_scout",
    "unknown_data_activity_tiny_scout",
}


def load_entry_mode_registry(path=None):
    registry_path = Path(path or ENTRY_MODE_REGISTRY_JSON)
    try:
        with registry_path.open("r", encoding="utf-8") as fh:
            registry = json.load(fh)
    except Exception as exc:
        return {
            "version": 0,
            "load_error": str(exc),
            "modes": {
                mode: {
                    "tier": "hard_shadow",
                    "paper_enabled": False,
                    "reason": "fallback_shadow_only_mode",
                }
                for mode in sorted(FALLBACK_SHADOW_ONLY_MODES)
            },
            "virtual_modes": {},
        }
    return registry if isinstance(registry, dict) else {"version": 0, "modes": {}, "virtual_modes": {}}


ENTRY_MODE_REGISTRY = load_entry_mode_registry()


def entry_mode_registry_entry(entry_mode, registry=None):
    mode = str(entry_mode or "").strip()
    if not mode:
        return None
    registry = registry or ENTRY_MODE_REGISTRY
    entry = (registry.get("modes") or {}).get(mode)
    if not isinstance(entry, dict):
        return None
    return {
        "entry_mode": mode,
        **entry,
    }


def entry_mode_registry_shadow_only_modes(registry=None):
    registry = registry or ENTRY_MODE_REGISTRY
    modes = registry.get("modes") or {}
    blocked = set()
    for mode, entry in modes.items():
        if not isinstance(entry, dict):
            continue
        if entry.get("paper_enabled") is False or entry.get("blocks_live") is True:
            blocked.add(str(mode))
    return blocked or set(FALLBACK_SHADOW_ONLY_MODES)


def entry_mode_registry_summary(registry=None):
    registry = registry or ENTRY_MODE_REGISTRY
    modes = registry.get("modes") or {}
    virtual_modes = registry.get("virtual_modes") or {}
    by_tier = {}
    paper_enabled = 0
    paper_blocked = 0
    for entry in modes.values():
        if not isinstance(entry, dict):
            continue
        tier = str(entry.get("tier") or "unknown")
        by_tier[tier] = by_tier.get(tier, 0) + 1
        if entry.get("paper_enabled") is True:
            paper_enabled += 1
        else:
            paper_blocked += 1
    return {
        "version": registry.get("version"),
        "updated_at": registry.get("updated_at"),
        "mode_count": len(modes),
        "virtual_mode_count": len(virtual_modes),
        "by_tier": by_tier,
        "paper_enabled_modes": paper_enabled,
        "paper_blocked_modes": paper_blocked,
        "shadow_only_modes": sorted(entry_mode_registry_shadow_only_modes(registry)),
    }
