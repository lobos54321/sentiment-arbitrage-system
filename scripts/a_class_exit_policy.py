#!/usr/bin/env python3
"""Deterministic A_CLASS tiny-probe exit policy.

This policy is intentionally small and hard-edged:
- it only applies to A_CLASS/fastlane tiny entries;
- it never weakens the global hard-loss cap;
- it protects early noisy entries with a short grace period, except for
  flash-crash/no-route/trapped conditions;
- it keeps convexity by partial-locking winners and leaving a moonbag.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class AClassExitConfig:
    grace_sec: float = 60.0
    flash_crash_loss_pct: float = -0.20
    fast_stop_loss_pct: float = -0.15
    no_positive_feedback_sec: float = 60.0
    breakeven_peak_pct: float = 0.20
    breakeven_floor_pct: float = 0.0
    partial_lock_peak_pct: float = 0.50
    partial_lock_sell_pct: float = 0.25
    principal_recovery_peak_pct: float = 1.00
    principal_recovery_sell_pct: float = 0.50
    moonbag_peak_pct: float = 3.00
    moonbag_max_sold_pct: float = 0.70


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "ok", "clean", "available"}
    return bool(value)


def _state(pos: Any) -> dict:
    state = getattr(pos, "monitor_state", None)
    return state if isinstance(state, dict) else {}


def is_a_class_position(pos: Any) -> bool:
    state = _state(pos)
    mode_text = " ".join(
        str(value or "")
        for value in (
            state.get("normalizedMode"),
            state.get("normalizedEntryMode"),
            state.get("normalized_mode"),
            state.get("entryMode"),
            state.get("entry_mode"),
            getattr(pos, "normalized_mode", ""),
            getattr(pos, "entry_mode", ""),
        )
    ).lower()
    return "a_class" in mode_text or "fastlane" in mode_text or "fast_lane" in mode_text


def _sold_pct(pos: Any) -> float:
    return max(0.0, min(1.0, _safe_float(_state(pos).get("soldPct"), 0.0) or 0.0))


def _already_locked(pos: Any, key: str) -> bool:
    return _truthy(_state(pos).get(key))


def evaluate_a_class_exit_policy(
    pos: Any,
    *,
    current_pnl: float | None,
    peak_pnl: float | None = None,
    held_sec: float | None = None,
    quote_available: bool = True,
    route_available: bool = True,
    no_route_flag: bool = False,
    trapped_flag: bool = False,
    liquidity_collapse: bool = False,
    spread_extreme: bool = False,
    config: AClassExitConfig | dict | None = None,
) -> dict:
    if isinstance(config, dict):
        config = AClassExitConfig(**{key: value for key, value in config.items() if key in AClassExitConfig.__dataclass_fields__})
    config = config or AClassExitConfig()
    if not is_a_class_position(pos):
        return {"action": "hold", "reason": "not_a_class_position", "applies": False}

    pnl = _safe_float(current_pnl, None)
    peak = max(_safe_float(peak_pnl, 0.0) or 0.0, _safe_float(getattr(pos, "peak_pnl", 0.0), 0.0) or 0.0)
    held = max(0.0, _safe_float(held_sec, 0.0) or 0.0)
    sold_pct = _sold_pct(pos)
    base = {
        "applies": True,
        "entry_mode": _state(pos).get("entryMode") or getattr(pos, "entry_mode", None),
        "normalized_mode": _state(pos).get("normalizedMode") or _state(pos).get("normalized_mode"),
        "current_pnl": pnl,
        "peak_pnl": peak,
        "held_sec": held,
        "sold_pct": sold_pct,
        "config": asdict(config),
    }
    if pnl is None:
        return {**base, "action": "hold", "reason": "missing_current_pnl"}
    if no_route_flag or trapped_flag or not quote_available or not route_available:
        return {
            **base,
            "action": "exit",
            "reason": "a_class_route_or_quote_failed",
            "terminal_risk": {
                "no_route_flag": bool(no_route_flag),
                "trapped_flag": bool(trapped_flag),
                "quote_available": bool(quote_available),
                "route_available": bool(route_available),
            },
        }
    if liquidity_collapse or spread_extreme:
        return {
            **base,
            "action": "exit",
            "reason": "a_class_execution_quality_collapsed",
            "terminal_risk": {
                "liquidity_collapse": bool(liquidity_collapse),
                "spread_extreme": bool(spread_extreme),
            },
        }
    if pnl <= config.flash_crash_loss_pct:
        return {**base, "action": "exit", "reason": "a_class_flash_crash_loss_cap"}
    if held < config.grace_sec and peak <= 0 and pnl > config.flash_crash_loss_pct:
        return {**base, "action": "hold", "reason": "a_class_grace_period_noise"}
    if pnl <= config.fast_stop_loss_pct:
        return {**base, "action": "exit", "reason": "a_class_fast_stop_loss"}
    if held >= config.no_positive_feedback_sec and peak <= 0 and pnl <= 0:
        return {**base, "action": "exit", "reason": "a_class_no_positive_feedback"}
    if peak >= config.moonbag_peak_pct and sold_pct < config.moonbag_max_sold_pct:
        return {
            **base,
            "action": "partial_sell",
            "reason": "a_class_moonbag_risk_reduction",
            "sell_pct": min(config.moonbag_max_sold_pct - sold_pct, 0.20),
            "partial_type": "A_CLASS_MOONBAG",
        }
    if peak >= config.principal_recovery_peak_pct and not _already_locked(pos, "aClassPrincipalRecovered"):
        return {
            **base,
            "action": "partial_sell",
            "reason": "a_class_principal_recovery",
            "sell_pct": min(config.principal_recovery_sell_pct, 1.0 - sold_pct),
            "partial_type": "A_CLASS_PRINCIPAL_RECOVERY",
        }
    if peak >= config.partial_lock_peak_pct and not _already_locked(pos, "aClassPartialLocked"):
        return {
            **base,
            "action": "partial_sell",
            "reason": "a_class_partial_lock",
            "sell_pct": min(config.partial_lock_sell_pct, 1.0 - sold_pct),
            "partial_type": "A_CLASS_PARTIAL_LOCK",
        }
    if peak >= config.breakeven_peak_pct and pnl <= config.breakeven_floor_pct:
        return {
            **base,
            "action": "exit",
            "reason": "a_class_breakeven_floor",
            "trail_floor": config.breakeven_floor_pct,
        }
    return {**base, "action": "hold", "reason": "a_class_hold"}
