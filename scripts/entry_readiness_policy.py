#!/usr/bin/env python3
"""
Entry readiness policy.

This module keeps lifecycle/risk odds separate from the live timing engine.
Historical strength can arm a candidate, but only a current timing node should
turn it into an entry.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
import os


ENTRY_READINESS_MAX_WAIT_SEC = int(os.environ.get("ENTRY_READINESS_MAX_WAIT_SEC", "900"))
ENTRY_READINESS_POLL_SEC = float(os.environ.get("ENTRY_READINESS_POLL_SEC", "10"))
GMGN_TINY_SCOUT_MODES = (
    "gmgn_concentration_tiny_scout",
    "gmgn_low_kline_tiny_scout",
    "gmgn_midcap_near_miss_scout",
)


@dataclass(frozen=True)
class EntryReadinessPolicy:
    decision: str
    lifecycle_profile: str
    min_odds_r: float
    min_p_follow: float
    max_spread_pct: float
    expected_loss_pct: float
    expected_upside_pct: float
    allowed_entry_modes: tuple[str, ...]
    reason: str
    detail: dict

    def to_dict(self) -> dict:
        data = asdict(self)
        data["allowed_entry_modes"] = list(self.allowed_entry_modes)
        return data


def _f(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _profile_from_lifecycle(route=None, lifecycle=None, pending=None, now_ts=None):
    lifecycle = lifecycle or {}
    pending = pending or {}
    features = lifecycle.get("lifecycle_features") or {}
    route_name = str(route or pending.get("signal_route") or pending.get("signal_type") or features.get("route") or "").upper()
    state = str(lifecycle.get("lifecycle_state") or "UNKNOWN").upper()
    bias = str(lifecycle.get("entry_bias") or "").upper()
    is_lotto = route_name == "LOTTO" or bool(pending.get("is_lotto"))
    is_probe = (
        bool((pending.get("lotto_state") or {}).get("probe"))
        or pending.get("replay_source") == "live_monitor_lotto_probe"
        or pending.get("entry_mode") == "lotto_real_probe_reentry_arm"
    )
    liquidity_unknown = bool(features.get("liquidity_unknown"))
    dex_id = str(features.get("dex_id") or "").lower()
    signal_age_sec = _f(features.get("age_sec"), 0.0)
    if now_ts is not None and pending.get("signal_ts"):
        signal_ts = _f(pending.get("signal_ts"), 0.0)
        if signal_ts > 1_000_000_000_000:
            signal_ts = signal_ts / 1000.0
        if signal_ts > 0:
            signal_age_sec = max(signal_age_sec, _f(now_ts, signal_ts) - signal_ts)

    if is_lotto and is_probe:
        return "LOTTO_REAL_PROBE", route_name, state, bias, signal_age_sec
    if is_lotto and (state == "NEWBORN_LAUNCH" or liquidity_unknown or dex_id == "pumpfun"):
        return "LOTTO_NEWBORN_RISKY", route_name, state, bias, signal_age_sec
    if is_lotto:
        return "LOTTO_NORMAL", route_name, state, bias, signal_age_sec
    if route_name == "ATH" and signal_age_sec > 2 * 60 * 60:
        return "ATH_STALE", route_name, state, bias, signal_age_sec
    if route_name == "ATH" and state in {"ATH_DEEP_RESET", "DEAD_CAT_BOUNCE"}:
        return "ATH_DEEP_RECLAIM", route_name, state, bias, signal_age_sec
    if route_name == "ATH":
        return "ATH_CONTINUATION", route_name, state, bias, signal_age_sec
    return "MATRIX_NORMAL", route_name, state, bias, signal_age_sec


def evaluate_entry_readiness_policy(*, route=None, lifecycle=None, pending=None, token_risk=None, now_ts=None):
    lifecycle = lifecycle or {}
    pending = pending or {}
    token_risk = token_risk or {}
    features = lifecycle.get("lifecycle_features") or {}
    profile, route_name, state, bias, signal_age_sec = _profile_from_lifecycle(
        route=route,
        lifecycle=lifecycle,
        pending=pending,
        now_ts=now_ts,
    )

    bad_states = {"DISTRIBUTION", "DEAD"}
    allowed_modes = ("momentum_direct_entry", "smart_entry_pullback_bounce")
    requested_entry_mode = str(pending.get("entry_mode") or "")
    explosive_direct_scout = requested_entry_mode == "explosive_newborn_direct_scout"
    gmgn_tiny_scout = requested_entry_mode in GMGN_TINY_SCOUT_MODES
    min_odds_r = 2.0
    min_p_follow = 0.58
    max_spread_pct = 2.0
    expected_loss_pct = 10.0

    if profile == "LOTTO_NEWBORN_RISKY":
        min_odds_r = 3.0
        min_p_follow = 0.74 if explosive_direct_scout else (0.72 if gmgn_tiny_scout else 0.68)
        max_spread_pct = 1.0
        expected_loss_pct = 12.0
        allowed_modes = (
            ("explosive_newborn_direct_scout", "smart_entry_pullback_bounce")
            if explosive_direct_scout
            else (requested_entry_mode, "smart_entry_pullback_bounce")
            if gmgn_tiny_scout
            else ("smart_entry_pullback_bounce",)
        )
    elif profile == "LOTTO_REAL_PROBE":
        min_odds_r = 3.0
        min_p_follow = 0.70
        max_spread_pct = 1.0
        expected_loss_pct = 12.0
        allowed_modes = ("smart_entry_pullback_bounce",)
    elif profile == "LOTTO_NORMAL":
        min_odds_r = 2.5
        min_p_follow = 0.68 if gmgn_tiny_scout else 0.62
        max_spread_pct = 1.5
        expected_loss_pct = 10.0
        if gmgn_tiny_scout:
            allowed_modes = (requested_entry_mode, "smart_entry_pullback_bounce")
    elif profile == "ATH_CONTINUATION":
        min_odds_r = 1.8
        min_p_follow = 0.56
        max_spread_pct = 2.0
        expected_loss_pct = 9.0
    elif profile == "ATH_DEEP_RECLAIM":
        min_odds_r = 3.0
        min_p_follow = 0.68
        max_spread_pct = 1.5
        expected_loss_pct = 12.0
    elif profile == "ATH_STALE":
        min_odds_r = 3.0
        min_p_follow = 0.70
        max_spread_pct = 1.0
        expected_loss_pct = 12.0
    else:
        min_odds_r = 2.0
        min_p_follow = 0.58
        max_spread_pct = 2.0
        expected_loss_pct = 10.0

    risk_profile = token_risk.get("risk_profile")
    if risk_profile in {"waterfall_memory", "waterfall_failure", "no_follow_failure", "doa_failure"}:
        min_odds_r += 0.5
        min_p_follow += 0.04
        max_spread_pct = max(0.5, max_spread_pct - 0.5)

    expected_upside_pct = expected_loss_pct * min_odds_r
    decision = "ARM"
    reason = "entry_readiness_arm_wait_for_timing_node"

    if state in bad_states:
        decision = "EXPIRE"
        reason = "entry_readiness_bad_lifecycle"
    elif profile == "ATH_STALE" and not (
        bool(pending.get("is_sustained_ath"))
        or _f(features.get("ath_distance_pct"), -1.0) >= -0.03
    ):
        decision = "WAIT"
        reason = "entry_readiness_stale_ath_requires_fresh_high"
    elif profile == "ATH_DEEP_RECLAIM" and bias == "REJECT":
        decision = "WAIT"
        reason = "entry_readiness_deep_reclaim_requires_current_node"

    return EntryReadinessPolicy(
        decision=decision,
        lifecycle_profile=profile,
        min_odds_r=min_odds_r,
        min_p_follow=round(min_p_follow, 3),
        max_spread_pct=max_spread_pct,
        expected_loss_pct=expected_loss_pct,
        expected_upside_pct=expected_upside_pct,
        allowed_entry_modes=allowed_modes,
        reason=reason,
        detail={
            "route": route_name,
            "lifecycle_state": state,
            "entry_bias": bias,
            "signal_age_sec": signal_age_sec,
            "risk_profile": risk_profile,
            "price_change_m5": features.get("price_change_m5"),
            "buy_sell_ratio": features.get("buy_sell_ratio"),
            "relative_volume": features.get("relative_volume") or features.get("volume_accel"),
        },
    )


def entry_mode_allowed(entry_mode, policy):
    mode = str(entry_mode or "")
    allowed = set(policy.allowed_entry_modes if hasattr(policy, "allowed_entry_modes") else policy.get("allowed_entry_modes", []))
    return mode in allowed
