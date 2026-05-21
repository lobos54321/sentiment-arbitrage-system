#!/usr/bin/env python3
"""
Entry readiness policy.

This module keeps lifecycle/risk odds separate from the live timing engine.
Historical strength can arm a candidate, but only a current timing node should
turn it into an entry.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
import datetime as dt
import os
import time


ENTRY_READINESS_MAX_WAIT_SEC = int(os.environ.get("ENTRY_READINESS_MAX_WAIT_SEC", "900"))
ENTRY_READINESS_POLL_SEC = float(os.environ.get("ENTRY_READINESS_POLL_SEC", "10"))
ENTRY_EXECUTION_MAX_SIGNAL_AGE_SEC = float(os.environ.get("ENTRY_EXECUTION_MAX_SIGNAL_AGE_SEC", "300"))
ENTRY_EXECUTION_MIN_LIQUIDITY_USD = float(os.environ.get("ENTRY_EXECUTION_MIN_LIQUIDITY_USD", "5000"))
ENTRY_EXECUTION_REQUIRE_QUOTE_CLEAN = os.environ.get("ENTRY_EXECUTION_REQUIRE_QUOTE_CLEAN", "true").lower() != "false"
ENTRY_EXECUTION_REQUIRE_TIMING = os.environ.get("ENTRY_EXECUTION_REQUIRE_TIMING", "true").lower() != "false"
CLEAN_DOG_RECLAIM_MIN_LIQUIDITY_USD = float(os.environ.get("CLEAN_DOG_RECLAIM_MIN_LIQUIDITY_USD", "0"))
CLEAN_DOG_RECLAIM_MIN_PEAK_PNL = float(os.environ.get("CLEAN_DOG_RECLAIM_MIN_PEAK_PNL", "0.25"))
GMGN_TINY_SCOUT_MODES = (
    "gmgn_concentration_tiny_scout",
    "gmgn_low_kline_tiny_scout",
    "gmgn_midcap_near_miss_scout",
    "gmgn_unknown_data_tiny_scout",
    "gmgn_reclaim_tiny_scout",
)
RECLAIM_TINY_SCOUT_MODES = (
    "smart_entry_reclaim_tiny_scout",
)
ATH_TINY_SCOUT_MODES = (
    "ath_flat_structure_tiny_scout",
    "ath_uncertainty_tiny_scout",
    "ath_no_kline_tiny_probe",
    "ath_high_mc_tiny_probe",
    "ath_reclaim_after_failure_tiny_probe",
    "ath_matrix_dissonance_tiny_probe",
    "ath_micro_reclaim_tiny_probe",
)
NEWBORN_TINY_SCOUT_MODES = (
    "newborn_momentum_tiny_scout",
)
LOTTO_UPSTREAM_TINY_SCOUT_MODES = (
    "lotto_upstream_miss_tiny_scout",
    "lotto_upstream_realtime_tiny_scout",
    "lotto_not_ath_reclaim_tiny_probe",
    "lotto_low_liquidity_reclaim_tiny_probe",
    "lotto_micro_reclaim_tiny_probe",
)
SOURCE_RESONANCE_TINY_PROBE_MODES = (
    "source_resonance_tiny_probe",
)
PULLBACK_TINY_SCOUT_MODES = (
    "pullback_tiny_scout",
)
DISCOVERY_TINY_SCOUT_MODES = (
    "ath_soft_reclaim_tiny_scout",
    "unknown_data_activity_tiny_scout",
    "matrix_reclaim_tiny_probe",
    "matrix_micro_momentum_tiny_probe",
    "lotto_high_risk_discovery_probe",
)
PAPER_TINY_SCOUT_MODES = (
    GMGN_TINY_SCOUT_MODES
    + RECLAIM_TINY_SCOUT_MODES
    + ATH_TINY_SCOUT_MODES
    + NEWBORN_TINY_SCOUT_MODES
    + LOTTO_UPSTREAM_TINY_SCOUT_MODES
    + SOURCE_RESONANCE_TINY_PROBE_MODES
    + PULLBACK_TINY_SCOUT_MODES
    + DISCOVERY_TINY_SCOUT_MODES
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


@dataclass(frozen=True)
class EntryExecutionEligibility:
    decision: str
    reason: str
    entry_mode: str
    route: str
    direct_entry_ok: bool
    freshness_ok: bool
    quote_clean_ok: bool
    quote_executable_ok: bool
    liquidity_ok: bool
    timing_ok: bool
    route_ev_ok: bool
    risk_ok: bool
    signal_age_sec: object
    liquidity_usd: object
    min_liquidity_usd: float
    checks: dict
    detail: dict

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class CleanDogReclaimEligibility:
    decision: str
    reason: str
    entry_branch: str
    route: str
    direct_reclaim_ok: bool
    clean_quote_ok: bool
    tradable_ok: bool
    would_stop_before_peak_ok: bool
    last_tradable_fresh_ok: bool
    reclaim_momentum_ok: bool
    liquidity_ok: bool
    toxic_ok: bool
    top_holder_ok: bool
    route_allowed: bool
    canary_budget_ok: bool
    last_tradable_age_sec: object
    max_tradable_age_sec: float
    liquidity_usd: object
    min_liquidity_usd: float
    checks: dict
    detail: dict

    def to_dict(self) -> dict:
        return asdict(self)


def _f(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _truthy(value):
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "pass", "clean", "ok"}
    return bool(value)


def _first_positive(*values):
    for value in values:
        try:
            if value is None:
                continue
            number = float(value)
            if number > 0:
                return number
        except (TypeError, ValueError):
            continue
    return None


def _ts_sec(value):
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    if ts > 1_000_000_000_000:
        return ts / 1000.0
    return ts


def _any_ts_sec(value):
    if value in (None, ""):
        return None
    ts = _ts_sec(value)
    if ts is not None:
        return ts
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            parsed = dt.datetime.strptime(text.replace("Z", "")[:26], fmt)
            return parsed.replace(tzinfo=dt.timezone.utc).timestamp()
        except ValueError:
            continue
    return None


def _first_ts(*values):
    for value in values:
        ts = _any_ts_sec(value)
        if ts is not None and ts > 0:
            return ts
    return None


def _signal_age_sec(signal_ts=None, now_ts=None, observed=None, pending=None):
    observed = observed or {}
    pending = pending or {}
    explicit_age = observed.get("signal_age_sec")
    if explicit_age is not None:
        try:
            return max(0.0, float(explicit_age))
        except (TypeError, ValueError):
            pass
    ts = _ts_sec(signal_ts or observed.get("signal_ts_sec") or pending.get("signal_ts"))
    if ts is None:
        return None
    now = _ts_sec(now_ts) or _ts_sec(observed.get("now_ts"))
    if now is None:
        return None
    return max(0.0, now - ts)


def build_clean_dog_reclaim_eligibility(
    payload=None,
    *,
    entry_branch="",
    route=None,
    now_ts=None,
    max_tradable_age_sec=300,
    min_liquidity_usd=None,
    route_allowed=True,
    canary_budget_ok=True,
):
    """Eligibility gate for missed-dog reclaim canaries.

    This is deliberately separate from generic entry execution eligibility:
    the original signal may be old, but a fresh clean quote/tradable reclaim
    can still be actionable. Freshness must therefore anchor to the latest
    tradable/clean quote attribution timestamp, not to the first signal time.
    """
    payload = payload or {}
    now = _any_ts_sec(now_ts) or time.time()
    min_liquidity_usd = (
        CLEAN_DOG_RECLAIM_MIN_LIQUIDITY_USD
        if min_liquidity_usd is None
        else float(min_liquidity_usd or 0.0)
    )
    max_tradable_age_sec = float(max_tradable_age_sec or 0.0)
    route_name = str(route or payload.get("route") or "").upper()
    branch = str(entry_branch or payload.get("entry_branch") or "")

    tradable_ok = _truthy(payload.get("tradable_missed"))
    clean_quote_ok = _truthy(
        payload.get("recovery_quote_clean")
        or payload.get("final_reclaim_quote_executable")
        or payload.get("quote_clean_seen")
        or payload.get("quote_clean")
        or tradable_ok
    )
    would_stop_before_peak_ok = not _truthy(payload.get("would_stop_before_peak"))

    fresh_ts = _first_ts(
        payload.get("last_tradable_ts"),
        payload.get("last_clean_quote_ts"),
        payload.get("missed_updated_at"),
        payload.get("updated_at"),
        payload.get("first_tradable_ts"),
    )
    last_tradable_age_sec = None if fresh_ts is None else max(0.0, now - fresh_ts)
    last_tradable_fresh_ok = (
        last_tradable_age_sec is not None
        and (max_tradable_age_sec <= 0 or last_tradable_age_sec <= max_tradable_age_sec)
    )

    peak_raw = (
        payload.get("executable_peak_pnl")
        if payload.get("executable_peak_pnl") is not None
        else payload.get("tradable_peak_pnl")
    )
    peak_present = peak_raw is not None
    peak_pnl = _f(peak_raw, 0.0)
    explicit_momentum = (
        payload.get("reclaim_momentum_ok")
        if payload.get("reclaim_momentum_ok") is not None
        else payload.get("momentum_reclaim")
        if payload.get("momentum_reclaim") is not None
        else payload.get("activity_reclaim")
        if payload.get("activity_reclaim") is not None
        else payload.get("activity_confirmed")
    )
    reclaim_momentum_ok = (
        _truthy(explicit_momentum)
        if explicit_momentum is not None
        else (tradable_ok and (not peak_present or peak_pnl >= CLEAN_DOG_RECLAIM_MIN_PEAK_PNL))
    )

    liquidity_usd = _first_positive(
        payload.get("liquidity_usd"),
        payload.get("gmgn_last_liquidity"),
        payload.get("last_liquidity"),
    )
    liquidity_ok = min_liquidity_usd <= 0 or (liquidity_usd is not None and liquidity_usd >= min_liquidity_usd)

    toxic_ok = not any(
        _truthy(payload.get(name))
        for name in ("toxic", "toxic_bundler", "bundler_toxic", "bundler_risk", "honeypot", "rug_pull")
    )
    top_holder_ok = not any(
        _truthy(payload.get(name))
        for name in ("top1_risk", "top10_reject", "top_holder_risk", "insider_concentration")
    )
    route_allowed = bool(route_allowed)
    canary_budget_ok = bool(canary_budget_ok)

    checks = {
        "clean_quote_ok": clean_quote_ok,
        "tradable_ok": tradable_ok,
        "would_stop_before_peak_ok": would_stop_before_peak_ok,
        "last_tradable_fresh_ok": last_tradable_fresh_ok,
        "reclaim_momentum_ok": reclaim_momentum_ok,
        "liquidity_ok": liquidity_ok,
        "toxic_ok": toxic_ok,
        "top_holder_ok": top_holder_ok,
        "route_allowed": route_allowed,
        "canary_budget_ok": canary_budget_ok,
    }
    reason = "clean_dog_reclaim_pass"
    if not canary_budget_ok:
        reason = "clean_dog_reclaim_canary_disabled"
    elif not route_allowed:
        reason = "clean_dog_reclaim_route_not_allowed"
    elif not clean_quote_ok or not tradable_ok:
        reason = "clean_dog_reclaim_recovery_quote_clean_missing"
    elif not would_stop_before_peak_ok:
        reason = "clean_dog_reclaim_stop_before_peak_watch_only"
    elif fresh_ts is None:
        reason = "clean_dog_reclaim_recovery_tradable_timestamp_missing"
    elif not last_tradable_fresh_ok:
        reason = "clean_dog_reclaim_recovery_tradable_signal_stale_watch_only"
    elif not reclaim_momentum_ok:
        reason = "clean_dog_reclaim_reclaim_momentum_missing"
    elif not liquidity_ok:
        reason = "clean_dog_reclaim_liquidity_required"
    elif not toxic_ok:
        reason = "clean_dog_reclaim_toxic_risk_block"
    elif not top_holder_ok:
        reason = "clean_dog_reclaim_top_holder_risk_block"

    direct_reclaim_ok = all(checks.values())
    return CleanDogReclaimEligibility(
        decision="pass" if direct_reclaim_ok else "watch_only",
        reason=reason,
        entry_branch=branch,
        route=route_name,
        direct_reclaim_ok=direct_reclaim_ok,
        clean_quote_ok=clean_quote_ok,
        tradable_ok=tradable_ok,
        would_stop_before_peak_ok=would_stop_before_peak_ok,
        last_tradable_fresh_ok=last_tradable_fresh_ok,
        reclaim_momentum_ok=reclaim_momentum_ok,
        liquidity_ok=liquidity_ok,
        toxic_ok=toxic_ok,
        top_holder_ok=top_holder_ok,
        route_allowed=route_allowed,
        canary_budget_ok=canary_budget_ok,
        last_tradable_age_sec=last_tradable_age_sec,
        max_tradable_age_sec=max_tradable_age_sec,
        liquidity_usd=liquidity_usd,
        min_liquidity_usd=min_liquidity_usd,
        checks=checks,
        detail={
            "freshness_anchor_ts": fresh_ts,
            "freshness_anchor_keys": [
                "last_tradable_ts",
                "last_clean_quote_ts",
                "missed_updated_at",
                "updated_at",
                "first_tradable_ts",
            ],
            "executable_peak_pnl": peak_pnl,
        },
    )


def build_entry_execution_eligibility(
    *,
    entry_mode="",
    route=None,
    signal_ts=None,
    now_ts=None,
    pending=None,
    observed=None,
    external_alpha=None,
    dex_snapshot=None,
    min_liquidity_usd=None,
    max_signal_age_sec=None,
    require_quote_clean=None,
    require_timing=None,
    timing_confirmed=None,
    quote_clean_seen=None,
    quote_executable=None,
    route_ev_detail=None,
    risk_ok=True,
):
    """Canonical final-entry eligibility gate.

    Lifecycle strength may arm a candidate, but executable entry still needs
    fresh signal state, quote evidence, liquidity, timing, and route health.
    """
    pending = pending or {}
    observed = observed or {}
    external_alpha = external_alpha or {}
    dex_snapshot = dex_snapshot or {}
    min_liquidity_usd = (
        ENTRY_EXECUTION_MIN_LIQUIDITY_USD
        if min_liquidity_usd is None
        else float(min_liquidity_usd or 0.0)
    )
    max_signal_age_sec = (
        ENTRY_EXECUTION_MAX_SIGNAL_AGE_SEC
        if max_signal_age_sec is None
        else float(max_signal_age_sec or 0.0)
    )
    require_quote_clean = ENTRY_EXECUTION_REQUIRE_QUOTE_CLEAN if require_quote_clean is None else bool(require_quote_clean)
    require_timing = ENTRY_EXECUTION_REQUIRE_TIMING if require_timing is None else bool(require_timing)
    signal_age_sec = _signal_age_sec(signal_ts=signal_ts, now_ts=now_ts, observed=observed, pending=pending)

    quote_clean = _truthy(
        quote_clean_seen
        if quote_clean_seen is not None
        else (
            observed.get("quote_clean_seen")
            or observed.get("quote_clean")
            or external_alpha.get("quote_clean_seen")
            or external_alpha.get("quote_clean")
            or external_alpha.get("quote_executable")
            or pending.get("quote_clean_seen")
            or pending.get("source_quote_clean_seen")
        )
    )
    quote_ok = _truthy(
        quote_executable
        if quote_executable is not None
        else (
            observed.get("quote_executable")
            or observed.get("quote_route_available")
            or external_alpha.get("quote_executable")
            or external_alpha.get("entry_quote_success_seen")
            or pending.get("quote_anchored_entry")
            or pending.get("final_reclaim_quote_executable")
        )
    )
    if quote_clean:
        quote_ok = True

    liquidity_usd = _first_positive(
        observed.get("liquidity_usd"),
        observed.get("gmgn_last_liquidity"),
        dex_snapshot.get("liquidity_usd"),
        dex_snapshot.get("liquidity"),
        external_alpha.get("last_liquidity"),
        external_alpha.get("gmgn_last_liquidity"),
        pending.get("liquidity_usd"),
    )
    freshness_ok = signal_age_sec is None or max_signal_age_sec <= 0 or signal_age_sec <= max_signal_age_sec
    quote_clean_ok = (not require_quote_clean) or quote_clean
    quote_executable_ok = quote_ok
    liquidity_ok = min_liquidity_usd <= 0 or (liquidity_usd is not None and liquidity_usd >= min_liquidity_usd)
    if timing_confirmed is None:
        timing_confirmed = (
            observed.get("timing_confirmed")
            or observed.get("timing_passed")
            or pending.get("timing_passed")
        )
    timing_ok = (not require_timing) or _truthy(timing_confirmed)

    route_ev_ok = True
    if isinstance(route_ev_detail, dict):
        route_ev_ok = route_ev_detail.get("pass") is not False
    elif route_ev_detail is not None:
        route_ev_ok = bool(route_ev_detail)
    risk_ok = bool(risk_ok)

    checks = {
        "freshness_ok": freshness_ok,
        "quote_clean_ok": quote_clean_ok,
        "quote_executable_ok": quote_executable_ok,
        "liquidity_ok": liquidity_ok,
        "timing_ok": timing_ok,
        "route_ev_ok": route_ev_ok,
        "risk_ok": risk_ok,
    }
    reason = "entry_execution_pass"
    if not freshness_ok:
        reason = "entry_execution_signal_stale"
    elif not quote_clean_ok:
        reason = "entry_execution_quote_clean_required"
    elif not quote_executable_ok:
        reason = "entry_execution_quote_executable_required"
    elif not liquidity_ok:
        reason = "entry_execution_liquidity_required"
    elif not timing_ok:
        reason = "entry_execution_timing_required"
    elif not route_ev_ok:
        reason = "entry_execution_route_ev_block"
    elif not risk_ok:
        reason = "entry_execution_risk_block"

    direct_entry_ok = all(checks.values())
    return EntryExecutionEligibility(
        decision="pass" if direct_entry_ok else "watch_only",
        reason=reason,
        entry_mode=str(entry_mode or ""),
        route=str(route or pending.get("signal_route") or pending.get("signal_type") or observed.get("route") or "").upper(),
        direct_entry_ok=direct_entry_ok,
        freshness_ok=freshness_ok,
        quote_clean_ok=quote_clean_ok,
        quote_executable_ok=quote_executable_ok,
        liquidity_ok=liquidity_ok,
        timing_ok=timing_ok,
        route_ev_ok=route_ev_ok,
        risk_ok=risk_ok,
        signal_age_sec=signal_age_sec,
        liquidity_usd=liquidity_usd,
        min_liquidity_usd=min_liquidity_usd,
        checks=checks,
        detail={
            "max_signal_age_sec": max_signal_age_sec,
            "require_quote_clean": require_quote_clean,
            "require_timing": require_timing,
            "quote_clean_seen": quote_clean,
            "quote_executable": quote_ok,
            "route_ev_detail": route_ev_detail if isinstance(route_ev_detail, dict) else None,
        },
    )


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
        or pending.get("replay_source") == "live_monitor_lotto_upstream_probe"
        or pending.get("replay_source") == "live_monitor_lotto_upstream_realtime"
        or pending.get("entry_mode") == "lotto_real_probe_reentry_arm"
        or pending.get("entry_mode") in LOTTO_UPSTREAM_TINY_SCOUT_MODES
        or pending.get("entry_mode") in SOURCE_RESONANCE_TINY_PROBE_MODES
        or pending.get("entry_mode") in DISCOVERY_TINY_SCOUT_MODES
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
    paper_tiny_scout = requested_entry_mode in PAPER_TINY_SCOUT_MODES
    min_odds_r = 2.0
    min_p_follow = 0.58
    max_spread_pct = 2.0
    expected_loss_pct = 10.0

    if profile == "LOTTO_NEWBORN_RISKY":
        min_odds_r = 3.0
        min_p_follow = 0.74 if explosive_direct_scout else (0.72 if paper_tiny_scout else 0.68)
        max_spread_pct = 1.0
        expected_loss_pct = 12.0
        allowed_modes = (
            ("explosive_newborn_direct_scout", "smart_entry_pullback_bounce")
            if explosive_direct_scout
            else (requested_entry_mode, "smart_entry_pullback_bounce")
            if paper_tiny_scout
            else ("smart_entry_pullback_bounce",)
        )
    elif profile == "LOTTO_REAL_PROBE":
        min_odds_r = 3.0
        min_p_follow = 0.70
        max_spread_pct = 2.0 if paper_tiny_scout else 1.0
        expected_loss_pct = 12.0
        allowed_modes = ("smart_entry_pullback_bounce",)
    elif profile == "LOTTO_NORMAL":
        min_odds_r = 2.5
        min_p_follow = 0.68 if paper_tiny_scout else 0.62
        max_spread_pct = 1.5
        expected_loss_pct = 10.0
        if paper_tiny_scout:
            allowed_modes = (requested_entry_mode, "smart_entry_pullback_bounce")
    elif profile == "ATH_CONTINUATION":
        min_odds_r = 1.8
        min_p_follow = 0.62 if paper_tiny_scout else 0.56
        max_spread_pct = (
            3.0
            if paper_tiny_scout and requested_entry_mode != "ath_flat_structure_tiny_scout"
            else 2.0
        )  # V9: 2.0→3.0 for scouts; flat structure keeps legacy 2.0.
        expected_loss_pct = 9.0
        if paper_tiny_scout:
            allowed_modes = (requested_entry_mode, "smart_entry_pullback_bounce")
    elif profile == "ATH_DEEP_RECLAIM":
        min_odds_r = 3.0
        min_p_follow = 0.70 if paper_tiny_scout else 0.68
        max_spread_pct = 1.5
        expected_loss_pct = 12.0
        if paper_tiny_scout:
            allowed_modes = (requested_entry_mode, "smart_entry_pullback_bounce")
    elif profile == "ATH_STALE":
        min_odds_r = 3.0
        min_p_follow = 0.72 if paper_tiny_scout else 0.70
        max_spread_pct = 1.0
        expected_loss_pct = 12.0
        if paper_tiny_scout:
            allowed_modes = (requested_entry_mode, "smart_entry_pullback_bounce")
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
