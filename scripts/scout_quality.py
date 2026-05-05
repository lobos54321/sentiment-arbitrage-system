#!/usr/bin/env python3
"""Shared quality gate for paper tiny-scout entries."""

from __future__ import annotations

import os


SCOUT_QUALITY_SIZE_CAP_SOL = float(os.environ.get("SCOUT_QUALITY_SIZE_CAP_SOL", "0.003"))
SCOUT_QUALITY_MIN_LIQUIDITY_USD = float(os.environ.get("SCOUT_QUALITY_MIN_LIQUIDITY_USD", "5000"))
SCOUT_QUALITY_MIN_BS_RATIO = float(os.environ.get("SCOUT_QUALITY_MIN_BS_RATIO", "1.20"))
SCOUT_QUALITY_MIN_VOL_M5 = float(os.environ.get("SCOUT_QUALITY_MIN_VOL_M5", "8000"))
SCOUT_QUALITY_MIN_TX_M5 = int(os.environ.get("SCOUT_QUALITY_MIN_TX_M5", "80"))
SCOUT_QUALITY_MAX_NEG_M5 = float(os.environ.get("SCOUT_QUALITY_MAX_NEG_M5", "-15"))
SCOUT_QUALITY_MAX_TOP1_PCT = float(os.environ.get("SCOUT_QUALITY_MAX_TOP1_PCT", "50"))
SCOUT_QUALITY_MAX_TOP10_PCT = float(os.environ.get("SCOUT_QUALITY_MAX_TOP10_PCT", "70"))

SCOUT_QUALITY_GMGN_MIDCAP_MIN_VOL_M5 = float(
    os.environ.get("SCOUT_QUALITY_GMGN_MIDCAP_MIN_VOL_M5", "15000")
)
SCOUT_QUALITY_GMGN_MIDCAP_MIN_TX_M5 = int(
    os.environ.get("SCOUT_QUALITY_GMGN_MIDCAP_MIN_TX_M5", "200")
)
SCOUT_QUALITY_UNKNOWN_MIN_VOL_M5 = float(
    os.environ.get("SCOUT_QUALITY_UNKNOWN_MIN_VOL_M5", "20000")
)
SCOUT_QUALITY_UNKNOWN_MIN_TX_M5 = int(
    os.environ.get("SCOUT_QUALITY_UNKNOWN_MIN_TX_M5", "250")
)
SCOUT_QUALITY_RECLAIM_MIN_VOL_M5 = float(
    os.environ.get("SCOUT_QUALITY_RECLAIM_MIN_VOL_M5", "12000")
)
SCOUT_QUALITY_RECLAIM_MIN_TX_M5 = int(
    os.environ.get("SCOUT_QUALITY_RECLAIM_MIN_TX_M5", "120")
)


_BASE_PROFILE = {
    "min_liquidity_usd": SCOUT_QUALITY_MIN_LIQUIDITY_USD,
    "allow_unknown_liquidity": False,
    "min_buy_sell_ratio": SCOUT_QUALITY_MIN_BS_RATIO,
    "min_vol_m5": SCOUT_QUALITY_MIN_VOL_M5,
    "min_tx_m5": SCOUT_QUALITY_MIN_TX_M5,
    "max_negative_m5": SCOUT_QUALITY_MAX_NEG_M5,
    "max_top1_pct": SCOUT_QUALITY_MAX_TOP1_PCT,
    "max_top10_pct": SCOUT_QUALITY_MAX_TOP10_PCT,
    "max_size_sol": SCOUT_QUALITY_SIZE_CAP_SOL,
    "block_gmgn_reject": True,
    "block_recent_failures": False,
}


_PROFILE_OVERRIDES = {
    "ath_uncertainty_tiny_scout": {
        "max_top10_pct": 45.0,
        "block_recent_failures": True,
    },
    "gmgn_midcap_near_miss_scout": {
        "min_vol_m5": SCOUT_QUALITY_GMGN_MIDCAP_MIN_VOL_M5,
        "min_tx_m5": SCOUT_QUALITY_GMGN_MIDCAP_MIN_TX_M5,
        "max_negative_m5": -15.0,
    },
    "gmgn_unknown_data_tiny_scout": {
        "min_liquidity_usd": 0.0,
        "allow_unknown_liquidity": True,
        "min_vol_m5": SCOUT_QUALITY_UNKNOWN_MIN_VOL_M5,
        "min_tx_m5": SCOUT_QUALITY_UNKNOWN_MIN_TX_M5,
        "max_negative_m5": -15.0,
    },
    "gmgn_reclaim_tiny_scout": {
        "min_vol_m5": SCOUT_QUALITY_RECLAIM_MIN_VOL_M5,
        "min_tx_m5": SCOUT_QUALITY_RECLAIM_MIN_TX_M5,
        "max_negative_m5": -8.0,
    },
    "pullback_tiny_scout": {
        "max_negative_m5": -15.0,
    },
    "lotto_upstream_miss_tiny_scout": {
        "max_negative_m5": -15.0,
    },
    "lotto_upstream_realtime_tiny_scout": {
        "max_negative_m5": -15.0,
    },
}


def _f(value, default=None):
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_number(*values, default=None):
    for value in values:
        number = _f(value, None)
        if number is not None:
            return number
    return default


def _rate_to_pct(value):
    number = _f(value, None)
    if number is None:
        return None
    if 0.0 < number <= 1.0:
        return number * 100.0
    return number


def _profile_for(mode):
    mode_name = str(mode or "")
    profile = dict(_BASE_PROFILE)
    profile.update(_PROFILE_OVERRIDES.get(mode_name, {}))
    return mode_name or "unknown", profile


def _sources(*dicts):
    for value in dicts:
        if isinstance(value, dict):
            yield value


def _value_from_sources(name, *dicts):
    for source in _sources(*dicts):
        if name in source:
            return source.get(name)
    return None


def _observed_values(
    *,
    trend=None,
    lifecycle=None,
    gmgn=None,
    live_concentration=None,
    position_size_sol=None,
    current_mc=None,
    liquidity_usd=None,
    top1_pct=None,
    top10_pct=None,
):
    lifecycle = lifecycle or {}
    features = lifecycle.get("lifecycle_features") or {}
    gmgn_features = (gmgn or {}).get("features") if isinstance(gmgn, dict) else {}
    gmgn_features = gmgn_features if isinstance(gmgn_features, dict) else {}
    trend = trend or {}
    live_concentration = live_concentration or {}

    buys_m5 = _first_number(
        _value_from_sources("buys_m5", trend, features, gmgn_features),
        default=None,
    )
    sells_m5 = _first_number(
        _value_from_sources("sells_m5", trend, features, gmgn_features),
        default=None,
    )
    tx_m5 = _first_number(
        _value_from_sources("tx_m5", trend, features, gmgn_features),
        default=None,
    )
    if tx_m5 is None and buys_m5 is not None and sells_m5 is not None:
        tx_m5 = buys_m5 + sells_m5

    buy_sell_ratio = _first_number(
        _value_from_sources("buy_sell_ratio", trend, features, gmgn_features),
        default=None,
    )
    if buy_sell_ratio is None and buys_m5 is not None:
        buy_sell_ratio = buys_m5 / max(sells_m5 or 0.0, 1.0)

    observed_liquidity = _first_number(
        liquidity_usd,
        _value_from_sources("liquidity_usd", trend, features, gmgn_features),
        default=None,
    )
    observed_top1 = _first_number(
        top1_pct,
        _value_from_sources("top1_pct", live_concentration, trend, features, gmgn_features),
        default=None,
    )
    observed_top10 = _first_number(
        top10_pct,
        _value_from_sources("top10_pct", live_concentration, trend, features, gmgn_features),
        default=None,
    )
    if observed_top10 is None:
        observed_top10 = _rate_to_pct(
            _value_from_sources("top10_holder_rate", gmgn_features, gmgn or {})
        )

    return {
        "price_change_m5": _first_number(
            _value_from_sources("price_change_m5", trend, features, gmgn_features),
            default=None,
        ),
        "buy_sell_ratio": buy_sell_ratio,
        "vol_m5": _first_number(
            _value_from_sources("vol_m5", trend, features, gmgn_features),
            default=None,
        ),
        "tx_m5": tx_m5,
        "buys_m5": buys_m5,
        "sells_m5": sells_m5,
        "liquidity_usd": observed_liquidity,
        "top1_pct": observed_top1,
        "top10_pct": observed_top10,
        "current_mc": _first_number(
            current_mc,
            _value_from_sources("market_cap", trend, features, gmgn_features),
            _value_from_sources("fdv", trend, features, gmgn_features),
            default=None,
        ),
        "position_size_sol": _f(position_size_sol, None),
    }


def _result(passed, reason, *, mode, profile, observed, thresholds, extras=None):
    payload = {
        "pass": bool(passed),
        "mode": mode,
        "profile": profile,
        "reason": reason,
        "observed": observed,
        "thresholds": thresholds,
    }
    if extras:
        payload.update(extras)
    return payload


def evaluate_scout_quality(
    *,
    mode=None,
    route=None,
    trend=None,
    lifecycle=None,
    gmgn=None,
    token_risk=None,
    spread_memory=None,
    live_concentration=None,
    position_size_sol=None,
    current_mc=None,
    liquidity_usd=None,
    top1_pct=None,
    top10_pct=None,
):
    """Return whether a paper tiny scout has enough current quality to enter."""
    mode_name, profile = _profile_for(mode)
    observed = _observed_values(
        trend=trend,
        lifecycle=lifecycle,
        gmgn=gmgn,
        live_concentration=live_concentration,
        position_size_sol=position_size_sol,
        current_mc=current_mc,
        liquidity_usd=liquidity_usd,
        top1_pct=top1_pct,
        top10_pct=top10_pct,
    )
    thresholds = {
        key: profile[key]
        for key in (
            "min_liquidity_usd",
            "min_buy_sell_ratio",
            "min_vol_m5",
            "min_tx_m5",
            "max_negative_m5",
            "max_top1_pct",
            "max_top10_pct",
            "max_size_sol",
        )
    }
    extras = {
        "route": route,
        "token_risk": token_risk or {},
        "spread_memory": spread_memory or {},
        "gmgn_action": (gmgn or {}).get("action") if isinstance(gmgn, dict) else None,
    }

    size_sol = observed.get("position_size_sol")
    if size_sol is not None and size_sol > profile["max_size_sol"] + 1e-9:
        return _result(
            False,
            "scout_quality_size_cap",
            mode=mode_name,
            profile=profile,
            observed=observed,
            thresholds=thresholds,
            extras=extras,
        )

    token_risk = token_risk or {}
    if profile.get("block_recent_failures") and (
        int(token_risk.get("severe_failure_count") or 0) > 0
        or int(token_risk.get("risk_memory_count") or 0) > 0
    ):
        return _result(
            False,
            "scout_quality_recent_token_failure",
            mode=mode_name,
            profile=profile,
            observed=observed,
            thresholds=thresholds,
            extras=extras,
        )
    if token_risk.get("blocked") and not token_risk.get("cooldown_expired"):
        return _result(
            False,
            token_risk.get("reason") or "scout_quality_token_risk_blocked",
            mode=mode_name,
            profile=profile,
            observed=observed,
            thresholds=thresholds,
            extras=extras,
        )

    gmgn_action = (gmgn or {}).get("action") if isinstance(gmgn, dict) else None
    if profile.get("block_gmgn_reject") and gmgn_action in {"reject", "shadow_reject"}:
        return _result(
            False,
            (gmgn or {}).get("reason") or "scout_quality_gmgn_reject",
            mode=mode_name,
            profile=profile,
            observed=observed,
            thresholds=thresholds,
            extras=extras,
        )

    liquidity = observed.get("liquidity_usd")
    if not profile.get("allow_unknown_liquidity"):
        if liquidity is None or liquidity < profile["min_liquidity_usd"]:
            return _result(
                False,
                "scout_quality_liquidity_low",
                mode=mode_name,
                profile=profile,
                observed=observed,
                thresholds=thresholds,
                extras=extras,
            )
    elif liquidity is not None and liquidity > 0 and liquidity < profile["min_liquidity_usd"]:
        return _result(
            False,
            "scout_quality_liquidity_low",
            mode=mode_name,
            profile=profile,
            observed=observed,
            thresholds=thresholds,
            extras=extras,
        )

    if observed.get("buy_sell_ratio") is None or observed["buy_sell_ratio"] < profile["min_buy_sell_ratio"]:
        return _result(
            False,
            "scout_quality_buy_pressure_weak",
            mode=mode_name,
            profile=profile,
            observed=observed,
            thresholds=thresholds,
            extras=extras,
        )
    if observed.get("vol_m5") is None or observed["vol_m5"] < profile["min_vol_m5"]:
        return _result(
            False,
            "scout_quality_volume_low",
            mode=mode_name,
            profile=profile,
            observed=observed,
            thresholds=thresholds,
            extras=extras,
        )
    if observed.get("tx_m5") is None or observed["tx_m5"] < profile["min_tx_m5"]:
        return _result(
            False,
            "scout_quality_tx_low",
            mode=mode_name,
            profile=profile,
            observed=observed,
            thresholds=thresholds,
            extras=extras,
        )
    if observed.get("price_change_m5") is not None and observed["price_change_m5"] < profile["max_negative_m5"]:
        return _result(
            False,
            "scout_quality_negative_trend",
            mode=mode_name,
            profile=profile,
            observed=observed,
            thresholds=thresholds,
            extras=extras,
        )
    if observed.get("top1_pct") is not None and observed["top1_pct"] > profile["max_top1_pct"]:
        return _result(
            False,
            "scout_quality_top1_high",
            mode=mode_name,
            profile=profile,
            observed=observed,
            thresholds=thresholds,
            extras=extras,
        )
    if observed.get("top10_pct") is not None and observed["top10_pct"] > profile["max_top10_pct"]:
        return _result(
            False,
            "scout_quality_top10_high",
            mode=mode_name,
            profile=profile,
            observed=observed,
            thresholds=thresholds,
            extras=extras,
        )

    return _result(
        True,
        "scout_quality_pass",
        mode=mode_name,
        profile=profile,
        observed=observed,
        thresholds=thresholds,
        extras=extras,
    )
