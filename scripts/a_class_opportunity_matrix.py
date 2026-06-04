"""A_CLASS 18-cell opportunity matrix.

The matrix is intentionally advisory. It turns six dimensions into
GREEN/YELLOW/RED evidence so later reviewers can explain why a tiny-probe was
allowed to stay in shadow, become a would-enter, or remain blocked.
"""

from __future__ import annotations

from typing import Any

from fastlane_config import load_a_class_config


GREEN = "GREEN"
YELLOW = "YELLOW"
RED = "RED"

DIMENSION_WEIGHTS = {
    "source_strength": 20,
    "execution_quality": 20,
    "market_flow": 15,
    "security_cleanliness": 20,
    "freshness_lifecycle": 15,
    "historical_ev": 10,
}

STATE_MULTIPLIER = {
    GREEN: 1.0,
    YELLOW: 0.55,
    RED: 0.0,
}


def _get(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "ok", "clean", "tradable"}
    return bool(value)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number


def _freshness_dict(freshness_decision: Any) -> dict:
    if freshness_decision is None:
        return {}
    if isinstance(freshness_decision, dict):
        return freshness_decision
    to_dict = getattr(freshness_decision, "to_dict", None)
    if callable(to_dict):
        return to_dict()
    return {
        "fresh": _truthy(_get(freshness_decision, "fresh", False)),
        "freshness_sources": _get(freshness_decision, "freshness_sources", []) or [],
        "opportunity_age_sec": _get(freshness_decision, "opportunity_age_sec"),
        "reason": _get(freshness_decision, "reason"),
    }


def _cell(state: str, score: float, reasons: list[str], *, hard_red: bool = False) -> dict:
    return {
        "state": state,
        "score": round(float(score), 4),
        "reasons": reasons,
        "hard_red": bool(hard_red),
    }


def _source_strength(candidate: Any) -> dict:
    hits = []
    for attr, reason in (
        ("gmgn_pre_seen", "gmgn_pre_seen"),
        ("source_resonance", "source_resonance"),
        ("fresh_ath_refresh", "fresh_ath_refresh"),
        ("premium_source_repeat_hit", "premium_source_repeat_hit"),
        ("fresh_source_hit", "fresh_source_hit"),
        ("missed_dog_cohort_strong", "missed_dog_cohort_strong"),
    ):
        if _truthy(_get(candidate, attr, False)):
            hits.append(reason)
    if len(hits) >= 2:
        return _cell(GREEN, 100, hits)
    if hits:
        return _cell(YELLOW, 55, hits)
    return _cell(RED, 0, ["no_strong_source_evidence"])


def _execution_quality(candidate: Any, config: Any) -> dict:
    reasons = []
    hard_red = False
    quote_ok = _truthy(_get(candidate, "quote_available", False)) and _truthy(_get(candidate, "quote_executable", False))
    route_ok = _truthy(_get(candidate, "route_available", False))
    quote_age = _safe_float(_get(candidate, "quote_age_sec"), None)
    liquidity = _safe_float(_get(candidate, "liquidity_usd"), None)
    spread = _safe_float(_get(candidate, "spread_pct"), None)
    route = _get(candidate, "route_bucket", "A_GRADE")
    min_liq = config.min_liquidity_for_route(route)
    max_spread = config.max_spread_for_route(route)

    if not quote_ok:
        reasons.append("quote_not_executable")
        hard_red = True
    if not route_ok:
        reasons.append("route_unavailable")
        hard_red = True
    if quote_age is None:
        reasons.append("quote_age_unknown")
        hard_red = True
    elif quote_age > config.quote_max_age_sec:
        reasons.append("quote_stale")
        hard_red = True
    else:
        reasons.append("quote_fresh")
    if liquidity is None:
        reasons.append("liquidity_unknown")
        hard_red = True
    elif liquidity < min_liq:
        reasons.append("liquidity_below_min")
        hard_red = True
    else:
        reasons.append("liquidity_sufficient")
    if spread is None and not _truthy(_get(candidate, "spread_verified", False)):
        reasons.append("spread_unknown")
    elif spread is not None and abs(spread) > config.extreme_spread_block_pct:
        reasons.append("spread_extreme")
        hard_red = True
    elif spread is not None and abs(spread) > max_spread:
        reasons.append("spread_too_high")
        hard_red = True
    else:
        reasons.append("spread_acceptable")
    if hard_red:
        return _cell(RED, 0, reasons, hard_red=True)
    if _truthy(_get(candidate, "route_stable_recent", False)) or _truthy(_get(candidate, "quote_clean_verified", False)):
        return _cell(GREEN, 100, reasons + ["route_or_quote_stability_seen"])
    return _cell(YELLOW, 55, reasons + ["route_stability_not_confirmed"])


def _market_flow(candidate: Any, freshness: dict) -> dict:
    reasons = []
    momentum = _safe_float(_get(candidate, "momentum_pct"), None)
    reason_text = f"{_get(candidate, 'source_reason', '')} {_get(candidate, 'route_failure_reason', '')}".lower()
    freshness_sources = set(freshness.get("freshness_sources") or [])
    if momentum is not None and momentum < 0:
        return _cell(RED, 0, ["negative_momentum"])
    if "negative_trend" in reason_text or "declining" in reason_text:
        return _cell(RED, 0, ["negative_trend_evidence"])
    if _truthy(_get(candidate, "fresh_momentum", False)) or (momentum is not None and momentum > 0):
        reasons.append("fresh_positive_momentum")
    if _truthy(_get(candidate, "gmgn_activity_fresh", False)) or "fresh_gmgn_activity" in freshness_sources:
        reasons.append("fresh_gmgn_activity")
    if _truthy(_get(candidate, "ath_continuation", False)):
        reasons.append("ath_continuation")
    if _truthy(_get(candidate, "lotto_early_momentum", False)):
        reasons.append("lotto_early_momentum")
    if len(reasons) >= 2:
        return _cell(GREEN, 100, reasons)
    if reasons or "fresh_quote" in freshness_sources or _truthy(_get(candidate, "fresh_reclaim", False)):
        return _cell(YELLOW, 55, reasons or ["quote_or_reclaim_fresh_but_flow_unconfirmed"])
    return _cell(YELLOW, 45, ["market_flow_unconfirmed"])


def _security_cleanliness(candidate: Any, config: Any) -> dict:
    reasons = []
    hard_red = False
    risk_text = " ".join(str(flag).lower() for flag in (_get(candidate, "risk_flags", []) or []))
    if _truthy(_get(candidate, "creator_close", False)):
        reasons.append("creator_close")
        hard_red = True
    if any(token in risk_text for token in ("rug", "honeypot", "blacklist", "trapped", "creator_dump")):
        reasons.append("security_red_flag")
        hard_red = True
    numeric_checks = (
        ("top10_pct", "top10_too_high", config.top10_hard_max_pct),
        ("bundler_rate", "bundler_red_flag", config.bundler_hard_max),
        ("rat_trader_rate", "rat_trader_red_flag", config.rat_trader_hard_max),
        ("entrapment_ratio", "entrapment_red_flag", config.entrapment_hard_max),
    )
    unknowns = 0
    for attr, reason, threshold in numeric_checks:
        value = _safe_float(_get(candidate, attr), None)
        if value is None:
            unknowns += 1
            continue
        if value > threshold:
            reasons.append(reason)
            hard_red = True
    if hard_red:
        return _cell(RED, 0, reasons, hard_red=True)
    if unknowns >= 3:
        return _cell(YELLOW, 55, ["security_metrics_partially_unknown"])
    return _cell(GREEN, 100, ["no_hard_security_red_flags"])


def _freshness_lifecycle(candidate: Any, freshness: dict) -> dict:
    sources = freshness.get("freshness_sources") or []
    age = _safe_float(freshness.get("opportunity_age_sec"), None)
    if not _truthy(freshness.get("fresh", False)):
        return _cell(RED, 0, [freshness.get("reason") or "no_fresh_opportunity"])
    reasons = [f"source:{source}" for source in sources] or ["fresh_opportunity"]
    if age is not None and age <= 30 and len(sources) >= 2:
        return _cell(GREEN, 100, reasons + ["opportunity_age_le_30s"])
    if age is not None and age <= 60:
        return _cell(YELLOW, 55, reasons + ["opportunity_age_le_60s"])
    return _cell(RED, 0, reasons + ["opportunity_age_above_60s"])


def _historical_ev(candidate: Any) -> dict:
    if _truthy(_get(candidate, "mode_circuit_broken", False)) or _truthy(_get(candidate, "recent_hard_loss", False)):
        return _cell(RED, 0, ["mode_or_token_recent_loss_red"])
    ev = _safe_float(_get(candidate, "cohort_ev", None), None)
    last20_ev = _safe_float(_get(candidate, "last20_ev", None), None)
    if _truthy(_get(candidate, "missed_dog_cohort_strong", False)):
        return _cell(GREEN, 100, ["missed_dog_cohort_strong"])
    if ev is not None or last20_ev is not None:
        best = max(value for value in (ev, last20_ev) if value is not None)
        if best > 0:
            return _cell(GREEN, 100, ["positive_cohort_ev"])
        if best < 0:
            return _cell(RED, 0, ["negative_cohort_ev"])
    return _cell(YELLOW, 55, ["historical_ev_unknown_shadow_first"])


def evaluate_a_class_opportunity_matrix(
    candidate: Any,
    freshness_decision: Any = None,
    *,
    config: Any = None,
) -> dict:
    """Return the 6x3 A_CLASS opportunity matrix for one candidate."""
    config = config or load_a_class_config()
    freshness = _freshness_dict(freshness_decision)
    dimensions = {
        "source_strength": _source_strength(candidate),
        "execution_quality": _execution_quality(candidate, config),
        "market_flow": _market_flow(candidate, freshness),
        "security_cleanliness": _security_cleanliness(candidate, config),
        "freshness_lifecycle": _freshness_lifecycle(candidate, freshness),
        "historical_ev": _historical_ev(candidate),
    }
    score = 0.0
    for name, detail in dimensions.items():
        score += DIMENSION_WEIGHTS[name] * STATE_MULTIPLIER.get(detail["state"], 0.0)
    green_count = sum(1 for detail in dimensions.values() if detail["state"] == GREEN)
    yellow_count = sum(1 for detail in dimensions.values() if detail["state"] == YELLOW)
    red_count = sum(1 for detail in dimensions.values() if detail["state"] == RED)
    hard_red_dimensions = [
        name
        for name, detail in dimensions.items()
        if detail["state"] == RED and detail.get("hard_red")
    ]
    if hard_red_dimensions or red_count:
        grade = "REJECT"
    elif score >= 92:
        grade = "A_PLUS"
    elif score >= 82:
        grade = "STRONG_A"
    elif score >= 70:
        grade = "A"
    else:
        grade = "SHADOW"
    return {
        "matrix_version": "v1.a_class_18_cell",
        "dimensions": dimensions,
        "source_strength": dimensions["source_strength"]["state"],
        "execution_quality": dimensions["execution_quality"]["state"],
        "market_flow": dimensions["market_flow"]["state"],
        "security_cleanliness": dimensions["security_cleanliness"]["state"],
        "freshness_lifecycle": dimensions["freshness_lifecycle"]["state"],
        "historical_ev": dimensions["historical_ev"]["state"],
        "green_count": green_count,
        "yellow_count": yellow_count,
        "red_count": red_count,
        "hard_red_dimensions": hard_red_dimensions,
        "matrix_score": round(score, 4),
        "matrix_grade": grade,
        "action_floor": "BLOCK" if hard_red_dimensions else ("SHADOW" if red_count else "TINY_CANDIDATE"),
        "advisory_only": True,
    }
