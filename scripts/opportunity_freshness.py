"""Opportunity freshness for A_CLASS_FASTLANE.

Raw signal age is only one fact. A stale signal can become actionable again
when quote, GMGN activity, reclaim, ATH refresh, or source activity renews.
"""

from dataclasses import asdict, dataclass
import time
from typing import Optional

from fastlane_config import load_a_class_config


def _get(candidate, key, default=None):
    if isinstance(candidate, dict):
        return candidate.get(key, default)
    return getattr(candidate, key, default)


def _truthy(value):
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "clean", "ok"}
    return bool(value)


def _safe_float(value, default=None):
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _age_from_ts_or_age(candidate, now_ts, ts_key, age_key):
    age = _safe_float(_get(candidate, age_key), None)
    if age is not None:
        return max(0.0, age), max(0.0, now_ts - age)
    ts = _safe_float(_get(candidate, ts_key), None)
    if ts is None:
        return None, None
    return max(0.0, now_ts - ts), ts


@dataclass(frozen=True)
class FreshnessDecision:
    fresh: bool
    opportunity_ts: Optional[float]
    freshness_sources: list
    raw_signal_age_sec: Optional[float]
    opportunity_age_sec: Optional[float]
    reason: str
    data_confidence: str = "unknown"

    def to_dict(self):
        return asdict(self)


def evaluate_opportunity_freshness(candidate, now_ts=None, config=None):
    config = config or load_a_class_config()
    now_ts = float(now_ts if now_ts is not None else time.time())
    raw_signal_age_sec = None
    signal_ts = _safe_float(_get(candidate, "signal_ts"), None)
    if signal_ts is not None:
        raw_signal_age_sec = max(0.0, now_ts - signal_ts)

    sources = []
    source_ages = []

    quote_age, quote_ts = _age_from_ts_or_age(candidate, now_ts, "quote_ts", "quote_age_sec")
    quote_clean = _truthy(_get(candidate, "quote_clean", True))
    if (
        quote_age is not None
        and quote_age <= config.quote_max_age_sec
        and _truthy(_get(candidate, "quote_available", False))
        and _truthy(_get(candidate, "quote_executable", False))
        and quote_clean
    ):
        sources.append("fresh_quote")
        source_ages.append((quote_age, quote_ts))

    momentum_age, momentum_ts = _age_from_ts_or_age(candidate, now_ts, "momentum_ts", "momentum_age_sec")
    if _truthy(_get(candidate, "fresh_momentum", False)) or (
        momentum_age is not None
        and momentum_age <= 30
        and _safe_float(_get(candidate, "momentum_pct"), 0.0) > 0
    ):
        sources.append("fresh_momentum")
        source_ages.append((0.0 if momentum_age is None else momentum_age, momentum_ts or now_ts))

    gmgn_age, gmgn_ts = _age_from_ts_or_age(candidate, now_ts, "gmgn_last_seen_ts", "gmgn_last_seen_age_sec")
    gmgn_activity = (
        _truthy(_get(candidate, "gmgn_activity_fresh", False))
        or _truthy(_get(candidate, "gmgn_changed_count_increased", False))
        or _truthy(_get(candidate, "gmgn_volume_confirmed", False))
        or _safe_float(_get(candidate, "gmgn_buy_pressure"), 0.0) > 0
    )
    if gmgn_activity and (gmgn_age is None or gmgn_age <= 60):
        sources.append("fresh_gmgn_activity")
        source_ages.append((0.0 if gmgn_age is None else gmgn_age, gmgn_ts or now_ts))

    reclaim_age, reclaim_ts = _age_from_ts_or_age(candidate, now_ts, "reclaim_ts", "reclaim_age_sec")
    if _truthy(_get(candidate, "fresh_reclaim", False)) and (reclaim_age is None or reclaim_age <= 60):
        sources.append("fresh_reclaim")
        source_ages.append((0.0 if reclaim_age is None else reclaim_age, reclaim_ts or now_ts))

    ath_age, ath_ts = _age_from_ts_or_age(candidate, now_ts, "ath_refresh_ts", "ath_refresh_age_sec")
    if (
        _truthy(_get(candidate, "fresh_ath_refresh", False))
        or _truthy(_get(candidate, "ath_count_increased", False))
    ) and (ath_age is None or ath_age <= 60):
        sources.append("fresh_ath_refresh")
        source_ages.append((0.0 if ath_age is None else ath_age, ath_ts or now_ts))

    source_age, source_ts = _age_from_ts_or_age(candidate, now_ts, "source_hit_ts", "source_hit_age_sec")
    if (
        _truthy(_get(candidate, "fresh_source_hit", False))
        or _truthy(_get(candidate, "premium_source_repeat_hit", False))
    ) and (source_age is None or source_age <= 60):
        sources.append("fresh_source_hit")
        source_ages.append((0.0 if source_age is None else source_age, source_ts or now_ts))

    if not source_ages:
        return FreshnessDecision(
            fresh=False,
            opportunity_ts=None,
            freshness_sources=[],
            raw_signal_age_sec=raw_signal_age_sec,
            opportunity_age_sec=None,
            reason="no_fresh_opportunity",
            data_confidence=str(_get(candidate, "data_confidence", "unknown") or "unknown"),
        )

    opportunity_age_sec, opportunity_ts = min(source_ages, key=lambda item: item[0])
    fresh = opportunity_age_sec <= config.opportunity_max_age_sec
    if fresh:
        reason = "fresh_opportunity"
    elif opportunity_age_sec <= config.opportunity_shadow_max_age_sec:
        reason = "opportunity_age_shadow_window"
    else:
        reason = "opportunity_stale"

    return FreshnessDecision(
        fresh=fresh,
        opportunity_ts=opportunity_ts,
        freshness_sources=sources,
        raw_signal_age_sec=raw_signal_age_sec,
        opportunity_age_sec=opportunity_age_sec,
        reason=reason,
        data_confidence=str(_get(candidate, "data_confidence", "unknown") or "unknown"),
    )
