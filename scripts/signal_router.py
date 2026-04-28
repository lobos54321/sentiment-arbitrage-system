#!/usr/bin/env python3
"""
Signal router for paper-trader strategy lanes.

Routes early NEW_TRENDING signals into the LOTTO fast lane, routes ATH updates
for existing LOTTO positions into a hold-boost path, and leaves the existing
matrix pipeline responsible for the rest.
"""

from dataclasses import dataclass
import os
import time


ROUTE_LOTTO = "LOTTO"
ROUTE_LOTTO_HOLD_BOOST = "LOTTO_HOLD_BOOST"
ROUTE_MATRIX = "MATRIX"
ROUTE_WATCHLIST = "WATCHLIST"
ROUTE_REJECT = "REJECT"


LOTTO_MC_MAX_USD = float(os.environ.get("LOTTO_MC_MAX_USD", "30000"))
WATCHLIST_MC_MAX_USD = float(os.environ.get("WATCHLIST_MC_MAX_USD", "80000"))
MATRIX_ATH_MC_MAX_USD = float(os.environ.get("MATRIX_ATH_MC_MAX_USD", "200000"))
LOTTO_SIGNAL_MAX_AGE_SEC = float(os.environ.get("LOTTO_SIGNAL_MAX_AGE_SEC", "30"))


@dataclass(frozen=True)
class RouteDecision:
    route: str
    reason: str
    signal_age_sec: float
    market_cap: float

    @property
    def is_lotto(self) -> bool:
        return self.route == ROUTE_LOTTO

    @property
    def is_lotto_boost(self) -> bool:
        return self.route == ROUTE_LOTTO_HOLD_BOOST


def normalize_signal_ts(value, *, default_now=None):
    """Return signal timestamp in seconds, accepting ms or sec input."""
    if default_now is None:
        default_now = time.time()
    if value is None:
        return default_now
    try:
        ts = int(value)
    except (TypeError, ValueError):
        return default_now
    return ts // 1000 if ts > 1_000_000_000_000 else ts


def _signal_type(sig):
    explicit = (sig.get("signal_type") or "").upper()
    if explicit:
        return explicit
    description = sig.get("description") or ""
    if "New Trending" in description:
        return "NEW_TRENDING"
    if "ATH" in description or "All Time High" in description:
        return "ATH"
    return ""


def route_signal(sig, *, now=None, existing_entry=None):
    now = now or time.time()
    signal_ts_sec = normalize_signal_ts(sig.get("timestamp"), default_now=now)
    signal_age_sec = max(0.0, now - signal_ts_sec)
    market_cap = float(sig.get("market_cap") or 0)
    sig_type = _signal_type(sig)
    is_ath = bool(sig.get("is_ath")) or sig_type == "ATH"

    if is_ath and existing_entry and existing_entry.get("type") == ROUTE_LOTTO:
        if existing_entry.get("status") in {"holding", "moon_bag"}:
            return RouteDecision(
                ROUTE_LOTTO_HOLD_BOOST,
                "ath_for_active_lotto_position",
                signal_age_sec,
                market_cap,
            )

    if (
        not is_ath
        and sig_type == "NEW_TRENDING"
        and 0 < market_cap < LOTTO_MC_MAX_USD
    ):
        if signal_age_sec <= LOTTO_SIGNAL_MAX_AGE_SEC:
            return RouteDecision(
                ROUTE_LOTTO,
                "new_trending_under_mc_cap_fresh",
                signal_age_sec,
                market_cap,
            )
        return RouteDecision(
            ROUTE_WATCHLIST,
            f"lotto_signal_stale_{signal_age_sec:.0f}s",
            signal_age_sec,
            market_cap,
        )

    if (
        not is_ath
        and sig_type == "NEW_TRENDING"
        and LOTTO_MC_MAX_USD <= market_cap < WATCHLIST_MC_MAX_USD
    ):
        return RouteDecision(
            ROUTE_WATCHLIST,
            "new_trending_30k_80k_watchlist_matrix_light",
            signal_age_sec,
            market_cap,
        )

    if is_ath and market_cap >= MATRIX_ATH_MC_MAX_USD:
        return RouteDecision(
            ROUTE_WATCHLIST,
            "ath_high_mc_watch_only",
            signal_age_sec,
            market_cap,
        )

    return RouteDecision(
        ROUTE_MATRIX if is_ath else ROUTE_WATCHLIST,
        "default_matrix_pipeline",
        signal_age_sec,
        market_cap,
    )
