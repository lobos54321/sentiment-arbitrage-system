#!/usr/bin/env python3
"""
Lifecycle-aware token classifier.

This is an attribution layer first: it labels where a token appears to be in
its lifecycle and how much live strength it still has. It should not directly
change trading decisions until enough paper data proves which states work.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import time


PROMISING_STATES = {
    "NEWBORN_LAUNCH",
    "FIRST_PUMP",
    "ATH_CONTINUATION",
    "ATH_SHALLOW_PULLBACK",
}

BAD_STATES = {
    "ATH_DEEP_RESET",
    "DEAD_CAT_BOUNCE",
    "DISTRIBUTION",
    "DEAD",
}


@dataclass(frozen=True)
class LifecycleClassification:
    state: str
    vitality_score: float
    entry_bias: str
    features: dict
    reasons: list[str]

    def to_payload(self) -> dict:
        return {
            "lifecycle_state": self.state,
            "vitality_score": self.vitality_score,
            "entry_bias": self.entry_bias,
            "lifecycle_features": self.features,
            "lifecycle_reasons": self.reasons,
        }

    def asdict(self) -> dict:
        return asdict(self)


def _f(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _i(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _clamp(value, lo=0.0, hi=100.0):
    return max(lo, min(hi, value))


def _signal_age_sec(signal_ts=None, now=None, fallback=None):
    now = now or time.time()
    if fallback is not None:
        return max(0.0, _f(fallback, 0.0))
    ts = _i(signal_ts, 0)
    if ts <= 0:
        return 0.0
    if ts > 1_000_000_000_000:
        ts = ts // 1000
    return max(0.0, now - ts)


def build_lifecycle_features(
    *,
    signal=None,
    watchlist_entry=None,
    dex_snapshot=None,
    live_concentration=None,
    route=None,
    signal_ts=None,
    signal_price=None,
    quote_available=None,
    mark_quote_gap=None,
    current_pnl=None,
    peak_pnl=None,
    now=None,
):
    signal = signal or {}
    watchlist_entry = watchlist_entry or {}
    dex_snapshot = dex_snapshot or {}
    live_concentration = live_concentration or {}
    now = now or time.time()

    signal_ts = signal_ts or signal.get("timestamp") or watchlist_entry.get("signal_ts")
    market_cap = (
        _f(signal.get("market_cap"), 0.0)
        or _f(watchlist_entry.get("signal_mc"), 0.0)
        or _f(dex_snapshot.get("market_cap"), 0.0)
        or _f(dex_snapshot.get("fdv"), 0.0)
    )
    ath_mc = max(
        _f(watchlist_entry.get("last_ath_mc"), 0.0),
        _f(signal.get("last_ath_mc"), 0.0),
        _f(signal.get("ath_mc"), 0.0),
    )
    drawdown_from_ath_pct = None
    ath_distance_pct = None
    if ath_mc > 0 and market_cap > 0:
        drawdown_from_ath_pct = max(0.0, (ath_mc - market_cap) / ath_mc)
        ath_distance_pct = (market_cap / ath_mc) - 1.0

    buys_m5 = _i(dex_snapshot.get("buys_m5"), 0)
    sells_m5 = _i(dex_snapshot.get("sells_m5"), 0)
    tx_m5 = buys_m5 + sells_m5
    buy_sell_ratio = buys_m5 / max(sells_m5, 1)
    vol_m5 = _f(dex_snapshot.get("vol_m5"), 0.0)
    vol_h1 = _f(dex_snapshot.get("vol_h1"), 0.0)
    volume_accel = ((vol_m5 * 12.0) / vol_h1) if vol_h1 > 0 else None

    price_change_m5 = _f(dex_snapshot.get("price_change_m5"), None)
    price_change_h1 = _f(dex_snapshot.get("price_change_h1"), None)
    if price_change_m5 is None:
        price_change_m5 = _f(signal.get("price_change_m5"), 0.0)
    if price_change_h1 is None:
        price_change_h1 = _f(signal.get("price_change_h1"), 0.0)

    age_sec = _signal_age_sec(signal_ts=signal_ts, now=now, fallback=watchlist_entry.get("age_sec"))
    if watchlist_entry.get("added_at"):
        age_sec = max(0.0, now - _f(watchlist_entry.get("added_at"), now))

    quote_available = quote_available
    if quote_available is None:
        quote_available = not bool(dex_snapshot.get("liquidity_unknown")) or _f(dex_snapshot.get("liquidity_usd"), 0.0) > 0

    return {
        "age_sec": age_sec,
        "route": route or watchlist_entry.get("signal_route") or watchlist_entry.get("type") or signal.get("signal_type"),
        "is_ath": bool(signal.get("is_ath") or watchlist_entry.get("type") == "ATH"),
        "market_cap": market_cap,
        "ath_mc": ath_mc,
        "ath_distance_pct": ath_distance_pct,
        "drawdown_from_ath_pct": drawdown_from_ath_pct,
        "price_change_m5": price_change_m5,
        "price_change_h1": price_change_h1,
        "vol_m5": vol_m5,
        "vol_h1": vol_h1,
        "volume_accel": volume_accel,
        "buys_m5": buys_m5,
        "sells_m5": sells_m5,
        "tx_m5": tx_m5,
        "buy_sell_ratio": buy_sell_ratio,
        "liquidity_usd": _f(dex_snapshot.get("liquidity_usd"), 0.0),
        "liquidity_unknown": bool(dex_snapshot.get("liquidity_unknown")),
        "dex_id": dex_snapshot.get("dex_id") or "",
        "quote_available": bool(quote_available),
        "mark_quote_gap": _f(mark_quote_gap, None),
        "current_pnl": _f(current_pnl, None),
        "peak_pnl": _f(peak_pnl, None),
        "hard_gate_status": signal.get("hard_gate_status"),
        "signal_price": _f(signal_price or signal.get("signal_price"), None),
        "live_top1_pct": _f(live_concentration.get("top1_pct"), None),
        "live_top10_pct": _f(live_concentration.get("top10_pct"), None),
    }


def classify_lifecycle(features=None, **kwargs):
    features = dict(features or build_lifecycle_features(**kwargs))
    reasons = []

    age_sec = _f(features.get("age_sec"), 0.0)
    pc5 = _f(features.get("price_change_m5"), 0.0)
    pch1 = _f(features.get("price_change_h1"), 0.0)
    vol_accel = features.get("volume_accel")
    vol_accel = _f(vol_accel, None)
    buy_sell_ratio = _f(features.get("buy_sell_ratio"), 0.0)
    tx_m5 = _i(features.get("tx_m5"), 0)
    liq = _f(features.get("liquidity_usd"), 0.0)
    drawdown = features.get("drawdown_from_ath_pct")
    drawdown = _f(drawdown, None)
    is_ath = bool(features.get("is_ath"))
    market_cap = _f(features.get("market_cap"), 0.0)
    quote_available = bool(features.get("quote_available"))
    mark_quote_gap = _f(features.get("mark_quote_gap"), 0.0)

    state = "UNKNOWN"

    if not quote_available and tx_m5 <= 2 and liq <= 0:
        state = "DEAD"
        reasons.append("quote_unavailable_and_no_activity")
    elif drawdown is not None and drawdown > 0.60 and (vol_accel is None or vol_accel < 1.0) and buy_sell_ratio < 1.2:
        state = "DEAD_CAT_BOUNCE"
        reasons.append("deep_ath_drawdown_weak_recovery")
    elif drawdown is not None and drawdown > 0.45:
        state = "ATH_DEEP_RESET"
        reasons.append("deep_ath_reset")
    elif drawdown is not None and drawdown <= 0.15 and (is_ath or pc5 >= 0):
        state = "ATH_CONTINUATION" if pc5 >= 5 else "ATH_SHALLOW_PULLBACK"
        reasons.append("near_ath_structure")
    elif drawdown is not None and drawdown <= 0.30:
        state = "PULLBACK_BOUNCE"
        reasons.append("moderate_pullback_bounce")
    elif age_sec <= 600 and (market_cap <= 50_000 or pc5 >= 20):
        state = "FIRST_PUMP" if pc5 >= 20 or pch1 >= 50 else "NEWBORN_LAUNCH"
        reasons.append("fresh_token_early_structure")
    elif age_sec <= 1800 and pc5 >= 20 and buy_sell_ratio >= 1.2:
        state = "FIRST_PUMP"
        reasons.append("young_token_with_buy_pressure")
    elif tx_m5 <= 2 and liq <= 0 and age_sec > 1800:
        state = "DEAD"
        reasons.append("inactive_mature_token")

    if state in {"ATH_CONTINUATION", "ATH_SHALLOW_PULLBACK", "PULLBACK_BOUNCE"}:
        if vol_accel is not None and vol_accel >= 2.0 and abs(pc5) <= 5 and buy_sell_ratio <= 1.0:
            state = "DISTRIBUTION"
            reasons.append("high_volume_flat_price_sell_pressure")

    score = 45.0
    score += min(max(pc5, -25.0), 60.0) * 0.35
    score += min(max(pch1, -50.0), 120.0) * 0.08
    if vol_accel is not None:
        score += min(vol_accel, 4.0) * 5.0
    score += min(buy_sell_ratio, 3.0) * 5.0
    score += min(tx_m5, 30) * 0.35
    if liq >= 5_000:
        score += 6.0
    elif features.get("liquidity_unknown") and features.get("dex_id") == "pumpfun" and tx_m5 >= 20:
        score += 3.0
    else:
        score -= 8.0
    if drawdown is not None:
        score -= min(drawdown, 0.9) * 30.0
    if state in PROMISING_STATES:
        score += 8.0
    if state in BAD_STATES:
        score -= 18.0
    if mark_quote_gap and abs(mark_quote_gap) >= 0.08:
        score -= min(abs(mark_quote_gap), 0.5) * 40.0

    score = round(_clamp(score), 2)
    if state in BAD_STATES or score < 35:
        bias = "REJECT"
    elif state in PROMISING_STATES and score >= 65:
        bias = "PROBE"
    elif score >= 50:
        bias = "WAIT"
    else:
        bias = "OBSERVE"

    return LifecycleClassification(
        state=state,
        vitality_score=score,
        entry_bias=bias,
        features=features,
        reasons=reasons,
    )
