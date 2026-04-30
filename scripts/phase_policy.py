#!/usr/bin/env python3
"""Shadow phase policy for lifecycle-aware paper exits.

This module is intentionally attribution-first. It classifies the current
position phase and emits the action a phase-aware policy would take, without
directly changing trade execution.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import math


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


def _ema(values, period):
    values = [float(v) for v in values if v is not None]
    if not values:
        return None
    if len(values) < period:
        return sum(values) / len(values)
    alpha = 2.0 / (period + 1.0)
    ema = values[0]
    for value in values[1:]:
        ema = (value * alpha) + (ema * (1.0 - alpha))
    return ema


@dataclass(frozen=True)
class KlinePosition:
    state: str
    ema5: float | None
    ema10: float | None
    ema20: float | None
    ema5_slope_pct: float | None
    distance_ema10_pct: float | None
    distance_recent_high_pct: float | None
    distance_recent_low_pct: float | None
    reasons: list[str]

    def to_payload(self):
        return asdict(self)


@dataclass(frozen=True)
class PhasePolicyDecision:
    phase_state: str
    shadow_action: str
    reason: str
    sell_pct: float
    floor_pnl: float | None
    ev_score: float
    rug_risk_score: float
    kline_position_state: str
    reasons: list[str]
    kline: dict

    def to_payload(self):
        return asdict(self)


def classify_kline_position(bars=None, current_price=None):
    bars = list(bars or [])
    clean = []
    for bar in bars:
        try:
            close = float(bar.get("close"))
            ts = int(bar.get("ts") or bar.get("timestamp") or 0)
        except Exception:
            continue
        if close > 0:
            clean.append((ts, close))
    clean.sort(key=lambda item: item[0])
    if current_price is None and clean:
        current_price = clean[-1][1]
    current_price = _f(current_price, None)
    if not clean or current_price is None or current_price <= 0:
        return KlinePosition("NO_KLINE", None, None, None, None, None, None, None, ["missing_kline"])

    closes = [close for _, close in clean]
    ema5 = _ema(closes, 5)
    ema10 = _ema(closes, 10)
    ema20 = _ema(closes, 20)
    prev_ema5 = _ema(closes[:-1], 5) if len(closes) > 1 else None
    ema5_slope_pct = ((ema5 / prev_ema5) - 1.0) if ema5 and prev_ema5 else None
    recent = closes[-20:]
    recent_high = max(recent) if recent else current_price
    recent_low = min(recent) if recent else current_price
    distance_ema10_pct = ((current_price / ema10) - 1.0) if ema10 else None
    distance_recent_high_pct = ((current_price / recent_high) - 1.0) if recent_high else None
    distance_recent_low_pct = ((current_price / recent_low) - 1.0) if recent_low else None

    reasons = []
    above5 = bool(ema5 and current_price >= ema5)
    above10 = bool(ema10 and current_price >= ema10)
    above20 = bool(ema20 and current_price >= ema20)
    slope_up = bool(ema5_slope_pct is not None and ema5_slope_pct > 0.002)
    slope_down = bool(ema5_slope_pct is not None and ema5_slope_pct < -0.002)

    if above5 and above10 and above20 and slope_up:
        state = "ABOVE_RISING_EMA"
        reasons.append("above_ema5_10_20_and_slope_up")
    elif above5 and above10:
        state = "ABOVE_FLAT_EMA"
        reasons.append("above_ema5_10")
    elif above5 and not above10:
        state = "BELOW_EMA_RECLAIM"
        reasons.append("reclaimed_ema5_below_ema10")
    elif not above5 and not above10 and slope_down:
        state = "EMA_BREAKDOWN"
        reasons.append("below_ema5_10_and_slope_down")
    elif distance_recent_high_pct is not None and distance_recent_high_pct <= -0.30:
        state = "DEEP_PULLBACK"
        reasons.append("far_below_recent_high")
    else:
        state = "MIXED_KLINE"
        reasons.append("mixed_kline")

    return KlinePosition(
        state,
        ema5,
        ema10,
        ema20,
        ema5_slope_pct,
        distance_ema10_pct,
        distance_recent_high_pct,
        distance_recent_low_pct,
        reasons,
    )


def principal_recovery_sell_pct(current_pnl):
    current_pnl = _f(current_pnl, 0.0)
    if current_pnl <= 0:
        return 0.0
    return max(0.0, min(0.90, 1.0 / (1.0 + current_pnl)))


def rug_risk_score(*, dex_snapshot=None, kline_state=None, current_pnl=0.0, peak_pnl=0.0, quote_pnl=None):
    dex_snapshot = dex_snapshot or {}
    current_pnl = _f(current_pnl, 0.0)
    peak_pnl = _f(peak_pnl, 0.0)
    quote_pnl = _f(quote_pnl, None)
    reasons = []
    score = 0.0

    buys_m5 = _i(dex_snapshot.get("buys_m5"), 0)
    sells_m5 = _i(dex_snapshot.get("sells_m5"), 0)
    sell_ratio = sells_m5 / max(buys_m5, 1)
    if sell_ratio >= 2.5 and sells_m5 >= 10:
        score += 35
        reasons.append("sell_pressure_spike")
    elif sell_ratio >= 1.5 and sells_m5 >= 10:
        score += 20
        reasons.append("sell_pressure_elevated")

    liq = _f(dex_snapshot.get("liquidity_usd"), 0.0)
    liq_unknown = bool(dex_snapshot.get("liquidity_unknown"))
    if liq_unknown:
        score += 8
        reasons.append("liquidity_unknown")
    elif 0 < liq < 5_000:
        score += 22
        reasons.append("thin_liquidity_lt_5k")
    elif 0 < liq < 10_000:
        score += 12
        reasons.append("thin_liquidity_lt_10k")

    if quote_pnl is not None:
        quote_gap = quote_pnl - current_pnl
        if quote_gap <= -0.20:
            score += 35
            reasons.append("quote_far_worse_than_mark")
        elif quote_gap <= -0.08:
            score += 18
            reasons.append("quote_worse_than_mark")

    giveback = peak_pnl - current_pnl
    if peak_pnl >= 0.08 and current_pnl <= 0:
        score += 25
        reasons.append("positive_peak_back_to_loss")
    if giveback >= 0.30:
        score += 25
        reasons.append("large_giveback_30pp")
    elif giveback >= 0.15:
        score += 14
        reasons.append("large_giveback_15pp")

    if kline_state == "EMA_BREAKDOWN":
        score += 20
        reasons.append("ema_breakdown")
    elif kline_state == "DEEP_PULLBACK":
        score += 12
        reasons.append("deep_pullback")

    return round(_clamp(score), 2), reasons


def ev_score_for_phase(*, phase_state, current_pnl, peak_pnl, kline_state, rug_score, lifecycle_state=None, vitality_score=None):
    score = 50.0
    current_pnl = _f(current_pnl, 0.0)
    peak_pnl = _f(peak_pnl, 0.0)
    vitality = _f(vitality_score, None)
    if vitality is not None and not math.isnan(vitality):
        score += (vitality - 50.0) * 0.25
    if kline_state in {"ABOVE_RISING_EMA", "ABOVE_FLAT_EMA"}:
        score += 12
    elif kline_state in {"EMA_BREAKDOWN", "DEEP_PULLBACK"}:
        score -= 14
    if phase_state == "NO_FOLLOW":
        score -= 18
    elif phase_state == "PROTECT_PROFIT":
        score += 3
    elif phase_state == "RECOVER_PRINCIPAL":
        score += 14
    elif phase_state == "MOON_RUNNER":
        score += 10
    elif phase_state == "RUG_DEFENSE":
        score -= 35
    if current_pnl < 0:
        score -= 8
    if peak_pnl >= 0.25:
        score += 8
    if lifecycle_state in {"DEAD", "DISTRIBUTION", "DEAD_CAT_BOUNCE"}:
        score -= 20
    score -= _f(rug_score, 0.0) * 0.35
    return round(_clamp(score), 2)


def evaluate_phase_policy(
    *,
    route=None,
    current_pnl=0.0,
    peak_pnl=0.0,
    held_sec=0.0,
    sold_pct=0.0,
    dex_snapshot=None,
    kline_bars=None,
    current_price=None,
    quote_pnl=None,
    lifecycle_state=None,
    vitality_score=None,
):
    current_pnl = _f(current_pnl, 0.0)
    peak_pnl = max(_f(peak_pnl, 0.0), current_pnl)
    held_sec = _f(held_sec, 0.0)
    sold_pct = max(0.0, min(1.0, _f(sold_pct, 0.0)))

    kline = classify_kline_position(kline_bars, current_price)
    rug_score, rug_reasons = rug_risk_score(
        dex_snapshot=dex_snapshot,
        kline_state=kline.state,
        current_pnl=current_pnl,
        peak_pnl=peak_pnl,
        quote_pnl=quote_pnl,
    )

    reasons = list(rug_reasons)
    floor = None
    sell_pct = 0.0
    action = "HOLD"

    if rug_score >= 70:
        phase = "RUG_DEFENSE"
        action = "EXIT"
        reason = f"rug_risk_{rug_score:.0f}"
    elif peak_pnl < 0.05:
        phase = "NO_FOLLOW"
        reason = "peak_under_5pct"
        if held_sec >= 20 and peak_pnl < 0.03 and current_pnl <= -0.08:
            action = "EXIT"
            reason = "no_follow_fast_fail_20s"
        elif held_sec >= 60:
            action = "EXIT"
            reason = "no_follow_60s_shadow"
        else:
            action = "WAIT"
    elif peak_pnl < 0.25:
        phase = "PROTECT_PROFIT"
        floor = max(0.0, min(0.20, peak_pnl * 0.80))
        if peak_pnl >= 0.08:
            floor = max(floor, 0.02)
        reason = f"protect_80pct_of_peak_floor_{floor:.1%}"
        action = "EXIT" if current_pnl <= floor else "TIGHTEN"
    elif peak_pnl < 0.50:
        phase = "RECOVER_PRINCIPAL"
        target_sold_pct = principal_recovery_sell_pct(current_pnl)
        sell_pct = max(0.0, target_sold_pct - sold_pct)
        floor = max(0.03, peak_pnl * 0.50)
        if sell_pct >= 0.05 and current_pnl > 0:
            action = "PARTIAL_SELL"
            reason = f"recover_principal_sell_{sell_pct:.0%}"
        else:
            action = "EXIT" if current_pnl <= floor else "HOLD"
            reason = f"recover_principal_floor_{floor:.1%}"
    else:
        phase = "MOON_RUNNER"
        target_sold_pct = principal_recovery_sell_pct(current_pnl)
        sell_pct = max(0.0, target_sold_pct - sold_pct)
        floor = peak_pnl * 0.50
        if sell_pct >= 0.05 and current_pnl > 0:
            action = "PARTIAL_SELL"
            reason = f"moon_runner_recover_principal_sell_{sell_pct:.0%}"
        else:
            action = "EXIT" if current_pnl <= floor else "HOLD"
            reason = f"moon_runner_50pct_trail_floor_{floor:.1%}"

    if kline.state in {"EMA_BREAKDOWN", "DEEP_PULLBACK"} and action == "HOLD" and peak_pnl >= 0.05:
        action = "TIGHTEN"
        reasons.append("kline_breakdown_tighten")

    ev = ev_score_for_phase(
        phase_state=phase,
        current_pnl=current_pnl,
        peak_pnl=peak_pnl,
        kline_state=kline.state,
        rug_score=rug_score,
        lifecycle_state=lifecycle_state,
        vitality_score=vitality_score,
    )

    return PhasePolicyDecision(
        phase,
        action,
        reason,
        round(max(0.0, min(1.0, sell_pct)), 4),
        floor,
        ev,
        rug_score,
        kline.state,
        reasons,
        kline.to_payload(),
    )
