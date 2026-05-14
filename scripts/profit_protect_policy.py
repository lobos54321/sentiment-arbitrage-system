#!/usr/bin/env python3
"""Small-winner exit protection shared by paper exit engines."""

import os


PROFIT_PROTECT_SLIP_BUFFER = float(os.environ.get("PROFIT_PROTECT_SLIP_BUFFER", "0.015"))
ATH_MOON_FLOOR_LOW_PEAK = float(os.environ.get("ATH_MOON_FLOOR_LOW_PEAK", "0.50"))
ATH_MOON_FLOOR_BLEND_PEAK = float(os.environ.get("ATH_MOON_FLOOR_BLEND_PEAK", "0.70"))
ATH_MOON_FLOOR_FULL_MOON_PEAK = float(os.environ.get("ATH_MOON_FLOOR_FULL_MOON_PEAK", "1.00"))
ATH_MOON_FLOOR_TIGHT_MARGIN = float(os.environ.get("ATH_MOON_FLOOR_TIGHT_MARGIN", "0.25"))
ATH_MOON_FLOOR_WIDE_MARGIN = float(os.environ.get("ATH_MOON_FLOOR_WIDE_MARGIN", "0.40"))
ATH_MOON_FLOOR_MIN_FACTOR = float(os.environ.get("ATH_MOON_FLOOR_MIN_FACTOR", "0.55"))
PROBE_RUNNER_FLOOR_START_PEAK = float(os.environ.get("PROBE_RUNNER_FLOOR_START_PEAK", "0.10"))
PROBE_RUNNER_FLOOR_MID_PEAK = float(os.environ.get("PROBE_RUNNER_FLOOR_MID_PEAK", "0.20"))
PROBE_RUNNER_FLOOR_HIGH_PEAK = float(os.environ.get("PROBE_RUNNER_FLOOR_HIGH_PEAK", "0.45"))
PROBE_RUNNER_FLOOR_LOW_MIN = float(os.environ.get("PROBE_RUNNER_FLOOR_LOW_MIN", "0.04"))
PROBE_RUNNER_FLOOR_MID_MIN = float(os.environ.get("PROBE_RUNNER_FLOOR_MID_MIN", "0.07"))
PROBE_RUNNER_FLOOR_HIGH_MIN = float(os.environ.get("PROBE_RUNNER_FLOOR_HIGH_MIN", "0.12"))
PROBE_RUNNER_FLOOR_FACTOR = float(os.environ.get("PROBE_RUNNER_FLOOR_FACTOR", "0.35"))
PROBE_RUNNER_FULL_MOON_MARGIN = float(os.environ.get("PROBE_RUNNER_FULL_MOON_MARGIN", "0.25"))
PROBE_RUNNER_FULL_MOON_FACTOR = float(os.environ.get("PROBE_RUNNER_FULL_MOON_FACTOR", "0.65"))
COHORT_AWARE_TRAIL_FLOOR_ENABLED = os.environ.get("COHORT_AWARE_TRAIL_FLOOR_ENABLED", "true").lower() != "false"
COHORT_TRAIL_TINY_WEAK_FACTOR = float(os.environ.get("COHORT_TRAIL_TINY_WEAK_FACTOR", "0.80"))
COHORT_TRAIL_TINY_DEFAULT_FACTOR = float(os.environ.get("COHORT_TRAIL_TINY_DEFAULT_FACTOR", "0.75"))
COHORT_TRAIL_RESONANCE_RUNNER_FACTOR = float(os.environ.get("COHORT_TRAIL_RESONANCE_RUNNER_FACTOR", "0.60"))
COHORT_TRAIL_SOURCE_RUNNER_FACTOR = float(os.environ.get("COHORT_TRAIL_SOURCE_RUNNER_FACTOR", "0.55"))
COHORT_TRAIL_MIN_PEAK = float(os.environ.get("COHORT_TRAIL_MIN_PEAK", "0.10"))
COHORT_TRAIL_MIN_FLOOR = float(os.environ.get("COHORT_TRAIL_MIN_FLOOR", "0.04"))


def profit_protect_floor(peak_pnl, *, slip_buffer=None):
    """Return the mark-PnL floor for protecting 8-50% peak winners.

    The tier rules define the desired realized floor, then widen the mark
    trigger by the expected trigger-to-fill slippage buffer.
    """
    try:
        peak = float(peak_pnl or 0.0)
    except (TypeError, ValueError):
        return None
    try:
        slip = float(PROFIT_PROTECT_SLIP_BUFFER if slip_buffer is None else slip_buffer)
    except (TypeError, ValueError):
        slip = PROFIT_PROTECT_SLIP_BUFFER

    if peak >= 0.50 or peak < 0.08:
        return None
    if peak >= 0.20:
        desired_realized = max(peak * 0.50, peak - 0.10)
    elif peak >= 0.10:
        desired_realized = max(peak * 0.60, peak - 0.05, 0.06)
    else:
        desired_realized = max(peak * 0.65, 0.04)
    return desired_realized + max(0.0, slip)


def ath_moon_bag_floor(peak_pnl):
    """Return the ATH moon-bag floor for 50%+ peak winners.

    The previous fixed `peak - 40pp` rule was fine for 80-100%+ runners, but too
    loose for just-crossed 50% peaks. This keeps wide room for moonshots while
    tightening the 50-70% band where giving back 40pp often means losing most of
    the captured move.
    """
    try:
        peak = float(peak_pnl or 0.0)
    except (TypeError, ValueError):
        return None
    if peak < ATH_MOON_FLOOR_LOW_PEAK:
        floor = peak - ATH_MOON_FLOOR_WIDE_MARGIN
        return floor if floor > 0 else None

    tight_floor = max(peak - ATH_MOON_FLOOR_TIGHT_MARGIN, peak * ATH_MOON_FLOOR_MIN_FACTOR)
    wide_floor = peak - ATH_MOON_FLOOR_WIDE_MARGIN

    if peak < ATH_MOON_FLOOR_BLEND_PEAK:
        return tight_floor
    if peak >= ATH_MOON_FLOOR_FULL_MOON_PEAK:
        return wide_floor

    blend_width = max(ATH_MOON_FLOOR_FULL_MOON_PEAK - ATH_MOON_FLOOR_BLEND_PEAK, 1e-9)
    blend = min(1.0, max(0.0, (peak - ATH_MOON_FLOOR_BLEND_PEAK) / blend_width))
    return tight_floor * (1.0 - blend) + wide_floor * blend


def probe_runner_floor(peak_pnl):
    """Return a post-lock runner floor for 10-50% probe peaks."""
    try:
        peak = float(peak_pnl or 0.0)
    except (TypeError, ValueError):
        return None
    if peak >= ATH_MOON_FLOOR_LOW_PEAK:
        return max(
            peak - PROBE_RUNNER_FULL_MOON_MARGIN,
            peak * PROBE_RUNNER_FULL_MOON_FACTOR,
        )
    if peak < PROBE_RUNNER_FLOOR_START_PEAK:
        return None
    if peak >= PROBE_RUNNER_FLOOR_HIGH_PEAK:
        return max(PROBE_RUNNER_FLOOR_HIGH_MIN, peak * PROBE_RUNNER_FLOOR_FACTOR)
    if peak >= PROBE_RUNNER_FLOOR_MID_PEAK:
        return max(PROBE_RUNNER_FLOOR_MID_MIN, peak * PROBE_RUNNER_FLOOR_FACTOR)
    return max(PROBE_RUNNER_FLOOR_LOW_MIN, peak * PROBE_RUNNER_FLOOR_FACTOR)


def cohort_aware_probe_runner_floor(peak_pnl, *, entry_mode=None, resonance_cohort=None, capital_tier=None):
    """Return a paper-only cohort-aware runner floor.

    This is intentionally conservative for weak tiny probes and looser for
    Telegram+GMGN/source-resonance runners. It is used after the trade has a
    material peak; it does not create new entries.
    """
    if not COHORT_AWARE_TRAIL_FLOOR_ENABLED:
        return probe_runner_floor(peak_pnl)
    try:
        peak = float(peak_pnl or 0.0)
    except (TypeError, ValueError):
        return None
    if peak < COHORT_TRAIL_MIN_PEAK:
        return probe_runner_floor(peak)
    mode = str(entry_mode or "").lower()
    cohort = str(resonance_cohort or "").lower()
    tier = str(capital_tier or "").lower()
    if "source_resonance" in mode or "telegram_gmgn_quote_clean" in cohort:
        factor = COHORT_TRAIL_SOURCE_RUNNER_FACTOR
    elif "gmgn" in cohort or "hard_gate_pass" in mode:
        factor = COHORT_TRAIL_RESONANCE_RUNNER_FACTOR
    elif tier == "tiny_probe" or "tiny" in mode or "probe" in mode or "scout" in mode:
        factor = COHORT_TRAIL_TINY_DEFAULT_FACTOR
    else:
        factor = COHORT_TRAIL_TINY_WEAK_FACTOR
    return max(COHORT_TRAIL_MIN_FLOOR, peak * factor)
