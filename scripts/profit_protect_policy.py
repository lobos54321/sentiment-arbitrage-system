#!/usr/bin/env python3
"""Small-winner exit protection shared by paper exit engines."""

import os


PROFIT_PROTECT_SLIP_BUFFER = float(os.environ.get("PROFIT_PROTECT_SLIP_BUFFER", "0.015"))


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
