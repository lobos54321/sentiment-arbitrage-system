#!/usr/bin/env python3
"""Entry-mode level live/shadow controller for paper probes.

This is a feedback controller, not a per-token predictor. It uses recent
closed trades from the same entry_mode to decide whether future candidates from
that path should remain live or temporarily fall back to shadow observation.
"""

import os
import time


ENTRY_MODE_QUALITY_ENABLED = os.environ.get("ENTRY_MODE_QUALITY_ENABLED", "true").lower() != "false"
ENTRY_MODE_QUALITY_WINDOW = max(5, int(os.environ.get("ENTRY_MODE_QUALITY_WINDOW", "20")))
ENTRY_MODE_QUALITY_MIN_SAMPLES = max(1, int(os.environ.get("ENTRY_MODE_QUALITY_MIN_SAMPLES", "8")))
ENTRY_MODE_QUALITY_SHADOW_SEC = max(60, int(os.environ.get("ENTRY_MODE_QUALITY_SHADOW_SEC", str(2 * 3600))))
ENTRY_MODE_QUALITY_PEAK_LOW = float(os.environ.get("ENTRY_MODE_QUALITY_PEAK_LOW", "0.03"))
ENTRY_MODE_QUALITY_PEAK_GOOD = float(os.environ.get("ENTRY_MODE_QUALITY_PEAK_GOOD", "0.10"))
ENTRY_MODE_QUALITY_LOW_PEAK_RATE = float(os.environ.get("ENTRY_MODE_QUALITY_LOW_PEAK_RATE", "0.60"))
ENTRY_MODE_QUALITY_MIN_AVG_PEAK = float(os.environ.get("ENTRY_MODE_QUALITY_MIN_AVG_PEAK", "0.05"))
ENTRY_MODE_QUALITY_BAD_AVG_FINAL = float(os.environ.get("ENTRY_MODE_QUALITY_BAD_AVG_FINAL", "-0.08"))
ENTRY_MODE_QUALITY_MIN_GOOD_PEAK_RATE = float(os.environ.get("ENTRY_MODE_QUALITY_MIN_GOOD_PEAK_RATE", "0.15"))

# Runtime memory keeps a degraded path shadow-only without needing a schema
# migration. Historical trades are still queried after restart.
_SHADOW_UNTIL = {}


def _safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _empty_stats(entry_mode):
    return {
        "entry_mode": entry_mode,
        "sample_n": 0,
        "closed_n": 0,
        "peak_gt_10_rate": 0.0,
        "peak_lt_3_rate": 0.0,
        "avg_peak": None,
        "avg_final": None,
    }


def recent_entry_mode_stats(db, entry_mode, *, window=None):
    """Return recent closed-trade stats for one entry mode."""
    entry_mode = str(entry_mode or "").strip()
    if not entry_mode or db is None:
        return _empty_stats(entry_mode)
    limit = int(window or ENTRY_MODE_QUALITY_WINDOW)
    try:
        rows = db.execute(
            """
            SELECT peak_pnl, pnl_pct
            FROM paper_trades
            WHERE entry_mode = ?
              AND exit_ts IS NOT NULL
              AND replay_source LIKE 'live_monitor%'
            ORDER BY entry_ts DESC
            LIMIT ?
            """,
            (entry_mode, limit),
        ).fetchall()
    except Exception as exc:
        stats = _empty_stats(entry_mode)
        stats["error"] = str(exc)
        return stats

    sample_n = len(rows)
    if sample_n <= 0:
        return _empty_stats(entry_mode)

    peaks = [_safe_float(row["peak_pnl"] if hasattr(row, "keys") else row[0], 0.0) for row in rows]
    finals = [_safe_float(row["pnl_pct"] if hasattr(row, "keys") else row[1], 0.0) for row in rows]
    peak_gt = sum(1 for value in peaks if value > ENTRY_MODE_QUALITY_PEAK_GOOD)
    peak_low = sum(1 for value in peaks if value < ENTRY_MODE_QUALITY_PEAK_LOW)
    return {
        "entry_mode": entry_mode,
        "sample_n": sample_n,
        "closed_n": sample_n,
        "peak_gt_10_rate": peak_gt / sample_n,
        "peak_lt_3_rate": peak_low / sample_n,
        "avg_peak": sum(peaks) / sample_n,
        "avg_final": sum(finals) / sample_n,
        "window": limit,
        "thresholds": {
            "min_samples": ENTRY_MODE_QUALITY_MIN_SAMPLES,
            "peak_low": ENTRY_MODE_QUALITY_PEAK_LOW,
            "peak_good": ENTRY_MODE_QUALITY_PEAK_GOOD,
            "low_peak_rate": ENTRY_MODE_QUALITY_LOW_PEAK_RATE,
            "min_avg_peak": ENTRY_MODE_QUALITY_MIN_AVG_PEAK,
            "bad_avg_final": ENTRY_MODE_QUALITY_BAD_AVG_FINAL,
            "min_good_peak_rate": ENTRY_MODE_QUALITY_MIN_GOOD_PEAK_RATE,
            "shadow_sec": ENTRY_MODE_QUALITY_SHADOW_SEC,
        },
    }


def evaluate_entry_mode_quality(db, entry_mode, *, now_ts=None, force_live=False):
    """Decide whether an entry mode should stay live or fall back to shadow."""
    now_ts = float(now_ts or time.time())
    entry_mode = str(entry_mode or "").strip()
    stats = recent_entry_mode_stats(db, entry_mode)
    shadow_until = float(_SHADOW_UNTIL.get(entry_mode, 0.0) or 0.0)

    base = {
        "enabled": ENTRY_MODE_QUALITY_ENABLED,
        "entry_mode": entry_mode,
        "decision": "allow_live",
        "reason": "entry_mode_quality_pass",
        "shadow_until": shadow_until if shadow_until > now_ts else None,
        "stats": stats,
    }
    if not ENTRY_MODE_QUALITY_ENABLED or force_live or not entry_mode:
        return base

    if shadow_until > now_ts:
        base.update({
            "decision": "shadow",
            "reason": "entry_mode_shadow_cooldown",
            "remaining_sec": shadow_until - now_ts,
        })
        return base

    if stats.get("sample_n", 0) < ENTRY_MODE_QUALITY_MIN_SAMPLES:
        base["reason"] = "entry_mode_quality_insufficient_samples"
        return base

    peak_lt_rate = _safe_float(stats.get("peak_lt_3_rate"), 0.0)
    peak_gt_rate = _safe_float(stats.get("peak_gt_10_rate"), 0.0)
    avg_peak = _safe_float(stats.get("avg_peak"), 0.0)
    avg_final = _safe_float(stats.get("avg_final"), 0.0)

    degraded = (
        peak_lt_rate >= ENTRY_MODE_QUALITY_LOW_PEAK_RATE
        and avg_peak < ENTRY_MODE_QUALITY_MIN_AVG_PEAK
    ) or (
        avg_final < ENTRY_MODE_QUALITY_BAD_AVG_FINAL
        and peak_gt_rate < ENTRY_MODE_QUALITY_MIN_GOOD_PEAK_RATE
    )

    if degraded:
        shadow_until = now_ts + ENTRY_MODE_QUALITY_SHADOW_SEC
        _SHADOW_UNTIL[entry_mode] = shadow_until
        base.update({
            "decision": "shadow",
            "reason": "entry_mode_quality_degraded",
            "shadow_until": shadow_until,
            "remaining_sec": ENTRY_MODE_QUALITY_SHADOW_SEC,
        })
        return base

    return base
