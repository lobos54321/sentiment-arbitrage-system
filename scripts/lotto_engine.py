#!/usr/bin/env python3
"""
LOTTO fast-lane rules for paper trading.

The LOTTO lane is intentionally small and deterministic: quick pre-entry
defense, fixed paper sizing, ATH feedback, and a wide-tail exit policy.
"""

from dataclasses import dataclass
import os
import time


LOTTO_STRATEGY_ID = os.environ.get("LOTTO_STRATEGY_ID", "lotto-v1")
LOTTO_STRATEGY_STAGE = "lotto"
LOTTO_SIGNAL_TYPE = "LOTTO"
LOTTO_POSITION_SIZE_SOL = float(os.environ.get("LOTTO_POSITION_SIZE_SOL", "0.05"))
LOTTO_MAX_CONCURRENT = int(os.environ.get("LOTTO_MAX_CONCURRENT", "10"))
LOTTO_ENTRY_STALE_SEC = float(os.environ.get("LOTTO_ENTRY_STALE_SEC", str(30 * 60)))
LOTTO_MC_MAX_USD = float(os.environ.get("LOTTO_MC_MAX_USD", "30000"))
LOTTO_MIN_HOLDERS = int(os.environ.get("LOTTO_MIN_HOLDERS", "50"))
LOTTO_TOP10_MAX_PCT = float(os.environ.get("LOTTO_TOP10_MAX_PCT", "70"))
LOTTO_MIN_LIQUIDITY_USD = float(os.environ.get("LOTTO_MIN_LIQUIDITY_USD", "5000"))
LOTTO_MIN_VOL24H_USD = float(os.environ.get("LOTTO_MIN_VOL24H_USD", "10000"))
LOTTO_LIVE_TOP1_MAX_PCT = float(os.environ.get("LOTTO_LIVE_TOP1_MAX_PCT", "35"))
LOTTO_LIVE_TOP10_MAX_PCT = float(os.environ.get("LOTTO_LIVE_TOP10_MAX_PCT", "85"))

LOTTO_TIME_EXIT_60S_PEAK = float(os.environ.get("LOTTO_TIME_EXIT_60S_PEAK", "0.05"))
LOTTO_TIME_EXIT_120S_PEAK = float(os.environ.get("LOTTO_TIME_EXIT_120S_PEAK", "0.10"))
LOTTO_STOP_LOSS = float(os.environ.get("LOTTO_STOP_LOSS", "-0.18"))
LOTTO_HARD_FLOOR = float(os.environ.get("LOTTO_HARD_FLOOR", "-0.30"))
LOTTO_PARTIAL_LOCK_PEAK = float(os.environ.get("LOTTO_PARTIAL_LOCK_PEAK", "1.00"))
LOTTO_PARTIAL_SELL_PCT = float(os.environ.get("LOTTO_PARTIAL_SELL_PCT", "0.25"))


@dataclass(frozen=True)
class LottoDecision:
    action: str
    reason: str
    detail: dict

    @property
    def allow(self) -> bool:
        return self.action == "allow"

    @property
    def expire(self) -> bool:
        return self.action == "expire"


def active_lotto_count(positions, pending_entries=None):
    pending_entries = pending_entries or {}
    count = sum(1 for p in pending_entries.values() if p.get("is_lotto"))
    for pos in positions.values():
        state = getattr(pos, "monitor_state", None) or {}
        if getattr(pos, "signal_type", None) == LOTTO_SIGNAL_TYPE or state.get("signalRoute") == "LOTTO":
            count += 1
    return count


def is_lotto_position(pos, w_entry=None):
    state = getattr(pos, "monitor_state", None) or {}
    return (
        getattr(pos, "signal_type", None) == LOTTO_SIGNAL_TYPE
        or state.get("signalRoute") == "LOTTO"
        or (w_entry is not None and w_entry.get("type") == "LOTTO")
    )


def evaluate_lotto_entry(
    w_entry,
    *,
    dex_snapshot=None,
    live_concentration=None,
    current_lotto_count=0,
    data_health_ok=True,
    now=None,
):
    now = now or time.time()
    detail = {}

    if not data_health_ok:
        return LottoDecision("wait", "data_health_bad", detail)

    age_sec = max(0.0, now - float(w_entry.get("added_at") or now))
    detail["age_sec"] = age_sec
    if age_sec > LOTTO_ENTRY_STALE_SEC:
        return LottoDecision("expire", f"lotto_stale_{age_sec:.0f}s", detail)

    if current_lotto_count >= LOTTO_MAX_CONCURRENT:
        detail["current_lotto_count"] = current_lotto_count
        return LottoDecision("wait", f"lotto_slots_full_{current_lotto_count}_{LOTTO_MAX_CONCURRENT}", detail)

    market_cap = float(w_entry.get("signal_mc") or 0)
    detail["market_cap"] = market_cap
    if market_cap <= 0 or market_cap >= LOTTO_MC_MAX_USD:
        return LottoDecision("expire", f"lotto_mc_{market_cap:.0f}", detail)

    holders = int(w_entry.get("signal_holders") or 0)
    detail["holders"] = holders
    if holders < LOTTO_MIN_HOLDERS:
        return LottoDecision("expire", f"lotto_holders_{holders}", detail)

    vol24h = float(w_entry.get("signal_vol24h") or 0)
    detail["vol24h"] = vol24h
    if vol24h < LOTTO_MIN_VOL24H_USD:
        return LottoDecision("expire", f"lotto_vol24h_{vol24h:.0f}", detail)

    top10 = float(w_entry.get("signal_top10") or 0)
    detail["top10_pct"] = top10
    if top10 > LOTTO_TOP10_MAX_PCT:
        return LottoDecision("expire", f"lotto_top10_{top10:.0f}pct", detail)

    liquidity = float((dex_snapshot or {}).get("liquidity_usd") or 0)
    detail["liquidity_usd"] = liquidity
    if liquidity < LOTTO_MIN_LIQUIDITY_USD:
        return LottoDecision("expire", f"lotto_liq_low_{liquidity:.0f}", detail)

    if live_concentration:
        live_top1 = float(live_concentration.get("top1_pct") or 0)
        live_top10 = float(live_concentration.get("top10_pct") or 0)
        detail["live_top1_pct"] = live_top1
        detail["live_top10_pct"] = live_top10
        if live_top1 > LOTTO_LIVE_TOP1_MAX_PCT:
            return LottoDecision("expire", f"lotto_live_top1_{live_top1:.0f}pct", detail)
        if live_top10 > LOTTO_LIVE_TOP10_MAX_PCT:
            return LottoDecision("expire", f"lotto_live_top10_{live_top10:.0f}pct", detail)

    return LottoDecision("allow", "lotto_fast_lane_ok", detail)


def build_lotto_pending(w_entry, lifecycle_id, detail=None):
    detail = detail or {}
    return {
        "token_ca": w_entry["ca"],
        "symbol": w_entry["symbol"],
        "signal_ts": w_entry["signal_ts"],
        "premium_signal_id": w_entry["premium_signal_id"],
        "signal_type": LOTTO_SIGNAL_TYPE,
        "pool": w_entry["pool_address"],
        "staged_at": time.time(),
        "trigger_price": None,
        "watchlist_id": w_entry["id"],
        "kelly_position_sol": LOTTO_POSITION_SIZE_SOL,
        "matrix_scores": {},
        "is_lotto": True,
        "signal_route": "LOTTO",
        "strategy_id": LOTTO_STRATEGY_ID,
        "strategy_stage": LOTTO_STRATEGY_STAGE,
        "stage_outcome": "lotto_entered",
        "replay_source": "live_monitor_lotto",
        "entry_mode": "lotto_fast_lane",
        "exit_strategy": "LOTTO",
        "timing_passed": True,
        "w_entry": w_entry,
        "lotto_state": {
            "route": "LOTTO",
            "entryDecision": detail,
            "lifecycleId": lifecycle_id,
            "athBoostCount": 0,
        },
        "momentum_snapshots": [],
        "momentum_pct": 0,
        "first_fire_pc_m5": None,
        "spread_abort_count": 0,
        "smart_entry_retries": 0,
    }


def compute_ath_lockout_sec(w_entry, signal_market_cap):
    previous_mc = float(w_entry.get("last_ath_mc") or 0)
    if previous_mc > 0 and signal_market_cap >= previous_mc * 2:
        return 300
    if signal_market_cap > previous_mc:
        return 180
    return 90


def build_ath_boost_updates(w_entry, *, signal_ts=None, signal_market_cap=0, now=None):
    now = now or time.time()
    lockout_sec = compute_ath_lockout_sec(w_entry, float(signal_market_cap or 0))
    return {
        "ath_count": int(w_entry.get("ath_count") or 0) + 1,
        "last_ath_ts": signal_ts or now,
        "last_ath_mc": float(signal_market_cap or w_entry.get("last_ath_mc") or 0),
        "trail_lockout_until": now + lockout_sec,
    }


def evaluate_lotto_exit(pos, w_entry, current_price, *, now=None):
    now = now or time.time()
    entry_price = float(getattr(pos, "entry_price", 0) or 0)
    if not entry_price or entry_price <= 0 or not current_price or current_price <= 0:
        return {"action": "hold", "reason": "lotto_no_price", "current_pnl": 0.0}

    current_pnl = (float(current_price) - entry_price) / entry_price
    prior_peak = max(float(getattr(pos, "peak_pnl", 0) or 0), float((w_entry or {}).get("peak_pnl") or 0))
    peak_pnl = max(prior_peak, current_pnl)
    held_sec = max(0.0, now - float(getattr(pos, "entry_ts", now) or now))

    if current_pnl <= LOTTO_HARD_FLOOR:
        return {
            "action": "exit",
            "reason": f"lotto_hard_floor ({current_pnl:.1%} <= {LOTTO_HARD_FLOOR:.1%})",
            "current_pnl": current_pnl,
            "peak_pnl": peak_pnl,
            "trail_floor": None,
        }

    if current_pnl <= LOTTO_STOP_LOSS:
        return {
            "action": "exit",
            "reason": f"lotto_sl ({current_pnl:.1%} <= {LOTTO_STOP_LOSS:.1%})",
            "current_pnl": current_pnl,
            "peak_pnl": peak_pnl,
            "trail_floor": None,
        }

    if held_sec >= 120 and peak_pnl < LOTTO_TIME_EXIT_120S_PEAK:
        return {
            "action": "exit",
            "reason": f"lotto_no_follow_120s (peak={peak_pnl:.1%} < {LOTTO_TIME_EXIT_120S_PEAK:.1%})",
            "current_pnl": current_pnl,
            "peak_pnl": peak_pnl,
            "trail_floor": None,
        }

    if held_sec >= 60 and peak_pnl < LOTTO_TIME_EXIT_60S_PEAK:
        return {
            "action": "exit",
            "reason": f"lotto_no_follow_60s (peak={peak_pnl:.1%} < {LOTTO_TIME_EXIT_60S_PEAK:.1%})",
            "current_pnl": current_pnl,
            "peak_pnl": peak_pnl,
            "trail_floor": None,
        }

    if peak_pnl >= LOTTO_PARTIAL_LOCK_PEAK and not (w_entry or {}).get("has_locked_profit"):
        return {
            "action": "lock_profit",
            "reason": f"lotto_partial_lock (peak={peak_pnl:.1%} >= {LOTTO_PARTIAL_LOCK_PEAK:.1%})",
            "current_pnl": current_pnl,
            "peak_pnl": peak_pnl,
            "trail_floor": None,
            "sell_pct": LOTTO_PARTIAL_SELL_PCT,
        }

    lockout_until = float((w_entry or {}).get("trail_lockout_until") or 0)
    if now < lockout_until:
        return {
            "action": "hold",
            "reason": f"lotto_ath_lockout ({int(lockout_until - now)}s)",
            "current_pnl": current_pnl,
            "peak_pnl": peak_pnl,
            "trail_floor": None,
        }

    if peak_pnl >= 5.00:
        trail_factor = 0.72
        phase = "phase3_500pct"
    elif peak_pnl >= 2.00:
        trail_factor = 0.60
        phase = "phase2_200pct"
    elif peak_pnl >= 0.50:
        trail_factor = 0.45
        phase = "phase1_50pct"
    else:
        trail_factor = None
        phase = "verify"

    if trail_factor is not None:
        trail_floor = peak_pnl * trail_factor
        if current_pnl < trail_floor:
            return {
                "action": "exit",
                "reason": f"lotto_trail_{phase} (pnl={current_pnl:.1%} < floor={trail_floor:.1%}, peak={peak_pnl:.1%})",
                "current_pnl": current_pnl,
                "peak_pnl": peak_pnl,
                "trail_floor": trail_floor,
            }
        return {
            "action": "hold",
            "reason": f"lotto_hold_{phase} (floor={trail_floor:.1%}, peak={peak_pnl:.1%})",
            "current_pnl": current_pnl,
            "peak_pnl": peak_pnl,
            "trail_floor": trail_floor,
        }

    return {
        "action": "hold",
        "reason": f"lotto_verify (held={held_sec:.0f}s peak={peak_pnl:.1%})",
        "current_pnl": current_pnl,
        "peak_pnl": peak_pnl,
        "trail_floor": None,
    }
