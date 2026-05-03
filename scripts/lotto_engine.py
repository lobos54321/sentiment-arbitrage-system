#!/usr/bin/env python3
"""
LOTTO fast-lane rules for paper trading.

The LOTTO lane is intentionally small and deterministic: quick pre-entry
defense, fixed paper sizing, ATH feedback, and a wide-tail exit policy.
"""

from dataclasses import dataclass
import os
import time

from profit_protect_policy import profit_protect_floor


LOTTO_STRATEGY_ID = os.environ.get("LOTTO_STRATEGY_ID", "lotto-v1")
LOTTO_STRATEGY_STAGE = "lotto"
LOTTO_SIGNAL_TYPE = "LOTTO"
LOTTO_POSITION_SIZE_SOL = float(os.environ.get("LOTTO_POSITION_SIZE_SOL", "0.05"))
LOTTO_MIDCAP_POSITION_SIZE_SOL = float(os.environ.get("LOTTO_MIDCAP_POSITION_SIZE_SOL", "0.03"))
LOTTO_MAX_CONCURRENT = int(os.environ.get("LOTTO_MAX_CONCURRENT", "10"))
LOTTO_ENTRY_STALE_SEC = float(os.environ.get("LOTTO_ENTRY_STALE_SEC", str(30 * 60)))
LOTTO_MC_MAX_USD = float(os.environ.get("LOTTO_MC_MAX_USD", "30000"))
LOTTO_MIDCAP_MAX_USD = float(os.environ.get("LOTTO_MIDCAP_MAX_USD", "150000"))
LOTTO_MIDCAP_MIN_LIQUIDITY_USD = float(os.environ.get("LOTTO_MIDCAP_MIN_LIQUIDITY_USD", "15000"))
LOTTO_MIDCAP_MIN_VOL_M5_USD = float(os.environ.get("LOTTO_MIDCAP_MIN_VOL_M5_USD", "8000"))
LOTTO_MIDCAP_MIN_M5_TXNS = int(os.environ.get("LOTTO_MIDCAP_MIN_M5_TXNS", "100"))
LOTTO_MIN_HOLDERS = int(os.environ.get("LOTTO_MIN_HOLDERS", "30"))
LOTTO_NORMAL_HOLDERS = int(os.environ.get("LOTTO_NORMAL_HOLDERS", "50"))
LOTTO_TOP10_MAX_PCT = float(os.environ.get("LOTTO_TOP10_MAX_PCT", "70"))
LOTTO_MIN_LIQUIDITY_USD = float(os.environ.get("LOTTO_MIN_LIQUIDITY_USD", "5000"))
LOTTO_MIN_VOL24H_USD = float(os.environ.get("LOTTO_MIN_VOL24H_USD", "10000"))
LOTTO_MIN_VOL_M5_USD = float(os.environ.get("LOTTO_MIN_VOL_M5_USD", "1000"))
LOTTO_MIN_M5_TXNS = int(os.environ.get("LOTTO_MIN_M5_TXNS", "6"))
LOTTO_PUMPFUN_LIQ_UNKNOWN_MIN_VOL_M5_USD = float(os.environ.get("LOTTO_PUMPFUN_LIQ_UNKNOWN_MIN_VOL_M5_USD", "1000"))
LOTTO_PUMPFUN_LIQ_UNKNOWN_MIN_M5_TXNS = int(os.environ.get("LOTTO_PUMPFUN_LIQ_UNKNOWN_MIN_M5_TXNS", "20"))
LOTTO_LIVE_TOP1_MAX_PCT = float(os.environ.get("LOTTO_LIVE_TOP1_MAX_PCT", "35"))
LOTTO_LIVE_TOP10_MAX_PCT = float(os.environ.get("LOTTO_LIVE_TOP10_MAX_PCT", "70"))
LOTTO_LIVE_TOP10_RISKY_MAX_PCT = float(os.environ.get("LOTTO_LIVE_TOP10_RISKY_MAX_PCT", "50"))
LOTTO_CONCENTRATED_SCOUT_ENABLED = os.environ.get("LOTTO_CONCENTRATED_SCOUT_ENABLED", "true").lower() != "false"
LOTTO_CONCENTRATED_SCOUT_SIZE_SOL = float(os.environ.get("LOTTO_CONCENTRATED_SCOUT_SIZE_SOL", "0.015"))
LOTTO_CONCENTRATED_SCOUT_MC_MIN_USD = float(os.environ.get("LOTTO_CONCENTRATED_SCOUT_MC_MIN_USD", "10000"))
LOTTO_CONCENTRATED_SCOUT_MC_MAX_USD = float(os.environ.get("LOTTO_CONCENTRATED_SCOUT_MC_MAX_USD", "40000"))
LOTTO_CONCENTRATED_SCOUT_TOP1_MAX_PCT = float(os.environ.get("LOTTO_CONCENTRATED_SCOUT_TOP1_MAX_PCT", "38"))
LOTTO_CONCENTRATED_SCOUT_TOP10_MAX_PCT = float(os.environ.get("LOTTO_CONCENTRATED_SCOUT_TOP10_MAX_PCT", "60"))
LOTTO_CONCENTRATED_SCOUT_MIN_M5_PCT = float(os.environ.get("LOTTO_CONCENTRATED_SCOUT_MIN_M5_PCT", "150"))
LOTTO_CONCENTRATED_SCOUT_MIN_VOL_M5_USD = float(os.environ.get("LOTTO_CONCENTRATED_SCOUT_MIN_VOL_M5_USD", "15000"))
LOTTO_CONCENTRATED_SCOUT_MIN_M5_TXNS = int(os.environ.get("LOTTO_CONCENTRATED_SCOUT_MIN_M5_TXNS", "300"))
LOTTO_EXPLOSIVE_DIRECT_SCOUT_ENABLED = os.environ.get("LOTTO_EXPLOSIVE_DIRECT_SCOUT_ENABLED", "true").lower() != "false"
LOTTO_EXPLOSIVE_DIRECT_SCOUT_SIZE_SOL = float(os.environ.get("LOTTO_EXPLOSIVE_DIRECT_SCOUT_SIZE_SOL", "0.008"))
LOTTO_EXPLOSIVE_DIRECT_SCOUT_TOP1_MAX_PCT = float(os.environ.get("LOTTO_EXPLOSIVE_DIRECT_SCOUT_TOP1_MAX_PCT", "35"))
LOTTO_EXPLOSIVE_DIRECT_SCOUT_TOP10_MAX_PCT = float(os.environ.get("LOTTO_EXPLOSIVE_DIRECT_SCOUT_TOP10_MAX_PCT", "60"))
LOTTO_EXPLOSIVE_DIRECT_SCOUT_MIN_M5_PCT = float(os.environ.get("LOTTO_EXPLOSIVE_DIRECT_SCOUT_MIN_M5_PCT", "300"))
LOTTO_EXPLOSIVE_DIRECT_SCOUT_MIN_VOL_M5_USD = float(os.environ.get("LOTTO_EXPLOSIVE_DIRECT_SCOUT_MIN_VOL_M5_USD", "20000"))
LOTTO_EXPLOSIVE_DIRECT_SCOUT_MIN_M5_TXNS = int(os.environ.get("LOTTO_EXPLOSIVE_DIRECT_SCOUT_MIN_M5_TXNS", "400"))
LOTTO_GMGN_MIN_POSITION_SIZE_SOL = float(os.environ.get("LOTTO_GMGN_MIN_POSITION_SIZE_SOL", "0.003"))

LOTTO_TIME_EXIT_60S_PEAK = float(os.environ.get("LOTTO_TIME_EXIT_60S_PEAK", "0.05"))
LOTTO_TIME_EXIT_120S_PEAK = float(os.environ.get("LOTTO_TIME_EXIT_120S_PEAK", "0.10"))
LOTTO_STOP_LOSS = float(os.environ.get("LOTTO_STOP_LOSS", "-0.18"))
LOTTO_HARD_FLOOR = float(os.environ.get("LOTTO_HARD_FLOOR", "-0.30"))
LOTTO_BREAKEVEN_PEAK = float(os.environ.get("LOTTO_BREAKEVEN_PEAK", "0.10"))
LOTTO_BREAKEVEN_EXIT_PNL = float(os.environ.get("LOTTO_BREAKEVEN_EXIT_PNL", "0.02"))
LOTTO_PARTIAL_LOCK_PEAK = float(os.environ.get("LOTTO_PARTIAL_LOCK_PEAK", "0.20"))
LOTTO_PARTIAL_LOCK_MIN_PNL = float(os.environ.get("LOTTO_PARTIAL_LOCK_MIN_PNL", "0.15"))
LOTTO_PARTIAL_SELL_PCT = float(os.environ.get("LOTTO_PARTIAL_SELL_PCT", "0.25"))
LOTTO_EPSILON = 1e-9


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
    if market_cap <= 0:
        return LottoDecision("expire", f"lotto_mc_{market_cap:.0f}", detail)
    if market_cap < LOTTO_MC_MAX_USD:
        detail["mc_tier"] = "newborn_micro"
    elif market_cap < LOTTO_MIDCAP_MAX_USD:
        detail["mc_tier"] = "newborn_midcap"
    else:
        detail["mc_tier"] = "observe_or_matrix_only"
        return LottoDecision("expire", f"lotto_mc_{market_cap:.0f}", detail)

    holders = int(w_entry.get("signal_holders") or 0)
    detail["holders"] = holders
    if holders <= 0:
        detail["holders_tier"] = "missing"
    elif holders < LOTTO_MIN_HOLDERS:
        return LottoDecision("expire", f"lotto_holders_{holders}", detail)
    elif holders < LOTTO_NORMAL_HOLDERS:
        detail["holders_tier"] = "low_confidence"
    else:
        detail["holders_tier"] = "normal"

    vol24h = float(w_entry.get("signal_vol24h") or 0)
    detail["vol24h"] = vol24h
    dex_snapshot = dex_snapshot or {}
    vol_m5 = float(dex_snapshot.get("vol_m5") or 0)
    buys_m5 = int(dex_snapshot.get("buys_m5") or 0)
    sells_m5 = int(dex_snapshot.get("sells_m5") or 0)
    price_change_m5 = float(dex_snapshot.get("price_change_m5") or 0)
    detail["vol_m5"] = vol_m5
    detail["tx_m5"] = buys_m5 + sells_m5
    detail["price_change_m5"] = price_change_m5
    if (
        vol24h < LOTTO_MIN_VOL24H_USD
        and vol_m5 < LOTTO_MIN_VOL_M5_USD
        and (buys_m5 + sells_m5) < LOTTO_MIN_M5_TXNS
    ):
        return LottoDecision(
            "wait",
            f"lotto_volume_unconfirmed_v24_{vol24h:.0f}_m5_{vol_m5:.0f}_tx_{buys_m5 + sells_m5}",
            detail,
        )

    top10 = float(w_entry.get("signal_top10") or 0)
    detail["top10_pct"] = top10
    if top10 > LOTTO_TOP10_MAX_PCT:
        return LottoDecision("expire", f"lotto_top10_{top10:.0f}pct", detail)

    liquidity = float(dex_snapshot.get("liquidity_usd") or 0)
    detail["liquidity_usd"] = liquidity
    detail["liquidity_unknown"] = bool(dex_snapshot.get("liquidity_unknown"))
    detail["dex_id"] = dex_snapshot.get("dex_id") or ""
    detail["pair_address"] = dex_snapshot.get("pair_address") or ""
    if liquidity < LOTTO_MIN_LIQUIDITY_USD:
        if detail["liquidity_unknown"] and detail["dex_id"] == "pumpfun":
            has_pumpfun_activity = (
                vol_m5 >= LOTTO_PUMPFUN_LIQ_UNKNOWN_MIN_VOL_M5_USD
                and (buys_m5 + sells_m5) >= LOTTO_PUMPFUN_LIQ_UNKNOWN_MIN_M5_TXNS
            )
            if has_pumpfun_activity:
                detail["liquidity_tier"] = "pumpfun_unknown_activity_confirmed"
            else:
                return LottoDecision(
                    "wait",
                    f"lotto_liq_unknown_pumpfun_wait_m5_{vol_m5:.0f}_tx_{buys_m5 + sells_m5}",
                    detail,
                )
        else:
            return LottoDecision("expire", f"lotto_liq_low_{liquidity:.0f}", detail)

    if detail.get("mc_tier") == "newborn_midcap":
        midcap_ok = (
            liquidity >= LOTTO_MIDCAP_MIN_LIQUIDITY_USD
            and vol_m5 >= LOTTO_MIDCAP_MIN_VOL_M5_USD
            and (buys_m5 + sells_m5) >= LOTTO_MIDCAP_MIN_M5_TXNS
        )
        detail["midcap_min_liquidity_usd"] = LOTTO_MIDCAP_MIN_LIQUIDITY_USD
        detail["midcap_min_vol_m5_usd"] = LOTTO_MIDCAP_MIN_VOL_M5_USD
        detail["midcap_min_m5_txns"] = LOTTO_MIDCAP_MIN_M5_TXNS
        if not midcap_ok:
            return LottoDecision("wait", "lotto_midcap_activity_unconfirmed", detail)

    if live_concentration:
        live_top1 = float(live_concentration.get("top1_pct") or 0)
        live_top10 = float(live_concentration.get("top10_pct") or 0)
        detail["live_top1_pct"] = live_top1
        detail["live_top10_pct"] = live_top10
        live_top10_max = LOTTO_LIVE_TOP10_MAX_PCT
        if detail["liquidity_unknown"] or detail["dex_id"] == "pumpfun":
            live_top10_max = min(live_top10_max, LOTTO_LIVE_TOP10_RISKY_MAX_PCT)
        detail["live_top10_max_pct"] = live_top10_max
        concentrated_scout_ok = (
            LOTTO_CONCENTRATED_SCOUT_ENABLED
            and detail["mc_tier"] == "newborn_micro"
            and (detail["liquidity_unknown"] or detail["dex_id"] == "pumpfun")
            and LOTTO_CONCENTRATED_SCOUT_MC_MIN_USD <= market_cap <= LOTTO_CONCENTRATED_SCOUT_MC_MAX_USD
            and live_top1 <= LOTTO_CONCENTRATED_SCOUT_TOP1_MAX_PCT
            and live_top10 <= LOTTO_CONCENTRATED_SCOUT_TOP10_MAX_PCT
            and price_change_m5 >= LOTTO_CONCENTRATED_SCOUT_MIN_M5_PCT
            and vol_m5 >= LOTTO_CONCENTRATED_SCOUT_MIN_VOL_M5_USD
            and (buys_m5 + sells_m5) >= LOTTO_CONCENTRATED_SCOUT_MIN_M5_TXNS
        )
        explosive_direct_scout_ok = (
            LOTTO_EXPLOSIVE_DIRECT_SCOUT_ENABLED
            and detail["mc_tier"] == "newborn_micro"
            and (detail["liquidity_unknown"] or detail["dex_id"] == "pumpfun")
            and LOTTO_CONCENTRATED_SCOUT_MC_MIN_USD <= market_cap <= LOTTO_CONCENTRATED_SCOUT_MC_MAX_USD
            and live_top1 <= LOTTO_EXPLOSIVE_DIRECT_SCOUT_TOP1_MAX_PCT
            and live_top10 <= LOTTO_EXPLOSIVE_DIRECT_SCOUT_TOP10_MAX_PCT
            and price_change_m5 >= LOTTO_EXPLOSIVE_DIRECT_SCOUT_MIN_M5_PCT
            and vol_m5 >= LOTTO_EXPLOSIVE_DIRECT_SCOUT_MIN_VOL_M5_USD
            and (buys_m5 + sells_m5) >= LOTTO_EXPLOSIVE_DIRECT_SCOUT_MIN_M5_TXNS
        )
        detail["concentrated_scout_enabled"] = LOTTO_CONCENTRATED_SCOUT_ENABLED
        detail["concentrated_scout_ok"] = concentrated_scout_ok
        detail["concentrated_scout_top1_max_pct"] = LOTTO_CONCENTRATED_SCOUT_TOP1_MAX_PCT
        detail["concentrated_scout_top10_max_pct"] = LOTTO_CONCENTRATED_SCOUT_TOP10_MAX_PCT
        detail["explosive_direct_scout_enabled"] = LOTTO_EXPLOSIVE_DIRECT_SCOUT_ENABLED
        detail["explosive_direct_scout_ok"] = explosive_direct_scout_ok
        detail["explosive_direct_scout_top1_max_pct"] = LOTTO_EXPLOSIVE_DIRECT_SCOUT_TOP1_MAX_PCT
        detail["explosive_direct_scout_top10_max_pct"] = LOTTO_EXPLOSIVE_DIRECT_SCOUT_TOP10_MAX_PCT
        if live_top1 > LOTTO_LIVE_TOP1_MAX_PCT and not concentrated_scout_ok:
            return LottoDecision("expire", f"lotto_live_top1_{live_top1:.0f}pct", detail)
        if live_top10 > live_top10_max and not concentrated_scout_ok:
            return LottoDecision("expire", f"lotto_live_top10_{live_top10:.0f}pct", detail)
        if explosive_direct_scout_ok and (live_top1 > LOTTO_LIVE_TOP1_MAX_PCT or live_top10 > live_top10_max):
            detail["entry_mode"] = "explosive_newborn_direct_scout"
            detail["position_size_sol"] = LOTTO_EXPLOSIVE_DIRECT_SCOUT_SIZE_SOL
            return LottoDecision("allow", "lotto_explosive_direct_scout_ok", detail)
        if concentrated_scout_ok and (live_top1 > LOTTO_LIVE_TOP1_MAX_PCT or live_top10 > live_top10_max):
            detail["entry_mode"] = "lotto_concentrated_scout"
            detail["position_size_sol"] = LOTTO_CONCENTRATED_SCOUT_SIZE_SOL
            return LottoDecision("allow", "lotto_concentrated_scout_ok", detail)

    return LottoDecision("allow", "lotto_fast_lane_ok", detail)


def build_lotto_pending(w_entry, lifecycle_id, detail=None):
    detail = detail or {}
    timing_passed = bool(detail.get("timing_passed", False))
    position_size_sol = LOTTO_POSITION_SIZE_SOL
    if detail.get("mc_tier") == "newborn_midcap":
        position_size_sol = LOTTO_MIDCAP_POSITION_SIZE_SOL
    if detail.get("entry_mode") == "lotto_concentrated_scout" or detail.get("concentrated_scout_ok"):
        position_size_sol = LOTTO_CONCENTRATED_SCOUT_SIZE_SOL
    if detail.get("entry_mode") == "explosive_newborn_direct_scout" or detail.get("explosive_direct_scout_ok"):
        position_size_sol = LOTTO_EXPLOSIVE_DIRECT_SCOUT_SIZE_SOL
    if detail.get("position_size_sol") is not None:
        try:
            position_size_sol = float(detail.get("position_size_sol"))
        except (TypeError, ValueError):
            pass
    original_position_size_sol = position_size_sol
    gmgn_policy = detail.get("gmgn_policy") or {}
    try:
        gmgn_size_multiplier = float(
            detail.get("gmgn_size_multiplier")
            or gmgn_policy.get("size_multiplier")
            or 1.0
        )
    except (TypeError, ValueError):
        gmgn_size_multiplier = 1.0
    if gmgn_size_multiplier < 1.0:
        position_size_sol = max(
            LOTTO_GMGN_MIN_POSITION_SIZE_SOL,
            position_size_sol * max(0.0, gmgn_size_multiplier),
        )
    position_size_sol = min(position_size_sol, original_position_size_sol)
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
        "kelly_position_sol": position_size_sol,
        "matrix_scores": {},
        "is_lotto": True,
        "signal_route": "LOTTO",
        "strategy_id": LOTTO_STRATEGY_ID,
        "strategy_stage": LOTTO_STRATEGY_STAGE,
        "stage_outcome": "lotto_entered",
        "replay_source": "live_monitor_lotto",
        "entry_mode": detail.get("entry_mode") or ("lotto_fast_arm" if not timing_passed else "lotto_fast_lane"),
        "exit_strategy": "LOTTO",
        "timing_passed": timing_passed,
        "w_entry": w_entry,
        "lotto_state": {
            "route": "LOTTO",
            "entryDecision": detail,
            "positionSizeSol": position_size_sol,
            "basePositionSizeSol": original_position_size_sol,
            "gmgnSizeMultiplier": gmgn_size_multiplier,
            "lifecycleId": lifecycle_id,
            "athBoostCount": 0,
        },
        "momentum_snapshots": [],
        "momentum_pct": 0,
        "first_fire_pc_m5": detail.get("price_change_m5"),
        "spread_abort_count": 0,
        "smart_entry_retries": int(w_entry.get("_smart_entry_retries", 0) or 0),
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

    protect_floor = profit_protect_floor(peak_pnl)
    if protect_floor is not None and current_pnl < protect_floor:
        return {
            "action": "exit",
            "reason": f"lotto_profit_protect (pnl={current_pnl:.1%} < floor={protect_floor:.1%}, peak={peak_pnl:.1%})",
            "current_pnl": current_pnl,
            "peak_pnl": peak_pnl,
            "trail_floor": protect_floor,
        }

    if peak_pnl >= LOTTO_BREAKEVEN_PEAK and current_pnl <= LOTTO_BREAKEVEN_EXIT_PNL + LOTTO_EPSILON:
        return {
            "action": "exit",
            "reason": f"lotto_breakeven_floor (pnl={current_pnl:.1%} <= floor={LOTTO_BREAKEVEN_EXIT_PNL:.1%}, peak={peak_pnl:.1%})",
            "current_pnl": current_pnl,
            "peak_pnl": peak_pnl,
            "trail_floor": LOTTO_BREAKEVEN_EXIT_PNL,
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

    if (
        peak_pnl >= LOTTO_PARTIAL_LOCK_PEAK
        and current_pnl >= LOTTO_PARTIAL_LOCK_MIN_PNL
        and not (w_entry or {}).get("has_locked_profit")
    ):
        return {
            "action": "lock_profit",
            "reason": f"lotto_partial_lock (pnl={current_pnl:.1%}, peak={peak_pnl:.1%}, sell={LOTTO_PARTIAL_SELL_PCT:.0%})",
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
