#!/usr/bin/env python3
"""
GMGN paper-only decision policy.

This layer turns read-only GMGN enrichment into paper-trader decisions. It is
kept separate from gmgn_readonly so the data adapter remains non-opinionated.
"""

import os

from scout_quality import evaluate_scout_quality


GMGN_PAPER_POLICY_ENABLED = os.environ.get("GMGN_PAPER_POLICY_ENABLED", "true").lower() != "false"
GMGN_PAPER_REJECT_ENABLED = os.environ.get("GMGN_PAPER_REJECT_ENABLED", "true").lower() != "false"
GMGN_PAPER_BOOST_ENABLED = os.environ.get("GMGN_PAPER_BOOST_ENABLED", "true").lower() != "false"
GMGN_PAPER_DOWNSIZE_ENABLED = os.environ.get("GMGN_PAPER_DOWNSIZE_ENABLED", "true").lower() != "false"

GMGN_RAT_REJECT_RATE = float(os.environ.get("GMGN_RAT_REJECT_RATE", "0.30"))
GMGN_ENTRAPMENT_REJECT_RATE = float(os.environ.get("GMGN_ENTRAPMENT_REJECT_RATE", "0.30"))
GMGN_CREATOR_HOLD_REJECT_RATE = float(os.environ.get("GMGN_CREATOR_HOLD_REJECT_RATE", "0.05"))
GMGN_DEV_TEAM_HOLD_REJECT_RATE = float(os.environ.get("GMGN_DEV_TEAM_HOLD_REJECT_RATE", "0.05"))
GMGN_TOP10_REJECT_RATE = float(os.environ.get("GMGN_TOP10_REJECT_RATE", "0.50"))
GMGN_BUNDLER_REJECT_RATE = float(os.environ.get("GMGN_BUNDLER_REJECT_RATE", "0.60"))
GMGN_TOP10_MC_AWARE_ENABLED = os.environ.get("GMGN_TOP10_MC_AWARE_ENABLED", "true").lower() != "false"
GMGN_TOP10_LOW_MC_USD = float(os.environ.get("GMGN_TOP10_LOW_MC_USD", "100000"))
GMGN_TOP10_MID_MC_USD = float(os.environ.get("GMGN_TOP10_MID_MC_USD", "300000"))
GMGN_TOP10_LOW_MC_REJECT_RATE = float(os.environ.get("GMGN_TOP10_LOW_MC_REJECT_RATE", "0.70"))
GMGN_TOP10_MID_MC_REJECT_RATE = float(os.environ.get("GMGN_TOP10_MID_MC_REJECT_RATE", "0.60"))

GMGN_BUNDLER_DOWNSIZE_RATE = float(os.environ.get("GMGN_BUNDLER_DOWNSIZE_RATE", "0.35"))
GMGN_BOT_DOWNSIZE_RATE = float(os.environ.get("GMGN_BOT_DOWNSIZE_RATE", "0.50"))
GMGN_SNIPER_DOWNSIZE_COUNT = int(os.environ.get("GMGN_SNIPER_DOWNSIZE_COUNT", "60"))
GMGN_DOWNSIZE_MULTIPLIER = float(os.environ.get("GMGN_DOWNSIZE_MULTIPLIER", "0.50"))
GMGN_MIN_SIZE_MULTIPLIER = float(os.environ.get("GMGN_MIN_SIZE_MULTIPLIER", "0.20"))

GMGN_SMART_BOOST_COUNT = int(os.environ.get("GMGN_SMART_BOOST_COUNT", "3"))
GMGN_RENOWNED_BOOST_COUNT = int(os.environ.get("GMGN_RENOWNED_BOOST_COUNT", "2"))
GMGN_CLEAN_TOP10_RATE = float(os.environ.get("GMGN_CLEAN_TOP10_RATE", "0.25"))
GMGN_CLEAN_BUNDLER_RATE = float(os.environ.get("GMGN_CLEAN_BUNDLER_RATE", "0.30"))
GMGN_CLEAN_RAT_RATE = float(os.environ.get("GMGN_CLEAN_RAT_RATE", "0.05"))

GMGN_TOXIC_SPREAD_PENALTY_PCT = float(os.environ.get("GMGN_TOXIC_SPREAD_PENALTY_PCT", "0.50"))
GMGN_DOWNSIZE_SPREAD_PENALTY_PCT = float(os.environ.get("GMGN_DOWNSIZE_SPREAD_PENALTY_PCT", "0.25"))
GMGN_TINY_SCOUT_ENABLED = os.environ.get("GMGN_TINY_SCOUT_ENABLED", "true").lower() != "false"
GMGN_TINY_SCOUT_SIZE_SOL = float(os.environ.get("GMGN_TINY_SCOUT_SIZE_SOL", "0.003"))
GMGN_BUNDLER_ONLY_TINY_RESCUE_ENABLED = os.environ.get("GMGN_BUNDLER_ONLY_TINY_RESCUE_ENABLED", "true").lower() != "false"
GMGN_BUNDLER_ONLY_TINY_RESCUE_MIN_EDGE_SCORE = int(os.environ.get("GMGN_BUNDLER_ONLY_TINY_RESCUE_MIN_EDGE_SCORE", "4"))
GMGN_BUNDLER_ONLY_TINY_RESCUE_MAX_BUNDLER_RATE = float(os.environ.get("GMGN_BUNDLER_ONLY_TINY_RESCUE_MAX_BUNDLER_RATE", "0.90"))
GMGN_CONCENTRATION_TINY_SCOUT_SIZE_SOL = float(os.environ.get("GMGN_CONCENTRATION_TINY_SCOUT_SIZE_SOL", "0.003"))
GMGN_TINY_SCOUT_MIN_EDGE_SCORE = int(os.environ.get("GMGN_TINY_SCOUT_MIN_EDGE_SCORE", "4"))
GMGN_TINY_SCOUT_MAX_TOXIC_SCORE = int(os.environ.get("GMGN_TINY_SCOUT_MAX_TOXIC_SCORE", "1"))
GMGN_TINY_SCOUT_TOP1_MAX_PCT = float(os.environ.get("GMGN_TINY_SCOUT_TOP1_MAX_PCT", "50"))
GMGN_TINY_SCOUT_TOP10_MAX_PCT = float(os.environ.get("GMGN_TINY_SCOUT_TOP10_MAX_PCT", "70"))
GMGN_CONCENTRATION_TINY_SCOUT_MIN_LIQUIDITY_USD = float(os.environ.get("GMGN_CONCENTRATION_TINY_SCOUT_MIN_LIQUIDITY_USD", "3000"))
GMGN_CONCENTRATION_TINY_SCOUT_MIN_VOL_M5 = float(os.environ.get("GMGN_CONCENTRATION_TINY_SCOUT_MIN_VOL_M5", "12000"))
GMGN_CONCENTRATION_TINY_SCOUT_MIN_TX_M5 = int(os.environ.get("GMGN_CONCENTRATION_TINY_SCOUT_MIN_TX_M5", "120"))
GMGN_CONCENTRATION_TINY_SCOUT_MAX_NEG_M5 = float(os.environ.get("GMGN_CONCENTRATION_TINY_SCOUT_MAX_NEG_M5", "-20"))
GMGN_UNKNOWN_DATA_TINY_SCOUT_MIN_VOL_M5 = float(os.environ.get("GMGN_UNKNOWN_DATA_TINY_SCOUT_MIN_VOL_M5", "20000"))
GMGN_UNKNOWN_DATA_TINY_SCOUT_MIN_TX_M5 = int(os.environ.get("GMGN_UNKNOWN_DATA_TINY_SCOUT_MIN_TX_M5", "250"))
GMGN_UNKNOWN_DATA_TINY_SCOUT_MAX_NEG_M5 = float(os.environ.get("GMGN_UNKNOWN_DATA_TINY_SCOUT_MAX_NEG_M5", "-45"))
GMGN_MIDCAP_NEAR_MISS_MIN_LIQUIDITY_USD = float(os.environ.get("GMGN_MIDCAP_NEAR_MISS_MIN_LIQUIDITY_USD", "5000"))
GMGN_MIDCAP_NEAR_MISS_MIN_VOL_M5 = float(os.environ.get("GMGN_MIDCAP_NEAR_MISS_MIN_VOL_M5", "15000"))
GMGN_MIDCAP_NEAR_MISS_MIN_TX_M5 = int(os.environ.get("GMGN_MIDCAP_NEAR_MISS_MIN_TX_M5", "200"))
GMGN_MIDCAP_NEAR_MISS_MAX_NEG_M5 = float(os.environ.get("GMGN_MIDCAP_NEAR_MISS_MAX_NEG_M5", "-15"))
GMGN_RECLAIM_TINY_SCOUT_MIN_VOL_M5 = float(os.environ.get("GMGN_RECLAIM_TINY_SCOUT_MIN_VOL_M5", "12000"))
GMGN_RECLAIM_TINY_SCOUT_MIN_TX_M5 = int(os.environ.get("GMGN_RECLAIM_TINY_SCOUT_MIN_TX_M5", "120"))
GMGN_RECLAIM_TINY_SCOUT_MIN_M5 = float(os.environ.get("GMGN_RECLAIM_TINY_SCOUT_MIN_M5", "-8"))


def _f(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _i(value, default=0):
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _truthy(value):
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "ok", "pass", "clean"}
    return bool(value)


def _first_positive(*values):
    for value in values:
        number = _f(value, 0.0)
        if number > 0:
            return number
    return 0.0


def gmgn_top10_threshold_for_market_cap(market_cap):
    market_cap = _f(market_cap, 0.0)
    if not GMGN_TOP10_MC_AWARE_ENABLED or market_cap <= 0:
        return GMGN_TOP10_REJECT_RATE, "fixed_or_unknown_mc"
    if market_cap < GMGN_TOP10_LOW_MC_USD:
        return max(GMGN_TOP10_REJECT_RATE, GMGN_TOP10_LOW_MC_REJECT_RATE), "low_mc"
    if market_cap < GMGN_TOP10_MID_MC_USD:
        return max(GMGN_TOP10_REJECT_RATE, GMGN_TOP10_MID_MC_REJECT_RATE), "mid_mc"
    return GMGN_TOP10_REJECT_RATE, "standard_mc"


def _execution_context(lotto_detail):
    lotto_detail = lotto_detail or {}
    eligibility = lotto_detail.get("entry_execution_eligibility")
    if not isinstance(eligibility, dict):
        eligibility = {}
    quote_clean_ok = _truthy(
        eligibility.get("quote_clean_ok")
        or eligibility.get("quote_clean_seen")
        or lotto_detail.get("quote_clean_ok")
        or lotto_detail.get("quote_clean_seen")
        or lotto_detail.get("source_quote_clean_seen")
        or lotto_detail.get("final_reclaim_quote_executable")
    )
    quote_executable_ok = _truthy(
        eligibility.get("quote_executable_ok")
        or eligibility.get("quote_executable")
        or lotto_detail.get("quote_executable")
        or lotto_detail.get("final_reclaim_quote_executable")
    )
    timing_ok = _truthy(
        eligibility.get("timing_ok")
        or lotto_detail.get("timing_ok")
        or lotto_detail.get("timing_passed")
        or lotto_detail.get("reclaim_passed")
        or lotto_detail.get("smart_entry_passed")
    )
    liquidity_ok = eligibility.get("liquidity_ok")
    if liquidity_ok is None:
        liquidity_ok = _first_positive(lotto_detail.get("liquidity_usd"), lotto_detail.get("last_liquidity")) > 0
    risk_ok = eligibility.get("risk_ok")
    if risk_ok is None:
        risk_ok = not _truthy(lotto_detail.get("toxic")) and not _truthy(lotto_detail.get("risk_blocked"))
    return {
        "quote_clean_ok": bool(quote_clean_ok),
        "quote_executable_ok": bool(quote_executable_ok),
        "timing_ok": bool(timing_ok),
        "liquidity_ok": bool(liquidity_ok),
        "risk_ok": bool(risk_ok),
        "direct_entry_ok": bool(eligibility.get("direct_entry_ok")),
    }


def _noop(reason):
    return {
        "enabled": GMGN_PAPER_POLICY_ENABLED,
        "action": "allow",
        "reason": reason,
        "toxic_score": 0,
        "edge_score": 0,
        "size_multiplier": 1.0,
        "spread_penalty_pct": 0.0,
        "flags": [],
        "features": {},
    }


def evaluate_gmgn_lotto_policy(gmgn, lotto_detail=None, lifecycle=None, entry_mode=None):
    """
    Evaluate GMGN enrichment for paper LOTTO entries.

    The policy can reject, downsize, or boost paper entries. It never increases
    position size and returns allow/no-op when GMGN is unavailable.
    """
    if not GMGN_PAPER_POLICY_ENABLED:
        return _noop("gmgn_policy_disabled")
    gmgn = gmgn or {}
    if gmgn.get("available") is False:
        result = _noop("gmgn_unavailable")
        result["features"] = {"source_reason": gmgn.get("reason")}
        return result
    if not gmgn:
        return _noop("gmgn_missing")

    lotto_detail = lotto_detail or {}
    entry_mode = entry_mode or lotto_detail.get("entry_mode") or ""
    market_cap = _first_positive(
        gmgn.get("market_cap"),
        gmgn.get("usd_market_cap"),
        lotto_detail.get("market_cap"),
        lotto_detail.get("current_mc"),
        lotto_detail.get("signal_mc"),
        lotto_detail.get("mc_usd"),
    )
    top10_threshold, top10_tier = gmgn_top10_threshold_for_market_cap(market_cap)
    execution_context = _execution_context(lotto_detail)
    top10_relax_execution_eligible = all(
        execution_context.get(name)
        for name in ("quote_clean_ok", "quote_executable_ok", "timing_ok", "liquidity_ok", "risk_ok")
    )

    features = {
        "entry_mode": entry_mode,
        "market_cap": market_cap,
        "top10_mc_tier": top10_tier,
        "top10_reject_rate_effective": top10_threshold,
        "top10_relax_execution_eligible": top10_relax_execution_eligible,
        "execution_context": execution_context,
        "bundler_rate": _f(gmgn.get("bundler_rate")),
        "rat_trader_amount_rate": _f(gmgn.get("rat_trader_amount_rate")),
        "entrapment_ratio": _f(gmgn.get("entrapment_ratio")),
        "bot_degen_rate": _f(gmgn.get("bot_degen_rate")),
        "top10_holder_rate": _f(gmgn.get("top10_holder_rate")),
        "creator_hold_rate": _f(gmgn.get("creator_hold_rate")),
        "dev_team_hold_rate": _f(gmgn.get("dev_team_hold_rate")),
        "smart_degen_count": _i(gmgn.get("smart_degen_count")),
        "renowned_count": _i(gmgn.get("renowned_count")),
        "sniper_count": _i(gmgn.get("sniper_count")),
        "creator_close": bool(gmgn.get("creator_close")),
        "has_social": bool(gmgn.get("twitter_username") or gmgn.get("website") or gmgn.get("telegram")),
        "cto_flag": _i(gmgn.get("cto_flag")),
        "dexscr_update_link": _i(gmgn.get("dexscr_update_link")),
        "dexscr_ad": _i(gmgn.get("dexscr_ad")),
        "dexscr_trending_bar": _i(gmgn.get("dexscr_trending_bar")),
        "launchpad_progress": _f(gmgn.get("launchpad_progress")),
    }

    flags = []
    hard_reject_flags = set()
    toxic_score = 0
    edge_score = 0

    reject_checks = [
        ("gmgn_toxic_rat_trader", features["rat_trader_amount_rate"], GMGN_RAT_REJECT_RATE),
        ("gmgn_toxic_entrapment", features["entrapment_ratio"], GMGN_ENTRAPMENT_REJECT_RATE),
        ("gmgn_creator_holding", features["creator_hold_rate"], GMGN_CREATOR_HOLD_REJECT_RATE),
        ("gmgn_dev_team_holding", features["dev_team_hold_rate"], GMGN_DEV_TEAM_HOLD_REJECT_RATE),
        ("gmgn_toxic_bundler", features["bundler_rate"], GMGN_BUNDLER_REJECT_RATE),
    ]
    for flag, value, threshold in reject_checks:
        if value > threshold:
            flags.append(flag)
            hard_reject_flags.add(flag)
            toxic_score += 2

    if features["top10_holder_rate"] > top10_threshold:
        flags.append("gmgn_high_top10_concentration")
        hard_reject_flags.add("gmgn_high_top10_concentration")
        toxic_score += 2
    elif features["top10_holder_rate"] > GMGN_TOP10_REJECT_RATE:
        if top10_relax_execution_eligible and not hard_reject_flags:
            flags.append("gmgn_mc_aware_top10_allowed")
            toxic_score += 1
        else:
            flags.append("gmgn_high_top10_requires_execution_eligibility")
            hard_reject_flags.add("gmgn_high_top10_requires_execution_eligibility")
            toxic_score += 2

    if features["bundler_rate"] > GMGN_BUNDLER_DOWNSIZE_RATE:
        flags.append("gmgn_medium_bundler_rate")
        toxic_score += 1
    if features["bot_degen_rate"] > GMGN_BOT_DOWNSIZE_RATE:
        flags.append("gmgn_high_bot_degen_rate")
        toxic_score += 1
    if features["sniper_count"] >= GMGN_SNIPER_DOWNSIZE_COUNT and features["smart_degen_count"] < GMGN_SMART_BOOST_COUNT:
        flags.append("gmgn_snipers_without_smart_money")
        toxic_score += 1

    if features["smart_degen_count"] >= GMGN_SMART_BOOST_COUNT:
        edge_score += 2
        flags.append("gmgn_smart_money_present")
    if features["renowned_count"] >= GMGN_RENOWNED_BOOST_COUNT:
        edge_score += 1
        flags.append("gmgn_renowned_wallets_present")
    if features["creator_close"]:
        edge_score += 1
        flags.append("gmgn_creator_close")
    if features["has_social"]:
        flags.append("gmgn_social_present")
    if features["cto_flag"]:
        flags.append("gmgn_cto_flag")
    if features["dexscr_update_link"]:
        flags.append("gmgn_dexscr_link_updated")
    if features["dexscr_ad"] or features["dexscr_trending_bar"]:
        flags.append("gmgn_paid_attention_present")
    if 0 < features["top10_holder_rate"] <= GMGN_CLEAN_TOP10_RATE:
        edge_score += 1
        flags.append("gmgn_clean_top10")
    if features["bundler_rate"] <= GMGN_CLEAN_BUNDLER_RATE:
        edge_score += 1
        flags.append("gmgn_clean_bundler")
    if features["rat_trader_amount_rate"] <= GMGN_CLEAN_RAT_RATE:
        edge_score += 1
        flags.append("gmgn_clean_rat_trader")

    action = "allow"
    reason = "gmgn_policy_allow"
    size_multiplier = 1.0
    spread_penalty_pct = 0.0

    hard_reject = bool(hard_reject_flags)
    if hard_reject:
        action = "reject" if GMGN_PAPER_REJECT_ENABLED else "shadow_reject"
        reason = next(
            flag for flag in flags
            if flag.startswith("gmgn_toxic_")
            or flag.endswith("_holding")
            or flag in {"gmgn_high_top10_concentration", "gmgn_high_top10_requires_execution_eligibility"}
        )
        spread_penalty_pct = GMGN_TOXIC_SPREAD_PENALTY_PCT
        size_multiplier = GMGN_MIN_SIZE_MULTIPLIER
    elif GMGN_PAPER_DOWNSIZE_ENABLED and toxic_score > 0:
        action = "downsize"
        reason = "gmgn_medium_toxic_downsize"
        size_multiplier = max(GMGN_MIN_SIZE_MULTIPLIER, min(1.0, GMGN_DOWNSIZE_MULTIPLIER))
        spread_penalty_pct = GMGN_DOWNSIZE_SPREAD_PENALTY_PCT
    elif GMGN_PAPER_BOOST_ENABLED and edge_score >= 4:
        action = "boost"
        reason = "gmgn_clean_smart_money_boost" if features["smart_degen_count"] >= GMGN_SMART_BOOST_COUNT else "gmgn_clean_structure_boost"

    return {
        "enabled": True,
        "action": action,
        "reason": reason,
        "toxic_score": toxic_score,
        "edge_score": edge_score,
        "size_multiplier": size_multiplier,
        "spread_penalty_pct": spread_penalty_pct,
        "flags": flags,
        "features": features,
    }


def gmgn_policy_blocks_explosive_direct(policy):
    """Return True when direct scout should not bypass chasing_top."""
    policy = policy or {}
    if policy.get("action") in {"reject", "shadow_reject"}:
        return True
    features = policy.get("features") or {}
    if _f(features.get("bundler_rate")) > GMGN_BUNDLER_REJECT_RATE:
        return True
    if _f(features.get("rat_trader_amount_rate")) > GMGN_RAT_REJECT_RATE:
        return True
    if _f(features.get("entrapment_ratio")) > GMGN_ENTRAPMENT_REJECT_RATE:
        return True
    if _i(policy.get("toxic_score")) > 1:
        return True
    return False


def gmgn_policy_allows_tiny_scout(policy):
    policy = policy or {}
    if not GMGN_TINY_SCOUT_ENABLED:
        return False
    bundler_only_rescue = gmgn_policy_allows_bundler_only_tiny_rescue(policy)
    if policy.get("action") in {"reject", "shadow_reject"}:
        if not bundler_only_rescue:
            return False
    if _i(policy.get("toxic_score")) > GMGN_TINY_SCOUT_MAX_TOXIC_SCORE:
        if not bundler_only_rescue:
            return False
    if _i(policy.get("edge_score")) < GMGN_TINY_SCOUT_MIN_EDGE_SCORE:
        return False
    features = policy.get("features") or {}
    if _f(features.get("rat_trader_amount_rate")) > GMGN_RAT_REJECT_RATE:
        return False
    if _f(features.get("entrapment_ratio")) > GMGN_ENTRAPMENT_REJECT_RATE:
        return False
    if _f(features.get("bundler_rate")) > GMGN_BUNDLER_REJECT_RATE and not bundler_only_rescue:
        return False
    if _f(features.get("creator_hold_rate")) > GMGN_CREATOR_HOLD_REJECT_RATE:
        return False
    if _f(features.get("dev_team_hold_rate")) > GMGN_DEV_TEAM_HOLD_REJECT_RATE:
        return False
    return True


def gmgn_policy_allows_bundler_only_tiny_rescue(policy):
    policy = policy or {}
    if not GMGN_BUNDLER_ONLY_TINY_RESCUE_ENABLED:
        return False
    flags = set(policy.get("flags") or [])
    features = policy.get("features") or {}
    if "gmgn_toxic_bundler" not in flags and policy.get("reason") != "gmgn_toxic_bundler":
        return False
    hard_toxic = {
        "gmgn_toxic_rat_trader",
        "gmgn_toxic_entrapment",
        "gmgn_creator_holding",
        "gmgn_dev_team_holding",
        "gmgn_high_top10_concentration",
    }
    if flags.intersection(hard_toxic):
        return False
    if _f(features.get("rat_trader_amount_rate")) > GMGN_RAT_REJECT_RATE:
        return False
    if _f(features.get("entrapment_ratio")) > GMGN_ENTRAPMENT_REJECT_RATE:
        return False
    if _f(features.get("creator_hold_rate")) > GMGN_CREATOR_HOLD_REJECT_RATE:
        return False
    if _f(features.get("dev_team_hold_rate")) > GMGN_DEV_TEAM_HOLD_REJECT_RATE:
        return False
    if _f(features.get("top10_holder_rate")) > GMGN_TOP10_REJECT_RATE:
        return False
    if _f(features.get("bundler_rate")) > GMGN_BUNDLER_ONLY_TINY_RESCUE_MAX_BUNDLER_RATE:
        return False
    if _i(policy.get("edge_score")) < GMGN_BUNDLER_ONLY_TINY_RESCUE_MIN_EDGE_SCORE:
        return False
    return True


def evaluate_gmgn_tiny_scout_rescue(reject_reason, policy, lotto_detail=None):
    """Return a paper-only tiny scout override for high-upside near misses."""
    reject_reason = str(reject_reason or "")
    lotto_detail = lotto_detail or {}
    if not gmgn_policy_allows_tiny_scout(policy):
        return {"allow": False, "reason": "gmgn_tiny_scout_policy_not_clean"}

    live_top1 = _f(lotto_detail.get("live_top1_pct"))
    live_top10 = _f(lotto_detail.get("live_top10_pct") or lotto_detail.get("top10_pct"))
    liquidity_usd = _f(lotto_detail.get("liquidity_usd"))
    vol_m5 = _f(lotto_detail.get("vol_m5"))
    tx_m5 = _f(lotto_detail.get("tx_m5"))
    price_change_m5 = _f(lotto_detail.get("price_change_m5"))

    concentration_reason = reject_reason.startswith("lotto_live_top1_") or reject_reason.startswith("lotto_live_top10_")
    if concentration_reason:
        if (
            live_top1 <= GMGN_TINY_SCOUT_TOP1_MAX_PCT
            and live_top10 <= GMGN_TINY_SCOUT_TOP10_MAX_PCT
            and liquidity_usd >= GMGN_CONCENTRATION_TINY_SCOUT_MIN_LIQUIDITY_USD
            and vol_m5 >= GMGN_CONCENTRATION_TINY_SCOUT_MIN_VOL_M5
            and tx_m5 >= GMGN_CONCENTRATION_TINY_SCOUT_MIN_TX_M5
            and price_change_m5 >= GMGN_CONCENTRATION_TINY_SCOUT_MAX_NEG_M5
        ):
            return {
                "allow": True,
                "entry_mode": "gmgn_concentration_tiny_scout",
                "reason": "gmgn_concentration_tiny_scout_ok",
                "position_size_sol": GMGN_CONCENTRATION_TINY_SCOUT_SIZE_SOL,
                "detail": {
                    "rescued_reject_reason": reject_reason,
                    "live_top1_pct": live_top1,
                    "live_top10_pct": live_top10,
                    "liquidity_usd": liquidity_usd,
                    "vol_m5": vol_m5,
                    "tx_m5": tx_m5,
                    "price_change_m5": price_change_m5,
                    "gmgn_tiny_scout_max_top1_pct": GMGN_TINY_SCOUT_TOP1_MAX_PCT,
                    "gmgn_tiny_scout_max_top10_pct": GMGN_TINY_SCOUT_TOP10_MAX_PCT,
                    "min_liquidity_usd": GMGN_CONCENTRATION_TINY_SCOUT_MIN_LIQUIDITY_USD,
                    "min_vol_m5": GMGN_CONCENTRATION_TINY_SCOUT_MIN_VOL_M5,
                    "min_tx_m5": GMGN_CONCENTRATION_TINY_SCOUT_MIN_TX_M5,
                    "max_negative_m5": GMGN_CONCENTRATION_TINY_SCOUT_MAX_NEG_M5,
                },
            }
        return {"allow": False, "reason": "gmgn_concentration_activity_not_enough"}

    if reject_reason == "lotto_midcap_activity_unconfirmed":
        scout_quality = evaluate_scout_quality(
            mode="gmgn_midcap_near_miss_scout",
            route="LOTTO",
            trend=lotto_detail,
            gmgn=policy,
            position_size_sol=GMGN_TINY_SCOUT_SIZE_SOL,
            liquidity_usd=liquidity_usd,
            top1_pct=live_top1,
            top10_pct=live_top10,
        )
        if scout_quality.get("pass"):
            return {
                "allow": True,
                "entry_mode": "gmgn_midcap_near_miss_scout",
                "reason": "gmgn_midcap_near_miss_scout_ok",
                "position_size_sol": GMGN_TINY_SCOUT_SIZE_SOL,
                "detail": {
                    "rescued_reject_reason": reject_reason,
                    "liquidity_usd": liquidity_usd,
                    "vol_m5": vol_m5,
                    "tx_m5": tx_m5,
                    "price_change_m5": price_change_m5,
                    "min_liquidity_usd": GMGN_MIDCAP_NEAR_MISS_MIN_LIQUIDITY_USD,
                    "min_vol_m5": GMGN_MIDCAP_NEAR_MISS_MIN_VOL_M5,
                    "min_tx_m5": GMGN_MIDCAP_NEAR_MISS_MIN_TX_M5,
                    "max_negative_m5": GMGN_MIDCAP_NEAR_MISS_MAX_NEG_M5,
                    "scout_quality": scout_quality,
                },
            }
        return {
            "allow": False,
            "reason": scout_quality.get("reason") or "gmgn_midcap_near_miss_quality_reject",
            "detail": {"scout_quality": scout_quality},
        }

    unknown_or_falling_knife_reason = reject_reason in {
        "lotto_newborn_falling_knife_low_liq",
        "lotto_liq_unknown_pumpfun_wait",
    } or reject_reason.startswith("lotto_liq_unknown_pumpfun_wait")
    if unknown_or_falling_knife_reason:
        if (
            live_top1 <= GMGN_TINY_SCOUT_TOP1_MAX_PCT
            and live_top10 <= GMGN_TINY_SCOUT_TOP10_MAX_PCT
            and vol_m5 >= GMGN_UNKNOWN_DATA_TINY_SCOUT_MIN_VOL_M5
            and tx_m5 >= GMGN_UNKNOWN_DATA_TINY_SCOUT_MIN_TX_M5
            and price_change_m5 >= GMGN_UNKNOWN_DATA_TINY_SCOUT_MAX_NEG_M5
        ):
            return {
                "allow": True,
                "entry_mode": "gmgn_unknown_data_tiny_scout",
                "reason": "gmgn_unknown_data_tiny_scout_ok",
                "position_size_sol": GMGN_CONCENTRATION_TINY_SCOUT_SIZE_SOL,
                "detail": {
                    "rescued_reject_reason": reject_reason,
                    "live_top1_pct": live_top1,
                    "live_top10_pct": live_top10,
                    "vol_m5": vol_m5,
                    "tx_m5": tx_m5,
                    "price_change_m5": price_change_m5,
                    "min_vol_m5": GMGN_UNKNOWN_DATA_TINY_SCOUT_MIN_VOL_M5,
                    "min_tx_m5": GMGN_UNKNOWN_DATA_TINY_SCOUT_MIN_TX_M5,
                    "max_negative_m5": GMGN_UNKNOWN_DATA_TINY_SCOUT_MAX_NEG_M5,
                },
            }
        return {"allow": False, "reason": "gmgn_unknown_data_activity_not_enough"}

    reclaim_reason = reject_reason in {"lotto_timing_negative_m5", "post_spread_abort"}
    if reclaim_reason:
        if (
            live_top1 <= GMGN_TINY_SCOUT_TOP1_MAX_PCT
            and live_top10 <= GMGN_TINY_SCOUT_TOP10_MAX_PCT
            and vol_m5 >= GMGN_RECLAIM_TINY_SCOUT_MIN_VOL_M5
            and tx_m5 >= GMGN_RECLAIM_TINY_SCOUT_MIN_TX_M5
            and price_change_m5 >= GMGN_RECLAIM_TINY_SCOUT_MIN_M5
        ):
            return {
                "allow": True,
                "entry_mode": "gmgn_reclaim_tiny_scout",
                "reason": "gmgn_reclaim_tiny_scout_ok",
                "position_size_sol": GMGN_CONCENTRATION_TINY_SCOUT_SIZE_SOL,
                "detail": {
                    "rescued_reject_reason": reject_reason,
                    "live_top1_pct": live_top1,
                    "live_top10_pct": live_top10,
                    "vol_m5": vol_m5,
                    "tx_m5": tx_m5,
                    "price_change_m5": price_change_m5,
                    "min_vol_m5": GMGN_RECLAIM_TINY_SCOUT_MIN_VOL_M5,
                    "min_tx_m5": GMGN_RECLAIM_TINY_SCOUT_MIN_TX_M5,
                    "min_price_change_m5": GMGN_RECLAIM_TINY_SCOUT_MIN_M5,
                },
            }
        return {"allow": False, "reason": "gmgn_reclaim_activity_not_enough"}
    return {"allow": False, "reason": "gmgn_tiny_scout_reason_not_rescueable"}
