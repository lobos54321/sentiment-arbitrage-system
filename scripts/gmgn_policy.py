#!/usr/bin/env python3
"""
GMGN paper-only decision policy.

This layer turns read-only GMGN enrichment into paper-trader decisions. It is
kept separate from gmgn_readonly so the data adapter remains non-opinionated.
"""

import os


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
GMGN_TINY_SCOUT_SIZE_SOL = float(os.environ.get("GMGN_TINY_SCOUT_SIZE_SOL", "0.005"))
GMGN_CONCENTRATION_TINY_SCOUT_SIZE_SOL = float(os.environ.get("GMGN_CONCENTRATION_TINY_SCOUT_SIZE_SOL", "0.003"))
GMGN_TINY_SCOUT_MIN_EDGE_SCORE = int(os.environ.get("GMGN_TINY_SCOUT_MIN_EDGE_SCORE", "4"))
GMGN_TINY_SCOUT_MAX_TOXIC_SCORE = int(os.environ.get("GMGN_TINY_SCOUT_MAX_TOXIC_SCORE", "1"))
GMGN_TINY_SCOUT_TOP1_MAX_PCT = float(os.environ.get("GMGN_TINY_SCOUT_TOP1_MAX_PCT", "58"))
GMGN_TINY_SCOUT_TOP10_MAX_PCT = float(os.environ.get("GMGN_TINY_SCOUT_TOP10_MAX_PCT", "78"))


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

    features = {
        "entry_mode": entry_mode,
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
    }

    flags = []
    toxic_score = 0
    edge_score = 0

    reject_checks = [
        ("gmgn_toxic_rat_trader", features["rat_trader_amount_rate"], GMGN_RAT_REJECT_RATE),
        ("gmgn_toxic_entrapment", features["entrapment_ratio"], GMGN_ENTRAPMENT_REJECT_RATE),
        ("gmgn_creator_holding", features["creator_hold_rate"], GMGN_CREATOR_HOLD_REJECT_RATE),
        ("gmgn_dev_team_holding", features["dev_team_hold_rate"], GMGN_DEV_TEAM_HOLD_REJECT_RATE),
        ("gmgn_high_top10_concentration", features["top10_holder_rate"], GMGN_TOP10_REJECT_RATE),
        ("gmgn_toxic_bundler", features["bundler_rate"], GMGN_BUNDLER_REJECT_RATE),
    ]
    for flag, value, threshold in reject_checks:
        if value > threshold:
            flags.append(flag)
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

    hard_reject = any(flag in flags for flag, _value, _threshold in reject_checks)
    if hard_reject:
        action = "reject" if GMGN_PAPER_REJECT_ENABLED else "shadow_reject"
        reason = next(flag for flag in flags if flag.startswith("gmgn_toxic_") or flag.endswith("_holding") or flag == "gmgn_high_top10_concentration")
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
    if policy.get("action") in {"reject", "shadow_reject"}:
        return False
    if _i(policy.get("toxic_score")) > GMGN_TINY_SCOUT_MAX_TOXIC_SCORE:
        return False
    if _i(policy.get("edge_score")) < GMGN_TINY_SCOUT_MIN_EDGE_SCORE:
        return False
    features = policy.get("features") or {}
    if _f(features.get("rat_trader_amount_rate")) > GMGN_RAT_REJECT_RATE:
        return False
    if _f(features.get("entrapment_ratio")) > GMGN_ENTRAPMENT_REJECT_RATE:
        return False
    if _f(features.get("bundler_rate")) > GMGN_BUNDLER_REJECT_RATE:
        return False
    if _f(features.get("creator_hold_rate")) > GMGN_CREATOR_HOLD_REJECT_RATE:
        return False
    if _f(features.get("dev_team_hold_rate")) > GMGN_DEV_TEAM_HOLD_REJECT_RATE:
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

    concentration_reason = reject_reason.startswith("lotto_live_top1_") or reject_reason.startswith("lotto_live_top10_")
    if concentration_reason:
        if live_top1 <= GMGN_TINY_SCOUT_TOP1_MAX_PCT and live_top10 <= GMGN_TINY_SCOUT_TOP10_MAX_PCT:
            return {
                "allow": True,
                "entry_mode": "gmgn_concentration_tiny_scout",
                "reason": "gmgn_concentration_tiny_scout_ok",
                "position_size_sol": GMGN_CONCENTRATION_TINY_SCOUT_SIZE_SOL,
                "detail": {
                    "rescued_reject_reason": reject_reason,
                    "live_top1_pct": live_top1,
                    "live_top10_pct": live_top10,
                    "gmgn_tiny_scout_max_top1_pct": GMGN_TINY_SCOUT_TOP1_MAX_PCT,
                    "gmgn_tiny_scout_max_top10_pct": GMGN_TINY_SCOUT_TOP10_MAX_PCT,
                },
            }
        return {"allow": False, "reason": "gmgn_tiny_scout_concentration_too_high"}

    if reject_reason == "lotto_midcap_activity_unconfirmed":
        if liquidity_usd >= 10_000 and vol_m5 >= 6_000 and tx_m5 >= 80:
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
                },
            }
    return {"allow": False, "reason": "gmgn_tiny_scout_reason_not_rescueable"}
