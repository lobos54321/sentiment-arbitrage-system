#!/usr/bin/env python3

import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from gmgn_policy import (  # noqa: E402
    evaluate_gmgn_lotto_policy,
    gmgn_policy_blocks_explosive_direct,
)
from entry_engine import evaluate_smart_entry  # noqa: E402
from lotto_engine import build_lotto_pending  # noqa: E402
from paper_trade_monitor import evaluate_entry_edge_budget  # noqa: E402


def test_gmgn_policy_noops_when_unavailable():
    policy = evaluate_gmgn_lotto_policy({"available": False, "reason": "disabled"})

    assert policy["action"] == "allow"
    assert policy["reason"] == "gmgn_unavailable"
    assert policy["size_multiplier"] == 1.0


def test_gmgn_policy_rejects_rat_trader_toxicity():
    policy = evaluate_gmgn_lotto_policy(
        {
            "available": True,
            "rat_trader_amount_rate": 0.42,
            "bundler_rate": 0.10,
            "entrapment_ratio": 0.01,
            "top10_holder_rate": 0.18,
        }
    )

    assert policy["action"] == "reject"
    assert policy["reason"] == "gmgn_toxic_rat_trader"
    assert policy["toxic_score"] >= 2
    assert policy["spread_penalty_pct"] > 0


def test_gmgn_policy_rejects_entrapment_toxicity():
    policy = evaluate_gmgn_lotto_policy(
        {
            "available": True,
            "entrapment_ratio": 0.35,
            "bundler_rate": 0.10,
            "rat_trader_amount_rate": 0.01,
            "top10_holder_rate": 0.18,
        }
    )

    assert policy["action"] == "reject"
    assert policy["reason"] == "gmgn_toxic_entrapment"


def test_gmgn_policy_downsizes_medium_bundler_without_rejecting():
    policy = evaluate_gmgn_lotto_policy(
        {
            "available": True,
            "bundler_rate": 0.45,
            "rat_trader_amount_rate": 0.01,
            "entrapment_ratio": 0.01,
            "top10_holder_rate": 0.18,
        }
    )

    assert policy["action"] == "downsize"
    assert policy["reason"] == "gmgn_medium_toxic_downsize"
    assert 0 < policy["size_multiplier"] < 1.0


def test_gmgn_policy_boosts_clean_smart_money():
    policy = evaluate_gmgn_lotto_policy(
        {
            "available": True,
            "smart_degen_count": 4,
            "renowned_count": 2,
            "creator_close": True,
            "top10_holder_rate": 0.18,
            "bundler_rate": 0.12,
            "rat_trader_amount_rate": 0.01,
            "entrapment_ratio": 0.01,
        }
    )

    assert policy["action"] == "boost"
    assert policy["reason"] == "gmgn_clean_smart_money_boost"
    assert policy["size_multiplier"] == 1.0
    assert policy["edge_score"] >= 4


def test_lotto_pending_applies_gmgn_downsize_without_exceeding_original_size():
    pending = build_lotto_pending(
        {
            "id": 7,
            "ca": "TokenCA",
            "symbol": "DOG",
            "signal_ts": 1000,
            "premium_signal_id": 42,
            "pool_address": "Pool",
        },
        "TokenCA:1000",
        detail={
            "entry_mode": "explosive_newborn_direct_scout",
            "position_size_sol": 0.008,
            "gmgn_policy": {
                "action": "downsize",
                "size_multiplier": 0.5,
            },
        },
    )

    assert pending["kelly_position_sol"] == 0.004
    assert pending["lotto_state"]["positionSizeSol"] == 0.004


def test_entry_edge_budget_applies_gmgn_toxic_spread_penalty():
    budget = evaluate_entry_edge_budget(
        route="LOTTO",
        trigger_price=1.0,
        quote_price=1.013,
        lifecycle={"lifecycle_features": {}},
        pending={
            "is_lotto": True,
            "lotto_state": {
                "entryDecision": {
                    "gmgn_policy": {
                        "action": "reject",
                        "toxic_score": 2,
                        "spread_penalty_pct": 0.5,
                    }
                }
            },
        },
    )

    assert budget["gmgn_spread_penalty_pct"] == 0.5
    assert budget["max_spread_pct"] == 1.5
    assert budget["pass"] is True


def test_entry_edge_budget_ignores_gmgn_policy_for_non_lotto():
    budget = evaluate_entry_edge_budget(
        route="ATH",
        trigger_price=1.0,
        quote_price=1.022,
        lifecycle={"lifecycle_features": {}},
        pending={
            "gmgn_policy": {
                "action": "reject",
                "toxic_score": 2,
                "spread_penalty_pct": 0.5,
            }
        },
    )

    assert budget["profile"] == "ath"
    assert budget["gmgn_spread_penalty_pct"] == 0.0
    assert budget["max_spread_pct"] == 2.5
    assert budget["pass"] is True


def test_gmgn_policy_blocks_toxic_explosive_direct():
    assert gmgn_policy_blocks_explosive_direct(
        {
            "action": "downsize",
            "toxic_score": 2,
            "features": {
                "bundler_rate": 0.45,
                "rat_trader_amount_rate": 0.01,
                "entrapment_ratio": 0.01,
            },
        }
    )


def test_smart_entry_toxic_gmgn_policy_blocks_explosive_chasing_bypass(monkeypatch):
    import entry_engine as entry_engine_module
    import paper_trade_monitor as monitor_module

    policy = {
        "allowed_entry_modes": ["explosive_newborn_direct_scout", "smart_entry_pullback_bounce"],
        "min_p_follow": 0.74,
        "lifecycle_profile": "LOTTO_NEWBORN_RISKY",
        "detail": {"route": "LOTTO"},
        "gmgn_policy": {
            "action": "downsize",
            "reason": "gmgn_medium_toxic_downsize",
            "toxic_score": 2,
            "features": {
                "bundler_rate": 0.45,
                "rat_trader_amount_rate": 0.01,
                "entrapment_ratio": 0.01,
            },
        },
    }
    trend = {
        "buys_m5": 215,
        "sells_m5": 187,
        "price_change_m5": 515,
        "vol_m5": 23658,
        "vol_h1": 12000,
        "fdv": 13230,
        "market_cap": 13230,
    }

    monkeypatch.setattr(monitor_module, "fetch_realtime_price", lambda *args, **kwargs: (1.0, "test", 0))
    monkeypatch.setattr(entry_engine_module, "fetch_dexscreener_trend_snapshot", lambda *_args, **_kwargs: trend)
    monkeypatch.setattr(entry_engine_module, "is_chasing_top", lambda *_args, **_kwargs: (True, "near_local_high"))

    should_enter, reason, detail, trigger = evaluate_smart_entry(
        "DogToken",
        symbol="Dog",
        pool_address="Pool",
        entry_readiness_policy=policy,
    )

    assert should_enter is False
    assert reason == "chasing_top"
    assert detail == "near_local_high"
    assert trigger is None
