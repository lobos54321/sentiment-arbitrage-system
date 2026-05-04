#!/usr/bin/env python3

import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from gmgn_policy import (  # noqa: E402
    evaluate_gmgn_lotto_policy,
    evaluate_gmgn_tiny_scout_rescue,
    gmgn_policy_blocks_explosive_direct,
)
from entry_engine import evaluate_smart_entry  # noqa: E402
from lotto_engine import build_lotto_pending  # noqa: E402
from paper_trade_monitor import evaluate_entry_edge_budget, pending_is_paper_tiny_scout  # noqa: E402


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


def test_gmgn_tiny_scout_rescues_clean_concentration_near_miss():
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
    rescue = evaluate_gmgn_tiny_scout_rescue(
        "lotto_live_top1_36pct",
        policy,
        {"live_top1_pct": 36.4, "live_top10_pct": 62.5},
    )

    assert rescue["allow"] is True
    assert rescue["entry_mode"] == "gmgn_concentration_tiny_scout"
    assert rescue["position_size_sol"] == 0.003


def test_gmgn_tiny_scout_does_not_rescue_toxic_policy():
    policy = evaluate_gmgn_lotto_policy(
        {
            "available": True,
            "rat_trader_amount_rate": 0.42,
            "bundler_rate": 0.10,
            "entrapment_ratio": 0.01,
            "top10_holder_rate": 0.18,
        }
    )
    rescue = evaluate_gmgn_tiny_scout_rescue(
        "lotto_live_top1_36pct",
        policy,
        {"live_top1_pct": 36.4, "live_top10_pct": 62.5},
    )

    assert rescue["allow"] is False


def test_gmgn_tiny_scout_rescues_unknown_data_with_clean_high_activity():
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
    rescue = evaluate_gmgn_tiny_scout_rescue(
        "lotto_newborn_falling_knife_low_liq",
        policy,
        {
            "live_top1_pct": 34,
            "live_top10_pct": 61,
            "vol_m5": 24000,
            "tx_m5": 310,
            "price_change_m5": -18,
        },
    )

    assert rescue["allow"] is True
    assert rescue["entry_mode"] == "gmgn_unknown_data_tiny_scout"
    assert rescue["position_size_sol"] == 0.003


def test_gmgn_tiny_scout_does_not_rescue_unknown_data_when_toxic():
    policy = evaluate_gmgn_lotto_policy(
        {
            "available": True,
            "rat_trader_amount_rate": 0.42,
            "bundler_rate": 0.10,
            "entrapment_ratio": 0.01,
            "top10_holder_rate": 0.18,
        }
    )
    rescue = evaluate_gmgn_tiny_scout_rescue(
        "lotto_newborn_falling_knife_low_liq",
        policy,
        {
            "live_top1_pct": 34,
            "live_top10_pct": 61,
            "vol_m5": 24000,
            "tx_m5": 310,
            "price_change_m5": -18,
        },
    )

    assert rescue["allow"] is False


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


def test_lotto_pending_honors_gmgn_tiny_scout_position_size():
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
            "entry_mode": "gmgn_concentration_tiny_scout",
            "position_size_sol": 0.003,
            "gmgn_policy": {"action": "boost", "size_multiplier": 1.0},
        },
    )

    assert pending["entry_mode"] == "gmgn_concentration_tiny_scout"
    assert pending["kelly_position_sol"] == 0.003
    assert pending["lotto_state"]["positionSizeSol"] == 0.003


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


def test_entry_edge_budget_allows_wider_spread_for_paper_tiny_scout():
    budget = evaluate_entry_edge_budget(
        route="LOTTO",
        trigger_price=1.0,
        quote_price=1.029,
        lifecycle={
            "lifecycle_features": {
                "liquidity_unknown": True,
            }
        },
        pending={
            "is_lotto": True,
            "entry_mode": "gmgn_concentration_tiny_scout",
            "kelly_position_sol": 0.003,
            "entry_readiness_policy": {
                "max_spread_pct": 1.0,
            },
        },
    )

    assert budget["is_tiny_scout"] is True
    assert budget["tiny_scout_spread_cap_pct"] == 3.0
    assert budget["max_spread_pct"] == 3.0
    assert budget["pass"] is True


def test_pending_is_paper_tiny_scout_detects_nested_paper_scout():
    pending = {
        "lotto_state": {
            "entryDecision": {
                "paper_only_scout": True,
                "position_size_sol": 0.003,
            },
        },
    }

    assert pending_is_paper_tiny_scout(pending) is True


def test_pending_is_paper_tiny_scout_rejects_large_paper_scout():
    pending = {
        "paper_only_scout": True,
        "kelly_position_sol": 0.08,
    }

    assert pending_is_paper_tiny_scout(pending) is False


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


def test_smart_entry_gmgn_tiny_scout_bypasses_low_kline_volume(monkeypatch):
    import entry_engine as entry_engine_module
    import paper_trade_monitor as monitor_module

    policy = {
        "allowed_entry_modes": ["gmgn_concentration_tiny_scout", "smart_entry_pullback_bounce"],
        "min_p_follow": 0.72,
        "lifecycle_profile": "LOTTO_NEWBORN_RISKY",
        "detail": {"route": "LOTTO"},
        "gmgn_policy": {
            "action": "boost",
            "reason": "gmgn_clean_smart_money_boost",
            "toxic_score": 0,
            "edge_score": 6,
            "features": {
                "bundler_rate": 0.12,
                "rat_trader_amount_rate": 0.01,
                "entrapment_ratio": 0.01,
                "creator_hold_rate": 0.0,
                "dev_team_hold_rate": 0.0,
            },
        },
    }
    trend = {
        "buys_m5": 124,
        "sells_m5": 100,
        "price_change_m5": 13.7,
        "vol_m5": 1800,
        "vol_h1": 72000,
        "fdv": 12490,
        "market_cap": 12490,
    }

    monkeypatch.setattr(monitor_module, "fetch_realtime_price", lambda *args, **kwargs: (1.0, "test", 0))
    monkeypatch.setattr(entry_engine_module, "fetch_dexscreener_trend_snapshot", lambda *_args, **_kwargs: trend)
    monkeypatch.setattr(entry_engine_module, "is_chasing_top", lambda *_args, **_kwargs: (False, ""))
    monkeypatch.setattr(entry_engine_module, "evaluate_trend_phase", lambda *_args, **_kwargs: ("WAIT", "mixed"))
    monkeypatch.setattr(entry_engine_module, "calculate_ema_deviation", lambda *_args, **_kwargs: (10.0, 0.9))
    monkeypatch.setattr(entry_engine_module, "get_recent_synthetic_bars", lambda *_args, **_kwargs: [])

    should_enter, reason, detail, trigger = evaluate_smart_entry(
        "DogToken",
        symbol="Dog",
        pool_address="Pool",
        entry_readiness_policy=policy,
    )

    assert should_enter is True
    assert reason == "gmgn_concentration_tiny_scout"
    assert "node=gmgn_tiny_scout" in detail
    assert trigger == 1.0


def test_smart_entry_reclaim_tiny_scout_bypasses_prior_spread_abort(monkeypatch):
    import entry_engine as entry_engine_module
    import paper_trade_monitor as monitor_module

    policy = {
        "allowed_entry_modes": ["smart_entry_reclaim_tiny_scout", "smart_entry_pullback_bounce"],
        "min_p_follow": 0.72,
        "lifecycle_profile": "LOTTO_NEWBORN_RISKY",
        "detail": {"route": "LOTTO"},
    }
    trend = {
        "buys_m5": 120,
        "sells_m5": 60,
        "price_change_m5": 15,
        "vol_m5": 10000,
        "vol_h1": 80000,
        "liquidity_usd": 7000,
        "fdv": 24000,
        "market_cap": 24000,
    }

    monkeypatch.setattr(monitor_module, "fetch_realtime_price", lambda *args, **kwargs: (1.0, "test", 0))
    monkeypatch.setattr(entry_engine_module, "fetch_dexscreener_trend_snapshot", lambda *_args, **_kwargs: trend)
    monkeypatch.setattr(entry_engine_module, "is_chasing_top", lambda *_args, **_kwargs: (False, ""))
    monkeypatch.setattr(entry_engine_module, "calculate_ema_deviation", lambda *_args, **_kwargs: (10.0, 0.9))

    should_enter, reason, detail, trigger = evaluate_smart_entry(
        "DogToken",
        symbol="Dog",
        pool_address="Pool",
        spread_abort_count=1,
        entry_readiness_policy=policy,
    )

    assert should_enter is True
    assert reason == "smart_entry_reclaim_tiny_scout"
    assert "node=smart_entry_reclaim_tiny_scout" in detail
    assert trigger == 1.0
