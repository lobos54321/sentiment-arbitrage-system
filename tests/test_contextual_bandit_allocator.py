import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from contextual_bandit_allocator import build_tiny_bandit_allocation


def test_bandit_allocator_is_shadow_only_and_requires_samples():
    allocation = build_tiny_bandit_allocation(
        [
            {"mode": "ATH_CONTINUATION", "closed_trades": 3, "avg_pnl_sol": 0.01, "status": "LIVE"},
        ],
        min_samples=30,
    )

    ath = next(row for row in allocation["arms"] if row["arm"] == "ATH_CONTINUATION")
    assert allocation["advisory_only"] is True
    assert allocation["can_trigger_trade"] is False
    assert ath["eligible"] is False
    assert ath["recommended_max_size_sol"] == 0.0


def test_bandit_allocator_winsorizes_outlier_reward():
    allocation = build_tiny_bandit_allocation(
        [
            {"mode": "A_CLASS_FASTLANE", "closed_trades": 50, "avg_pnl_sol": 10.0, "status": "LIVE"},
            {"mode": "ATH_CONTINUATION", "closed_trades": 50, "avg_pnl_sol": 0.001, "status": "LIVE"},
        ],
        total_budget_sol=0.006,
        reward_cap_sol=0.003,
        min_samples=30,
    )

    a_class = next(row for row in allocation["arms"] if row["arm"] == "A_CLASS_FASTLANE")
    assert a_class["winsorized_reward_sol"] == 0.003
    assert a_class["recommended_max_size_sol"] <= 0.003


def test_bandit_allocator_excludes_disabled_arm():
    allocation = build_tiny_bandit_allocation(
        [
            {"mode": "LOTTO_TINY_SCOUT", "closed_trades": 50, "avg_pnl_sol": 0.002, "status": "DISABLED"},
            {"mode": "ATH_CONTINUATION", "closed_trades": 50, "avg_pnl_sol": 0.001, "status": "LIVE"},
        ],
        min_samples=30,
    )

    lotto = next(row for row in allocation["arms"] if row["arm"] == "LOTTO_TINY_SCOUT")
    ath = next(row for row in allocation["arms"] if row["arm"] == "ATH_CONTINUATION")
    assert lotto["eligible"] is False
    assert lotto["recommended_max_size_sol"] == 0.0
    assert ath["recommended_max_size_sol"] > 0
