#!/usr/bin/env python3
"""Shadow-only contextual bandit allocation for tiny strategy buckets.

The allocator is deliberately conservative:
- it never triggers trades;
- rewards are winsorized so one 100x outlier cannot dominate;
- priors are strong enough that small samples stay near equal/low allocation.
"""

from __future__ import annotations

from typing import Any


DEFAULT_ARMS = ("ATH_CONTINUATION", "A_CLASS_FASTLANE", "RECLAIM_REVIVAL", "LOTTO_TINY_SCOUT")


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _winsorized(value: float, *, low: float, high: float) -> float:
    return min(high, max(low, float(value)))


def _row_by_mode(scorecard_rows: list[dict]) -> dict[str, dict]:
    out = {}
    for row in scorecard_rows or []:
        mode = str(row.get("mode") or row.get("bucket") or "")
        if mode:
            out[mode] = row
    return out


def build_tiny_bandit_allocation(
    scorecard_rows: list[dict],
    *,
    arms: tuple[str, ...] = DEFAULT_ARMS,
    total_budget_sol: float = 0.006,
    max_arm_size_sol: float = 0.003,
    min_samples: int = 30,
    prior_mean_sol: float = 0.0,
    prior_n: int = 30,
    reward_floor_sol: float = -0.0006,
    reward_cap_sol: float = 0.003,
) -> dict:
    rows = _row_by_mode(scorecard_rows)
    arm_rows = []
    for arm in arms:
        row = rows.get(arm, {})
        trades = int(row.get("closed_trades") or row.get("trades") or 0)
        avg = _safe_float(row.get("outlier_adjusted_avg_pnl_sol"), None)
        if avg is None:
            avg = _safe_float(row.get("avg_pnl_sol"), 0.0) or 0.0
        reward = _winsorized(avg, low=reward_floor_sol, high=reward_cap_sol)
        posterior_mean = ((prior_mean_sol * prior_n) + (reward * trades)) / (prior_n + trades)
        eligible = trades >= min_samples and str(row.get("status") or "SHADOW").upper() not in {"DISABLED", "BLOCKED"}
        arm_rows.append({
            "arm": arm,
            "trades": trades,
            "observed_reward_sol": avg,
            "winsorized_reward_sol": reward,
            "posterior_mean_sol": posterior_mean,
            "eligible": eligible,
            "status": row.get("status") or ("SHADOW" if trades < min_samples else "LIVE"),
        })

    positive = [row for row in arm_rows if row["eligible"] and row["posterior_mean_sol"] > 0]
    total_positive = sum(row["posterior_mean_sol"] for row in positive)
    for row in arm_rows:
        if not positive or row not in positive or total_positive <= 0:
            row["allocation_weight"] = 0.0
            row["recommended_max_size_sol"] = 0.0
        else:
            weight = row["posterior_mean_sol"] / total_positive
            row["allocation_weight"] = weight
            row["recommended_max_size_sol"] = min(max_arm_size_sol, total_budget_sol * weight)
    return {
        "schema_version": "v1.shadow_contextual_bandit_allocation",
        "advisory_only": True,
        "can_trigger_trade": False,
        "arms": arm_rows,
        "total_budget_sol": total_budget_sol,
        "max_arm_size_sol": max_arm_size_sol,
        "min_samples": min_samples,
        "reward_winsorization": {
            "floor_sol": reward_floor_sol,
            "cap_sol": reward_cap_sol,
            "prior_mean_sol": prior_mean_sol,
            "prior_n": prior_n,
        },
        "notes": [
            "Use only after canonical ledger coverage is stable.",
            "Do not enable allocation without hard gate, loss budget, and no-route controls.",
        ],
    }
