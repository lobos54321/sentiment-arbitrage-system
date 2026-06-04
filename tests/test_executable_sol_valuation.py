import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from executable_sol_valuation import defined_loss_pct_from_valuation, executable_sol_valuation


def test_executable_sol_valuation_accepts_fresh_executable_quote():
    result = executable_sol_valuation(
        {
            "quote_source": "jupiter",
            "route_available": True,
            "quote_executable": True,
            "quote_clean": True,
            "quote_age_sec": 4,
            "output_sol": 0.0018,
            "price_sol_per_token": 0.000001,
            "spread_pct": 1.2,
        },
        now_ts=1_000,
    )

    assert result.usable is True
    assert result.valuation_sol == 0.0018
    assert result.route_clean is True
    assert result.hard_blockers == ()


def test_executable_sol_valuation_blocks_stale_or_non_executable_quote():
    result = executable_sol_valuation(
        {
            "route_available": False,
            "quote_executable": False,
            "quote_age_sec": 60,
            "output_sol": 0.001,
        },
        now_ts=1_000,
        max_quote_age_sec=10,
    )

    assert result.usable is False
    assert set(result.hard_blockers) == {
        "route_unavailable",
        "quote_not_executable",
        "quote_stale",
    }


def test_defined_loss_pct_uses_executable_sol_value():
    assert defined_loss_pct_from_valuation(0.002, 0.0015) == 0.25
    assert defined_loss_pct_from_valuation(0.002, 0.003) == 0.0
    assert defined_loss_pct_from_valuation(0, 0.001) is None
