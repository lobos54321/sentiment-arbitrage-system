import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from final_entry_contract import evaluate_final_entry_contract
from fastlane_config import load_a_class_config


def base_candidate(**overrides):
    data = {
        "token_ca": "TokenA",
        "symbol": "TOK",
        "route_bucket": "ATH",
        "entry_mode": "ath_micro_reclaim_tiny_probe",
        "quote_source": "jupiter",
        "route_available": True,
        "quote_executable": True,
        "quote_clean": True,
        "quote_age_sec": 5,
        "output_sol": 0.001,
        "liquidity_usd": 50_000,
        "spread_pct": 1.0,
        "expected_rr": 2.5,
        "defined_risk_pct": 0.20,
        "top10_pct": 35,
        "bundler_rate": 0.01,
        "rat_trader_rate": 0.01,
        "entrapment_ratio": 0.01,
    }
    data.update(overrides)
    return data


def test_final_entry_contract_passes_clean_candidate():
    decision = evaluate_final_entry_contract(
        base_candidate(),
        mode_state={"status": "LIVE"},
        budget_state={"active_count": 0, "max_concurrent": 1},
        config=load_a_class_config({}),
        now_ts=1_000,
    )

    assert decision.passed is True
    assert decision.hard_blockers == []
    assert decision.route_bucket == "ATH"


def test_final_entry_contract_blocks_hard_execution_and_risk_failures():
    decision = evaluate_final_entry_contract(
        base_candidate(
            route_available=False,
            quote_executable=False,
            quote_age_sec=99,
            liquidity_usd=None,
            spread_pct=8.0,
            expected_rr=1.5,
            defined_risk_pct=0.25,
            risk_flags=["obvious_rug"],
            prior_exposure_in_lifecycle=True,
        ),
        mode_state={"status": "DISABLED"},
        budget_state={"active_count": 1, "max_concurrent": 1, "daily_loss_budget_hit": True},
        config=load_a_class_config({}),
        now_ts=1_000,
    )

    assert decision.decision == "BLOCK"
    assert {
        "route_unavailable",
        "quote_not_executable",
        "quote_stale",
        "liquidity_unknown",
        "spread_extreme",
        "expected_rr_below_2",
        "defined_loss_risk_above_20pct",
        "security_red_flag",
        "prior_exposure_in_lifecycle",
        "mode_disabled",
        "max_concurrent_reached",
        "daily_loss_budget_hit",
    }.issubset(set(decision.hard_blockers))


def test_final_entry_contract_notes_missing_soft_rr_but_does_not_block():
    decision = evaluate_final_entry_contract(
        base_candidate(expected_rr=None, defined_risk_pct=None),
        mode_state={"status": "LIVE"},
        budget_state={"active_count": 0, "max_concurrent": 1},
        config=load_a_class_config({}),
        now_ts=1_000,
    )

    assert decision.passed is True
    assert "expected_rr_missing" in decision.soft_notes
    assert "defined_risk_missing" in decision.soft_notes
