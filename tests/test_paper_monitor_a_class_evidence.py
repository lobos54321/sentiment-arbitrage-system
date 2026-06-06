import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from final_entry_contract import evaluate_final_entry_contract
from opportunity_events import fetch_opportunity_events
from paper_trade_monitor import (
    build_final_entry_contract_candidate,
    record_monitor_opportunity_event,
)


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    return db


def test_a_class_live_entry_preserves_source_hydrate_outcome_in_opportunity_event():
    source_result = {
        "source_table": "source_resonance_candidates",
        "opportunity_key": "a_class:source:token:1000",
        "candidate": {
            "provider_hydrate_outcome": "success",
            "provider_hydrate_reason": "provider_hydrated_route_ok",
            "source_component": "source_resonance_shadow",
        },
    }
    pending = {
        "token_ca": "TokenHydrated",
        "symbol": "HYD",
        "signal_ts": 1_000,
        "source_component": "source_resonance_shadow",
        "a_class_fastlane": {"source_decision_event": source_result},
        "expected_rr": 3.0,
        "expected_upside_pct": 0.60,
        "defined_risk_pct": 0.20,
    }
    execution = {
        "success": True,
        "quoteTs": 1_005,
    }

    final_candidate = build_final_entry_contract_candidate(
        pending=pending,
        execution=execution,
        entry_mode="a_class_fastlane_tiny_canary",
        normalized_mode="A_CLASS_FASTLANE",
        route_bucket="A_GRADE",
        lifecycle_id="TokenHydrated:1000",
        entry_ts=1_006,
        entry_price=0.000001,
        position_size_sol=0.001,
        token_amount_raw="1000000000",
        token_decimals=6,
        spread_pct=1.0,
        liquidity_usd=50_000,
        market_cap=100_000,
        entry_decision_contract={"odds_r": 3.0, "expected_upside_pct": 60.0, "expected_loss_pct": 20.0},
        entry_execution_availability="available",
        entry_execution_data_source="gmgn_entry_quote",
        now_ts=1_006,
    )
    assert final_candidate["provider_hydrate_outcome"] == "success"
    assert final_candidate["provider_hydrate_reason"] == "provider_hydrated_route_ok"
    assert final_candidate["source_opportunity_key"] == "a_class:source:token:1000"

    db = memory_db()
    final_decision = evaluate_final_entry_contract(final_candidate, now_ts=1_006)
    assert final_decision.passed

    record_monitor_opportunity_event(
        db,
        final_candidate=final_candidate,
        final_decision=final_decision,
        event_ts=1_006,
        linked_trade_id="trade-1",
        did_enter=True,
    )
    event = fetch_opportunity_events(db)[0]
    assert event["did_enter"] == 1
    assert event["hydrate_outcome"] == "success"
    assert event["hydrate_success"] == 1

