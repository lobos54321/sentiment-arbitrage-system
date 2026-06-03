import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from a_class_fastlane import AClassCandidate, evaluate_a_class_fastlane
from canonical_ledger import (
    fetch_a_class_events,
    init_canonical_ledger,
    record_a_class_decision_event,
    record_canonical_trade_entry,
    record_canonical_trade_exit,
)
from fastlane_config import load_a_class_config


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    return db


def test_init_creates_ledger_tables():
    db = memory_db()

    init_canonical_ledger(db)

    tables = {
        row["name"]
        for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    assert "canonical_trade_ledger" in tables
    assert "a_class_decision_events" in tables


def test_shadow_decision_event_does_not_create_trade_row():
    db = memory_db()
    candidate = AClassCandidate.from_mapping(
        {
            "token_ca": "TokenCA123",
            "symbol": "AFAST",
            "route_bucket": "ATH",
            "quote_available": True,
            "quote_executable": True,
            "quote_source": "gmgn",
            "quote_age_sec": 5,
            "route_available": True,
            "liquidity_usd": 50_000,
            "spread_pct": 1,
            "gmgn_pre_seen": True,
            "gmgn_activity_fresh": True,
            "gmgn_last_seen_age_sec": 5,
            "source_resonance": True,
            "fresh_momentum": True,
            "fresh_ath_refresh": True,
            "ath_continuation": True,
        }
    )
    decision = evaluate_a_class_fastlane(candidate, now_ts=1_000, config=load_a_class_config({}))

    record_a_class_decision_event(
        db,
        candidate=candidate,
        decision=decision,
        stored_action="WOULD_ENTER",
        source_table="unit",
        source_id=1,
        now_ts=1_000,
    )

    events = fetch_a_class_events(db, limit=5)
    trade_count = db.execute("SELECT COUNT(*) AS n FROM canonical_trade_ledger").fetchone()["n"]
    assert events[0]["action"] == "WOULD_ENTER"
    assert trade_count == 0


def test_entry_and_exit_use_sol_accounting():
    db = memory_db()

    record_canonical_trade_entry(
        db,
        {
            "trade_id": "trade-1",
            "token_ca": "TokenCA123",
            "symbol": "AFAST",
            "entry_ts": 1_000,
            "entry_size_sol": 0.001,
            "entry_quote_source": "gmgn",
            "entry_route_available": True,
            "entry_quote_executable": True,
            "is_a_class_fastlane": True,
            "a_class_grade": "A",
            "a_class_score": 75,
        },
    )
    record_canonical_trade_exit(
        db,
        "trade-1",
        {
            "exit_ts": 1_060,
            "realized_exit_sol": 0.0018,
            "exit_quote_source": "gmgn",
            "exit_route_available": True,
            "exit_quote_executable": True,
            "exit_reason": "take_profit",
        },
    )

    row = db.execute(
        "SELECT realized_pnl_sol, realized_pnl_pct, time_held_sec FROM canonical_trade_ledger WHERE trade_id = 'trade-1'"
    ).fetchone()
    assert round(row["realized_pnl_sol"], 10) == 0.0008
    assert round(row["realized_pnl_pct"], 4) == 0.8
    assert row["time_held_sec"] == 60


def test_no_route_trapped_and_outlier_flags_are_recorded():
    db = memory_db()

    record_canonical_trade_entry(
        db,
        {
            "trade_id": "trade-2",
            "token_ca": "TokenCA456",
            "entry_ts": 1_000,
            "entry_size_sol": 0.002,
            "entry_quote_source": "gmgn",
            "entry_route_available": True,
            "entry_quote_executable": True,
        },
    )
    record_canonical_trade_exit(
        db,
        "trade-2",
        {
            "exit_ts": 1_030,
            "realized_exit_sol": 0,
            "exit_reason": "no_route",
            "no_route_flag": True,
            "trapped_flag": True,
            "outlier_flag": True,
            "outlier_reason": "route_disappeared",
            "accounting_source": "no_route_zero_exit",
        },
    )

    row = db.execute(
        "SELECT no_route_flag, trapped_flag, outlier_flag, outlier_reason, accounting_source FROM canonical_trade_ledger WHERE trade_id = 'trade-2'"
    ).fetchone()
    assert row["no_route_flag"] == 1
    assert row["trapped_flag"] == 1
    assert row["outlier_flag"] == 1
    assert row["outlier_reason"] == "route_disappeared"
    assert row["accounting_source"] == "no_route_zero_exit"
