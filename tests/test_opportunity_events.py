import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from opportunity_events import fetch_opportunity_events, init_opportunity_events, record_opportunity_event


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    return db


def test_record_opportunity_event_upserts_by_opportunity_key():
    db = memory_db()
    init_opportunity_events(db)

    key = record_opportunity_event(
        db,
        {
            "opportunity_key": "source:1:TOKEN",
            "event_ts": 1_000,
            "token_ca": "TOKEN",
            "symbol": "TOK",
            "source_type": "paper_decision_events",
            "route_bucket": "ATH",
            "quote_available": True,
            "quote_executable": True,
            "quote_clean": True,
            "route_available": True,
            "liquidity_usd": 50_000,
            "matrix_score": 82,
            "expected_rr": 3.2,
            "would_enter_a_class": True,
        },
    )
    assert key == "source:1:TOKEN"

    record_opportunity_event(
        db,
        {
            "opportunity_key": "source:1:TOKEN",
            "event_ts": 1_000,
            "token_ca": "TOKEN",
            "symbol": "TOK",
            "source_type": "paper_decision_events",
            "route_bucket": "ATH",
            "quote_available": True,
            "quote_executable": True,
            "quote_clean": True,
            "route_available": True,
            "liquidity_usd": 70_000,
            "matrix_score": 92,
            "expected_rr": 4.0,
            "would_enter_a_class": True,
            "did_enter": True,
            "linked_trade_id": "trade-1",
        },
    )

    rows = fetch_opportunity_events(db)
    assert len(rows) == 1
    assert rows[0]["liquidity_usd"] == 70_000
    assert rows[0]["matrix_score"] == 92
    assert rows[0]["did_enter"] == 1
    assert rows[0]["linked_trade_id"] == "trade-1"
