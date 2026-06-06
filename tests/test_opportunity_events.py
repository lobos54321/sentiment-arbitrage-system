import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from opportunity_events import (
    fetch_opportunity_events,
    init_opportunity_events,
    record_linked_trade_path_sample,
    record_opportunity_event,
    record_opportunity_path_sample,
)


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
    assert rows[0]["evidence_status"] == "quote_clean_executable"
    assert rows[0]["block_cause"] == "UNKNOWN"
    assert rows[0]["path_sample_count"] == 1

    sample = db.execute(
        "SELECT quote_pnl_pct, quote_clean, route_available FROM opportunity_event_path_samples WHERE opportunity_key = ?",
        (key,),
    ).fetchone()
    assert sample["quote_pnl_pct"] == 0
    assert sample["quote_clean"] == 1
    assert sample["route_available"] == 1


def test_record_opportunity_event_marks_route_unavailable_as_path_sample():
    db = memory_db()
    key = record_opportunity_event(
        db,
        {
            "opportunity_key": "source:2:TOKEN",
            "event_ts": 2_000,
            "token_ca": "TOKEN",
            "source_type": "unit",
            "route_bucket": "ATH",
            "quote_available": False,
            "quote_executable": False,
            "route_available": False,
            "hard_blockers": ["quote_not_available", "route_unavailable"],
        },
    )

    event = fetch_opportunity_events(db)[0]
    assert event["evidence_status"] == "no_route_or_route_unavailable"
    assert event["block_cause"] == "INFRA"
    assert event["recoverability"] == "provider_or_evidence_recoverable"
    sample = db.execute(
        "SELECT no_route_flag, quote_clean FROM opportunity_event_path_samples WHERE opportunity_key = ?",
        (key,),
    ).fetchone()
    assert sample["no_route_flag"] == 1
    assert sample["quote_clean"] == 0


def test_record_opportunity_event_persists_provider_hydrate_outcome():
    db = memory_db()
    key = record_opportunity_event(
        db,
        {
            "opportunity_key": "source:hydrate:TOKEN",
            "event_ts": 2_500,
            "token_ca": "TOKEN",
            "source_type": "unit",
            "route_bucket": "ATH",
            "quote_available": True,
            "quote_executable": True,
            "quote_clean": True,
            "route_available": True,
            "data_confidence": "provider_hydrated_quote",
            "provider_hydrate_outcome": "success",
        },
    )

    event = fetch_opportunity_events(db)[0]
    assert key == "source:hydrate:TOKEN"
    assert event["hydrate_outcome"] == "success"
    assert event["hydrate_success"] == 1


def test_path_sample_derives_pnl_from_repeated_current_price_observations():
    db = memory_db()
    key = record_opportunity_event(
        db,
        {
            "opportunity_key": "source:path-price:TOKEN",
            "event_ts": 1_000,
            "token_ca": "TOKEN",
            "source_type": "unit",
            "route_bucket": "ATH",
            "quote_available": True,
            "quote_executable": True,
            "quote_clean": True,
            "route_available": True,
            "current_price": 1.0,
            "would_enter_a_class": True,
        },
    )

    ok = record_opportunity_path_sample(
        db,
        key,
        {
            "sample_ts": 1_060,
            "quote_clean": True,
            "quote_executable": True,
            "route_available": True,
            "current_price": 1.6,
        },
    )

    assert ok is True
    samples = db.execute(
        "SELECT sample_ts, quote_pnl_pct, valuation_sol FROM opportunity_event_path_samples WHERE opportunity_key = ? ORDER BY sample_ts",
        (key,),
    ).fetchall()
    assert len(samples) == 2
    assert samples[0]["quote_pnl_pct"] == 0
    assert samples[0]["valuation_sol"] == 1.0
    assert round(samples[1]["quote_pnl_pct"], 6) == 0.6
    assert samples[1]["valuation_sol"] == 1.6


def test_linked_trade_path_sample_updates_all_linked_opportunities():
    db = memory_db()
    record_opportunity_event(
        db,
        {
            "opportunity_key": "source:trade:TOKEN",
            "event_ts": 1_000,
            "token_ca": "TOKEN",
            "source_type": "unit",
            "route_bucket": "ATH",
            "quote_available": True,
            "quote_executable": True,
            "quote_clean": True,
            "route_available": True,
            "linked_trade_id": "trade-1",
        },
    )

    count = record_linked_trade_path_sample(
        db,
        "trade-1",
        {
            "sample_ts": 1_020,
            "quote_pnl_pct": 0.55,
            "quote_clean": True,
            "quote_executable": True,
            "route_available": True,
            "quote_source": "gmgn",
        },
    )

    assert count == 1
    samples = db.execute(
        "SELECT quote_pnl_pct FROM opportunity_event_path_samples WHERE opportunity_key = ? ORDER BY sample_ts",
        ("source:trade:TOKEN",),
    ).fetchall()
    assert [row["quote_pnl_pct"] for row in samples] == [0, 0.55]
