#!/usr/bin/env python3

import os
import sqlite3
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from external_alpha_shadow import (  # noqa: E402
    compute_next_external_alpha_state,
    init_external_alpha_shadow,
    lookup_external_alpha,
    record_external_alpha_candidates,
    record_external_alpha_health,
)
import gmgn_candidate_scout  # noqa: E402
from gmgn_candidate_scout import collect_candidates_with_errors, normalize_token  # noqa: E402


def candidate(mc, *, volume=10000, swaps=100, buys=60, sells=40, captured_at=1000):
    return {
        "source": "gmgn",
        "category": "trending",
        "chain": "sol",
        "ca": "TokenCA",
        "symbol": "DOG",
        "name": "Dog Token",
        "market_cap": mc,
        "liquidity": 5000,
        "volume": volume,
        "swaps": swaps,
        "buys": buys,
        "sells": sells,
        "captured_at": captured_at,
    }


def test_external_alpha_momentum_requires_changed_up_rounds():
    first = compute_next_external_alpha_state(candidate(10000, captured_at=1000), captured_at=1000)
    same = compute_next_external_alpha_state(candidate(10000, captured_at=1060), first, captured_at=1060)
    second = compute_next_external_alpha_state(candidate(10500, buys=61, captured_at=1120), same, captured_at=1120)
    third = compute_next_external_alpha_state(candidate(11000, buys=62, captured_at=1180), second, captured_at=1180)

    assert first["changed_count"] == 1
    assert same["changed_count"] == 1
    assert same["momentum_rounds"] == 1
    assert second["momentum_rounds"] == 2
    assert third["momentum_rounds"] == 3
    assert third["momentum_gain_pct"] == 10.0
    assert third["volume_confirmed"] == 1
    assert third["momentum_confirmed"] == 1


def test_record_and_lookup_external_alpha_state():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_external_alpha_shadow(db)

    assert record_external_alpha_candidates(db, [candidate(10000)], captured_at=1000) == {
        "recorded": 1,
        "momentum_confirmed": 0,
    }
    record_external_alpha_candidates(db, [candidate(10600, buys=62)], captured_at=1060)
    summary = record_external_alpha_candidates(db, [candidate(11200, buys=64)], captured_at=1120)

    assert summary == {"recorded": 1, "momentum_confirmed": 1}
    lookup = lookup_external_alpha(db, "TokenCA", chain="sol", now=1130, signal_ts=1180)
    assert lookup["available"] is True
    assert lookup["gmgn_pre_seen"] is True
    assert lookup["gmgn_momentum_confirmed"] is True
    assert lookup["gmgn_momentum_rounds"] == 3
    assert lookup["gmgn_lead_time_sec"] == 180
    assert lookup["last_seen_age_sec"] == 10


def test_lookup_external_alpha_handles_missing_and_stale():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_external_alpha_shadow(db)

    missing = lookup_external_alpha(db, "MissingCA", now=2000)
    assert missing == {"available": False, "reason": "external_alpha_not_seen"}

    record_external_alpha_candidates(db, [candidate(10000)], captured_at=1000)
    stale = lookup_external_alpha(db, "TokenCA", now=2000, lookback_sec=600)
    assert stale["available"] is False
    assert stale["reason"] == "external_alpha_stale"
    assert stale["last_seen_age_sec"] == 1000


def test_external_alpha_health_records_success_and_errors():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_external_alpha_shadow(db)

    ok = record_external_alpha_health(
        db,
        source="gmgn_candidate_scout",
        run_ts=1000,
        success=True,
        candidate_count=12,
        recorded_count=10,
        momentum_confirmed_count=2,
    )
    assert ok["success"] is True

    record_external_alpha_health(
        db,
        source="gmgn_candidate_scout",
        run_ts=1060,
        success=False,
        error="gmgn-cli failed",
    )
    row = db.execute("SELECT * FROM external_alpha_health WHERE source = 'gmgn_candidate_scout'").fetchone()
    assert row["last_run_ts"] == 1060
    assert row["last_success_ts"] == 1000
    assert row["candidate_count"] == 0
    assert row["error_count"] == 1
    assert row["last_error"] == "gmgn-cli failed"


def test_gmgn_candidate_normalize_accepts_ca_and_camel_base_token():
    normalized = normalize_token(
        {
            "ca": "TokenCA",
            "baseToken": {"symbol": "DOG", "name": "Dog Token"},
            "usd_market_cap": 12345,
        },
        source="gmgn_test",
        captured_at=1000,
    )
    assert normalized["ca"] == "TokenCA"
    assert normalized["symbol"] == "DOG"
    assert normalized["name"] == "Dog Token"
    assert normalized["market_cap"] == 12345


def test_gmgn_candidate_collect_keeps_working_source_when_one_source_fails(monkeypatch):
    def fake_run_gmgn(args, timeout=20):
        if args[:2] == ["market", "trending"]:
            return {"data": {"rank": [{"address": "TokenCA", "symbol": "DOG", "market_cap": 10000}]}}
        raise RuntimeError("upstream failed")

    monkeypatch.setattr(gmgn_candidate_scout, "run_gmgn", fake_run_gmgn)

    candidates, errors = collect_candidates_with_errors(chain="sol", limit=3)

    assert [candidate["ca"] for candidate in candidates] == ["TokenCA"]
    assert len(errors) == 2
