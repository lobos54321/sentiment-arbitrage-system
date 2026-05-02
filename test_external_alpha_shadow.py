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
)


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
