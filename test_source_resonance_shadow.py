#!/usr/bin/env python3

import os
import sqlite3
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from external_alpha_shadow import init_external_alpha_shadow, lookup_external_alpha, record_external_alpha_candidates  # noqa: E402
from source_resonance_shadow import lookup_entry_quote_audit, lookup_quote_shadow, run_once  # noqa: E402


def test_source_resonance_builds_gmgn_quote_clean_cohort(tmp_path):
    signal_db_path = tmp_path / "sentiment.db"
    paper_db_path = tmp_path / "paper.db"

    signal_db = sqlite3.connect(signal_db_path)
    signal_db.execute(
        """
        CREATE TABLE premium_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_ca TEXT,
            symbol TEXT,
            market_cap REAL,
            holders INTEGER,
            volume_24h REAL,
            top10_pct REAL,
            timestamp INTEGER,
            source_message_ts INTEGER,
            receive_ts INTEGER,
            signal_type TEXT,
            is_ath INTEGER,
            signal_source TEXT,
            source_event_id TEXT,
            gate_result TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    signal_db.execute(
        """
        INSERT INTO premium_signals (
            token_ca, symbol, market_cap, holders, volume_24h, top10_pct,
            timestamp, source_message_ts, receive_ts, signal_type, is_ath,
            signal_source, source_event_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("TokenCA", "DOG", 12000, 120, 50000, 20, 1000, 900, 950, "NEW_TRENDING", 0, "premium_channel", "evt-1"),
    )
    signal_db.commit()
    signal_db.close()

    paper_db = sqlite3.connect(paper_db_path)
    paper_db.row_factory = sqlite3.Row
    init_external_alpha_shadow(paper_db)
    record_external_alpha_candidates(
        paper_db,
        [{
            "source": "gmgn_trending_1m",
            "chain": "sol",
            "ca": "TokenCA",
            "symbol": "DOG",
            "market_cap": 10000,
            "liquidity": 6000,
            "volume": 20000,
            "swaps": 100,
            "buys": 70,
            "sells": 30,
        }],
        captured_at=800,
    )
    paper_db.execute(
        """
        CREATE TABLE lotto_not_ath_watch_shadow_snapshots (
            token_ca TEXT,
            signal_ts INTEGER,
            quote_clean INTEGER,
            snapshot_pass INTEGER
        )
        """
    )
    paper_db.executemany(
        "INSERT INTO lotto_not_ath_watch_shadow_snapshots VALUES (?, ?, ?, ?)",
        [("TokenCA", 1000, 1, 0), ("TokenCA", 1000, 1, 1)],
    )
    paper_db.execute(
        """
        CREATE TABLE paper_decision_events (
            event_ts REAL,
            token_ca TEXT,
            signal_ts INTEGER,
            component TEXT,
            event_type TEXT,
            decision TEXT
        )
        """
    )
    paper_db.execute(
        "INSERT INTO paper_decision_events VALUES (?, ?, ?, ?, ?, ?)",
        (980, "TokenCA", 1000, "execution_api", "entry_quote", "pass"),
    )
    paper_db.commit()
    paper_db.close()

    summary = run_once(
        paper_db_path=paper_db_path,
        signal_db_path=signal_db_path,
        lookback_hours=1,
        limit=10,
        now=1200,
    )

    assert summary == {
        "signals": 1,
        "candidates": 1,
        "gmgn_pre_seen": 1,
        "dual_source": 1,
        "quote_clean": 1,
    }

    db = sqlite3.connect(paper_db_path)
    db.row_factory = sqlite3.Row
    row = db.execute("SELECT * FROM source_resonance_candidates WHERE token_ca = 'TokenCA'").fetchone()
    assert row["cohort"] == "telegram_gmgn_quote_clean"
    assert row["resonance_level"] == 3
    assert row["gmgn_lead_time_sec"] == 100
    assert row["two_quote_clean_snapshots"] == 1

    stages = {
        row["stage"]
        for row in db.execute("SELECT stage FROM latency_audit_events WHERE token_ca = 'TokenCA'")
    }
    assert {"source_event", "telegram_receive", "premium_signal_recorded", "paper_first_decision"} <= stages
    db.close()


def test_external_alpha_lookup_tolerates_small_clock_skew(tmp_path):
    paper_db_path = tmp_path / "paper.db"
    db = sqlite3.connect(paper_db_path)
    db.row_factory = sqlite3.Row
    init_external_alpha_shadow(db)
    record_external_alpha_candidates(
        db,
        [{
            "source": "gmgn_trending_1m",
            "chain": "sol",
            "ca": "TokenSkew",
            "symbol": "SKEW",
            "market_cap": 12000,
            "liquidity": 6000,
            "volume": 20000,
            "swaps": 100,
            "buys": 60,
            "sells": 40,
        }],
        captured_at=1060,
    )
    db.commit()

    alpha = lookup_external_alpha(db, "TokenSkew", now=1000, signal_ts=1000, lookback_sec=600)

    assert alpha["available"] is True
    assert alpha["timestamp_valid"] is True
    assert alpha["last_seen_age_sec"] == 0
    assert alpha["timestamp_adjusted_future_skew_sec"] == 60
    assert alpha["timestamp_adjusted_lead_skew_sec"] == 60
    db.close()


def test_entry_quote_audit_excludes_synthetic_fallback_from_quote_clean(tmp_path):
    paper_db_path = tmp_path / "paper.db"
    db = sqlite3.connect(paper_db_path)
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_decision_events (
            event_ts REAL,
            token_ca TEXT,
            signal_ts INTEGER,
            component TEXT,
            event_type TEXT,
            decision TEXT
        )
        """
    )
    db.executemany(
        "INSERT INTO paper_decision_events VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1001, "TokenCA", 1000, "execution_api", "entry_quote", "fallback"),
            (1002, "TokenCA", 1000, "execution_api", "entry_quote", "filled_synthetic_paper"),
        ],
    )
    db.commit()

    audit = lookup_entry_quote_audit(db, "TokenCA", 1000)

    assert audit["entry_quote_success_seen"] == 0
    assert audit["entry_quote_fail_seen"] == 0
    assert audit["first_decision_ts"] == 1001
    db.close()


def test_quote_clean_lookup_matches_nearby_signal_ts(tmp_path):
    paper_db_path = tmp_path / "paper.db"
    db = sqlite3.connect(paper_db_path)
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE lotto_not_ath_watch_shadow_snapshots (
            token_ca TEXT,
            signal_ts INTEGER,
            quote_clean INTEGER,
            snapshot_pass INTEGER
        )
        """
    )
    db.execute(
        "INSERT INTO lotto_not_ath_watch_shadow_snapshots VALUES (?, ?, ?, ?)",
        ("TokenCA", 1300, 1, 1),
    )
    db.execute(
        """
        CREATE TABLE paper_decision_events (
            event_ts REAL,
            token_ca TEXT,
            signal_ts INTEGER,
            component TEXT,
            event_type TEXT,
            decision TEXT
        )
        """
    )
    db.execute(
        "INSERT INTO paper_decision_events VALUES (?, ?, ?, ?, ?, ?)",
        (1302, "TokenCA", 1300, "execution_api", "entry_quote", "pass"),
    )
    db.commit()

    quote_shadow = lookup_quote_shadow(db, "TokenCA", 1000)
    quote_audit = lookup_entry_quote_audit(db, "TokenCA", 1000)

    assert quote_shadow["quote_clean_seen"] == 1
    assert quote_shadow["snapshot_pass_seen"] == 1
    assert quote_audit["entry_quote_success_seen"] == 1
    assert quote_audit["first_decision_ts"] == 1302
    db.close()
