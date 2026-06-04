import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from missed_dog_blocker_ranking import build_missed_dog_blocker_ranking


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            created_event_ts REAL,
            token_ca TEXT,
            symbol TEXT,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            tradable_missed INTEGER,
            tradability_status TEXT,
            tradability_reason TEXT,
            max_pnl_recorded REAL,
            pnl_15m REAL
        )
        """
    )
    return db


def insert_missed(db, *, route="ATH", component="scout_quality", reason="buy_pressure_weak", tradable=1, peak=1.2):
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            created_event_ts, token_ca, symbol, route, component, reject_reason,
            tradable_missed, tradability_status, tradability_reason,
            max_pnl_recorded, pnl_15m
        ) VALUES (1000, 'Token'||random(), 'DOG', ?, ?, ?, ?, 'tradable_reclaim', '', ?, ?)
        """,
        (route, component, reason, tradable, peak, peak),
    )
    db.commit()


def test_ranking_allows_a_class_only_for_soft_missed_dog_blocker():
    db = memory_db()
    insert_missed(db, reason="scout_quality_buy_pressure_weak", peak=1.2)
    insert_missed(db, reason="scout_quality_buy_pressure_weak", peak=0.7)

    rows = build_missed_dog_blocker_ranking(db)["rows"]

    assert rows[0]["reject_reason"] == "scout_quality_buy_pressure_weak"
    assert rows[0]["dog100_n"] == 1
    assert rows[0]["dog50_n"] == 2
    assert rows[0]["recommendation"] == "allow_a_class_only"


def test_ranking_keeps_security_blocker_hard_even_with_big_peak():
    db = memory_db()
    insert_missed(db, component="security", reason="security_red_flag_creator_dump", peak=3.0)

    row = build_missed_dog_blocker_ranking(db)["rows"][0]

    assert row["security_blocker_count"] == 1
    assert row["recommendation"] == "keep_hard_block"


def test_ranking_reports_missing_table():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row

    result = build_missed_dog_blocker_ranking(db)

    assert result["available"] is False
    assert result["reason"] == "paper_missed_signal_attribution_missing"
